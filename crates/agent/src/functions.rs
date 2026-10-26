// The functions the engine's runs call (docs/designs/03-runs-on-the-engine.md, A):
// 1. the registry and typed outcomes; 2. MCP tools from the snapshot;
// 3. loop functions; 4. context functions; 5. child starters and their
// settlement; 6. guard functions; 7. dispatcher functions; 8. workflow steps;
// 9. the catalog authors see.
//
// Every handler holds the engine weakly. A host function serves only the runs
// its host data says it serves (`FunctionContext::host`): loop functions a
// loop run, guard functions a guard run, dispatcher functions the signal's
// run. YAML can neither read nor forge host data, and a run a workflow starts
// itself has none (invariant 3), so a refusal is the answer to any other
// caller, recorded as not executed.
use crate::{
    agent::{Effect, Engine, EngineDeps, LoopOptions, Refusal, refuse, tool_call},
    errors::is_aborted,
    providers,
    runs::{GUARD, GuardHost, PRIMARY, RECOVERY, SIGNAL, SignalHost},
    supervisor::PendingSignal,
    tracing::{ContextUpdate, EventStart, Level, Scope, Span, SpanEnd, SpanKind, SpanStart},
};
use ouro_runtime::{ChildStart, Delivery, Function, FunctionContext, FunctionError, Registry};
use serde_json::{Map, Value, json};
use std::future::Future;
use std::sync::{Arc, Weak};

// ── 1. the registry ─────────────────────────────────────────────────────────

/// Taking signals is the supervisor's: a workflow that did would steal them.
pub const WITHHELD: &[&str] = &["get_next_signal"];

/// The registry of the engine's runs: every MCP tool the snapshot ever held
/// (permissive schemas, at most once unless MCP marks it read-only; an absent
/// one answers `not_executed`), the host functions and the workflow steps.
pub fn registry(engine: Weak<Engine>, deps: &EngineDeps) -> Registry {
    let mut registry = Registry::new();
    let mut tools: std::collections::BTreeMap<String, bool> =
        deps.db.tool_snapshot().unwrap_or_default().into_iter().collect();
    for tool in deps.mcp.tools() {
        let read_only = deps.mcp.read_only(&tool.name);
        tools.insert(tool.name, read_only);
    }
    for (name, read_only) in tools {
        if WITHHELD.contains(&name.as_str())
            || crate::mcp::SUPERVISOR_ONLY_TOOLS.contains(&name.as_str())
            || HOST_FUNCTIONS.iter().any(|f| f.name == name)
            || matches!(name.as_str(), "start_child" | "child_outcome")
        {
            continue;
        }
        let delivery = if read_only { Delivery::AtLeastOnce } else { Delivery::AtMostOnce };
        let tool = name.clone();
        add(&mut registry, &engine, &name, delivery, move |engine, args, ctx| {
            mcp_tool(engine, tool.clone(), args, ctx)
        });
    }
    let once = Delivery::AtMostOnce;
    let repeat = Delivery::AtLeastOnce;
    add(&mut registry, &engine, "agent_turn", repeat, agent_turn);
    add(&mut registry, &engine, "agent_settle", repeat, agent_settle);
    add(&mut registry, &engine, "loop_setup", repeat, loop_setup);
    add(&mut registry, &engine, "classify", repeat, classify);
    add(&mut registry, &engine, "tool_once", once, |e, a, c| tool(e, a, c, true));
    add(&mut registry, &engine, "tool_repeat", repeat, |e, a, c| tool(e, a, c, false));
    for name in CONTEXT_FUNCTIONS {
        add(&mut registry, &engine, name, repeat, move |e, a, c| context(e, a, c, name));
    }
    add(&mut registry, &engine, "start_guard", repeat, start_guard);
    add(&mut registry, &engine, "start_worker", repeat, start_worker);
    add(&mut registry, &engine, "primary_loop", repeat, primary_loop);
    add(&mut registry, &engine, "recovery_loop", repeat, recovery_loop);
    add(&mut registry, &engine, "run_saved_workflow", repeat, run_saved_workflow);
    add(&mut registry, &engine, "route_signal", repeat, route_signal);
    add(&mut registry, &engine, "settle_child", repeat, settle_child);
    add(&mut registry, &engine, "review", repeat, review);
    add(&mut registry, &engine, "confirm_request", once, confirm_request);
    add(&mut registry, &engine, "resolve_review", repeat, resolve_review);
    add(&mut registry, &engine, "deliver_event", repeat, deliver_event);
    add(&mut registry, &engine, "signal_session", repeat, signal_session);
    add(&mut registry, &engine, "signal_transcript", repeat, signal_transcript);
    add(&mut registry, &engine, "set_memory", once, set_memory);
    for name in ["llm_compose", "filter_relevant", "choose", "hash_value", "skill_catalog_snapshot"] {
        add(&mut registry, &engine, name, repeat, move |e, a, c| step(e, a, c, name));
    }
    registry
}

fn add<F, Fut>(registry: &mut Registry, engine: &Weak<Engine>, name: &str, delivery: Delivery, handler: F)
where
    F: Fn(Arc<Engine>, Value, FunctionContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Value, FunctionError>> + Send + 'static,
{
    let engine = engine.clone();
    let function = Function::new(name, json!({}), json!({}), move |args: Value, ctx: FunctionContext| {
        let call = engine.upgrade().map(|engine| handler(engine, args, ctx));
        async move {
            let Some(call) = call else { return Err(FunctionError::interrupted("the agent engine is closed")) };
            // A handler's panic is its call's failure: the engine worker
            // polls every run's calls, and must outlive a bug in one.
            match futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(call)).await {
                Ok(result) => result,
                Err(panic) => {
                    let detail = panic
                        .downcast_ref::<String>()
                        .map(String::as_str)
                        .or_else(|| panic.downcast_ref::<&str>().copied())
                        .unwrap_or("a handler panicked");
                    Err(FunctionError::runtime(format!("internal error: {detail}")))
                }
            }
        }
    })
    .with_delivery(delivery);
    registry.register(function).expect("unique function names");
}

/// A host function's error as the engine records it: a refusal did
/// nothing (`not_executed`); a stop (shutdown, a cancelled run) is no answer
/// (`interrupted`, the run resumes); anything else is a definite failure.
fn failed(error: anyhow::Error) -> FunctionError {
    if error.downcast_ref::<Refusal>().is_some() {
        FunctionError::not_executed(error.to_string())
    } else if is_aborted(&error) {
        FunctionError::interrupted(error.to_string())
    } else {
        FunctionError::runtime(error.to_string())
    }
}

// An at-most-once call's error: a refusal proves nothing happened; anything
// else (a lost loop, a stop mid-send) leaves its outcome unknown.
fn failed_once(error: anyhow::Error) -> FunctionError {
    if error.downcast_ref::<Refusal>().is_some() {
        FunctionError::not_executed(error.to_string())
    } else {
        FunctionError::unknown_outcome(error.to_string())
    }
}

fn task_label(ctx: &FunctionContext) -> String {
    ctx.task_path.rsplit('/').next().unwrap_or(&ctx.function).to_owned()
}

fn metadata(value: Value) -> Map<String, Value> {
    value.as_object().cloned().unwrap_or_default()
}

fn strings(args: &Value, key: &str) -> Vec<String> {
    args[key].as_array().into_iter().flatten().filter_map(Value::as_str).map(str::to_owned).collect()
}

// ── 2. MCP tools ────────────────────────────────────────────────────────────

/// A direct call of an MCP tool from a workflow: its outcome typed by how far
/// it got (design 03, R-M8): not offered by MCP now or refused before sending
/// (`not_executed`), sent without an answer (`unknown_outcome`), a tool's own
/// error (a definite failure).
async fn mcp_tool(
    engine: Arc<Engine>,
    name: String,
    args: Value,
    ctx: FunctionContext,
) -> Result<Value, FunctionError> {
    let Value::Object(args) = args else {
        return Err(FunctionError::not_executed(format!("{name}: arguments must be an object")));
    };
    if !engine.deps.mcp.tools().iter().any(|t| t.name == name) {
        return Err(FunctionError::not_executed(format!("{name} is not offered by MCP now")));
    }
    let scope = engine.scope_for(&ctx.run_id, &ctx.definition);
    let span = scope.span(
        SpanStart::new(&name, SpanKind::Tool)
            .input(json!(args))
            .metadata(metadata(json!({"tool":name,"reference":ctx.task_path,"workflow_run":ctx.run_id}))),
    );
    let call = async {
        if ctx.delivery == Delivery::AtMostOnce {
            engine.deps.mcp.call_tool_once(&name, args, &ctx.cancel).await
        } else {
            engine.deps.mcp.call_tool(&name, args, &ctx.cancel).await.map_err(crate::mcp::ToolCallFailure::Unknown)
        }
    };
    let raw = trace_mcp_call(span, call).await.map_err(|failure| match failure {
        crate::mcp::ToolCallFailure::NotExecuted(error) => FunctionError::not_executed(format!("{name}: {error}")),
        crate::mcp::ToolCallFailure::Unknown(error) if is_aborted(&error) => {
            FunctionError::interrupted(error.to_string())
        }
        crate::mcp::ToolCallFailure::Unknown(error) => FunctionError::unknown_outcome(format!("{name}: {error}")),
    })?;
    if raw.starts_with("[tool error]") {
        return Err(FunctionError::communication(500, raw));
    }
    Ok(crate::flows::parse_tool_json(&raw))
}

// Calls need their own spans: the complete wire reply. The future may be
// dropped by the engine on cancellation, which must end the span too.
async fn trace_mcp_call(
    span: Arc<dyn Span>,
    call: impl Future<Output = Result<String, crate::mcp::ToolCallFailure>>,
) -> Result<String, crate::mcp::ToolCallFailure> {
    let mut pending = PendingToolSpan(Some(span));
    let result = call.await;
    let end = match &result {
        Ok(raw) if raw.starts_with("[tool error]") => SpanEnd::error(raw).with_output(json!(raw)),
        Ok(raw) => SpanEnd::output(json!(raw)),
        Err(crate::mcp::ToolCallFailure::NotExecuted(error) | crate::mcp::ToolCallFailure::Unknown(error)) => {
            SpanEnd::error(error.to_string()).with_output(json!({"error":error.to_string()}))
        }
    };
    if let Some(span) = pending.0.take() {
        span.end(end);
    }
    result
}

struct PendingToolSpan(Option<Arc<dyn Span>>);

impl Drop for PendingToolSpan {
    fn drop(&mut self) {
        if let Some(span) = self.0.take() {
            let error = "Workflow tool call interrupted; outcome unknown";
            span.end(SpanEnd::error(error).with_output(json!({"error":error})));
        }
    }
}

// ── 3. loop functions ───────────────────────────────────────────────────────

async fn live_loop(engine: &Arc<Engine>, ctx: &FunctionContext) -> anyhow::Result<crate::agent::Entered> {
    Ok(engine.loop_of(&ctx.run_id, ctx.host(), &ctx.function).await?.0)
}

async fn agent_turn(engine: Arc<Engine>, args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    let live = live_loop(&engine, &ctx).await.map_err(failed)?;
    crate::agent::loop_turn(&engine, &live, &args).await.map_err(failed)
}

async fn agent_settle(engine: Arc<Engine>, args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    let live = live_loop(&engine, &ctx).await.map_err(failed)?;
    crate::agent::loop_settle(&live, &args).await.map_err(failed)
}

async fn loop_setup(engine: Arc<Engine>, args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    let setup = async {
        let live = live_loop(&engine, &ctx).await?;
        let saved = engine.runtime.get(&ctx.run_id).await?.ok_or_else(|| anyhow::anyhow!("unknown run"))?;
        let records = engine.runtime.records(&ctx.run_id).await?;
        crate::agent::loop_setup(&engine, &live, &saved.admission.input, &args, &records).await
    };
    setup.await.map_err(failed)
}

async fn classify(engine: Arc<Engine>, args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    let classified = async {
        live_loop(&engine, &ctx).await?;
        let call = tool_call(&args["call"])?;
        let once =
            crate::agent::runs_once(&engine, &call, args["guard_run"].as_str(), args["worker_run"].as_str().is_some())?;
        Ok(json!({"once": once}))
    };
    classified.await.map_err(failed)
}

async fn tool(engine: Arc<Engine>, args: Value, ctx: FunctionContext, once: bool) -> Result<Value, FunctionError> {
    let call = async {
        let live = live_loop(&engine, &ctx).await?;
        crate::agent::loop_tool(&engine, &live, &args, &ctx.run_id, &ctx.idempotency_key, once).await
    };
    call.await.map_err(if once { failed_once } else { failed })
}

// ── 4. context functions ────────────────────────────────────────────────────
// Callable by the signal's run and by loops (design 03, R-M11), on the signal
// the runtime started the run for.

pub const CONTEXT_FUNCTIONS: [&str; 4] =
    ["signal_input", "recent_activity", "pending_confirmations", "preselect_skills"];

async fn context(
    engine: Arc<Engine>,
    args: Value,
    ctx: FunctionContext,
    name: &'static str,
) -> Result<Value, FunctionError> {
    let call = async {
        let at = match crate::runs::host_kind(ctx.host()) {
            Some(SIGNAL) => {
                let (host, signal) = signal_of(&engine, &ctx, name).await?;
                let records = engine.runtime.records(&ctx.run_id).await?;
                crate::agent::ContextHost {
                    run: ctx.run_id.clone(),
                    session: signal_session_of(&engine, &host, &records)?,
                    scope: engine.scope_for(&ctx.run_id, &ctx.definition),
                    signal: Some(signal),
                    loaded: Vec::new(),
                }
            }
            _ => {
                let (live, host) = engine.loop_of(&ctx.run_id, ctx.host(), name).await?;
                let signal = match host.signal_id {
                    Some(id) => engine
                        .runtime
                        .get(&signal_run(id))
                        .await?
                        .and_then(|run| crate::supervisor::admitted_signal(&run.admission.input["signal"])),
                    None => None,
                };
                crate::agent::ContextHost {
                    run: ctx.run_id.clone(),
                    session: live.opts.session.clone(),
                    scope: live.scope(),
                    signal,
                    loaded: live.skill_names(),
                }
            }
        };
        crate::agent::loop_context(&engine, &at, name, &args, &ctx.cancel).await
    };
    call.await.map_err(failed)
}

pub fn signal_run(id: i64) -> String {
    format!("signal:{id}")
}

// ── 5. child starters ───────────────────────────────────────────────────────
// A host function vouches for a child by starting it (design 03, decision 3):
// at least once, its child's id derived from the call (`child_id`), so a
// re-dispatch joins the child its first dispatch committed. Starters build no
// loop (R2-M1); anchors and evidence are written before the start,
// insert-or-ignore.

// The child's trace: a span under the starter's scope, which the child's
// functions trace into while it lives in this process.
async fn anchor_child(
    engine: &Engine,
    child: &str,
    span: Arc<dyn Span>,
    session: Option<String>,
) -> anyhow::Result<()> {
    if let Some(anchor) = span.anchor() {
        let row = crate::runs::RunTrace {
            run_id: child.into(),
            trace_id: anchor.trace_id,
            session_id: session.or(anchor.session_id),
            parent_span_id: Some(anchor.parent_span_id),
            recorded: false,
            created_ms: crate::runs::now_ms(),
        };
        engine.deps.db.write(move |db| db.insert_run_trace(&row)).await?;
    }
    if engine.scopes.current(child).is_none() {
        engine.scopes.own(child, span);
    } else {
        span.end(SpanEnd::default());
    }
    Ok(())
}

/// `start_guard { calls, i }`: the round's guard, a host child named guard
/// whose host data names this loop, the round, the confirmation target and
/// the guard context it judges by; `{run: null}` for a round with only local
/// reads (no review, no model request).
async fn start_guard(engine: Arc<Engine>, args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    let start = async {
        let live = live_loop(&engine, &ctx).await?;
        let calls = args["calls"]
            .as_array()
            .ok_or_else(|| refuse("start_guard: calls required"))?
            .iter()
            .map(tool_call)
            .collect::<anyhow::Result<Vec<_>>>()?;
        if !calls.iter().any(|c| crate::guard::needs_guard(&engine, c)) {
            return Ok(json!({"run": null}));
        }
        let round = args["i"].as_u64().ok_or_else(|| refuse("start_guard: a round `i` is required"))?;
        let id = ctx.child_id("guard");
        // Freeze history before independent calls in this round can run.
        let (chain, count) = live.guard_history().await;
        let input = json!({"calls": args["calls"], "round": round, "history": [chain, count]});
        let context = live.guard_snapshot().await?;
        let host = GuardHost {
            kind: GUARD.into(),
            parent_loop: ctx.run_id.clone(),
            round,
            confirm: live.opts.confirm.clone(),
            evidence: {
                let (run, body) = (id.clone(), serde_json::to_string(&context)?);
                engine.deps.db.write(move |db| db.put_evidence(&run, &body)).await?
            },
        };
        let span = live.scope().span(SpanStart::new("guard", SpanKind::Chain).input(input.clone()));
        anchor_child(&engine, &id, span, live.opts.session_id.clone()).await?;
        let child = ctx
            .start_child(ChildStart {
                key: "guard".into(),
                name: crate::flows::GUARD_NAME.into(),
                version: None,
                input,
                host: serde_json::to_value(&host)?,
            })
            .await
            .map_err(anyhow::Error::from)?;
        // The calls the joined child reviews: its own stored input.
        let joined = engine.runtime.get(&child.id).await?.map(|r| r.admission.input["calls"].clone());
        let guarded: Vec<usize> = joined
            .as_ref()
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .enumerate()
            .filter(|(_, c)| tool_call(c).is_ok_and(|c| crate::guard::needs_guard(&engine, &c)))
            .map(|(i, _)| i)
            .collect();
        Ok(json!({"run": child.id, "key": child.key, "done": child.done, "waiting": child.waiting, "guarded": guarded}))
    };
    start.await.map_err(failed)
}

/// `start_worker { call, i, index, memory }`: the model's `invoke_sub_agent`
/// call as a host child, a loop with the worker's host data: it inherits the
/// confirmation target (invariant 5) and the guard's evidence. `{id: null}`
/// when it cannot start; `tool_repeat` then presents the error to the model.
async fn start_worker(engine: Arc<Engine>, args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    let start = async {
        let live = live_loop(&engine, &ctx).await?;
        if !live.offers("invoke_sub_agent") {
            return Ok(json!({"id": null}));
        }
        live.restore_working_memory(args.get("memory"))?;
        let call = tool_call(&args["call"])?;
        let (i, index) = args["i"]
            .as_u64()
            .zip(args["index"].as_u64())
            .ok_or_else(|| refuse("start_worker: i and index are required"))?;
        let key = crate::agent::worker_key(i, index);
        let id = ctx.child_id(&key);
        let Ok(arguments) = serde_json::from_str::<Map<String, Value>>(&call.arguments) else {
            return Ok(json!({"id": null}));
        };
        let mut meta = live.observation_metadata();
        meta.insert("tool".into(), call.name.clone().into());
        meta.insert("tool_call_id".into(), call.id.clone().into());
        let span = live
            .scope()
            .span(SpanStart::new("invoke_sub_agent", SpanKind::Agent).input(json!(arguments)).metadata(meta));
        let (mut opts, prompt) = match live.worker_options(&arguments, span.clone(), Some(&id)).await {
            Ok(prepared) => prepared,
            Err(error) => {
                span.end(SpanEnd::error(error.to_string()));
                return Ok(json!({"id": null}));
            }
        };
        opts.trace_scope = None;
        let prepared = match crate::agent::prepare(&engine, &opts).await {
            Ok(prepared) => prepared,
            Err(error) => {
                span.end(SpanEnd::error(error.to_string()));
                return Ok(json!({"id": null}));
            }
        };
        let input = json!({
            "messages": [providers::system(&prepared.system), providers::user(&prompt)],
            "memory": live.opts.session.memory.snapshot(),
        });
        let host = crate::agent::loop_host(
            &engine,
            &opts,
            crate::runs::WORKER,
            &id,
            &prepared.skill_text(&opts.loaded_skill_text),
        )
        .await?;
        anchor_child(&engine, &id, span, live.opts.session_id.clone()).await?;
        let child = ctx
            .start_child(ChildStart {
                key,
                name: opts.workflow.clone(),
                version: None,
                input,
                host: serde_json::to_value(&host)?,
            })
            .await
            .map_err(anyhow::Error::from)?;
        Ok(serde_json::to_value(child)?)
    };
    start.await.map_err(failed)
}

/// `primary_loop { input, confirm, skills, loaded, preset, allow_skill_writes,
/// system, loop? }`: the signal's primary agent, a host child. The `signal`
/// definition is the trusted source of these arguments (invariant 3
/// restated); what the signal's context functions captured (earlier user
/// messages, the working memory, the preselected block) comes from their
/// records in this run's journal (decision 4).
async fn primary_loop(engine: Arc<Engine>, args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    let start = async {
        let (host, signal) = signal_of(&engine, &ctx, "primary_loop").await?;
        let records = engine.runtime.records(&ctx.run_id).await?;
        let session = signal_session_of(&engine, &host, &records)?;
        let recorded_input = crate::agent::recorded_output(&records, "signal_input");
        if let Some(memory) = recorded_input.and_then(|o| o["memory"].as_str()) {
            let snapshot = engine.evidence_of(Some(&memory.to_owned()))?.unwrap_or_default();
            session.memory.merge(&serde_json::from_str(&snapshot)?)?;
        }
        let earlier: Vec<String> = match recorded_input.and_then(|o| o["earlier"].as_str()) {
            Some(digest) => serde_json::from_str(&engine.evidence_of(Some(&digest.to_owned()))?.unwrap_or_default())?,
            None => Vec::new(),
        };
        let input = args["input"].as_str().ok_or_else(|| anyhow::anyhow!("input: Required"))?;
        let preset = args["preset"].as_str().unwrap_or("smart");
        let preset = crate::models::PresetName::selectable(preset)
            .ok_or_else(|| anyhow::anyhow!("preset: unknown preset {preset}"))?;
        let system = strings(&args, "system");
        let key = "primary";
        let id = ctx.child_id(key);
        let mut opts = LoopOptions::new(&id, session.clone());
        opts.confirm =
            args.get("confirm").filter(|v| !v.is_null()).map(|v| serde_json::from_value(v.clone())).transpose()?;
        opts.earlier_user_messages = earlier;
        if let Some(block) =
            crate::agent::recorded_output(&records, "preselect_skills").and_then(|o| o["block"].as_str())
            && system.iter().any(|part| part.contains(block))
        {
            opts.loaded_skill_text = block.to_owned();
        }
        opts.skills = strings(&args, "skills");
        opts.allow_skill_writes = args["allow_skill_writes"].as_bool().unwrap_or(false);
        opts.system_prompt = Some(system.join("\n\n"));
        opts.preset = preset;
        opts.guard_request = engine.evidence_of(Some(&host.evidence.content))?;
        opts.conversation = crate::supervisor::conversation(&signal);
        opts.signal_id = Some(host.signal_id);
        opts.session_id = Some(session.id.clone());
        let prepared = crate::agent::prepare(&engine, &opts).await?;
        let input = json!({
            "messages": [providers::system(&prepared.system), providers::user(input)],
            "memory": session.memory.snapshot(),
        });
        let loop_host =
            crate::agent::loop_host(&engine, &opts, PRIMARY, &id, &prepared.skill_text(&opts.loaded_skill_text))
                .await?;
        let scope = engine.scope_for(&ctx.run_id, &ctx.definition);
        let span = scope
            .span(SpanStart::new("agent_loop", SpanKind::Chain).metadata(metadata(json!({"activity":"primary_loop"}))));
        anchor_child(&engine, &id, span, Some(session.id.clone())).await?;
        let child = ctx
            .start_child(ChildStart {
                key: key.into(),
                name: opts.workflow.clone(),
                version: None,
                input,
                host: serde_json::to_value(&loop_host)?,
            })
            .await
            .map_err(anyhow::Error::from)?;
        Ok(serde_json::to_value(child)?)
    };
    start.await.map_err(failed)
}

/// `recovery_loop { error, prompt, skills, preset, max_iterations, system }`:
/// the failure report as an independent root `recovery:<signal id>` (design
/// 03, R2-H2), so an exhausted tree budget never blocks it; returns at once
/// and the signal raises its failure as before. Nothing before
/// `signal_session` gets a report.
async fn recovery_loop(engine: Arc<Engine>, args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    let start = async {
        let (host, _) = signal_of(&engine, &ctx, "recovery_loop").await?;
        let records = engine.runtime.records(&ctx.run_id).await?;
        if crate::agent::recorded_output(&records, "signal_session").is_none() {
            return Ok(Value::Null);
        }
        let session = signal_session_of(&engine, &host, &records)?;
        let id = format!("{RECOVERY}:{}", host.signal_id);
        let prompt = args["prompt"].as_str().ok_or_else(|| anyhow::anyhow!("prompt: Required"))?;
        let preset = args["preset"].as_str().unwrap_or("base");
        let preset = crate::models::PresetName::selectable(preset)
            .ok_or_else(|| anyhow::anyhow!("preset: unknown preset {preset}"))?;
        let iterations = args["max_iterations"]
            .as_u64()
            .filter(|n| *n > 0)
            .and_then(|n| usize::try_from(n).ok())
            .ok_or_else(|| anyhow::anyhow!("max_iterations: Expected positive integer"))?;
        let mut opts = LoopOptions::new(&id, session.clone());
        opts.parent_id = Some(session.id.clone());
        opts.guard_request = engine.evidence_of(Some(&host.evidence.content))?;
        opts.skills = strings(&args, "skills");
        opts.include_engine_skills = false;
        opts.preset = preset;
        opts.max_iterations = iterations;
        opts.system_prompt = Some(strings(&args, "system").join("\n\n"));
        opts.session_id = Some(session.id.clone());
        let prepared = crate::agent::prepare(&engine, &opts).await?;
        let input = json!({
            "messages": [providers::system(&prepared.system), providers::user(prompt)],
            "memory": [],
        });
        let loop_host =
            crate::agent::loop_host(&engine, &opts, RECOVERY, &id, &prepared.skill_text(&opts.loaded_skill_text))
                .await?;
        let scope = engine.scope_for(&ctx.run_id, &ctx.definition);
        let span =
            scope.span(SpanStart::new("recovery", SpanKind::Agent).metadata(metadata(json!({"skill":"recovery"}))));
        anchor_child(&engine, &id, span, Some(session.id.clone())).await?;
        engine
            .runtime
            .start_with(
                &id,
                crate::agent::AGENT_LOOP_NAME,
                input,
                ouro_runtime::Start { version: None, host: serde_json::to_value(&loop_host)? },
            )
            .await?;
        Ok(json!({"started": id}))
    };
    // A reporting failure is an observation, never the signal's error.
    let started: anyhow::Result<Value> = start.await;
    match started {
        Ok(started) => Ok(started),
        Err(error) if error.downcast_ref::<Refusal>().is_some() || is_aborted(&error) => Err(failed(error)),
        Err(error) => {
            eprintln!("[{}] recovery failed: {error:#}", ctx.run_id);
            Ok(json!({"error": error.to_string()}))
        }
    }
}

/// `run_saved_workflow { name }`: a scheduled task's saved workflow. No saved
/// workflows run until the versions step (design 03, decision 9): the task
/// goes to the agent loop.
async fn run_saved_workflow(engine: Arc<Engine>, args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    let call = async {
        signal_of(&engine, &ctx, "run_saved_workflow").await?;
        let name = args["name"].as_str().ok_or_else(|| anyhow::anyhow!("name: Required"))?;
        let scope = engine.scope_for(&ctx.run_id, &ctx.definition);
        scope.event(EventStart {
            name: "fallback".into(),
            level: Some(Level::Warning),
            metadata: Some(metadata(json!({"stage":"saved_workflow","workflow":name,"reason":"missing"}))),
            ..Default::default()
        });
        scope.update(ContextUpdate::metadata(metadata(
            json!({"fallback":"agent-loop","workflow_failure_stage":"saved_workflow"}),
        )));
        Ok(json!({"done": false, "handover": format!(
            "This task names the saved workflow `{name}`, which has no active version. Do the task yourself."
        )}))
    };
    call.await.map_err(failed)
}

/// `route_signal { context }`: no routes until the versions step (decision 9).
async fn route_signal(engine: Arc<Engine>, _args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    signal_of(&engine, &ctx, "route_signal").await.map_err(failed)?;
    Ok(json!({"routed": false}))
}

/// `settle_child { ref, kind }`: what a child's end means for the signal
/// (design 03, decision 6), so YAML only switches on `action`: a primary or
/// route that faulted raises; one that was cancelled is done, without a
/// report; a saved workflow that changed nothing hands over, one that faulted
/// after a write raises.
async fn settle_child(engine: Arc<Engine>, args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    let settle = async {
        signal_of(&engine, &ctx, "settle_child").await?;
        let id = args["ref"]["id"].as_str().ok_or_else(|| refuse("settle_child: ref.id required"))?;
        let kind = args["kind"].as_str().unwrap_or(PRIMARY);
        let child = engine.runtime.get(id).await?.filter(|c| c.child.as_ref().is_some_and(|l| l.parent == ctx.run_id));
        let Some(child) = child else {
            return Err(refuse(format!("settle_child: {id} is not a child of this run")));
        };
        use ouro_runtime::ows::run::Status;
        if let Some(outcome) = &child.outcome {
            engine.ended_child(id, outcome).await;
        }
        let settled = match (&child.outcome, kind) {
            (Some(Status::Completed(output)), _) => {
                let text = output.as_str().map(str::to_owned).unwrap_or_else(|| output.to_string());
                if kind == PRIMARY {
                    let records = engine.runtime.records(&ctx.run_id).await?;
                    let loaded = primary_loaded(&records);
                    engine.scope_for(&ctx.run_id, &ctx.definition).update(ContextUpdate {
                        output: Some(json!(text)),
                        metadata: Some(metadata(json!({"skills": loaded}))),
                        ..Default::default()
                    });
                }
                json!({"action": "done", "output": text})
            }
            (Some(Status::Cancelled), _) => json!({"action": "done", "output": null}),
            (Some(Status::Faulted(error)), "saved") if !effects_started(&engine, id).await? => json!({
                "action": "handover",
                "handover": format!("The saved workflow for this task failed before changing anything: {}. Do the task yourself.", error.detail.clone().unwrap_or_else(|| error.to_string())),
            }),
            (Some(Status::Faulted(error)), _) => json!({"action": "raise", "error": error}),
            (_, "saved") => json!({"action": "done", "output": null}),
            _ => json!({"action": "done", "output": json!({"ok": true, "resumes": true}).to_string()}),
        };
        Ok(settled)
    };
    settle.await.map_err(failed)
}

fn primary_loaded(records: &crate::agent::Records) -> Vec<String> {
    use ouro_runtime::ows::durable::Record;
    records
        .values()
        .find_map(|record| match record {
            Record::Call { function, arguments, .. } if function == "primary_loop" => {
                Some(strings(arguments, "loaded"))
            }
            _ => None,
        })
        .unwrap_or_default()
}

/// Whether a run or one of its descendants started a write: an at-most-once
/// call that completed, failed or has an unknown outcome.
async fn effects_started(engine: &Engine, id: &str) -> anyhow::Result<bool> {
    use ouro_runtime::CallStatus;
    let mut pending = vec![id.to_owned()];
    while let Some(run) = pending.pop() {
        for call in engine.runtime.calls(&run).await.unwrap_or_default() {
            if call.delivery == Delivery::AtMostOnce
                && matches!(call.state, CallStatus::Completed | CallStatus::Failed | CallStatus::Unknown)
            {
                return Ok(true);
            }
        }
        let filter = ouro_runtime::durable::control::Runs { parent: Some(run), ..Default::default() };
        let page = engine.control.list(&filter, None, 1_000).await.map_err(anyhow::Error::from)?;
        pending.extend(page.items.into_iter().map(|r| r.admission.id));
    }
    Ok(false)
}

// ── 6. guard functions ──────────────────────────────────────────────────────

fn guard_host(ctx: &FunctionContext, function: &str) -> anyhow::Result<GuardHost> {
    match crate::runs::host_kind(ctx.host()) {
        Some(GUARD) => Ok(serde_json::from_value(ctx.host().cloned().unwrap_or_default())?),
        _ => Err(refuse(format!("{function}: only the built-in guard round calls it"))),
    }
}

async fn review(engine: Arc<Engine>, _args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    let call = async {
        let host = guard_host(&ctx, "review")?;
        let saved = engine.runtime.get(&ctx.run_id).await?.ok_or_else(|| anyhow::anyhow!("unknown run"))?;
        let scope = engine.scope_for(&ctx.run_id, &ctx.definition);
        crate::agent::guard_review(&engine, &host, &saved.admission.input, &scope, &ctx.cancel).await
    };
    call.await.map_err(failed)
}

// The reviewed item of `call` from this guard run's own `review` record,
// never from YAML.
async fn reviewed(
    engine: &Engine,
    ctx: &FunctionContext,
    args: &Value,
) -> anyhow::Result<(providers::ToolCall, String, crate::guard::Verdict, crate::agent::Records)> {
    let call = tool_call(&args["call"])?;
    let key = crate::guard::key(&call);
    let records = engine.runtime.records(&ctx.run_id).await?;
    let review = crate::agent::recorded_output(&records, "review").ok_or_else(|| refuse("missing review evidence"))?;
    let item = review["items"]
        .as_array()
        .and_then(|items| items.iter().find(|i| i["key"] == key))
        .ok_or_else(|| refuse("call was not reviewed"))?;
    let verdict: crate::guard::Verdict = serde_json::from_value(item["verdict"].clone())?;
    Ok((call, key, verdict, records))
}

/// `confirm_request { call }`: the card for a reviewed call that needs the
/// user's confirmation, sent at most once (invariant 6).
async fn confirm_request(engine: Arc<Engine>, args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    let host = guard_host(&ctx, "confirm_request").map_err(failed)?;
    let (call, _, verdict, _) = reviewed(&engine, &ctx, &args).await.map_err(failed)?;
    if verdict.label != "needs_confirmation" {
        return Err(FunctionError::not_executed("call needs no confirmation"));
    }
    let target = host.confirm.clone().ok_or_else(|| FunctionError::not_executed("no confirmation target"))?;
    let context: crate::guard::Context = engine
        .deps
        .db
        .evidence(&host.evidence)
        .map_err(failed)?
        .and_then(|e| serde_json::from_str(&e).ok())
        .ok_or_else(|| FunctionError::not_executed("guard evidence is missing"))?;
    let goal = if context.root_request.is_empty() { &context.task } else { &context.root_request };
    crate::confirmation::request(
        &engine.deps.db,
        &*engine.deps.mcp,
        crate::confirmation::Request {
            run: &ctx.run_id,
            loop_run: &host.parent_loop,
            call: &call,
            target: &target,
            guidance: &verdict.guidance,
            goal,
            card: &crate::confirmation::Card::load(&*engine.deps.memory),
        },
        &ctx.cancel,
    )
    .await
    .map_err(failed_once)
}

/// `resolve_review { call, outcome }`: the call's verdict. For one that
/// needed confirmation, the outcome is the event this run consumed, checked
/// against the card's chat, message and thread and spent once (invariant 1);
/// the YAML's word counts only for a real expiry.
async fn resolve_review(engine: Arc<Engine>, args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    let call = async {
        let host = guard_host(&ctx, "resolve_review")?;
        let (call, key, verdict, records) = reviewed(&engine, &ctx, &args).await?;
        let verdict = if verdict.label == "needs_confirmation" && host.confirm.is_some() {
            let store: &dyn crate::confirmation::Store = &engine.deps.db;
            let (run, card_key, claimed, events) = (
                ctx.run_id.clone(),
                key.clone(),
                args["outcome"].as_str().unwrap_or("unverified").to_owned(),
                consumed(&records),
            );
            let outcome = engine
                .deps
                .db
                .write(move |db| {
                    let store: &dyn crate::confirmation::Store = db;
                    match store.outcome(&run, &card_key)? {
                        Some(outcome) => Ok(outcome),
                        None => store.resolve(&run, &card_key, &claimed, &events),
                    }
                })
                .await?;
            crate::confirmation::close_card(
                store,
                &*engine.deps.mcp,
                &ctx.run_id,
                &call,
                &outcome,
                &crate::confirmation::Card::load(&*engine.deps.memory),
                &ctx.cancel,
            )
            .await;
            crate::agent::confirmation_verdict(&call, &outcome)
        } else {
            verdict
        };
        Ok(json!([key, verdict]))
    };
    call.await.map_err(failed)
}

// The events a run consumed, in record order.
fn consumed(records: &crate::agent::Records) -> Vec<crate::confirmation::Consumed> {
    use ouro_runtime::ows::durable::Record;
    records
        .values()
        .filter_map(|record| match record {
            Record::Event { event } => Some(crate::confirmation::Consumed {
                type_: event["type"].as_str().unwrap_or_default().to_owned(),
                data: event["data"].clone(),
            }),
            _ => None,
        })
        .collect()
}

// ── 7. dispatcher functions ─────────────────────────────────────────────────

const DISPATCHER_ONLY: &str = "only the signal dispatcher calls it";

/// The signal run's host data and its signal (the run's input, written by
/// the supervisor), or a refusal for any other run.
async fn signal_of(
    engine: &Engine,
    ctx: &FunctionContext,
    function: &str,
) -> anyhow::Result<(SignalHost, PendingSignal)> {
    let host: SignalHost = match crate::runs::host_kind(ctx.host()) {
        Some(SIGNAL) => serde_json::from_value(ctx.host().cloned().unwrap_or_default())?,
        _ => return Err(refuse(format!("{function}: {DISPATCHER_ONLY}"))),
    };
    let saved = engine.runtime.get(&ctx.run_id).await?.ok_or_else(|| anyhow::anyhow!("unknown run"))?;
    let signal = crate::supervisor::admitted_signal(&saved.admission.input["signal"])
        .ok_or_else(|| anyhow::anyhow!("the signal run has no signal"))?;
    Ok((host, signal))
}

// The signal's session, on the clock `signal_session` recorded.
fn signal_session_of(
    engine: &Engine,
    host: &SignalHost,
    records: &crate::agent::Records,
) -> anyhow::Result<crate::session::Session> {
    let recorded = crate::agent::recorded_output(records, "signal_session")
        .ok_or_else(|| anyhow::anyhow!("signal_session has not run"))?;
    let env = crate::session::EnvData::from_json(&recorded["env"])?;
    Ok(engine.session(&format!("{}:{}", host.source, host.signal_id), env))
}

/// `signal_session`: the incoming signal's session; its clock and env are
/// recorded in the output for the run's later functions (decision 4).
async fn signal_session(engine: Arc<Engine>, _args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    let call = async {
        let (host, signal) = signal_of(&engine, &ctx, "signal_session").await?;
        let deps = crate::session::EnvDeps {
            mcp: engine.deps.mcp.clone(),
            memory: engine.deps.memory.clone(),
            user_email: engine.deps.env.user_email.clone(),
            telegram_chat_id: engine.deps.env.telegram_chat_id.clone(),
        };
        let env = crate::session::gather_env_data(&deps, &ctx.cancel).await?;
        let context = crate::session::render_context(&env)?;
        let id = format!("{}:{}", host.source, host.signal_id);
        let default_chat = env.telegram_chat_id.clone();
        Ok(json!({"context": context, "engine_skills": engine.deps.skills,
            "conversation": crate::supervisor::conversation(&signal), "default_chat": default_chat,
            "env": env.to_json(), "session": id}))
    };
    call.await.map_err(failed)
}

/// `deliver_event`: a Telegram button press, delivered to the guard round
/// whose card holds the pressed token (design 03, decision 8), idempotent by
/// the press's identity. The press comes from the signal, never from YAML.
/// One that finds no waiting card falls through to the primary loop with
/// the reason, and the agent decides what to say.
async fn deliver_event(engine: Arc<Engine>, _args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    let call = async {
        let (host, signal) = signal_of(&engine, &ctx, "deliver_event").await?;
        let button = crate::supervisor::signal_value(&signal)["button"].clone();
        let token = button["data"].as_str().ok_or_else(|| refuse("deliver_event: the signal is not a button press"))?;
        let store: &dyn crate::confirmation::Store = &engine.deps.db;
        let card = store.by_token(token)?;
        let reason = match &card {
            None => Some("unknown_card"),
            Some((_, Some(_))) => Some("already_answered"),
            Some((card, None)) => match engine.runtime.get(&card.run).await? {
                Some(run) if run.ended_ms.is_none() => None,
                _ => Some("expired"),
            },
        };
        let delivered = match (&card, reason) {
            (Some((card, _)), None) => {
                let event = json!({"source": "telegram", "id": format!("telegram_button:{}", host.signal_id),
                    "type": token, "data": button});
                engine.runtime.deliver(event, Some(&card.run)).await?.is_some()
            }
            _ => false,
        };
        if delivered && let Some((card, _)) = &card {
            trace_delivery(&engine, &ctx, &card.run, token, &button);
            return Ok(json!({"delivered": true, "output": "Button delivered"}));
        }
        let reason = reason.unwrap_or("expired");
        let note = match reason {
            "already_answered" => {
                "The pressed button belongs to a confirmation card that was already answered; the press changed nothing."
            }
            "unknown_card" => "No confirmation card matches the pressed button; the press changed nothing.",
            _ => {
                "The pressed button's confirmation request is no longer waiting (it expired or its run ended); the press changed nothing."
            }
        };
        Ok(json!({"delivered": false, "reason": reason, "note": note}))
    };
    call.await.map_err(failed)
}

// The press in the trace of the run it went to; the signal's trace joins
// that request's session.
fn trace_delivery(engine: &Engine, ctx: &FunctionContext, target: &str, token: &str, data: &Value) {
    let Ok(Some(row)) = engine.deps.db.run_trace(target) else { return };
    engine
        .scope_for(&ctx.run_id, &ctx.definition)
        .update(ContextUpdate { session_id: row.session_id.clone(), ..Default::default() });
    let Some(anchor) = row.anchor() else { return };
    let trace = engine.deps.tracer.trace(crate::tracing::TraceStart {
        name: "delivery".into(),
        session_id: anchor.session_id.clone(),
        continuation: Some(anchor),
        ..Default::default()
    });
    trace.event(EventStart {
        name: format!("event:{token}"),
        input: Some(data.clone()),
        metadata: Some(metadata(json!({"run": target}))),
        ..Default::default()
    });
    // Its write may wait for the engine's lock on agent.db (runs.rs §2).
    tokio::task::spawn_blocking(move || trace.end());
}

/// `signal_transcript { ref }`: the bounded JSON transcript of this run's
/// own primary child, from its last loop record; `[]` without one.
async fn signal_transcript(engine: Arc<Engine>, args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    let call = async {
        signal_of(&engine, &ctx, "signal_transcript").await?;
        let Some(id) = args["ref"]["id"].as_str() else { return Ok(json!("[]")) };
        let own =
            engine.runtime.get(id).await?.is_some_and(|c| c.child.as_ref().is_some_and(|l| l.parent == ctx.run_id));
        if !own {
            return Err(refuse(format!("signal_transcript: {id} is not a child of this run")));
        }
        let records = engine.runtime.records(id).await.unwrap_or_default();
        let messages = crate::agent::history_messages(&records);
        let transcript = if messages.iter().any(|m| m["role"] != "system") {
            Value::Array(messages.to_vec()).to_string()
        } else {
            "[]".into()
        };
        Ok(json!(crate::activity::tail_utf16(&transcript, 20000)))
    };
    call.await.map_err(failed)
}

// ── 8. workflow steps ───────────────────────────────────────────────────────

fn text<'a>(args: &'a Value, function: &str, key: &str) -> Result<Option<&'a str>, FunctionError> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s)),
        Some(_) => Err(invalid(function, format!("`{key}` must be a string"))),
    }
}

fn invalid(name: &str, detail: impl std::fmt::Display) -> FunctionError {
    FunctionError::validation(format!("{name}: {detail}"))
}

/// `set_memory { key, value }`: a write to the memory KV, at most once. The
/// `engine.` keys are the runtime's own and refused (design 03, R2-L).
async fn set_memory(engine: Arc<Engine>, args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    let name = "set_memory";
    let key = text(&args, name, "key")?
        .filter(|k| !k.is_empty())
        .ok_or_else(|| invalid(name, "`key` must be a nonempty string"))?;
    if key.starts_with(crate::runs::RESERVED_MEMORY_PREFIX) {
        return Err(FunctionError::not_executed(format!("{name}: `{key}` is a key the runtime keeps")));
    }
    let value = text(&args, name, "value")?.ok_or_else(|| invalid(name, "`value` must be a string"))?;
    let scope = engine.scope_for(&ctx.run_id, &ctx.definition);
    let span = scope.span(
        SpanStart::new(name, SpanKind::Tool)
            .input(args.clone())
            .metadata(metadata(json!({"tool":name,"reference":ctx.task_path,"workflow_run":ctx.run_id}))),
    );
    // A write may wait for the engine's lock on agent.db (runs.rs §2).
    let (memory, stored, written) = (engine.deps.memory.clone(), key.to_owned(), value.to_owned());
    let result = match tokio::task::spawn_blocking(move || memory.set(&stored, &written)).await {
        Ok(set) => set.map(|()| json!({"ok":true,"key":key})),
        Err(error) => Err(anyhow::Error::from(error)),
    };
    span.end(match &result {
        Ok(value) => SpanEnd::output(value.clone()),
        Err(error) => SpanEnd::error(error.to_string()).with_output(json!({"error":error.to_string()})),
    });
    result.map_err(|e| FunctionError::not_executed(e.to_string()))
}

async fn step(
    engine: Arc<Engine>,
    args: Value,
    ctx: FunctionContext,
    name: &'static str,
) -> Result<Value, FunctionError> {
    let scope = engine.scope_for(&ctx.run_id, &ctx.definition);
    run_step(&engine, name, &args, &scope, &task_label(&ctx), &ctx.cancel).await
}

/// The model steps of a saved workflow: `llm_compose`, `filter_relevant`,
/// `choose`, and two reads, `hash_value` and `skill_catalog_snapshot`
/// (`set_memory` too, for the evals workbench), traced under `scope`.
pub async fn run_step(
    engine: &Arc<Engine>,
    name: &str,
    args: &Value,
    scope: &Scope,
    label: &str,
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<Value, FunctionError> {
    let (args, label) = (args.clone(), label.to_owned());
    if name == "set_memory" {
        let key = text(&args, name, "key")?
            .filter(|k| !k.is_empty())
            .ok_or_else(|| invalid(name, "`key` must be a nonempty string"))?;
        if key.starts_with(crate::runs::RESERVED_MEMORY_PREFIX) {
            return Err(FunctionError::not_executed(format!("{name}: `{key}` is a key the runtime keeps")));
        }
        let value = text(&args, name, "value")?.ok_or_else(|| invalid(name, "`value` must be a string"))?;
        return engine
            .deps
            .memory
            .set(key, value)
            .map(|()| json!({"ok":true,"key":key}))
            .map_err(|e| FunctionError::not_executed(e.to_string()));
    }
    let model = |e: anyhow::Error| {
        if is_aborted(&e) { FunctionError::interrupted(e.to_string()) } else { FunctionError::runtime(e.to_string()) }
    };
    match name {
        "hash_value" => {
            let value = args.get("value").ok_or_else(|| invalid(name, "`value` is required"))?;
            Ok(json!({"hash":crate::skills::hash_value(value)}))
        }
        "skill_catalog_snapshot" => crate::skills::catalog_snapshot(&*engine.deps.skill_store).await.map_err(model),
        "llm_compose" => {
            let preset = text(&args, name, "preset")?.unwrap_or("compose");
            let skill = text(&args, name, "skill")?;
            let mut parts = Vec::new();
            if let Some(prompt) = text(&args, name, "prompt")? {
                parts.push(prompt.to_owned());
            }
            match args.get("input") {
                None | Some(Value::Null) => {}
                Some(Value::Object(input)) => parts.push(crate::flows::render_input_xml(input)),
                Some(_) => return Err(invalid(name, "`input` must be an object")),
            }
            let json = match args.get("json") {
                None | Some(Value::Null) => false,
                Some(Value::Bool(json)) => *json,
                Some(_) => return Err(invalid(name, "`json` must be true or false")),
            };
            let user = parts.into_iter().filter(|p| !p.is_empty()).collect::<Vec<_>>().join("\n\n");
            let span = scope.span(
                SpanStart::new(format!("call:{label}"), SpanKind::Tool).input(json!({"skill":skill,"prompt":user})),
            );
            let scope: Scope = span.clone();
            let result = crate::flows::compose(engine, preset, skill, &user, json, &label, &scope, cancel).await;
            span.end(match &result {
                Ok(output) => SpanEnd::output(output.clone()),
                Err(error) => SpanEnd::error(error.to_string()),
            });
            result.map_err(model)
        }
        "filter_relevant" => {
            let decisions = engine
                .deps
                .decisions
                .as_ref()
                .ok_or_else(|| FunctionError::runtime("no decision model configured (OPEN_ROUTER_API_KEY)"))?;
            let items =
                args.get("items").and_then(Value::as_array).ok_or_else(|| invalid(name, "`items` must be an array"))?;
            let prompt = text(&args, name, "prompt")?.ok_or_else(|| invalid(name, "`prompt` is required"))?;
            let threshold = match args.get("threshold") {
                None | Some(Value::Null) => None,
                Some(t) => Some(t.as_f64().ok_or_else(|| invalid(name, "`threshold` must be a number"))?),
            };
            let filtered =
                crate::decisions::filter(decisions, items, prompt, threshold, scope, cancel).await.map_err(model)?;
            Ok(json!(filtered.kept))
        }
        "choose" => {
            let decisions = engine
                .deps
                .decisions
                .as_ref()
                .ok_or_else(|| FunctionError::runtime("no decision model configured (OPEN_ROUTER_API_KEY)"))?;
            let question = text(&args, name, "question")?
                .filter(|q| !q.trim().is_empty())
                .ok_or_else(|| invalid(name, "`question` must be a nonempty string"))?;
            let options = crate::flows::choose_options(&args).map_err(|e| invalid(name, e))?;
            let input = args.get("input").cloned().unwrap_or(Value::Null);
            if input.to_string().len() > crate::decisions::FILTER_MAX_BATCH_CHARS {
                return Err(invalid(
                    name,
                    format!(
                        "`input` is over {} characters as JSON; pass the fields the choice needs",
                        crate::decisions::FILTER_MAX_BATCH_CHARS
                    ),
                ));
            }
            let floor = match (args.get("min_confidence"), args.get("default")) {
                (None | Some(Value::Null), None | Some(Value::Null)) => None,
                (Some(min), Some(Value::String(default))) => {
                    let min = min
                        .as_f64()
                        .filter(|m| (0.0..=1.0).contains(m))
                        .ok_or_else(|| invalid(name, "`min_confidence` must be a number from 0 to 1"))?;
                    if !options.contains_key(default) {
                        return Err(invalid(name, format!("`default` `{default}` is not one of the options")));
                    }
                    Some((min, default.clone()))
                }
                _ => return Err(invalid(name, "`min_confidence` and `default` (an option) go together")),
            };
            let span = scope.span(
                SpanStart::new(format!("call:{label}"), SpanKind::Tool)
                    .input(json!({"question":question,"options":options,"input":input})),
            );
            let child: Scope = span.clone();
            let answer = crate::decisions::choose(decisions, question, options, &input, &child, cancel).await;
            let answer = match answer {
                Ok(answer) => answer,
                Err(error) => {
                    span.end(SpanEnd::error(error.to_string()));
                    return Err(model(error));
                }
            };
            let (choice, fallback) = match floor {
                Some((min, default)) if answer.confidence < min => (default, true),
                _ => (answer.choice, false),
            };
            let output = json!({"choice":choice,"confidence":answer.confidence,
                "probabilities":answer.probabilities,"fallback":fallback});
            span.update(ContextUpdate::metadata(metadata(
                json!({"choice":choice,"confidence":answer.confidence,"fallback":fallback}),
            )));
            span.end(SpanEnd::output(output.clone()));
            Ok(output)
        }
        _ => Err(FunctionError::not_executed(format!("unknown step {name}"))),
    }
}

// ── 9. the catalog authors see ──────────────────────────────────────────────

/// A function of the runtime as a workflow's author sees it (`workflow_functions`).
#[derive(Clone, Copy)]
pub struct HostFunction {
    pub name: &'static str,
    pub effect: Effect,
    pub takes: &'static [&'static str],
    pub needs: &'static [&'static str],
    pub description: &'static str,
    // Which runs may call it: "workflow" (any), "agent-loop", "signal",
    // "signal|agent-loop", "guard".
    pub context: &'static str,
}

const fn f(
    name: &'static str,
    effect: Effect,
    takes: &'static [&'static str],
    needs: &'static [&'static str],
    context: &'static str,
    description: &'static str,
) -> HostFunction {
    HostFunction { name, effect, takes, needs, description, context }
}

use Effect::{Read, Write};

pub const HOST_FUNCTIONS: &[HostFunction] = &[
    f(
        "agent_turn",
        Read,
        &["messages", "memory", "i", "extra_turn"],
        &["messages"],
        "agent-loop",
        "Generate the next model message with the run's host-vouched model parameters (a loop the runtime started). Pass messages, working-memory snapshot and zero-based turn i; extra_turn allows one additional turn. Returns message, calls and content. A workflow that calls it must start the guard child each round: copy agent-loop's guarded round.",
    ),
    f(
        "agent_settle",
        Read,
        &["calls", "results", "memory"],
        &["calls", "results"],
        "agent-loop",
        "Settle ordered tool results into the run's transcript and working memory. Returns replies, memory, finished, output and outcomes: {name, arguments, ok, result}. Use these facts in workflow exit rules.",
    ),
    f(
        "loop_setup",
        Read,
        &["system", "skills", "input"],
        &[],
        "agent-loop",
        "Set up the loop's instructions, once and before its first agent_turn. system is an array of strings added to the system prompt; skills names skills loaded in full, with their tools; input replaces the first user message. Returns messages and memory to export into $context, and setup (what a restart restores).",
    ),
    f(
        "classify",
        Read,
        &["call", "guard_run", "worker_run"],
        &["call"],
        "agent-loop",
        "Whether one of the model's calls runs at most once (tool_once: writes and confirmed open-world reads) or may be repeated (tool_repeat). Returns once.",
    ),
    f(
        "tool_once",
        Write,
        &["call", "guard_run", "i", "index", "memory"],
        &["call"],
        "agent-loop",
        "Execute one of the model's calls at most once under the host-verified guard verdict. Its unknown outcome raises urn:ouro:errors:call.unknown: catch it as agent-loop does. Returns presented, failed, result and working-memory updates for agent_settle.",
    ),
    f(
        "tool_repeat",
        Read,
        &["call", "guard_run", "worker_run", "i", "index", "memory"],
        &["call"],
        "agent-loop",
        "Execute one of the model's reads (or join a worker) under the host-verified guard verdict; refuses a call that must run at most once. Returns presented, failed, result and working-memory updates for agent_settle.",
    ),
    f(
        "start_guard",
        Read,
        &["calls", "i"],
        &["calls", "i"],
        "agent-loop",
        "Start the round's guard child for the model's calls. Returns run (null when the round needs no review), key and guarded (the indices to run under its verdicts); join it with listen on done:<run>.",
    ),
    f(
        "start_worker",
        Read,
        &["call", "i", "index", "memory"],
        &["call", "i", "index"],
        "agent-loop",
        "Start the model's invoke_sub_agent call as a worker child. Returns id, key, done and waiting (id null when it cannot start); join it with listen, then tool_repeat with worker_run.",
    ),
    f(
        "signal_session",
        Read,
        &[],
        &[],
        "signal",
        "Open the incoming signal's session. Returns context, engine_skills, conversation {chatId, messageThreadId} or null, default_chat and the recorded env.",
    ),
    f(
        "deliver_event",
        Write,
        &[],
        &[],
        "signal",
        "Deliver the signal's button press to the guard round waiting for it. Returns delivered and output, or delivered:false with reason and note.",
    ),
    f(
        "run_saved_workflow",
        Write,
        &["name"],
        &["name"],
        "signal",
        "Run the active saved workflow a scheduled signal names. Until the versions step none run: returns done:false with handover for the primary loop.",
    ),
    f(
        "signal_input",
        Read,
        &[],
        &[],
        "signal|agent-loop",
        "Prepare the incoming message with its source history. Returns input (the string for primary_loop or loop_setup), earlier and memory (evidence the host keeps).",
    ),
    f(
        "recent_activity",
        Read,
        &[],
        &[],
        "signal|agent-loop",
        "Read recent activity-journal notes as a context string, or null when unavailable.",
    ),
    f(
        "pending_confirmations",
        Read,
        &["target"],
        &["target"],
        "signal|agent-loop",
        "Read pending confirmation cards for target {chatId, messageThreadId}. Returns an informational string or null; it does not answer or consume a card.",
    ),
    f(
        "preselect_skills",
        Read,
        &["request", "context", "loaded"],
        &[],
        "signal|agent-loop",
        "Select relevant skills for this signal. Returns names and block, or null; pass block in primary_loop's or loop_setup's system array.",
    ),
    f(
        "primary_loop",
        Write,
        &["input", "loop", "confirm", "skills", "loaded", "preset", "allow_skill_writes", "system"],
        &["input"],
        "signal",
        "Start the signal's primary agent as a child after signal_session. Returns id, key, done and waiting: join it with listen on done, child_outcome and settle_child.",
    ),
    f(
        "route_signal",
        Write,
        &["context"],
        &["context"],
        "signal",
        "Dispatch the incoming signal to a matching route. Until the versions step there are none: returns routed:false.",
    ),
    f(
        "settle_child",
        Read,
        &["ref", "kind"],
        &["ref"],
        "signal",
        "What a child's end means: action done (with output), handover (with handover) or raise (with error).",
    ),
    f(
        "signal_transcript",
        Read,
        &["ref"],
        &[],
        "signal",
        "Read the bounded JSON transcript string of this run's primary child for recovery. Returns [] as a string when none was started or it took no turn.",
    ),
    f(
        "recovery_loop",
        Write,
        &["error", "prompt", "skills", "preset", "max_iterations", "system"],
        &["prompt", "max_iterations"],
        "signal",
        "Start one failure-reporting agent outside the signal's run tree after signal_session. Returns at once; keep the catch's final raise to preserve the original signal failure.",
    ),
    f("review", Read, &[], &[], "guard", "The guard round's review."),
    f("confirm_request", Write, &["call"], &["call"], "guard", "The guard round's confirmation card."),
    f("resolve_review", Read, &["call", "outcome"], &["call"], "guard", "The guard round's verdict for a call."),
    f("set_memory", Write, &["key", "value"], &["key", "value"], "workflow", "Store a value in the memory KV."),
    f(
        "llm_compose",
        Read,
        &["preset", "skill", "prompt", "input", "json"],
        &[],
        "workflow",
        "A step of the workflow runtime; the skill describes it.",
    ),
    f(
        "filter_relevant",
        Read,
        &["items", "prompt", "threshold"],
        &["items", "prompt"],
        "workflow",
        "A step of the workflow runtime; the skill describes it.",
    ),
    f(
        "choose",
        Read,
        &["question", "options", "input", "min_confidence", "default"],
        &["question", "options"],
        "workflow",
        "A step of the workflow runtime; the skill describes it.",
    ),
    f(
        "hash_value",
        Read,
        &["value"],
        &["value"],
        "workflow",
        "A step of the workflow runtime; the skill describes it.",
    ),
    f("skill_catalog_snapshot", Read, &[], &[], "workflow", "A step of the workflow runtime; the skill describes it."),
];

/// The engine's own workflow functions (`start_child { name, input }`,
/// `child_outcome { id, key }`), for the author's catalog.
pub const ENGINE_FUNCTIONS: &[HostFunction] = &[
    f(
        "start_child",
        Read,
        &["name", "input"],
        &["name"],
        "workflow",
        "The engine's own: start an owned child of a saved workflow (no host data: it cannot take agent turns). Returns id, key, done and waiting.",
    ),
    f("child_outcome", Read, &["id", "key"], &["id", "key"], "workflow", "The engine's own: an owned child's outcome."),
];

pub fn host_function(name: &str) -> Option<&'static HostFunction> {
    HOST_FUNCTIONS.iter().chain(ENGINE_FUNCTIONS).find(|f| f.name == name)
}
