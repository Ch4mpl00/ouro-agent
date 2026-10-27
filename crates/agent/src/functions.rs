// The functions the engine's runs call (docs/designs/03-runs-on-the-engine.md, A):
// 1. the registry and typed outcomes; 2. MCP tools from the snapshot;
// 3. loop functions; 4. context functions; 5. child starters and their
// settlement, 5b. loops a workflow starts; 6. guard functions; 7. dispatcher functions; 8. workflow steps;
// 8b. scheduled tasks; 9. the catalog authors see.
//
// A handler reads its run tree (its own run, its ancestors, its descendants
// and their records) through the call's run access (`FunctionContext::run`,
// `records`, `children`; runs.rs §11), never through the engine's own
// handle: in a dry run the call is driven by a private store (design 05,
// decision 3).
//
// Every handler holds the engine weakly. A host function serves only the runs
// its host data says it serves (`FunctionContext::host`): loop functions a
// loop run, guard functions a guard run, dispatcher functions the signal's
// run. YAML can neither read nor forge host data, and a run a workflow starts
// itself has none (invariant 3) unless it inherits its parent's unchanged
// (`start_child { host: inherit }`, design 09: a loop a workflow starts, §5b),
// so a refusal is the answer to any other caller, recorded as not executed.
use crate::{
    agent::{AGENT_LOOP_NAME, Effect, Engine, EngineDeps, LoopOptions, Refusal, refuse, tool_call},
    errors::is_aborted,
    providers,
    runs::{Choice, Fallback, GUARD, GuardHost, Named, PRIMARY, RECOVERY, RunReads, SIGNAL, SignalHost, Site},
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

/// The host functions sent at most once: a write the engine must never
/// repeat (the model's write, a confirmation card, the memory KV).
pub const AT_MOST_ONCE: &[&str] = &["tool_once", "confirm_request", "set_memory"];

/// The registry of the engine's runs: every MCP tool the snapshot ever held
/// (permissive schemas, at most once unless MCP marks it read-only; an absent
/// one answers `not_executed`), the host functions and the workflow steps.
pub fn registry(engine: Weak<Engine>, deps: &EngineDeps) -> Registry {
    let mut registry = Registry::new();
    for (name, read_only) in mcp_functions(deps) {
        let delivery = if read_only { Delivery::AtLeastOnce } else { Delivery::AtMostOnce };
        let tool = name.clone();
        add(&mut registry, &engine, &name, delivery, move |engine, args, ctx| {
            mcp_tool(engine, tool.clone(), args, ctx)
        });
    }
    let (once, repeat) = (Delivery::AtMostOnce, Delivery::AtLeastOnce);
    debug_assert!(AT_MOST_ONCE.iter().all(|f| ["tool_once", "confirm_request", "set_memory"].contains(f)));
    add(&mut registry, &engine, "agent_turn", repeat, agent_turn);
    add(&mut registry, &engine, "agent_settle", repeat, agent_settle);
    add(&mut registry, &engine, "loop_setup", repeat, loop_setup);
    add(&mut registry, &engine, "loop_begin", repeat, loop_begin);
    add(&mut registry, &engine, "classify", repeat, classify);
    add(&mut registry, &engine, "tool_once", once, |e, a, c| tool(e, a, c, true));
    add(&mut registry, &engine, "tool_repeat", repeat, |e, a, c| tool(e, a, c, false));
    for name in CONTEXT_FUNCTIONS {
        add(&mut registry, &engine, name, repeat, move |e, a, c| context(e, a, c, name));
    }
    add(&mut registry, &engine, "start_guard", repeat, start_guard);
    add(&mut registry, &engine, "start_worker", repeat, start_worker);
    add(&mut registry, &engine, "primary_loop", repeat, primary_loop);
    add(&mut registry, &engine, "loop_outcome", repeat, loop_outcome);
    add(&mut registry, &engine, "loop_transcript", repeat, loop_transcript);
    add(&mut registry, &engine, "observe_step", repeat, observe_step);
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
    add(&mut registry, &engine, "fire_scheduled", repeat, fire_scheduled);
    for name in ["llm_compose", "filter_relevant", "choose", "hash_value", "skill_catalog_snapshot"] {
        add(&mut registry, &engine, name, repeat, move |e, a, c| step(e, a, c, name));
    }
    registry
}

/// The MCP tools the registry registers, each with whether MCP marks it
/// read-only: every tool the snapshot ever held and MCP's current ones, but
/// those no workflow may call. A dry run mocks the ones not read-only
/// (sandbox.rs, design 05 decision 6).
pub fn mcp_functions(deps: &EngineDeps) -> std::collections::BTreeMap<String, bool> {
    let mut tools: std::collections::BTreeMap<String, bool> =
        deps.db.tool_snapshot().unwrap_or_default().into_iter().collect();
    for tool in deps.mcp.tools() {
        let read_only = deps.mcp.read_only(&tool.name);
        tools.insert(tool.name, read_only);
    }
    tools.retain(|name, _| {
        !(WITHHELD.contains(&name.as_str())
            || crate::mcp::SUPERVISOR_ONLY_TOOLS.contains(&name.as_str())
            || HOST_FUNCTIONS.iter().any(|f| f.name == name)
            || matches!(name.as_str(), "start_child" | "child_outcome"))
    });
    tools
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
    Ok(engine.loop_of(ctx, &ctx.run_id, ctx.host(), &ctx.function).await?.0)
}

async fn agent_turn(engine: Arc<Engine>, args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    let live = live_loop(&engine, &ctx).await.map_err(failed)?;
    crate::agent::loop_turn(&engine, &live, &args).await.map_err(failed)
}

async fn agent_settle(engine: Arc<Engine>, args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    let live = live_loop(&engine, &ctx).await.map_err(failed)?;
    crate::agent::loop_settle(&engine, &live, &ctx.run_id, &args).await.map_err(failed)
}

async fn loop_setup(engine: Arc<Engine>, args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    let setup = async {
        let live = live_loop(&engine, &ctx).await?;
        // A loop a workflow started begins with its preparation (design 09).
        let admitted = match engine.deps.db.prepared_loop(&ctx.run_id)? {
            Some(prepared) => prepared.input,
            None => {
                RunReads::run(&ctx, &ctx.run_id).await?.ok_or_else(|| anyhow::anyhow!("unknown run"))?.admission.input
            }
        };
        let records = RunReads::records(&ctx, &ctx.run_id).await?;
        crate::agent::loop_setup(&engine, &live, &ctx.run_id, &admitted, &args, &records).await
    };
    setup.await.map_err(failed)
}

/// `loop_begin`: the first transcript and working memory of a loop a
/// workflow started with its request (`start_child { host: inherit }`,
/// design 09), as its preparation stored them: `{messages, memory}`, and
/// the conversation, confirmation target and observer its steps read, what
/// the host admits a loop it starts itself with. The loop exports them into
/// `$context` before its first `agent_turn`.
async fn loop_begin(engine: Arc<Engine>, _args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    let begin = async {
        live_loop(&engine, &ctx).await?;
        engine.deps.db.prepared_loop(&ctx.run_id)?.map(|prepared| prepared.input).ok_or_else(|| {
            anyhow::anyhow!(
                "loop_begin: this loop was started with its transcript, not a request: skip the step with `if: '${{ $context.messages == null }}'`"
            )
        })
    };
    begin.await.map_err(failed)
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
        let at = crate::agent::CallSite {
            runs: &ctx,
            run: &ctx.run_id,
            task: &ctx.task_path,
            invocation: &ctx.idempotency_key,
        };
        crate::agent::loop_tool(&engine, &live, &args, &at, once).await
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
                let (host, signal) = signal_of(&ctx, name).await?;
                let records = RunReads::records(&ctx, &ctx.run_id).await?;
                crate::agent::ContextHost {
                    run: ctx.run_id.clone(),
                    session: signal_session_of(&engine, &host, &records)?,
                    scope: engine.scope_for(&ctx.run_id, &ctx.definition),
                    signal: Some(signal),
                    loaded: Vec::new(),
                }
            }
            _ => {
                let (live, host) = engine.loop_of(&ctx, &ctx.run_id, ctx.host(), name).await?;
                // The signal run that started this loop: an ancestor.
                let signal = match host.signal_id {
                    Some(id) => RunReads::run(&ctx, &signal_run(id))
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
// insert-or-ignore. A name the agent may have overridden starts through
// `runs::start_named_child` (design 04): a set-aside version gives way to
// the built-in `agent-loop` at a loop site, and steps the route or the
// saved workflow aside for the default path.

// The child's trace: a span under the starter's scope, which the child's
// functions trace into while it lives in this process. `outer`: a span the
// starter opened around it, which ends with it.
async fn anchor_child(
    engine: &Engine,
    child: &str,
    span: Arc<dyn Span>,
    outer: Option<Arc<dyn Span>>,
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
        match outer {
            Some(outer) => engine.scopes.own_wrapped(child, span, outer),
            None => engine.scopes.own(child, span),
        }
    } else {
        span.end(SpanEnd::default());
        if let Some(outer) = outer {
            outer.end(SpanEnd::default());
        }
    }
    Ok(())
}

// A start that failed after its anchor was written, with nothing stored
// under the child's id: its span ends with the error and its anchor goes,
// so a later start of the id anchors afresh.
async fn unanchor(engine: &Engine, ctx: &FunctionContext, child: &str, error: &str) {
    if RunReads::run(ctx, child).await.ok().flatten().is_some() {
        return;
    }
    engine.scopes.stop(child, Some(SpanEnd::error(error)));
    let run = child.to_owned();
    if let Err(e) = engine.deps.db.write(move |db| db.forget_run_trace(&run)).await {
        eprintln!("[engine] forgetting the anchor of {child}: {e:#}");
    }
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
        // Always the built-in round, whatever version of `guard` is active:
        // its stored outcome is what authorizes the round's calls.
        let version = crate::flows::builtin_version(&engine.runtime, crate::flows::GUARD_NAME)
            .await?
            .ok_or_else(|| anyhow::anyhow!("the built-in guard is not installed"))?;
        let span = live.scope().span(SpanStart::new("guard", SpanKind::Chain).input(input.clone()));
        anchor_child(&engine, &id, span, None, live.opts.session_id.clone()).await?;
        let child = ctx
            .start_child(ChildStart {
                key: "guard".into(),
                name: crate::flows::GUARD_NAME.into(),
                version: Some(version),
                input,
                host: serde_json::to_value(&host)?,
            })
            .await
            .map_err(anyhow::Error::from)?;
        // The calls the joined child reviews: its own stored input.
        let joined = RunReads::run(&ctx, &child.id).await?.map(|r| r.admission.input["calls"].clone());
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

// A loop's options with the line of the version set aside for it, if any,
// as the last part of its instructions, running the definition chosen.
fn chosen(opts: &LoopOptions, choice: &Choice) -> LoopOptions {
    let mut opts = opts.clone();
    opts.workflow = choice.name.clone();
    if let Some(fallback) = &choice.fallback {
        let parts = [opts.system_prompt.take().unwrap_or_default(), fallback.line.clone()];
        opts.system_prompt = Some(parts.into_iter().filter(|p| !p.is_empty()).collect::<Vec<_>>().join("\n\n"));
    }
    opts
}

/// What a named start builds a loop's admission with (`runs::start_named_*`).
type Build = futures::future::BoxFuture<'static, anyhow::Result<(Value, Value)>>;

// The builder of a loop's admission: `opts` running the definition chosen,
// the first user message `prompt`, its working memory.
fn loop_builder(
    engine: &Arc<Engine>,
    opts: &LoopOptions,
    kind: &'static str,
    id: &str,
    prompt: &str,
    memory: Value,
) -> impl Fn(Choice) -> Build + use<> {
    let (engine, opts, id, prompt) = (engine.clone(), opts.clone(), id.to_owned(), prompt.to_owned());
    move |choice: Choice| {
        let (engine, opts, id, prompt, memory) =
            (engine.clone(), chosen(&opts, &choice), id.clone(), prompt.clone(), memory.clone());
        Box::pin(async move {
            let (input, host) =
                loop_admission(&engine, &opts, kind, &id, &prompt, memory, choice.fallback.as_ref()).await?;
            Ok((input, serde_json::to_value(&host)?))
        })
    }
}

// A loop's input and host data for the definition chosen (`chosen`): its
// instructions, the first user message `prompt`, its working memory.
async fn loop_admission(
    engine: &Engine,
    opts: &LoopOptions,
    kind: &str,
    id: &str,
    prompt: &str,
    memory: Value,
    fallback: Option<&Fallback>,
) -> anyhow::Result<(Value, crate::runs::LoopHost)> {
    let prepared = crate::agent::prepare(engine, opts).await?;
    let messages = json!([providers::system(&prepared.system), providers::user(prompt)]);
    let input = crate::agent::loop_input(messages, memory, opts);
    let mut host =
        crate::agent::loop_host(engine, opts, kind, id, &prepared.skill_text(&opts.loaded_skill_text)).await?;
    host.fallback = fallback.cloned();
    Ok((input, host))
}

/// `start_worker { call, i, index, memory, loop? }`: the model's
/// `invoke_sub_agent` call as a host child, a loop with the worker's host
/// data: it inherits the confirmation target (invariant 5) and the guard's
/// evidence. The loop it runs (design 04, R-M9): the YAML's `loop`, else the
/// call's, else its skill's preference, else agent-loop. `{id: null}` when
/// it cannot start; `tool_repeat` then presents the error to the model.
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
        if let Some(name) = args["loop"].as_str().filter(|name| !name.is_empty()) {
            opts.workflow = name.to_owned();
        }
        // Its skills must exist whichever loop runs it.
        if let Err(error) = crate::agent::prepare(&engine, &opts).await {
            span.end(SpanEnd::error(error.to_string()));
            return Ok(json!({"id": null}));
        }
        anchor_child(&engine, &id, span.clone(), None, live.opts.session_id.clone()).await?;
        let memory = engine.memory_out(&ctx.run_id, &live.opts.session.memory).await?;
        let build = loop_builder(&engine, &opts, crate::runs::WORKER, &id, &prompt, memory);
        let started = crate::runs::start_named_child(&engine, &ctx, &key, &opts.workflow, Site::Loop, build).await;
        if let Err(error) = &started {
            unanchor(&engine, &ctx, &id, &error.to_string()).await;
        }
        match started? {
            Named::Started(child, ran) => {
                if let Some(fallback) = &ran.fallback {
                    crate::runs::trace_fallback(span.clone(), fallback, true);
                }
                Ok(serde_json::to_value(child)?)
            }
            Named::SetAside(_) => Ok(json!({"id": null})),
        }
    };
    start.await.map_err(failed)
}

/// `primary_loop { input, confirm, skills, loaded, preset, allow_skill_writes,
/// system, loop? }`: the signal's primary agent, a host child. The `signal`
/// definition is the trusted source of these arguments (invariant 3
/// restated); what the signal's context functions captured (earlier user
/// messages, the working memory, the preselected block) comes from their
/// records in this run's journal (decision 4). `loop` names the loop it runs
/// (agent-loop by default); one set aside gives way to the built-in.
async fn primary_loop(engine: Arc<Engine>, args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    let start = async {
        let (host, signal) = signal_of(&ctx, "primary_loop").await?;
        let input = args["input"].as_str().ok_or_else(|| anyhow::anyhow!("input: Required"))?;
        let records = RunReads::records(&ctx, &ctx.run_id).await?;
        let session = signal_session_of(&engine, &host, &records)?;
        let recorded_input = crate::agent::recorded_output(&records, "signal_input");
        if let Some(memory) = recorded_input.and_then(|o| o["memory"].as_str()) {
            let snapshot = engine.evidence_of(Some(&memory.to_owned()))?.unwrap_or_default();
            engine.memory_in(&session.memory, &serde_json::from_str(&snapshot)?)?;
        }
        let earlier: Vec<String> = match recorded_input.and_then(|o| o["earlier"].as_str()) {
            Some(digest) => serde_json::from_str(&engine.evidence_of(Some(&digest.to_owned()))?.unwrap_or_default())?,
            None => Vec::new(),
        };
        let preset = args["preset"].as_str().unwrap_or("smart");
        let preset = crate::models::PresetName::selectable(preset)
            .ok_or_else(|| anyhow::anyhow!("preset: unknown preset {preset}"))?;
        let system = strings(&args, "system");
        let key = "primary";
        let id = ctx.child_id(key);
        let mut opts = LoopOptions::new(&id, session.clone());
        opts.workflow = args["loop"].as_str().filter(|l| !l.is_empty()).unwrap_or(AGENT_LOOP_NAME).to_owned();
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
        narrow(&engine, &args, &mut opts)?;
        opts.guard_request = engine.evidence_of(Some(&host.evidence.content))?;
        opts.conversation = crate::supervisor::conversation(&signal);
        opts.signal_id = Some(host.signal_id);
        opts.session_id = Some(session.id.clone());
        let scope = engine.scope_for(&ctx.run_id, &ctx.definition);
        let span = scope
            .span(SpanStart::new("agent_loop", SpanKind::Chain).metadata(metadata(json!({"activity":"primary_loop"}))));
        anchor_child(&engine, &id, span.clone(), None, Some(session.id.clone())).await?;
        let memory = engine.memory_out(&ctx.run_id, &session.memory).await?;
        let build = loop_builder(&engine, &opts, PRIMARY, &id, input, memory);
        let started = crate::runs::start_named_child(&engine, &ctx, key, &opts.workflow, Site::Loop, build).await;
        if let Err(error) = &started {
            unanchor(&engine, &ctx, &id, &error.to_string()).await;
        }
        match started? {
            Named::Started(child, ran) => {
                if let Some(fallback) = &ran.fallback {
                    crate::runs::trace_fallback(span.clone(), fallback, true);
                }
                Ok(serde_json::to_value(child)?)
            }
            Named::SetAside(fallback) => Err(anyhow::anyhow!("{}", fallback.line)),
        }
    };
    start.await.map_err(failed)
}

// What a loop's starter may narrow (design 08): tools taken away
// (`exclude_tools`, added to what the caller already lacks) and an observer
// of its acting turns (`on_step: {workflow, input}`). A name that is no tool
// fails the start: a typo must not leave the tool in. Known tools are every
// one MCP ever offered (the snapshot), MCP's current ones and the synthetic
// ones, so a reconnect cannot fail a signal.
fn narrow(engine: &Engine, args: &Value, opts: &mut LoopOptions) -> anyhow::Result<()> {
    let excluded = match &args["exclude_tools"] {
        Value::Null => Vec::new(),
        Value::Array(names) => names
            .iter()
            .map(|n| n.as_str().filter(|n| !n.is_empty()).map(str::to_owned))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| anyhow::anyhow!("exclude_tools: Expected tool names"))?,
        _ => anyhow::bail!("exclude_tools: Expected an array of tool names"),
    };
    if !excluded.is_empty() {
        let mut known: std::collections::BTreeSet<String> =
            engine.deps.db.tool_snapshot().unwrap_or_default().into_iter().map(|(name, _)| name).collect();
        known.extend(engine.deps.mcp.tools().into_iter().map(|t| t.name));
        known.extend(crate::agent::synthetic_definitions().iter().map(|t| t.name.clone()));
        if let Some(unknown) = excluded.iter().find(|name| !known.contains(*name)) {
            anyhow::bail!("exclude_tools: `{unknown}` is no tool");
        }
    }
    opts.exclude_tools.extend(excluded);
    match &args["on_step"] {
        Value::Null => {}
        on_step => {
            let name = on_step["workflow"]
                .as_str()
                .filter(|n| !n.is_empty())
                .ok_or_else(|| anyhow::anyhow!("on_step.workflow: Expected a workflow name"))?;
            crate::flows::refuse_reserved(name)?;
            anyhow::ensure!(
                matches!(on_step.get("input"), None | Some(Value::Null) | Some(Value::Object(_))),
                "on_step.input: Expected an object"
            );
            opts.on_step = Some(json!({"workflow": name, "input": on_step.get("input").cloned().unwrap_or(json!({}))}));
        }
    }
    Ok(())
}

// ── 5b. loops a workflow starts ─────────────────────────────────────────────
// Design 09: a workflow starts a loop with the engine's `start_child { name,
// host: inherit, input: { request } }` and joins it with `listen`. Its host
// data is its parent's, byte-equal: the tree it belongs to (the signal, the
// conversation, the confirmation target, the session). Its parameters are
// its request, ordinary input: an activated workflow is trusted code, so the
// request is checked for mistakes (a preset that cannot be selected, a skill
// that does not exist, a name that is no tool, a field that is no
// parameter), never for authority. A bad request faults the child; the
// caller reads why with `loop_outcome`. The first host function that serves
// such a run (any of them, through `Engine::loop_for`) prepares it and
// stores the preparation insert-once (`prepared_loops`): every later call,
// recovery and guard reads the first one stored, and nothing reads the
// request or the skill files again. A refused start (the loop's version
// paused or tripped) is the caller's to catch: nothing runs in its place.

/// A loop's parameters as a workflow asks for them (`input.request`).
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct LoopRequest {
    // The first user message.
    input: String,
    #[serde(default)]
    skills: Vec<String>,
    // The skills it runs, as its starter says (the signal's trace shows
    // them for a wrapper that took no turn); its `skills` when absent.
    #[serde(default)]
    loaded: Option<Vec<String>>,
    #[serde(default)]
    preset: Option<String>,
    // Its instructions, after the parent's own.
    #[serde(default)]
    system: Vec<String>,
    #[serde(default)]
    exclude_tools: Value,
    #[serde(default)]
    on_step: Value,
    #[serde(default)]
    allow_skill_writes: bool,
}

/// The preparation of run `run` if a workflow started it as a loop (its
/// input has `request` and its host data is its parent's): the stored one,
/// or the one prepared now and stored (the first stored wins); `None` for a
/// run the host started. `host` is the run's host data, its parent's.
pub(crate) async fn inherited_loop(
    engine: &Arc<Engine>,
    runs: &dyn RunReads,
    run: &str,
    host: &crate::runs::LoopHost,
) -> anyhow::Result<Option<crate::runs::PreparedLoop>> {
    if let Some(stored) = engine.deps.db.prepared_loop(run)? {
        return Ok(Some(stored));
    }
    let Some(saved) = runs.run(run).await? else { return Ok(None) };
    let Some(request) = saved.admission.input.get("request").filter(|r| !r.is_null()) else { return Ok(None) };
    let Some(link) = &saved.child else { return Ok(None) };
    let Some(parent) = runs.run(&link.parent).await? else { return Ok(None) };
    // A host child's host data is the host's own, not its parent's.
    if parent.admission.host() != saved.admission.host() {
        return Ok(None);
    }
    // The parent's parameters: its own preparation when a workflow started
    // it too, so a narrowing holds down the tree.
    let parent_host = match Box::pin(inherited_loop(engine, runs, &link.parent, host)).await? {
        Some(prepared) => prepared.host,
        None => host.clone(),
    };
    let prepared = prepare_inherited(engine, runs, &saved, &parent, &parent_host, request).await?;
    let id = run.to_owned();
    Ok(Some(engine.deps.db.write(move |db| db.insert_prepared_loop(&id, &prepared)).await?))
}

// A loop a workflow started, prepared from its parent's parameters
// (`parent_host`), the context its parent's run captured (`signal_input`'s
// earlier messages and working memory, `preselect_skills`' block) and its
// request, as the host would have admitted it had it started the loop.
async fn prepare_inherited(
    engine: &Arc<Engine>,
    runs: &dyn RunReads,
    saved: &ouro_runtime::durable::SavedRun,
    parent: &ouro_runtime::durable::SavedRun,
    parent_host: &crate::runs::LoopHost,
    request: &Value,
) -> anyhow::Result<crate::runs::PreparedLoop> {
    let id = saved.admission.id.clone();
    // Its trace: a span under its parent's current one, opened once (a
    // re-dispatch finds the anchor), before anything can fault it. Tagged
    // `loop`, not `primary_loop`: the journal's answer is the parent's.
    if engine.deps.db.run_trace(&id)?.is_none() {
        let preset = request["preset"].as_str().unwrap_or(&parent_host.preset);
        let span = engine.scope_for(&parent.admission.id, &parent.admission.name).span(
            SpanStart::new("agent_loop", SpanKind::Chain)
                .metadata(metadata(json!({"activity": "loop", "workflow": saved.admission.name, "preset": preset}))),
        );
        anchor_child(engine, &id, span, None, parent_host.session_id.clone()).await?;
    }
    let request: LoopRequest = serde_json::from_value(request.clone()).map_err(|e| anyhow::anyhow!("request: {e}"))?;
    let env = crate::session::EnvData::from_json(&parent_host.env)?;
    let session = engine.session(&parent_host.session, env);
    let records = runs.records(&parent.admission.id).await?;
    let recorded_input = crate::agent::recorded_output(&records, "signal_input");
    if let Some(memory) = recorded_input.and_then(|o| o["memory"].as_str()) {
        let snapshot = engine.evidence_of(Some(&memory.to_owned()))?.unwrap_or_default();
        engine.memory_in(&session.memory, &serde_json::from_str(&snapshot)?)?;
    }
    let mut opts = LoopOptions::new(&id, session.clone());
    opts.workflow = saved.admission.name.clone();
    opts.conversation = parent_host.conversation.clone();
    opts.confirm = parent_host.confirm.clone();
    opts.signal_id = parent_host.signal_id;
    opts.session_id = parent_host.session_id.clone();
    opts.parent_id = parent_host.parent_id.clone();
    opts.root_request = engine.evidence_of(parent_host.root_request_digest.as_ref())?;
    opts.guard_request = engine.evidence_of(parent_host.guard_request.as_ref())?;
    opts.tool_whitelist = parent_host.tool_whitelist.as_ref().map(|w| w.iter().cloned().collect());
    // The parent's exclusions, then the request's (`narrow`).
    opts.exclude_tools = parent_host.exclude_tools.iter().cloned().collect();
    opts.include_engine_skills = parent_host.include_engine_skills;
    opts.max_iterations = parent_host.max_iterations;
    opts.max_concurrent_tools = parent_host.max_concurrent_tools;
    opts.allow_skill_writes = request.allow_skill_writes;
    let earlier = match recorded_input.and_then(|o| o["earlier"].as_str()) {
        Some(digest) => engine.evidence_of(Some(&digest.to_owned()))?,
        None => engine.evidence_of(parent_host.evidence.earlier.as_ref())?,
    };
    opts.earlier_user_messages = earlier.map(|e| serde_json::from_str(&e)).transpose()?.unwrap_or_default();
    opts.preset = match &request.preset {
        None => crate::agent::preset_named(&parent_host.preset)?,
        Some(preset) => crate::models::PresetName::selectable(preset)
            .ok_or_else(|| anyhow::anyhow!("preset: unknown preset {preset}"))?,
    };
    if let Some(block) = crate::agent::recorded_output(&records, "preselect_skills").and_then(|o| o["block"].as_str())
        && request.system.iter().any(|part| part.contains(block))
    {
        opts.loaded_skill_text = block.to_owned();
    }
    let inherited = engine.evidence_of(parent_host.evidence.system.as_ref())?;
    let parts = inherited.into_iter().chain(request.system).filter(|p| !p.is_empty()).collect::<Vec<_>>();
    opts.system_prompt = Some(parts.join("\n\n"));
    narrow(engine, &json!({"exclude_tools": request.exclude_tools, "on_step": request.on_step}), &mut opts)?;
    opts.skills = request.skills;
    let skills = crate::agent::prepare(engine, &opts).await?;
    let messages = json!([providers::system(&skills.system), providers::user(&request.input)]);
    let memory = engine.memory_out(&id, &session.memory).await?;
    let input = crate::agent::loop_input(messages, memory, &opts);
    let skill_text = skills.skill_text(&opts.loaded_skill_text);
    let host = crate::agent::loop_host(engine, &opts, PRIMARY, &id, &skill_text).await?;
    let loaded = request.loaded.unwrap_or_else(|| opts.skills.clone());
    Ok(crate::runs::PreparedLoop { host, input, skills, loaded })
}

// An owned child of the calling run, or a refusal.
async fn own_child(
    ctx: &FunctionContext,
    args: &Value,
    function: &str,
) -> anyhow::Result<ouro_runtime::durable::SavedRun> {
    let id = args["ref"]["id"].as_str().ok_or_else(|| refuse(format!("{function}: ref.id required")))?;
    RunReads::run(ctx, id)
        .await?
        .filter(|c| c.child.as_ref().is_some_and(|l| l.parent == ctx.run_id))
        .ok_or_else(|| refuse(format!("{function}: {id} is not a child of this run")))
}

/// `loop_outcome { ref }`: how a loop this run started (the ref
/// `start_child` returned) ended: `{status: completed, output}`, `{status:
/// faulted, error}` (a bad request too, design 09), `{status: cancelled}`,
/// or `{status: running}` before it ended.
async fn loop_outcome(engine: Arc<Engine>, args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    let read = async {
        crate::agent::loop_host_of(ctx.host(), "loop_outcome")?;
        let child = own_child(&ctx, &args, "loop_outcome").await?;
        use ouro_runtime::ows::run::Status;
        if let Some(outcome) = &child.outcome {
            engine.ended_child(&child.admission.id, outcome).await;
        }
        Ok(match &child.outcome {
            Some(Status::Completed(output)) => {
                let text = output.as_str().map(str::to_owned).unwrap_or_else(|| output.to_string());
                json!({"status": "completed", "output": text})
            }
            Some(Status::Faulted(error)) => json!({"status": "faulted", "error": error}),
            Some(Status::Cancelled) => json!({"status": "cancelled"}),
            _ => json!({"status": "running"}),
        })
    };
    read.await.map_err(failed)
}

/// `loop_transcript { ref }`: what a loop this run started (the ref
/// `start_child` returned) did, without its instructions
/// (`agent::compact_transcript`); "" before its first turn.
async fn loop_transcript(_engine: Arc<Engine>, args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    let read = async {
        crate::agent::loop_host_of(ctx.host(), "loop_transcript")?;
        let child = own_child(&ctx, &args, "loop_transcript").await?;
        let records = RunReads::records(&ctx, &child.admission.id).await.unwrap_or_default();
        Ok(json!(crate::agent::compact_transcript(crate::agent::history_messages(&records), TRANSCRIPT_CHARS)))
    };
    read.await.map_err(failed)
}

// What a reader of a loop's work gets at most.
const TRANSCRIPT_CHARS: usize = 12_000;

/// `observe_step { i, calls, content, reasoning }`: the loop's observer
/// (its host data's `on_step`, design 08) started as an owned child that is
/// never joined, told this turn's number (`seq`) and what it decided (calls
/// with their arguments clipped, the text beside them). It has no host data:
/// it runs only its own trusted steps. A refusal of any kind (no such
/// workflow, a tripped or paused one) answers `{id: null}`: a broken
/// observer never stops the loop. Traced as `observe:<i>`, which the
/// journal leaves out.
async fn observe_step(engine: Arc<Engine>, args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    let observe = async {
        let live = live_loop(&engine, &ctx).await?;
        let Some(on_step) = live.opts.on_step.clone() else { return Ok(json!({"id": null})) };
        let name = on_step["workflow"].as_str().unwrap_or_default().to_owned();
        let i = args["i"].as_u64().ok_or_else(|| refuse("observe_step: a turn `i` is required"))?;
        let clip = |value: &Value, max: usize| value.as_str().map(|text| crate::activity::clip(text, max));
        let calls: Vec<Value> = args["calls"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|c| json!({"name": c["name"], "arguments": clip(&c["arguments"], 200)}))
            .collect();
        let step =
            json!({"calls": calls, "content": clip(&args["content"], 400), "reasoning": clip(&args["reasoning"], 400)});
        let mut input = on_step["input"].as_object().cloned().unwrap_or_default();
        input.insert("seq".into(), json!(i));
        input.insert("step".into(), step);
        let input = Value::Object(input);
        let key = format!("observe:{i}");
        let id = ctx.child_id(&key);
        let span = live.scope().span(
            SpanStart::new(key.clone(), SpanKind::Chain)
                .input(input.clone())
                .metadata(metadata(json!({"activity": "observer", "workflow": name}))),
        );
        anchor_child(&engine, &id, span, None, live.opts.session_id.clone()).await?;
        let build = move |_: Choice| -> Build {
            let input = input.clone();
            Box::pin(async move { Ok((input, Value::Null)) })
        };
        let started = crate::runs::start_named_child(&engine, &ctx, &key, &name, Site::Observer, build).await;
        match started {
            Ok(Named::Started(child, _)) => Ok(json!({"id": child.id})),
            Ok(Named::SetAside(fallback)) => {
                unanchor(&engine, &ctx, &id, &fallback.line).await;
                Ok(json!({"id": null, "skipped": fallback.line}))
            }
            Err(error)
                if is_aborted(&error)
                    || matches!(
                        error.downcast_ref::<ouro_runtime::ChildError>(),
                        Some(ouro_runtime::ChildError::Interrupted(_))
                    ) =>
            {
                Err(error)
            }
            Err(error) => {
                unanchor(&engine, &ctx, &id, &error.to_string()).await;
                eprintln!("[{}] the observer `{name}` did not start: {error:#}", ctx.run_id);
                Ok(json!({"id": null, "skipped": error.to_string()}))
            }
        }
    };
    observe.await.map_err(failed)
}

/// `recovery_loop { error, prompt, skills, preset, max_iterations, system }`:
/// the failure report as an independent root `recovery:<signal id>` (design
/// 03, R2-H2), so an exhausted tree budget never blocks it; returns at once
/// and the signal raises its failure as before. Nothing before
/// `signal_session` gets a report. It runs `agent-loop`: an override of the
/// agent's, or the built-in while that is set aside (design 04).
async fn recovery_loop(engine: Arc<Engine>, args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    let start = async {
        let (host, _) = signal_of(&ctx, "recovery_loop").await?;
        let records = RunReads::records(&ctx, &ctx.run_id).await?;
        if crate::agent::recorded_output(&records, "signal_session").is_none() {
            return Ok(Value::Null);
        }
        let session = signal_session_of(&engine, &host, &records)?;
        let id = format!("{RECOVERY}:{}", host.signal_id);
        let scope = engine.scope_for(&ctx.run_id, &ctx.definition);
        let span =
            scope.span(SpanStart::new("recovery", SpanKind::Agent).metadata(metadata(json!({"skill":"recovery"}))));
        // A report that cannot be set up ends its span with the reason.
        let setup = async {
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
            // Fails before anything starts when its skills are missing.
            crate::agent::prepare(&engine, &opts).await?;
            Ok::<_, anyhow::Error>((opts, prompt))
        };
        let (opts, prompt) = match setup.await {
            Ok(setup) => setup,
            Err(error) => {
                span.end(SpanEnd::error(error.to_string()));
                return Err(error);
            }
        };
        anchor_child(&engine, &id, span.clone(), None, Some(session.id.clone())).await?;
        let build = loop_builder(&engine, &opts, RECOVERY, &id, prompt, json!([]));
        // A dry run records the start it would make; the sandbox runs it as
        // a second dry run once the signal's has ended (design 05, decision 13).
        if let Some(sandbox) = &engine.sandbox {
            let choice = crate::runs::choose(&engine, AGENT_LOOP_NAME, Site::Loop).await?;
            let (input, host) = build(choice.clone()).await?;
            if let Some(fallback) = &choice.fallback {
                crate::runs::trace_fallback(span.clone(), fallback, true);
            }
            sandbox.recovery(crate::sandbox::Start { id: id.clone(), choice, input, host });
            return Ok(json!({"started": id}));
        }
        let started = crate::runs::start_named_root(&engine, &id, AGENT_LOOP_NAME, Site::Loop, build).await;
        if let Err(error) = &started {
            unanchor(&engine, &ctx, &id, &error.to_string()).await;
        }
        let (_, ran) = started?;
        if let Some(fallback) = &ran.fallback {
            crate::runs::trace_fallback(span.clone(), fallback, true);
        }
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

/// How the runtime starts a workflow on a signal it was not asked to pick:
/// a route the dispatcher matched, or the saved workflow a scheduled task
/// names (design 04, R-H3 and R2-M5). Either runs as the signal's primary
/// run, admitted like `main`'s `admission(.., Launch)` with host data read
/// from the activated version's own metadata (vetted by the gate).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Launch {
    Route,
    Saved,
}

pub const ROUTE: &str = "route";
pub const SAVED: &str = "saved";

impl Launch {
    fn key(self) -> &'static str {
        match self {
            Launch::Route => ROUTE,
            Launch::Saved => SAVED,
        }
    }
}

// The signal run's own fallback line, when it runs the built-in signal in
// place of a set-aside override (runs.rs §10): every primary run hears it.
async fn signal_fallback_line(ctx: &FunctionContext) -> anyhow::Result<Option<String>> {
    let saved = RunReads::run(ctx, &ctx.run_id).await?.ok_or_else(|| anyhow::anyhow!("unknown run"))?;
    Ok(saved.admission.input["signal"]["fallback"].as_str().map(str::to_owned))
}

// One start of `name` as the signal's primary run, at the route or saved
// site. A version set aside starts nothing: its anchor, written before the
// refused start, is taken back.
async fn launch(
    engine: &Arc<Engine>,
    ctx: &FunctionContext,
    launch: Launch,
    name: &str,
    context: &Value,
) -> anyhow::Result<Named<ouro_runtime::ChildRef>> {
    let function = if launch == Launch::Route { "route_signal" } else { "run_saved_workflow" };
    let (host, signal) = signal_of(ctx, function).await?;
    let records = RunReads::records(ctx, &ctx.run_id).await?;
    let session = signal_session_of(engine, &host, &records)?;
    let key = launch.key();
    let id = ctx.child_id(key);
    let scope = engine.scope_for(&ctx.run_id, &ctx.definition);
    let fallback_line = signal_fallback_line(ctx).await?;
    let content = engine.evidence_of(Some(&host.evidence.content))?;
    let source_skill = match launch {
        Launch::Saved => engine.deps.skill_store.read_skill(&signal.source).await?.is_some(),
        Launch::Route => false,
    };
    // Whether the builder anchored the child (a refused start takes it back).
    let anchored: Arc<std::sync::atomic::AtomicBool> = Default::default();
    let handover = (launch == Launch::Route).then(|| context["handover"].as_str().map(str::to_owned)).flatten();
    // A press that found no waiting card reaches the route: its loop hears
    // the press changed nothing (design 08, review G-M7).
    let note = (launch == Launch::Route).then(|| context["button"]["note"].as_str().map(str::to_owned)).flatten();
    let build = {
        let (engine, anchored, scope, id, parent) =
            (engine.clone(), anchored.clone(), scope.clone(), id.clone(), ctx.run_id.clone());
        move |choice: Choice| -> Build {
            let (engine, anchored, scope, id, parent) =
                (engine.clone(), anchored.clone(), scope.clone(), id.clone(), parent.clone());
            let (session, signal, host) = (session.clone(), signal.clone(), host.clone());
            let (content, handover, fallback_line) = (content.clone(), handover.clone(), fallback_line.clone());
            let note = note.clone();
            Box::pin(async move {
                let wf = ouro_runtime::ows::model::parse_yaml(&choice.yaml)?;
                let declared = crate::flows::agent_start(&wf)?;
                let mut opts = LoopOptions::new(&id, session.clone());
                opts.workflow = choice.name.clone();
                opts.preset = declared.preset.unwrap_or(match launch {
                    Launch::Route => crate::models::PresetName::Smart,
                    Launch::Saved => crate::models::PresetName::Base,
                });
                opts.conversation = crate::supervisor::conversation(&signal);
                opts.confirm = match declared.confirm.unwrap_or(crate::flows::Confirm::Conversation) {
                    crate::flows::Confirm::Conversation => crate::supervisor::conversation(&signal).or_else(|| {
                        let chat_id = session.env.telegram_chat_id.clone()?;
                        Some(crate::agent::ConfirmTarget { chat_id, message_thread_id: None })
                    }),
                    crate::flows::Confirm::None => None,
                };
                if source_skill {
                    opts.skills = vec![signal.source.clone()];
                }
                opts.guard_request = content;
                let parts = [
                    Some(crate::session::render_context(&session.env)?),
                    Some(format!("Signal source: {}", signal.source)),
                    signal.env_context.clone(),
                    handover,
                    fallback_line,
                    note,
                ];
                opts.system_prompt =
                    Some(parts.into_iter().flatten().filter(|p| !p.is_empty()).collect::<Vec<_>>().join("\n\n"));
                opts.signal_id = Some(host.signal_id);
                opts.session_id = Some(session.id.clone());
                let memory = engine.memory_out(&parent, &session.memory).await?;
                let (mut input, loop_host) =
                    loop_admission(&engine, &opts, PRIMARY, &id, &signal.content, memory, None).await?;
                let workflow_signal = crate::flows::WorkflowSignal {
                    id: signal.id,
                    source: signal.source.clone(),
                    content: signal.content.clone(),
                    env_context: signal.env_context.clone(),
                };
                if let Some(env) = crate::flows::signal_input(&workflow_signal, &session.env).remove("env") {
                    input["env"] = env;
                }
                input["signal"] = crate::supervisor::signal_value(&signal);
                // The route's or the task's span, and in it the primary run's.
                let named = match launch {
                    Launch::Route => format!("route:{}", choice.name),
                    Launch::Saved => "runner".into(),
                };
                let outer = scope.span(
                    SpanStart::new(named, SpanKind::Chain)
                        .metadata(metadata(json!({"workflow": choice.name, "workflow_version": choice.version}))),
                );
                let span = outer.span(SpanStart::new("agent_loop", SpanKind::Chain).metadata(metadata(json!({
                    "activity": "primary_loop", "kind": launch.key(), "workflow": choice.name,
                    "workflow_version": choice.version, "preset": opts.preset.as_str(),
                }))));
                anchor_child(&engine, &id, span, Some(outer), Some(session.id.clone())).await?;
                anchored.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok((input, serde_json::to_value(&loop_host)?))
            })
        }
    };
    let named = crate::runs::start_named_child(engine, ctx, key, name, Site::from(launch), build).await?;
    if let Named::SetAside(fallback) = &named {
        crate::runs::trace_fallback(scope.clone(), fallback, false);
        // A start refused after its anchor was written: nothing ran.
        if anchored.load(std::sync::atomic::Ordering::SeqCst) {
            unanchor(engine, ctx, &id, &fallback.line).await;
        }
    }
    Ok(named)
}

impl From<Launch> for Site {
    fn from(launch: Launch) -> Site {
        match launch {
            Launch::Route => Site::Route,
            Launch::Saved => Site::Saved,
        }
    }
}

/// `run_saved_workflow { name, context }`: the saved workflow a scheduled
/// task names, as the signal's primary run (`{started}`), or `{handover}`
/// for the default path when it has no active version or is set aside.
async fn run_saved_workflow(engine: Arc<Engine>, args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    let call = async {
        signal_of(&ctx, "run_saved_workflow").await?;
        let name = args["name"].as_str().ok_or_else(|| anyhow::anyhow!("name: Required"))?;
        match launch(&engine, &ctx, Launch::Saved, name, &args["context"]).await? {
            Named::Started(child, _) => Ok(json!({"started": child})),
            Named::SetAside(fallback) => {
                let scope = engine.scope_for(&ctx.run_id, &ctx.definition);
                scope.update(ContextUpdate::metadata(metadata(
                    json!({"fallback":"agent-loop","workflow_failure_stage":"saved_workflow"}),
                )));
                Ok(
                    json!({"handover": format!("This task names the saved workflow `{name}`. {} Do the task yourself.", fallback.line)}),
                )
            }
        }
    };
    call.await.map_err(failed)
}

/// `route_signal { context }`: the most specific active route the signal
/// matches, started as the signal's primary run (`{started}`). A route set
/// aside gives way to the next match (R2-L2); with none started, `{routed:
/// false, fallback?}` and the default path runs, the line in its instructions.
async fn route_signal(engine: Arc<Engine>, args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    let call = async {
        let (_, signal) = signal_of(&ctx, "route_signal").await?;
        // A route started before (a re-dispatch) is joined, whatever matches now.
        let names = match RunReads::run(&ctx, &ctx.child_id(ROUTE)).await? {
            Some(saved) => vec![saved.admission.name],
            None => {
                let payload = signal.payload.clone().unwrap_or(Value::Null);
                let routes = crate::flows::signal_routes(&engine, &signal.source, &payload).await?;
                routes.into_iter().map(|(name, _)| name).collect()
            }
        };
        let mut first = None;
        for name in names {
            match launch(&engine, &ctx, Launch::Route, &name, &args["context"]).await? {
                Named::Started(child, _) => return Ok(json!({"started": child})),
                Named::SetAside(fallback) => {
                    first.get_or_insert(fallback);
                }
            }
        }
        Ok(json!({"routed": false, "fallback": first}))
    };
    call.await.map_err(failed)
}

/// `settle_child { ref, kind }`: what a child's end means for the signal
/// (design 03, decision 6), so YAML only switches on `action`: a primary or
/// a route that faulted raises; one that was cancelled is done, without a
/// report; a saved workflow that changed nothing hands over, one that
/// faulted after a write raises.
async fn settle_child(engine: Arc<Engine>, args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    let settle = async {
        signal_of(&ctx, "settle_child").await?;
        let id = args["ref"]["id"].as_str().ok_or_else(|| refuse("settle_child: ref.id required"))?;
        let kind = args["kind"].as_str().unwrap_or(PRIMARY);
        let child = RunReads::run(&ctx, id).await?.filter(|c| c.child.as_ref().is_some_and(|l| l.parent == ctx.run_id));
        let Some(child) = child else {
            return Err(refuse(format!("settle_child: {id} is not a child of this run")));
        };
        use ouro_runtime::ows::run::Status;
        if let Some(outcome) = &child.outcome {
            engine.ended_child(id, outcome).await;
        }
        let scope = engine.scope_for(&ctx.run_id, &ctx.definition);
        let workflow = child.admission.name.clone();
        let settled = match (&child.outcome, kind) {
            // The signal's answer (and its trace's output) names the task's
            // workflow beside what it answered.
            (Some(Status::Completed(output)), SAVED) => {
                let output = json!({"ok": true, "workflow": workflow, "output": output});
                scope.update(ContextUpdate::output(output.clone()));
                json!({"action": "done", "output": output})
            }
            (Some(Status::Completed(output)), _) => {
                let text = output.as_str().map(str::to_owned).unwrap_or_else(|| output.to_string());
                // A route's run is the signal's primary run: its answer is
                // the signal's; a route's skills are its own loop's, read
                // from its run (no primary_loop record).
                if kind == PRIMARY || kind == ROUTE {
                    let records = RunReads::records(&ctx, &ctx.run_id).await?;
                    let loaded = if kind == PRIMARY {
                        recorded_loaded(&records, "primary_loop")
                    } else {
                        let records = RunReads::records(&ctx, id).await?;
                        let host = crate::agent::loop_host_of(child.admission.host(), "settle_child")?;
                        let mut names = crate::agent::loop_skill_names(&engine, &host, &records);
                        // A route that took no turn: its loops' skills (the
                        // `loaded` of each one's stored preparation, design
                        // 09) are the ones that ran.
                        if !crate::agent::turned(&records) {
                            for child in RunReads::children(&ctx, id).await? {
                                let Some(prepared) = engine.deps.db.prepared_loop(&child.admission.id)? else {
                                    continue;
                                };
                                for name in prepared.loaded {
                                    if !names.contains(&name) {
                                        names.push(name);
                                    }
                                }
                            }
                        }
                        names
                    };
                    let metadata = Some(metadata(json!({"skills": loaded})));
                    scope.update(ContextUpdate { output: Some(json!(text)), metadata, ..Default::default() });
                }
                json!({"action": "done", "output": text})
            }
            (Some(Status::Cancelled), _) => json!({"action": "done", "output": null}),
            (Some(Status::Faulted(error)), SAVED) if !effects_started(&ctx, id).await? => {
                let detail = error.detail.clone().unwrap_or_else(|| error.to_string());
                scope.event(EventStart {
                    name: "fallback".into(),
                    level: Some(Level::Warning),
                    metadata: Some(metadata(
                        json!({"stage":"saved_workflow","workflow":workflow,"error":error,"effects_started":false}),
                    )),
                    ..Default::default()
                });
                scope.update(ContextUpdate::metadata(metadata(
                    json!({"fallback":"agent-loop","workflow_failure_stage":"saved_workflow"}),
                )));
                json!({
                    "action": "handover",
                    "handover": format!("The saved workflow `{workflow}` for this task failed before changing anything: {detail}. Do the task yourself."),
                })
            }
            (Some(Status::Faulted(error)), SAVED) => {
                let detail = error.detail.clone().unwrap_or_else(|| error.to_string());
                json!({"action": "raise", "error": {
                    "title": "Saved workflow failed",
                    "detail": format!("Saved workflow `{workflow}` failed after it may have changed something: {detail}"),
                }})
            }
            (Some(Status::Faulted(error)), _) => json!({"action": "raise", "error": error}),
            (_, SAVED) => json!({"action": "done", "output": null}),
            _ => json!({"action": "done", "output": json!({"ok": true, "resumes": true}).to_string()}),
        };
        Ok(settled)
    };
    settle.await.map_err(failed)
}

// The skills the signal's loop starter (`primary_loop`) said its loop runs.
fn recorded_loaded(records: &crate::agent::Records, starter: &str) -> Vec<String> {
    use ouro_runtime::ows::durable::Record;
    records
        .values()
        .find_map(|record| match record {
            Record::Call { function, arguments, .. } if function == starter => Some(strings(arguments, "loaded")),
            _ => None,
        })
        .unwrap_or_default()
}

/// Whether a run or one of its descendants started a write: an at-most-once
/// call that completed, failed or has an unknown outcome. The call journal
/// comes from each run's records (run access has no journal of its own).
async fn effects_started(runs: &dyn RunReads, id: &str) -> anyhow::Result<bool> {
    use ouro_runtime::CallStatus;
    let mut pending = vec![id.to_owned()];
    while let Some(run) = pending.pop() {
        let records = runs.records(&run).await.unwrap_or_default();
        for call in ouro_runtime::ows::durable::call_journal(&records) {
            if call.delivery == Delivery::AtMostOnce
                && matches!(call.state, CallStatus::Completed | CallStatus::Failed | CallStatus::Unknown)
            {
                return Ok(true);
            }
        }
        pending.extend(runs.children(&run).await?.into_iter().map(|r| r.admission.id));
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
        let saved = RunReads::run(&ctx, &ctx.run_id).await?.ok_or_else(|| anyhow::anyhow!("unknown run"))?;
        let scope = engine.scope_for(&ctx.run_id, &ctx.definition);
        crate::agent::guard_review(&engine, &host, &saved.admission.input, &scope, &ctx.cancel).await
    };
    call.await.map_err(failed)
}

// The reviewed item of `call` from this guard run's own `review` record,
// never from YAML.
async fn reviewed(
    ctx: &FunctionContext,
    args: &Value,
) -> anyhow::Result<(providers::ToolCall, String, crate::guard::Verdict, crate::agent::Records)> {
    let call = tool_call(&args["call"])?;
    let key = crate::guard::key(&call);
    let records = RunReads::records(ctx, &ctx.run_id).await?;
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
    let (call, _, verdict, _) = reviewed(&ctx, &args).await.map_err(failed)?;
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
    let card = crate::confirmation::Card::load(&*engine.deps.memory);
    let request = crate::confirmation::Request {
        run: &ctx.run_id,
        loop_run: &host.parent_loop,
        call: &call,
        target: &target,
        guidance: &verdict.guidance,
        goal,
        card: &card,
    };
    // A dry run sends no card: it stores one and answers it at once, as the
    // try asked, with a press delivered to this run (design 05, decision 7).
    if let Some(sandbox) = &engine.sandbox {
        let scope = engine.scope_for(&ctx.run_id, &ctx.definition);
        return sandbox.press(&engine.deps.db, &ctx, request, &host.parent_loop, &scope).await.map_err(failed_once);
    }
    crate::confirmation::request(&engine.deps.db, &*engine.deps.mcp, request, &ctx.cancel).await.map_err(failed_once)
}

/// `resolve_review { call, outcome }`: the call's verdict. For one that
/// needed confirmation, the outcome is the event this run consumed, checked
/// against the card's chat, message and thread and spent once (invariant 1);
/// the YAML's word counts only for a real expiry.
async fn resolve_review(engine: Arc<Engine>, args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    let call = async {
        let host = guard_host(&ctx, "resolve_review")?;
        let (call, key, verdict, records) = reviewed(&ctx, &args).await?;
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
            // A dry run's card was never sent: there is none to edit.
            if engine.sandbox.is_none() {
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
            }
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
async fn signal_of(ctx: &FunctionContext, function: &str) -> anyhow::Result<(SignalHost, PendingSignal)> {
    let host: SignalHost = match crate::runs::host_kind(ctx.host()) {
        Some(SIGNAL) => serde_json::from_value(ctx.host().cloned().unwrap_or_default())?,
        _ => return Err(refuse(format!("{function}: {DISPATCHER_ONLY}"))),
    };
    let saved = RunReads::run(ctx, &ctx.run_id).await?.ok_or_else(|| anyhow::anyhow!("unknown run"))?;
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
        let (host, signal) = signal_of(&ctx, "signal_session").await?;
        // Design 05, test 19: a deliberate read through the engine's own
        // handle in a dry run, which the sandbox refuses and counts.
        if engine.sandbox.as_ref().is_some_and(|s| s.stray_read()) {
            let _ = engine.runtime.get(&ctx.run_id).await;
        }
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
        let (host, signal) = signal_of(&ctx, "deliver_event").await?;
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
async fn signal_transcript(_engine: Arc<Engine>, args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    let call = async {
        signal_of(&ctx, "signal_transcript").await?;
        let Some(id) = args["ref"]["id"].as_str() else { return Ok(json!("[]")) };
        let own =
            RunReads::run(&ctx, id).await?.is_some_and(|c| c.child.as_ref().is_some_and(|l| l.parent == ctx.run_id));
        if !own {
            return Err(refuse(format!("signal_transcript: {id} is not a child of this run")));
        }
        let mut records = RunReads::records(&ctx, id).await.unwrap_or_default();
        // A route that took no turn ran a loop of its own (its child, design
        // 09): the report reads the last of its loops that did (design 08, G-M5).
        if !crate::agent::turned(&records) {
            for child in RunReads::children(&ctx, id).await.unwrap_or_default() {
                let child = RunReads::records(&ctx, &child.admission.id).await.unwrap_or_default();
                if crate::agent::turned(&child) {
                    records = child;
                }
            }
        }
        let messages = crate::agent::history_messages(&records);
        // A run that took no turn (a saved workflow's steps) has only its
        // admitted instructions: no transcript.
        let transcript = if crate::agent::turned(&records) && messages.iter().any(|m| m["role"] != "system") {
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

// ── 8b. scheduled tasks ─────────────────────────────────────────────────────

/// `fire_scheduled { slot }`: a task's trigger run records its fire
/// (schedule.rs §5) and ends at once; the supervisor's drain runs it. A
/// storage error is no answer (`interrupted`: the engine resends it).
async fn fire_scheduled(engine: Arc<Engine>, args: Value, ctx: FunctionContext) -> Result<Value, FunctionError> {
    let run = RunReads::run(&ctx, &ctx.run_id)
        .await
        .map_err(failed)?
        .ok_or_else(|| FunctionError::interrupted("the call lost its run"))?;
    crate::schedule::fire(&engine, &run, &args["slot"]).await.map_err(|failure| match failure {
        crate::schedule::FireFailure::Storage(error) => FunctionError::interrupted(format!("{error:#}")),
        crate::schedule::FireFailure::Invalid(detail) => FunctionError::runtime(detail),
    })
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
    // "signal|agent-loop", or a reserved context no version of the agent's
    // may call: "guard", "task" (`RESERVED_CONTEXTS`).
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
        "loop_begin",
        Read,
        &[],
        &[],
        "agent-loop",
        "The first messages and working memory of a loop a workflow started with start_child (host: inherit, input.request), as the host prepared them once from its request; also conversation, confirm and on_step. Call it first when $context.messages is null and export the result into $context, as agent-loop's begin step does.",
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
        &["call", "i", "index", "memory", "loop"],
        &["call", "i", "index"],
        "agent-loop",
        "Start the model's invoke_sub_agent call as a worker child. loop names the loop it runs, before the call's own loop argument and its skills' preference (default agent-loop; one set aside runs agent-loop). Returns id, key, done and waiting (id null when it cannot start); join it with listen, then tool_repeat with worker_run.",
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
        &["name", "context"],
        &["name"],
        "signal",
        "Start the active saved workflow a scheduled signal names as the signal's primary run (base preset and the source's skill unless its metadata.agent says otherwise; it may take agent turns), after signal_session. Returns started (a child ref: join it with listen, child_outcome and settle_child with kind saved), or handover for the default path when it has no active version or is set aside.",
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
        &[
            "input",
            "loop",
            "confirm",
            "skills",
            "loaded",
            "preset",
            "allow_skill_writes",
            "system",
            "exclude_tools",
            "on_step",
        ],
        &["input"],
        "signal",
        "Start the signal's primary agent as a child after signal_session; loop names the loop it runs (default agent-loop; one set aside runs agent-loop). Returns id, key, done and waiting: join it with listen on done, child_outcome and settle_child.",
    ),
    f(
        "loop_outcome",
        Read,
        &["ref"],
        &["ref"],
        "agent-loop",
        "How a loop this run started ended (ref: what start_child returned): status completed (with output), faulted (with error: a bad request faults it too), cancelled, or running.",
    ),
    f(
        "loop_transcript",
        Read,
        &["ref"],
        &["ref"],
        "agent-loop",
        "What a loop this run started did (ref: what start_child returned), as compact text without its instructions: the request, its calls with their results, its notes; long work keeps the end.",
    ),
    f(
        "observe_step",
        Read,
        &["i", "calls", "content", "reasoning"],
        &["i", "calls"],
        "agent-loop",
        "Start the loop's on_step observer for turn i (never joined; a missing or set-aside observer is skipped). Returns id, or null.",
    ),
    f(
        "route_signal",
        Write,
        &["context"],
        &["context"],
        "signal",
        "Start the most specific active metadata.route match (source and top-level payload fields) as the signal's primary run, admitted with the smart preset unless its metadata says otherwise; a route set aside gives way to the next match. Pass context: $context after session setup (its handover reaches the route). Returns started (a child ref: settle it with kind route), or routed:false with the fallback record of a set-aside route.",
    ),
    f(
        "settle_child",
        Read,
        &["ref", "kind"],
        &["ref"],
        "signal",
        "What a child's end means (kind primary, route or saved): action done (with output), handover (with handover: a saved workflow that changed nothing) or raise (with error).",
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
    f("fire_scheduled", Write, &["slot"], &["slot"], "task", "A scheduled task's trigger records its fire."),
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

/// The engine's own workflow functions (`start_child { name, input, host }`,
/// `child_outcome { id, key }`), for the author's catalog.
pub const ENGINE_FUNCTIONS: &[HostFunction] = &[
    f(
        "start_child",
        Read,
        &["name", "input", "host"],
        &["name"],
        "workflow",
        "The engine's own: start an owned child of a workflow. Without host it has no host data and cannot take agent turns. host: inherit gives it this run's host data unchanged, which is how a workflow starts a loop (agent-loop or one of yours): input { request: { input, skills, loaded, preset, system, exclude_tools, on_step, allow_skill_writes } }. The loop keeps this run's conversation, confirmation target, session and excluded tools; its instructions start with this run's, then system; exclude_tools takes more tools away from it and its workers; on_step {workflow, input} starts that workflow after each acting turn with input + {seq, step}. A bad request faults the loop; a paused or tripped loop is refused with an error you catch (urn:ouro:errors:definition.tripped): nothing runs in its place. Returns id, key, done and waiting: join with listen, then loop_outcome and loop_transcript.",
    ),
    f("child_outcome", Read, &["id", "key"], &["id", "key"], "workflow", "The engine's own: an owned child's outcome."),
];

/// The contexts whose functions belong to the runtime alone: no version of
/// the agent's may call them, and the author's catalog leaves them out.
pub const RESERVED_CONTEXTS: &[&str] = &["guard", "task"];

pub fn host_function(name: &str) -> Option<&'static HostFunction> {
    HOST_FUNCTIONS.iter().chain(ENGINE_FUNCTIONS).find(|f| f.name == name)
}
