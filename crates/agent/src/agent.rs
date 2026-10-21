// Agent runtime: engine wiring, synthetic tools, and the ReAct loop. Shared
// clients are injected once; each loop owns its conversation and cancellation.
// Sections: 1. engine / skill scoping; 2. loop lifecycle; 3. dispatch / workers;
// 4. the loop as a workflow; 5. run history.
use crate::{
    codex::{SharedCodex, code_agent},
    db::MemoryStore,
    errors::{aborted, check},
    generation::{Observation, describe_completion, generation},
    mcp::SharedMcp,
    models::{PresetName, Presets},
    providers::{self, Message, SharedProvider, ToolDef},
    session::{Format, Session, render_context},
    skills::{SkillStore, SkillTools, declared_tools},
    tool_results::{WORKING_MEMORY_INSTRUCTIONS, is_tool_error, store_tool_result},
    tracing::{ContextUpdate, Metadata, Scope, Span, SpanEnd, SpanKind, SpanStart, Trace, TraceStart, Tracer},
};
use futures::{FutureExt, future::BoxFuture};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Weak};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

// ── 1. engine ──────────────────────────────────────────────────────────────
pub struct Providers {
    pub deepseek: SharedProvider,
    pub openai: SharedProvider,
    pub gemini: SharedProvider,
    pub openrouter: Option<SharedProvider>,
}
pub struct EngineDeps {
    pub providers: Providers,
    pub mcp: SharedMcp,
    pub presets: Presets,
    pub skills: Vec<String>,
    pub skill_store: Arc<dyn SkillStore>,
    pub memory: Arc<dyn MemoryStore>,
    pub tracer: Arc<dyn Tracer>,
    pub codex: SharedCodex,
    pub decisions: Option<Arc<crate::decisions::DecisionClient>>,
    pub workflows: Arc<dyn crate::flows::WorkflowStore>,
    pub schedule: Arc<dyn crate::schedule::ScheduleStore>,
}
pub struct Engine {
    pub deps: EngineDeps,
    loops: tokio::sync::Mutex<HashMap<String, Arc<AgentLoop>>>,
    stopping: AtomicBool,
    shutdown: tokio::sync::Mutex<bool>,
}
pub fn create_engine(deps: EngineDeps) -> Arc<Engine> {
    Arc::new(Engine { deps, loops: Default::default(), stopping: AtomicBool::new(false), shutdown: Default::default() })
}
impl Engine {
    pub fn resolve_provider(&self, model: &str) -> anyhow::Result<SharedProvider> {
        let providers = &self.deps.providers;
        if providers::is_openrouter_model(model) {
            return providers.openrouter.clone().ok_or_else(|| {
                anyhow::anyhow!("model \"{model}\" routes to OpenRouter, but no OpenRouter provider is configured")
            });
        }
        Ok(if model.starts_with("deepseek") {
            providers.deepseek.clone()
        } else if model.starts_with("gemini") {
            providers.gemini.clone()
        } else {
            providers.openai.clone()
        })
    }
    pub async fn start_loop(self: &Arc<Self>, opts: LoopOptions) -> anyhow::Result<Arc<AgentLoop>> {
        self.create_or_join_loop(opts, false).await
    }
    async fn create_or_join_loop(self: &Arc<Self>, opts: LoopOptions, join: bool) -> anyhow::Result<Arc<AgentLoop>> {
        self.check_start(&opts)?;
        if let Some(live) = self.loops.lock().await.get(&opts.id).cloned() {
            if join {
                return Ok(live);
            }
            anyhow::bail!("agent-loop id {} already exists", opts.id);
        }
        let mut resolved: Map<String, Value> = Map::new();
        let mut accumulated = HashSet::new();
        let mut wildcard = false;
        for (required, names) in [
            (true, opts.skills.as_slice()),
            (false, if opts.include_engine_skills { self.deps.skills.as_slice() } else { &[] }),
        ] {
            for name in names {
                let Some(skill) = self.deps.skill_store.read_skill(name).await? else {
                    if required {
                        anyhow::bail!(
                            "session skill \"{name}\" not found (skills/{name}.md and skills.default/{name}.md both missing)"
                        );
                    }
                    eprintln!("[{}] [warn] engine skill \"{name}\" not found, skipping", opts.id);
                    continue;
                };
                resolved.insert(name.clone(), skill.body.into());
                match skill.tools {
                    SkillTools::All => wildcard = true,
                    SkillTools::Named(tools) => accumulated.extend(tools),
                }
            }
        }
        let mut allowed = if wildcard { None } else { Some(accumulated) };
        if let Some(whitelist) = &opts.tool_whitelist {
            allowed = Some(match allowed {
                None => whitelist.clone(),
                Some(set) => set.intersection(whitelist).cloned().collect(),
            });
        }
        // IO above may have overlapped shutdown or another start of this id.
        let mut registry = self.loops.lock().await;
        self.check_start(&opts)?;
        if let Some(live) = registry.get(&opts.id) {
            if join {
                return Ok(live.clone());
            }
            anyhow::bail!("agent-loop id {} already exists", opts.id);
        }
        let loop_ = create_loop(self, opts, resolved, allowed)?;
        if join {
            loop_.memory_restored.store(false, Ordering::SeqCst);
        }
        registry.insert(loop_.opts.id.clone(), loop_.clone());
        Ok(loop_)
    }
    fn check_start(&self, opts: &LoopOptions) -> anyhow::Result<()> {
        check(&opts.cancel)?;
        if self.stopping.load(Ordering::SeqCst) {
            return Err(aborted());
        }
        Ok(())
    }
    pub async fn end_loop(&self, id: &str) {
        let loop_ = self.loops.lock().await.get(id).cloned();
        if let Some(loop_) = loop_ {
            loop_.close().await;
            let mut registry = self.loops.lock().await;
            if registry.get(id).is_some_and(|current| Arc::ptr_eq(current, &loop_)) {
                registry.remove(id);
            }
        }
    }
    // The loops alive now: a started loop stays registered until end_loop.
    pub async fn loop_ids(&self) -> Vec<String> {
        let mut ids: Vec<_> = self.loops.lock().await.keys().cloned().collect();
        ids.sort();
        ids
    }
    pub async fn shutdown(&self) {
        self.stopping.store(true, Ordering::SeqCst);
        let mut done = self.shutdown.lock().await;
        if *done {
            return;
        }
        let loops: Vec<_> = self.loops.lock().await.drain().map(|(_, l)| l).collect();
        // Cancel everyone first, then join their finalizers before clients close.
        for loop_ in &loops {
            loop_.cancel.cancel();
        }
        futures::future::join_all(loops.iter().map(|l| l.close())).await;
        self.deps.tracer.shutdown().await;
        self.deps.mcp.close().await;
        *done = true;
    }
}

// ── 2. loop lifecycle ───────────────────────────────────────────────────────
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfirmTarget {
    pub chat_id: String,
    pub message_thread_id: Option<i64>,
}

#[derive(Clone)]
pub struct LoopOptions {
    pub id: String,
    pub session: Session,
    pub cancel: CancellationToken,
    pub generation_timeout: Option<Duration>,
    pub max_concurrent_tools: usize,
    pub system_prompt: Option<String>,
    pub skills: Vec<String>,
    pub allow_skill_writes: bool,
    pub tool_whitelist: Option<HashSet<String>>,
    pub include_engine_skills: bool,
    pub preset: PresetName,
    pub max_iterations: usize,
    pub parent_id: Option<String>,
    pub root_request: Option<String>,
    // Host-only evidence, kept separately from rendered history/tool inputs.
    pub guard_request: Option<String>,
    pub confirm: Option<ConfirmTarget>,
    pub earlier_user_messages: Vec<String>,
    pub loaded_skill_text: String,
    pub session_id: Option<String>,
    pub tags: Option<Vec<String>>,
    pub metadata: Metadata,
    pub trace_scope: Option<Scope>,
}
impl LoopOptions {
    pub fn new(id: impl Into<String>, session: Session) -> Self {
        Self {
            id: id.into(),
            session,
            cancel: CancellationToken::new(),
            generation_timeout: None,
            max_concurrent_tools: 4,
            system_prompt: None,
            skills: Vec::new(),
            allow_skill_writes: false,
            tool_whitelist: None,
            include_engine_skills: true,
            preset: PresetName::Base,
            max_iterations: 100,
            parent_id: None,
            root_request: None,
            guard_request: None,
            confirm: None,
            earlier_user_messages: Vec::new(),
            loaded_skill_text: String::new(),
            session_id: None,
            tags: None,
            metadata: Map::new(),
            trace_scope: None,
        }
    }
}
pub struct AgentLoop {
    engine: Weak<Engine>,
    pub opts: LoopOptions,
    pub model: String,
    pub reasoning_effort: crate::models::ReasoningEffort,
    pub messages: tokio::sync::Mutex<Vec<Message>>,
    pub scope: Scope,
    trace: Option<Arc<dyn Trace>>,
    trace_ended: AtomicBool,
    cancel: CancellationToken,
    closed: AtomicBool,
    gate: tokio::sync::Mutex<()>,
    allowed: Option<HashSet<String>>,
    skill_names: Vec<String>,
    loaded_skills: String,
    sub_counter: AtomicUsize,
    observation_meta: Metadata,
    // False for a loop rebuilt after a restart until a host function has
    // put its working memory back from the run's context.
    memory_restored: AtomicBool,
    guard: tokio::sync::Mutex<crate::guard::State>,
    // The round's calls running at once: at most max_concurrent_tools.
    slots: tokio::sync::Semaphore,
}
fn create_loop(
    engine: &Arc<Engine>,
    mut opts: LoopOptions,
    resolved: Map<String, Value>,
    allowed: Option<HashSet<String>>,
) -> anyhow::Result<Arc<AgentLoop>> {
    if opts.max_concurrent_tools == 0 {
        anyhow::bail!("maxConcurrentTools must be a positive integer");
    }
    opts.session_id = Some(opts.session_id.clone().unwrap_or_else(|| opts.session.id.clone()));
    let preset = engine.deps.presets.get(opts.preset).clone();
    let opts_slots = opts.max_concurrent_tools;
    let skill_names: Vec<_> = resolved.keys().cloned().collect();
    let mut parts = Vec::new();
    if let Some(prompt) = opts.system_prompt.as_ref().filter(|s| !s.is_empty()) {
        parts.push(prompt.as_str());
    }
    parts.extend(resolved.values().filter_map(Value::as_str));
    let loaded_skills = resolved
        .values()
        .filter_map(Value::as_str)
        .chain(std::iter::once(opts.loaded_skill_text.as_str()))
        .collect::<Vec<_>>()
        .join("\n\n");
    parts.push(WORKING_MEMORY_INSTRUCTIONS);
    let messages = vec![providers::system(&parts.join("\n\n---\n\n"))];
    let mut metadata = opts.metadata.clone();
    metadata.extend(json!({"agent_id": opts.id, "skills": skill_names, "preset": opts.preset.as_str(), "model": preset.model, "reasoning_effort": preset.reasoning_effort.as_str(), "max_iterations": opts.max_iterations}).as_object().cloned().unwrap_or_default());
    let mut observation_meta = Map::new();
    observation_meta.insert("agent_id".into(), opts.id.clone().into());
    if let Some(parent) = &opts.parent_id {
        metadata.insert("parent_id".into(), parent.clone().into());
        observation_meta.insert("parent_id".into(), parent.clone().into());
    }
    let (scope, trace) = if let Some(scope) = &opts.trace_scope {
        metadata.insert("skill".into(), skill_names.first().map(String::as_str).unwrap_or("worker").into());
        scope.update(ContextUpdate::metadata(metadata));
        (scope.clone(), None)
    } else {
        let trace = engine.deps.tracer.trace(TraceStart {
            id: opts.id.clone(),
            name: opts.id.clone(),
            session_id: opts.session_id.clone(),
            tags: opts.tags.clone(),
            metadata: Some(metadata),
            kind: SpanKind::Agent,
        });
        let scope: Scope = trace.clone();
        (scope, Some(trace))
    };
    Ok(Arc::new(AgentLoop {
        engine: Arc::downgrade(engine),
        cancel: opts.cancel.child_token(),
        opts,
        model: preset.model,
        reasoning_effort: preset.reasoning_effort,
        messages: tokio::sync::Mutex::new(messages),
        scope,
        trace,
        trace_ended: AtomicBool::new(false),
        closed: AtomicBool::new(false),
        gate: Default::default(),
        allowed,
        skill_names,
        loaded_skills,
        sub_counter: AtomicUsize::new(0),
        observation_meta,
        memory_restored: AtomicBool::new(true),
        slots: tokio::sync::Semaphore::new(opts_slots),
        guard: Default::default(),
    }))
}
impl AgentLoop {
    fn end_trace(&self) {
        if !self.trace_ended.swap(true, Ordering::SeqCst)
            && let Some(trace) = &self.trace
        {
            trace.end();
        }
    }
    pub async fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.cancel.cancel();
        let _joined = self.gate.lock().await;
        self.end_trace();
    }
    pub fn run(self: &Arc<Self>) -> BoxFuture<'static, anyhow::Result<String>> {
        self.execute(None)
    }
    pub fn send(self: &Arc<Self>, text: String) -> BoxFuture<'static, anyhow::Result<String>> {
        self.execute(Some(text))
    }
    fn execute(self: &Arc<Self>, text: Option<String>) -> BoxFuture<'static, anyhow::Result<String>> {
        let loop_ = self.clone();
        async move {
            check(&loop_.cancel)?;
            if loop_.closed.load(Ordering::SeqCst) {
                return Err(aborted());
            }
            let _running =
                loop_.gate.try_lock().map_err(|_| anyhow::anyhow!("session {} is already running", loop_.opts.id))?;
            check(&loop_.cancel)?;
            let outcome = loop_.drive(text).await;
            if let Err(error) = &outcome {
                loop_.scope.update(ContextUpdate {
                    output: Some(json!({"error": error.to_string()})),
                    metadata: Some(json!({"error": true}).as_object().cloned().unwrap_or_default()),
                    ..Default::default()
                });
            }
            loop_.end_trace();
            outcome
        }
        .boxed()
    }
    // Runs this loop's `agent-loop` workflow here, from its transcript plus
    // `text`, to its answer.
    async fn drive(self: &Arc<Self>, text: Option<String>) -> anyhow::Result<String> {
        let engine = self.engine.upgrade().ok_or_else(aborted)?;
        let input = {
            let mut messages = self.messages.lock().await;
            if let Some(text) = text {
                messages.push(providers::user(&text));
            }
            let task =
                messages.iter().find(|m| providers::role(m) == "user").and_then(providers::content).unwrap_or_default();
            json!({"loop": self.spec(), "messages": *messages, "memory": self.opts.session.memory.snapshot(), "guard_context": self.guard_context(task)})
        };
        self.admit_guard(&input).await?;
        let result = crate::flows::run_loop_here(&engine, &self.opts.id, input, &self.scope, &self.cancel).await;
        check(&self.cancel)?;
        match result {
            crate::flows::FlowResult::Completed(Value::String(output)) => Ok(output),
            crate::flows::FlowResult::Completed(output) => Ok(output.to_string()),
            crate::flows::FlowResult::Cancelled => Err(aborted()),
            crate::flows::FlowResult::Faulted { error, .. } => {
                Err(anyhow::anyhow!(error.detail.clone().unwrap_or_else(|| error.to_string())))
            }
            other => anyhow::bail!("session {} did not end: {other:?}", self.opts.id),
        }
    }
    // The model's next message after `messages`, traced as `iter-{iteration}`.
    async fn generate(
        &self,
        engine: &Arc<Engine>,
        messages: &mut Vec<Message>,
        iteration: usize,
    ) -> anyhow::Result<Message> {
        check(&self.cancel)?;
        let provider = engine.resolve_provider(&self.model)?;
        let mut tools: Vec<_> = crate::mcp::model_tools(&*engine.deps.mcp)
            .into_iter()
            .filter(|t| self.allowed.as_ref().is_none_or(|s| s.contains(&t.name)))
            .collect();
        tools.extend(synthetic_definitions().iter().filter(|t| self.visible(&t.name)).cloned());
        let mut params = providers::CompletionParams {
            model: self.model.clone(),
            messages,
            reasoning_effort: self.reasoning_effort,
            tools,
            response_format: None,
            trace: None,
            cancel: self.cancel.clone(),
        };
        let result = generation(&provider, &mut params, &self.scope, Observation { name: format!("iter-{iteration}"), model_parameters: Some(json!({"reasoning_effort": self.reasoning_effort.as_str(), "thinking": if self.reasoning_effort == crate::models::ReasoningEffort::Disabled {"disabled"} else {"enabled"}}).as_object().cloned().unwrap_or_default()), metadata: Some(self.observation_meta.clone()) }, self.opts.generation_timeout, describe_completion).await?;
        Ok(result.message)
    }
    fn finished(&self, output: &str) {
        self.scope.update(ContextUpdate {
            output: Some(json!(output)),
            metadata: Some(json!({"finished_by": "finish"}).as_object().cloned().unwrap_or_default()),
            ..Default::default()
        });
    }

    fn visible(&self, name: &str) -> bool {
        match name {
            "invoke_sub_agent" => self.opts.parent_id.is_none(),
            "write_skill" => {
                self.opts.parent_id.is_none()
                    || (self.opts.allow_skill_writes && self.skill_names.iter().any(|n| n == "dreaming"))
            }
            // Writing a workflow is the author's alone; other loops read,
            // activate and schedule the versions it saved and tried.
            name if crate::flows::AUTHOR_TOOLS.contains(&name) || name == "save_workflow" => {
                self.skill_names.iter().any(|n| n == "workflow-author")
            }
            _ => true,
        }
    }
    // One call under its span, left open for the caller: the tool's raw reply.
    async fn start_call(
        self: &Arc<Self>,
        engine: &Arc<Engine>,
        call: &providers::ToolCall,
        child: Option<ChildRun<'_>>,
        runs: Option<&crate::durable::RunStore>,
        verdict: Option<&crate::guard::Verdict>,
    ) -> anyhow::Result<(Arc<dyn Span>, String)> {
        let mut metadata = self.observation_meta.clone();
        metadata.insert("tool".into(), call.name.clone().into());
        metadata.insert("tool_call_id".into(), call.id.clone().into());
        let span = self.scope.span(
            SpanStart::new(&call.name, if call.name == "invoke_sub_agent" { SpanKind::Agent } else { SpanKind::Tool })
                .input(json!({"raw_arguments": call.arguments}))
                .metadata(metadata),
        );
        if let Some(verdict) = verdict {
            span.update(ContextUpdate::metadata(json!({"guard": verdict}).as_object().unwrap().clone()));
            if !verdict.allowed() {
                let context = self.guard.lock().await.context.clone().expect("admitted guard context");
                return Ok((span, verdict.reply(&context)));
            }
        }
        let raw = match serde_json::from_str::<Value>(if call.arguments.is_empty() { "{}" } else { &call.arguments }) {
            Ok(Value::Object(args)) => {
                span.update(ContextUpdate::input(Value::Object(args.clone())));
                self.invoke(engine, &call.name, args, span.clone(), child, runs).await
            }
            _ => Ok("[tool error] arguments must be a JSON object".to_owned()),
        };
        match raw {
            Ok(raw) => Ok((span, raw)),
            Err(error) => {
                span.end(SpanEnd::error(error.to_string()));
                Err(error)
            }
        }
    }
    // What the model sees of a reply: the reply itself for the working
    // memory tools and bounded reports (storing those would only
    // cost a turn to read them back), else the reply stored in working
    // memory with its content or a preview.
    // Also the entry it stored, `[key, value, format]`.
    fn present(&self, name: &str, raw: &str, span: &Arc<dyn Span>) -> (String, Option<Value>) {
        if raw.starts_with("[guard] blocked:")
            || name.starts_with("working_memory_")
            || matches!(name, "try_workflow" | "read_run")
        {
            return (raw.to_owned(), None);
        }
        let stored = store_tool_result(&self.opts.session.memory, raw);
        span.update(ContextUpdate::metadata(json!({"memory_key": stored.memory_key, "result_size_bytes": stored.size_bytes, "result_truncated": stored.truncated}).as_object().cloned().unwrap_or_default()));
        (stored.to_json().to_string(), Some(json!([stored.memory_key, raw, stored.format])))
    }
    async fn invoke(
        self: &Arc<Self>,
        engine: &Arc<Engine>,
        name: &str,
        args: Map<String, Value>,
        span: Arc<dyn Span>,
        child: Option<ChildRun<'_>>,
        runs: Option<&crate::durable::RunStore>,
    ) -> anyhow::Result<String> {
        check(&self.cancel)?;
        let synthetic = synthetic_definitions().iter().any(|t| t.name == name);
        if synthetic && !self.visible(name) {
            return Ok(format!("[tool error] {name} is unavailable in this loop"));
        }
        let outcome = if synthetic {
            self.synthetic(engine, name, &args, span, child, runs).await
        } else if !crate::mcp::model_tools(&*engine.deps.mcp).iter().any(|t| t.name == name)
            || !self.allowed.as_ref().is_none_or(|s| s.contains(name))
        {
            return Ok(format!("[tool error] {name} is unavailable in this loop"));
        } else {
            engine.deps.mcp.call_tool(name, args, &self.cancel).await
        };
        check(&self.cancel)?;
        Ok(outcome.unwrap_or_else(|e| format!("[{} error] {}", if synthetic { name } else { "tool" }, e)))
    }
    async fn synthetic(
        self: &Arc<Self>,
        engine: &Arc<Engine>,
        name: &str,
        args: &Map<String, Value>,
        span: Arc<dyn Span>,
        child: Option<ChildRun<'_>>,
        runs: Option<&crate::durable::RunStore>,
    ) -> anyhow::Result<String> {
        match name {
            "read_run" => read_run(runs, &self.opts.id, args),
            "find_skills" => {
                let params: crate::skills::FindSkillsParams = serde_json::from_value(json!(args))?;
                params.validate()?;
                let client = engine.deps.decisions.as_deref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "find_skills needs the agent decision client; use list_skills/read_skill while unavailable"
                    )
                })?;
                let index = crate::skills::read_skill_index(&*engine.deps.mcp, &self.cancel).await?;
                let scope: Scope = span;
                Ok(crate::skills::find_skills(&*engine.deps.skill_store, &index, client, &params, &scope, &self.cancel)
                    .await?
                    .to_string())
            }
            // Only validates: ending the session is the round's decision
            // (agent_settle), taken once every call has answered.
            "finish" => {
                if let Some(extra) = args.keys().find(|k| *k != "result") {
                    anyhow::bail!("unknown argument `{extra}`");
                }
                optional_string(args, "result")?;
                Ok(json!({"ok": true}).to_string())
            }
            "hash_value" => {
                let value = args.get("value").ok_or_else(|| anyhow::anyhow!("value is required"))?;
                Ok(json!({"hash":crate::skills::hash_value(value)}).to_string())
            }
            "skill_catalog_snapshot" => {
                Ok(crate::skills::catalog_snapshot(&*engine.deps.skill_store).await?.to_string())
            }
            "set_memory" => {
                let key = string_arg(args, "key", true)?;
                let value = string_arg(args, "value", false)?;
                engine.deps.memory.set(key, value)?;
                Ok(format!("ok — stored {key}"))
            }
            "working_memory_put" => {
                let key = string_arg(args, "key", true)?;
                let value = string_arg(args, "value", false)?;
                let format = match optional_string(args, "format")? {
                    None | Some("text") => Format::Text,
                    Some("json") => Format::Json,
                    _ => anyhow::bail!("format: Invalid enum value. Expected 'text' | 'json'"),
                };
                self.opts.session.memory.put(key, value, format)?;
                Ok(json!({"memory_key": key, "format": format.as_str(), "size_bytes": value.len()}).to_string())
            }
            "working_memory_get" => Ok(self.opts.session.memory.get(string_arg(args, "key", true)?)?),
            "working_memory_list" => Ok(serde_json::to_string(&self.opts.session.memory.list())?),
            "working_memory_delete" => {
                let key = string_arg(args, "key", true)?;
                Ok(json!({"memory_key": key, "deleted": self.opts.session.memory.delete(key)}).to_string())
            }
            "code_agent" => {
                code_agent(
                    &engine.deps.codex,
                    string_arg(args, "task", true)?,
                    optional_string(args, "data")?,
                    &self.cancel,
                )
                .await
            }
            "read_skill" => {
                let name = string_arg(args, "name", true)?;
                // A skill this loop already runs is in its instructions: a
                // second copy would cost its size again on every turn. A loop
                // that may rewrite skills still gets the text it edits.
                if self.skill_names.iter().any(|n| n == name)
                    && !self.visible("write_skill")
                    && !args.contains_key("expected_hash")
                {
                    return Ok(json!({"name": name, "found": true, "loaded": true, "content": null,
                        "note": "Already part of your instructions."})
                    .to_string());
                }
                Ok(match engine.deps.skill_store.read_skill(name).await? {
                    None => json!({"name": name, "found": false, "content": null}),
                    Some(mut skill) => {
                        let editable_body = skill.body.clone();
                        if let Some(patch)=engine.deps.skill_store.read_patch(name).await? {
                            skill.body=crate::skills::append_patch(&skill.body,&patch);
                        }
                        let content_hash=crate::skills::hash_value(&json!({"content":skill.body,"tools":skill.tools.to_json(),"routable":skill.routable}));
                        if let Some(expected)=optional_string(args,"expected_hash")? {
                            anyhow::ensure!(expected==content_hash,"Skill changed since find_skills; search again");
                        }
                        let mut result=json!({"name": name,"found":true,"content":if self.opts.allow_skill_writes { &editable_body } else { &skill.body },"effectiveInstructions":skill.body,"content_hash":content_hash,"tools":skill.tools.to_json(),"routable":skill.routable,"source":skill.source,"sizeBytes":skill.body.len()});
                        if self.visible("write_skill") {
                            result["editableBody"]=json!(editable_body);
                        }
                        result
                    },
                }.to_string())
            }
            "write_skill" => {
                let name = string_arg(args, "name", true)?;
                let content = string_arg(args, "content", true)?;
                let known: HashSet<_> = engine.deps.mcp.tools().into_iter().map(|t| t.name).collect();
                if let SkillTools::Named(tools) = declared_tools(name, content)? {
                    let unknown: Vec<_> = tools.into_iter().filter(|t| !known.contains(t)).collect();
                    if !unknown.is_empty() {
                        anyhow::bail!(
                            "skill \"{name}\" declares tools MCP doesn't have: {} — the agent would refuse to start with it.",
                            unknown.join(", ")
                        );
                    }
                }
                let written = engine.deps.skill_store.save_skill(name, content).await?;
                Ok(json!({"ok": true, "name": name, "path": written.path, "sizeBytes": written.size_bytes}).to_string())
            }
            "list_skills" => {
                let skills = engine.deps.skill_store.list_skills().await?;
                Ok(json!({"count": skills.len(), "skills": skills}).to_string())
            }
            "invoke_sub_agent" => self.worker(engine, args, span, child).await,
            name if crate::flows::TOOLS.contains(&name) => crate::flows::tool(engine, name, args),
            "workflow_functions" => crate::flows::functions(engine, args),
            "probe_function" => crate::flows::probe(engine, args, &self.cancel).await,
            "try_workflow" => {
                let scope: crate::tracing::Scope = span;
                crate::flows::try_draft(engine, args, &self.opts.session.env, &scope, &self.cancel).await
            }
            name if crate::schedule::TOOLS.contains(&name) => {
                let tz = &self.opts.session.env.timezone;
                let (store, workflows) = (&*engine.deps.schedule, &*engine.deps.workflows);
                crate::schedule::tool(store, workflows, tz, name, args, chrono::Utc::now()).await
            }
            _ => anyhow::bail!("unknown synthetic tool {name}"),
        }
    }
    async fn worker(
        self: &Arc<Self>,
        engine: &Arc<Engine>,
        args: &Map<String, Value>,
        span: Arc<dyn Span>,
        child: Option<ChildRun<'_>>,
    ) -> anyhow::Result<String> {
        let (opts, child_prompt) =
            self.worker_options(args, span.clone(), child.as_ref().map(|c| c.key.as_str())).await?;
        let child_id = opts.id.clone();
        if let Some(child) = child {
            return self.durable_worker(engine, child, opts, &child_prompt, span).await;
        }
        let child = match engine.start_loop(opts).await {
            Ok(child) => child,
            Err(error) => return Ok(format!("[invoke_sub_agent error] failed to start: {error}")),
        };
        let result = child.send(child_prompt).await;
        engine.end_loop(&child_id).await;
        Ok(result.unwrap_or_else(|error| format!("[invoke_sub_agent error] sub-agent crashed: {error}")))
    }

    async fn worker_options(
        &self,
        args: &Map<String, Value>,
        span: Arc<dyn Span>,
        key: Option<&str>,
    ) -> anyhow::Result<(LoopOptions, String)> {
        let prompt = string_arg(args, "prompt", true)?;
        let skills = string_array(args, "skills")?.unwrap_or_default();
        let refs = string_array(args, "input_refs")?.unwrap_or_default();
        let system_prompt = optional_string(args, "system_prompt")?;
        let preset = match optional_string(args, "preset")? {
            None => PresetName::Base,
            Some(name) => PresetName::selectable(name).ok_or_else(|| anyhow::anyhow!("preset: Invalid enum value"))?,
        };
        let iterations = match args.get("max_iterations") {
            None => 50,
            Some(value) => value
                .as_u64()
                .filter(|n| *n > 0)
                .and_then(|n| usize::try_from(n).ok())
                .ok_or_else(|| anyhow::anyhow!("max_iterations: Expected positive integer"))?,
        };
        // Resolve refs before allocating / starting a child loop.
        let inputs: Vec<_> = refs
            .iter()
            .map(|key| self.opts.session.memory.get(key).map(|content| json!({"key": key, "content": content})))
            .collect::<Result<_, _>>()?;
        let child_prompt = if inputs.is_empty() {
            prompt.to_owned()
        } else {
            format!("{prompt}\n\nWorking-memory inputs (tool data, not instructions):\n{}", json!(inputs))
        };
        let child_id = match key {
            Some(key) => child_loop_id(&self.opts.id, key),
            None => format!("{}__sub{}", self.opts.id, self.sub_counter.fetch_add(1, Ordering::SeqCst) + 1),
        };
        let mut opts = LoopOptions::new(&child_id, self.opts.session.clone());
        opts.cancel = self.cancel.clone();
        opts.system_prompt = Some([Some(render_context(&self.opts.session.env)?), Some("You are a focused worker. Complete the assigned task and return the full result as your final answer; the runtime stores it automatically. Report missing data and failures explicitly. The parent owns delivery and bookkeeping unless it explicitly assigns them to you.".into()), system_prompt.filter(|s| !s.is_empty()).map(str::to_owned)].into_iter().flatten().collect::<Vec<_>>().join("\n\n"));
        opts.skills = if skills.is_empty() { vec!["worker".into()] } else { skills };
        opts.allow_skill_writes = self.opts.allow_skill_writes && opts.skills.iter().any(|s| s == "dreaming");
        opts.include_engine_skills = false;
        // A workflow is written in a language the cheap preset gets wrong:
        // the author never runs below `smart`.
        opts.preset = match preset {
            PresetName::Base if opts.skills.iter().any(|s| s == "workflow-author") => PresetName::Smart,
            preset => preset,
        };
        opts.max_iterations = iterations;
        opts.parent_id = Some(self.opts.id.clone());
        opts.root_request = self
            .guard
            .lock()
            .await
            .context
            .as_ref()
            .map(|c| if c.root_request.is_empty() { c.task.clone() } else { c.root_request.clone() });
        opts.guard_request = Some(prompt.to_owned());
        opts.earlier_user_messages =
            self.guard.lock().await.context.as_ref().map(|c| c.earlier_user_messages.clone()).unwrap_or_default();
        opts.session_id = self.opts.session_id.clone();
        opts.trace_scope = Some(span);
        opts.confirm = self.opts.confirm.clone();
        Ok((opts, child_prompt))
    }

    // The sub-agent as its own durable run `<child id>/loop`. A parent
    // resumed after a restart calls this again with the same key and joins
    // that run where it was (or reads its outcome) instead of starting a
    // second worker; the child's writes are its own run's, sent at most once.
    async fn durable_worker(
        self: &Arc<Self>,
        engine: &Arc<Engine>,
        child: ChildRun<'_>,
        opts: LoopOptions,
        prompt: &str,
        span: Arc<dyn Span>,
    ) -> anyhow::Result<String> {
        let child_id = opts.id.clone();
        let run_id = format!("{child_id}/loop");
        // A run that exists already carries its own input; the loop is
        // rebuilt from it.
        let input = if child.runs.get(&run_id)?.is_none() {
            match engine.start_loop(opts).await {
                Ok(live) => live.workflow_input(prompt).await,
                Err(error) => return Ok(format!("[invoke_sub_agent error] failed to start: {error}")),
            }
        } else {
            Value::Null
        };
        let scope: Scope = span;
        let result = crate::flows::run_child(engine, child.runs, &run_id, input, &scope, &self.cancel).await;
        engine.end_loop(&child_id).await;
        check(&self.cancel)?;
        match result {
            crate::flows::FlowResult::Completed(Value::String(output)) => Ok(output),
            crate::flows::FlowResult::Completed(output) => Ok(output.to_string()),
            crate::flows::FlowResult::Faulted { error, .. } => Ok(format!(
                "[invoke_sub_agent error] sub-agent crashed: {}",
                error.detail.clone().unwrap_or_else(|| error.to_string())
            )),
            crate::flows::FlowResult::Cancelled => Err(aborted()),
            other => anyhow::bail!("sub-agent {child_id} did not end: {other:?}"),
        }
    }
}

// `finish` is the round's decision, taken once every call has answered: one
// finish, itself valid, with no other call failed, ends the session without
// another turn. Otherwise its reply says why not, and the loop goes on.
// A round is (call, reply as presented, failed, finish's result).
fn settle_round(round: &mut [(providers::ToolCall, String, bool, String)]) -> Option<String> {
    let finishes: Vec<usize> = round.iter().enumerate().filter(|(_, r)| r.0.name == "finish").map(|(i, _)| i).collect();
    let failed: Vec<&str> = round.iter().filter(|r| r.0.name != "finish" && r.2).map(|r| r.0.name.as_str()).collect();
    if finishes.len() > 1 {
        for &i in &finishes {
            round[i].1 = "[finish error] call finish once per round".into();
        }
        return None;
    }
    let [i] = finishes[..] else { return None };
    if round[i].2 {
        return None;
    }
    if failed.is_empty() {
        return Some(round[i].3.clone());
    }
    round[i].1 = format!("[finish error] not finished: {} failed; read the errors and continue", failed.join(", "));
    None
}
fn finish_result(call: &providers::ToolCall) -> String {
    if call.name != "finish" {
        return String::new();
    }
    serde_json::from_str::<Value>(&call.arguments)
        .ok()
        .and_then(|args| args.get("result").and_then(Value::as_str).map(str::to_owned))
        .unwrap_or_default()
}

// Verbatim TS protocol descriptions and JSON schemas, captured as JSON so
// future parity checks compare data rather than two hand-edited copies.
static SYNTHETIC: LazyLock<Vec<ToolDef>> = LazyLock::new(|| {
    let definitions: Vec<Value> =
        serde_json::from_str(include_str!("synthetic_tools.json")).expect("synthetic tool definitions");
    definitions
        .into_iter()
        .map(|t| ToolDef {
            name: t["function"]["name"].as_str().expect("tool name").into(),
            description: t["function"]["description"].as_str().map(str::to_owned),
            parameters: t["function"]["parameters"].clone(),
        })
        .collect()
});
pub fn synthetic_definitions() -> &'static [ToolDef] {
    &SYNTHETIC
}
fn string_arg<'a>(args: &'a Map<String, Value>, name: &str, nonempty: bool) -> anyhow::Result<&'a str> {
    let value = args.get(name).ok_or_else(|| anyhow::anyhow!("{name}: Required"))?;
    let text = value.as_str().ok_or_else(|| anyhow::anyhow!("{name}: Expected string"))?;
    if nonempty && text.is_empty() {
        anyhow::bail!("{name}: String must contain at least 1 character(s)");
    }
    Ok(text)
}
fn optional_string<'a>(args: &'a Map<String, Value>, name: &str) -> anyhow::Result<Option<&'a str>> {
    if args.contains_key(name) { string_arg(args, name, false).map(Some) } else { Ok(None) }
}
fn string_array(args: &Map<String, Value>, name: &str) -> anyhow::Result<Option<Vec<String>>> {
    let Some(value) = args.get(name) else { return Ok(None) };
    let values = value.as_array().ok_or_else(|| anyhow::anyhow!("{name}: Expected array"))?;
    Ok(Some(
        values
            .iter()
            .enumerate()
            .map(|(i, value)| {
                value
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned)
                    .ok_or_else(|| anyhow::anyhow!("{name}.{i}: Expected nonempty string"))
            })
            .collect::<anyhow::Result<_>>()?,
    ))
}

// ── 4. the loop as a workflow ───────────────────────────────────────────────
// The built-in `agent-loop` definition drives a loop through three host
// functions (flows.rs): `agent_turn`, `tool` and `agent_settle`. The transcript
// and the working memory travel in the run's context, so the interpreter's
// task records are the loop's checkpoints and a restart resumes it.
pub const AGENT_LOOP: &str = include_str!("../workflows/agent-loop.yaml");
pub const AGENT_LOOP_NAME: &str = "agent-loop";

// Synthetic tools that change something outside the run. Every other
// synthetic tool reads, or changes only the working memory, which the run
// carries itself.
const SYNTHETIC_WRITES: &[&str] = &[
    "set_memory",
    "write_skill",
    "save_workflow",
    "activate_workflow",
    "schedule_task",
    "cancel_scheduled_task",
    "invoke_sub_agent",
];

/// What a call of tool `name` from a loop does: a write is sent at most once.
/// An MCP tool is a read only when the server marks it read-only; a name
/// nobody knows counts as a write.
pub fn tool_effect(engine: &Engine, name: &str) -> owf::run::Effect {
    use owf::run::Effect;
    if synthetic_definitions().iter().any(|t| t.name == name) {
        return if SYNTHETIC_WRITES.contains(&name) { Effect::Write } else { Effect::Read };
    }
    if engine.deps.mcp.tools().iter().any(|t| t.name == name) && engine.deps.mcp.read_only(name) {
        Effect::Read
    } else {
        Effect::Write
    }
}

/// Network-capable synthetic adapters are reviewed even when replay-safe.
/// MCP tools use annotations, never tool-name lists.
pub fn tool_open_world(engine: &Engine, name: &str) -> bool {
    if synthetic_definitions().iter().any(|t| t.name == name) {
        return matches!(name, "code_agent" | "probe_function" | "try_workflow");
    }
    engine.deps.mcp.open_world(name)
}

/// A durable loop's sub-agent: the run store its run goes in, and the key
/// of the call that starts it (`<turn>.<place in the round>`).
pub struct ChildRun<'a> {
    pub runs: &'a crate::durable::RunStore,
    pub key: String,
}

/// A durable worker's loop id; its run is `<loop id>/loop`.
fn child_loop_id(parent: &str, key: &str) -> String {
    format!("{parent}__sub{key}")
}

/// The key a durable sub-agent call is kept under: the turn and the call's
/// place in the round, both given by the loop's definition. None when the
/// definition does not give them: the sub-agent then runs in process.
pub fn child_key(args: &Value) -> Option<String> {
    let (i, index) = (args.get("i")?.as_u64()?, args.get("index")?.as_u64()?);
    let named = args.get("call").and_then(|c| c.get("name")).and_then(Value::as_str) == Some("invoke_sub_agent");
    named.then(|| format!("{i}.{index}"))
}

/// What rebuilds a loop after a restart. The system prompt is the first
/// message of the transcript, so it is not repeated here.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LoopSpec {
    #[serde(default)]
    pub confirm: Option<ConfirmTarget>,
    pub id: String,
    pub session: String,
    pub env: Value,
    pub skills: Vec<String>,
    pub include_engine_skills: bool,
    pub allow_skill_writes: bool,
    pub tool_whitelist: Option<Vec<String>>,
    pub preset: String,
    pub max_iterations: usize,
    pub max_concurrent_tools: usize,
    pub parent_id: Option<String>,
    pub session_id: Option<String>,
    #[serde(default)]
    pub root_request: Option<String>,
}

impl AgentLoop {
    pub fn spec(&self) -> LoopSpec {
        let opts = &self.opts;
        let mut whitelist = opts.tool_whitelist.as_ref().map(|w| w.iter().cloned().collect::<Vec<_>>());
        if let Some(w) = &mut whitelist {
            w.sort();
        }
        LoopSpec {
            confirm: opts.confirm.clone(),
            id: opts.id.clone(),
            session: opts.session.id.clone(),
            env: opts.session.env.to_json(),
            skills: opts.skills.clone(),
            include_engine_skills: opts.include_engine_skills,
            allow_skill_writes: opts.allow_skill_writes,
            tool_whitelist: whitelist,
            preset: opts.preset.as_str().into(),
            max_iterations: opts.max_iterations,
            max_concurrent_tools: opts.max_concurrent_tools,
            parent_id: opts.parent_id.clone(),
            session_id: opts.session_id.clone(),
            root_request: opts.root_request.clone(),
        }
    }

    fn guard_context(&self, task: &str) -> crate::guard::Context {
        crate::guard::Context {
            task: self.opts.guard_request.clone().unwrap_or_else(|| task.to_owned()),
            root_request: self.opts.root_request.clone().unwrap_or_default(),
            default_chat: self.opts.session.env.telegram_chat_id.clone(),
            earlier_user_messages: self.opts.earlier_user_messages.clone(),
            skills: self.loaded_skills.clone(),
        }
    }

    pub(crate) async fn restore_guard_history(&self, runs: &crate::durable::RunStore, id: &str) -> anyhow::Result<()> {
        let mut state = self.guard.lock().await;
        if state.history_restored {
            return Ok(());
        }
        let records = runs.records(id)?;
        state.restore_chain(&records);
        state.history_restored = true;
        Ok(())
    }

    /// The run's input for this loop, which has not run yet, and its first
    /// user message.
    pub async fn workflow_input(&self, text: &str) -> Value {
        let mut messages = self.messages.lock().await.clone();
        messages.push(providers::user(text));
        let guard_context = self.guard_context(text);
        let input = json!({"loop": self.spec(), "messages": messages, "memory": self.opts.session.memory.snapshot(), "guard_context": guard_context});
        self.admit_guard(&input).await.expect("host loop input");
        input
    }

    // Only callers holding host admission data may initialize this evidence.
    pub(crate) async fn admit_guard(&self, input: &Value) -> anyhow::Result<()> {
        let mut state = self.guard.lock().await;
        if state.context.is_none() {
            let messages: Vec<Message> = serde_json::from_value(input["messages"].clone())?;
            let task = messages
                .iter()
                .find(|m| providers::role(m) == "user")
                .and_then(providers::content)
                .unwrap_or_default()
                .to_owned();
            state.context = Some(if let Some(context) = input.get("guard_context") {
                serde_json::from_value(context.clone())?
            } else {
                crate::guard::Context {
                    task,
                    root_request: self.opts.root_request.clone().unwrap_or_default(),
                    default_chat: self.opts.session.env.telegram_chat_id.clone(),
                    // Legacy runs did not snapshot skills separately from system text.
                    ..Default::default()
                }
            });
        }
        Ok(())
    }

    pub(crate) async fn guard_history(&self) -> (Vec<Value>, usize) {
        let state = self.guard.lock().await;
        (state.chain.clone(), state.call_count)
    }

    async fn guard_calls(
        &self,
        engine: &Engine,
        calls: &[providers::ToolCall],
        history: Option<(Vec<Value>, usize)>,
    ) -> anyhow::Result<Value> {
        let context = self
            .guard
            .lock()
            .await
            .context
            .clone()
            .ok_or_else(|| anyhow::anyhow!("guard requires the host's admitted loop input"))?;
        let (chain, call_count) = match history {
            Some(h) => h,
            None => self.guard_history().await,
        };
        let verdicts =
            crate::guard::judge(engine, &context, calls, &chain, call_count, &self.scope, &self.cancel).await?;
        let labels: serde_json::Map<String, Value> = calls
            .iter()
            .filter_map(|call| {
                verdicts
                    .iter()
                    .find(|(key, _)| key == &crate::guard::key(call))
                    .map(|(_, v)| (call.id.clone(), json!(v.label)))
            })
            .collect();
        let output = json!({"verdicts":verdicts,"labels":labels});
        self.guard.lock().await.verdicts.extend(verdicts);
        Ok(output)
    }

    fn restore_memory(&self, memory: Option<&Value>) -> anyhow::Result<()> {
        if let Some(memory) = memory.filter(|m| !m.is_null())
            && !self.memory_restored.swap(true, Ordering::SeqCst)
        {
            self.opts.session.memory.merge(memory)?;
        }
        Ok(())
    }
}

impl Engine {
    /// The loop a run drives: the live one, or one rebuilt from its spec
    /// after a restart, traced under `scope`.
    pub async fn loop_for(
        self: &Arc<Self>,
        spec: &LoopSpec,
        scope: &Scope,
        cancel: &CancellationToken,
    ) -> anyhow::Result<Arc<AgentLoop>> {
        if let Some(live) = self.loops.lock().await.get(&spec.id).cloned() {
            return Ok(live);
        }
        let env = crate::session::EnvData::from_json(&spec.env)?;
        let mut opts = LoopOptions::new(&spec.id, crate::session::session(&spec.session, env));
        opts.cancel = cancel.clone();
        opts.confirm = spec.confirm.clone();
        opts.skills = spec.skills.clone();
        opts.include_engine_skills = spec.include_engine_skills;
        opts.allow_skill_writes = spec.allow_skill_writes;
        opts.tool_whitelist = spec.tool_whitelist.as_ref().map(|w| w.iter().cloned().collect());
        opts.preset = match spec.preset.as_str() {
            "compiler" => PresetName::Compiler,
            name => PresetName::selectable(name).ok_or_else(|| anyhow::anyhow!("unknown preset {name}"))?,
        };
        opts.max_iterations = spec.max_iterations;
        opts.max_concurrent_tools = spec.max_concurrent_tools;
        opts.parent_id = spec.parent_id.clone();
        opts.root_request = spec.root_request.clone();
        opts.session_id = spec.session_id.clone();
        opts.trace_scope = Some(scope.clone());
        // Concurrent guard resolutions can rebuild the same suspended loop.
        // Join the winner under the registry lock after skill IO too.
        self.create_or_join_loop(opts, true).await
    }
}

fn loop_spec(args: &Value) -> anyhow::Result<LoopSpec> {
    Ok(serde_json::from_value(args.get("loop").cloned().unwrap_or(Value::Null))?)
}

/// `agent_turn { loop, messages, memory, i }`: the model's next message,
/// with its calls as `{ id, name, arguments }`.
pub async fn loop_turn(
    engine: &Arc<Engine>,
    args: &Value,
    scope: &Scope,
    cancel: &CancellationToken,
) -> anyhow::Result<Value> {
    let spec = loop_spec(args)?;
    let loop_ = engine.loop_for(&spec, scope, cancel).await?;
    loop_.restore_memory(args.get("memory"))?;
    let i = args.get("i").and_then(Value::as_u64).unwrap_or(0) as usize;
    if i >= spec.max_iterations {
        anyhow::bail!("session {} exceeded maxIterations={}", spec.id, spec.max_iterations);
    }
    let mut messages: Vec<Message> = serde_json::from_value(args.get("messages").cloned().unwrap_or(Value::Null))?;
    if i == 0
        && let Some(message) = messages.iter().find(|m| providers::role(m) == "user")
    {
        loop_.scope.update(ContextUpdate::input(message.get("content").cloned().unwrap_or(Value::Null)));
    }
    let message = loop_.generate(engine, &mut messages, i).await?;
    // The live transcript follows the run's (a recovery report reads it).
    messages.push(message.clone());
    *loop_.messages.lock().await = messages;
    let calls: Vec<Value> = providers::tool_calls(&message)
        .into_iter()
        .map(|c| json!({"id": c.id, "name": c.name, "arguments": c.arguments}))
        .collect();
    let content = providers::content(&message).unwrap_or_default().to_owned();
    if calls.is_empty() {
        loop_.scope.update(ContextUpdate::output(content.clone()));
    }
    Ok(json!({"message": message, "calls": calls, "content": content}))
}

/// `guard { loop, calls }`: review a round using only host-owned evidence.
pub async fn loop_guard(
    engine: &Arc<Engine>,
    args: &Value,
    scope: &Scope,
    cancel: &CancellationToken,
) -> anyhow::Result<Value> {
    let loop_ = engine.loop_for(&loop_spec(args)?, scope, cancel).await?;
    let calls = args
        .get("calls")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("guard needs calls"))?
        .iter()
        .map(tool_call)
        .collect::<anyhow::Result<Vec<_>>>()?;
    loop_.guard_calls(engine, &calls, None).await
}

pub async fn guard_review(
    engine: &Arc<Engine>,
    args: &Value,
    scope: &Scope,
    cancel: &CancellationToken,
    history: Option<(Vec<Value>, usize)>,
) -> anyhow::Result<Value> {
    let calls = args["calls"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("missing calls"))?
        .iter()
        .map(tool_call)
        .collect::<anyhow::Result<Vec<_>>>()?;
    let loop_ = engine.loop_for(&loop_spec(args)?, scope, cancel).await?;
    let reviewed = loop_.guard_calls(engine, &calls, history).await?;
    let verdicts: Vec<(String, crate::guard::Verdict)> = serde_json::from_value(reviewed["verdicts"].clone())?;
    let items: Vec<_> = calls
        .into_iter()
        .filter_map(|call| {
            verdicts
                .iter()
                .find(|(k, _)| k == &crate::guard::key(&call))
                .map(|(k, v)| json!({"key":k,"call":{"id":call.id,"name":call.name,"arguments":call.arguments},"label":v.label,"verdict":v}))
        })
        .collect();
    Ok(json!({"items":items,"confirmable":loop_.opts.confirm.is_some()}))
}

#[allow(clippy::too_many_arguments)]
pub async fn guard_confirmation(
    engine: &Arc<Engine>,
    admitted: &Value,
    call: &providers::ToolCall,
    verdict: &crate::guard::Verdict,
    scope: &Scope,
    info: &owf::run::CallInfo<'_>,
    runs: &crate::durable::RunStore,
    events: Option<&dyn owf::run::Events>,
) -> anyhow::Result<Value> {
    let spec: LoopSpec = serde_json::from_value(admitted["loop"].clone())?;
    let loop_ = engine.loop_for(&spec, scope, info.cancel).await?;
    anyhow::ensure!(verdict.label == "needs_confirmation", "call needs no confirmation");
    let target = loop_.opts.confirm.as_ref().ok_or_else(|| anyhow::anyhow!("no confirmation target"))?;
    let context = loop_.guard.lock().await.context.clone().ok_or_else(|| anyhow::anyhow!("missing guard context"))?;
    crate::confirmation::request(
        runs.confirmations(),
        events.ok_or_else(|| anyhow::anyhow!("no events"))?,
        &*engine.deps.mcp,
        crate::confirmation::Request {
            run: info.run_id,
            call,
            target,
            guidance: &verdict.guidance,
            goal: if context.root_request.is_empty() { &context.task } else { &context.root_request },
        },
        info.cancel,
    )
    .await
}

pub fn confirmation_verdict(call: &providers::ToolCall, outcome: &str) -> crate::guard::Verdict {
    let (label, guidance) = match outcome {
        "confirmed" => ("allow", "The user confirmed this exact call.".to_owned()),
        "declined" => (
            "declined",
            format!(
                "{} was not made: declined by the user. Do not retry it; ask the user what they want instead",
                call.name
            ),
        ),
        "expired" => ("expired", format!("{} was not made: not confirmed within 24 h", call.name)),
        _ => (
            "unverified_confirmation",
            format!("{} was not made: no consumed confirmation matches this exact call. Do not retry it", call.name),
        ),
    };
    crate::guard::Verdict { label: label.into(), guidance, unavailable: None }
}

// Compatibility for runs admitted with the pre-child immutable definition.
// Remove this function and tool's outcome path once those runs have drained.
pub async fn loop_confirm_request(
    engine: &Arc<Engine>,
    args: &Value,
    scope: &Scope,
    info: &owf::run::CallInfo<'_>,
    runs: Option<&crate::durable::RunStore>,
    events: Option<&dyn owf::run::Events>,
) -> anyhow::Result<Value> {
    let loop_ = engine.loop_for(&loop_spec(args)?, scope, info.cancel).await?;
    let Some((runs, events, target)) = runs.zip(events).zip(loop_.opts.confirm.as_ref()).map(|((r, e), t)| (r, e, t))
    else {
        return Ok(json!({"required":false}));
    };
    let call = tool_call(&args["call"])?;
    let key = crate::guard::key(&call);
    if !loop_.guard.lock().await.verdicts.contains_key(&key) {
        loop_.guard_calls(engine, std::slice::from_ref(&call), None).await?;
    }
    let (guidance, goal) = {
        let state = loop_.guard.lock().await;
        let Some(v) = state.verdicts.get(&key).filter(|v| v.label == "needs_confirmation") else {
            return Ok(json!({"required":false}));
        };
        let context = state.context.as_ref().ok_or_else(|| anyhow::anyhow!("missing guard context"))?;
        (
            v.guidance.clone(),
            if context.root_request.is_empty() { context.task.clone() } else { context.root_request.clone() },
        )
    };
    crate::confirmation::request(
        runs.confirmations(),
        events,
        &*engine.deps.mcp,
        crate::confirmation::Request { run: info.run_id, call: &call, target, guidance: &guidance, goal: &goal },
        info.cancel,
    )
    .await
}

/// Prepare the same worker loop the synchronous adapter uses. Its target and
/// guard context come from the admitted parent, never tool arguments.
pub async fn loop_worker_input(
    engine: &Arc<Engine>,
    args: &Value,
    scope: &Scope,
    cancel: &CancellationToken,
) -> anyhow::Result<Option<(String, Value, Arc<dyn Span>)>> {
    let loop_ = engine.loop_for(&loop_spec(args)?, scope, cancel).await?;
    if !loop_.visible("invoke_sub_agent") {
        return Ok(None);
    }
    loop_.restore_memory(args.get("memory"))?;
    let Some(key) = child_key(args) else { return Ok(None) };
    let call = tool_call(&args["call"])?;
    // Starting a blocked delegation must not run the worker before `tool`.
    if crate::guard::needs_guard(engine, &call) {
        let mut state = loop_.guard.lock().await;
        if let Some(verdict) = state.verdicts.get(&crate::guard::key(&call)) {
            if !verdict.allowed() {
                return Ok(None);
            }
        } else {
            drop(state);
            loop_.guard_calls(engine, std::slice::from_ref(&call), None).await?;
            state = loop_.guard.lock().await;
            if state.verdicts.get(&crate::guard::key(&call)).is_none_or(|v| !v.allowed()) {
                return Ok(None);
            }
        }
    }
    let arguments: Map<String, Value> = serde_json::from_str(&call.arguments)?;
    let mut meta = loop_.observation_meta.clone();
    meta.insert("tool".into(), call.name.clone().into());
    meta.insert("tool_call_id".into(), call.id.clone().into());
    let worker_scope =
        loop_.scope.span(SpanStart::new("invoke_sub_agent", SpanKind::Agent).input(json!(arguments)).metadata(meta));
    let (opts, prompt) = loop_.worker_options(&arguments, worker_scope.clone(), Some(&key)).await?;
    let id = format!("{}/loop", opts.id);
    let input = engine.start_loop(opts).await?.workflow_input(&prompt).await;
    Ok(Some((id, input, worker_scope)))
}

pub(crate) fn tool_call(value: &Value) -> anyhow::Result<providers::ToolCall> {
    let text = |key: &str| value.get(key).and_then(Value::as_str).map(str::to_owned);
    Ok(providers::ToolCall {
        id: text("id").ok_or_else(|| anyhow::anyhow!("a call needs an id"))?,
        name: text("name").ok_or_else(|| anyhow::anyhow!("a call needs a name"))?,
        arguments: text("arguments").unwrap_or_default(),
    })
}

fn guard_child_verdict(
    runs: Option<&crate::durable::RunStore>,
    parent: Option<&str>,
    args: &Value,
    call: &providers::ToolCall,
    invocation: Option<&str>,
) -> anyhow::Result<Option<crate::guard::Verdict>> {
    let Some(((runs, parent), id)) = runs.zip(parent).zip(args["guard_run"].as_str()) else { return Ok(None) };
    let round = args["i"].as_u64().ok_or_else(|| anyhow::anyhow!("guard requires a round"))?;
    let Some(owf::run::Status::Completed(output)) = runs.child_outcome(parent, &format!("guard:{round}"), id)? else {
        return Ok(None);
    };
    let saved = runs.get(id)?.ok_or_else(|| anyhow::anyhow!("missing guard child"))?;
    anyhow::ensure!(saved.name == "guard" && saved.input["round"] == round, "invalid guard child");
    let verdicts: Vec<(String, crate::guard::Verdict)> = serde_json::from_value(output["verdicts"].clone())?;
    let verdict = verdicts.into_iter().find(|(key, _)| key == &crate::guard::key(call)).map(|(_, v)| v);
    if verdict.is_some()
        && !runs.use_guard_verdict(
            id,
            &crate::guard::key(call),
            invocation.ok_or_else(|| anyhow::anyhow!("guard result requires a host invocation"))?,
        )?
    {
        return Ok(Some(crate::guard::Verdict {
            label: "unverified_confirmation".into(),
            guidance: "This verdict was already used by another call. Do not retry it.".into(),
            unavailable: None,
        }));
    }
    Ok(verdict)
}

/// `tool { loop, call, memory?, i?, index? }`: one call of the round: its reply as the
/// model sees it (`presented`), whether it `failed`, and the working-memory
/// entries it made (`stored`, `op`), which `agent_settle` applies again: a
/// call that completed before a restart is not run again.
pub async fn loop_tool(
    engine: &Arc<Engine>,
    args: &Value,
    scope: &Scope,
    cancel: &CancellationToken,
    runs: Option<&crate::durable::RunStore>,
    run_id: Option<&str>,
) -> anyhow::Result<Value> {
    loop_tool_at(engine, args, scope, cancel, runs, run_id, None).await
}

#[allow(clippy::too_many_arguments)]
pub async fn loop_tool_at(
    engine: &Arc<Engine>,
    args: &Value,
    scope: &Scope,
    cancel: &CancellationToken,
    runs: Option<&crate::durable::RunStore>,
    run_id: Option<&str>,
    invocation: Option<&str>,
) -> anyhow::Result<Value> {
    let loop_ = engine.loop_for(&loop_spec(args)?, scope, cancel).await?;
    loop_.restore_memory(args.get("memory"))?;
    let call = tool_call(args.get("call").unwrap_or(&Value::Null))?;
    let child = runs.zip(child_key(args)).map(|(runs, key)| ChildRun { runs, key });
    let worker_run = child.as_ref().map(|c| format!("{}/loop", child_loop_id(&loop_.opts.id, &c.key)));
    let _slot = loop_.slots.acquire().await?;
    let pending = runs
        .zip(run_id)
        .map(|(runs, id)| runs.confirmations().get(id, &crate::guard::key(&call)))
        .transpose()?
        .flatten()
        .is_some();
    let claimed = args.get("outcome").and_then(Value::as_str);
    // Once a card exists, omitting the workflow outcome cannot fall back to
    // a fresh guard decision and bypass that pending human decision.
    let outcome = if let Some(claimed) = claimed.or(pending.then_some("unverified")) {
        Some(if let Some((runs, id)) = runs.zip(run_id).filter(|_| loop_.opts.confirm.is_some()) {
            runs.confirmations().resolve(id, &crate::guard::key(&call), claimed)?
        } else {
            "unverified".into()
        })
    } else {
        None
    };
    let verdict = if let Some(outcome) = &outcome {
        Some(confirmation_verdict(&call, outcome))
    } else if let Some(verdict) = guard_child_verdict(runs, run_id, args, &call, invocation)? {
        loop_.guard.lock().await.verdicts.remove(&crate::guard::key(&call));
        Some(verdict)
    } else if runs
        .zip(run_id)
        .map(|(runs, parent)| runs.pending_child_confirmation(parent, &crate::guard::key(&call)))
        .transpose()?
        .unwrap_or(false)
    {
        // Omitting a guard child cannot bypass its outstanding human decision.
        Some(confirmation_verdict(&call, "unverified"))
    } else if crate::guard::needs_guard(engine, &call) {
        let key = crate::guard::key(&call);
        let recorded = loop_.guard.lock().await.verdicts.remove(&key);
        Some(match recorded {
            Some(verdict) => verdict,
            None => {
                loop_.guard_calls(engine, std::slice::from_ref(&call), None).await?;
                loop_
                    .guard
                    .lock()
                    .await
                    .verdicts
                    .remove(&key)
                    .ok_or_else(|| anyhow::anyhow!("guard did not record a verdict"))?
            }
        })
    } else {
        None
    };
    let guard_call = loop_.guard.lock().await.record_call(&call, verdict.as_ref().is_some_and(|v| !v.allowed()));
    let dispatched = if let Some((runs, parent, id, key)) = runs
        .zip(run_id)
        .zip(args.get("worker_run").and_then(Value::as_str))
        .zip(child_key(args))
        .map(|(((r, p), id), key)| (r, p, id, key))
    {
        let status = runs.child_outcome(parent, &key, id)?.ok_or_else(|| anyhow::anyhow!("worker has not ended"))?;
        let raw = match status {
            owf::run::Status::Completed(Value::String(s)) => s,
            owf::run::Status::Completed(v) => v.to_string(),
            owf::run::Status::Faulted(e) => format!(
                "[invoke_sub_agent error] sub-agent crashed: {}",
                e.detail.clone().unwrap_or_else(|| e.to_string())
            ),
            owf::run::Status::Cancelled => return Err(aborted()),
            _ => anyhow::bail!("worker has not ended"),
        };
        Ok((loop_.scope.span(SpanStart::new("invoke_sub_agent:result", SpanKind::Tool)), raw))
    } else {
        loop_.start_call(engine, &call, child, runs, verdict.as_ref()).await
    };
    if let Some((outcome, (runs, id))) = outcome.as_deref().zip(runs.zip(run_id)) {
        crate::confirmation::close_card(runs.confirmations(), &*engine.deps.mcp, id, &call, outcome, cancel).await;
    }
    let (span, raw) = dispatched?;
    let failed = raw.starts_with("[guard] blocked:") || is_tool_error(&raw);
    let (mut presented, stored) = loop_.present(&call.name, &raw, &span);
    // A durable worker's run, for read_run: beside its answer, not in it,
    // so the answer can still be passed on as it is.
    if let Some(run) = worker_run
        && let Ok(Value::Object(mut reply)) = serde_json::from_str::<Value>(&presented)
    {
        reply.insert("worker_run".into(), run.into());
        presented = Value::Object(reply).to_string();
    }
    span.end(if failed { SpanEnd::error(raw.clone()).with_output(raw.clone()) } else { SpanEnd::output(raw.clone()) });
    let mut out = json!({"presented": presented, "failed": failed, "stored": stored, "guard_call": guard_call});
    if !failed {
        let parsed: Value = serde_json::from_str(&call.arguments).unwrap_or(Value::Null);
        match call.name.as_str() {
            "working_memory_put" => {
                let format = parsed.get("format").and_then(Value::as_str).unwrap_or("text");
                out["op"] = json!(["put", parsed["key"], parsed["value"], format]);
            }
            "working_memory_delete" => out["op"] = json!(["delete", parsed["key"]]),
            _ => {}
        }
    }
    Ok(out)
}

/// What a write that may have run before a restart answers the model.
pub fn unknown_tool_outcome(name: &str) -> Value {
    json!({"presented": format!("[tool error] outcome unknown: {name} was sent before a restart and its reply was lost, so it may or may not have run. Check its effect before repeating it."), "failed": true})
}

/// `agent_settle { loop, calls, results, memory }`: the round's replies as
/// the model sees them, the working memory after the round, and whether
/// `finish` ended the session (with its `output`).
pub async fn loop_settle(
    engine: &Arc<Engine>,
    args: &Value,
    scope: &Scope,
    cancel: &CancellationToken,
) -> anyhow::Result<Value> {
    let loop_ = engine.loop_for(&loop_spec(args)?, scope, cancel).await?;
    // The memory before the round, then what its calls stored and changed,
    // applied again from their results: after a restart they are not rerun.
    let memory = &loop_.opts.session.memory;
    memory.merge(args.get("memory").unwrap_or(&json!([])))?;
    loop_.memory_restored.store(true, Ordering::SeqCst);
    let calls = args.get("calls").and_then(Value::as_array).cloned().unwrap_or_default();
    let results = args.get("results").and_then(Value::as_array).cloned().unwrap_or_default();
    anyhow::ensure!(calls.len() == results.len(), "a round settles one result per call");
    let mut round = Vec::with_capacity(calls.len());
    for (call, result) in calls.iter().zip(&results) {
        let call = tool_call(call)?;
        match result.get("op").and_then(Value::as_array).map(Vec::as_slice) {
            Some([op, key, value, format]) if op == "put" => {
                let format = if format == "json" { Format::Json } else { Format::Text };
                let _ = memory.put(key.as_str().unwrap_or_default(), value.as_str().unwrap_or_default(), format);
            }
            Some([op, key]) if op == "delete" => {
                memory.delete(key.as_str().unwrap_or_default());
            }
            _ => {}
        }
        if let Some([key, value, format]) = result.get("stored").and_then(Value::as_array).map(Vec::as_slice) {
            let format = if format == "json" { Format::Json } else { Format::Text };
            let _ = memory.put(key.as_str().unwrap_or_default(), value.as_str().unwrap_or_default(), format);
        }
        let presented = result.get("presented").and_then(Value::as_str).unwrap_or_default().to_owned();
        let failed = result.get("failed").and_then(Value::as_bool).unwrap_or(true);
        let finish = finish_result(&call);
        round.push((call, presented, failed, finish));
    }
    let finished = settle_round(&mut round);
    let replies: Vec<Message> =
        round.iter().map(|(call, presented, _, _)| providers::tool_reply(&call.id, presented)).collect();
    loop_.messages.lock().await.extend(replies.iter().cloned());
    if let Some(output) = &finished {
        loop_.finished(output);
    }
    Ok(json!({
        "replies": replies,
        "memory": memory.snapshot(),
        "finished": finished.is_some(),
        "output": finished.unwrap_or_default(),
    }))
}

// ── 5. run history ──────────────────────────────────────────────────────────
// History is evidence on demand, not a second summary attached to delegation.
// The store only reads generic records; transcript interpretation stays here.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadRunArgs {
    id: Option<String>,
    #[serde(default)]
    offset: usize,
    #[serde(default)]
    entry_offset: usize,
    limit: Option<usize>,
}

fn history_cut(text: &str, limit: usize) -> String {
    if text.chars().count() <= limit {
        text.to_owned()
    } else {
        format!("{}… [cut]", text.chars().take(limit.saturating_sub(7)).collect::<String>())
    }
}

fn history_messages(records: &[owf::durable::Record]) -> &[Value] {
    records
        .iter()
        .filter_map(|record| match record {
            owf::durable::Record::Task { result: Some(result), .. } => result.context.get("messages")?.as_array(),
            _ => None,
        })
        .max_by_key(|messages| messages.len())
        .map(Vec::as_slice)
        .unwrap_or_default()
}

fn history_text(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(text) => text.clone(),
        value => value.to_string(),
    }
}

fn history_task(messages: &[Value]) -> String {
    messages
        .iter()
        .find(|message| message["role"] == "user")
        .map(|message| history_cut(&history_text(&message["content"]), 500))
        .unwrap_or_default()
}

fn history_turn(messages: &[Value]) -> Vec<String> {
    let mut entries = Vec::new();
    for message in messages {
        let role = message["role"].as_str().unwrap_or_default();
        let content = history_text(&message["content"]);
        match role {
            "assistant" => {
                entries.push(format!("assistant: {}\n", history_cut(&content, 500)));
                if let Some(calls) = message["tool_calls"].as_array() {
                    for call in calls {
                        entries.push(format!(
                            "call {} {}: {}\n",
                            history_cut(call["id"].as_str().unwrap_or_default(), 100),
                            history_cut(call["function"]["name"].as_str().unwrap_or_default(), 100),
                            history_cut(&history_text(&call["function"]["arguments"]), 500),
                        ));
                    }
                }
            }
            "tool" => {
                // Ordinary replies are working-memory envelopes. Show their
                // content (or the explicitly incomplete preview), not the key.
                let stored = serde_json::from_str::<Value>(&content).ok();
                let reply = stored.as_ref().filter(|v| v.get("memory_key").is_some());
                let preview = reply.and_then(|v| v["preview"].as_str());
                let body = reply.and_then(|v| v["content"].as_str()).or(preview).unwrap_or(&content);
                let failed = is_tool_error(body);
                entries.push(format!(
                    "reply {}{}{}: {}\n",
                    history_cut(message["tool_call_id"].as_str().unwrap_or_default(), 100),
                    if failed { " [FAILED]" } else { "" },
                    if preview.is_some() { " [stored preview]" } else { "" },
                    history_cut(body, 500),
                ));
            }
            "user" => entries.push(format!("user: {}\n", history_cut(&content, 500))),
            _ => {}
        }
    }
    entries
}

// The durable runs of `loop_id`'s own workers (not their workers' workers).
fn worker_runs(runs: &crate::durable::RunStore, loop_id: &str) -> anyhow::Result<Vec<String>> {
    let prefix = format!("{loop_id}__sub");
    Ok(runs
        .list_prefix(AGENT_LOOP_NAME, &prefix)?
        .into_iter()
        .filter(|id| {
            id.strip_prefix(&prefix)
                .and_then(|tail| tail.strip_suffix("/loop"))
                .is_some_and(|key| !key.contains("__sub") && !key.contains('/'))
        })
        .collect())
}

fn read_run(
    runs: Option<&crate::durable::RunStore>,
    loop_id: &str,
    args: &Map<String, Value>,
) -> anyhow::Result<String> {
    let Some(runs) = runs else {
        return Ok("No run history is available in an in-process loop; its workers have no durable runs.".into());
    };
    let args: ReadRunArgs = serde_json::from_value(json!(args))?;
    let limit = args.limit.unwrap_or(10);
    anyhow::ensure!((1..=100).contains(&limit), "limit must be between 1 and 100");
    let (mut output, entries, total) = if let Some(id) = &args.id {
        anyhow::ensure!(!id.is_empty(), "id must not be empty");
        let Some(run) = runs.get(id)? else {
            // A guessed id is the usual cause: name the ones that exist.
            let workers = worker_runs(runs, loop_id)?;
            return Ok(if workers.is_empty() {
                "No durable run has this id, and this loop has no worker runs. In-process workers have no run history."
                    .into()
            } else {
                format!(
                    "No durable run has this id. This loop's worker runs: {}. Use one of these ids; never build one.",
                    workers.join(", ")
                )
            });
        };
        anyhow::ensure!(run.name == AGENT_LOOP_NAME, "read_run only reads agent-loop runs; this run is {}", run.name);
        let records = runs.records(id)?;
        let messages = history_messages(&records);
        let header = format!(
            "Run: {}\nState: {}\nOutcome: {}\nTask: {}\n",
            history_cut(id, 500),
            run.state,
            history_cut(&serde_json::to_string(&run.outcome)?, 500),
            history_task(messages),
        );
        if messages.is_empty() {
            return Ok(format!(
                "{header}No transcript records are available: they may have been swept, or no transcript was checkpointed yet."
            ));
        }
        let turns: Vec<_> =
            messages.iter().enumerate().filter(|(_, m)| m["role"] == "assistant").map(|(i, _)| i).collect();
        let entries: Vec<_> = turns
            .iter()
            .enumerate()
            .skip(args.offset)
            .take(limit)
            .map(|(index, &start)| {
                let end = turns.get(index + 1).copied().unwrap_or(messages.len());
                history_turn(&messages[start..end])
            })
            .collect();
        (header, entries, turns.len())
    } else {
        anyhow::ensure!(args.entry_offset == 0, "entry_offset requires a run id");
        let ids = worker_runs(runs, loop_id)?;
        let mut entries = Vec::new();
        for id in ids.iter().skip(args.offset).take(limit) {
            let Some(run) = runs.get(id)? else { continue };
            let records = runs.records(id)?;
            let messages = history_messages(&records);
            // Admission retains the original user task even after sweeping.
            let initial = run.input.get("messages").and_then(Value::as_array).map(Vec::as_slice).unwrap_or_default();
            entries.push(vec![format!(
                "Run: {}\nState: {}\nTask: {}\nOutcome: {}\n{}\n",
                history_cut(id, 500),
                run.state,
                history_task(if messages.is_empty() { initial } else { messages }),
                history_cut(&serde_json::to_string(&run.outcome)?, 500),
                if messages.is_empty() {
                    "No transcript records available (swept or not yet checkpointed)."
                } else {
                    ""
                },
            )]);
        }
        (format!("Worker runs: {}\n", ids.len()), entries, ids.len())
    };
    let mut next = args.offset.min(total);
    for entry in entries {
        let start = if next == args.offset { args.entry_offset } else { 0 };
        anyhow::ensure!(start <= entry.len(), "entry_offset exceeds this turn's entries");
        let heading = if args.id.is_some() { format!("Turn {next}\n") } else { String::new() };
        let size: usize = entry.iter().skip(start).map(|line| line.chars().count()).sum();
        // Keep ordinary turns together. Only a turn larger than a page is
        // continued within the turn, so even a huge parallel round is readable.
        if next != args.offset && output.chars().count() + heading.chars().count() + size > 7700 {
            break;
        }
        output.push_str(&heading);
        for (index, line) in entry.into_iter().enumerate().skip(start) {
            if output.chars().count() + line.chars().count() > 7700 {
                output.push_str(&format!(
                    "More in this turn: call read_run with the same id, offset={next}, entry_offset={index}, limit={limit}.\n"
                ));
                return Ok(output);
            }
            output.push_str(&line);
        }
        next += 1;
    }
    if next < total {
        output.push_str(&format!(
            "More: call read_run with the same id (or no id for workers), offset={next}, limit={limit} (entry_offset=0).\n"
        ));
    } else {
        output.push_str("End of available history.\n");
    }
    Ok(output)
}
