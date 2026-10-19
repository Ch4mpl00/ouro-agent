// Agent runtime: engine wiring, synthetic tools, and the ReAct loop. Shared
// clients are injected once; each loop owns its conversation and cancellation.
// Sections: 1. engine / skill scoping; 2. loop lifecycle; 3. dispatch / workers.
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
use futures::{FutureExt, StreamExt, future::BoxFuture, stream};
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
        self.check_start(&opts)?;
        if self.loops.lock().await.contains_key(&opts.id) {
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
        if registry.contains_key(&opts.id) {
            anyhow::bail!("agent-loop id {} already exists", opts.id);
        }
        let loop_ = create_loop(self, opts, resolved, allowed)?;
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
    sub_counter: AtomicUsize,
    observation_meta: Metadata,
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
    let skill_names: Vec<_> = resolved.keys().cloned().collect();
    let mut parts = Vec::new();
    if let Some(prompt) = opts.system_prompt.as_ref().filter(|s| !s.is_empty()) {
        parts.push(prompt.as_str());
    }
    parts.extend(resolved.values().filter_map(Value::as_str));
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
        sub_counter: AtomicUsize::new(0),
        observation_meta,
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
            if let Some(text) = text {
                loop_.messages.lock().await.push(providers::user(&text));
            }
            let outcome = loop_.run_until_settled().await;
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
    async fn run_until_settled(self: &Arc<Self>) -> anyhow::Result<String> {
        let engine = self.engine.upgrade().ok_or_else(aborted)?;
        let provider = engine.resolve_provider(&self.model)?;
        let mut messages = self.messages.lock().await;
        if let Some(message) = messages.iter().find(|m| providers::role(m) == "user") {
            self.scope.update(ContextUpdate::input(message.get("content").cloned().unwrap_or(Value::Null)));
        }
        for iteration in 0..self.opts.max_iterations {
            check(&self.cancel)?;
            let mut tools: Vec<_> = crate::mcp::model_tools(&*engine.deps.mcp)
                .into_iter()
                .filter(|t| self.allowed.as_ref().is_none_or(|s| s.contains(&t.name)))
                .collect();
            tools.extend(synthetic_definitions().iter().filter(|t| self.visible(&t.name)).cloned());
            let mut params = providers::CompletionParams {
                model: self.model.clone(),
                messages: &mut messages,
                reasoning_effort: self.reasoning_effort,
                tools,
                response_format: None,
                trace: None,
                cancel: self.cancel.clone(),
            };
            let result = generation(&provider, &mut params, &self.scope, Observation { name: format!("iter-{iteration}"), model_parameters: Some(json!({"reasoning_effort": self.reasoning_effort.as_str(), "thinking": if self.reasoning_effort == crate::models::ReasoningEffort::Disabled {"disabled"} else {"enabled"}}).as_object().cloned().unwrap_or_default()), metadata: Some(self.observation_meta.clone()) }, self.opts.generation_timeout, describe_completion).await?;
            let calls = providers::tool_calls(&result.message);
            let content = providers::content(&result.message).unwrap_or_default().to_owned();
            messages.push(result.message);
            if calls.is_empty() {
                self.scope.update(ContextUpdate::output(content.clone()));
                return Ok(content);
            }
            // A finished call frees its slot at once (`buffered` would hold it
            // until every earlier call finished); replies go back in call
            // order. Cancellation is shared by all tools, and every dispatch
            // runs to its end, so no worker is dropped before its trace /
            // child-loop finalizers run.
            let mut results: Vec<_> = stream::iter(
                calls.into_iter().enumerate().map(|(i, call)| self.dispatch(engine.clone(), call).map(move |r| (i, r))),
            )
            .buffer_unordered(self.opts.max_concurrent_tools)
            .collect()
            .await;
            results.sort_by_key(|(i, _)| *i);
            for (_, result) in results {
                let (call, presented) = result?;
                messages.push(providers::tool_reply(&call.id, &presented));
            }
        }
        anyhow::bail!("session {} exceeded maxIterations={}", self.opts.id, self.opts.max_iterations)
    }

    // ── 3. dispatch / workers ───────────────────────────────────────────────
    fn visible(&self, name: &str) -> bool {
        match name {
            "invoke_sub_agent" => self.opts.parent_id.is_none(),
            "write_skill" => self.opts.allow_skill_writes && self.skill_names.iter().any(|n| n == "dreaming"),
            // Writing a workflow is the author's alone; other loops read,
            // activate and schedule the versions it saved and tried.
            name if crate::flows::AUTHOR_TOOLS.contains(&name) || name == "save_workflow" => {
                self.skill_names.iter().any(|n| n == "workflow-author")
            }
            _ => true,
        }
    }
    async fn dispatch(
        self: &Arc<Self>,
        engine: Arc<Engine>,
        call: providers::ToolCall,
    ) -> anyhow::Result<(providers::ToolCall, String)> {
        let mut metadata = self.observation_meta.clone();
        metadata.insert("tool".into(), call.name.clone().into());
        metadata.insert("tool_call_id".into(), call.id.clone().into());
        let span = self.scope.span(
            SpanStart::new(&call.name, if call.name == "invoke_sub_agent" { SpanKind::Agent } else { SpanKind::Tool })
                .input(json!({"raw_arguments": call.arguments}))
                .metadata(metadata),
        );
        let raw = match serde_json::from_str::<Value>(if call.arguments.is_empty() { "{}" } else { &call.arguments }) {
            Ok(Value::Object(args)) => {
                span.update(ContextUpdate::input(Value::Object(args.clone())));
                self.invoke(&engine, &call.name, args, span.clone()).await
            }
            _ => Ok("[tool error] arguments must be a JSON object".to_owned()),
        };
        let raw = match raw {
            Ok(raw) => raw,
            Err(error) => {
                span.end(SpanEnd::error(error.to_string()));
                return Err(error);
            }
        };
        // A dry run's report is bounded and read whole: storing it would
        // only cost a turn to read it back.
        let presented = if call.name.starts_with("working_memory_") || call.name == "try_workflow" {
            raw.clone()
        } else {
            let stored = store_tool_result(&self.opts.session.memory, &raw);
            span.update(ContextUpdate::metadata(json!({"memory_key": stored.memory_key, "result_size_bytes": stored.size_bytes, "result_truncated": stored.truncated}).as_object().cloned().unwrap_or_default()));
            stored.to_json().to_string()
        };
        span.end(if is_tool_error(&raw) { SpanEnd::error(raw.clone()).with_output(raw) } else { SpanEnd::output(raw) });
        Ok((call, presented))
    }
    async fn invoke(
        self: &Arc<Self>,
        engine: &Arc<Engine>,
        name: &str,
        args: Map<String, Value>,
        span: Arc<dyn Span>,
    ) -> anyhow::Result<String> {
        check(&self.cancel)?;
        let synthetic = synthetic_definitions().iter().any(|t| t.name == name);
        if synthetic && !self.visible(name) {
            return Ok(format!("[tool error] {name} is unavailable in this loop"));
        }
        let outcome = if synthetic {
            self.synthetic(engine, name, &args, span).await
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
    ) -> anyhow::Result<String> {
        match name {
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
                if self.skill_names.iter().any(|n| n == name) && !self.opts.allow_skill_writes {
                    return Ok(json!({"name": name, "found": true, "loaded": true, "content": null,
                        "note": "Already part of your instructions."})
                    .to_string());
                }
                Ok(match engine.deps.skill_store.read_skill(name).await? {
                    None => json!({"name": name, "found": false, "content": null}),
                    Some(skill) => json!({"name": name, "found": true, "content": skill.body, "tools": skill.tools.to_json(), "source": skill.source, "sizeBytes": skill.body.len()}),
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
            "invoke_sub_agent" => self.worker(engine, args, span).await,
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
    ) -> anyhow::Result<String> {
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
        let child_id = format!("{}__sub{}", self.opts.id, self.sub_counter.fetch_add(1, Ordering::SeqCst) + 1);
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
        opts.session_id = self.opts.session_id.clone();
        opts.trace_scope = Some(span);
        let child = match engine.start_loop(opts).await {
            Ok(child) => child,
            Err(error) => return Ok(format!("[invoke_sub_agent error] failed to start: {error}")),
        };
        let result = child.send(child_prompt).await;
        engine.end_loop(&child_id).await;
        Ok(result.unwrap_or_else(|error| format!("[invoke_sub_agent error] sub-agent crashed: {error}")))
    }
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
