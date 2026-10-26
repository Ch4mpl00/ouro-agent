#![allow(dead_code)]
pub mod properties;
use agent::{
    agent::{Engine, EngineDeps, Providers, create_engine},
    codex::Codex,
    db::AgentDb,
    local_recorder::local_recorder_tracer,
    mcp::Mcp,
    models::default_presets,
    providers::{ChatProvider, CompletionParams, CompletionResult, Message, ProviderKind, ToolDef},
    session::{EnvData, Session, session},
    skills::FsSkillStore,
};
use async_trait::async_trait;
use serde_json::{Map, Value, json};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

pub fn context() -> Session {
    session(
        "telegram:42",
        EnvData {
            now: "2026-09-06T12:00:00Z".parse().unwrap(),
            timezone: "Europe/Chisinau".into(),
            user_email: None,
            news_last_read_at: None,
            tech_last_read_at: None,
            telegram_chat_id: None,
        },
    )
}
pub fn calls(calls: &[(&str, Value)]) -> Message {
    json!({"role": "assistant", "content": null, "tool_calls": calls.iter().enumerate().map(|(i,(name,args))| json!({"id": format!("call-{i}"), "type": "function", "function": {"name": name, "arguments": args.to_string()}})).collect::<Vec<_>>()}).as_object().unwrap().clone()
}
#[derive(Clone)]
pub struct Seen {
    pub messages: Vec<Message>,
    pub tools: Vec<ToolDef>,
    pub model: String,
    pub reasoning_effort: agent::models::ReasoningEffort,
}
pub struct Scripted {
    pub script: Mutex<VecDeque<Message>>,
    pub seen: Mutex<Vec<Seen>>,
}
impl Scripted {
    pub fn new(messages: Vec<Message>) -> Self {
        Self { script: Mutex::new(messages.into()), seen: Mutex::new(Vec::new()) }
    }
}
#[async_trait]
impl ChatProvider for Scripted {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Openai
    }
    async fn complete(&self, p: &mut CompletionParams<'_>) -> anyhow::Result<CompletionResult> {
        self.seen.lock().unwrap().push(Seen {
            messages: p.messages.clone(),
            tools: p.tools.clone(),
            model: p.model.clone(),
            reasoning_effort: p.reasoning_effort,
        });
        let message = self.script.lock().unwrap().pop_front().ok_or_else(|| anyhow::anyhow!("script exhausted"))?;
        Ok(CompletionResult { message, finish_reason: Some("stop".into()), usage: None })
    }
}
#[derive(Default)]
pub struct FakeMcp {
    pub defs: Vec<ToolDef>,
    pub called: Mutex<Vec<(String, Map<String, Value>)>>,
    pub active: AtomicUsize,
    pub peak: AtomicUsize,
    pub closed: AtomicUsize,
}
#[async_trait]
impl Mcp for FakeMcp {
    fn tools(&self) -> Vec<ToolDef> {
        self.defs.clone()
    }
    async fn call_tool(
        &self,
        name: &str,
        args: Map<String, Value>,
        cancel: &CancellationToken,
    ) -> anyhow::Result<String> {
        self.called.lock().unwrap().push((name.into(), args.clone()));
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        let result = agent::errors::or_cancel(cancel, async {
            tokio::time::sleep(std::time::Duration::from_millis(
                args.get("delay").and_then(Value::as_u64).unwrap_or(0),
            ))
            .await;
            match name {
                "t_throw" => Err(anyhow::anyhow!("connection reset")),
                "t_err" => Ok("[tool error] refused".into()),
                "t_ok" => Ok(format!("ok {}", Value::Object(args.clone()))),
                _ => Ok(args.get("output").and_then(Value::as_str).unwrap_or("ok").to_owned()),
            }
        })
        .await;
        self.active.fetch_sub(1, Ordering::SeqCst);
        result
    }
    async fn close(&self) {
        self.closed.fetch_add(1, Ordering::SeqCst);
    }
}
pub struct NoCodex;
#[async_trait]
impl Codex for NoCodex {
    async fn run(&self, _: Value, _: &CancellationToken) -> anyhow::Result<String> {
        anyhow::bail!("unexpected codex call")
    }
}
pub struct Fixture {
    pub root: tempfile::TempDir,
    pub db: Arc<AgentDb>,
    pub skills: Arc<FsSkillStore>,
    pub mcp: Arc<FakeMcp>,
    pub engine: Arc<Engine>,
}
pub fn fixture(provider: Arc<dyn ChatProvider>, definitions: &[&str], skills: &[(&str, &str)]) -> Fixture {
    fixture_on(provider, definitions, skills, |_| {})
}
/// A fixture whose engine dependencies `configure` changes before the engine
/// is built (its registry is built from them, and handlers hold it weakly, so
/// it cannot be changed after).
pub fn fixture_on(
    provider: Arc<dyn ChatProvider>,
    definitions: &[&str],
    skills: &[(&str, &str)],
    configure: impl FnOnce(&mut EngineDeps),
) -> Fixture {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("skills.default")).unwrap();
    for (name, body) in skills {
        std::fs::write(root.path().join("skills.default").join(format!("{name}.md")), body).unwrap();
    }
    let db = Arc::new(AgentDb::open(root.path().join("agent.db")).unwrap());
    let store = Arc::new(FsSkillStore::new(root.path()));
    // The reference Telegram loop calls its status tool directly, so every
    // fixture knows it (activation checks a definition's calls).
    let mcp = Arc::new(FakeMcp {
        defs: definitions
            .iter()
            .copied()
            .chain((!definitions.contains(&STATUS_TOOL)).then_some(STATUS_TOOL))
            .map(|name| ToolDef::new(name, name, json!({"type":"object"})))
            .collect(),
        ..Default::default()
    });
    let mut deps = EngineDeps {
        providers: Providers {
            openai: provider.clone(),
            gemini: provider.clone(),
            deepseek: provider.clone(),
            openrouter: Some(provider),
        },
        mcp: mcp.clone(),
        presets: default_presets(),
        skills: Vec::new(),
        skill_store: store.clone(),
        memory: db.clone(),
        tracer: Arc::new(local_recorder_tracer(db.clone())),
        codex: Arc::new(NoCodex),
        decisions: Some(allow_guard()),
        schedule: std::sync::Arc::new(agent::schedule::MemorySchedule::default()),
        env: Default::default(),
        db: (*db).clone(),
        store: agent::runs::open_store(&root.path().join("agent.db")).unwrap(),
    };
    configure(&mut deps);
    let engine = create_engine(deps);
    Fixture { root, db, skills: store, mcp, engine }
}

// ── The TS agent-loop harness ───────────────────────────────────────────────
//
// src/agent-loop.test.ts drives a real engine with scripted model turns, a
// recording MCP, a recording KV and a skill store that knows every skill.
// Each turn sees what the provider was given and returns the next message.

use agent::{
    db::{MemoryStore, StoredTrace, TraceStore},
    skills::{SkillEntry, SkillFile, SkillSource, SkillStore, SkillTools},
    trace_model::Observation,
    tracing::{TokenUsage, Trace, TraceStart, Tracer},
};
use futures::future::BoxFuture;

pub struct TurnParams {
    pub messages: Vec<Message>,
    pub tools: Vec<ToolDef>,
    pub cancel: CancellationToken,
}

pub type Turn = Box<dyn FnOnce(TurnParams) -> BoxFuture<'static, anyhow::Result<Message>> + Send>;

pub fn turn(f: impl FnOnce(TurnParams) -> Message + Send + 'static) -> Turn {
    Box::new(move |p| {
        let message = f(p);
        Box::pin(async move { Ok(message) })
    })
}

pub fn turn_async<F>(f: impl FnOnce(TurnParams) -> F + Send + 'static) -> Turn
where
    F: std::future::Future<Output = anyhow::Result<Message>> + Send + 'static,
{
    Box::new(move |p| Box::pin(f(p)))
}

pub fn fail(message: &'static str) -> Turn {
    Box::new(move |_| Box::pin(async move { Err(anyhow::anyhow!(message)) }))
}

pub struct Turns {
    turns: Mutex<VecDeque<Turn>>,
    pub requests: Mutex<Vec<Vec<Message>>>,
}

impl Turns {
    pub fn new(turns: Vec<Turn>) -> Arc<Turns> {
        Arc::new(Turns { turns: Mutex::new(turns.into()), requests: Mutex::default() })
    }
}

#[async_trait]
impl ChatProvider for Turns {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Openai
    }
    async fn complete(&self, p: &mut CompletionParams<'_>) -> anyhow::Result<CompletionResult> {
        self.requests.lock().unwrap().push(p.messages.clone());
        let next = self.turns.lock().unwrap().pop_front().ok_or_else(|| anyhow::anyhow!("Unexpected LLM call"))?;
        let message =
            next(TurnParams { messages: p.messages.clone(), tools: p.tools.clone(), cancel: p.cancel.clone() }).await?;
        Ok(CompletionResult {
            message,
            finish_reason: Some("stop".into()),
            usage: Some(TokenUsage { input: 10, output: 5, total: 15, cached: Some(2) }),
        })
    }
}

pub fn answer(content: &str) -> Message {
    agent::providers::assistant(content)
}

pub fn call(name: &str, args: Value) -> Message {
    call_id(name, args, "call_1")
}

pub fn call_id(name: &str, args: Value, id: &str) -> Message {
    json!({"role":"assistant","content":null,"tool_calls":[{"id":id,"type":"function","function":{"name":name,"arguments":args.to_string()}}]})
        .as_object()
        .unwrap()
        .clone()
}

// Several calls in one assistant turn.
pub fn calls_of(messages: &[Message]) -> Message {
    let mut first = messages[0].clone();
    let all: Vec<Value> =
        messages.iter().flat_map(|m| m["tool_calls"].as_array().cloned().unwrap_or_default()).collect();
    first.insert("tool_calls".into(), Value::Array(all));
    first
}

pub fn last_result(messages: &[Message]) -> String {
    let last = messages.last().expect("a message");
    assert_eq!(last["role"], "tool", "Expected a tool reply");
    last["content"].as_str().expect("tool reply text").to_owned()
}

pub fn last_json(messages: &[Message]) -> Value {
    serde_json::from_str(&last_result(messages)).unwrap()
}

pub fn tool_replies(messages: &[Message]) -> Vec<Value> {
    messages
        .iter()
        .filter(|m| m["role"] == "tool")
        .map(|m| {
            let text = m["content"].as_str().unwrap();
            serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.to_owned()))
        })
        .collect()
}

pub fn search_tool() -> ToolDef {
    ToolDef {
        name: "search_news".into(),
        description: None,
        parameters: json!({"type":"object","properties":{"query":{"type":"string"}},"required":["query"]}),
    }
}

pub type ToolFn =
    Box<dyn FnOnce(String, Map<String, Value>, CancellationToken) -> BoxFuture<'static, anyhow::Result<String>> + Send>;
pub type ToolHandler = Arc<
    dyn Fn(String, Map<String, Value>, CancellationToken) -> BoxFuture<'static, anyhow::Result<String>> + Send + Sync,
>;

// Records every call; answers from the one-shot queue first (vi's
// mockImplementationOnce), then from the standing handler.
pub struct ToolMcp {
    pub defs: Vec<ToolDef>,
    pub calls: Mutex<Vec<(String, Map<String, Value>)>>,
    pub once: Mutex<VecDeque<ToolFn>>,
    pub handler: Mutex<ToolHandler>,
    pub closed: AtomicUsize,
}

impl ToolMcp {
    pub fn returning(defs: Vec<ToolDef>, result: &str) -> Arc<ToolMcp> {
        let result = result.to_owned();
        Arc::new(ToolMcp {
            defs,
            calls: Mutex::default(),
            once: Mutex::default(),
            handler: Mutex::new(Arc::new(move |_, _, _| {
                let result = result.clone();
                Box::pin(async move { Ok(result) })
            })),
            closed: AtomicUsize::new(0),
        })
    }
    pub fn once(&self, f: ToolFn) {
        self.once.lock().unwrap().push_back(f);
    }
    pub fn always(&self, f: ToolHandler) {
        *self.handler.lock().unwrap() = f;
    }
    pub fn names(&self) -> Vec<String> {
        self.calls.lock().unwrap().iter().map(|(n, _)| n.clone()).collect()
    }
}

#[async_trait]
impl Mcp for ToolMcp {
    fn tools(&self) -> Vec<ToolDef> {
        self.defs.clone()
    }
    async fn call_tool(
        &self,
        name: &str,
        args: Map<String, Value>,
        cancel: &CancellationToken,
    ) -> anyhow::Result<String> {
        self.calls.lock().unwrap().push((name.into(), args.clone()));
        let once = self.once.lock().unwrap().pop_front();
        let future = match once {
            Some(f) => f(name.into(), args, cancel.clone()),
            None => {
                let handler = self.handler.lock().unwrap().clone();
                handler(name.into(), args, cancel.clone())
            }
        };
        future.await
    }
    async fn close(&self) {
        self.closed.fetch_add(1, Ordering::SeqCst);
    }
}

// The memory KV's writes of the agent, in order; the keys the runtime keeps
// for itself (`engine.`: built-in digests, the reconciler's watermarks) are
// kept apart, readable, and out of the log.
#[derive(Default)]
pub struct PersistLog(pub Mutex<Vec<(String, String)>>, pub Mutex<HashMap<String, String>>);

impl MemoryStore for PersistLog {
    fn get(&self, key: &str) -> anyhow::Result<Option<String>> {
        Ok(self.1.lock().unwrap().get(key).cloned())
    }
    fn set(&self, key: &str, value: &str) -> anyhow::Result<()> {
        if key.starts_with(agent::runs::RESERVED_MEMORY_PREFIX) {
            self.1.lock().unwrap().insert(key.into(), value.into());
        } else {
            self.0.lock().unwrap().push((key.into(), value.into()));
        }
        Ok(())
    }
}

// Knows every skill: "test skill", all tools.
pub struct AnySkill;

#[async_trait]
impl SkillStore for AnySkill {
    async fn read_skill(&self, _: &str) -> anyhow::Result<Option<SkillFile>> {
        Ok(Some(SkillFile {
            routable: true,
            loop_name: None,
            body: "test skill".into(),
            tools: SkillTools::All,
            source: SkillSource::Default,
        }))
    }
    async fn read_patch(&self, _: &str) -> anyhow::Result<Option<String>> {
        Ok(None)
    }
    async fn save_skill(&self, _: &str, _: &str) -> anyhow::Result<agent::skills::Written> {
        Ok(agent::skills::Written { path: "test.md".into(), size_bytes: 0 })
    }
    async fn list_skills(&self) -> anyhow::Result<Vec<SkillEntry>> {
        Ok(Vec::new())
    }
}

// What the local recorder hands over on trace end.
#[derive(Default)]
pub struct TraceLog(pub Mutex<Vec<StoredTrace>>);

impl TraceLog {
    pub fn all(&self) -> Vec<StoredTrace> {
        self.0.lock().unwrap().clone()
    }
    pub fn len(&self) -> usize {
        self.0.lock().unwrap().len()
    }
}

impl TraceStore for TraceLog {
    fn write_trace(&self, t: &StoredTrace) -> anyhow::Result<()> {
        self.0.lock().unwrap().push(t.clone());
        Ok(())
    }
    fn get_trace(&self, _: &str) -> anyhow::Result<Option<(agent::trace_model::Trace, Vec<Observation>)>> {
        Ok(None)
    }
}

pub fn recording() -> (Arc<dyn Tracer>, Arc<TraceLog>) {
    let written = Arc::new(TraceLog::default());
    (Arc::new(local_recorder_tracer(written.clone())), written)
}

pub fn find<'a>(observations: &'a [Observation], name: &str) -> &'a Observation {
    observations.iter().find(|o| o.name == name).unwrap_or_else(|| panic!("no observation {name}"))
}

pub fn task_context(id: &str) -> Session {
    session(
        id,
        EnvData {
            now: "2026-09-06T12:00:00Z".parse().unwrap(),
            timezone: "UTC".into(),
            user_email: None,
            news_last_read_at: None,
            tech_last_read_at: None,
            telegram_chat_id: None,
        },
    )
}

pub struct Harness {
    pub engine: Arc<Engine>,
    pub session: Session,
    pub mcp: Arc<ToolMcp>,
    pub persist: Arc<PersistLog>,
    pub model: Arc<Turns>,
}

impl Harness {
    pub fn new(session: Session, turns: Vec<Turn>, tool_result: &str, tracer: Arc<dyn Tracer>) -> Harness {
        Self::configured(session, turns, tool_result, tracer, |_| {})
    }

    /// A harness whose engine dependencies `configure` changes before the
    /// engine is built (handlers hold it weakly: it cannot change after).
    pub fn configured(
        session: Session,
        turns: Vec<Turn>,
        tool_result: &str,
        tracer: Arc<dyn Tracer>,
        configure: impl FnOnce(&mut EngineDeps),
    ) -> Harness {
        let model = Turns::new(turns);
        let status = ToolDef::new(STATUS_TOOL, STATUS_TOOL, json!({"type":"object"}));
        let mcp = ToolMcp::returning(vec![search_tool(), status], tool_result);
        let persist = Arc::new(PersistLog::default());
        let provider: Arc<dyn ChatProvider> = model.clone();
        let mut deps = EngineDeps {
            providers: Providers {
                openai: provider.clone(),
                gemini: provider.clone(),
                deepseek: provider.clone(),
                openrouter: Some(provider),
            },
            mcp: mcp.clone(),
            presets: default_presets(),
            skills: Vec::new(),
            skill_store: Arc::new(AnySkill),
            memory: persist.clone(),
            tracer,
            codex: Arc::new(NoCodex),
            decisions: Some(allow_guard()),
            schedule: std::sync::Arc::new(agent::schedule::MemorySchedule::default()),
            env: Default::default(),
            db: agent::db::AgentDb::open(":memory:").unwrap(),
            store: agent::runs::memory_store(),
        };
        configure(&mut deps);
        let engine = create_engine(deps);
        Harness { engine, session, mcp, persist, model }
    }

    pub fn options(&self) -> agent::agent::LoopOptions {
        let mut opts = agent::agent::LoopOptions::new("parent", self.session.clone());
        opts.skills = vec!["test".into()];
        opts
    }

    pub async fn start(&self) -> Arc<agent::agent::AgentLoop> {
        self.engine.start_loop(self.options()).await.unwrap()
    }

    pub fn requests(&self) -> usize {
        self.model.requests.lock().unwrap().len()
    }
}

// Wraps a tracer, counting shutdowns and checking what was written by then.
pub struct ShutdownProbe {
    pub inner: Arc<dyn Tracer>,
    pub written: Arc<TraceLog>,
    pub shutdowns: AtomicUsize,
    pub written_at_shutdown: Mutex<Vec<usize>>,
}

#[async_trait]
impl Tracer for ShutdownProbe {
    fn trace(&self, opts: TraceStart) -> Arc<dyn Trace> {
        self.inner.trace(opts)
    }
    async fn shutdown(&self) {
        self.shutdowns.fetch_add(1, Ordering::SeqCst);
        self.written_at_shutdown.lock().unwrap().push(self.written.len());
        self.inner.shutdown().await;
    }
}

// Most loop fixtures test dispatch/durability rather than policy. Their
// decision service explicitly allows writes; guard tests override its label.
#[derive(Default)]
pub struct GuardApi {
    pub requests: Mutex<Vec<Value>>,
    pub label: Mutex<Option<String>>,
    pub broken: std::sync::atomic::AtomicBool,
}
#[async_trait]
impl agent::providers::JsonApi for GuardApi {
    async fn post(&self, _: &str, body: Value, _: &CancellationToken) -> anyhow::Result<Value> {
        self.requests.lock().unwrap().push(body.clone());
        if self.broken.load(Ordering::SeqCst) {
            anyhow::bail!("guard test service unavailable");
        }
        let label = self.label.lock().unwrap().clone().unwrap_or_else(|| "allow".into());
        let answers: Map<String, Value> = body["questions"]
            .as_object()
            .unwrap()
            .keys()
            .map(|key| {
                (
                    key.clone(),
                    json!({"type":"choice","choice":label,"confidence":1.0,"probabilities":{label.clone():1.0}}),
                )
            })
            .collect();
        Ok(json!({"model":"guard-test","answers":answers}))
    }
}
pub fn allow_guard() -> Arc<agent::decisions::DecisionClient> {
    agent::decisions::decision_client("guard-test".into(), Arc::new(GuardApi::default()))
}

// Reference workflows are agent-authored examples, never runtime built-ins.
// Saving and activating them return with the engine migration's versions
// step (design 03, decision 9): the tests that use this are ignored until then.
pub fn activate_definition(_engine: &Arc<Engine>, name: &str, _definition: &str) -> i64 {
    panic!("activating `{name}` returns with the versions step (design 03, decision 9)")
}
// The reference flow of a channel: one loop that declares its route.
pub const TELEGRAM_LOOP: &str = include_str!("../fixtures/telegram-loop.yaml");
pub const STATUS_TOOL: &str = "telegram_send_status";
pub fn activate_telegram_loop(engine: &Arc<Engine>) -> i64 {
    activate_definition(engine, "telegram-loop", TELEGRAM_LOOP)
}
