// The dry-run sandbox: `try_workflow` with a signal runs it through the real
// chain (dispatcher → route → loop → guard → workers) as if it had arrived,
// with the drafts it names in place of the active versions. Model turns,
// reads (the internet's and MCP's) and the guard are real; every write is
// recorded and answered with MOCK_REPLY, a confirmation is answered on the
// spot instead of sent, and nothing persists. Sections: 1. the sandbox and
// what it records; 2. the dry engine's stores; 3. the YAML path; 4. the
// signal types; 5. try_workflow with a signal; 6. the report.
//
// Isolation is by injection (CLAUDE.md, modules + DI): a dry run gets an
// engine of its own whose stores read through to the real ones and keep or
// refuse every write, a run store in an in-memory agent.db (runs, events,
// cards, guard verdicts) and a tracer nested under the builder's call. The
// domains ask `Engine::sandbox` only where a dry run differs in kind: what a
// write answers, how a card is answered, that no card is edited. The builder
// judges the trace; the sandbox has no expectations of its own.
use crate::{
    agent::{Engine, EngineDeps, Providers, create_sandbox_engine},
    confirmation::{Answer, Request},
    db::{AgentDb, MemoryStore},
    flows::DRY_RUN_TIMEOUT,
    flows::{SavedWorkflow, WorkflowStore, check, create_durable_flows},
    mcp::{Mcp, SharedMcp, ToolCallFailure},
    providers::{self, Message, ToolCall, ToolDef},
    schedule::{NewTask, ScheduleStore, TaskRow},
    session::{EnvDeps, Session},
    skills::{SkillEntry, SkillFile, SkillStore, Written},
    supervisor::{PendingSignal, create_supervisor_with_cancel},
    tracing::{
        ContextUpdate, EventStart, Generation, GenerationStart, Scope, Span, SpanEnd, SpanKind, SpanStart, Trace,
        TraceAnchor, TraceContext, TraceStart, Tracer,
    },
};
use async_trait::async_trait;
use owf::run::{Event, Host, Observer, Status};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};
use tokio_util::sync::CancellationToken;

// ── 1. the sandbox ─────────────────────────────────────────────────────────

/// What every write of a dry run answers, MCP's and the runtime's alike. A
/// loop rule that read a write's reply would see this instead of the real
/// one, so rules decide on a call's arguments and `ok` (workflow-author).
pub const MOCK_REPLY: &str = r#"{"mock":true,"note":"dry run: not executed","delivered":true}"#;

// A run's path keeps this many steps; the rest are counted.
const PATH_MAX: usize = 200;
// A reply longer than this goes to the builder's working memory, with a
// preview in the report, as any large tool result does.
const RESULT_INLINE_CHARS: usize = 1500;
// Model text and reasoning per turn, each argument of a call.
const TEXT_MAX_CHARS: usize = 3000;
const ARGUMENT_MAX_CHARS: usize = 3000;

pub struct Sandbox {
    answer: Answer,
    // The draft versions in use, by name.
    drafts: BTreeMap<String, i64>,
    record: Mutex<Record>,
}

// What a dry run did, in the order it happened.
#[derive(Default)]
struct Record {
    runs: Vec<RunRecord>,
    turns: Vec<Value>,
    calls: Vec<Value>,
    // A loop's last settled round: whether `finish` ended it.
    finished: HashMap<String, bool>,
    // The answer given to each card, by loop and call key.
    answers: HashMap<(String, String), String>,
    writes: Vec<Value>,
}

struct RunRecord {
    id: String,
    workflow: String,
    version: String,
    parent: Option<String>,
    // The agent loop a loop run drives.
    loop_id: Option<String>,
    path: Vec<String>,
    omitted: usize,
}

impl Sandbox {
    fn new(answer: Answer, drafts: BTreeMap<String, i64>) -> Arc<Sandbox> {
        Arc::new(Sandbox { answer, drafts, record: Mutex::default() })
    }

    fn lock(&self) -> MutexGuard<'_, Record> {
        self.record.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Records a write that is not made and returns what it answers.
    pub fn mock(&self, function: &str, arguments: Value) -> String {
        self.lock().writes.push(json!({"function": function, "arguments": clip_arguments(arguments)}));
        MOCK_REPLY.to_owned()
    }

    /// Makes `host` a dry run's: its steps are recorded as the run `run`'s
    /// path, and it never sleeps (a wait or a retry delay is cut to nothing,
    /// a listen with a timeout takes an event already there or expires).
    #[allow(clippy::too_many_arguments)]
    pub fn watch(
        self: &Arc<Self>,
        host: &mut Host,
        run: &str,
        workflow: &str,
        version: Option<i64>,
        parent: Option<&str>,
        loop_id: Option<&str>,
    ) {
        let version = match (version, self.drafts.get(workflow)) {
            (Some(v), Some(draft)) if v == *draft => format!("draft v{v}"),
            (Some(v), _) => format!("v{v}"),
            (None, _) => "built-in".into(),
        };
        let mut record = self.lock();
        if !record.runs.iter().any(|r| r.id == run) {
            record.runs.push(RunRecord {
                id: run.into(),
                workflow: workflow.into(),
                version,
                parent: parent.map(str::to_owned),
                loop_id: loop_id.map(str::to_owned),
                path: Vec::new(),
                omitted: 0,
            });
        }
        host.observer = Arc::new(Path { sandbox: self.clone(), run: run.into(), inner: host.observer.clone() });
        host.limits.max_sleep = Some(std::time::Duration::ZERO);
        host.limits.max_listen = Some(std::time::Duration::ZERO);
    }

    /// A model turn of loop `loop_id`, the `n`th.
    pub fn turn(&self, loop_id: &str, n: u64, message: &Message, nudged: bool) {
        let reasoning = ["reasoning_content", "reasoning"]
            .iter()
            .find_map(|key| message.get(*key).and_then(Value::as_str).filter(|r| !r.trim().is_empty()));
        let calls: Vec<Value> = providers::tool_calls(message)
            .iter()
            .map(|c| json!({"id": c.id, "name": c.name, "arguments": arguments(&c.arguments)}))
            .collect();
        let mut turn = json!({"loop": loop_id, "turn": n});
        if let Some(reasoning) = reasoning {
            turn["reasoning"] = json!(crate::activity::clip(reasoning, TEXT_MAX_CHARS));
        }
        if let Some(text) = providers::content(message).filter(|t| !t.trim().is_empty()) {
            turn["text"] = json!(crate::activity::clip(text, TEXT_MAX_CHARS));
        }
        turn["calls"] = json!(calls);
        if nudged {
            turn["after_nudge"] = json!(true);
        }
        self.lock().turns.push(turn);
    }

    /// One call of loop `loop_id`'s round `round`: the verdict it ran under
    /// (none: it needed no review) and its raw reply.
    pub fn call(
        &self,
        loop_id: &str,
        round: Option<u64>,
        call: &ToolCall,
        verdict: Option<&crate::guard::Verdict>,
        ok: bool,
        raw: &str,
    ) {
        let mut entry = json!({"loop": loop_id, "round": round, "id": call.id, "key": crate::guard::key(call),
            "verdict": verdict, "ok": ok, "mock": raw == MOCK_REPLY, "raw": raw});
        if verdict.is_none() {
            entry.as_object_mut().expect("object").shift_remove("verdict");
        }
        self.lock().calls.push(entry);
    }

    pub fn settled(&self, loop_id: &str, finished: bool) {
        self.lock().finished.insert(loop_id.into(), finished);
    }

    /// The card a guard would send for `req`: answered on the spot instead,
    /// as the try asked (`confirm`), and traced as such.
    pub fn press(
        &self,
        runs: &crate::durable::RunStore,
        events: &dyn owf::run::Events,
        req: Request<'_>,
        loop_id: &str,
        scope: &Scope,
    ) -> anyhow::Result<Value> {
        let line = format!("confirmation requested: {} → auto-{}", req.call.name, self.answer.word());
        let key = crate::guard::key(req.call);
        let requested = crate::confirmation::press(runs, events, req, self.answer)?;
        scope.event(EventStart { name: line.clone(), ..Default::default() });
        self.lock().answers.insert((loop_id.into(), key), line);
        Ok(requested)
    }

    fn path(&self, run: &str, step: Step) {
        let mut record = self.lock();
        let Some(run) = record.runs.iter_mut().find(|r| r.id == run) else { return };
        match step {
            Step::Case(task, case) => {
                if let Some(entry) = run.path.iter_mut().rev().find(|e| **e == task) {
                    *entry = format!("{task} → {case}");
                }
            }
            Step::Add(entry) if run.path.len() < PATH_MAX => run.path.push(entry),
            Step::Add(_) => run.omitted += 1,
        }
    }

    // Whether the draft of `workflow` ran, not merely a run of that name (a
    // breaker can put the built-in one in its place).
    fn ran(&self, workflow: &str) -> bool {
        ran(&self.lock(), &self.drafts, workflow)
    }
}

fn ran(record: &Record, drafts: &BTreeMap<String, i64>, workflow: &str) -> bool {
    let Some(version) = drafts.get(workflow) else { return false };
    record.runs.iter().any(|r| r.workflow == workflow && r.version == format!("draft v{version}"))
}

fn arguments(raw: &str) -> Value {
    clip_arguments(serde_json::from_str(raw).unwrap_or_else(|_| json!(raw)))
}

fn clip_arguments(mut value: Value) -> Value {
    match &mut value {
        Value::Object(fields) => {
            for v in fields.values_mut() {
                if v.to_string().chars().count() > ARGUMENT_MAX_CHARS {
                    let text = v.as_str().map(str::to_owned).unwrap_or_else(|| v.to_string());
                    *v = json!(crate::activity::clip(&text, ARGUMENT_MAX_CHARS));
                }
            }
            value
        }
        Value::String(text) => json!(crate::activity::clip(text, ARGUMENT_MAX_CHARS)),
        _ => value,
    }
}

// ── 2. the dry engine's stores ─────────────────────────────────────────────

// MCP as a dry run sees it: a tool the server marks read-only runs; any
// other is recorded and answered with the mock. The one place MCP writes
// stop, whoever makes them (a loop, a workflow step, the runtime).
struct DryMcp {
    real: SharedMcp,
    sandbox: Arc<Sandbox>,
}

#[async_trait]
impl Mcp for DryMcp {
    fn tools(&self) -> Vec<ToolDef> {
        self.real.tools()
    }
    fn read_only(&self, name: &str) -> bool {
        self.real.read_only(name)
    }
    fn open_world(&self, name: &str) -> bool {
        self.real.open_world(name)
    }
    async fn call_tool(
        &self,
        name: &str,
        args: Map<String, Value>,
        cancel: &CancellationToken,
    ) -> anyhow::Result<String> {
        if self.real.read_only(name) {
            return self.real.call_tool(name, args, cancel).await;
        }
        Ok(self.sandbox.mock(name, Value::Object(args)))
    }
    async fn call_tool_once(
        &self,
        name: &str,
        args: Map<String, Value>,
        cancel: &CancellationToken,
    ) -> Result<String, ToolCallFailure> {
        if self.real.read_only(name) {
            return self.real.call_tool_once(name, args, cancel).await;
        }
        Ok(self.sandbox.mock(name, Value::Object(args)))
    }
    // The session is the agent's; a dry run never closes it.
    async fn close(&self) {}
}

// The memory KV: reads see the real values, writes (a breaker's count) stay
// in the run.
struct DryMemory {
    real: Arc<dyn MemoryStore>,
    kept: Mutex<HashMap<String, String>>,
}

impl MemoryStore for DryMemory {
    fn get(&self, key: &str) -> anyhow::Result<Option<String>> {
        match self.kept.lock().unwrap_or_else(|e| e.into_inner()).get(key) {
            Some(value) => Ok(Some(value.clone())),
            None => self.real.get(key),
        }
    }
    fn set(&self, key: &str, value: &str) -> anyhow::Result<()> {
        self.kept.lock().unwrap_or_else(|e| e.into_inner()).insert(key.into(), value.into());
        Ok(())
    }
}

const NO_WRITES: &str = "a dry run changes nothing";

// Saved workflows with the drafts active, in this run only.
struct DraftWorkflows {
    real: Arc<dyn WorkflowStore>,
    drafts: HashMap<String, SavedWorkflow>,
}

impl WorkflowStore for DraftWorkflows {
    fn save(&self, _: &str, _: &str, _: Option<&str>) -> anyhow::Result<SavedWorkflow> {
        anyhow::bail!(NO_WRITES)
    }
    fn activate(&self, _: &str, _: i64) -> anyhow::Result<()> {
        anyhow::bail!(NO_WRITES)
    }
    fn mark_tried(&self, _: &str, _: i64) -> anyhow::Result<()> {
        anyhow::bail!(NO_WRITES)
    }
    fn get(&self, name: &str, version: Option<i64>) -> anyhow::Result<Option<SavedWorkflow>> {
        self.real.get(name, version)
    }
    fn active(&self, name: &str) -> anyhow::Result<Option<SavedWorkflow>> {
        match self.drafts.get(name) {
            Some(draft) => Ok(Some(SavedWorkflow { status: crate::flows::Status::Active, ..draft.clone() })),
            None => self.real.active(name),
        }
    }
    fn list(&self) -> anyhow::Result<Vec<SavedWorkflow>> {
        self.real.list()
    }
}

struct DrySkills(Arc<dyn SkillStore>);

#[async_trait]
impl SkillStore for DrySkills {
    async fn read_skill(&self, name: &str) -> anyhow::Result<Option<SkillFile>> {
        self.0.read_skill(name).await
    }
    async fn read_patch(&self, name: &str) -> anyhow::Result<Option<String>> {
        self.0.read_patch(name).await
    }
    async fn save_skill(&self, _: &str, _: &str) -> anyhow::Result<Written> {
        anyhow::bail!(NO_WRITES)
    }
    async fn list_skills(&self) -> anyhow::Result<Vec<SkillEntry>> {
        self.0.list_skills().await
    }
}

struct DrySchedule(Arc<dyn ScheduleStore>);

#[async_trait]
impl ScheduleStore for DrySchedule {
    async fn insert(&self, _: &NewTask) -> anyhow::Result<TaskRow> {
        anyhow::bail!(NO_WRITES)
    }
    async fn list_active(&self) -> anyhow::Result<Vec<TaskRow>> {
        self.0.list_active().await
    }
    async fn swap_last_run(&self, _: i64, _: Option<i64>, _: Option<i64>) -> anyhow::Result<bool> {
        anyhow::bail!(NO_WRITES)
    }
    async fn delete(&self, _: i64) -> anyhow::Result<bool> {
        anyhow::bail!(NO_WRITES)
    }
    async fn record_fire(&self, _: i64, _: i64) -> anyhow::Result<i64> {
        anyhow::bail!(NO_WRITES)
    }
    async fn initialize(&self, _: &[NewTask]) -> anyhow::Result<bool> {
        anyhow::bail!(NO_WRITES)
    }
}

// Every trace a dry run would start is a span under the builder's call: the
// dry run shows in the builder's own trace and never as a signal of its own
// (the activity journal would take that for something that happened).
struct Nested(Arc<dyn Span>);

#[async_trait]
impl Tracer for Nested {
    fn trace(&self, opts: TraceStart) -> Arc<dyn Trace> {
        let mut start = SpanStart::new(opts.name, opts.kind);
        start.metadata = opts.metadata;
        Arc::new(NestedTrace(self.0.span(start)))
    }
    async fn shutdown(&self) {}
}

struct NestedTrace(Arc<dyn Span>);

impl TraceContext for NestedTrace {
    fn anchor(&self) -> Option<TraceAnchor> {
        self.0.anchor()
    }
    fn update(&self, data: ContextUpdate) {
        self.0.update(data)
    }
    fn generation(&self, opts: GenerationStart) -> Arc<dyn Generation> {
        self.0.generation(opts)
    }
    fn span(&self, opts: SpanStart) -> Arc<dyn Span> {
        self.0.span(opts)
    }
    fn event(&self, opts: EventStart) {
        self.0.event(opts)
    }
}

impl Trace for NestedTrace {
    fn id(&self) -> String {
        self.0.id()
    }
    fn end(&self) {
        self.0.end(SpanEnd::default())
    }
}

// ── 3. the YAML path ───────────────────────────────────────────────────────

enum Step {
    Add(String),
    // A switch's task and the case it took.
    Case(String, String),
}

// A dry run's observer: every step of a run as one line, the case each
// switch took, a fault with its error (a jq error names the expression and
// the value it met). The run's own observer still gets every event.
struct Path {
    sandbox: Arc<Sandbox>,
    run: String,
    inner: Arc<dyn Observer>,
}

fn task(reference: &str) -> &str {
    reference.rsplit('/').next().unwrap_or(reference)
}

impl Observer for Path {
    fn event(&self, event: &Event<'_>) {
        self.inner.event(event);
        let step = match event {
            Event::TaskStarted { reference, kind, .. } if *kind != "do" => Step::Add(task(reference).into()),
            Event::TaskSkipped { reference, .. } => Step::Add(format!("{} (skipped)", task(reference))),
            Event::SwitchCase { reference, case } => Step::Case(task(reference).into(), (*case).into()),
            // A fault is reported where it happened, not by every task it passes.
            Event::TaskFaulted { reference, error } if error.instance.as_deref().is_none_or(|i| i == *reference) => {
                Step::Add(format!("{} faulted: {error}", task(reference)))
            }
            Event::TaskSuspended { reference, .. } => Step::Add(format!("{} (suspended)", task(reference))),
            Event::TaskRetrying { reference, attempt, error, .. } => {
                Step::Add(format!("{} retry {attempt}: {error}", task(reference)))
            }
            _ => return,
        };
        self.sandbox.path(&self.run, step);
    }
}

// ── 4. the signal types ────────────────────────────────────────────────────

// How far back the latest signal of each source is looked for.
const SIGNAL_TYPES_DAYS: i64 = 30;
const SIGNAL_EXAMPLE_CHARS: usize = 600;

/// The sources a signal can come from, each with its latest real signal as
/// the shape to model a dry run's on. They come from MCP's queue
/// (`list_signals`) for now (#57), plus the agent's own scheduler, whose
/// fires never pass that queue.
pub async fn signal_types(mcp: &dyn Mcp, cancel: &CancellationToken) -> anyhow::Result<Vec<Value>> {
    let mut latest: BTreeMap<String, Value> = BTreeMap::new();
    if mcp.tools().iter().any(|t| t.name == "list_signals") {
        let since = chrono::Utc::now() - chrono::TimeDelta::days(SIGNAL_TYPES_DAYS);
        let args = json!({"since": crate::session::iso(since), "limit": 2000});
        let raw = mcp.call_tool("list_signals", args.as_object().cloned().unwrap_or_default(), cancel).await?;
        anyhow::ensure!(!crate::tool_results::is_tool_error(&raw), "list_signals: {raw}");
        let listed: Value = serde_json::from_str(&raw)?;
        for signal in listed["signals"].as_array().into_iter().flatten() {
            let Some(source) = signal["source"].as_str() else { continue };
            latest.insert(
                source.to_owned(),
                json!({"source": source, "latest": {
                    "content": crate::activity::clip(signal["content"].as_str().unwrap_or_default(), SIGNAL_EXAMPLE_CHARS),
                    "created_at": signal["created_at"],
                }}),
            );
        }
    }
    latest.entry("scheduler".into()).or_insert_with(|| {
        json!({"source": "scheduler", "latest": null,
            "shape": "content: the scheduled task's prompt; payload: {\"workflow\": \"<name>\"} when the task runs a saved workflow"})
    });
    Ok(latest.into_values().collect())
}

// ── 5. try_workflow with a signal ──────────────────────────────────────────

// The id a dry signal carries: it names its runs and its session.
const DRY_SIGNAL_ID: i64 = 0;

/// `try_workflow(name, signal, drafts?, confirm?, version?)`: runs `signal`
/// as if it had just arrived, through the dispatcher and whatever it routes
/// to, with `name` and each of `drafts` at its latest saved version (`name`
/// at `version`) instead of the active one. The reply is the trace (§6).
/// A completed run marks every draft that ran as tried, which activation
/// requires.
pub async fn try_signal(
    engine: &Arc<Engine>,
    args: &Map<String, Value>,
    session: &Session,
    scope: &Scope,
    cancel: &CancellationToken,
) -> anyhow::Result<String> {
    let name = args.get("name").and_then(Value::as_str).filter(|n| !n.is_empty());
    let name = name.ok_or_else(|| anyhow::anyhow!("name: Expected nonempty string"))?;
    let version = match args.get("version") {
        None | Some(Value::Null) => None,
        Some(v) => {
            Some(v.as_i64().filter(|v| *v > 0).ok_or_else(|| anyhow::anyhow!("version: Expected positive integer"))?)
        }
    };
    let answer = Answer::parse(match args.get("confirm") {
        None | Some(Value::Null) => None,
        Some(v) => Some(v.as_str().ok_or_else(|| anyhow::anyhow!("confirm: Expected a string"))?),
    })?;
    let mut names = vec![name.to_owned()];
    for draft in args.get("drafts").and_then(Value::as_array).into_iter().flatten() {
        let draft =
            draft.as_str().filter(|d| !d.is_empty()).ok_or_else(|| anyhow::anyhow!("drafts: Expected names"))?;
        if !names.iter().any(|n| n == draft) {
            names.push(draft.to_owned());
        }
    }
    let mut drafts = HashMap::new();
    for draft in &names {
        anyhow::ensure!(draft != "guard", "guard is a reserved built-in workflow and has no drafts");
        let version = if draft == name { version } else { None };
        let Some(saved) = engine.deps.workflows.get(draft, version)? else {
            // Trying the dispatcher (or the loop) as it is, with drafts below it.
            if draft == name && version.is_none() && crate::flows::built_in(name).is_some() {
                continue;
            }
            anyhow::bail!("no saved workflow `{draft}`");
        };
        check(&*engine.deps.mcp, &saved.definition).map_err(|e| anyhow::anyhow!("`{draft}`: {e}"))?;
        drafts.insert(draft.clone(), saved);
    }
    let signal =
        args.get("signal").and_then(Value::as_object).ok_or_else(|| anyhow::anyhow!("signal: Expected an object"))?;
    let types = signal_types(&*engine.deps.mcp, cancel).await?;
    let source = signal.get("source").and_then(Value::as_str).unwrap_or_default();
    if !types.iter().any(|t| t["source"] == source) {
        anyhow::bail!(
            "{}. Model the signal on one of these types, each shown with its latest real signal: {}",
            if source.is_empty() {
                "signal.source is missing".into()
            } else {
                format!("signal.source `{source}` is not a signal type this agent receives")
            },
            Value::Array(types)
        );
    }
    let content = signal
        .get("content")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("signal.content: Expected a string"))?;
    let text = |key: &str| signal.get(key).and_then(Value::as_str).map(str::to_owned);
    let pending = PendingSignal {
        id: DRY_SIGNAL_ID,
        source: source.to_owned(),
        content: content.to_owned(),
        env_context: text("envContext"),
        created_at: chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
        payload: signal.get("payload").filter(|p| !p.is_null()).cloned(),
    };

    let sandbox = Sandbox::new(answer, drafts.iter().map(|(n, s)| (n.clone(), s.version)).collect());
    let root = scope.span(
        SpanStart::new("dry-run", SpanKind::Chain).input(json!({"signal": signal, "confirm": answer.word()})).metadata(
            json!({"dry_run": true, "workflow": name, "drafts": sandbox.drafts})
                .as_object()
                .cloned()
                .unwrap_or_default(),
        ),
    );
    let deps = &engine.deps;
    let mcp: SharedMcp = Arc::new(DryMcp { real: deps.mcp.clone(), sandbox: sandbox.clone() });
    let memory: Arc<dyn MemoryStore> = Arc::new(DryMemory { real: deps.memory.clone(), kept: Mutex::default() });
    let dry = create_sandbox_engine(
        EngineDeps {
            providers: Providers {
                deepseek: deps.providers.deepseek.clone(),
                openai: deps.providers.openai.clone(),
                gemini: deps.providers.gemini.clone(),
                openrouter: deps.providers.openrouter.clone(),
            },
            mcp: mcp.clone(),
            presets: deps.presets.clone(),
            skills: deps.skills.clone(),
            skill_store: Arc::new(DrySkills(deps.skill_store.clone())),
            memory: memory.clone(),
            tracer: Arc::new(Nested(root.clone())),
            codex: deps.codex.clone(),
            decisions: deps.decisions.clone(),
            workflows: Arc::new(DraftWorkflows { real: deps.workflows.clone(), drafts: drafts.clone() }),
            schedule: Arc::new(DrySchedule(deps.schedule.clone())),
        },
        sandbox.clone(),
    );
    // Runs, events, cards and verdicts live in a database of the run's own.
    let db = AgentDb::open(":memory:")?;
    let runs = crate::durable::RunStore::new(db.clone());
    let stop = cancel.child_token();
    let supervisor = create_supervisor_with_cancel(
        dry.clone(),
        EnvDeps {
            mcp,
            memory,
            user_email: session.env.user_email.clone(),
            telegram_chat_id: session.env.telegram_chat_id.clone(),
        },
        create_durable_flows(dry.clone(), db),
        stop.clone(),
    );
    let execution = supervisor.run_signal(&pending);
    tokio::pin!(execution);
    let mut timed_out = false;
    let result = tokio::select! {
        result = &mut execution => result,
        _ = tokio::time::sleep(DRY_RUN_TIMEOUT) => {
            timed_out = true;
            stop.cancel();
            execution.await
        }
    };
    // Its MCP and tracer are the sandbox's: this closes nothing real.
    dry.shutdown().await;
    let (status, output, error) = match result {
        Ok(output) if serde_json::from_str::<Value>(&output).is_ok_and(|o| o["resumes"] == true) => {
            ("suspended", Value::Null, json!("a run waited for something that never came; see the paths"))
        }
        Ok(output) => ("completed", json!(output), Value::Null),
        Err(_) if timed_out => ("timed_out", Value::Null, json!(format!("stopped after {DRY_RUN_TIMEOUT:?}"))),
        Err(error) if cancel.is_cancelled() => return Err(error),
        Err(error) => ("faulted", Value::Null, json!(error.to_string())),
    };
    if status == "completed" {
        for (draft, saved) in &drafts {
            if sandbox.ran(draft) {
                engine.deps.workflows.mark_tried(draft, saved.version)?;
            }
        }
    }
    let report = report(&sandbox, &runs, name, status, output, error, &pending, session);
    root.end(SpanEnd::output(
        json!({"status": report["status"], "output": report["output"], "error": report["error"]}),
    ));
    Ok(report.to_string())
}

// ── 6. the report ──────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
fn report(
    sandbox: &Sandbox,
    runs: &crate::durable::RunStore,
    name: &str,
    status: &str,
    output: Value,
    error: Value,
    signal: &PendingSignal,
    session: &Session,
) -> Value {
    let record = sandbox.lock();
    let drafts: Vec<Value> = sandbox
        .drafts
        .iter()
        .map(|(n, v)| json!({"name": n, "version": v, "ran": ran(&record, &sandbox.drafts, n)}))
        .collect();
    let unused: Vec<&str> =
        sandbox.drafts.keys().filter(|n| !ran(&record, &sandbox.drafts, n)).map(String::as_str).collect();
    let tree: Vec<Value> =
        record.runs.iter().filter(|r| r.parent.is_none()).map(|r| run_report(&record, r, runs, session)).collect();
    let primary = record
        .runs
        .iter()
        .find(|r| r.parent.is_none() && r.loop_id.is_some())
        .map(|r| ended(&record, r, runs))
        .unwrap_or(Value::Null);
    let mut next = "Compare this trace with what the user asked for: the path and each switch's case, every call's arguments and verdict, the mocked writes, how each loop ended and its output. Anything wrong is yours to fix: save a new version and try it again. When it does what was asked, describe to the user what it will do, the writes above all, and activate it only after they confirm.".to_owned();
    if !unused.is_empty() {
        next.push_str(&format!(
            " These drafts did not run, so they are not tried: {}. Check the signal (its source and payload) against the routes' `when` and the loop each route selects.",
            unused.join(", ")
        ));
    }
    json!({
        "name": name,
        "status": status,
        "output": output,
        "error": error,
        "ended": primary,
        "writes": record.writes,
        "drafts": drafts,
        "confirm": sandbox.answer.word(),
        "signal": {"source": signal.source, "content": crate::activity::clip(&signal.content, 500), "payload": signal.payload},
        "runs": tree,
        "note": format!("A dry run: model turns, reads and the guard ran for real. Every write was recorded, not made, and answered with {MOCK_REPLY} (marked mock); each confirmation was answered automatically ({}). Waits did not sleep, and nothing was kept: no run, card, message, memory, journal, schedule, skill or workflow changed.", sandbox.answer.word()),
        "next": next,
    })
}

fn run_report(record: &Record, run: &RunRecord, runs: &crate::durable::RunStore, session: &Session) -> Value {
    let mut out = json!({"run": run.id, "workflow": run.workflow, "version": run.version, "path": run.path});
    if run.omitted > 0 {
        out["omitted_steps"] = json!(run.omitted);
    }
    if let Some(loop_id) = &run.loop_id {
        out["turns"] = json!(turns(record, loop_id, session));
        out["ended"] = ended(record, run, runs);
    }
    let children: Vec<Value> = record
        .runs
        .iter()
        .filter(|r| r.parent.as_deref() == Some(run.id.as_str()))
        .map(|r| run_report(record, r, runs, session))
        .collect();
    if !children.is_empty() {
        out["children"] = json!(children);
    }
    out
}

// A loop's turns, each with its calls as they ran: the verdict, the card's
// answer, the reply (a mock for a write; a large one in working memory).
fn turns(record: &Record, loop_id: &str, session: &Session) -> Vec<Value> {
    record
        .turns
        .iter()
        .filter(|t| t["loop"] == loop_id)
        .map(|turn| {
            let mut turn = turn.clone();
            let n = turn["turn"].as_u64();
            let fields = turn.as_object_mut().expect("object");
            fields.shift_remove("loop");
            for call in fields.get_mut("calls").and_then(Value::as_array_mut).into_iter().flatten() {
                let Some(ran) = record
                    .calls
                    .iter()
                    .rev()
                    .find(|c| c["loop"] == loop_id && c["round"].as_u64() == n && c["id"] == call["id"])
                else {
                    call["result"] = json!("[not run]");
                    continue;
                };
                if let Some(verdict) = ran.get("verdict") {
                    call["verdict"] = verdict.clone();
                }
                if let Some(answer) =
                    record.answers.get(&(loop_id.to_owned(), ran["key"].as_str().unwrap_or_default().to_owned()))
                {
                    call["confirmation"] = json!(answer);
                }
                call["ok"] = ran["ok"].clone();
                if ran["mock"] == true {
                    call["mock"] = json!(true);
                }
                call["result"] = result(ran["raw"].as_str().unwrap_or_default(), session);
            }
            turn
        })
        .collect()
}

fn result(raw: &str, session: &Session) -> Value {
    if raw.chars().count() <= RESULT_INLINE_CHARS {
        return crate::flows::parse_tool_json(raw);
    }
    let stored = crate::tool_results::store_tool_result(&session.memory, raw);
    json!({"memory_key": stored.memory_key, "size_bytes": stored.size_bytes,
        "preview": crate::activity::clip(raw, 500)})
}

// How a loop run ended: by `finish`, by a text answer, by a rule of its
// workflow (the step it ended at), at its turn limit, or by a fault.
fn ended(record: &Record, run: &RunRecord, runs: &crate::durable::RunStore) -> Value {
    let loop_id = run.loop_id.as_deref().unwrap_or_default();
    let turns: Vec<&Value> = record.turns.iter().filter(|t| t["loop"] == loop_id).collect();
    let nudged = turns.iter().any(|t| t["after_nudge"] == true);
    let last = run.path.iter().rev().find(|s| !s.ends_with("(skipped)")).cloned();
    let outcome = runs.get(&run.id).ok().flatten().and_then(|r| r.outcome);
    let mut out = match outcome {
        Some(Status::Completed(output)) => {
            let by = if record.finished.get(loop_id) == Some(&true) {
                "finish"
            } else if turns.last().is_some_and(|t| t["calls"].as_array().is_some_and(Vec::is_empty)) {
                "answer"
            } else {
                "workflow rule"
            };
            json!({"by": by, "at": last, "output": output})
        }
        Some(Status::Faulted(error)) => {
            let by = if error.to_string().contains("exceeded maxIterations") { "limit" } else { "fault" };
            json!({"by": by, "at": last, "error": error.to_string()})
        }
        Some(Status::Cancelled) => json!({"by": "cancelled", "at": last}),
        _ => json!({"by": "unfinished", "at": last}),
    };
    out["turns"] = json!(turns.len());
    if nudged {
        out["nudged"] = json!(true);
    }
    out
}
