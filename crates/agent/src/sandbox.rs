// The dry-run sandbox (docs/designs/05-sandbox.md): `try_workflow` with a
// signal runs it through the real chain (dispatcher → route → loop → guard
// → workers) as if it had arrived, with the drafts it names in place of the
// active versions; without a signal it runs one draft alone, on a scheduled
// fire's input. Model turns, reads (the internet's and MCP's) and the guard
// are real; every write is recorded and answered with a mock, a
// confirmation is answered on the spot instead of sent, and nothing
// persists. Sections: 1. the sandbox and what it records; 2. the dry
// engine's stores; 3. the guard store and the drafts' overlay; 4. the
// observer; 5. the signal types; 6. try_workflow; 7. the report.
//
// It runs on the engine's dry run (`Runtime::dry_run_opts`, ouro design 07)
// of a second agent `Engine` built per try and never started: its deps are
// the dry ones below, an in-memory agent.db, and as its run store a guard
// store that answers definitions (with the drafts overlaid as active) and
// refuses every run read and write. The dry run's runs live in a private
// store the handlers reach through their call's run access (functions.rs).
// The domains ask `Engine::sandbox` only where a dry run differs in kind
// (design 05, decision 12). The builder judges the trace; the sandbox has no
// expectations of its own.
use crate::{
    agent::{Engine, EngineDeps, Providers, create_sandbox_engine},
    confirmation::{Answer, Request},
    db::{AgentDb, MemoryStore},
    flows::{DRY_RUN_TIMEOUT, WorkflowMeta},
    mcp::{Mcp, SharedMcp, ToolCallFailure},
    providers::{self, Message, ToolCall, ToolDef},
    runs::{Choice, Fallback, Site, Status},
    schedule::{Fire, FireStore, Schedules, Task},
    session::{EnvConfig, Session},
    skills::{SkillEntry, SkillFile, SkillStore, Written},
    supervisor::PendingSignal,
    tracing::{
        ContextUpdate, EventStart, Generation, GenerationStart, Scope, Span, SpanEnd, SpanKind, SpanStart, Trace,
        TraceAnchor, TraceContext, TraceStart, Tracer,
    },
};
use async_trait::async_trait;
use ouro_runtime::{
    DryRun, DryRunOptions, DryRunRun, FunctionContext, FunctionError, Mock, Output,
    durable::{
        Activation, Admission, Arming, Breaker, ChildLink, Compatibility, Definition, Definitions, Fence, Fired,
        Firing, Policy, Rearm, RunSnapshot, RunStore, SavedRun, Swept, Trigger, control,
    },
    observer::{Event as RunEvent, EventKind, Observer, RunOutcome, TaskOutcome},
    ows::durable::Record as RunRecord,
};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

// ── 1. the sandbox ─────────────────────────────────────────────────────────

/// What every write of a dry run on a signal answers, MCP's and the
/// runtime's alike. A loop rule that read a write's reply would see this
/// instead of the real one, so rules decide on a call's arguments and `ok`
/// (workflow-author).
pub const MOCK_REPLY: &str = r#"{"mock":true,"note":"dry run: not executed","delivered":true}"#;

/// What a write answers in a dry run without a signal: `main`'s stub, with
/// `ok`, which saved workflows (skill-index) check (design 05, deviation 2).
pub const DRAFT_REPLY: &str = r#"{"ok":true,"dryRun":true}"#;

// A run's path keeps this many steps; the rest are counted.
const PATH_MAX: usize = 200;
// The steps a draft's report shows at most (without a signal).
const STEPS_MAX: usize = 40;
const STEP_OUTPUT_CHARS: usize = 2000;
// A reply longer than this goes to the builder's working memory, with a
// preview in the report, as any large tool result does.
const RESULT_INLINE_CHARS: usize = 1500;
// Model text and reasoning per turn, each argument of a call.
const TEXT_MAX_CHARS: usize = 3000;
const ARGUMENT_MAX_CHARS: usize = 3000;

/// How a try runs: its time budget (`DRY_RUN_TIMEOUT`), and a test-only
/// hook (`stray_read`: a handler reads its run through the engine's own
/// handle, which the guard store refuses and counts; design 05, test 19).
#[derive(Clone, Debug)]
pub struct TryOptions {
    pub timeout: Duration,
    #[doc(hidden)]
    pub stray_read: bool,
}

impl Default for TryOptions {
    fn default() -> Self {
        TryOptions { timeout: DRY_RUN_TIMEOUT, stray_read: false }
    }
}

/// A draft of a try: a saved version of a name, overlaid as its active one.
#[derive(Clone, Debug)]
pub struct Draft {
    pub version: u64,
    pub digest: String,
    pub yaml: String,
}

/// Where a recorded write came from (design 05, decision 6): a workflow's
/// own call (the engine's mock), a model's call (`tool`), or a handler's
/// own MCP call (the dry MCP).
#[derive(Clone, Copy, Debug)]
pub enum Via {
    Yaml,
    Tool,
    Handler,
}

impl Via {
    fn word(self) -> &'static str {
        match self {
            Via::Yaml => "yaml",
            Via::Tool => "tool",
            Via::Handler => "handler",
        }
    }
}

/// A root start a dry run records instead of making (the recovery report,
/// decision 13): run later as a dry run of its own.
pub struct Start {
    pub id: String,
    pub choice: Choice,
    pub input: Value,
    pub host: Value,
}

pub struct Sandbox {
    answer: Answer,
    drafts: BTreeMap<String, Draft>,
    reply: &'static str,
    stray_read: bool,
    refusals: Arc<AtomicUsize>,
    record: Mutex<Record>,
}

// What a dry run did, in the order it happened.
#[derive(Default)]
struct Record {
    turns: Vec<Value>,
    calls: Vec<Value>,
    // A loop's last settled round: whether `finish` ended it.
    finished: HashMap<String, bool>,
    // The answer given to each card, by loop and call key.
    answers: HashMap<(String, String), String>,
    writes: Vec<Value>,
    paths: HashMap<String, Path>,
    // The calls each run made, with their answers (its steps).
    steps: HashMap<String, Vec<Value>>,
    // A version set aside, by the run that ran in its place; and every
    // version set aside at a start.
    fallbacks: HashMap<String, Fallback>,
    set_aside: Vec<Fallback>,
    recovery: Option<Start>,
}

#[derive(Default)]
struct Path {
    steps: Vec<String>,
    omitted: usize,
}

impl Sandbox {
    fn new(answer: Answer, drafts: BTreeMap<String, Draft>, reply: &'static str, stray_read: bool) -> Arc<Sandbox> {
        Arc::new(Sandbox { answer, drafts, reply, stray_read, refusals: Arc::default(), record: Mutex::default() })
    }

    fn lock(&self) -> MutexGuard<'_, Record> {
        self.record.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[doc(hidden)]
    pub fn stray_read(&self) -> bool {
        self.stray_read
    }

    /// Records a write that is not made and returns what it answers.
    pub fn mock(&self, via: Via, run: Option<&str>, task: Option<&str>, function: &str, arguments: Value) -> String {
        self.lock().writes.push(json!({"run": run, "task": task, "function": function,
            "arguments": clip_arguments(arguments), "via": via.word()}));
        self.reply.to_owned()
    }

    /// A model turn of loop `loop_id`, the `n`th (its calls run as round `n`).
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
            "verdict": verdict, "ok": ok, "mock": raw == self.reply, "raw": raw});
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
    pub async fn press(
        &self,
        db: &AgentDb,
        ctx: &FunctionContext,
        req: Request<'_>,
        loop_id: &str,
        scope: &Scope,
    ) -> anyhow::Result<Value> {
        let line = format!("confirmation requested: {} → auto-{}", req.call.name, self.answer.word());
        let key = crate::guard::key(req.call);
        let requested = crate::confirmation::press(db, ctx, req, self.answer).await?;
        scope.event(EventStart { name: line.clone(), ..Default::default() });
        self.lock().answers.insert((loop_id.into(), key), line);
        Ok(requested)
    }

    /// A version set aside, with the run that ran in its place.
    pub fn fallback(&self, run: &str, fallback: &Fallback) {
        self.lock().fallbacks.insert(run.into(), fallback.clone());
    }

    /// A version set aside at a start (whatever ran in its place).
    pub fn set_aside(&self, fallback: &Fallback) {
        self.lock().set_aside.push(fallback.clone());
    }

    /// The recovery report's start, made after the signal's dry run.
    pub fn recovery(&self, start: Start) {
        self.lock().recovery.get_or_insert(start);
    }

    fn path(&self, run: &str, step: Step) {
        let mut record = self.lock();
        let path = record.paths.entry(run.into()).or_default();
        match step {
            Step::Case(task, case) => {
                if let Some(entry) = path.steps.iter_mut().rev().find(|e| **e == task) {
                    *entry = format!("{task} → {case}");
                }
            }
            Step::Add(entry) if path.steps.len() < PATH_MAX => path.steps.push(entry),
            Step::Add(_) => path.omitted += 1,
        }
    }

    fn step(&self, run: &str, step: Value) {
        self.lock().steps.entry(run.into()).or_default().push(step);
    }

    fn refusals(&self) -> usize {
        self.refusals.load(Ordering::SeqCst)
    }
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

// A value as a report shows it: whole while short, else clipped JSON text.
fn cut(value: &Value, max: usize) -> Value {
    let text = match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    };
    if text.chars().count() <= max { value.clone() } else { json!(crate::activity::clip(&text, max)) }
}

// The engine's mock (decision 6): an MCP tool of the registry not marked
// read-only, `set_memory`, `deliver_event` (it delivers into a real run) and
// `fire_scheduled` (a try never records a fire, design 06 decision 5) are
// answered by the sandbox; every other function dispatches.
struct DryMock {
    sandbox: Arc<Sandbox>,
    writes: BTreeSet<String>,
}

impl Mock for DryMock {
    fn mocks(&self, function: &str, _arguments: &Value) -> bool {
        self.writes.contains(function) || matches!(function, "set_memory" | "deliver_event" | "fire_scheduled")
    }
    fn response(&self, function: &str, arguments: &Value, ctx: &FunctionContext) -> Result<Output, FunctionError> {
        let reply = self.sandbox.mock(Via::Yaml, Some(&ctx.run_id), Some(&ctx.task_path), function, arguments.clone());
        Ok(serde_json::from_str::<Value>(&reply).unwrap_or(Value::String(reply)).into())
    }
}

// ── 2. the dry engine's stores ─────────────────────────────────────────────

// MCP as a dry run sees it: a tool the server marks read-only runs; any
// other is recorded and answered with the mock. The last line of defence
// for a handler's own write (the engine's mock and `tool` stop the others).
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
        Ok(self.sandbox.mock(Via::Handler, None, None, name, Value::Object(args)))
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
        Ok(self.sandbox.mock(Via::Handler, None, None, name, Value::Object(args)))
    }
    // The session is the agent's; a dry run never closes it.
    async fn close(&self) {}
}

// The memory KV: reads see the real values, writes stay in the try.
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

// The fire table as a dry run sees it (R2-L8): the real one read through
// (`list_scheduled_tasks` shows the real last fires), every write refused.
// A try never records a fire: `fire_scheduled` is mocked (`DryMock`), and a
// model's `schedule_task` is a mocked write.
struct DryFires(Arc<dyn FireStore>);

#[async_trait]
impl FireStore for DryFires {
    async fn record(&self, _: &str, _: &Task, _: i64) -> anyhow::Result<i64> {
        anyhow::bail!(NO_WRITES)
    }
    async fn next_pending(&self) -> anyhow::Result<Option<Fire>> {
        self.0.next_pending().await
    }
    async fn done(&self, _: i64) -> anyhow::Result<()> {
        anyhow::bail!(NO_WRITES)
    }
    async fn latest(&self, task_id: i64) -> anyhow::Result<Option<Fire>> {
        self.0.latest(task_id).await
    }
    async fn next_task_id(&self) -> anyhow::Result<i64> {
        anyhow::bail!(NO_WRITES)
    }
    async fn seed_id(&self) -> anyhow::Result<i64> {
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

// The engine of one try (decision 1): the real engine's providers, presets,
// skills, codex and decisions; the dry stores; a tracer nested under `root`;
// the builder session's env (R-L4); an in-memory agent.db holding the MCP
// tool snapshot and `workflow_meta`; and the guard store with the drafts.
fn sandbox_engine(
    real: &Engine,
    sandbox: &Arc<Sandbox>,
    root: Arc<dyn Span>,
    env: EnvConfig,
) -> anyhow::Result<Arc<Engine>> {
    let deps = &real.deps;
    let db = AgentDb::open(":memory:")?;
    db.snapshot_tools(&deps.db.tool_snapshot()?)?;
    deps.db.copy_workflow_meta(&db)?;
    let store = Arc::new(GuardStore {
        view: deps.dry_store.clone(),
        drafts: sandbox.drafts.clone(),
        refusals: sandbox.refusals.clone(),
    });
    let deps = EngineDeps {
        providers: Providers {
            deepseek: deps.providers.deepseek.clone(),
            openai: deps.providers.openai.clone(),
            gemini: deps.providers.gemini.clone(),
            openrouter: deps.providers.openrouter.clone(),
        },
        mcp: Arc::new(DryMcp { real: deps.mcp.clone(), sandbox: sandbox.clone() }),
        presets: deps.presets.clone(),
        skills: deps.skills.clone(),
        skill_store: Arc::new(DrySkills(deps.skill_store.clone())),
        memory: Arc::new(DryMemory { real: deps.memory.clone(), kept: Mutex::default() }),
        tracer: Arc::new(Nested(root)),
        codex: deps.codex.clone(),
        decisions: deps.decisions.clone(),
        schedule: Arc::new(Schedules::new(Arc::new(DryFires(deps.schedule.fires.clone())))),
        env,
        db,
        store,
        dry_store: deps.dry_store.clone(),
    };
    Ok(create_sandbox_engine(deps, sandbox.clone()))
}

// ── 3. the guard store and the drafts' overlay ─────────────────────────────

const USE_RUN_ACCESS: &str = "sandbox: use the call's run access";

// The sandbox engine's run store (decisions 3 and 4): definitions come from
// a read-only view of the real store, with each listed draft answered as its
// name's active version (its trip shown only when an operator paused the
// name or the draft is the tripped content); every run read and every write
// is refused and counted, so a handler that reads through the engine's own
// handle instead of its call's run access fails loudly and the report says
// how often.
struct GuardStore {
    view: Arc<dyn RunStore>,
    drafts: BTreeMap<String, Draft>,
    refusals: Arc<AtomicUsize>,
}

impl GuardStore {
    fn refuse<T>(&self) -> anyhow::Result<T> {
        self.refusals.fetch_add(1, Ordering::SeqCst);
        Err(anyhow::anyhow!(USE_RUN_ACCESS))
    }

    // A draft is its name's active version; the trip it meets is an
    // operator's pause or a trip of its own content. Any other is hidden: the
    // engine admits other content, and a try is how a fix becomes tried.
    fn overlay(&self, name: &str, active: &mut Option<u64>, tripped: &mut Option<ouro_runtime::durable::Trip>) {
        let Some(draft) = self.drafts.get(name) else { return };
        *active = Some(draft.version);
        if tripped.as_ref().is_some_and(|t| t.manual.is_none() && t.digest != draft.digest) {
            *tripped = None;
        }
    }
}

#[async_trait]
impl RunStore for GuardStore {
    async fn apply_control(
        &self,
        _: &control::Request,
        _: &control::Command,
        _: i64,
    ) -> anyhow::Result<control::Applied> {
        self.refuse()
    }
    async fn stopped(&self) -> anyhow::Result<bool> {
        self.refuse()
    }
    async fn control_entry(&self, _: &str) -> anyhow::Result<Option<control::Entry>> {
        self.refuse()
    }
    async fn control_history(
        &self,
        _: Option<&str>,
        _: Option<&control::Cursor>,
        _: usize,
    ) -> anyhow::Result<control::Page<control::Entry>> {
        self.refuse()
    }
    async fn query_runs(
        &self,
        _: &control::Runs,
        _: bool,
        _: Option<&control::Cursor>,
        _: usize,
    ) -> anyhow::Result<control::Page<SavedRun>> {
        self.refuse()
    }
    async fn query_definitions(
        &self,
        cursor: Option<&control::Cursor>,
        limit: usize,
    ) -> anyhow::Result<control::Page<control::DefinitionSummary>> {
        let mut page = self.view.query_definitions(cursor, limit).await?;
        for summary in &mut page.items {
            let name = summary.name.clone();
            self.overlay(&name, &mut summary.active, &mut summary.tripped);
        }
        Ok(page)
    }
    async fn definition_control(&self, name: &str) -> anyhow::Result<Option<control::DefinitionSummary>> {
        let mut summary = self.view.definition_control(name).await?;
        if let Some(summary) = &mut summary {
            self.overlay(name, &mut summary.active, &mut summary.tripped);
        }
        Ok(summary)
    }
    async fn pending_waits(
        &self,
        _: &control::Waits,
        _: Option<&control::Cursor>,
        _: usize,
    ) -> anyhow::Result<control::Page<control::Wait>> {
        self.refuse()
    }
    async fn inspect_control(&self, _: &str) -> anyhow::Result<Option<control::Inspection>> {
        self.refuse()
    }
    async fn totals(&self, _: &str, _: i64, _: i64) -> anyhow::Result<control::DefinitionTotals> {
        self.refuse()
    }
    async fn beat(&self, _: &control::Worker) -> anyhow::Result<()> {
        self.refuse()
    }
    async fn workers(&self, _: Option<&control::Cursor>, _: usize) -> anyhow::Result<control::Page<control::Worker>> {
        self.refuse()
    }
    async fn describe_wait(
        &self,
        _: &Fence,
        _: &str,
        _: &ouro_runtime::ows::events::PendingWait,
        _: i64,
    ) -> anyhow::Result<()> {
        self.refuse()
    }
    fn policy(&self) -> Policy {
        self.view.policy()
    }
    async fn admit(&self, _: &Admission, _: i64) -> anyhow::Result<bool> {
        self.refuse()
    }
    async fn start(&self, _: &Admission, _: i64) -> anyhow::Result<Option<Fence>> {
        self.refuse()
    }
    async fn claim(&self, _: &str, _: i64) -> anyhow::Result<Option<Fence>> {
        self.refuse()
    }
    async fn claim_as(&self, _: &str, _: i64, _: &Compatibility) -> anyhow::Result<Option<Fence>> {
        self.refuse()
    }
    async fn check_lease(&self, _: &Fence, _: i64) -> anyhow::Result<()> {
        self.refuse()
    }
    async fn renew(&self, _: &Fence, _: i64) -> anyhow::Result<()> {
        self.refuse()
    }
    async fn release(&self, _: &Fence, _: i64) -> anyhow::Result<()> {
        self.refuse()
    }
    async fn load_records(&self, _: &Fence, _: i64) -> anyhow::Result<BTreeMap<String, RunRecord>> {
        self.refuse()
    }
    async fn save_record(&self, _: &Fence, _: &str, _: &RunRecord, _: i64) -> anyhow::Result<()> {
        self.refuse()
    }
    async fn register_wait(&self, _: &Fence, _: &[String], _: i64) -> anyhow::Result<()> {
        self.refuse()
    }
    async fn unregister_wait(&self, _: &Fence, _: &str, _: &[String], _: i64) -> anyhow::Result<()> {
        self.refuse()
    }
    async fn deliver(&self, _: &Value, _: Option<&str>, _: i64) -> anyhow::Result<Option<String>> {
        self.refuse()
    }
    async fn take_event(&self, _: &Fence, _: &str, _: &[String], _: i64) -> anyhow::Result<Option<Value>> {
        self.refuse()
    }
    async fn subscribe(&self, _: &Fence, _: &str, _: &[String], _: i64) -> anyhow::Result<()> {
        self.refuse()
    }
    async fn unsubscribe(&self, _: &Fence, _: &str, _: i64) -> anyhow::Result<()> {
        self.refuse()
    }
    async fn publish(&self, _: &Value, _: Option<&str>, _: i64) -> anyhow::Result<Vec<String>> {
        self.refuse()
    }
    async fn ingested(&self, _: &Value, _: i64) -> anyhow::Result<bool> {
        self.refuse()
    }
    async fn trigger(&self, _: &str) -> anyhow::Result<Option<Trigger>> {
        self.refuse()
    }
    async fn due_triggers(&self, _: i64, _: usize) -> anyhow::Result<Vec<(String, Trigger)>> {
        self.refuse()
    }
    async fn triggers_for(&self, _: &str, _: usize) -> anyhow::Result<Vec<(String, Trigger)>> {
        self.refuse()
    }
    async fn queued(&self, _: &str) -> anyhow::Result<Option<Admission>> {
        self.refuse()
    }
    async fn fire(&self, _: &Firing<'_>, _: i64) -> anyhow::Result<Fired> {
        self.refuse()
    }
    async fn swap_trigger(
        &self,
        _: &str,
        _: Option<&Trigger>,
        _: Option<&Trigger>,
        _: &[Admission],
        _: Option<&Rearm>,
        _: i64,
    ) -> anyhow::Result<bool> {
        self.refuse()
    }
    async fn finish(&self, _: &Fence, _: &Status, _: i64, _: u32) -> anyhow::Result<Status> {
        self.refuse()
    }
    async fn start_child(&self, _: &Fence, _: &Admission, _: &ChildLink, _: i64) -> anyhow::Result<Option<Fence>> {
        self.refuse()
    }
    async fn child_outcome(&self, _: &str, _: &str, _: &str) -> anyhow::Result<Option<Status>> {
        self.refuse()
    }
    async fn spend(&self, _: &Fence, _: &str, _: u64, _: i64) -> anyhow::Result<RunRecord> {
        self.refuse()
    }
    async fn due(&self, _: i64, _: usize) -> anyhow::Result<Vec<String>> {
        self.refuse()
    }
    async fn due_as(&self, _: i64, _: usize, _: &Compatibility) -> anyhow::Result<Vec<String>> {
        self.refuse()
    }
    async fn cancel(&self, _: &str, _: i64) -> anyhow::Result<bool> {
        self.refuse()
    }
    async fn sweep(&self, _: i64, _: i64) -> anyhow::Result<Swept> {
        self.refuse()
    }
    async fn get(&self, _: &str) -> anyhow::Result<Option<SavedRun>> {
        self.refuse()
    }
    async fn list(&self, _: Option<&str>, _: &str) -> anyhow::Result<Vec<SavedRun>> {
        self.refuse()
    }
    async fn inspect(&self, _: &str) -> anyhow::Result<Option<RunSnapshot>> {
        self.refuse()
    }
    async fn save_definition(&self, _: &str, _: &str, _: Breaker) -> anyhow::Result<Definition> {
        self.refuse()
    }
    async fn activate(&self, _: &str, _: u64, _: i64) -> anyhow::Result<()> {
        self.refuse()
    }
    async fn activate_with(
        &self,
        _: &str,
        _: u64,
        _: u64,
        _: &Arming,
        _: &Activation,
    ) -> anyhow::Result<Option<Definitions>> {
        self.refuse()
    }
    async fn definitions(&self, name: &str) -> anyhow::Result<Definitions> {
        let mut definitions = self.view.definitions(name).await?;
        self.overlay(name, &mut definitions.active, &mut definitions.tripped);
        Ok(definitions)
    }
    async fn notices(&self, _: usize) -> anyhow::Result<Vec<(String, Value)>> {
        self.refuse()
    }
    async fn acknowledge(&self, _: &str) -> anyhow::Result<bool> {
        self.refuse()
    }
}

// ── 4. the observer ────────────────────────────────────────────────────────

enum Step {
    Add(String),
    // A switch's task and the case it took.
    Case(String, String),
}

fn task(reference: &str) -> &str {
    reference.rsplit('/').next().unwrap_or(reference)
}

// The dry run's own observer (decisions 8 and 9): every step of a run as one
// line (a task entered or skipped, the case each switch took, a fault where
// it happened, a retry), each call's answer, and each run's end, handed to
// the sandbox's lifecycle task: a never started engine has no drain, and
// `Observer` is synchronous.
struct Watch {
    sandbox: Arc<Sandbox>,
    ended: Mutex<Option<tokio::sync::mpsc::UnboundedSender<(String, Status)>>>,
}

impl Watch {
    // No more ends: the lifecycle task drains what it has and stops.
    fn close(&self) {
        self.ended.lock().unwrap_or_else(|e| e.into_inner()).take();
    }
}

impl Observer for Watch {
    fn observe(&self, event: &RunEvent) {
        let reference = event.task_path.as_deref().unwrap_or_default();
        let step = match &event.kind {
            EventKind::TaskEntered { kind, .. } if kind != "do" => Step::Add(task(reference).into()),
            EventKind::TaskSkipped { .. } => Step::Add(format!("{} (skipped)", task(reference))),
            EventKind::SwitchTaken { case } => Step::Case(task(reference).into(), case.clone()),
            // A fault is reported where it happened, not by every task it passes.
            EventKind::TaskExited { outcome: TaskOutcome::Faulted { error } }
                if error.instance.as_deref().is_none_or(|i| i == reference) =>
            {
                Step::Add(format!("{} faulted: {error}", task(reference)))
            }
            EventKind::RetryScheduled { attempt, error, .. } => {
                Step::Add(format!("{} retry {attempt}: {error}", task(reference)))
            }
            EventKind::CallFinished { function, result, error, .. } => {
                let output = match (result, error) {
                    (Some(result), _) => cut(result, STEP_OUTPUT_CHARS),
                    (None, Some(error)) => json!({"error": error.to_string()}),
                    (None, None) => Value::Null,
                };
                self.sandbox.step(&event.run_id, json!({"task": reference, "function": function, "output": output}));
                return;
            }
            EventKind::RunEnded { outcome, .. } => {
                let status = match outcome {
                    RunOutcome::Completed { output } => Status::Completed(output.clone().unwrap_or(Value::Null)),
                    RunOutcome::Faulted { error } => Status::Faulted(error.clone()),
                    RunOutcome::Cancelled => Status::Cancelled,
                };
                if let Some(ended) = &*self.ended.lock().unwrap_or_else(|e| e.into_inner()) {
                    let _ = ended.send((event.run_id.clone(), status));
                }
                return;
            }
            _ => return,
        };
        self.sandbox.path(&event.run_id, step);
    }
}

// ── 5. the signal types ────────────────────────────────────────────────────

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

// ── 6. try_workflow ────────────────────────────────────────────────────────

// The id a dry signal carries: it names its runs and its session.
const DRY_SIGNAL_ID: i64 = 0;

/// `try_workflow`: with `signal`, the signal through the chain
/// (`try_signal`); without, the draft alone (`try_draft`).
pub async fn try_workflow(
    engine: &Arc<Engine>,
    args: &Map<String, Value>,
    session: &Session,
    scope: &Scope,
    cancel: &CancellationToken,
) -> anyhow::Result<String> {
    if args.get("signal").is_some_and(|s| !s.is_null()) {
        try_signal(engine, args, session, scope, cancel).await
    } else {
        try_draft(engine, args, session, scope, cancel).await
    }
}

/// `try_workflow(name, signal, drafts?, confirm?, version?)` with the
/// default options (`try_signal_with`).
pub async fn try_signal(
    engine: &Arc<Engine>,
    args: &Map<String, Value>,
    session: &Session,
    scope: &Scope,
    cancel: &CancellationToken,
) -> anyhow::Result<String> {
    try_signal_with(engine, args, session, scope, cancel, TryOptions::default()).await
}

// The drafts a try names (R-L1): `name` at `version`, else each name's latest
// version the agent saved (`workflow_meta.builtin = 0`). A built-in name with
// no such version is no draft: it runs its active version (trying `signal`
// or `agent-loop` as it is is allowed for `name`). `guard` is refused, and
// each draft passes the gate.
async fn resolve_drafts(
    engine: &Engine,
    name: &str,
    version: Option<u64>,
    args: &Map<String, Value>,
) -> anyhow::Result<BTreeMap<String, Draft>> {
    let mut names = vec![name.to_owned()];
    for draft in args.get("drafts").and_then(Value::as_array).into_iter().flatten() {
        let draft =
            draft.as_str().filter(|d| !d.is_empty()).ok_or_else(|| anyhow::anyhow!("drafts: Expected names"))?;
        if !names.iter().any(|n| n == draft) {
            names.push(draft.to_owned());
        }
    }
    let mut drafts = BTreeMap::new();
    for draft in &names {
        anyhow::ensure!(draft != crate::flows::GUARD_NAME, "guard is a reserved built-in workflow and has no drafts");
        let definitions = engine.runtime.definitions(draft).await?;
        let chosen = match version.filter(|_| draft == name) {
            Some(version) => definitions.versions.get(&version),
            None => {
                let mut chosen = None;
                for d in definitions.versions.values().rev() {
                    if WorkflowMeta::get(&engine.deps.db, draft, &d.digest)?.is_some_and(|m| !m.builtin) {
                        chosen = Some(d);
                        break;
                    }
                }
                chosen
            }
        };
        let Some(d) = chosen else {
            if draft == name && version.is_none() && crate::flows::built_in(name).is_some() {
                continue;
            }
            anyhow::bail!("no saved workflow `{draft}`");
        };
        crate::flows::gate(engine, draft, &d.yaml).map_err(|e| anyhow::anyhow!("`{draft}`: {e}"))?;
        drafts.insert(draft.clone(), Draft { version: d.version, digest: d.digest.clone(), yaml: d.yaml.clone() });
    }
    Ok(drafts)
}

fn version_arg(args: &Map<String, Value>) -> anyhow::Result<Option<u64>> {
    match args.get("version") {
        None | Some(Value::Null) => Ok(None),
        Some(v) => Ok(Some(
            v.as_u64().filter(|v| *v > 0).ok_or_else(|| anyhow::anyhow!("version: Expected positive integer"))?,
        )),
    }
}

fn name_arg(args: &Map<String, Value>) -> anyhow::Result<&str> {
    let name = args.get("name").and_then(Value::as_str).filter(|n| !n.is_empty());
    name.ok_or_else(|| anyhow::anyhow!("name: Expected nonempty string"))
}

fn metadata(value: Value) -> Map<String, Value> {
    value.as_object().cloned().unwrap_or_default()
}

fn env_of(session: &Session) -> EnvConfig {
    EnvConfig { user_email: session.env.user_email.clone(), telegram_chat_id: session.env.telegram_chat_id.clone() }
}

/// `try_workflow(name, signal, drafts?, confirm?, version?)`: runs `signal`
/// as if it had just arrived, through the dispatcher and whatever it routes
/// to, with the drafts (`name` and each of `drafts`) overlaid as the active
/// versions. The reply is the report (§7). A completed run marks every
/// draft whose runs all completed as tried, which activation requires.
pub async fn try_signal_with(
    engine: &Arc<Engine>,
    args: &Map<String, Value>,
    session: &Session,
    scope: &Scope,
    cancel: &CancellationToken,
    options: TryOptions,
) -> anyhow::Result<String> {
    let name = name_arg(args)?;
    let signal =
        args.get("signal").and_then(Value::as_object).ok_or_else(|| anyhow::anyhow!("signal: Expected an object"))?;
    // Discovery is a read, not a failed run. It must work before any draft
    // exists and must not mark drafts tried or start a model/guard session.
    if signal.is_empty() {
        return Ok(json!({
            "status": "signal_types", "signal_types": signal_types(&*engine.deps.mcp, cancel).await?,
            "next": "Copy a real signal's shape, replace its message, then call try_workflow with signal.source, signal.content and any drafts of the chain.",
        })
        .to_string());
    }
    let version = version_arg(args)?;
    let answer = Answer::parse(match args.get("confirm") {
        None | Some(Value::Null) => None,
        Some(v) => Some(v.as_str().ok_or_else(|| anyhow::anyhow!("confirm: Expected a string"))?),
    })?;
    let drafts = resolve_drafts(engine, name, version, args).await?;
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

    let sandbox = Sandbox::new(answer, drafts, MOCK_REPLY, options.stray_read);
    let listed: BTreeMap<&String, u64> = sandbox.drafts.iter().map(|(n, d)| (n, d.version)).collect();
    let root = scope.span(
        SpanStart::new("dry-run", SpanKind::Chain)
            .input(json!({"signal": signal, "confirm": answer.word()}))
            .metadata(metadata(json!({"dry_run": true, "workflow": name, "drafts": listed}))),
    );
    let dry = sandbox_engine(engine, &sandbox, root.clone(), env_of(session))?;
    // The signal's trace, as the supervisor's, a span of the builder's.
    let trace = root.span(
        SpanStart::new(format!("signal:{source}"), SpanKind::Agent)
            .input(json!(content))
            .metadata(metadata(json!({"signal_id": DRY_SIGNAL_ID, "signal_source": source, "dry_run": true}))),
    );
    let run = crate::functions::signal_run(DRY_SIGNAL_ID);
    // The root as live (decision 5): the same choice, the same admission.
    let admitted = async {
        let choice = crate::runs::choose(&dry, crate::supervisor::SIGNAL_NAME, Site::Signal).await?;
        let build = crate::supervisor::signal_admission(&dry, &pending, trace.anchor()).await?;
        let (input, host) = build(choice.clone()).await?;
        anyhow::Ok((choice, input, host))
    }
    .await;
    let budget = tokio::time::Instant::now() + options.timeout;
    let (outcome, choice) = match admitted {
        Ok((choice, input, host)) => {
            if let Some(fallback) = &choice.fallback {
                crate::runs::trace_fallback(trace.clone(), fallback, true);
            }
            dry.scopes.borrow(&run, trace.clone());
            let ran =
                dry_run(&dry, &sandbox, &choice.yaml, input, &run, &choice.name, Some(host), budget, cancel).await;
            (ran, Some(choice))
        }
        Err(error) => (Ran::Failed(error), None),
    };
    if matches!(outcome, Ran::Cancelled) {
        close(&dry).await;
        trace.end(SpanEnd::error(crate::errors::aborted().to_string()));
        root.end(SpanEnd::error(crate::errors::aborted().to_string()));
        return Err(crate::errors::aborted());
    }
    // The recovery report a faulted signal asked for: a dry run of its own
    // (decision 13), under the same budget, its writes its own.
    let signal_writes = sandbox.lock().writes.len();
    let recovery = sandbox.lock().recovery.take();
    let recovery = match recovery {
        Some(start) => {
            let Start { id, choice, input, host } = start;
            let ran = dry_run(&dry, &sandbox, &choice.yaml, input, &id, &choice.name, Some(host), budget, cancel).await;
            Some((ran, choice))
        }
        None => None,
    };
    close(&dry).await;
    if matches!(recovery, Some((Ran::Cancelled, _))) {
        trace.end(SpanEnd::error(crate::errors::aborted().to_string()));
        root.end(SpanEnd::error(crate::errors::aborted().to_string()));
        return Err(crate::errors::aborted());
    }
    let status = outcome.status(options.timeout);
    let runs = outcome.runs();
    if status.word == "completed" {
        mark_tried(engine, &sandbox, &runs)?;
    }
    trace.update(ContextUpdate::output(json!({"status": status.word, "output": status.output, "error": status.error})));
    trace.end(SpanEnd::default());
    let mut report = report(&sandbox, name, &status, &runs, choice.as_ref(), session);
    let writes = sandbox.lock().writes.clone();
    let (signal_writes, recovery_writes) = writes.split_at(signal_writes.min(writes.len()));
    report["writes"] = json!(signal_writes);
    report["confirm"] = json!(answer.word());
    report["signal"] = json!({"source": pending.source, "content": crate::activity::clip(&pending.content, 500), "payload": pending.payload});
    if let Some((ran, choice)) = &recovery {
        let status = ran.status(options.timeout);
        report["recovery"] = json!({
            "status": status.word, "error": status.error,
            "runs": tree(&sandbox, &ran.runs(), Some(choice), session),
            "writes": recovery_writes,
        });
    }
    root.end(SpanEnd::output(
        json!({"status": report["status"], "output": report["output"], "error": report["error"]}),
    ));
    Ok(report.to_string())
}

/// `try_workflow(name, version?, drafts?)` without a signal (decision 11):
/// a workflow that takes no agent turn runs once as the root, on the input
/// a scheduled fire admits, with every write recorded (`DRAFT_REPLY`). A
/// workflow that handles signals or takes agent turns is refused with the
/// form that tries it. Returns the steps, the writes and how it ended; a
/// completed run marks the drafts whose runs all completed as tried.
pub async fn try_draft(
    engine: &Arc<Engine>,
    args: &Map<String, Value>,
    session: &Session,
    scope: &Scope,
    cancel: &CancellationToken,
) -> anyhow::Result<String> {
    let name = name_arg(args)?;
    let version = version_arg(args)?;
    let drafts = resolve_drafts(engine, name, version, args).await?;
    let draft = drafts.get(name).cloned().ok_or_else(|| anyhow::anyhow!("no saved workflow `{name}`"))?;
    let wf = crate::flows::gate(engine, name, &draft.yaml)?.wf;
    // A loop, a route or the dispatcher runs on a signal, never on a
    // scheduled fire's empty input; so does a workflow taking agent turns,
    // which needs the input the host admits a signal's run with.
    anyhow::ensure!(
        !crate::flows::is_runtime_workflow(name, &wf),
        "`{name}` handles signals: try it with a signal (try_workflow with `signal`, and `drafts` for the rest of the chain)"
    );
    anyhow::ensure!(
        !crate::flows::takes_turns(&wf),
        "`{name}` takes agent turns, which run on the input the host admits a signal with: try it on a fire of its task (try_workflow with signal {{source: scheduler, content: the task's prompt, payload: {{workflow: \"{name}\"}}}})"
    );
    let signal =
        crate::flows::WorkflowSignal { id: 0, source: "scheduler".into(), content: String::new(), env_context: None };
    let input = Value::Object(crate::flows::signal_input(&signal, &session.env));
    let sandbox = Sandbox::new(Answer::Confirm, drafts, DRAFT_REPLY, false);
    let root = scope.span(
        SpanStart::new("dry-run", SpanKind::Chain)
            .input(input.clone())
            .metadata(metadata(json!({"workflow": name, "workflow_version": draft.version, "dry_run": true}))),
    );
    let dry = sandbox_engine(engine, &sandbox, root.clone(), env_of(session))?;
    let id = format!("try:{name}");
    dry.scopes.borrow(&id, root.clone());
    let budget = tokio::time::Instant::now() + DRY_RUN_TIMEOUT;
    let outcome = dry_run(&dry, &sandbox, &draft.yaml, input, &id, name, None, budget, cancel).await;
    close(&dry).await;
    if matches!(outcome, Ran::Cancelled) {
        root.end(SpanEnd::error(crate::errors::aborted().to_string()));
        return Err(crate::errors::aborted());
    }
    let status = outcome.status(DRY_RUN_TIMEOUT);
    let runs = outcome.runs();
    if status.word == "completed" {
        mark_tried(engine, &sandbox, &runs)?;
    }
    root.end(match status.word {
        "completed" => SpanEnd::output(status.output.clone()),
        word => SpanEnd::error(word),
    });
    let drafts = (sandbox.drafts.len() > 1).then(|| drafts_report(&sandbox, &runs));
    let record = sandbox.lock();
    let path = record.paths.get(&id);
    let mut steps = record.steps.get(&id).cloned().unwrap_or_default();
    let omitted = steps.len().saturating_sub(STEPS_MAX) + path.map_or(0, |p| p.omitted);
    steps.truncate(STEPS_MAX);
    let mut reply = json!({
        "name": name, "version": draft.version, "status": status.word, "error": status.error,
        "output": cut(&status.output, STEP_OUTPUT_CHARS),
        "path": path.map(|p| p.steps.clone()).unwrap_or_default(), "steps": steps, "omitted_steps": omitted,
        "writes": record.writes,
        "note": format!("A dry run: reads and model steps ran for real on today's data; nothing changed. Each write above was recorded, not made, and answered with {DRAFT_REPLY}, so a step that reads a write's reply saw that stub. Waits and retry delays did not sleep."),
        "next": "Check the path, every write's arguments and the message against each thing the user asked for. Anything wrong or questionable is yours to fix: change the definition, save a new version and try it again (three tries in all) before you report. Report only what needs the user's decision.",
    });
    drop(record);
    if let Some(drafts) = drafts {
        reply["drafts"] = json!(drafts);
    }
    let warnings = crate::flows::warnings(&wf);
    if !warnings.is_empty() {
        reply["warnings"] = json!(warnings);
    }
    if sandbox.refusals() > 0 {
        reply["sandbox_refusals"] = json!(sandbox.refusals());
    }
    Ok(reply.to_string())
}

// How a dry run went: the engine's report (and whether the try's budget cut
// it), the engine failing before any report, or the builder's own
// cancellation.
enum Ran {
    Done(DryRun, bool),
    Failed(anyhow::Error),
    Cancelled,
}

struct Outcome {
    word: &'static str,
    output: Value,
    error: Value,
}

impl Ran {
    // The status mapping (R-L2).
    fn status(&self, timeout: Duration) -> Outcome {
        let outcome = |word, output, error| Outcome { word, output, error };
        match self {
            Ran::Done(_, true) => outcome("timed_out", Value::Null, json!(format!("stopped after {timeout:?}"))),
            Ran::Done(dry, false) => match &dry.status {
                Status::Completed(output) => outcome("completed", output.clone(), Value::Null),
                Status::Waiting { .. } => outcome(
                    "suspended",
                    Value::Null,
                    json!("a run waited for something that never came; see the paths"),
                ),
                Status::Faulted(error) | Status::Interrupted(error) => {
                    outcome("faulted", Value::Null, json!(error.detail.clone().unwrap_or_else(|| error.to_string())))
                }
                Status::Cancelled => outcome("faulted", Value::Null, json!("cancelled")),
            },
            Ran::Failed(error) => outcome("faulted", Value::Null, json!(format!("{error:#}"))),
            Ran::Cancelled => outcome("faulted", Value::Null, json!("cancelled")),
        }
    }

    fn runs(&self) -> Vec<DryRunRun> {
        match self {
            Ran::Done(dry, _) => dry.runs.clone(),
            _ => Vec::new(),
        }
    }
}

// One dry run of `yaml` as root `id` (named `name`) on the sandbox engine,
// heard by the sandbox's observer, until `budget` (decisions 2, 8, 9, 14).
#[allow(clippy::too_many_arguments)]
async fn dry_run(
    dry: &Arc<Engine>,
    sandbox: &Arc<Sandbox>,
    yaml: &str,
    input: Value,
    id: &str,
    name: &str,
    host: Option<Value>,
    budget: tokio::time::Instant,
    cancel: &CancellationToken,
) -> Ran {
    let writes =
        crate::functions::mcp_functions(&dry.deps).into_iter().filter(|(_, read_only)| !read_only).map(|(n, _)| n);
    let mock = Arc::new(DryMock { sandbox: sandbox.clone(), writes: writes.collect() });
    let (ended, mut ends) = tokio::sync::mpsc::unbounded_channel::<(String, Status)>();
    let watch = Arc::new(Watch { sandbox: sandbox.clone(), ended: Mutex::new(Some(ended)) });
    // The lifecycle of the dry run's runs: a run that ended ends its scope
    // and closes its loop, with no store read; a suspension changes nothing.
    let lifecycle = {
        let dry = dry.clone();
        tokio::spawn(async move {
            while let Some((run, status)) = ends.recv().await {
                dry.scopes.stop(&run, Some(crate::runs::span_end(&status)));
                dry.end_run_loop(&run).await;
            }
        })
    };
    let options = DryRunOptions { id: Some(id.into()), name: Some(name.into()), host, observer: Some(watch.clone()) };
    let stop = cancel.child_token();
    let execution = dry.runtime.dry_run_opts(yaml, input, mock, options, &stop);
    tokio::pin!(execution);
    let mut timed_out = false;
    let result = tokio::select! {
        result = &mut execution => result,
        _ = tokio::time::sleep_until(budget) => {
            timed_out = true;
            stop.cancel();
            execution.await
        }
    };
    watch.close();
    let _ = lifecycle.await;
    if cancel.is_cancelled() {
        return Ran::Cancelled;
    }
    match result {
        Ok(dry) => Ran::Done(dry, timed_out),
        Err(error) => Ran::Failed(error),
    }
}

// After the dry runs: any loop and scope still open closes, as stopped by
// the end of the try (decision 9); the engine is never shut down.
async fn close(dry: &Arc<Engine>) {
    for id in dry.loop_ids().await {
        dry.end_loop(&id).await;
    }
    for run in dry.scopes.open() {
        dry.scopes.stop(&run, Some(SpanEnd::error("dry run ended")));
        dry.scopes.release(&run);
    }
}

// Decision 10: a listed draft is tried when the dry run completed and every
// run of its content completed. The only write a try makes outside it.
fn mark_tried(engine: &Engine, sandbox: &Sandbox, runs: &[DryRunRun]) -> anyhow::Result<()> {
    for (name, draft) in &sandbox.drafts {
        let of: Vec<&DryRunRun> = runs.iter().filter(|r| r.name == *name && r.digest == draft.digest).collect();
        if !of.is_empty() && of.iter().all(|r| matches!(r.outcome, Some(Status::Completed(_)))) {
            WorkflowMeta::mark_tried(&engine.deps.db, name, &draft.digest)?;
        }
    }
    Ok(())
}

// ── 7. the report ──────────────────────────────────────────────────────────

fn report(
    sandbox: &Sandbox,
    name: &str,
    status: &Outcome,
    runs: &[DryRunRun],
    root: Option<&Choice>,
    session: &Session,
) -> Value {
    let drafts = drafts_report(sandbox, runs);
    let unused: Vec<&str> = drafts.iter().filter(|d| d["ran"] == false).filter_map(|d| d["name"].as_str()).collect();
    let tree = tree(sandbox, runs, root, session);
    let record = sandbox.lock();
    // How the signal's primary loop ended: the first loop among the root's children.
    let root_id = runs.first().map(|r| r.id.as_str());
    let primary = runs
        .iter()
        .find(|r| r.parent.is_some() && r.parent.as_deref() == root_id && is_loop(&record, r))
        .map(|r| ended(&record, r))
        .unwrap_or(Value::Null);
    let mut next = "Compare this trace with what the user asked for: the path and each switch's case, every call's arguments and verdict, the mocked writes, how each loop ended and its output. Anything wrong is yours to fix: save a new version and try it again. When it does what was asked, describe to the user what it will do, the writes above all, and activate it only after they confirm.".to_owned();
    if !unused.is_empty() {
        next.push_str(&format!(
            " These drafts did not run, so they are not tried: {}. Check the signal (its source and payload) against the routes' `when` and the loop each route selects.",
            unused.join(", ")
        ));
    }
    let mut report = json!({
        "name": name,
        "status": status.word,
        "output": status.output,
        "error": status.error,
        "ended": primary,
        "drafts": drafts,
        "runs": tree,
        "note": format!("A dry run: model turns, reads and the guard ran for real. Every write was recorded, not made, and answered with {MOCK_REPLY} (marked mock); each confirmation was answered automatically ({}). Waits did not sleep, and nothing was kept: no run, card, message, memory, journal, schedule, skill or workflow changed. pending_confirmations saw only the dry run's own cards, none of the user's.", sandbox.answer.word()),
        "next": next,
    });
    if sandbox.refusals() > 0 {
        report["sandbox_refusals"] = json!(sandbox.refusals());
    }
    report
}

// Each listed draft: whether it ran (a run of its content), and how.
fn drafts_report(sandbox: &Sandbox, runs: &[DryRunRun]) -> Vec<Value> {
    let record = sandbox.lock();
    sandbox
        .drafts
        .iter()
        .map(|(name, draft)| {
            let of: Vec<&DryRunRun> = runs.iter().filter(|r| r.name == *name && r.digest == draft.digest).collect();
            let faulted = of.iter().find_map(|r| match &r.outcome {
                Some(Status::Faulted(error)) => Some(error.detail.clone().unwrap_or_else(|| error.to_string())),
                _ => None,
            });
            let outcome = if of.is_empty() {
                match record.set_aside.iter().find(|f| f.name == *name && f.version == Some(draft.version)) {
                    Some(fallback) => format!("set aside: {}", fallback.line),
                    None => "did not run".into(),
                }
            } else if let Some(error) = faulted {
                format!("faulted: {error}")
            } else if of.iter().all(|r| matches!(r.outcome, Some(Status::Completed(_)))) {
                "completed".into()
            } else if of.iter().any(|r| matches!(r.outcome, Some(Status::Cancelled))) {
                "cancelled".into()
            } else {
                "unfinished".into()
            };
            json!({"name": name, "version": draft.version, "ran": !of.is_empty(), "outcome": outcome})
        })
        .collect()
}

// The version a run ran, as the builder reads it: a listed draft's, a
// built-in text, or a saved version.
fn version_of(sandbox: &Sandbox, name: &str, digest: &str, version: u64) -> String {
    match sandbox.drafts.get(name) {
        Some(draft) if draft.digest == digest => format!("draft v{}", draft.version),
        _ if crate::flows::built_in(name).is_some_and(|yaml| ouro_runtime::durable::digest_of(yaml) == digest) => {
            "built-in".into()
        }
        _ => format!("v{version}"),
    }
}

// Whether a run is an agent loop: it took a turn.
fn is_loop(record: &Record, run: &DryRunRun) -> bool {
    run.calls.iter().any(|c| c.function == "agent_turn") || record.turns.iter().any(|t| t["loop"] == run.id)
}

// The run tree from the dry run's own list (decision 8): each run's path,
// its turns and how a loop ended, its children. The root's version is the
// one the sandbox chose (the private store saved it as its own version 1).
fn tree(sandbox: &Sandbox, runs: &[DryRunRun], root: Option<&Choice>, session: &Session) -> Vec<Value> {
    let record = sandbox.lock();
    runs.iter()
        .filter(|r| r.parent.is_none())
        .enumerate()
        .map(|(i, run)| {
            let chosen = root.filter(|_| i == 0);
            let version = match chosen {
                Some(choice) => {
                    version_of(sandbox, &choice.name, &ouro_runtime::durable::digest_of(&choice.yaml), choice.version)
                }
                None => version_of(sandbox, &run.name, &run.digest, run.version),
            };
            let mut out = run_report(sandbox, &record, runs, run, version, session);
            if let Some(fallback) = chosen.and_then(|c| c.fallback.as_ref()) {
                out["fallback"] = json!(fallback.line);
            }
            out
        })
        .collect()
}

fn run_report(
    sandbox: &Sandbox,
    record: &Record,
    runs: &[DryRunRun],
    run: &DryRunRun,
    version: String,
    session: &Session,
) -> Value {
    let path = record.paths.get(&run.id);
    let mut out = json!({"run": run.id, "workflow": run.name, "version": version,
        "path": path.map(|p| p.steps.clone()).unwrap_or_default()});
    if let Some(key) = &run.key {
        out["key"] = json!(task(key));
    }
    if let Some(omitted) = path.map(|p| p.omitted).filter(|n| *n > 0) {
        out["omitted_steps"] = json!(omitted);
    }
    if is_loop(record, run) {
        out["turns"] = json!(turns(record, &run.id, session));
        out["ended"] = ended(record, run);
    }
    if let Some(fallback) = record.fallbacks.get(&run.id) {
        out["fallback"] = json!(fallback.line);
    }
    let children: Vec<Value> = runs
        .iter()
        .filter(|r| r.parent.as_deref() == Some(run.id.as_str()))
        .map(|r| {
            let version = version_of(sandbox, &r.name, &r.digest, r.version);
            run_report(sandbox, record, runs, r, version, session)
        })
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
fn ended(record: &Record, run: &DryRunRun) -> Value {
    let turns: Vec<&Value> = record.turns.iter().filter(|t| t["loop"] == run.id).collect();
    let nudged = turns.iter().any(|t| t["after_nudge"] == true);
    let last =
        record.paths.get(&run.id).and_then(|p| p.steps.iter().rev().find(|s| !s.ends_with("(skipped)")).cloned());
    let mut out = match &run.outcome {
        Some(Status::Completed(output)) => {
            let by = if record.finished.get(&run.id) == Some(&true) {
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
