// Saved workflows: Open Workflow Specification definitions (`crates/owf`).
// 1. the store (agent.db `workflows`); 2. the function catalog (MCP tools,
// the model steps, the agent loop's host functions); 2b. model composition
// and signal input; 3. tracing (owf events → spans under the signal's
// trace); 4. running one; 4b. which definition a run runs; 5. authoring.
//
// One kind of workflow: every run the runtime starts on a signal (a route,
// a scheduled task's `{ "workflow": "…" }`, the primary loop) and every
// worker is admitted with the model's parameters (`admitted_input`), so any
// workflow may mix deterministic steps with agent turns, and a recurring
// task can start as a copy of `agent-loop` and be rewritten step by step
// without changing kind. `metadata.loop` only lists a workflow among the
// loops a worker may run. The supervisor hands a scheduled run that faulted
// before any `Write` call to the agent loop; one that faulted after goes to
// recovery, which only reports.
use crate::{
    agent::Engine,
    db::AgentDb,
    errors::is_aborted,
    generation::{Observation, generation},
    mcp::{Mcp, model_tools},
    models::PresetName,
    providers::{self, CompletionParams, ToolDef},
    session::{EnvData, Session, iso},
    skills::append_patch,
    tracing::{EventStart, GenerationEnd, Level, Scope, Span, SpanEnd, SpanKind, SpanStart},
};
use async_trait::async_trait;
use futures::{FutureExt, StreamExt, future::BoxFuture};
use owf::durable::{CallFailure, Delivery};
use owf::run::{CallInfo, Effect, Event, Host, Limits, Observer, Status as RunStatus, SystemClock, WorkflowError};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

// ── 1. store ───────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Draft,
    Active,
    Retired,
}

impl Status {
    fn parse(text: &str) -> anyhow::Result<Status> {
        Ok(match text {
            "draft" => Status::Draft,
            "active" => Status::Active,
            "retired" => Status::Retired,
            other => anyhow::bail!("unknown workflow status `{other}`"),
        })
    }
}

// A version never changes once saved; only its status moves (draft → active
// → retired), with at most one active version per name. `tried`: a dry run
// of it completed, which activation requires.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SavedWorkflow {
    pub name: String,
    pub version: i64,
    pub status: Status,
    // The YAML as authored.
    pub definition: String,
    pub note: Option<String>,
    pub created_at: String,
    pub tried: bool,
}

pub trait WorkflowStore: Send + Sync {
    // Stores `definition` as the next version of `name`, a draft.
    fn save(&self, name: &str, definition: &str, note: Option<&str>) -> anyhow::Result<SavedWorkflow>;
    // Makes `version` the active one; the version active before retires.
    fn activate(&self, name: &str, version: i64) -> anyhow::Result<()>;
    // Records that a dry run of `version` completed.
    fn mark_tried(&self, name: &str, version: i64) -> anyhow::Result<()>;
    // A version, or the newest one when `version` is None.
    fn get(&self, name: &str, version: Option<i64>) -> anyhow::Result<Option<SavedWorkflow>>;
    fn active(&self, name: &str) -> anyhow::Result<Option<SavedWorkflow>>;
    // The newest version of every workflow, by name.
    fn list(&self) -> anyhow::Result<Vec<SavedWorkflow>>;
}

const COLUMNS: &str = "name, version, status, definition, note, created_at, tried_at IS NOT NULL";

type Row = (String, i64, String, String, Option<String>, String, bool);

fn saved(row: &rusqlite::Row) -> rusqlite::Result<Row> {
    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?))
}

fn into_saved((name, version, status, definition, note, created_at, tried): Row) -> anyhow::Result<SavedWorkflow> {
    Ok(SavedWorkflow { name, version, status: Status::parse(&status)?, definition, note, created_at, tried })
}

impl WorkflowStore for AgentDb {
    fn save(&self, name: &str, definition: &str, note: Option<&str>) -> anyhow::Result<SavedWorkflow> {
        let conn = self.conn();
        let row = conn.query_row(
            &format!(
                "INSERT INTO workflows (name, version, definition, note)
                 VALUES (?1, (SELECT COALESCE(MAX(version), 0) + 1 FROM workflows WHERE name = ?1), ?2, ?3)
                 RETURNING {COLUMNS}"
            ),
            params![name, definition, note],
            saved,
        )?;
        into_saved(row)
    }

    fn activate(&self, name: &str, version: i64) -> anyhow::Result<()> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let exists = tx
            .query_row("SELECT 1 FROM workflows WHERE name = ?1 AND version = ?2", params![name, version], |_| Ok(()))
            .optional()?;
        anyhow::ensure!(exists.is_some(), "no version {version} of workflow `{name}`");
        tx.execute("UPDATE workflows SET status = 'retired' WHERE name = ?1 AND status = 'active'", params![name])?;
        tx.execute(
            "UPDATE workflows SET status = 'active', activated_at = datetime('now') WHERE name = ?1 AND version = ?2",
            params![name, version],
        )?;
        tx.commit()?;
        Ok(())
    }

    fn mark_tried(&self, name: &str, version: i64) -> anyhow::Result<()> {
        let changed = self.conn().execute(
            "UPDATE workflows SET tried_at = datetime('now') WHERE name = ?1 AND version = ?2",
            params![name, version],
        )?;
        anyhow::ensure!(changed > 0, "no version {version} of workflow `{name}`");
        Ok(())
    }

    fn get(&self, name: &str, version: Option<i64>) -> anyhow::Result<Option<SavedWorkflow>> {
        let row = self
            .conn()
            .query_row(
                &format!(
                    "SELECT {COLUMNS} FROM workflows WHERE name = ?1 AND (?2 IS NULL OR version = ?2)
                     ORDER BY version DESC LIMIT 1"
                ),
                params![name, version],
                saved,
            )
            .optional()?;
        row.map(into_saved).transpose()
    }

    fn active(&self, name: &str) -> anyhow::Result<Option<SavedWorkflow>> {
        let row = self
            .conn()
            .query_row(
                &format!("SELECT {COLUMNS} FROM workflows WHERE name = ?1 AND status = 'active'"),
                params![name],
                saved,
            )
            .optional()?;
        row.map(into_saved).transpose()
    }

    fn list(&self) -> anyhow::Result<Vec<SavedWorkflow>> {
        let conn = self.conn();
        let mut statement = conn.prepare(&format!(
            "SELECT {COLUMNS} FROM workflows w
              WHERE version = (SELECT MAX(version) FROM workflows WHERE name = w.name)
              ORDER BY name"
        ))?;
        let rows = statement.query_map([], saved)?.collect::<rusqlite::Result<Vec<_>>>()?;
        rows.into_iter().map(into_saved).collect()
    }
}

// The same contract without a database, for tests and tools that need none.
#[derive(Default)]
pub struct MemoryWorkflows(Mutex<Vec<SavedWorkflow>>);

impl WorkflowStore for MemoryWorkflows {
    fn save(&self, name: &str, definition: &str, note: Option<&str>) -> anyhow::Result<SavedWorkflow> {
        let mut all = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let version = all.iter().filter(|w| w.name == name).map(|w| w.version).max().unwrap_or(0) + 1;
        let saved = SavedWorkflow {
            name: name.into(),
            version,
            status: Status::Draft,
            definition: definition.into(),
            note: note.map(str::to_owned),
            created_at: chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
            tried: false,
        };
        all.push(saved.clone());
        Ok(saved)
    }

    fn activate(&self, name: &str, version: i64) -> anyhow::Result<()> {
        let mut all = self.0.lock().unwrap_or_else(|e| e.into_inner());
        anyhow::ensure!(
            all.iter().any(|w| w.name == name && w.version == version),
            "no version {version} of workflow `{name}`"
        );
        for w in all.iter_mut().filter(|w| w.name == name) {
            if w.version == version {
                w.status = Status::Active;
            } else if w.status == Status::Active {
                w.status = Status::Retired;
            }
        }
        Ok(())
    }

    fn mark_tried(&self, name: &str, version: i64) -> anyhow::Result<()> {
        let mut all = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let saved = all.iter_mut().find(|w| w.name == name && w.version == version);
        saved.ok_or_else(|| anyhow::anyhow!("no version {version} of workflow `{name}`"))?.tried = true;
        Ok(())
    }

    fn get(&self, name: &str, version: Option<i64>) -> anyhow::Result<Option<SavedWorkflow>> {
        let all = self.0.lock().unwrap_or_else(|e| e.into_inner());
        Ok(all
            .iter()
            .filter(|w| w.name == name && version.is_none_or(|v| w.version == v))
            .max_by_key(|w| w.version)
            .cloned())
    }

    fn active(&self, name: &str) -> anyhow::Result<Option<SavedWorkflow>> {
        let all = self.0.lock().unwrap_or_else(|e| e.into_inner());
        Ok(all.iter().find(|w| w.name == name && w.status == Status::Active).cloned())
    }

    fn list(&self) -> anyhow::Result<Vec<SavedWorkflow>> {
        let all = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let mut newest: Vec<SavedWorkflow> = Vec::new();
        for w in all.iter() {
            match newest.iter_mut().find(|n| n.name == w.name) {
                Some(n) if n.version < w.version => *n = w.clone(),
                Some(_) => {}
                None => newest.push(w.clone()),
            }
        }
        newest.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(newest)
    }
}

// ── 2. function catalog ────────────────────────────────────────────────────

// The runtime's own steps a workflow may call besides MCP tools (a
// generation, the decision classifier's filter, a watermark): the name, the
// effect, the arguments it takes and the ones it needs.
type ModelFunction = (&'static str, Effect, &'static [&'static str], &'static [&'static str]);

/// Host functions need an admitted signal or loop. Keep their author-facing
/// contract with the registry execution uses, so discovery cannot omit them.
#[derive(Clone, Copy)]
pub struct HostFunction {
    pub name: &'static str,
    pub effect: Effect,
    pub takes: &'static [&'static str],
    pub needs: &'static [&'static str],
    pub description: &'static str,
}

pub(crate) const fn host_function(
    name: &'static str,
    effect: Effect,
    takes: &'static [&'static str],
    needs: &'static [&'static str],
    description: &'static str,
) -> HostFunction {
    HostFunction { name, effect, takes, needs, description }
}

const MODEL_FUNCTIONS: &[ModelFunction] = &[
    (
        "start_child",
        Effect::Read,
        &["workflow", "loop", "key", "input", "calls", "call", "i", "index", "memory"],
        &["workflow"],
    ),
    ("child_outcome", Effect::Read, &["run", "key"], &["run", "key"]),
    ("hash_value", Effect::Read, &["value"], &["value"]),
    ("skill_catalog_snapshot", Effect::Read, &[], &[]),
    ("llm_compose", Effect::Read, &["preset", "skill", "prompt", "input", "json"], &[]),
    ("filter_relevant", Effect::Read, &["items", "prompt", "threshold"], &["items", "prompt"]),
    ("choose", Effect::Read, &["question", "options", "input", "min_confidence", "default"], &["question", "options"]),
    ("set_memory", Effect::Write, &["key", "value"], &["key", "value"]),
];

// Taking signals is the supervisor's: a workflow that did would steal them.
const WITHHELD: &[&str] = &["get_next_signal"];

fn mcp_tool(mcp: &dyn Mcp, name: &str) -> Option<ToolDef> {
    if WITHHELD.contains(&name) {
        return None;
    }
    model_tools(mcp).into_iter().find(|t| t.name == name)
}

// What `name` does, or None when a workflow cannot call it. An MCP tool not
// marked read-only counts as a write.
pub fn effect(mcp: &dyn Mcp, name: &str) -> Option<Effect> {
    if let Some(function) = LOOP_FUNCTIONS.iter().find(|f| f.name == name) {
        return Some(function.effect);
    }
    if let Some(function) = crate::supervisor::SIGNAL_FUNCTIONS.iter().find(|f| f.name == name) {
        return Some(function.effect);
    }
    if let Some((_, effect, _, _)) = MODEL_FUNCTIONS.iter().find(|f| f.0 == name) {
        return Some(*effect);
    }
    mcp_tool(mcp, name)?;
    Some(if mcp.read_only(name) { Effect::Read } else { Effect::Write })
}

// The argument names `name` takes (None: any) and the ones it needs.
fn arguments(mcp: &dyn Mcp, name: &str) -> Option<(Option<Vec<String>>, Vec<String>)> {
    let names = |list: &[&str]| list.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    if let Some((_, _, takes, needs)) = MODEL_FUNCTIONS.iter().find(|f| f.0 == name) {
        return Some((Some(names(takes)), names(needs)));
    }
    let schema = mcp_tool(mcp, name)?.parameters;
    let open = schema.get("additionalProperties").is_some_and(|a| a != &Value::Bool(false));
    let takes =
        schema.get("properties").and_then(Value::as_object).filter(|_| !open).map(|p| p.keys().cloned().collect());
    let needs = schema
        .get("required")
        .and_then(Value::as_array)
        .map(|r| r.iter().filter_map(Value::as_str).map(str::to_owned).collect())
        .unwrap_or_default();
    Some((takes, needs))
}

// The catalog for one run: calls go to MCP or the model, traced under the
// span of the task that made them.
struct Functions {
    engine: Arc<Engine>,
    spans: Arc<Spans>,
    // The store of the durable run these functions serve: a loop's
    // sub-agents become durable runs of their own in it.
    runs: Option<crate::durable::RunStore>,
    guard_only: bool,
    // The admitted model input of a run without a run store (in process).
    loop_input: Option<Value>,
    events: Option<Arc<dyn owf::run::Events>>,
    children: Arc<Children>,
}

// Every child task belongs to this execution attempt. Before its trace and
// lease close, shutdown releases children for recovery and joins their cleanup.
struct Children {
    tasks: tokio::sync::Mutex<tokio::task::JoinSet<()>>,
    cancel: CancellationToken,
    slots: Arc<tokio::sync::Semaphore>,
}

impl Default for Children {
    fn default() -> Self {
        Self {
            tasks: Default::default(),
            cancel: CancellationToken::new(),
            slots: Arc::new(tokio::sync::Semaphore::new(16)),
        }
    }
}

impl Children {
    async fn close(&self) {
        self.cancel.cancel();
        let mut tasks = self.tasks.lock().await;
        while tasks.join_next().await.is_some() {}
    }
}

fn invalid(name: &str, detail: impl std::fmt::Display) -> WorkflowError {
    WorkflowError::validation(format!("{name}: {detail}"))
}

fn text<'a>(args: &'a Value, function: &str, key: &str) -> Result<Option<&'a str>, WorkflowError> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s)),
        Some(_) => Err(invalid(function, format!("`{key}` must be a string"))),
    }
}

// `choose`'s options: 2–255 labels a jq string comparison can name, each
// with a nonempty criterion.
fn choose_options(args: &Value) -> Result<Map<String, Value>, String> {
    let options =
        args.get("options").and_then(Value::as_object).ok_or("`options` must be an object of label: criterion")?;
    if !(2..=crate::decisions::MAX_CHOICE_OPTIONS).contains(&options.len()) {
        return Err(format!("`options` needs 2–{} labels", crate::decisions::MAX_CHOICE_OPTIONS));
    }
    for (label, criterion) in options {
        if !CHOICE_LABEL.is_match(label) {
            return Err(format!(
                "option `{label}`: a label is lowercase letters, digits and `_`, starting with a letter"
            ));
        }
        if criterion.as_str().is_none_or(|c| c.trim().is_empty()) {
            return Err(format!("option `{label}`: the criterion must be a nonempty string"));
        }
    }
    Ok(options.clone())
}

static CHOICE_LABEL: std::sync::LazyLock<regex::Regex> =
    std::sync::LazyLock::new(|| regex::Regex::new(r"^[a-z][a-z0-9_]*$").expect("valid label regex"));

// How a host function failed. `Stopped`: a cancellation abort cut it short
// (`errors::Aborted`: shutdown, or the loop's owner released or ended), so
// it gave no answer; `invoke` reports that as an interruption, which a
// durable run does not record and resumes at. Typed here, before the error
// is only a message.
enum HostError {
    Failed(WorkflowError),
    Stopped(WorkflowError),
}

impl From<WorkflowError> for HostError {
    fn from(error: WorkflowError) -> Self {
        HostError::Failed(error)
    }
}

impl HostError {
    fn into_error(self) -> WorkflowError {
        match self {
            HostError::Failed(error) | HostError::Stopped(error) => error,
        }
    }

    // A stop is an interruption; anything else as `failure` classifies it.
    fn or_failure(self, failure: impl FnOnce(WorkflowError) -> CallFailure) -> CallFailure {
        match self {
            HostError::Stopped(error) => CallFailure::Interrupted(error),
            HostError::Failed(error) => failure(error),
        }
    }
}

// A failure the model or a tool reported, as opposed to the transport.
fn failed(e: anyhow::Error) -> HostError {
    let error = WorkflowError::runtime(e.to_string());
    if is_aborted(&e) { HostError::Stopped(error) } else { HostError::Failed(error) }
}

// The agent loop's functions (`agent::AGENT_LOOP`): a turn, one tool call
// by name, a round's settlement. They read the model's parameters from the
// run's admitted input (`admitted_input`), which the host gives every run it
// starts on a signal and every worker: any such workflow may take agent
// turns, a loop or not. The YAML passes the transcript; the spec it passes
// is replaced by the admitted one.
// `guard` and `confirm_request` remain executable for immutable runs admitted
// before guard children. Remove them after those runs have drained. New
// definitions cannot call confirm_request; only the reserved guard can.
const LOOP_FUNCTIONS: &[HostFunction] = &[
    host_function(
        "agent_turn",
        Effect::Read,
        &["loop", "messages", "memory", "i", "extra_turn"],
        &["messages"],
        "Generate the next model message with the run's admitted model parameters (any workflow the runtime starts on a signal, and workers). Pass messages, working-memory snapshot and zero-based turn i; extra_turn allows one additional turn. Returns message, calls and content. A workflow that calls it must start the guard child each round: copy agent-loop's guarded round.",
    ),
    host_function(
        "guard",
        Effect::Read,
        &["loop", "calls"],
        &["calls"],
        "Legacy round review. New loops start the reserved guard child with start_child and join it with listen; copy agent-loop's guarded round.",
    ),
    host_function(
        "confirm_request",
        Effect::Write,
        &["loop", "call"],
        &["call"],
        "Reserved confirmation function; supported only for existing admitted definitions.",
    ),
    host_function(
        "start_worker",
        Effect::Read,
        &["loop", "call", "i", "index", "memory", "worker_loop"],
        &["call"],
        "Legacy worker start in an admitted loop. New loops use start_child with the model's invoke_sub_agent call, then listen and tool; returns run and loop.",
    ),
    host_function(
        "tool",
        Effect::Read,
        &["loop", "call", "guard_run", "worker_run", "worker_loop", "i", "index", "memory", "outcome"],
        &["call"],
        "Execute one of the model's calls under the host-verified guard verdict. Pass its round i and index, guard_run and any joined worker_run/worker_loop. Returns presented, failed, result and working-memory updates for agent_settle. Its actual effect depends on the called tool; keep the base loop's guard and worker joins.",
    ),
    host_function(
        "agent_settle",
        Effect::Read,
        &["loop", "calls", "results", "memory"],
        &["calls", "results"],
        "Settle ordered tool results into the run's transcript and working memory. Returns replies, memory, finished, output and outcomes: {name, arguments, ok, result}. Use these facts in workflow exit rules.",
    ),
    host_function(
        "loop_setup",
        Effect::Read,
        &["system", "skills", "input"],
        &[],
        "Set up the admitted run's instructions, once and before its first agent_turn. system is an array of strings (null and empty entries are dropped) added to the system prompt; skills names skills loaded in full, with their tools; input replaces the first user message (a signal's run starts with the signal's text, e.g. with signal_input's history). Returns messages and memory to export into $context, and setup (what a restart restores). Loaded skills and a preselect_skills block passed in system count for the guard.",
    ),
];

fn is_loop_function(name: &str) -> bool {
    LOOP_FUNCTIONS.iter().any(|f| f.name == name)
}

// The signal workflow's context functions any admitted run may call too,
// before its `loop_setup`: on the signal it was started for (agent.rs §4).
const LOOP_CONTEXT: &[&str] = &["signal_input", "recent_activity", "pending_confirmations", "preselect_skills"];

// A host function of an admitted run.
fn is_loop_host(name: &str) -> bool {
    is_loop_function(name) || LOOP_CONTEXT.contains(&name)
}

// A function only the signal dispatcher runs (`signal.yaml`), on the
// signal its run serves.
fn is_dispatcher_function(name: &str) -> bool {
    !LOOP_CONTEXT.contains(&name) && crate::supervisor::SIGNAL_FUNCTIONS.iter().any(|f| f.name == name)
}

const DISPATCHER_ONLY: &str = "only the signal dispatcher calls it";

/// Whether a definition calls a dispatcher-only function: a signal route
/// of the kind loop routes superseded (it calls `primary_loop` itself), run
/// on the dispatcher's host with its context as input.
pub fn calls_dispatcher(wf: &owf::model::Workflow) -> bool {
    owf::model::call_sites(wf).iter().any(|s| is_dispatcher_function(&s.function))
}

// Whether a definition takes agent turns (or other steps that need the
// run's admitted model input): it runs on a signal, never on the scheduled
// form of a dry run.
fn takes_turns(wf: &owf::model::Workflow) -> bool {
    owf::model::call_sites(wf).iter().any(|s| is_loop_host(&s.function))
}

/// The admitted model input of a durable run, or None. The host admits a
/// run it starts on a signal with the model's parameters (`loop`: the spec,
/// with the confirmation target and the tools), the transcript and the
/// guard's evidence (`AgentLoop::workflow_input`), and a worker's run the
/// same way; `agent_turn`, `tool` and the rest read only this. A run started
/// by a workflow's own `start_child` is never admitted, whatever its input
/// says (`start_child` refuses an input naming `loop`, and older children
/// are told apart by not being loops, which only the host starts as children).
pub fn admitted_input(run: &crate::durable::SavedRun) -> Option<&Value> {
    (run.input.get("loop").is_some_and(Value::is_object)
        && (run.parent_run.is_none() || is_loop_definition(&run.name, &run.definition)))
    .then_some(&run.input)
}

fn runtime_functions() -> impl Iterator<Item = (HostFunction, &'static str)> {
    MODEL_FUNCTIONS
        .iter()
        .map(|&(name, effect, takes, needs)| {
            (
                host_function(name, effect, takes, needs, "A step of the workflow runtime; the skill describes it."),
                "workflow",
            )
        })
        .chain(LOOP_FUNCTIONS.iter().filter(|f| f.name != "confirm_request").map(|f| (*f, "agent-loop")))
        .chain(
            crate::supervisor::SIGNAL_FUNCTIONS
                .iter()
                .map(|f| (*f, if LOOP_CONTEXT.contains(&f.name) { "signal|agent-loop" } else { "signal" })),
        )
}

fn loop_tool_name(args: &Value) -> &str {
    args.get("call").and_then(|c| c.get("name")).and_then(Value::as_str).unwrap_or_default()
}

#[async_trait]
impl owf::run::Functions for Functions {
    fn events(&self) -> Option<&dyn owf::run::Events> {
        self.events.as_deref()
    }
    fn effect(&self, name: &str) -> Option<Effect> {
        if self.guard_only {
            return match name {
                "review" | "resolve_review" => Some(Effect::Read),
                "confirm_request" => Some(Effect::Write),
                _ => None,
            };
        }
        effect(&*self.engine.deps.mcp, name)
    }

    fn effect_of(&self, name: &str, args: &Value) -> Option<Effect> {
        if name == "start_child" && args["workflow"] != "guard" {
            return Some(Effect::Write);
        }
        if name == "tool" {
            return Some(crate::agent::tool_effect(&self.engine, loop_tool_name(args)));
        }
        self.effect(name)
    }

    // A durable sub-agent is the exception: calling it again joins its run.
    fn at_most_once(&self, name: &str, args: &Value) -> bool {
        // A confirmed open-world read is held exactly once too, as the old
        // outcome argument made it. Card metadata is host-owned evidence.
        let held = name == "tool"
            && self.runs.as_ref().zip(args["guard_run"].as_str()).is_some_and(|(runs, id)| {
                crate::agent::tool_call(&args["call"]).map_or(true, |call| {
                    runs.confirmations().get(id, &crate::guard::key(&call)).map_or(true, |p| p.is_some())
                })
            });
        name == "confirm_request"
            || (name == "tool"
                && (self.effect_of(name, args) == Some(Effect::Write)
                    || held
                    || args.get("outcome").is_some_and(|v| !v.is_null()))
                && (self.runs.is_none() || crate::agent::child_key(args).is_none()))
    }

    fn unknown_outcome(&self, name: &str, args: &Value) -> Option<Value> {
        (name == "tool").then(|| crate::agent::unknown_tool_outcome(loop_tool_name(args)))
    }

    // A loop's turn and calls stop on its cancellation and end their spans
    // and child loops first.
    fn cooperative(&self, name: &str) -> bool {
        is_loop_host(name) || name == "start_child" || self.guard_only
    }

    // A stop (shutdown, the loop's owner released) that cuts a call short is
    // an interruption, not its result: the run resumes at the call. Not for
    // a call that may have sent something: `confirm_request` (a card) and a
    // `tool` call held at most once keep their recorded or unknown outcome.
    async fn invoke(&self, name: &str, args: Value, info: &CallInfo<'_>) -> Result<Value, CallFailure> {
        if self.guard_only {
            return self.guard_step(name, args, info).await.map_err(|error| match name {
                "confirm_request" => CallFailure::Response(error.into_error()),
                _ => error.or_failure(CallFailure::Response),
            });
        }
        // A call written into a definition is trusted: the user approved
        // the definition after its dry run. Only the model's calls go through
        // `tool` and the guard. What stays refused protects the dispatcher:
        // its functions act on the one signal its run serves (an admitted
        // run has only its context subset).
        if is_dispatcher_function(name) {
            return Err(CallFailure::NotExecuted(invalid(name, DISPATCHER_ONLY)));
        }
        if is_loop_host(name) {
            // A tool reports its own failures in its reply; an error here
            // (a lost loop, a stop) leaves a held write's outcome unknown.
            let failure: fn(WorkflowError) -> CallFailure =
                if name == "tool" { CallFailure::Unknown } else { CallFailure::Response };
            let held = name == "confirm_request" || (name == "tool" && self.at_most_once(name, &args));
            return self
                .host_call(name, args, info)
                .await
                .map_err(|error| if held { failure(error.into_error()) } else { error.or_failure(failure) });
        }
        if MODEL_FUNCTIONS.iter().any(|f| f.0 == name) {
            if info.delivery == Delivery::AtMostOnce && name != "set_memory" {
                return Err(CallFailure::NotExecuted(WorkflowError::validation(
                    "model adapters do not yet support at_most_once",
                )));
            }
            return self.host_call(name, args, info).await.map_err(|error| {
                error.or_failure(|error| {
                    if error.is("validation") { CallFailure::NotExecuted(error) } else { CallFailure::Unknown(error) }
                })
            });
        }
        let Value::Object(args) = args else {
            return Err(CallFailure::NotExecuted(invalid(name, "arguments must be an object")));
        };
        let span = self.spans.tool_call(name, &args, info);
        let raw = trace_mcp_call(span, self.engine.deps.mcp.call_tool_once(name, args, info.cancel)).await.map_err(
            |failure| match failure {
                crate::mcp::ToolCallFailure::NotExecuted(error) => {
                    CallFailure::NotExecuted(WorkflowError::communication(503, error.to_string()))
                }
                crate::mcp::ToolCallFailure::Unknown(error) => {
                    CallFailure::Unknown(WorkflowError::communication(503, error.to_string()))
                }
            },
        )?;
        if raw.starts_with("[tool error]") {
            return Err(CallFailure::Response(WorkflowError::communication(500, raw)));
        }
        Ok(parse_tool_json(&raw))
    }

    async fn call(&self, name: &str, args: Value, info: &CallInfo<'_>) -> Result<Value, WorkflowError> {
        self.host_call(name, args, info).await.map_err(HostError::into_error)
    }
}

impl Functions {
    // The run's admitted model input: its record's in a run store, else the
    // one the host handed this in-process run.
    fn admitted(&self, name: &str, info: &CallInfo<'_>) -> Result<Value, HostError> {
        let admitted = match &self.runs {
            Some(runs) => runs.get(info.run_id).map_err(failed)?.as_ref().and_then(admitted_input).cloned(),
            None => self.loop_input.clone(),
        };
        admitted.ok_or_else(|| {
            invalid(
                name,
                "requires the host's admitted model input: a run the runtime started on a signal, or a worker; a child of start_child has none",
            )
            .into()
        })
    }

    // `call`, with a stop told apart from a failure for `invoke`.
    async fn host_call(&self, name: &str, args: Value, info: &CallInfo<'_>) -> Result<Value, HostError> {
        if self.guard_only {
            return self.guard_step(name, args, info).await;
        }
        if is_dispatcher_function(name) {
            return Err(invalid(name, DISPATCHER_ONLY).into());
        }
        let span = self.spans.scope(info.reference);
        let label = info.reference.rsplit('/').next().unwrap_or(name);
        match name {
            "agent_turn"
            | "guard"
            | "confirm_request"
            | "start_worker"
            | "tool"
            | "agent_settle"
            | "loop_setup"
            | "signal_input"
            | "recent_activity"
            | "pending_confirmations"
            | "preselect_skills" => {
                // A loop rebuilt after a restart is traced under the runner.
                let root: Scope = self.spans.root.clone();
                let engine = &self.engine;
                // The run id is supplied by the interpreter, not by YAML.
                // Rebuild from the admitted input, never a rewritten loop spec.
                let admitted = self.admitted(name, info)?;
                let spec: crate::agent::LoopSpec =
                    serde_json::from_value(admitted["loop"].clone()).map_err(|e| invalid(name, e))?;
                let live = engine.loop_for(&spec, &root, info.cancel).await.map_err(failed)?;
                live.admit_guard(&admitted).await.map_err(failed)?;
                if let Some(runs) = &self.runs {
                    live.restore_guard_history(runs, info.run_id).await.map_err(failed)?;
                }
                if LOOP_CONTEXT.contains(&name) {
                    return crate::agent::loop_context(
                        engine,
                        &live,
                        &admitted,
                        name,
                        &args,
                        self.runs.as_ref(),
                        info.cancel,
                    )
                    .await
                    .map_err(failed);
                }
                if name == "loop_setup" {
                    let records =
                        self.runs.as_ref().map(|runs| runs.records(info.run_id)).transpose().map_err(failed)?;
                    return crate::agent::loop_setup(engine, &live, &admitted, &args, records.as_deref())
                        .await
                        .map_err(failed);
                }
                let mut args = args;
                args["loop"] = admitted["loop"].clone();
                match name {
                    "agent_turn" => crate::agent::loop_turn(engine, &args, &root, info.cancel).await,
                    "guard" => {
                        let mut output =
                            crate::agent::loop_guard(engine, &args, &root, info.cancel).await.map_err(failed)?;
                        output["confirmable"] =
                            json!(self.runs.is_some() && self.events.is_some() && spec.confirm.is_some());
                        Ok(output)
                    }
                    "start_worker" => {
                        let Some((runs, events, key)) = self
                            .runs
                            .as_ref()
                            .zip(self.events.as_deref())
                            .zip(crate::agent::child_key(&args))
                            .map(|((r, e), k)| (r, e, k))
                        else {
                            return Ok(json!({"run":null,"loop":args["worker_loop"]}));
                        };
                        let expected = format!("{}__sub{key}/loop", spec.id);
                        let (id, input, worker_scope) = if let Some(saved) = runs.get(&expected).map_err(failed)? {
                            (expected, saved.input, root.span(SpanStart::new("invoke_sub_agent", SpanKind::Agent)))
                        } else {
                            match crate::agent::loop_worker_input(engine, &args, &root, info.cancel).await {
                                Ok(Some(prepared)) => prepared,
                                // `tool` presents the ordinary worker error to the model.
                                Ok(None) | Err(_) => return Ok(json!({"run":null})),
                            }
                        };
                        let name = loop_name(&input).to_owned();
                        let (definition, version) = loop_definition_named(engine, &name);
                        let input = with_override(input, version);
                        self.start_child(runs, events, &id, &name, &definition, input, &key, info, &worker_scope)
                            .await?;
                        Ok(json!({"run":id,"loop":name}))
                    }
                    "confirm_request" => {
                        crate::agent::loop_confirm_request(
                            engine,
                            &args,
                            &root,
                            info,
                            self.runs.as_ref(),
                            self.events.as_deref(),
                        )
                        .await
                    }
                    "tool" => {
                        crate::agent::loop_tool_at(
                            engine,
                            &args,
                            &root,
                            info.cancel,
                            self.runs.as_ref(),
                            Some(info.run_id),
                            Some(info.reference),
                        )
                        .await
                    }
                    _ => crate::agent::loop_settle(engine, &args, &root, info.cancel).await,
                }
                .map_err(failed)
            }
            // The guard child and a worker (the model's `invoke_sub_agent`
            // call) are the host's children of an admitted run; any other is
            // a saved workflow of the author's.
            "start_child" if args["workflow"] == "guard" || args.get("call").is_some_and(|c| !c.is_null()) => {
                self.loop_child(args, info).await
            }
            "start_child" => {
                let workflow = text(&args, name, "workflow")?.ok_or_else(|| invalid(name, "workflow required"))?;
                let key = text(&args, name, "key")?.ok_or_else(|| invalid(name, "key required"))?;
                let runs = self.runs.as_ref().ok_or_else(|| invalid(name, "requires a durable run"))?;
                let events = self.events.as_deref().ok_or_else(|| invalid(name, "requires events"))?;
                let id = format!("{}/child/{}", info.run_id, crate::skills::hash_value(&json!([info.reference, key])));
                if built_in(workflow).is_some() {
                    return Err(invalid(name, "a built-in child workflow is the host's: the guard, or a worker for the model's invoke_sub_agent `call`").into());
                }
                // The model's parameters are the host's, never a workflow's.
                if args.get("input").is_some_and(|i| i.get("loop").is_some()) {
                    return Err(invalid(
                        name,
                        "a child's input cannot carry `loop`: only the host admits the model's parameters",
                    )
                    .into());
                }
                let definition = self
                    .engine
                    .deps
                    .workflows
                    .active(workflow)
                    .map_err(failed)?
                    .ok_or_else(|| invalid(name, "workflow is not active"))?
                    .definition;
                if is_loop_definition(workflow, &definition) {
                    return Err(
                        invalid(name, "a loop child is a worker: pass the model's invoke_sub_agent `call`").into()
                    );
                }
                let input = args.get("input").cloned().unwrap_or(json!({}));
                self.start_child(
                    runs,
                    events,
                    &id,
                    workflow,
                    &definition,
                    input,
                    key,
                    info,
                    &self.spans.task_span(info.reference),
                )
                .await?;
                Ok(json!({"run":id}))
            }
            "child_outcome" => {
                let id = text(&args, name, "run")?.ok_or_else(|| invalid(name, "run required"))?;
                let key = text(&args, name, "key")?.ok_or_else(|| invalid(name, "key required"))?;
                let status = self
                    .runs
                    .as_ref()
                    .ok_or_else(|| invalid(name, "requires a durable run"))?
                    .child_outcome(info.run_id, key, id)
                    .map_err(failed)?;
                Ok(json!(status))
            }
            "hash_value" => {
                let value = args.get("value").ok_or_else(|| invalid(name, "`value` is required"))?;
                Ok(json!({"hash":crate::skills::hash_value(value)}))
            }
            "skill_catalog_snapshot" => {
                crate::skills::catalog_snapshot(&*self.engine.deps.skill_store).await.map_err(failed)
            }
            "llm_compose" => {
                let preset = text(&args, name, "preset")?.unwrap_or("compose");
                let skill = text(&args, name, "skill")?;
                let mut parts = Vec::new();
                if let Some(prompt) = text(&args, name, "prompt")? {
                    parts.push(prompt.to_owned());
                }
                match args.get("input") {
                    None | Some(Value::Null) => {}
                    Some(Value::Object(input)) => parts.push(render_input_xml(input)),
                    Some(_) => return Err(invalid(name, "`input` must be an object").into()),
                }
                let json = match args.get("json") {
                    None | Some(Value::Null) => false,
                    Some(Value::Bool(json)) => *json,
                    Some(_) => return Err(invalid(name, "`json` must be true or false").into()),
                };
                let user = parts.into_iter().filter(|p| !p.is_empty()).collect::<Vec<_>>().join("\n\n");
                span.update(crate::tracing::ContextUpdate::input(json!({"skill":skill,"prompt":user})));
                compose(&self.engine, preset, skill, &user, json, label, &span, info.cancel).await.map_err(failed)
            }
            "filter_relevant" => {
                let decisions = self.engine.deps.decisions.as_ref().ok_or_else(|| {
                    WorkflowError::runtime("no decision model configured (OPEN_ROUTER_API_KEY)".to_string())
                })?;
                let items = args
                    .get("items")
                    .and_then(Value::as_array)
                    .ok_or_else(|| invalid(name, "`items` must be an array"))?;
                let prompt = text(&args, name, "prompt")?.ok_or_else(|| invalid(name, "`prompt` is required"))?;
                let threshold = match args.get("threshold") {
                    None | Some(Value::Null) => None,
                    Some(t) => Some(t.as_f64().ok_or_else(|| invalid(name, "`threshold` must be a number"))?),
                };
                let filtered = crate::decisions::filter(decisions, items, prompt, threshold, &span, info.cancel)
                    .await
                    .map_err(failed)?;
                Ok(json!(filtered.kept))
            }
            "choose" => {
                let decisions = self.engine.deps.decisions.as_ref().ok_or_else(|| {
                    WorkflowError::runtime("no decision model configured (OPEN_ROUTER_API_KEY)".to_string())
                })?;
                let question = text(&args, name, "question")?
                    .filter(|q| !q.trim().is_empty())
                    .ok_or_else(|| invalid(name, "`question` must be a nonempty string"))?;
                let options = choose_options(&args).map_err(|e| invalid(name, e))?;
                let input = args.get("input").cloned().unwrap_or(Value::Null);
                if input.to_string().len() > crate::decisions::FILTER_MAX_BATCH_CHARS {
                    return Err(invalid(
                        name,
                        format!(
                            "`input` is over {} characters as JSON; pass the fields the choice needs",
                            crate::decisions::FILTER_MAX_BATCH_CHARS
                        ),
                    )
                    .into());
                }
                let floor = match (args.get("min_confidence"), args.get("default")) {
                    (None | Some(Value::Null), None | Some(Value::Null)) => None,
                    (Some(min), Some(Value::String(default))) => {
                        let min = min
                            .as_f64()
                            .filter(|m| (0.0..=1.0).contains(m))
                            .ok_or_else(|| invalid(name, "`min_confidence` must be a number from 0 to 1"))?;
                        if !options.contains_key(default) {
                            return Err(
                                invalid(name, format!("`default` `{default}` is not one of the options")).into()
                            );
                        }
                        Some((min, default.clone()))
                    }
                    _ => return Err(invalid(name, "`min_confidence` and `default` (an option) go together").into()),
                };
                span.update(crate::tracing::ContextUpdate::input(
                    json!({"question":question,"options":options,"input":input}),
                ));
                let answer = crate::decisions::choose(decisions, question, options, &input, &span, info.cancel)
                    .await
                    .map_err(failed)?;
                let (choice, fallback) = match floor {
                    Some((min, default)) if answer.confidence < min => (default, true),
                    _ => (answer.choice, false),
                };
                span.update(crate::tracing::ContextUpdate::metadata(metadata(
                    json!({"choice":choice,"confidence":answer.confidence,"fallback":fallback}),
                )));
                Ok(json!({"choice":choice,"confidence":answer.confidence,
                    "probabilities":answer.probabilities,"fallback":fallback}))
            }
            "set_memory" => {
                let key = text(&args, name, "key")?
                    .filter(|k| !k.is_empty())
                    .ok_or_else(|| invalid(name, "`key` must be a nonempty string"))?;
                let value = text(&args, name, "value")?.ok_or_else(|| invalid(name, "`value` must be a string"))?;
                let span = self.spans.tool_call(name, &args, info);
                let result = if let Some(sandbox) = self.engine.sandbox() {
                    Ok(parse_tool_json(&sandbox.mock(name, args.clone())))
                } else {
                    self.engine.deps.memory.set(key, value).map(|()| json!({"ok":true,"key":key}))
                };
                span.end(match &result {
                    Ok(value) => SpanEnd::output(value.clone()),
                    Err(error) => SpanEnd::error(error.to_string()).with_output(json!({"error":error.to_string()})),
                });
                result.map_err(failed)
            }
            tool => {
                let Value::Object(args) = args else { return Err(invalid(tool, "arguments must be an object").into()) };
                let span = self.spans.tool_call(tool, &args, info);
                let raw =
                    trace_mcp_call(span, async {
                        self.engine
                            .deps
                            .mcp
                            .call_tool(tool, args, info.cancel)
                            .await
                            .map_err(crate::mcp::ToolCallFailure::Unknown)
                    })
                    .await
                    .map_err(|failure| {
                        let (crate::mcp::ToolCallFailure::Unknown(error)
                        | crate::mcp::ToolCallFailure::NotExecuted(error)) = failure;
                        WorkflowError::communication(503, format!("{tool}: {error}"))
                    })?;
                if raw.starts_with("[tool error]") {
                    return Err(WorkflowError::communication(500, raw).into());
                }
                Ok(parse_tool_json(&raw))
            }
        }
    }
}

// ── 2b. model composition and signal input ──────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkflowSignal {
    pub id: i64,
    pub source: String,
    pub content: String,
    #[serde(rename = "envContext")]
    pub env_context: Option<String>,
}

pub fn signal_input(signal: &WorkflowSignal, env: &EnvData) -> Map<String, Value> {
    let local_now = match env.timezone.parse::<chrono_tz::Tz>() {
        Ok(zone) => env.now.with_timezone(&zone).to_rfc3339_opts(chrono::SecondsFormat::Secs, false),
        Err(_) => env.now.to_rfc3339_opts(chrono::SecondsFormat::Secs, false),
    };
    json!({"env":{"timezone":env.timezone,"now":iso(env.now),"localNow":local_now,"newsLastReadAt":env.news_last_read_at,"techLastReadAt":env.tech_last_read_at,"userEmail":env.user_email,"telegramChatId":env.telegram_chat_id},"signal":{"source":signal.source,"content":signal.content,"id":signal.id}}).as_object().unwrap().clone()
}

pub fn render_input_xml(input: &Map<String, Value>) -> String {
    input
        .iter()
        .map(|(key, value)| {
            format!(
                "<{key}>\n{}\n</{key}>",
                value.as_str().map(str::to_owned).unwrap_or_else(|| serde_json::to_string_pretty(value).unwrap())
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

pub fn parse_tool_json(raw: &str) -> Value {
    if raw.starts_with(['{', '[']) { serde_json::from_str(raw).unwrap_or_else(|_| json!(raw)) } else { json!(raw) }
}

/// One `llm_compose` generation: the `composer` base prompt plus `skill`
/// (with its patch) as the system message, `user` as the only user message.
/// Traced as `llm_compose:<label>` tagged `judge_node: compose`, which is how
/// the judge finds compose nodes. A reply that is JSON comes back parsed.
/// With `json` the provider is held to one JSON object, and a reply that is
/// not one is an error rather than text a branch would misread.
#[allow(clippy::too_many_arguments)]
pub async fn compose(
    engine: &Engine,
    preset: &str,
    skill: Option<&str>,
    user: &str,
    json: bool,
    label: &str,
    scope: &Scope,
    cancel: &CancellationToken,
) -> anyhow::Result<Value> {
    let preset = engine
        .deps
        .presets
        .get(PresetName::for_compose(preset).ok_or_else(|| anyhow::anyhow!("unknown preset"))?)
        .clone();
    let provider = engine.resolve_provider(&preset.model)?;
    let base = engine.deps.skill_store.read_skill("composer").await?.map(|s| s.body);
    let system = if let Some(skill) = skill {
        let body = engine
            .deps
            .skill_store
            .read_skill(skill)
            .await?
            .ok_or_else(|| anyhow::anyhow!("skill not found: {skill}"))?
            .body;
        let patch = engine.deps.skill_store.read_patch(skill).await?.unwrap_or_default();
        let body = append_patch(&body, &patch);
        if let Some(base) = base.filter(|s| !s.is_empty()) { Some(format!("{base}\n\n{body}")) } else { Some(body) }
    } else {
        base
    };
    let mut messages = Vec::new();
    if let Some(system) = system {
        messages.push(providers::system(&system));
    }
    // OpenAI refuses JSON mode unless the messages ask for JSON.
    if json {
        messages.push(providers::user(&format!("{user}\n\nReply with one JSON object.")));
    } else {
        messages.push(providers::user(user));
    }
    let mut model_parameters = json!({"reasoning_effort":preset.reasoning_effort.as_str(),"tools_mode":"none"});
    if json {
        model_parameters["response_format"] = json!("json_object");
    }
    let mut params = CompletionParams {
        model: preset.model,
        messages: &mut messages,
        reasoning_effort: preset.reasoning_effort,
        tools: Vec::new(),
        response_format: json.then(|| json!({"type":"json_object"})),
        trace: None,
        cancel: cancel.clone(),
    };
    let result = generation(
        &provider,
        &mut params,
        scope,
        Observation {
            name: format!("llm_compose:{label}"),
            model_parameters: model_parameters.as_object().cloned(),
            metadata: Some(json!({"judge_node":"compose","skill":skill}).as_object().unwrap().clone()),
        },
        None,
        |r| GenerationEnd {
            output: Some(json!(providers::content(&r.message).unwrap_or_default())),
            usage: r.usage.clone(),
            ..Default::default()
        },
    )
    .await
    .map_err(|e| if is_aborted(&e) { e } else { anyhow::anyhow!("llm call failed: {e}") })?;
    crate::errors::check(cancel)?;
    let reply = providers::content(&result.message).unwrap_or_default();
    if json {
        return match serde_json::from_str::<Value>(reply.trim()) {
            Ok(object @ Value::Object(_)) => Ok(object),
            _ => Err(anyhow::anyhow!(
                "llm call failed: the reply is not a JSON object: {}",
                crate::activity::clip(reply, 200)
            )),
        };
    }
    Ok(parse_tool_json(reply))
}

// ── 3. tracing ─────────────────────────────────────────────────────────────

// One span per task, nested as the tasks are; a call's model generation
// nests inside its task's span.
struct Spans {
    root: Arc<dyn Span>,
    open: Mutex<HashMap<String, Arc<dyn Span>>>,
}

impl Spans {
    fn scope(&self, reference: &str) -> Scope {
        self.task_span(reference)
    }
    fn task_span(&self, reference: &str) -> Arc<dyn Span> {
        match self.open.lock().unwrap_or_else(|e| e.into_inner()).get(reference) {
            Some(span) => span.clone(),
            None => self.root.clone(),
        }
    }

    fn take(&self, reference: &str) -> Option<Arc<dyn Span>> {
        self.open.lock().unwrap_or_else(|e| e.into_inner()).remove(reference)
    }

    fn tool_call(&self, name: &str, args: &impl Serialize, info: &CallInfo<'_>) -> Arc<dyn Span> {
        let task = self.open.lock().unwrap_or_else(|e| e.into_inner()).get(info.reference).cloned();
        let parent = task.as_ref().unwrap_or(&self.root);
        let call = parent.span(
            SpanStart::new(name, SpanKind::Tool)
                .input(json!(args))
                .metadata(metadata(json!({"tool":name,"reference":info.reference,"workflow_run":info.run_id}))),
        );
        if let Some(task) = task {
            // The task still records its transformed output, but consumers
            // count the actual tool call as the action, not its wrapper too.
            task.update(crate::tracing::ContextUpdate::metadata(metadata(json!({"tool_span":call.id()}))));
        }
        call
    }
}

// Calls need their own spans even when loop task scaffolding is silent.
// Record the complete wire reply before YAML output transforms; never write
// call arguments over the runner's admitted input. The future may be dropped
// by the interpreter on cancellation, which must end the span too.
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
    pending.0.take().expect("call ends once").end(end);
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

fn task_name(reference: &str) -> &str {
    reference.rsplit('/').next().unwrap_or(reference)
}

fn span_kind(kind: &str) -> SpanKind {
    match kind {
        "call" => SpanKind::Tool,
        "do" | "for" | "fork" | "try" | "switch" => SpanKind::Chain,
        _ => SpanKind::Span,
    }
}

fn metadata(value: Value) -> Map<String, Value> {
    value.as_object().cloned().unwrap_or_default()
}

impl Observer for Spans {
    fn event(&self, event: &Event<'_>) {
        match event {
            Event::TaskStarted { reference, parent, kind, input } => {
                let parent = parent.map(|p| self.scope(p)).unwrap_or_else(|| self.root.clone());
                let span = parent.span(
                    SpanStart::new(format!("{kind}:{}", task_name(reference)), span_kind(kind))
                        .input((*input).clone())
                        .metadata(metadata(json!({"reference":reference,"kind":kind}))),
                );
                self.open.lock().unwrap_or_else(|e| e.into_inner()).insert(reference.to_string(), span);
            }
            Event::TaskCompleted { reference, output } => {
                if let Some(span) = self.take(reference) {
                    span.end(SpanEnd::output((*output).clone()));
                }
            }
            Event::TaskSuspended { reference, wake_at_ms } => {
                if let Some(span) = self.take(reference) {
                    span.end(SpanEnd::output(json!({"waiting_until_ms":wake_at_ms})));
                }
            }
            Event::SwitchCase { reference, case } => {
                self.scope(reference).update(crate::tracing::ContextUpdate::metadata(metadata(json!({"case":case}))))
            }
            Event::TaskFaulted { reference, error } => match self.take(reference) {
                Some(span) => span.end(SpanEnd::error(error.to_string()).with_output(json!(error))),
                // Faulted before it started (its `if` or a limit).
                None => self.root.event(EventStart {
                    name: format!("faulted:{}", task_name(reference)),
                    level: Some(Level::Error),
                    metadata: Some(metadata(json!({"reference":reference,"error":error}))),
                    ..Default::default()
                }),
            },
            Event::TaskSkipped { reference, parent } => {
                let parent = parent.map(|p| self.scope(p)).unwrap_or_else(|| self.root.clone());
                parent.event(EventStart {
                    name: format!("skipped:{}", task_name(reference)),
                    metadata: Some(metadata(json!({"reference":reference}))),
                    ..Default::default()
                });
            }
            Event::TaskRetrying { reference, attempt, delay, error } => self.scope(reference).event(EventStart {
                name: "retry".into(),
                level: Some(Level::Warning),
                metadata: Some(metadata(
                    json!({"reference":reference,"attempt":attempt,"delay_ms":delay.as_millis() as u64,"error":error}),
                )),
                ..Default::default()
            }),
        }
    }
}

// ── 4. running a saved workflow ────────────────────────────────────────────

#[derive(Debug)]
pub enum FlowResult {
    // No active version of that name.
    Missing,
    Completed(Value),
    // `effects_started`: a `Write` call had started, so the run may have
    // changed something; before that, it changed nothing.
    Faulted { error: WorkflowError, effects_started: bool },
    Cancelled,
    Waiting { wake_at_ms: i64 },
    Interrupted { error: WorkflowError },
}

#[async_trait]
pub trait FlowRun: Send + Sync {
    fn pending_confirmations(&self, _target: &crate::agent::ConfirmTarget) -> anyhow::Result<Value> {
        Ok(json!([]))
    }

    fn deliver_event(&self, _type: &str, _data: &Value) -> anyhow::Result<bool> {
        Ok(false)
    }

    fn deliver_event_in(&self, type_: &str, data: &Value, _scope: &Scope) -> anyhow::Result<bool> {
        self.deliver_event(type_, data)
    }

    /// The one way a run starts: the workflow `input.loop.workflow` as run
    /// `id` on the host's admitted input (`AgentLoop::workflow_input`),
    /// driven to its next stop. A run the runtime starts on a signal may be
    /// any workflow (`run_definition`); a worker's runs a loop. A run that
    /// exists already continues on its own pinned input and definition.
    async fn run_loop(&self, id: &str, input: Value, scope: &Scope, cancel: &CancellationToken) -> FlowResult;
}

// Runs the recovery worker resumes at once.
const RECOVERY_CONCURRENCY: usize = 4;
// How often the recovery worker deletes the records of long-finished runs.
const SWEEP_EVERY: std::time::Duration = std::time::Duration::from_secs(3_600);

pub struct Flows {
    engine: Arc<Engine>,
    runs: Option<crate::durable::RunStore>,
}

/// Ephemeral runner for tests and callers explicitly needing no persistence.
pub fn create_flows(engine: Arc<Engine>) -> Arc<Flows> {
    Arc::new(Flows { engine, runs: None })
}

pub fn create_durable_flows(engine: Arc<Engine>, db: AgentDb) -> Arc<Flows> {
    Arc::new(Flows { engine, runs: Some(crate::durable::RunStore::new(db)) })
}

pub fn flow_result(status: RunStatus, effects_started: bool) -> FlowResult {
    match status {
        RunStatus::Completed(output) => FlowResult::Completed(output),
        RunStatus::Faulted(error) => {
            let effects_started = effects_started || error.is("outcome_unknown");
            FlowResult::Faulted { error, effects_started }
        }
        RunStatus::Cancelled => FlowResult::Cancelled,
        RunStatus::Waiting { wake_at_ms } => FlowResult::Waiting { wake_at_ms },
        RunStatus::Interrupted(error) => FlowResult::Interrupted { error },
    }
}

// Runs a claimed run to its next stop under `lease`, renewing it, and
// stores how it ended (a stop of this process hands it back).
fn execute_saved<'a>(
    engine: &'a Arc<Engine>,
    runs: &'a crate::durable::RunStore,
    saved: crate::durable::SavedRun,
    lease: crate::durable::Lease,
    scope: &'a Scope,
    cancel: &'a CancellationToken,
) -> BoxFuture<'a, FlowResult> {
    async move {
        // A run the host admitted (on a signal, or a worker) counts toward
        // its definition's breaker and drives a live loop.
        let admitted = admitted_input(&saved).is_some();
        let version = saved.input.get(OVERRIDE).and_then(Value::as_i64);
        let guard_parent = if saved.name == "guard" {
            let parent = saved.parent_run.as_deref().map(|id| runs.get(id)).transpose().ok().flatten().flatten();
            let id = parent.and_then(|p| p.input["loop"]["id"].as_str().map(str::to_owned));
            match id {
                Some(id) if !engine.loop_ids().await.contains(&id) => Some(id),
                _ => None,
            }
        } else {
            None
        };
        let children = Arc::new(Children::default());
        let lease = Arc::new(lease);
        let root = scope.span(SpanStart::new("runner", SpanKind::Chain).input(saved.input.clone())
            .metadata(metadata(json!({"durable_run":saved.id,"durable_workflow":saved.name,"workflow":saved.name}))));
        let mut stopped = false;
        let status = match owf::model::parse_yaml(&saved.definition) {
            Err(error) => (RunStatus::Interrupted(WorkflowError::validation(error.to_string())), true),
            Ok(wf) => {
                let mut host = host(
                    engine,
                    &root,
                    is_loop_definition(&saved.name, &saved.definition),
                    saved.name == "guard",
                    Some(runs.clone()),
                    None,
                    Some(lease.clone()),
                    children.clone(),
                );
                if let Some(sandbox) = engine.sandbox() {
                    let version = match saved.name.as_str() {
                        _ if admitted => version,
                        "guard" => None,
                        name => engine.deps.workflows.active(name).ok().flatten().map(|w| w.version),
                    };
                    let loop_id = saved.input["loop"]["id"].as_str().filter(|_| admitted);
                    sandbox.watch(&mut host, &saved.id, &saved.name, version, saved.parent_run.as_deref(), loop_id);
                }
                // Shutdown releases work for recovery; only explicit run
                // cancellation is allowed to create a terminal Cancelled state.
                let execution_cancel = CancellationToken::new();
                let execution = owf::run::resume(&host, &wf, &saved.id, saved.input, &execution_cancel, &*lease);
                tokio::pin!(execution);
                let mut heartbeat =
                    tokio::time::interval(std::time::Duration::from_millis(crate::durable::HEARTBEAT_MS));
                loop {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => {
                            stopped = true;
                            break (RunStatus::Interrupted(WorkflowError::runtime("worker stopped")), true);
                        }
                        _ = heartbeat.tick() => if let Err(error) = lease.renew() {
                            break (RunStatus::Interrupted(WorkflowError::runtime(error.to_string())), true);
                        },
                        outcome = &mut execution => break (outcome.status, outcome.effects_started),
                    }
                }
            }
        };
        children.close().await;
        if let Some(id) = guard_parent {
            engine.end_loop(&id).await;
        }
        let (status, effects_started) = status;
        // Publish the breaker's decision before completion wakes the parent;
        // otherwise a fast parent can admit the next worker on a bad version.
        if admitted {
            record_outcome(engine, &saved.name, version, &flow_result(status.clone(), effects_started));
        }
        // A host call cut short by a stop this attempt did not see first (a
        // worker's model turn aborted by shutdown or by its parent's release
        // before `children.close` reaches it) ends Interrupted, not stopped:
        // it takes the interruption backoff rather than a release. The first
        // retry is due 2 s later, which the recovery worker of the next
        // process picks up at once; a release would be due at once too, but
        // a cause that aborts every attempt (a loop whose token stays
        // cancelled) would then retry forever without MAX_INTERRUPTIONS.
        // Neither publishes `done:` or `waiting:`, so a parent keeps waiting.
        let interrupted = matches!(status, RunStatus::Interrupted(_));
        let result = match if stopped { lease.release().map(|()| status) } else { lease.finish(&status) } {
            Ok(status) => flow_result(status, effects_started),
            Err(error) => FlowResult::Interrupted { error: WorkflowError::runtime(error.to_string()) },
        };
        if admitted && interrupted && matches!(result, FlowResult::Faulted { .. }) {
            record_outcome(engine, &saved.name, version, &result);
        }
        root.update(crate::tracing::ContextUpdate::metadata(metadata(json!({"durable_terminal": matches!(result, FlowResult::Completed(_) | FlowResult::Faulted { .. } | FlowResult::Cancelled)}))));
        root.end(match &result {
            // Handed back for the next process to resume: not a failure.
            FlowResult::Interrupted { .. } if stopped => SpanEnd::output(json!({"released":true})),
            FlowResult::Completed(output) => SpanEnd::output(output.clone()),
            FlowResult::Faulted { error, .. } | FlowResult::Interrupted { error } => SpanEnd::error(error.to_string()),
            FlowResult::Waiting { wake_at_ms } => SpanEnd::output(json!({"waiting_until_ms":wake_at_ms})),
            _ => SpanEnd::default(),
        });
        result
    }
    .boxed()
}

impl Flows {
    // Claims up to `limit` runs that are due: a passed deadline or an
    // expired lease.
    fn claim_due(
        &self,
        runs: &crate::durable::RunStore,
        limit: usize,
    ) -> anyhow::Result<Vec<(crate::durable::SavedRun, crate::durable::Lease)>> {
        let mut claimed = Vec::new();
        for id in runs.due()? {
            if claimed.len() >= limit {
                break;
            }
            let Some(lease) = runs.claim(&id)? else { continue };
            let saved = runs.get(&id)?.ok_or_else(|| anyhow::anyhow!("run {id} disappeared"))?;
            claimed.push((saved, lease));
        }
        Ok(claimed)
    }

    async fn resume(
        &self,
        runs: &crate::durable::RunStore,
        saved: crate::durable::SavedRun,
        lease: crate::durable::Lease,
        cancel: &CancellationToken,
    ) {
        let id = saved.id.clone();
        let admitted = admitted_input(&saved).is_some();
        // Pre-anchor runs retain their fresh-trace/session-only recovery.
        // New runs attach an execution segment to their original scope.
        let admitted_session = |run: &crate::durable::SavedRun| {
            let value = if admitted_input(run).is_some() {
                &run.input["loop"]["session"]
            } else if run.parent_run.is_none() {
                &run.input["__session_id"]
            } else {
                return None;
            };
            value.as_str().map(str::to_owned)
        };
        let mut session = admitted_session(&saved);
        let mut session_run = id.clone();
        let mut parent = saved.parent_run.clone();
        while session.is_none() {
            let Some(id) = parent.take() else { break };
            session_run = id.clone();
            let Ok(Some(run)) = runs.get(&id) else { break };
            session = admitted_session(&run);
            parent = run.parent_run;
        }
        let anchored = saved.trace_anchor.is_some();
        let owns_output = !anchored || saved.parent_run.is_none();
        let trace = self.engine.deps.tracer.trace(crate::tracing::TraceStart {
            continuation: saved.trace_anchor.clone(),
            id: uuid::Uuid::new_v4().to_string(),
            name: format!("resume:{}", saved.name),
            session_id: saved
                .trace_anchor
                .as_ref()
                .and_then(|a| a.session_id.clone())
                .or(Some(session.unwrap_or(session_run))),
            metadata: Some(json!({"run": id}).as_object().expect("object").clone()),
            ..Default::default()
        });
        if let Some(task) = saved.input["guard_context"]["task"].as_str().filter(|_| admitted && !anchored) {
            trace.update(crate::tracing::ContextUpdate::input(task));
        }
        let scope: Scope = trace.clone();
        let driven = admitted.then(|| saved.input["loop"]["id"].as_str().map(str::to_owned)).flatten();
        let result = execute_saved(&self.engine, runs, saved, lease, &scope, cancel).await;
        if owns_output {
            match &result {
                FlowResult::Completed(output) => trace.update(crate::tracing::ContextUpdate::output(output.clone())),
                FlowResult::Faulted { error, .. } => trace.update(crate::tracing::ContextUpdate {
                    output: Some(json!({"error":error.to_string()})),
                    metadata: Some(metadata(json!({"error":true}))),
                    ..Default::default()
                }),
                FlowResult::Cancelled => trace.update(crate::tracing::ContextUpdate::output(json!({"cancelled":true}))),
                _ => {}
            }
        }
        if let Some(id) = driven {
            self.engine.end_loop(&id).await;
        }
        trace.end();
        if let FlowResult::Faulted { error, .. } | FlowResult::Interrupted { error } = result {
            eprintln!("[durable] {id}: {error}");
        }
    }

    /// Resumes the runs due now, concurrently, and waits for them.
    pub async fn recover_once(&self, cancel: &CancellationToken) -> anyhow::Result<usize> {
        let Some(runs) = &self.runs else { return Ok(0) };
        let claimed = self.claim_due(runs, RECOVERY_CONCURRENCY)?;
        let count = claimed.len();
        futures::future::join_all(claimed.into_iter().map(|(saved, lease)| self.resume(runs, saved, lease, cancel)))
            .await;
        Ok(count)
    }

    /// Resumes due runs as they come due, up to RECOVERY_CONCURRENCY at once:
    /// a long run does not hold up another's deadline. Runs here go alongside
    /// the supervisor's signal, which is fine: each run owns its own state.
    pub async fn worker(&self, cancel: CancellationToken) {
        let Some(runs) = &self.runs else { return };
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut running = futures::stream::FuturesUnordered::new();
        let mut swept: Option<std::time::Instant> = None;
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                Some(()) = running.next(), if !running.is_empty() => continue,
                _ = tick.tick() => {}
            }
            if swept.is_none_or(|at| at.elapsed() >= SWEEP_EVERY) {
                swept = Some(std::time::Instant::now());
                if let Err(error) = runs.sweep(crate::durable::RECORDS_KEPT_MS) {
                    eprintln!("[durable] sweep: {error:#}");
                }
            }
            match self.claim_due(runs, RECOVERY_CONCURRENCY - running.len()) {
                Ok(claimed) => {
                    running.extend(claimed.into_iter().map(|(saved, lease)| self.resume(runs, saved, lease, &cancel)))
                }
                Err(error) => eprintln!("[durable] recovery: {error:#}"),
            }
        }
        // The runs see the same cancel and release their leases.
        while running.next().await.is_some() {}
    }
}

// Parses and validates `definition` against what this agent can call, and
// each call's argument names against the function's: a misspelt argument
// fails here rather than on the first scheduled run.
pub fn check(mcp: &dyn Mcp, definition: &str) -> Result<owf::model::Workflow, String> {
    struct Known<'a>(&'a dyn Mcp);
    impl owf::model::Catalog for Known<'_> {
        fn events(&self) -> bool {
            true
        }
        fn has(&self, name: &str) -> bool {
            name != "confirm_request" && effect(self.0, name).is_some()
        }
    }
    let wf = owf::model::parse_yaml(definition).map_err(|e| e.to_string())?;
    owf::model::validate(&wf, &Known(mcp)).map_err(|e| e.to_string())?;
    let mut problems = Vec::new();
    for site in owf::model::call_sites(&wf) {
        if site.delivery == Delivery::AtMostOnce
            && matches!(site.function.as_str(), "llm_compose" | "filter_relevant" | "choose")
        {
            problems
                .push(format!("{}/metadata/delivery: model adapters do not yet support at_most_once", site.reference));
        }
        if wf.uses.functions.iter().any(|(n, _)| *n == site.function) {
            continue;
        }
        let Some((takes, needs)) = arguments(mcp, &site.function) else { continue };
        // The parser only accepts an object of arguments.
        let given: Vec<String> =
            site.with.iter().filter_map(Value::as_object).flat_map(|a| a.keys().cloned()).collect();
        let (at, function) = (&site.reference, &site.function);
        if let Some(takes) = &takes {
            for arg in given.iter().filter(|a| !takes.contains(a)) {
                problems.push(format!(
                    "{at}/with: `{function}` takes no argument `{arg}` (it takes: {})",
                    takes.join(", ")
                ));
            }
        }
        for arg in needs.iter().filter(|a| !given.contains(a)) {
            problems.push(format!("{at}/with: `{function}` needs the argument `{arg}`"));
        }
    }
    if problems.is_empty() {
        Ok(wf)
    } else {
        Err(format!(
            "invalid workflow:\n{}",
            problems.iter().map(|p| format!("  - {p}")).collect::<Vec<_>>().join("\n")
        ))
    }
}

#[async_trait]
impl FlowRun for Flows {
    fn pending_confirmations(&self, target: &crate::agent::ConfirmTarget) -> anyhow::Result<Value> {
        self.runs.as_ref().map_or(Ok(json!([])), |r| r.confirmations().pending(target))
    }

    fn deliver_event(&self, type_: &str, data: &Value) -> anyhow::Result<bool> {
        self.deliver_event_in(type_, data, &crate::tracing::null_scope())
    }

    fn deliver_event_in(&self, type_: &str, data: &Value, scope: &Scope) -> anyhow::Result<bool> {
        let Some(runs) = &self.runs else { return Ok(false) };
        let Some(id) = runs.deliver_to(type_, data)? else { return Ok(false) };
        if let Some(anchor) = runs.get(&id)?.and_then(|r| r.trace_anchor) {
            scope.update(crate::tracing::ContextUpdate { session_id: anchor.session_id.clone(), ..Default::default() });
            let trace = self.engine.deps.tracer.trace(crate::tracing::TraceStart {
                name: "delivery".into(),
                session_id: anchor.session_id.clone(),
                continuation: Some(anchor),
                ..Default::default()
            });
            trace.event(crate::tracing::EventStart {
                name: format!("event:{type_}"),
                input: Some(data.clone()),
                metadata: Some(metadata(json!({"run":id}))),
                ..Default::default()
            });
            trace.end();
        }
        Ok(true)
    }

    async fn run_loop(&self, id: &str, input: Value, scope: &Scope, cancel: &CancellationToken) -> FlowResult {
        match &self.runs {
            Some(runs) => run_loop_durable(&self.engine, runs, id, input, scope, cancel).await,
            None => run_loop_here(&self.engine, id, input, scope, cancel).await,
        }
    }
}

impl Flows {
    /// A scheduled fire of the saved workflow `name` without a supervisor
    /// (tools, tests): the supervisor's scheduled launch (supervisor.rs,
    /// `run_saved_workflow`) with its defaults and no context of its own,
    /// so the same resolution, the same admission and the same `run_loop`
    /// as run `<source>:<id>`. A run of that id that exists continues.
    pub async fn run(
        &self,
        name: &str,
        signal: &WorkflowSignal,
        session: &Session,
        scope: &Scope,
        cancel: &CancellationToken,
    ) -> FlowResult {
        let id = format!("{}:{}", signal.source, signal.id);
        let exists = match self.runs.as_ref().map(|runs| runs.get(&id)).transpose() {
            Ok(run) => run.flatten().is_some(),
            Err(error) => return FlowResult::Interrupted { error: WorkflowError::runtime(error.to_string()) },
        };
        if exists {
            return self.run_loop(&id, Value::Null, scope, cancel).await;
        }
        if let Some(stop) = unrunnable(&self.engine, name) {
            return stop;
        }
        let pending = crate::supervisor::PendingSignal {
            id: signal.id,
            source: signal.source.clone(),
            content: signal.content.clone(),
            env_context: signal.env_context.clone(),
            created_at: chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
            payload: Some(json!({"workflow": name})),
        };
        let launch = crate::supervisor::Launch::Scheduled;
        let admitted = async {
            let mut opts = crate::supervisor::admission(&self.engine, &pending, session, name, launch, None).await?;
            opts.cancel = cancel.clone();
            opts.trace_scope = Some(scope.clone());
            let agent = self.engine.start_loop(opts).await?;
            Ok::<_, anyhow::Error>(crate::supervisor::run_input(&agent, &pending.content, &pending, session).await)
        }
        .await;
        let result = match admitted {
            Ok(input) => self.run_loop(&id, input, scope, cancel).await,
            Err(error) if is_aborted(&error) => FlowResult::Cancelled,
            Err(error) => {
                FlowResult::Faulted { error: WorkflowError::runtime(error.to_string()), effects_started: false }
            }
        };
        self.engine.end_loop(&id).await;
        result
    }
}

/// Starts (or claims) the loop's run `id` and drives it here. `Waiting`
/// when another worker holds it.
pub async fn run_loop_durable(
    engine: &Arc<Engine>,
    runs: &crate::durable::RunStore,
    id: &str,
    input: Value,
    scope: &Scope,
    cancel: &CancellationToken,
) -> FlowResult {
    // A legacy worker join may supply no input. Resolve only new admissions;
    // existing runs own their name, definition and override version already.
    let (name, definition, input) = match runs.get(id) {
        Ok(Some(saved)) => (saved.name, saved.definition, saved.input),
        Ok(None) => {
            let name = loop_name(&input).to_owned();
            let (definition, version) = run_definition(engine, &name, is_worker(&input));
            (name, definition, with_override(input, version))
        }
        Err(error) => return FlowResult::Interrupted { error: WorkflowError::runtime(error.to_string()) },
    };
    let claimed = (|| -> anyhow::Result<Option<(crate::durable::SavedRun, crate::durable::Lease)>> {
        let lease = match runs.start_anchored(id, &name, &definition, &input, scope.anchor().as_ref())? {
            Some(lease) => lease,
            None => match runs.claim(id)? {
                Some(lease) => lease,
                // Another worker holds it, or it already ended.
                None => return Ok(None),
            },
        };
        Ok(Some((runs.get(id)?.ok_or_else(|| anyhow::anyhow!("run {id} disappeared"))?, lease)))
    })();
    match claimed {
        Err(error) => FlowResult::Interrupted { error: WorkflowError::runtime(error.to_string()) },
        Ok(Some((saved, lease))) => execute_saved(engine, runs, saved, lease, scope, cancel).await,
        Ok(None) => match runs.get(id) {
            Ok(Some(crate::durable::SavedRun { outcome: Some(status), .. })) => flow_result(status, true),
            Ok(_) => FlowResult::Waiting { wake_at_ms: chrono::Utc::now().timestamp_millis() },
            Err(error) => FlowResult::Interrupted { error: WorkflowError::runtime(error.to_string()) },
        },
    }
}

/// A sub-agent's run `id`, driven to its end: while the recovery worker
/// holds it, or it waits out an interruption's backoff, this waits for it.
pub async fn run_child(
    engine: &Arc<Engine>,
    runs: &crate::durable::RunStore,
    id: &str,
    input: Value,
    scope: &Scope,
    cancel: &CancellationToken,
) -> FlowResult {
    loop {
        match run_loop_durable(engine, runs, id, input.clone(), scope, cancel).await {
            FlowResult::Waiting { .. } | FlowResult::Interrupted { .. } if !cancel.is_cancelled() => {
                tokio::select! {
                    _ = cancel.cancelled() => return FlowResult::Cancelled,
                    _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
                }
            }
            result => return result,
        }
    }
}

impl Functions {
    async fn loop_child(&self, mut args: Value, info: &CallInfo<'_>) -> Result<Value, HostError> {
        let root: Scope = self.spans.root.clone();
        let workflow = args["workflow"].as_str().unwrap_or_default().to_owned();
        if workflow != "guard" {
            args["worker_loop"] = args
                .get("loop")
                .filter(|v| v.is_string())
                .cloned()
                .or_else(|| (workflow != crate::agent::AGENT_LOOP_NAME).then(|| json!(workflow)))
                .unwrap_or(Value::Null);
            // Same worker preparation and id scheme as legacy durable_worker.
            return Box::pin(self.host_call("start_worker", args, info)).await;
        }
        let admitted = self.admitted("start_child", info)?;
        args["loop"] = admitted["loop"].clone();
        let spec: crate::agent::LoopSpec = serde_json::from_value(admitted["loop"].clone()).map_err(failed_json)?;
        let live = self.engine.loop_for(&spec, &root, info.cancel).await.map_err(failed)?;
        live.admit_guard(&admitted).await.map_err(failed)?;
        if let Some(runs) = &self.runs {
            live.restore_guard_history(runs, info.run_id).await.map_err(failed)?;
        }
        // Rounds with only local reads need no child and make no model request.
        let calls = args["calls"]
            .as_array()
            .ok_or_else(|| invalid("start_child", "calls required"))?
            .iter()
            .map(crate::agent::tool_call)
            .collect::<anyhow::Result<Vec<_>>>()
            .map_err(failed)?;
        if !calls.iter().any(|c| crate::guard::needs_guard(&self.engine, c)) {
            return Ok(json!({"run":null}));
        }
        let Some((runs, events)) = self.runs.as_ref().zip(self.events.as_deref()) else {
            crate::agent::loop_guard(&self.engine, &args, &root, info.cancel).await.map_err(failed)?;
            return Ok(json!({"run":null}));
        };
        let round = args["i"].as_u64().ok_or_else(|| invalid("start_child", "guard requires a round"))?;
        let key = format!("guard:{round}");
        let id = format!("{}/guard/{round}", info.run_id);
        // Freeze history before independent calls in this round can run.
        let input = json!({"calls":args["calls"],"round":round,"history":live.guard_history().await});
        let guard_scope = self.spans.root.span(SpanStart::new("guard", SpanKind::Chain).input(input.clone()));
        self.start_child(
            runs,
            events,
            &id,
            "guard",
            built_in("guard").expect("built-in guard"),
            input,
            &key,
            info,
            &guard_scope,
        )
        .await?;
        let guarded: Vec<_> = calls
            .iter()
            .enumerate()
            .filter(|(_, c)| crate::guard::needs_guard(&self.engine, c))
            .map(|(i, _)| i)
            .collect();
        Ok(json!({"run":id,"guarded":guarded}))
    }

    async fn guard_step(&self, name: &str, args: Value, info: &CallInfo<'_>) -> Result<Value, HostError> {
        let root: Scope = self.spans.root.clone();
        let runs = self.runs.as_ref().ok_or_else(|| invalid(name, "requires a durable guard run"))?;
        let saved = runs
            .get(info.run_id)
            .map_err(failed)?
            .filter(|r| r.name == "guard")
            .ok_or_else(|| invalid(name, "only the built-in guard calls it"))?;
        let parent = saved.parent_run.as_deref().ok_or_else(|| invalid(name, "missing parent"))?;
        let admitted = runs
            .get(parent)
            .map_err(failed)?
            .as_ref()
            .and_then(admitted_input)
            .cloned()
            .ok_or_else(|| invalid(name, "guard requires an admitted parent"))?;
        let spec: crate::agent::LoopSpec = serde_json::from_value(admitted["loop"].clone()).map_err(failed_json)?;
        let live = self.engine.loop_for(&spec, &root, info.cancel).await.map_err(failed)?;
        live.admit_guard(&admitted).await.map_err(failed)?;
        live.restore_guard_history(runs, parent).await.map_err(failed)?;
        if name == "review" {
            return crate::agent::guard_review(
                &self.engine,
                &json!({"loop":admitted["loop"],"calls":saved.input["calls"]}),
                &root,
                info.cancel,
                saved.input.get("history").cloned().map(serde_json::from_value).transpose().map_err(failed_json)?,
            )
            .await
            .map_err(failed);
        }
        let call = crate::agent::tool_call(&args["call"]).map_err(failed)?;
        let key = crate::guard::key(&call);
        // Review output is read from the trusted journal, never from YAML.
        let review =
            runs.records(info.run_id)
                .map_err(failed)?
                .into_iter()
                .find_map(|r| match r {
                    owf::durable::Record::Call {
                        function, state: owf::durable::CallState::Done(Ok(output)), ..
                    } if function == "review" => Some(output),
                    _ => None,
                })
                .ok_or_else(|| invalid(name, "missing review evidence"))?;
        let item = review["items"]
            .as_array()
            .and_then(|items| items.iter().find(|i| i["key"] == key))
            .ok_or_else(|| invalid(name, "call was not reviewed"))?;
        let verdict: crate::guard::Verdict = serde_json::from_value(item["verdict"].clone()).map_err(failed_json)?;
        match name {
            "confirm_request" => crate::agent::guard_confirmation(
                &self.engine,
                &admitted,
                &call,
                &verdict,
                &root,
                info,
                runs,
                self.events.as_deref(),
            )
            .await
            .map_err(failed),
            "resolve_review" => {
                let verdict = if verdict.label == "needs_confirmation" && spec.confirm.is_some() {
                    let outcome = match runs.confirmations().outcome(info.run_id, &key).map_err(failed)? {
                        Some(outcome) => outcome,
                        None => runs
                            .confirmations()
                            .resolve(info.run_id, &key, args["outcome"].as_str().unwrap_or("unverified"))
                            .map_err(failed)?,
                    };
                    // A dry run's card was never sent: there is nothing to edit.
                    if self.engine.sandbox().is_none() {
                        crate::confirmation::close_card(
                            runs.confirmations(),
                            &*self.engine.deps.mcp,
                            info.run_id,
                            &call,
                            &outcome,
                            &crate::confirmation::Card::load(&*self.engine.deps.memory),
                            info.cancel,
                        )
                        .await;
                    }
                    crate::agent::confirmation_verdict(&call, &outcome)
                } else {
                    verdict
                };
                Ok(json!([key, verdict]))
            }
            _ => Err(invalid(name, "only guard review, confirmation and resolution are available").into()),
        }
    }
}

fn failed_json(error: serde_json::Error) -> WorkflowError {
    WorkflowError::validation(error.to_string())
}

impl Functions {
    #[allow(clippy::too_many_arguments)]
    async fn start_child(
        &self,
        runs: &crate::durable::RunStore,
        events: &dyn owf::run::Events,
        id: &str,
        workflow: &str,
        definition: &str,
        input: Value,
        key: &str,
        info: &CallInfo<'_>,
        scope: &Arc<dyn Span>,
    ) -> Result<(), HostError> {
        let permit = self.children.slots.clone().acquire_owned().await.map_err(|e| invalid("start_child", e))?;
        events.register(info.run_id, &[format!("done:{id}"), format!("waiting:{id}")]).map_err(failed)?;
        let lease = runs
            .start_child_anchored(
                id,
                workflow,
                definition,
                &input,
                crate::durable::ChildLink { parent: info.run_id, key, reference: info.reference },
                scope.anchor().as_ref(),
            )
            .map_err(failed)?;
        let lease = match lease {
            Some(l) => Some(l),
            None => runs.claim(id).map_err(failed)?,
        };
        if let Some(lease) = lease {
            let saved = runs.get(id).map_err(failed)?.ok_or_else(|| invalid("start_child", "run disappeared"))?;
            let (engine, runs, scope, cancel) =
                (self.engine.clone(), runs.clone(), scope.clone(), self.children.cancel.child_token());
            self.children.tasks.lock().await.spawn(async move {
                let _permit = permit;
                let saved_guard = saved.name == "guard";
                let loop_id = admitted_input(&saved).and_then(|i| i["loop"]["id"].as_str().map(str::to_owned));
                let execution_scope: Scope = scope.clone();
                let result = execute_saved(&engine, &runs, saved, lease, &execution_scope, &cancel).await;
                if loop_id.is_some() || saved_guard {
                    scope.end(match result {
                        FlowResult::Completed(output) => SpanEnd::output(output),
                        FlowResult::Faulted { error, .. } | FlowResult::Interrupted { error } => {
                            SpanEnd::error(error.to_string())
                        }
                        _ => SpanEnd::default(),
                    });
                }
                if let Some(id) = loop_id {
                    engine.end_loop(&id).await;
                }
            });
        } else {
            scope.end(SpanEnd::default());
        }
        Ok(())
    }
}

// ── 4b. runtime definitions, loop variants and signal routes ────────────────
// Active versions replace git definitions by name. Each name has its own
// breaker; runs keep the definition and override version admitted with them.
//
// A route (`metadata.route`) is declared by the workflow it routes to. A loop
// declaring one is started as the signal's primary loop and gathers its own
// context (supervisor.rs `loop_route`); that is how a channel's flow is one
// workflow. A route on a non-loop workflow (a signal route, which calls
// `primary_loop` itself) is superseded by loop routes and kept for the routes
// already active; both are matched by the same rules.

const LOOP_BREAKER: u32 = 3;
// Where a run's input names the saved version it runs (null: the built-in).
const OVERRIDE: &str = "override_version";

/// The host an `agent-loop` run executes on, traced under `root`: for a
/// caller that owns the run's journal itself (a test injecting crashes).
pub fn loop_host(engine: &Arc<Engine>, root: &Arc<dyn Span>, input: Value) -> Host {
    host(engine, root, true, false, None, Some(input), None, Arc::default())
}

/// Existing standalone function adapters for an isolated host such as the
/// optimizer workbench. This grants no admitted signal/loop context. The
/// caller must restrict capabilities before invoking these adapters.
pub fn standalone_host(engine: &Arc<Engine>, root: &Arc<dyn Span>) -> Host {
    host(engine, root, false, false, None, None, None, Arc::default())
}

// `silent`: a loop's generations and tool calls trace themselves; spans for
// its turn/round/settle tasks would only repeat them. Any other workflow's
// tasks are spans, its agent turns among them.
#[allow(clippy::too_many_arguments)]
fn host(
    engine: &Arc<Engine>,
    root: &Arc<dyn Span>,
    silent: bool,
    guard_only: bool,
    runs: Option<crate::durable::RunStore>,
    loop_input: Option<Value>,
    events: Option<Arc<dyn owf::run::Events>>,
    children: Arc<Children>,
) -> Host {
    let spans = Arc::new(Spans { root: root.clone(), open: Mutex::default() });
    let observer: Arc<dyn owf::run::Observer> = if silent { Arc::new(owf::run::Silent) } else { spans.clone() };
    Host {
        functions: Arc::new(Functions {
            engine: engine.clone(),
            spans,
            runs,
            guard_only,
            loop_input,
            events,
            children,
        }),
        observer,
        clock: Arc::new(SystemClock),
        limits: Limits::default(),
        runtime: json!({"name":"agent-supervisor","version":env!("CARGO_PKG_VERSION")}),
    }
}

// The breaker's state: the saved version it counts, its faults in a row,
// and whether they reached LOOP_BREAKER.
#[derive(Default, serde::Serialize, serde::Deserialize)]
struct Breaker {
    version: i64,
    faults: u32,
    open: bool,
}

/// The definition in git of a workflow the runtime itself runs.
pub fn built_in(name: &str) -> Option<&'static str> {
    match name {
        crate::agent::AGENT_LOOP_NAME => Some(crate::agent::AGENT_LOOP),
        crate::supervisor::SIGNAL_NAME => Some(crate::supervisor::SIGNAL),
        "guard" => Some(include_str!("../workflows/guard.yaml")),
        _ => None,
    }
}

// `agent_loop.breaker`, `signal.breaker` in the memory KV.
fn breaker_key(name: &str) -> String {
    format!("{}.breaker", name.replace('-', "_"))
}

fn breaker(engine: &Engine, name: &str) -> Breaker {
    let stored = engine.deps.memory.get(&breaker_key(name)).ok().flatten();
    stored.and_then(|b| serde_json::from_str(&b).ok()).unwrap_or_default()
}

/// The definition a new run of the built-in workflow `name` runs, with the
/// saved version it comes from (None: the built-in). A saved version that no
/// longer validates, or whose breaker is open, gives way to the built-in one.
pub fn definition(engine: &Engine, name: &str) -> (String, Option<i64>) {
    let builtin = (built_in(name).unwrap_or_default().to_owned(), None);
    if name == "guard" {
        return builtin;
    }
    let Ok(Some(saved)) = engine.deps.workflows.active(name) else { return builtin };
    let state = breaker(engine, name);
    if (state.open && state.version == saved.version) || check(&*engine.deps.mcp, &saved.definition).is_err() {
        return builtin;
    }
    (saved.definition, Some(saved.version))
}

// A workflow the runtime runs on a signal of its own choosing: the
// dispatcher, the loop, a loop variant or a route (a scheduled task's
// workflow runs on its own fires).
fn is_runtime_workflow(name: &str, wf: &owf::model::Workflow) -> bool {
    built_in(name).is_some()
        || workflow_metadata(wf).get("loop").is_some()
        || workflow_metadata(wf).get("route").is_some()
}

/// Metadata lives in document.metadata, the OWF extension point.
fn workflow_metadata(wf: &owf::model::Workflow) -> &Value {
    &wf.definition["document"]["metadata"]
}

pub fn is_loop_definition(name: &str, definition: &str) -> bool {
    name == crate::agent::AGENT_LOOP_NAME
        || owf::model::parse_yaml(definition).is_ok_and(|wf| workflow_metadata(&wf).get("loop").is_some())
}

pub fn loop_name(input: &Value) -> &str {
    input["loop"]["workflow"].as_str().unwrap_or(crate::agent::AGENT_LOOP_NAME)
}

pub fn loop_definition(engine: &Engine) -> (String, Option<i64>) {
    loop_definition_named(engine, crate::agent::AGENT_LOOP_NAME)
}

pub fn loop_definition_named(engine: &Engine, name: &str) -> (String, Option<i64>) {
    let (resolved, version) = definition(engine, name);
    if !resolved.is_empty() && is_loop_definition(name, &resolved) {
        (resolved, version)
    } else {
        (crate::agent::AGENT_LOOP.to_owned(), None)
    }
}

/// The definition a new admitted run of `name` runs. A worker runs a loop
/// (`loop_definition_named`). A run the runtime starts on a signal runs any
/// workflow: a loop, a route, a scheduled task's (one copied from
/// `agent-loop` included); while its version faults (the breaker) or no
/// longer validates, its built-in one, else `agent-loop`. The dispatcher
/// and the guard are never a signal's run.
pub fn run_definition(engine: &Engine, name: &str, worker: bool) -> (String, Option<i64>) {
    if worker {
        return loop_definition_named(engine, name);
    }
    let (resolved, version) = definition(engine, name);
    if resolved.is_empty() || name == crate::supervisor::SIGNAL_NAME || name == "guard" {
        return (crate::agent::AGENT_LOOP.to_owned(), None);
    }
    (resolved, version)
}

// A worker's admitted input names its parent loop.
fn is_worker(input: &Value) -> bool {
    input["loop"]["parent_id"].is_string()
}

/// Why the saved workflow `name` cannot start on a scheduled signal now, as
/// the outcome its run would have had, which changed nothing: no active
/// version (`Missing`), one that no longer validates, or one whose breaker
/// is open (three faults in a row; the task goes to the agent loop, as
/// `run_definition` would put `agent-loop` in its place). None: it runs.
pub fn unrunnable(engine: &Engine, name: &str) -> Option<FlowResult> {
    let unchanged = |error| Some(FlowResult::Faulted { error, effects_started: false });
    let saved = match engine.deps.workflows.active(name) {
        Ok(Some(saved)) => saved,
        Ok(None) => return Some(FlowResult::Missing),
        Err(e) => return unchanged(WorkflowError::runtime(format!("loading workflow `{name}`: {e}"))),
    };
    // A definition that no longer validates (a tool went away) changed nothing.
    if let Err(problems) = check(&*engine.deps.mcp, &saved.definition) {
        return unchanged(WorkflowError::validation(problems));
    }
    let state = breaker(engine, name);
    if state.open && state.version == saved.version {
        return unchanged(WorkflowError::runtime(format!(
            "version {} faulted {} times in a row, so it is set aside until a new version is activated",
            saved.version, state.faults
        )));
    }
    None
}

#[derive(Clone, Debug)]
struct Route {
    when: std::collections::BTreeMap<String, Vec<String>>,
    priority: i64,
}

fn route(wf: &owf::model::Workflow) -> anyhow::Result<Option<Route>> {
    let Some(route) = workflow_metadata(wf).get("route") else { return Ok(None) };
    let when = route
        .get("when")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow::anyhow!("metadata.route.when must be an object"))?;
    anyhow::ensure!(!when.is_empty(), "metadata.route.when must have at least one key");
    let mut fields = std::collections::BTreeMap::new();
    for (key, value) in when {
        let values = match value {
            Value::String(value) => vec![value.clone()],
            Value::Array(values) if !values.is_empty() && values.iter().all(Value::is_string) => {
                values.iter().filter_map(Value::as_str).map(str::to_owned).collect()
            }
            _ => anyhow::bail!("metadata.route.when.{key} must be a string or a nonempty list of strings"),
        };
        fields.insert(key.clone(), values);
    }
    let priority = match route.get("priority") {
        None => 0,
        Some(priority) => {
            priority.as_i64().ok_or_else(|| anyhow::anyhow!("metadata.route.priority must be an integer"))?
        }
    };
    Ok(Some(Route { when: fields, priority }))
}

/// What the confirmation target of a signal's run is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Confirm {
    // The signal's conversation, else the default chat (the dispatcher's
    // default path does the same).
    Conversation,
    None,
}

/// The model's parameters a workflow declares for the runs the runtime
/// starts on a signal, pinned at admission: `metadata.agent.preset` (base,
/// smart or smartest) and `metadata.agent.confirm` (`conversation` or
/// `none`). A route may say the same under `metadata.route`, as loop routes
/// always could. Unset, the launch decides (supervisor.rs `Launch`).
#[derive(Clone, Copy, Debug, Default)]
pub struct AgentStart {
    pub preset: Option<PresetName>,
    pub confirm: Option<Confirm>,
}

// Where the parameters are declared, if anywhere: `metadata.agent`, else a
// route's own.
fn start_block(wf: &owf::model::Workflow) -> Option<(&'static str, &Value)> {
    let metadata = workflow_metadata(wf);
    if let Some(agent) = metadata.get("agent") {
        return Some(("metadata.agent", agent));
    }
    let route = metadata.get("route")?;
    (route.get("preset").is_some() || route.get("confirm").is_some()).then_some(("metadata.route", route))
}

pub fn agent_start(wf: &owf::model::Workflow) -> anyhow::Result<AgentStart> {
    let Some((at, block)) = start_block(wf) else { return Ok(AgentStart::default()) };
    anyhow::ensure!(block.is_object(), "{at} must be an object of preset and confirm");
    if at == "metadata.agent"
        && let Some(other) =
            block.as_object().into_iter().flatten().map(|(k, _)| k).find(|k| *k != "preset" && *k != "confirm")
    {
        anyhow::bail!("{at} takes preset and confirm, not `{other}`");
    }
    let preset = match block.get("preset") {
        None | Some(Value::Null) => None,
        Some(name) => Some(
            name.as_str()
                .and_then(PresetName::selectable)
                .ok_or_else(|| anyhow::anyhow!("{at}.preset must be base, smart or smartest"))?,
        ),
    };
    let confirm = match block.get("confirm") {
        None | Some(Value::Null) => None,
        Some(Value::String(c)) if c == "conversation" => Some(Confirm::Conversation),
        Some(Value::String(c)) if c == "none" => Some(Confirm::None),
        _ => anyhow::bail!("{at}.confirm must be `conversation` or `none`"),
    };
    Ok(AgentStart { preset, confirm })
}

fn require_metadata(wf: &owf::model::Workflow) -> anyhow::Result<()> {
    let metadata = workflow_metadata(wf);
    if let Some(loop_) = metadata.get("loop") {
        anyhow::ensure!(
            loop_["description"].as_str().is_some_and(|s| !s.trim().is_empty()),
            "metadata.loop.description must be a nonempty string"
        );
    }
    let route_starts = metadata.get("route").is_some_and(|r| r.get("preset").is_some() || r.get("confirm").is_some());
    anyhow::ensure!(
        !(route_starts && metadata.get("agent").is_some()),
        "declare preset and confirm once: metadata.agent, or the route's own"
    );
    // A signal route runs on the dispatcher's host and passes them to
    // primary_loop itself; nothing else would read them.
    anyhow::ensure!(
        !(calls_dispatcher(wf) && (route_starts || metadata.get("agent").is_some())),
        "a workflow that calls the dispatcher's functions (primary_loop) passes preset and confirm to primary_loop; metadata.agent and metadata.route.preset/confirm are for the runs the runtime admits itself"
    );
    agent_start(wf)?;
    route(wf)?;
    anyhow::ensure!(
        wf.document.name != "signal" || metadata.get("route").is_none(),
        "the signal dispatcher cannot route to itself"
    );
    Ok(())
}

fn require_route_priority(store: &dyn WorkflowStore, name: &str, wf: &owf::model::Workflow) -> anyhow::Result<()> {
    let Some(candidate) = route(wf)? else { return Ok(()) };
    for (other, definition) in runtime_definitions(store)? {
        if other == name {
            continue;
        }
        let parsed = owf::model::parse_yaml(&definition)?;
        let Some(existing) = route(&parsed)? else { continue };
        let overlaps = candidate.when.iter().all(|(key, values)| {
            existing.when.get(key).is_none_or(|others| values.iter().any(|value| others.contains(value)))
        });
        anyhow::ensure!(
            candidate.when.len() != existing.when.len() || !overlaps || candidate.priority != existing.priority,
            "route `{name}` conflicts with `{other}` at equal specificity; set distinct metadata.route.priority values to choose a winner"
        );
    }
    Ok(())
}

/// Deterministic routing on source and top-level payload fields. Activation
/// rejects overlapping ties, so a live signal needs no model or tie decision.
pub fn signal_route(
    engine: &Engine,
    source: &str,
    payload: &Value,
) -> anyhow::Result<Option<(String, String, Option<i64>)>> {
    let mut matches = Vec::new();
    for (name, _) in runtime_definitions(&*engine.deps.workflows)? {
        let (definition, version) = definition(engine, &name);
        if definition.is_empty() {
            continue;
        }
        let Ok(wf) = owf::model::parse_yaml(&definition) else { continue };
        let Some(route) = route(&wf)? else { continue };
        if route.when.iter().all(|(key, values)| {
            let field = if key == "source" { Some(source) } else { payload.get(key).and_then(Value::as_str) };
            field.is_some_and(|field| values.iter().any(|value| value == field))
        }) {
            matches.push((route.when.len(), route.priority, name, definition, version));
        }
    }
    matches.sort_by(|a, b| (a.0, a.1, &a.2).cmp(&(b.0, b.1, &b.2)));
    Ok(matches.pop().map(|(_, _, name, definition, version)| (name, definition, version)))
}

/// Active versions replace built-ins even if a newer draft exists.
fn runtime_definitions(store: &dyn WorkflowStore) -> anyhow::Result<Vec<(String, String)>> {
    let mut definitions: std::collections::BTreeMap<String, String> = ["agent-loop", "signal"]
        .into_iter()
        .map(|name| (name.to_owned(), built_in(name).expect("built-in").to_owned()))
        .collect();
    for saved in store.list()? {
        if let Some(active) = store.active(&saved.name)? {
            definitions.insert(active.name, active.definition);
        }
    }
    Ok(definitions.into_iter().collect())
}

pub fn loop_catalog(store: &dyn WorkflowStore) -> anyhow::Result<String> {
    let mut entries = Vec::new();
    for (name, definition) in runtime_definitions(store)? {
        let Ok(wf) = owf::model::parse_yaml(&definition) else { continue };
        if let Some(description) = workflow_metadata(&wf)["loop"]["description"].as_str() {
            entries.push(format!("{name}: {description}"));
        }
    }
    Ok(entries.join("\n"))
}

fn with_override(mut input: Value, version: Option<i64>) -> Value {
    if let Value::Object(fields) = &mut input {
        fields.insert(OVERRIDE.into(), json!(version));
    }
    input
}

/// Counts a run of the saved version `version` of the built-in workflow
/// `name`: a fault adds one (LOOP_BREAKER in a row open the breaker), a
/// completed run starts the count again. Cancellation and an interruption
/// say nothing about the definition.
pub fn record_outcome(engine: &Engine, name: &str, version: Option<i64>, result: &FlowResult) {
    let Some(version) = version else { return };
    let mut state = breaker(engine, name);
    if state.version != version {
        state = Breaker { version, ..Breaker::default() };
    }
    match result {
        // Nothing counted, nothing to write: a run that completes leaves no
        // record of its own.
        FlowResult::Completed(_) if state.faults == 0 && !state.open => return,
        FlowResult::Completed(_) => state.faults = 0,
        FlowResult::Faulted { error, .. } => {
            state.faults += 1;
            if state.faults >= LOOP_BREAKER && !state.open {
                state.open = true;
                eprintln!(
                    "[{name}] version {version} faulted {} times in a row; back to the built-in one: {error}",
                    state.faults
                );
            }
        }
        _ => return,
    }
    let saved = serde_json::to_string(&state).map_err(anyhow::Error::from);
    if let Err(error) = saved.and_then(|s| engine.deps.memory.set(&breaker_key(name), &s)) {
        eprintln!("[{name}] breaker: {error:#}");
    }
}

/// Runs an admitted run in this process, without checkpoints: a worker's,
/// a recovery's, or any run of a runner without a store. `input` is
/// `AgentLoop::workflow_input`'s.
pub async fn run_loop_here(
    engine: &Arc<Engine>,
    id: &str,
    input: Value,
    scope: &Scope,
    cancel: &CancellationToken,
) -> FlowResult {
    let name = loop_name(&input).to_owned();
    let (definition, version) = run_definition(engine, &name, is_worker(&input));
    let root = scope.span(
        SpanStart::new("runner", SpanKind::Chain)
            .input(input.clone())
            .metadata(metadata(json!({"workflow":name,"workflow_version":version,"engine":"owf"}))),
    );
    let result = match owf::model::parse_yaml(&definition) {
        Err(error) => {
            FlowResult::Faulted { error: WorkflowError::validation(error.to_string()), effects_started: false }
        }
        Ok(wf) => {
            let silent = is_loop_definition(&name, &definition);
            let mut host = host(engine, &root, silent, false, None, Some(input.clone()), None, Arc::default());
            if let Some(sandbox) = engine.sandbox() {
                sandbox.watch(&mut host, id, &name, version, None, input["loop"]["id"].as_str());
            }
            let outcome = owf::run::run(&host, &wf, id, with_override(input, version), cancel).await;
            flow_result(outcome.status, outcome.effects_started)
        }
    };
    record_outcome(engine, &name, version, &result);
    if let FlowResult::Completed(output) = &result {
        scope.update(crate::tracing::ContextUpdate::output(output.clone()));
    }
    root.end(match &result {
        FlowResult::Faulted { error, .. } => SpanEnd::error(error.to_string()),
        _ => SpanEnd::default(),
    });
    result
}

// ── 5. authoring tools ─────────────────────────────────────────────────────

// Synthetic tools of the agent loop for writing saved workflows. Saving
// validates and stores a draft; only activation (after the user agreed in
// the chat) lets a schedule run it.
//
// The model's calls go through `tool` and the guard: a workflow that takes
// agent turns starts the guard child on each round's calls, whatever its
// kind. The check is structural (it does not prove the round runs the
// calls under the verdicts); `tool` itself still enforces a verdict for a
// call that needs one, so an older stored definition stays guarded.
fn require_guarded_turns(wf: &owf::model::Workflow) -> anyhow::Result<()> {
    let sites = owf::model::call_sites(wf);
    anyhow::ensure!(
        !sites.iter().any(|s| s.function == "agent_turn")
            || sites
                .iter()
                .any(|s| s.function == "start_child" && s.with.as_ref().is_some_and(|a| a["workflow"] == "guard")),
        "a workflow that calls agent_turn requires a `call: start_child` of workflow guard before its tool round: copy agent-loop's guarded round (read_workflow agent-loop); the host still enforces older overrides"
    );
    Ok(())
}

pub const TOOLS: &[&str] = &["save_workflow", "activate_workflow", "list_workflows", "read_workflow"];

// The workflow-author worker's own tools (`workflow-author` skill, which
// takes no MCP tools of its own): the functions a workflow can call, one
// line each or one in full, a run of one read function, and a dry run of a
// saved draft, so that a definition is built from real replies and checked
// on real data without the worker being able to change anything.
pub const AUTHOR_TOOLS: &[&str] = &["workflow_functions", "probe_function", "try_workflow"];

// What a probe returns at most: enough to see a reply's shape, never a
// whole store.
const PROBE_MAX_CHARS: usize = 6000;

// A dry run's report, small enough to come back whole: each step's output
// cut to this, at most this many steps and path entries, a write's argument
// cut to that; the whole run stopped after this long.
const DRY_STEP_MAX_CHARS: usize = 400;

const DRY_MAX_STEPS: usize = 40;

const DRY_MAX_PATH: usize = 120;

const DRY_WRITE_MAX_CHARS: usize = 5000;

pub(crate) const DRY_RUN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

fn effect_name(mcp: &dyn Mcp, wf: &owf::model::Workflow, function: &str) -> &'static str {
    match effect(mcp, function) {
        Some(Effect::Read) => "read",
        Some(Effect::Write) => "write",
        None if wf.uses.functions.iter().any(|(n, _)| n == function) => "function",
        None => "unknown",
    }
}

// Flows that are legal but usually a mistake, for the author: a task a jump
// leads to that also runs right after the task before it, because that one
// does not jump (a "nothing new" text after the real one overwrites it).
// A shared tail does this on purpose, so it is a warning, not a problem.
// And a `choose` followed by its `switch` whose cases miss one of its
// options (with no default case) or name one it doesn't have; only literal
// options and `"label"` strings in `when` are compared, jq is not parsed.
pub fn warnings(wf: &owf::model::Workflow) -> Vec<String> {
    use owf::model::{Flow, Kind, Task, TaskList};
    fn jumps_to(task: &Task, target: &str) -> bool {
        let goto = |flow: &Flow| matches!(flow, Flow::Goto(t) if t == target);
        task.then.as_ref().is_some_and(goto)
            || match &task.kind {
                Kind::Switch(cases) => cases.iter().any(|(_, case)| goto(&case.then)),
                Kind::Try { catch, .. } => catch.then.as_ref().is_some_and(goto),
                _ => false,
            }
    }
    fn falls_through(task: &Task) -> bool {
        match &task.kind {
            Kind::Raise(_) => false,
            Kind::Switch(cases) => {
                cases.iter().any(|(_, case)| case.then == Flow::Continue)
                    || !cases.iter().any(|(_, c)| c.when.is_none())
            }
            _ => matches!(task.then, None | Some(Flow::Continue)),
        }
    }
    fn choose_cases(call: &Task, switch: &Task, at: &str, out: &mut Vec<String>) {
        let (Kind::Call { function, with: Some(with) }, Kind::Switch(cases)) = (&call.kind, &switch.kind) else {
            return;
        };
        let Some(options) = with.get("options").and_then(Value::as_object).filter(|_| function == "choose") else {
            return;
        };
        let literal = regex::Regex::new(r#""([a-z][a-z0-9_]*)""#).expect("valid literal regex");
        let named: Vec<&str> = cases
            .iter()
            .filter_map(|(_, case)| case.when.as_deref())
            .filter(|when| when.contains("choice"))
            .flat_map(|when| literal.captures_iter(when).map(|c| c.get(1).map_or("", |m| m.as_str())))
            .collect();
        if !cases.iter().any(|(_, case)| case.when.is_none()) {
            for label in options.keys().filter(|l| !named.contains(&l.as_str())) {
                out.push(format!(
                    "{at}: choose option `{label}` has no case in the switch after it, and that switch has no default case"
                ));
            }
        }
        for label in named.iter().filter(|l| !options.contains_key(**l)) {
            out.push(format!("{at}: the switch after it compares with `{label}`, which is not one of its options"));
        }
    }
    fn list(tasks: &TaskList, at: &str, out: &mut Vec<String>) {
        for (i, (name, task)) in tasks.iter().enumerate() {
            if let Some((_, next)) = tasks.get(i + 1) {
                choose_cases(task, next, &format!("{at}/{i}/{name}"), out);
            }
            if let Some((previous, before)) = i.checked_sub(1).map(|j| (&tasks[j].0, &tasks[j].1)) {
                let from: Vec<&str> =
                    tasks.iter().filter(|(n, t)| n != previous && jumps_to(t, name)).map(|(n, _)| n.as_str()).collect();
                if !from.is_empty() && falls_through(before) {
                    out.push(format!(
                        "{at}/{i}/{name}: a jump from `{}` leads here, and it also runs right after `{previous}`, which does not jump; if it belongs to another branch, end `{previous}` with `then:` the task that should follow it",
                        from.join("`, `")
                    ));
                }
            }
            inner(task, &format!("{at}/{i}/{name}"), out);
        }
    }
    // The lists inside a task; a fork's branches run side by side, so only
    // what each one holds.
    fn inner(task: &Task, at: &str, out: &mut Vec<String>) {
        match &task.kind {
            Kind::Do(tasks) | Kind::For { tasks, .. } => list(tasks, &format!("{at}/do"), out),
            Kind::Try { tasks, catch } => {
                list(tasks, &format!("{at}/try"), out);
                if let Some(handler) = &catch.tasks {
                    list(handler, &format!("{at}/catch/do"), out);
                }
            }
            Kind::Fork { branches, .. } => {
                for (j, (branch, task)) in branches.iter().enumerate() {
                    inner(task, &format!("{at}/fork/branches/{j}/{branch}"), out);
                }
            }
            _ => {}
        }
    }
    let mut out = Vec::new();
    list(&wf.tasks, "/do", &mut out);
    out
}

pub fn tool(engine: &Engine, name: &str, args: &Map<String, Value>) -> anyhow::Result<String> {
    let store = &*engine.deps.workflows;
    let mcp = &*engine.deps.mcp;
    let text = |key: &str| {
        args.get(key)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow::anyhow!("{key}: Expected nonempty string"))
    };
    let version = || match args.get("version") {
        None | Some(Value::Null) => Ok(None),
        Some(v) => {
            v.as_i64().filter(|v| *v > 0).map(Some).ok_or_else(|| anyhow::anyhow!("version: Expected positive integer"))
        }
    };
    match name {
        "save_workflow" => {
            let (name, definition) = (text("name")?, text("definition")?);
            anyhow::ensure!(name != "guard", "guard is a reserved built-in workflow and cannot be saved or activated");
            let wf = check(mcp, definition).map_err(anyhow::Error::msg)?;
            anyhow::ensure!(
                wf.document.name == name,
                "document.name is `{}` but the workflow is saved as `{name}`; make them the same",
                wf.document.name
            );
            require_guarded_turns(&wf)?;
            require_metadata(&wf)?;
            let saved = store.save(name, definition, args.get("note").and_then(Value::as_str))?;
            let calls: Vec<Value> = owf::model::call_sites(&wf)
                .into_iter()
                .map(|c| json!({"reference":c.reference,"function":c.function,"effect":effect_name(mcp, &wf, &c.function)}))
                .collect();
            let then = "before the user sees it, then describe in words what it will do, the write calls above all; activate this version only after they confirm it in the chat.";
            let next = if is_runtime_workflow(name, &wf) {
                format!(
                    "Dry-run it on a signal of the type it handles (try_workflow with `signal`, and `drafts` for the other drafts of the chain) {then}"
                )
            } else if takes_turns(&wf) {
                format!(
                    "Dry-run it on a fire of its task (try_workflow with signal {{source: scheduler, content: the task's prompt, payload: {{workflow: \"{name}\"}}}}) {then}"
                )
            } else {
                format!("Dry-run it (try_workflow) {then}")
            };
            let mut reply = json!({
                "ok": true, "name": name, "version": saved.version, "status": "draft", "calls": calls, "next": next,
            });
            let warnings = warnings(&wf);
            if !warnings.is_empty() {
                reply["warnings"] = json!(warnings);
            }
            Ok(reply.to_string())
        }
        "activate_workflow" => {
            let name = text("name")?;
            anyhow::ensure!(name != "guard", "guard is a reserved built-in workflow and cannot be saved or activated");
            let version = version()?.ok_or_else(|| anyhow::anyhow!("version: Required"))?;
            let saved = store
                .get(name, Some(version))?
                .ok_or_else(|| anyhow::anyhow!("no version {version} of workflow `{name}`"))?;
            // Tools may have changed since it was saved.
            let wf = check(mcp, &saved.definition).map_err(anyhow::Error::msg)?;
            require_guarded_turns(&wf)?;
            require_metadata(&wf)?;
            require_route_priority(store, name, &wf)?;
            if is_runtime_workflow(name, &wf) {
                // Tried on a signal like any workflow; the breaker stays the
                // second line, taking it back after a few faults in a row.
                anyhow::ensure!(
                    saved.tried,
                    "version {version} of `{name}` has not completed a dry run; the workflow-author worker tries it on a signal (try_workflow with `signal` and `drafts`) before it is shown to the user"
                );
                store.activate(name, version)?;
                let runs = match (is_loop_definition(name, &saved.definition), route(&wf)?.is_some()) {
                    (true, true) => "A matching signal, a loop selecting this name or a task scheduled with it,",
                    (true, false) => "A loop selecting this name, or a task scheduled with it,",
                    _ => "A matching signal",
                };
                return Ok(json!({
                    "ok": true, "name": name, "version": version,
                    "next": format!("{runs} started from now runs this version, the one the dry run tried; three faults in a row put the built-in one back."),
                })
                .to_string());
            }
            anyhow::ensure!(
                saved.tried,
                "version {version} of `{name}` has not completed a dry run; the workflow-author worker tries it (try_workflow) before it is shown to the user"
            );
            store.activate(name, version)?;
            Ok(json!({
                "ok": true, "name": name, "version": version,
                "next": format!("It runs whenever a task scheduled with schedule_task(workflow: \"{name}\") fires; create one if none exists. Existing schedules run the new version; three faults in a row hand its task to the agent loop until a new version is activated."),
            })
            .to_string())
        }
        "list_workflows" => {
            let workflows = store
                .list()?
                .into_iter()
                .map(|w| {
                    let active = store.active(&w.name)?.map(|a| a.version);
                    Ok(json!({"name":w.name,"latest_version":w.version,"latest_status":w.status,"active_version":active,"note":w.note,"created_at":w.created_at}))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            Ok(json!({"count": workflows.len(), "workflows": workflows}).to_string())
        }
        "read_workflow" => {
            let name = text("name")?;
            if name == "guard" {
                return Ok(
                    json!({"name":name,"found":true,"version":null,"status":"built-in","definition":built_in(name)})
                        .to_string(),
                );
            }
            Ok(match store.get(name, version()?)? {
                // What the runtime runs until the agent saves its own version.
                None if built_in(name).is_some() => {
                    json!({"name":name,"found":true,"version":null,"status":"built-in","definition":built_in(name)})
                }
                None => json!({"name": name, "found": false}),
                Some(w) => json!({"name":w.name,"found":true,"version":w.version,"status":w.status,"note":w.note,"definition":w.definition}),
            }
            .to_string())
        }
        _ => anyhow::bail!("unknown workflow tool {name}"),
    }
}

// The type of one JSON-schema property, short: `string`, `integer`, an
// enum as its values joined by `|`, an array as `[item]`.
fn short_type(schema: &Value, property: &Value) -> String {
    let resolve = |p: &Value| -> Value {
        match p.get("$ref").and_then(Value::as_str).and_then(|r| r.strip_prefix("#/$defs/")) {
            Some(name) => schema["$defs"][name].clone(),
            None => p.clone(),
        }
    };
    let p = resolve(property);
    let variants: Vec<Value> = match p.get("anyOf").or_else(|| p.get("oneOf")).and_then(Value::as_array) {
        Some(v) => v.iter().map(resolve).collect(),
        None => vec![p],
    };
    let mut names = Vec::new();
    for v in &variants {
        if let Some(values) = v.get("enum").and_then(Value::as_array) {
            names.push(values.iter().filter_map(Value::as_str).collect::<Vec<_>>().join("|"));
            continue;
        }
        let types: Vec<&str> = match &v["type"] {
            Value::String(t) => vec![t.as_str()],
            Value::Array(t) => t.iter().filter_map(Value::as_str).collect(),
            _ => vec![],
        };
        for t in types.into_iter().filter(|t| *t != "null") {
            names.push(if t == "array" { format!("[{}]", short_type(schema, &v["items"])) } else { t.to_owned() });
        }
    }
    if names.is_empty() { "any".into() } else { names.join("|") }
}

fn first_sentence(text: &str) -> String {
    let line = text.split(['\n']).next().unwrap_or_default();
    let end = line.find(". ").map_or(line.len(), |i| i + 1);
    line[..end].trim().to_owned()
}

/// `workflow_functions`: every function a workflow can call, one line each,
/// or with `name` that one in full (description and argument schema).
pub fn functions(engine: &Engine, args: &Map<String, Value>) -> anyhow::Result<String> {
    let mcp = &*engine.deps.mcp;
    let tools: Vec<ToolDef> = model_tools(mcp).into_iter().filter(|t| !WITHHELD.contains(&t.name.as_str())).collect();
    let effect_of = |name: &str| match effect(mcp, name) {
        Some(Effect::Read) => "read",
        _ => "write",
    };
    if let Some(name) = args.get("name").and_then(Value::as_str).filter(|n| !n.is_empty()) {
        if let Some((function, context)) = runtime_functions().find(|(f, _)| f.name == name) {
            let properties: Map<String, Value> = function.takes.iter().map(|n| ((*n).to_owned(), json!({}))).collect();
            let mut reply = json!({
                "name": name, "effect": effect_of(name), "takes": function.takes, "needs": function.needs,
                "description": function.description, "context": context,
                "parameters": {"type": "object", "properties": properties, "required": function.needs,
                    "additionalProperties": false},
            });
            if context != "workflow" {
                reply["probe"] = json!(
                    "Requires an admitted runtime context; test it through try_workflow with signal and drafts instead of probe_function."
                );
            }
            return Ok(reply.to_string());
        }
        let tool = tools.iter().find(|t| t.name == name).ok_or_else(|| anyhow::anyhow!("no function `{name}`"))?;
        return Ok(json!({"name": name, "effect": effect_of(name), "description": tool.description,
            "parameters": tool.parameters})
        .to_string());
    }
    let mut lines = vec![
        "One line per function: name(arguments; `?` optional) effect — summary. \
        workflow_functions(name) gives one in full. Signal and agent-loop host functions \
        require their admitted runtime context; test them with try_workflow(signal: ...)."
            .to_owned(),
    ];
    for (function, context) in runtime_functions() {
        let HostFunction { name, takes, needs, description, .. } = function;
        let args: Vec<String> =
            takes.iter().map(|a| if needs.contains(a) { a.to_string() } else { format!("{a}?") }).collect();
        let summary = first_sentence(description);
        lines.push(format!("{name}({}) {} — [{context}] {summary}", args.join(", "), effect_of(name)));
    }
    for tool in &tools {
        let schema = &tool.parameters;
        let required: Vec<&str> =
            schema["required"].as_array().map(|r| r.iter().filter_map(Value::as_str).collect()).unwrap_or_default();
        let args: Vec<String> = schema["properties"]
            .as_object()
            .map(|p| {
                p.iter()
                    .map(|(n, prop)| {
                        let optional = if required.contains(&n.as_str()) { "" } else { "?" };
                        format!("{n}{optional}: {}", short_type(schema, prop))
                    })
                    .collect()
            })
            .unwrap_or_default();
        let summary = first_sentence(tool.description.as_deref().unwrap_or_default());
        lines.push(format!("{}({}) {} — {summary}", tool.name, args.join(", "), effect_of(&tool.name)));
    }
    Ok(lines.join("\n"))
}

/// `probe_function`: runs one read function once and returns its reply,
/// cut to its first PROBE_MAX_CHARS. Writes and model steps are refused.
pub async fn probe(engine: &Engine, args: &Map<String, Value>, cancel: &CancellationToken) -> anyhow::Result<String> {
    let mcp = &*engine.deps.mcp;
    let name = args.get("name").and_then(Value::as_str).filter(|n| !n.is_empty());
    let name = name.ok_or_else(|| anyhow::anyhow!("name: Expected nonempty string"))?;
    anyhow::ensure!(
        !is_loop_function(name) && !crate::supervisor::SIGNAL_FUNCTIONS.iter().any(|f| f.name == name),
        "`{name}` requires an admitted runtime context; test it through try_workflow with signal and drafts instead"
    );
    if name == "skill_catalog_snapshot" || name == "hash_value" {
        crate::errors::check(cancel)?;
        let arguments = args.get("arguments").cloned().unwrap_or_else(|| json!({}));
        let value = if name == "skill_catalog_snapshot" {
            crate::skills::catalog_snapshot(&*engine.deps.skill_store).await?
        } else {
            let value = arguments.get("value").ok_or_else(|| anyhow::anyhow!("value is required"))?;
            json!({"hash":crate::skills::hash_value(value)})
        };
        return Ok(cut(&value, PROBE_MAX_CHARS).to_string());
    }
    anyhow::ensure!(
        !MODEL_FUNCTIONS.iter().any(|f| f.0 == name),
        "`{name}` is a model step; probe the reads that feed it instead"
    );
    mcp_tool(mcp, name).ok_or_else(|| anyhow::anyhow!("no function `{name}`"))?;
    anyhow::ensure!(
        effect(mcp, name) == Some(Effect::Read),
        "`{name}` changes something; probe_function runs only read functions"
    );
    let arguments = match args.get("arguments") {
        None | Some(Value::Null) => Map::new(),
        Some(Value::Object(a)) => a.clone(),
        Some(_) => anyhow::bail!("arguments: Expected an object"),
    };
    let raw = mcp.call_tool(name, arguments, cancel).await?;
    let total = raw.chars().count();
    Ok(if total <= PROBE_MAX_CHARS {
        raw
    } else {
        let head: String = raw.chars().take(PROBE_MAX_CHARS).collect();
        format!("{head}\n… [cut: {total} characters in all; the shape above is the whole reply's]")
    })
}

fn cut(value: &Value, max: usize) -> Value {
    let text = match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    let total = text.chars().count();
    if total <= max {
        return value.clone();
    }
    let head: String = text.chars().take(max).collect();
    Value::String(format!("{head}… [cut: {total} characters in all]"))
}

// A dry run's catalog: reads and model steps run as in a scheduled run;
// a write is recorded with its arguments and answered with a stub instead.
struct DryRun {
    functions: Functions,
    writes: Mutex<Vec<Value>>,
}

// Kept for the scheduled form: saved workflows (skill-index) check a write's
// `ok`. A signal's dry run answers with the sandbox's explicit mock instead.
const DRY_WRITE_REPLY: &str = r#"{"ok":true,"dryRun":true}"#;

impl DryRun {
    fn intercept(&self, name: &str, args: &Value, reference: &str) -> Option<Value> {
        (effect(&*self.functions.engine.deps.mcp, name) == Some(Effect::Write)).then(|| {
            let write = json!({"task": reference, "function": name, "arguments": args});
            self.writes.lock().unwrap_or_else(|e| e.into_inner()).push(write);
            serde_json::from_str(DRY_WRITE_REPLY).expect("the stub reply")
        })
    }
}

#[async_trait]
impl owf::run::Functions for DryRun {
    fn effect(&self, name: &str) -> Option<Effect> {
        owf::run::Functions::effect(&self.functions, name)
    }

    async fn invoke(&self, name: &str, args: Value, info: &CallInfo<'_>) -> Result<Value, CallFailure> {
        match self.intercept(name, &args, info.reference) {
            Some(stub) => Ok(stub),
            None => owf::run::Functions::invoke(&self.functions, name, args, info).await,
        }
    }

    async fn call(&self, name: &str, args: Value, info: &CallInfo<'_>) -> Result<Value, WorkflowError> {
        match self.intercept(name, &args, info.reference) {
            Some(stub) => Ok(stub),
            None => owf::run::Functions::call(&self.functions, name, args, info).await,
        }
    }
}

// What a dry run reports, besides tracing it: the path (every task that
// started, by name, in order; skipped ones marked), and the tasks that do
// something (`call`, `set`, `raise`, `wait`) with their outputs, the skipped
// ones, faults and retries. A container's output repeats its children's.
#[derive(Default)]
struct DryReport {
    kinds: HashMap<String, &'static str>,
    path: Vec<String>,
    steps: Vec<Value>,
    omitted: usize,
}

struct DrySteps {
    spans: Arc<Spans>,
    report: Mutex<DryReport>,
}

impl Observer for DrySteps {
    fn event(&self, event: &Event<'_>) {
        self.spans.event(event);
        let mut report = self.report.lock().unwrap_or_else(|e| e.into_inner());
        let mut walk = |step: String| {
            if report.path.len() < DRY_MAX_PATH {
                report.path.push(step);
            }
        };
        match event {
            Event::TaskStarted { reference, kind, .. } if *kind != "do" => walk(task_name(reference).to_owned()),
            Event::TaskSkipped { reference, .. } => walk(format!("{} (skipped)", task_name(reference))),
            _ => {}
        }
        let step = match event {
            Event::TaskStarted { reference, kind, .. } => {
                report.kinds.insert(reference.to_string(), *kind);
                return;
            }
            Event::TaskCompleted { reference, output } => match report.kinds.get(*reference).copied() {
                Some(kind @ ("call" | "set" | "raise" | "wait")) => {
                    json!({"task": reference, "kind": kind, "output": cut(output, DRY_STEP_MAX_CHARS)})
                }
                _ => return,
            },
            Event::TaskSkipped { reference, .. } => json!({"task": reference, "skipped": true}),
            Event::TaskFaulted { reference, error } => json!({"task": reference, "error": error.to_string()}),
            Event::TaskRetrying { reference, attempt, error, .. } => {
                json!({"task": reference, "retry": attempt, "error": error.to_string()})
            }
            Event::TaskSuspended { .. } | Event::SwitchCase { .. } => return,
        };
        if report.steps.len() < DRY_MAX_STEPS {
            report.steps.push(step);
        } else {
            report.omitted += 1;
        }
    }
}

/// `try_workflow` without a signal, for a workflow that takes no agent turn:
/// runs a saved version (the latest by default) once on the
/// session's input, as a scheduled fire would, except that nothing changes:
/// every write is recorded and answered with a stub, waits and retry delays
/// don't sleep, and nothing is journaled. Returns the steps, the writes it
/// would have made and how it ended.
pub async fn try_draft(
    engine: &Arc<Engine>,
    args: &Map<String, Value>,
    env: &crate::session::EnvData,
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
    let saved =
        engine.deps.workflows.get(name, version)?.ok_or_else(|| anyhow::anyhow!("no saved workflow `{name}`"))?;
    let wf = check(&*engine.deps.mcp, &saved.definition).map_err(anyhow::Error::msg)?;
    // A loop, a route or the dispatcher runs on a signal, never on a
    // scheduled fire's empty input; so does a workflow taking agent turns,
    // which needs the input the host admits a signal's run with.
    anyhow::ensure!(
        !is_runtime_workflow(name, &wf),
        "`{name}` handles signals: try it with a signal (try_workflow with `signal`, and `drafts` for the rest of the chain)"
    );
    anyhow::ensure!(
        !takes_turns(&wf),
        "`{name}` takes agent turns, which run on the input the host admits a signal with: try it on a fire of its task (try_workflow with signal {{source: scheduler, content: the task's prompt, payload: {{workflow: \"{name}\"}}}})"
    );
    let signal = WorkflowSignal { id: 0, source: "scheduler".into(), content: String::new(), env_context: None };
    let input = Value::Object(signal_input(&signal, env));
    let root =
        scope.span(SpanStart::new("dry-run", SpanKind::Chain).input(input.clone()).metadata(metadata(
            json!({"workflow":name,"workflow_version":saved.version,"engine":"owf","dry_run":true}),
        )));
    let spans = Arc::new(Spans { root: root.clone(), open: Mutex::default() });
    let functions = Arc::new(DryRun {
        functions: Functions {
            engine: engine.clone(),
            spans: spans.clone(),
            runs: None,
            guard_only: false,
            loop_input: None,
            events: None,
            children: Arc::default(),
        },
        writes: Mutex::default(),
    });
    let steps = Arc::new(DrySteps { spans, report: Mutex::default() });
    let host = Host {
        functions: functions.clone(),
        observer: steps.clone(),
        clock: Arc::new(SystemClock),
        limits: Limits { max_sleep: Some(std::time::Duration::ZERO), ..Limits::default() },
        runtime: json!({"name":"agent-supervisor","version":env!("CARGO_PKG_VERSION")}),
    };
    let stop = cancel.child_token();
    let execution = owf::run::run(&host, &wf, "dry-run", input, &stop);
    tokio::pin!(execution);
    let mut timed_out = false;
    let outcome = tokio::select! {
        outcome = &mut execution => outcome,
        _ = tokio::time::sleep(DRY_RUN_TIMEOUT) => {
            timed_out = true;
            stop.cancel();
            execution.await
        }
    };
    let (status, output, error) = match &outcome.status {
        RunStatus::Completed(output) => ("completed", cut(output, DRY_STEP_MAX_CHARS), Value::Null),
        RunStatus::Faulted(error) | RunStatus::Interrupted(error) => ("faulted", Value::Null, json!(error)),
        RunStatus::Cancelled if timed_out => ("timed_out", Value::Null, Value::Null),
        RunStatus::Cancelled => anyhow::bail!("cancelled"),
        RunStatus::Waiting { .. } => ("waiting", Value::Null, Value::Null),
    };
    root.end(match &outcome.status {
        RunStatus::Completed(output) => SpanEnd::output(output.clone()),
        _ => SpanEnd::error(status),
    });
    if status == "completed" {
        engine.deps.workflows.mark_tried(name, saved.version)?;
    }
    let writes: Vec<Value> = std::mem::take(&mut *functions.writes.lock().unwrap_or_else(|e| e.into_inner()))
        .into_iter()
        .map(|mut write| {
            if let Some(Value::Object(args)) = write.get_mut("arguments") {
                args.values_mut().for_each(|v| *v = cut(v, DRY_WRITE_MAX_CHARS));
            }
            write
        })
        .collect();
    let report = std::mem::take(&mut *steps.report.lock().unwrap_or_else(|e| e.into_inner()));
    let mut reply = json!({
        "name": name, "version": saved.version, "status": status, "error": error, "output": output,
        "path": report.path, "steps": report.steps, "omitted_steps": report.omitted, "writes": writes,
        "note": format!("A dry run: reads and model steps ran for real on today's data; nothing changed. Each write above was recorded, not made, and answered with {DRY_WRITE_REPLY}, so a step that reads a write's reply saw that stub. Waits and retry delays did not sleep."),
        "next": "Check the path, every write's arguments and the message against each thing the user asked for. Anything wrong or questionable is yours to fix: change the definition, save a new version and try it again (three tries in all) before you report. Report only what needs the user's decision.",
    });
    let warnings = warnings(&wf);
    if !warnings.is_empty() {
        reply["warnings"] = json!(warnings);
    }
    Ok(reply.to_string())
}
