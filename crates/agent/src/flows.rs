// Saved workflows: Open Workflow Specification definitions (`crates/owf`)
// that run without a model deciding what to do next. 1. the store (agent.db
// `workflows`); 2. the function catalog (MCP tools and the model steps);
// 3. tracing (owf events → spans under the signal's trace, as `workflow.rs`
// writes them); 4. running one for a scheduler signal.
//
// A scheduled task naming a workflow (`schedule_task` with `workflow`; its
// fire's payload is `{ "workflow": "…" }`) runs its active version here.
// The supervisor hands a run that faulted before any `Write` call to the
// agent loop; one that faulted after goes to recovery, which only reports.
use crate::{
    agent::Engine,
    db::AgentDb,
    mcp::{Mcp, model_tools},
    providers::ToolDef,
    session::Session,
    tracing::{EventStart, Level, Scope, Span, SpanEnd, SpanKind, SpanStart},
    workflow::{WorkflowSignal, compose, parse_tool_json, render_input_xml, signal_input},
};
use async_trait::async_trait;
use futures::StreamExt;
use owf::durable::{CallFailure, Delivery};
use owf::run::{CallInfo, Effect, Event, Host, Limits, Observer, Status as RunStatus, SystemClock, WorkflowError};
use rusqlite::{OptionalExtension, params};
use serde::Serialize;
use serde_json::{Map, Value, json};
use std::collections::HashMap;
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
// → retired), with at most one active version per name.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SavedWorkflow {
    pub name: String,
    pub version: i64,
    pub status: Status,
    // The YAML as authored.
    pub definition: String,
    pub note: Option<String>,
    pub created_at: String,
}

pub trait WorkflowStore: Send + Sync {
    // Stores `definition` as the next version of `name`, a draft.
    fn save(&self, name: &str, definition: &str, note: Option<&str>) -> anyhow::Result<SavedWorkflow>;
    // Makes `version` the active one; the version active before retires.
    fn activate(&self, name: &str, version: i64) -> anyhow::Result<()>;
    // A version, or the newest one when `version` is None.
    fn get(&self, name: &str, version: Option<i64>) -> anyhow::Result<Option<SavedWorkflow>>;
    fn active(&self, name: &str) -> anyhow::Result<Option<SavedWorkflow>>;
    // The newest version of every workflow, by name.
    fn list(&self) -> anyhow::Result<Vec<SavedWorkflow>>;
}

const COLUMNS: &str = "name, version, status, definition, note, created_at";

fn saved(row: &rusqlite::Row) -> rusqlite::Result<(String, i64, String, String, Option<String>, String)> {
    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?))
}

fn into_saved(
    (name, version, status, definition, note, created_at): (String, i64, String, String, Option<String>, String),
) -> anyhow::Result<SavedWorkflow> {
    Ok(SavedWorkflow { name, version, status: Status::parse(&status)?, definition, note, created_at })
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

// The model steps a workflow may call besides MCP tools, the same ones the
// planner's workflows have: the name, the effect, the arguments it takes and
// the ones it needs.
type ModelFunction = (&'static str, Effect, &'static [&'static str], &'static [&'static str]);
const MODEL_FUNCTIONS: &[ModelFunction] = &[
    ("llm_compose", Effect::Read, &["preset", "skill", "prompt", "input"], &[]),
    ("llm_decide", Effect::Read, &["items", "prompt", "threshold"], &["items", "prompt"]),
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

// A failure the model or a tool reported, as opposed to the transport.
fn failed(e: anyhow::Error) -> WorkflowError {
    WorkflowError::runtime(e.to_string())
}

#[async_trait]
impl owf::run::Functions for Functions {
    fn effect(&self, name: &str) -> Option<Effect> {
        effect(&*self.engine.deps.mcp, name)
    }

    async fn invoke(&self, name: &str, args: Value, info: &CallInfo<'_>) -> Result<Value, CallFailure> {
        if MODEL_FUNCTIONS.iter().any(|f| f.0 == name) {
            if info.delivery == Delivery::AtMostOnce && name != "set_memory" {
                return Err(CallFailure::NotExecuted(WorkflowError::validation(
                    "model adapters do not yet support at_most_once",
                )));
            }
            return self.call(name, args, info).await.map_err(|error| {
                if error.is("validation") { CallFailure::NotExecuted(error) } else { CallFailure::Unknown(error) }
            });
        }
        let Value::Object(args) = args else {
            return Err(CallFailure::NotExecuted(invalid(name, "arguments must be an object")));
        };
        let span = self.spans.scope(info.reference);
        span.update(crate::tracing::ContextUpdate::input(json!({"tool":name,"args":args})));
        let raw =
            self.engine.deps.mcp.call_tool_once(name, args, info.cancel).await.map_err(|failure| match failure {
                crate::mcp::ToolCallFailure::NotExecuted(error) => {
                    CallFailure::NotExecuted(WorkflowError::communication(503, error.to_string()))
                }
                crate::mcp::ToolCallFailure::Unknown(error) => {
                    CallFailure::Unknown(WorkflowError::communication(503, error.to_string()))
                }
            })?;
        if raw.starts_with("[tool error]") {
            return Err(CallFailure::Response(WorkflowError::communication(500, raw)));
        }
        Ok(parse_tool_json(&raw))
    }

    async fn call(&self, name: &str, args: Value, info: &CallInfo<'_>) -> Result<Value, WorkflowError> {
        let span = self.spans.scope(info.reference);
        let label = info.reference.rsplit('/').next().unwrap_or(name);
        match name {
            "llm_compose" => {
                let preset = text(&args, name, "preset")?.unwrap_or("base");
                let skill = text(&args, name, "skill")?;
                let mut parts = Vec::new();
                if let Some(prompt) = text(&args, name, "prompt")? {
                    parts.push(prompt.to_owned());
                }
                match args.get("input") {
                    None | Some(Value::Null) => {}
                    Some(Value::Object(input)) => parts.push(render_input_xml(input)),
                    Some(_) => return Err(invalid(name, "`input` must be an object")),
                }
                let user = parts.into_iter().filter(|p| !p.is_empty()).collect::<Vec<_>>().join("\n\n");
                span.update(crate::tracing::ContextUpdate::input(json!({"skill":skill,"prompt":user})));
                compose(&self.engine, preset, skill, &user, label, &span, info.cancel).await.map_err(failed)
            }
            "llm_decide" => {
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
            "set_memory" => {
                let key = text(&args, name, "key")?
                    .filter(|k| !k.is_empty())
                    .ok_or_else(|| invalid(name, "`key` must be a nonempty string"))?;
                let value = text(&args, name, "value")?.ok_or_else(|| invalid(name, "`value` must be a string"))?;
                self.engine.deps.memory.set(key, value).map_err(failed)?;
                Ok(json!({"ok":true,"key":key}))
            }
            tool => {
                let Value::Object(args) = args else { return Err(invalid(tool, "arguments must be an object")) };
                span.update(crate::tracing::ContextUpdate::input(json!({"tool":tool,"args":args})));
                let raw = self
                    .engine
                    .deps
                    .mcp
                    .call_tool(tool, args, info.cancel)
                    .await
                    .map_err(|e| WorkflowError::communication(503, format!("{tool}: {e}")))?;
                if raw.starts_with("[tool error]") {
                    return Err(WorkflowError::communication(500, raw));
                }
                Ok(parse_tool_json(&raw))
            }
        }
    }
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
        match self.open.lock().unwrap_or_else(|e| e.into_inner()).get(reference) {
            Some(span) => span.clone(),
            None => self.root.clone(),
        }
    }

    fn take(&self, reference: &str) -> Option<Arc<dyn Span>> {
        self.open.lock().unwrap_or_else(|e| e.into_inner()).remove(reference)
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
    async fn run(
        &self,
        name: &str,
        signal: &WorkflowSignal,
        session: &Session,
        scope: &Scope,
        cancel: &CancellationToken,
    ) -> FlowResult;
}

// Runs the recovery worker resumes at once.
const RECOVERY_CONCURRENCY: usize = 4;

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

fn flow_result(status: RunStatus, effects_started: bool) -> FlowResult {
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

impl Flows {
    async fn execute_saved(
        &self,
        saved: crate::durable::SavedRun,
        lease: crate::durable::Lease,
        scope: &Scope,
        cancel: &CancellationToken,
    ) -> FlowResult {
        let root = scope.span(SpanStart::new("runner", SpanKind::Chain).input(saved.input.clone()));
        let mut stopped = false;
        let status = match owf::model::parse_yaml(&saved.definition) {
            Err(error) => (RunStatus::Interrupted(WorkflowError::validation(error.to_string())), true),
            Ok(wf) => {
                let spans = Arc::new(Spans { root: root.clone(), open: Mutex::default() });
                let host = Host {
                    functions: Arc::new(Functions { engine: self.engine.clone(), spans: spans.clone() }),
                    observer: spans,
                    clock: Arc::new(SystemClock),
                    limits: Limits::default(),
                    runtime: json!({"name":"agent-supervisor","version":env!("CARGO_PKG_VERSION")}),
                };
                // Shutdown releases work for recovery; only explicit run
                // cancellation is allowed to create a terminal Cancelled state.
                let execution_cancel = CancellationToken::new();
                let execution = owf::run::resume(&host, &wf, &saved.id, saved.input, &execution_cancel, &lease);
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
        let (status, effects_started) = status;
        let result = match if stopped { lease.release().map(|()| status) } else { lease.finish(&status) } {
            Ok(status) => flow_result(status, effects_started),
            Err(error) => FlowResult::Interrupted { error: WorkflowError::runtime(error.to_string()) },
        };
        root.end(match &result {
            FlowResult::Completed(output) => SpanEnd::output(output.clone()),
            FlowResult::Faulted { error, .. } | FlowResult::Interrupted { error } => SpanEnd::error(error.to_string()),
            FlowResult::Waiting { wake_at_ms } => SpanEnd::output(json!({"waiting_until_ms":wake_at_ms})),
            _ => SpanEnd::default(),
        });
        result
    }

    async fn run_durable(
        &self,
        runs: &crate::durable::RunStore,
        name: &str,
        signal: &WorkflowSignal,
        session: &Session,
        scope: &Scope,
        cancel: &CancellationToken,
    ) -> anyhow::Result<FlowResult> {
        let id = format!("{}:{}", signal.source, signal.id);
        let mut started = None;
        if runs.get(&id)?.is_none() {
            let Some(saved) = self.engine.deps.workflows.active(name)? else { return Ok(FlowResult::Missing) };
            if let Err(error) = check(&*self.engine.deps.mcp, &saved.definition) {
                return Ok(FlowResult::Faulted { error: WorkflowError::validation(error), effects_started: false });
            }
            started = runs.start(&id, name, &saved.definition, &Value::Object(signal_input(signal, &session.env)))?;
        }
        let saved = runs.get(&id)?.ok_or_else(|| anyhow::anyhow!("run {id} disappeared"))?;
        anyhow::ensure!(saved.name == name, "run {id} belongs to another workflow");
        if let Some(status) = saved.outcome {
            return Ok(flow_result(status, true));
        }
        let lease = match started {
            Some(lease) => lease,
            None => match runs.claim(&id)? {
                Some(lease) => lease,
                None => {
                    return Ok(FlowResult::Waiting { wake_at_ms: chrono::Utc::now().timestamp_millis() + 1_000 });
                }
            },
        };
        Ok(self.execute_saved(saved, lease, scope, cancel).await)
    }

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

    async fn resume(&self, saved: crate::durable::SavedRun, lease: crate::durable::Lease, cancel: &CancellationToken) {
        let id = saved.id.clone();
        let trace = self.engine.deps.tracer.trace(crate::tracing::TraceStart {
            id: uuid::Uuid::new_v4().to_string(),
            name: format!("resume:{}", saved.name),
            session_id: Some(id.clone()),
            ..Default::default()
        });
        let scope: Scope = trace.clone();
        let result = self.execute_saved(saved, lease, &scope, cancel).await;
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
        futures::future::join_all(claimed.into_iter().map(|(saved, lease)| self.resume(saved, lease, cancel))).await;
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
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                Some(()) = running.next(), if !running.is_empty() => continue,
                _ = tick.tick() => {}
            }
            match self.claim_due(runs, RECOVERY_CONCURRENCY - running.len()) {
                Ok(claimed) => {
                    running.extend(claimed.into_iter().map(|(saved, lease)| self.resume(saved, lease, &cancel)))
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
        fn has(&self, name: &str) -> bool {
            effect(self.0, name).is_some()
        }
    }
    let wf = owf::model::parse_yaml(definition).map_err(|e| e.to_string())?;
    owf::model::validate(&wf, &Known(mcp)).map_err(|e| e.to_string())?;
    let mut problems = Vec::new();
    for site in owf::model::call_sites(&wf) {
        if site.delivery == Delivery::AtMostOnce && matches!(site.function.as_str(), "llm_compose" | "llm_decide") {
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
    async fn run(
        &self,
        name: &str,
        signal: &WorkflowSignal,
        session: &Session,
        scope: &Scope,
        cancel: &CancellationToken,
    ) -> FlowResult {
        if let Some(runs) = &self.runs {
            return self
                .run_durable(runs, name, signal, session, scope, cancel)
                .await
                .unwrap_or_else(|error| FlowResult::Interrupted { error: WorkflowError::runtime(error.to_string()) });
        }
        let unchanged = |error| FlowResult::Faulted { error, effects_started: false };
        let saved = match self.engine.deps.workflows.active(name) {
            Ok(Some(saved)) => saved,
            Ok(None) => return FlowResult::Missing,
            Err(e) => return unchanged(WorkflowError::runtime(format!("loading workflow `{name}`: {e}"))),
        };
        let input = Value::Object(signal_input(signal, &session.env));
        let root = scope.span(
            SpanStart::new("runner", SpanKind::Chain)
                .input(input.clone())
                .metadata(metadata(json!({"workflow":name,"workflow_version":saved.version,"engine":"owf"}))),
        );
        let result = match check(&*self.engine.deps.mcp, &saved.definition) {
            // A definition that no longer validates (a tool went away) changed nothing.
            Err(problems) => unchanged(WorkflowError::validation(problems)),
            Ok(wf) => {
                let spans = Arc::new(Spans { root: root.clone(), open: Mutex::default() });
                let host = Host {
                    functions: Arc::new(Functions { engine: self.engine.clone(), spans: spans.clone() }),
                    observer: spans,
                    clock: Arc::new(SystemClock),
                    limits: Limits::default(),
                    runtime: json!({"name":"agent-supervisor","version":env!("CARGO_PKG_VERSION")}),
                };
                let run_id = format!("{}:{}", signal.source, signal.id);
                let outcome = owf::run::run(&host, &wf, &run_id, input, cancel).await;
                flow_result(outcome.status, outcome.effects_started)
            }
        };
        root.end(match &result {
            FlowResult::Completed(output) => SpanEnd::output(output.clone()),
            FlowResult::Faulted { error, effects_started } => {
                SpanEnd::error(error.to_string()).with_output(json!({"error":error,"effects_started":effects_started}))
            }
            FlowResult::Cancelled => SpanEnd::error("cancelled"),
            FlowResult::Waiting { wake_at_ms } => SpanEnd::output(json!({"waiting_until_ms":wake_at_ms})),
            FlowResult::Interrupted { error } => SpanEnd::error(error.to_string()),
            FlowResult::Missing => SpanEnd::default(),
        });
        result
    }
}

// ── 5. authoring tools ─────────────────────────────────────────────────────

// Synthetic tools of the agent loop for writing saved workflows. Saving
// validates and stores a draft; only activation (after the user agreed in
// the chat) lets a schedule run it.
pub const TOOLS: &[&str] = &["save_workflow", "activate_workflow", "list_workflows", "read_workflow"];

fn effect_name(mcp: &dyn Mcp, wf: &owf::model::Workflow, function: &str) -> &'static str {
    match effect(mcp, function) {
        Some(Effect::Read) => "read",
        Some(Effect::Write) => "write",
        None if wf.uses.functions.iter().any(|(n, _)| n == function) => "function",
        None => "unknown",
    }
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
            let wf = check(mcp, definition).map_err(anyhow::Error::msg)?;
            anyhow::ensure!(
                wf.document.name == name,
                "document.name is `{}` but the workflow is saved as `{name}`; make them the same",
                wf.document.name
            );
            let saved = store.save(name, definition, args.get("note").and_then(Value::as_str))?;
            let calls: Vec<Value> = owf::model::call_sites(&wf)
                .into_iter()
                .map(|c| json!({"reference":c.reference,"function":c.function,"effect":effect_name(mcp, &wf, &c.function)}))
                .collect();
            Ok(json!({
                "ok": true, "name": name, "version": saved.version, "status": "draft", "calls": calls,
                "next": "Show the user what it will do, the write calls above all, and activate this version only after they confirm it in the chat.",
            })
            .to_string())
        }
        "activate_workflow" => {
            let name = text("name")?;
            let version = version()?.ok_or_else(|| anyhow::anyhow!("version: Required"))?;
            let saved = store
                .get(name, Some(version))?
                .ok_or_else(|| anyhow::anyhow!("no version {version} of workflow `{name}`"))?;
            // Tools may have changed since it was saved.
            check(mcp, &saved.definition).map_err(anyhow::Error::msg)?;
            store.activate(name, version)?;
            Ok(json!({
                "ok": true, "name": name, "version": version,
                "next": format!("It runs whenever a task scheduled with schedule_task(workflow: \"{name}\") fires; create one if none exists. Existing schedules run the new version."),
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
            Ok(match store.get(name, version()?)? {
                None => json!({"name": name, "found": false}),
                Some(w) => json!({"name":w.name,"found":true,"version":w.version,"status":w.status,"note":w.note,"definition":w.definition}),
            }
            .to_string())
        }
        _ => anyhow::bail!("unknown workflow tool {name}"),
    }
}
