// Saved workflows: Open Workflow Specification definitions (`crates/owf`)
// that run without a model deciding what to do next. 1. the store (agent.db
// `workflows`); 2. the function catalog (MCP tools and the model steps);
// 2b. model composition and signal input; 3. tracing (owf events → spans
// under the signal's trace); 4. running one for a scheduler signal.
//
// A scheduled task naming a workflow (`schedule_task` with `workflow`; its
// fire's payload is `{ "workflow": "…" }`) runs its active version here.
// The supervisor hands a run that faulted before any `Write` call to the
// agent loop; one that faulted after goes to recovery, which only reports.
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
use futures::StreamExt;
use owf::durable::{CallFailure, Delivery};
use owf::run::{CallInfo, Effect, Event, Host, Limits, Observer, Status as RunStatus, SystemClock, WorkflowError};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
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
const MODEL_FUNCTIONS: &[ModelFunction] = &[
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
    if LOOP_FUNCTIONS.contains(&name) {
        return Some(Effect::Read);
    }
    if let Some((_, effect)) = crate::supervisor::SIGNAL_FUNCTIONS.iter().find(|f| f.0 == name) {
        return Some(*effect);
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

// A failure the model or a tool reported, as opposed to the transport.
fn failed(e: anyhow::Error) -> WorkflowError {
    WorkflowError::runtime(e.to_string())
}

// The agent loop's functions (`agent::AGENT_LOOP`): a turn, one tool call
// by name, a round's settlement. They take the loop from the run's context,
// so they belong to an `agent-loop` definition.
const LOOP_FUNCTIONS: &[&str] = &["agent_turn", "tool", "agent_settle"];

fn loop_tool_name(args: &Value) -> &str {
    args.get("call").and_then(|c| c.get("name")).and_then(Value::as_str).unwrap_or_default()
}

#[async_trait]
impl owf::run::Functions for Functions {
    fn effect(&self, name: &str) -> Option<Effect> {
        effect(&*self.engine.deps.mcp, name)
    }

    fn effect_of(&self, name: &str, args: &Value) -> Option<Effect> {
        if name == "tool" {
            return Some(crate::agent::tool_effect(&self.engine, loop_tool_name(args)));
        }
        self.effect(name)
    }

    // A durable sub-agent is the exception: calling it again joins its run.
    fn at_most_once(&self, name: &str, args: &Value) -> bool {
        name == "tool"
            && self.effect_of(name, args) == Some(Effect::Write)
            && (self.runs.is_none() || crate::agent::child_key(args).is_none())
    }

    fn unknown_outcome(&self, name: &str, args: &Value) -> Option<Value> {
        (name == "tool").then(|| crate::agent::unknown_tool_outcome(loop_tool_name(args)))
    }

    // A loop's turn and calls stop on its cancellation and end their spans
    // and child loops first.
    fn cooperative(&self, name: &str) -> bool {
        LOOP_FUNCTIONS.contains(&name)
    }

    async fn invoke(&self, name: &str, args: Value, info: &CallInfo<'_>) -> Result<Value, CallFailure> {
        if crate::supervisor::SIGNAL_FUNCTIONS.iter().any(|f| f.0 == name) {
            return Err(CallFailure::NotExecuted(invalid(name, "only the signal workflow calls it")));
        }
        if LOOP_FUNCTIONS.contains(&name) {
            // A tool reports its own failures in its reply; an error here
            // (cancellation, a lost loop) leaves a write's outcome unknown.
            return self.call(name, args, info).await.map_err(|error| {
                if name == "tool" { CallFailure::Unknown(error) } else { CallFailure::Response(error) }
            });
        }
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
            "agent_turn" | "tool" | "agent_settle" => {
                // A loop rebuilt after a restart is traced under the runner.
                let root: Scope = self.spans.root.clone();
                let engine = &self.engine;
                match name {
                    "agent_turn" => crate::agent::loop_turn(engine, &args, &root, info.cancel).await,
                    "tool" => crate::agent::loop_tool(engine, &args, &root, info.cancel, self.runs.as_ref()).await,
                    _ => crate::agent::loop_settle(engine, &args, &root, info.cancel).await,
                }
                .map_err(failed)
            }
            "hash_value" => {
                let value = args.get("value").ok_or_else(|| invalid(name, "`value` is required"))?;
                Ok(json!({"hash":crate::skills::hash_value(value)}))
            }
            "skill_catalog_snapshot" => {
                crate::skills::catalog_snapshot(&*self.engine.deps.skill_store).await.map_err(failed)
            }
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
                let json = match args.get("json") {
                    None | Some(Value::Null) => false,
                    Some(Value::Bool(json)) => *json,
                    Some(_) => return Err(invalid(name, "`json` must be true or false")),
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
        .get(PresetName::selectable(preset).ok_or_else(|| anyhow::anyhow!("unknown preset"))?)
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

    /// Drives an agent loop as a run `id` of the built-in `agent-loop`
    /// definition on `input` (`AgentLoop::workflow_input`). `Missing` when
    /// this runner does not drive loops: the caller runs the loop itself.
    async fn run_loop(&self, _id: &str, _input: Value, _scope: &Scope, _cancel: &CancellationToken) -> FlowResult {
        FlowResult::Missing
    }
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
async fn execute_saved(
    engine: &Arc<Engine>,
    runs: &crate::durable::RunStore,
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
            let host = host(engine, &root, saved.name == crate::agent::AGENT_LOOP_NAME, Some(runs.clone()));
            // Shutdown releases work for recovery; only explicit run
            // cancellation is allowed to create a terminal Cancelled state.
            let execution_cancel = CancellationToken::new();
            let execution = owf::run::resume(&host, &wf, &saved.id, saved.input, &execution_cancel, &lease);
            tokio::pin!(execution);
            let mut heartbeat = tokio::time::interval(std::time::Duration::from_millis(crate::durable::HEARTBEAT_MS));
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
        // Handed back for the next process to resume: not a failure.
        FlowResult::Interrupted { .. } if stopped => SpanEnd::output(json!({"released":true})),
        FlowResult::Completed(output) => SpanEnd::output(output.clone()),
        FlowResult::Faulted { error, .. } | FlowResult::Interrupted { error } => SpanEnd::error(error.to_string()),
        FlowResult::Waiting { wake_at_ms } => SpanEnd::output(json!({"waiting_until_ms":wake_at_ms})),
        _ => SpanEnd::default(),
    });
    result
}

impl Flows {
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
        Ok(execute_saved(&self.engine, runs, saved, lease, scope, cancel).await)
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

    async fn resume(
        &self,
        runs: &crate::durable::RunStore,
        saved: crate::durable::SavedRun,
        lease: crate::durable::Lease,
        cancel: &CancellationToken,
    ) {
        let id = saved.id.clone();
        let trace = self.engine.deps.tracer.trace(crate::tracing::TraceStart {
            id: uuid::Uuid::new_v4().to_string(),
            name: format!("resume:{}", saved.name),
            session_id: Some(id.clone()),
            ..Default::default()
        });
        let scope: Scope = trace.clone();
        let driven = (saved.name == crate::agent::AGENT_LOOP_NAME)
            .then(|| saved.input["loop"]["id"].as_str().map(str::to_owned))
            .flatten();
        let version = saved.input.get(OVERRIDE).and_then(Value::as_i64);
        let result = execute_saved(&self.engine, runs, saved, lease, &scope, cancel).await;
        if let Some(id) = driven {
            loop_outcome(&self.engine, version, &result);
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
        fn has(&self, name: &str) -> bool {
            effect(self.0, name).is_some()
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
                    functions: Arc::new(Functions { engine: self.engine.clone(), spans: spans.clone(), runs: None }),
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

    async fn run_loop(&self, id: &str, input: Value, scope: &Scope, cancel: &CancellationToken) -> FlowResult {
        match &self.runs {
            Some(runs) => run_loop_durable(&self.engine, runs, id, input, scope, cancel).await,
            None => run_loop_here(&self.engine, id, input, scope, cancel).await,
        }
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
    let (definition, version) = loop_definition(engine);
    let input = with_override(input, version);
    let claimed = (|| -> anyhow::Result<Option<(crate::durable::SavedRun, crate::durable::Lease)>> {
        let lease = match runs.start(id, crate::agent::AGENT_LOOP_NAME, &definition, &input)? {
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
        Ok(Some((saved, lease))) => {
            let version = saved.input.get(OVERRIDE).and_then(Value::as_i64);
            let result = execute_saved(engine, runs, saved, lease, scope, cancel).await;
            loop_outcome(engine, version, &result);
            result
        }
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

// ── 4b. the agent loop's definition ─────────────────────────────────────────
// Every agent loop runs `agent-loop`: the version the agent saved and
// activated under that name, or the built-in one in git (`workflows/agent-loop.yaml`).
// A circuit breaker guards the agent's version: LOOP_BREAKER faults in a row
// and loops go back to the built-in one until another version is activated.

// The same goes for every workflow the runtime itself runs (`built_in`):
// `signal` too (`supervisor.rs`), each with its own breaker.

const LOOP_BREAKER: u32 = 3;
// Where a run's input names the saved version it runs (null: the built-in).
const OVERRIDE: &str = "override_version";

/// The host an `agent-loop` run executes on, traced under `root`: for a
/// caller that owns the run's journal itself (a test injecting crashes).
pub fn loop_host(engine: &Arc<Engine>, root: &Arc<dyn Span>) -> Host {
    host(engine, root, true, None)
}

fn host(engine: &Arc<Engine>, root: &Arc<dyn Span>, agent_loop: bool, runs: Option<crate::durable::RunStore>) -> Host {
    let spans = Arc::new(Spans { root: root.clone(), open: Mutex::default() });
    // A loop's generations and tool calls trace themselves; spans for its
    // turn/round/settle tasks would only repeat them.
    let observer: Arc<dyn owf::run::Observer> = if agent_loop { Arc::new(owf::run::Silent) } else { spans.clone() };
    Host {
        functions: Arc::new(Functions { engine: engine.clone(), spans, runs }),
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
    let Ok(Some(saved)) = engine.deps.workflows.active(name) else { return builtin };
    let state = breaker(engine, name);
    if (state.open && state.version == saved.version) || check(&*engine.deps.mcp, &saved.definition).is_err() {
        return builtin;
    }
    (saved.definition, Some(saved.version))
}

/// The definition a new loop runs (`definition` of `agent-loop`).
pub fn loop_definition(engine: &Engine) -> (String, Option<i64>) {
    definition(engine, crate::agent::AGENT_LOOP_NAME)
}

fn with_override(mut input: Value, version: Option<i64>) -> Value {
    if let Value::Object(fields) = &mut input {
        fields.insert(OVERRIDE.into(), json!(version));
    }
    input
}

fn loop_outcome(engine: &Engine, version: Option<i64>, result: &FlowResult) {
    record_outcome(engine, crate::agent::AGENT_LOOP_NAME, version, result)
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

/// Runs a loop in this process, without checkpoints: a worker's, a
/// recovery's. `input` is `AgentLoop::workflow_input`'s.
pub async fn run_loop_here(
    engine: &Arc<Engine>,
    id: &str,
    input: Value,
    scope: &Scope,
    cancel: &CancellationToken,
) -> FlowResult {
    let (definition, version) = loop_definition(engine);
    let root = scope.span(SpanStart::new("runner", SpanKind::Chain).metadata(metadata(
        json!({"workflow":crate::agent::AGENT_LOOP_NAME,"workflow_version":version,"engine":"owf"}),
    )));
    let result = match owf::model::parse_yaml(&definition) {
        Err(error) => {
            FlowResult::Faulted { error: WorkflowError::validation(error.to_string()), effects_started: false }
        }
        Ok(wf) => {
            let outcome =
                owf::run::run(&host(engine, &root, true, None), &wf, id, with_override(input, version), cancel).await;
            flow_result(outcome.status, outcome.effects_started)
        }
    };
    loop_outcome(engine, version, &result);
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
const DRY_RUN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

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
            let mut reply = json!({
                "ok": true, "name": name, "version": saved.version, "status": "draft", "calls": calls,
                "next": "Dry-run it (try_workflow) before the user sees it, then describe in words what it will do, the write calls above all; activate this version only after they confirm it in the chat.",
            });
            let warnings = warnings(&wf);
            if !warnings.is_empty() {
                reply["warnings"] = json!(warnings);
            }
            Ok(reply.to_string())
        }
        "activate_workflow" => {
            let name = text("name")?;
            let version = version()?.ok_or_else(|| anyhow::anyhow!("version: Required"))?;
            let saved = store
                .get(name, Some(version))?
                .ok_or_else(|| anyhow::anyhow!("no version {version} of workflow `{name}`"))?;
            // Tools may have changed since it was saved.
            check(mcp, &saved.definition).map_err(anyhow::Error::msg)?;
            if built_in(name).is_some() {
                // The runtime's own workflow is tried by running: the breaker
                // takes it back after a few faults in a row.
                store.activate(name, version)?;
                let runs = if name == crate::agent::AGENT_LOOP_NAME { "Every agent loop" } else { "Every signal" };
                return Ok(json!({
                    "ok": true, "name": name, "version": version,
                    "next": format!("{runs} started from now runs this version; three faults in a row put the built-in one back."),
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
        if let Some((_, _, takes, needs)) = MODEL_FUNCTIONS.iter().find(|f| f.0 == name) {
            return Ok(json!({"name": name, "effect": effect_of(name), "takes": takes, "needs": needs,
                "description": "A step of the workflow runtime; the skill describes it."})
            .to_string());
        }
        let tool = tools.iter().find(|t| t.name == name).ok_or_else(|| anyhow::anyhow!("no function `{name}`"))?;
        return Ok(json!({"name": name, "effect": effect_of(name), "description": tool.description,
            "parameters": tool.parameters})
        .to_string());
    }
    let mut lines = vec![
        "One line per function: name(arguments; `?` optional) effect — summary. \
        workflow_functions(name) gives one in full."
            .to_owned(),
    ];
    for (name, effect, takes, needs) in MODEL_FUNCTIONS {
        let args: Vec<String> =
            takes.iter().map(|a| if needs.contains(a) { a.to_string() } else { format!("{a}?") }).collect();
        let effect = if *effect == Effect::Read { "read" } else { "write" };
        lines.push(format!("{name}({}) {effect} — a runtime step; see the skill", args.join(", ")));
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
            Event::TaskSuspended { .. } => return,
        };
        if report.steps.len() < DRY_MAX_STEPS {
            report.steps.push(step);
        } else {
            report.omitted += 1;
        }
    }
}

/// `try_workflow`: runs a saved version (the latest by default) once on the
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
    let signal = WorkflowSignal { id: 0, source: "scheduler".into(), content: String::new(), env_context: None };
    let input = Value::Object(signal_input(&signal, env));
    let root = scope.span(
        SpanStart::new("dry-run", SpanKind::Chain)
            .input(input.clone())
            .metadata(metadata(json!({"workflow":name,"workflow_version":saved.version,"engine":"owf"}))),
    );
    let spans = Arc::new(Spans { root: root.clone(), open: Mutex::default() });
    let functions = Arc::new(DryRun {
        functions: Functions { engine: engine.clone(), spans: spans.clone(), runs: None },
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
