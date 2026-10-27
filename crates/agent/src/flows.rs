// The workflows the agent runs and the tools for authoring them:
// 1. the built-in definitions and the agent's notes on its versions
// (`workflow_meta`); 2. model steps (composition, choice options, input
// rendering); 3. what a workflow can call, and the gate every save and
// activation passes; 4. routes, loops and the model's parameters a definition
// declares; 5. authoring tools; 6. the evals workbench's in-memory adapter.
//
// Every run is an engine run: the registry the runs call is functions.rs,
// the runtime wiring runs.rs. The engine is the version store
// (docs/designs/04-versions-and-breaker.md, decision 2): a version's number,
// text, digest, activation history, active version and breaker are the
// engine's; the agent keeps only what the engine lacks, by content.
use crate::errors::is_aborted;
use crate::{
    agent::{Effect, Engine},
    db::AgentDb,
    generation::{Observation, generation},
    mcp::{Mcp, model_tools},
    models::PresetName,
    providers::{self, CompletionParams, ToolDef},
    session::{EnvData, iso},
    skills::append_patch,
    tracing::{GenerationEnd, Scope},
};
use ouro_runtime::ows::model::{Flow, Kind, Task, TaskList, Workflow};
use ows::durable::Delivery;
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::sync::Mutex;
use tokio_util::sync::CancellationToken;

// ── 1. built-in definitions ─────────────────────────────────────────────────

pub const GUARD_NAME: &str = "guard";
pub const GUARD: &str = include_str!("../workflows/guard.yaml");
/// A Telegram signal's route: the wrapper around its loop, and the observer
/// that writes its progress bubble (docs/designs/08-telegram-loop.md). The
/// agent may override either.
pub const TELEGRAM_NAME: &str = "telegram";
pub const TELEGRAM: &str = include_str!("../workflows/telegram.yaml");
pub const TELEGRAM_STATUS_NAME: &str = "telegram-status";
pub const TELEGRAM_STATUS: &str = include_str!("../workflows/telegram-status.yaml");

/// The definitions the runtime itself runs, saved and activated at boot.
pub fn builtins() -> Vec<(&'static str, &'static str)> {
    vec![
        (crate::supervisor::SIGNAL_NAME, crate::supervisor::SIGNAL),
        (crate::agent::AGENT_LOOP_NAME, crate::agent::AGENT_LOOP),
        (GUARD_NAME, GUARD),
        (TELEGRAM_STATUS_NAME, TELEGRAM_STATUS),
        (TELEGRAM_NAME, TELEGRAM),
    ]
}

pub fn built_in(name: &str) -> Option<&'static str> {
    builtins().into_iter().find(|(n, _)| *n == name).map(|(_, yaml)| yaml)
}

/// The names the agent can never save or activate a version of (design 04,
/// decision 1, narrowing design 01's 17): only the guard round, whose stored
/// outcome authorizes the model's calls. `agent-loop` and `signal` are the
/// agent's to override (#55).
pub const RESERVED: &[&str] = &[GUARD_NAME];

/// Whether `name` is the runtime's alone: a reserved built-in, or a
/// scheduled task's definition (`task-<id>`, which only the schedule tools
/// write, design 06 decision 1).
pub fn reserved(name: &str) -> bool {
    RESERVED.contains(&name) || name.starts_with(crate::schedule::TASK_PREFIX)
}

/// The one reserved-name check: `save_workflow`, `activate_workflow`, a
/// named start (`runs::start_named_child`, so `launch` too) and
/// `schedule_task(workflow:)` refuse what `reserved` names.
pub fn refuse_reserved(name: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !name.starts_with(crate::schedule::TASK_PREFIX),
        "`{name}` is a scheduled task's definition: only schedule_task and cancel_scheduled_task write it"
    );
    anyhow::ensure!(
        !RESERVED.contains(&name),
        "{name} is a reserved built-in workflow and cannot be saved or activated"
    );
    Ok(())
}

/// The stored version of `name` whose content is the compiled built-in text
/// (`None`: no built-in, or not saved yet).
pub async fn builtin_version(runtime: &ouro_runtime::Runtime, name: &str) -> anyhow::Result<Option<u64>> {
    let Some(yaml) = built_in(name) else { return Ok(None) };
    let digest = ouro_runtime::durable::digest_of(yaml);
    Ok(runtime.definitions(name).await?.versions.values().find(|d| d.digest == digest).map(|d| d.version))
}

/// What the agent knows of a version that the engine does not, by content
/// (`name`, `digest`): its note, whether a dry run tried it (activation
/// needs that), whether it is a built-in text, and when it was first saved.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Meta {
    pub name: String,
    pub digest: String,
    pub note: Option<String>,
    pub builtin: bool,
    pub created_ms: i64,
    pub tried_ms: Option<i64>,
}

/// The `workflow_meta` table (agent.db, migration 0016). Rows are written
/// before the engine's save (R-L2): an orphan row (a crash between them) is
/// harmless, and `tried` and the note attach to the content a dry run proves.
pub trait WorkflowMeta: Send + Sync {
    /// An upsert (R2-L6): the first write sets `created_ms`; a given note
    /// replaces the stored one; `builtin` only goes from false to true;
    /// `tried` is left as it is.
    fn record(&self, name: &str, digest: &str, note: Option<&str>, builtin: bool) -> anyhow::Result<()>;
    /// Records that a dry run of this content completed; never cleared.
    fn mark_tried(&self, name: &str, digest: &str) -> anyhow::Result<()>;
    fn get(&self, name: &str, digest: &str) -> anyhow::Result<Option<Meta>>;
    /// Every name with a row, sorted.
    fn names(&self) -> anyhow::Result<Vec<String>>;
}

fn meta_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Meta> {
    Ok(Meta {
        name: r.get(0)?,
        digest: r.get(1)?,
        note: r.get(2)?,
        builtin: r.get(3)?,
        created_ms: r.get(4)?,
        tried_ms: r.get(5)?,
    })
}

impl WorkflowMeta for AgentDb {
    fn record(&self, name: &str, digest: &str, note: Option<&str>, builtin: bool) -> anyhow::Result<()> {
        self.conn().execute(
            "INSERT INTO workflow_meta (name, digest, note, builtin, created_ms) VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT (name, digest) DO UPDATE SET
               note = COALESCE(excluded.note, workflow_meta.note),
               builtin = MAX(workflow_meta.builtin, excluded.builtin)",
            params![name, digest, note, builtin, crate::runs::now_ms()],
        )?;
        Ok(())
    }

    fn mark_tried(&self, name: &str, digest: &str) -> anyhow::Result<()> {
        let changed = self.conn().execute(
            "UPDATE workflow_meta SET tried_ms = COALESCE(tried_ms, ?3) WHERE name = ?1 AND digest = ?2",
            params![name, digest, crate::runs::now_ms()],
        )?;
        anyhow::ensure!(changed > 0, "no saved version of workflow `{name}` has this content");
        Ok(())
    }

    fn get(&self, name: &str, digest: &str) -> anyhow::Result<Option<Meta>> {
        Ok(self
            .read()
            .query_row(
                "SELECT name, digest, note, builtin, created_ms, tried_ms FROM workflow_meta
                 WHERE name = ?1 AND digest = ?2",
                params![name, digest],
                meta_row,
            )
            .optional()?)
    }

    fn names(&self) -> anyhow::Result<Vec<String>> {
        let conn = self.read();
        let mut statement = conn.prepare("SELECT DISTINCT name FROM workflow_meta ORDER BY name")?;
        Ok(statement.query_map([], |r| r.get(0))?.collect::<rusqlite::Result<_>>()?)
    }
}

impl AgentDb {
    /// Copies every `workflow_meta` row into `to`: what a dry run's
    /// database knows of the versions (sandbox.rs, design 05 decision 1).
    pub fn copy_workflow_meta(&self, to: &AgentDb) -> anyhow::Result<()> {
        let rows: Vec<Meta> = {
            let conn = self.read();
            let mut statement =
                conn.prepare("SELECT name, digest, note, builtin, created_ms, tried_ms FROM workflow_meta")?;
            statement.query_map([], meta_row)?.collect::<rusqlite::Result<_>>()?
        };
        let mut conn = to.conn();
        let tx = conn.transaction()?;
        for m in rows {
            tx.execute(
                "INSERT OR REPLACE INTO workflow_meta (name, digest, note, builtin, created_ms, tried_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![m.name, m.digest, m.note, m.builtin, m.created_ms, m.tried_ms],
            )?;
        }
        tx.commit()?;
        Ok(())
    }
}

/// The same contract without a database, for tests and tools that need none.
#[derive(Default)]
pub struct MemoryMeta(Mutex<BTreeMap<(String, String), Meta>>);

impl WorkflowMeta for MemoryMeta {
    fn record(&self, name: &str, digest: &str, note: Option<&str>, builtin: bool) -> anyhow::Result<()> {
        let mut rows = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let row = rows.entry((name.into(), digest.into())).or_insert_with(|| Meta {
            name: name.into(),
            digest: digest.into(),
            note: None,
            builtin: false,
            created_ms: crate::runs::now_ms(),
            tried_ms: None,
        });
        if let Some(note) = note {
            row.note = Some(note.into());
        }
        row.builtin |= builtin;
        Ok(())
    }

    fn mark_tried(&self, name: &str, digest: &str) -> anyhow::Result<()> {
        let mut rows = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let row = rows
            .get_mut(&(name.to_owned(), digest.to_owned()))
            .ok_or_else(|| anyhow::anyhow!("no saved version of workflow `{name}` has this content"))?;
        row.tried_ms.get_or_insert_with(crate::runs::now_ms);
        Ok(())
    }

    fn get(&self, name: &str, digest: &str) -> anyhow::Result<Option<Meta>> {
        Ok(self.0.lock().unwrap_or_else(|e| e.into_inner()).get(&(name.to_owned(), digest.to_owned())).cloned())
    }

    fn names(&self) -> anyhow::Result<Vec<String>> {
        let rows = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let mut names: Vec<String> = rows.keys().map(|(name, _)| name.clone()).collect();
        names.dedup();
        Ok(names)
    }
}

/// A version's status, derived from the engine's catalog: the active one;
/// one activated before (`retired`); one never activated (`draft`).
pub fn status(definitions: &ouro_runtime::durable::Definitions, version: u64) -> &'static str {
    if definitions.active == Some(version) {
        "active"
    } else if definitions.activations.contains_key(&version) {
        "retired"
    } else {
        "draft"
    }
}

/// Every name the agent has a version of, the built-ins first.
pub fn names(db: &AgentDb) -> anyhow::Result<Vec<String>> {
    let mut names: Vec<String> = builtins().into_iter().map(|(name, _)| name.to_owned()).collect();
    for name in WorkflowMeta::names(db)? {
        if !names.contains(&name) {
            names.push(name);
        }
    }
    Ok(names)
}

// ── 2. model steps ──────────────────────────────────────────────────────────

// `choose`'s options: 2–255 labels a jq string comparison can name, each
// with a nonempty criterion.
pub(crate) fn choose_options(args: &Value) -> Result<Map<String, Value>, String> {
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

// ── 3. what a workflow can call ─────────────────────────────────────────────

// The argument names `name` takes (None: any) and the ones it needs.
fn arguments(mcp: &dyn Mcp, name: &str) -> Option<(Option<Vec<String>>, Vec<String>)> {
    let names = |list: &[&str]| list.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    if let Some(function) = crate::functions::host_function(name) {
        return Some((Some(names(function.takes)), names(function.needs)));
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

fn mcp_tool(mcp: &dyn Mcp, name: &str) -> Option<ToolDef> {
    if crate::functions::WITHHELD.contains(&name) {
        return None;
    }
    model_tools(mcp).into_iter().find(|t| t.name == name)
}

/// What `name` does, or None when a workflow cannot call it. An MCP tool not
/// marked read-only counts as a write.
pub fn effect(mcp: &dyn Mcp, name: &str) -> Option<Effect> {
    if let Some(function) = crate::functions::host_function(name) {
        return Some(function.effect);
    }
    mcp_tool(mcp, name)?;
    Some(if mcp.read_only(name) { Effect::Read } else { Effect::Write })
}

// Each call's argument names against the function's: a misspelt argument
// fails at save rather than on the first run. The engine's own binding of
// an MCP tool is permissive (design 01, decision 16: a tool's schema may
// change under a run), so the names are checked here, against MCP's
// current schemas and the host functions' own lists.
fn argument_problems(mcp: &dyn Mcp, sites: &[(String, String, Option<Value>, bool)], local: &[String]) -> Vec<String> {
    let mut problems = Vec::new();
    for (at, function, with, once) in sites {
        if *once && matches!(function.as_str(), "llm_compose" | "filter_relevant" | "choose") {
            problems.push(format!("{at}/metadata/delivery: model adapters do not yet support at_most_once"));
        }
        if local.contains(function) {
            continue;
        }
        let Some((takes, needs)) = arguments(mcp, function) else { continue };
        // The parser only accepts an object of arguments.
        let given: Vec<String> = with.iter().filter_map(Value::as_object).flat_map(|a| a.keys().cloned()).collect();
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
    problems
}

fn invalid(problems: &[String]) -> String {
    format!("invalid workflow:\n{}", problems.iter().map(|p| format!("  - {p}")).collect::<Vec<_>>().join("\n"))
}

/// A definition in the engine's dialect, checked against what this agent can
/// call, with no registry: the evals workbench's (crates/evals, design 07).
/// Saving and activating use `gate`, against the engine's registry.
pub fn check(mcp: &dyn Mcp, definition: &str) -> Result<Workflow, String> {
    struct Known<'a>(&'a dyn Mcp);
    impl ows::model::Catalog for Known<'_> {
        fn events(&self) -> bool {
            true
        }
        fn has(&self, name: &str) -> bool {
            !matches!(name, "confirm_request" | "review" | "resolve_review") && effect(self.0, name).is_some()
        }
    }
    let wf = ows::model::parse_yaml(definition).map_err(|e| e.to_string())?;
    ows::model::validate(&wf, &Known(mcp)).map_err(|e| e.to_string())?;
    let sites: Vec<(String, String, Option<Value>, bool)> = ows::model::call_sites(&wf)
        .into_iter()
        .map(|s| (s.reference, s.function, s.with, s.delivery == Delivery::AtMostOnce))
        .collect();
    let local: Vec<String> = wf.uses.functions.iter().map(|(n, _)| n.clone()).collect();
    let problems = argument_problems(mcp, &sites, &local);
    if problems.is_empty() { Ok(wf) } else { Err(invalid(&problems)) }
}

// Every task of `wf` with its reference: the top level, reusable functions
// and extensions, and every nested list.
pub(crate) fn each_task(wf: &Workflow, visit: &mut dyn FnMut(&Task, &str)) {
    fn list(tasks: &TaskList, at: &str, visit: &mut dyn FnMut(&Task, &str)) {
        for (i, (name, t)) in tasks.iter().enumerate() {
            task(t, &format!("{at}/{i}/{name}"), visit);
        }
    }
    fn task(t: &Task, at: &str, visit: &mut dyn FnMut(&Task, &str)) {
        visit(t, at);
        match &t.kind {
            Kind::Do(tasks) | Kind::For { tasks, .. } => list(tasks, &format!("{at}/do"), visit),
            Kind::Fork { branches, .. } => list(branches, &format!("{at}/fork/branches"), visit),
            Kind::Try { tasks, catch } => {
                list(tasks, &format!("{at}/try"), visit);
                if let Some(handler) = &catch.tasks {
                    list(handler, &format!("{at}/catch/do"), visit);
                }
            }
            Kind::Listen(listen) => {
                if let Some(each) = &listen.foreach {
                    list(&each.tasks, &format!("{at}/foreach/do"), visit);
                }
            }
            _ => {}
        }
    }
    for (name, f) in &wf.uses.functions {
        task(f, &format!("/use/functions/{name}"), visit);
    }
    for (i, (name, e)) in wf.uses.extensions.iter().enumerate() {
        for (key, tasks) in [("before", &e.before), ("after", &e.after)] {
            if let Some(tasks) = tasks {
                list(tasks, &format!("/use/extensions/{i}/{name}/{key}"), visit);
            }
        }
    }
    list(&wf.tasks, "/do", visit);
}

/// What an agent version may not contain (design 04, the gate, R2-M4): a
/// schedule (activation would arm engine triggers that fire outside the
/// supervisor, with no host data: a task's definition is the host's,
/// written by `schedule_task`, design 06), its own breaker policy
/// (decision 3), `emit` (design 01, 17), `run: workflow` (its version
/// resolution picks versions the agent never named), the reserved contexts'
/// functions (the guard round's, `fire_scheduled`) and a `start_child` of a
/// task's definition by a literal name. The engine's `call: start_child {
/// name, input, host? }` stays allowed: it starts the active version of a
/// name the YAML names, with no host data, or with this run's unchanged
/// (`host: inherit`): how a workflow starts a loop, its parameters its
/// request (design 09). An activated workflow is trusted code; the guard on
/// the model's calls is the boundary.
fn denied(wf: &Workflow) -> Vec<String> {
    let mut problems = Vec::new();
    if wf.schedule.is_some() {
        problems.push(
            "/schedule: a version of yours cannot declare a schedule (schedule or metadata.ouro.at); schedule_task starts it on time"
                .to_owned(),
        );
    }
    if wf.breaker != ouro_runtime::ows::model::Breaker::Inherit {
        problems.push(
            "/document/metadata/ouro/breaker: remove `metadata.ouro.breaker`: your versions use the runtime's breaker"
                .into(),
        );
    }
    each_task(wf, &mut |task, at| {
        match &task.kind {
        Kind::Emit(_) => problems.push(format!("{at}/emit: a version of yours cannot emit events")),
        Kind::Run(run) if matches!(run.process, ouro_runtime::ows::model::Process::Workflow { .. }) => problems.push(
            format!("{at}/run: `run: workflow` is not available to your versions; start a child with `call: start_child` (name, input; host: inherit for a loop) and join it with listen"),
        ),
        Kind::Call { function, .. } if crate::functions::host_function(function).is_some_and(|f| f.context == "guard") => {
            problems.push(format!("{at}/call: `{function}` belongs to the reserved guard round"))
        }
        Kind::Call { function, .. } if crate::functions::host_function(function).is_some_and(|f| crate::functions::RESERVED_CONTEXTS.contains(&f.context)) => {
            problems.push(format!("{at}/call: `{function}` belongs to the runtime's scheduled tasks"))
        }
        // A literal task name only: one built by an expression is refused at
        // run time, by `fire_scheduled`'s root check (design 06, decision 5).
        Kind::Call { function, with: Some(with) }
            if function == "start_child"
                && with.get("name").and_then(Value::as_str).is_some_and(|n| n.starts_with(crate::schedule::TASK_PREFIX)) =>
        {
            problems.push(format!("{at}/call: `start_child` cannot start a scheduled task's definition"))
        }
        _ => {}
    }
    });
    problems
}

/// A definition that passed the gate, with the warnings for its author.
pub struct Gated {
    pub wf: Workflow,
    pub warnings: Vec<String>,
}

/// The gate a save and an activation pass (design 04, R-H1): the name is not
/// reserved; the engine validates the definition against its dialect and
/// the registry now; the document is named as saved; `signal` declares no
/// route and is no loop; nothing on the deny-list; every call's argument
/// names; a guarded round wherever `agent_turn` is; the metadata a launch
/// reads. Flows that are legal but usually a mistake come back as warnings,
/// never refused (R2-L4).
pub fn gate(engine: &Engine, name: &str, definition: &str) -> anyhow::Result<Gated> {
    refuse_reserved(name)?;
    let wf = engine.runtime.validate(definition).map_err(|e| anyhow::anyhow!("invalid workflow: {e:#}"))?;
    anyhow::ensure!(
        wf.document.name == name,
        "document.name is `{}` but the workflow is saved as `{name}`; make them the same",
        wf.document.name
    );
    if name == crate::supervisor::SIGNAL_NAME {
        let metadata = workflow_metadata(&wf);
        anyhow::ensure!(metadata.get("route").is_none(), "the signal dispatcher cannot route to itself");
        anyhow::ensure!(metadata.get("loop").is_none(), "the signal dispatcher is not a loop: remove metadata.loop");
    }
    let sites: Vec<(String, String, Option<Value>, bool)> = ouro_runtime::ows::model::call_sites(&wf)
        .into_iter()
        .map(|s| (s.reference, s.function, s.with, s.delivery == ouro_runtime::Delivery::AtMostOnce))
        .collect();
    let local: Vec<String> = wf.uses.functions.iter().map(|(n, _)| n.clone()).collect();
    let mut problems = denied(&wf);
    problems.extend(crate::reflexes::problems(&wf));
    problems.extend(argument_problems(&*engine.deps.mcp, &sites, &local));
    anyhow::ensure!(problems.is_empty(), "{}", invalid(&problems));
    require_guarded_turns(&wf)?;
    require_metadata(&wf)?;
    let warnings = warnings(&wf);
    Ok(Gated { wf, warnings })
}

/// The gate's rules that need no registry: the deny-list, the guarded
/// round wherever `agent_turn` is, and the metadata a launch reads. The
/// authoring skill's examples are held to them (tests/flows.rs).
pub fn agent_rules(wf: &Workflow) -> anyhow::Result<()> {
    let mut problems = denied(wf);
    problems.extend(crate::reflexes::problems(wf));
    anyhow::ensure!(problems.is_empty(), "{}", invalid(&problems));
    require_guarded_turns(wf)?;
    require_metadata(wf)
}

// The model's calls go through the round's guard: a workflow that takes
// agent turns starts the guard child (`start_guard`) on each round's calls,
// whatever its kind. The check is structural (it does not prove the round
// runs the calls under the verdicts); `tool_once`/`tool_repeat` still
// enforce a verdict for a call that needs one, so a stored definition that
// skips the round stays guarded.
fn require_guarded_turns(wf: &Workflow) -> anyhow::Result<()> {
    let sites = ouro_runtime::ows::model::call_sites(wf);
    anyhow::ensure!(
        !sites.iter().any(|s| s.function == "agent_turn") || sites.iter().any(|s| s.function == "start_guard"),
        "a workflow that calls agent_turn requires a `call: start_guard` round before its tools run: copy agent-loop's guarded round (read_workflow agent-loop); the host still enforces a verdict for every call that needs one"
    );
    Ok(())
}

// Flows that are legal but usually a mistake, for the author: a task a jump
// leads to that also runs right after the task before it, because that one
// does not jump (a "nothing new" text after the real one overwrites it).
// A shared tail does this on purpose, so it is a warning, not a problem.
// And a `choose` followed by its `switch` whose cases miss one of its
// options (with no default case) or name one it doesn't have; only literal
// options and `"label"` strings in `when` are compared, jq is not parsed.
pub fn warnings(wf: &Workflow) -> Vec<String> {
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

// ── 4. routes, loops and the model's parameters ─────────────────────────────
// Active versions replace built-ins by name (#55). A route (`metadata.route`)
// is declared by the workflow it routes to: the dispatcher's `route_signal`
// starts it as the signal's primary run, and it gathers its own context
// (`signal_input`, `loop_setup`); that is how a channel's flow is one
// workflow. A loop (`metadata.loop`) is one a worker or the primary may
// run instead of `agent-loop`. A workflow that calls the dispatcher's own
// functions is a `signal` override, never a route (design 04, the gate).

/// Metadata lives in document.metadata, the OWF extension point.
pub fn workflow_metadata(wf: &Workflow) -> &Value {
    &wf.definition["document"]["metadata"]
}

/// Whether the definition `name` runs as a loop: `agent-loop` itself, or one
/// that declares `metadata.loop`.
pub fn is_loop_definition(name: &str, definition: &str) -> bool {
    name == crate::agent::AGENT_LOOP_NAME
        || ouro_runtime::ows::model::parse_yaml(definition).is_ok_and(|wf| workflow_metadata(&wf).get("loop").is_some())
}

/// Whether a workflow calls the signal dispatcher's own functions.
pub fn calls_dispatcher(wf: &Workflow) -> bool {
    ouro_runtime::ows::model::call_sites(wf)
        .iter()
        .any(|s| crate::functions::host_function(&s.function).is_some_and(|f| f.context == "signal"))
}

/// Whether a workflow takes agent turns.
pub fn takes_turns(wf: &Workflow) -> bool {
    ouro_runtime::ows::model::call_sites(wf).iter().any(|s| s.function == "agent_turn")
}

/// A workflow the runtime runs on a signal of its own choosing: the
/// dispatcher, the loop, a loop variant or a route (a scheduled task's
/// workflow runs on its own fires). Ported from `main`.
pub fn is_runtime_workflow(name: &str, wf: &Workflow) -> bool {
    built_in(name).is_some()
        || workflow_metadata(wf).get("loop").is_some()
        || workflow_metadata(wf).get("route").is_some()
}

#[derive(Clone, Debug)]
pub struct Route {
    pub when: BTreeMap<String, Vec<String>>,
    pub priority: i64,
}

pub fn route(wf: &Workflow) -> anyhow::Result<Option<Route>> {
    let Some(route) = workflow_metadata(wf).get("route") else { return Ok(None) };
    let when = route
        .get("when")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow::anyhow!("metadata.route.when must be an object"))?;
    anyhow::ensure!(!when.is_empty(), "metadata.route.when must have at least one key");
    let mut fields = BTreeMap::new();
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

impl Route {
    /// Whether a signal of `source` with `payload` matches: every key, the
    /// source or a top-level payload string, one of its values.
    pub fn matches(&self, source: &str, payload: &Value) -> bool {
        self.when.iter().all(|(key, values)| {
            let field = if key == "source" { Some(source) } else { payload.get(key).and_then(Value::as_str) };
            field.is_some_and(|field| values.iter().any(|value| value == field))
        })
    }
}

/// What the confirmation target of a run the runtime starts on a signal is.
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
/// `none`). A route may say the same under `metadata.route`. Unset, the
/// launch decides (functions.rs §5, route and saved launches).
#[derive(Clone, Copy, Debug, Default)]
pub struct AgentStart {
    pub preset: Option<PresetName>,
    pub confirm: Option<Confirm>,
}

// Where the parameters are declared, if anywhere: `metadata.agent`, else a
// route's own.
fn start_block(wf: &Workflow) -> Option<(&'static str, &Value)> {
    let metadata = workflow_metadata(wf);
    if let Some(agent) = metadata.get("agent") {
        return Some(("metadata.agent", agent));
    }
    let route = metadata.get("route")?;
    (route.get("preset").is_some() || route.get("confirm").is_some()).then_some(("metadata.route", route))
}

pub fn agent_start(wf: &Workflow) -> anyhow::Result<AgentStart> {
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

fn require_metadata(wf: &Workflow) -> anyhow::Result<()> {
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
    // The dispatcher's functions serve the signal's own run: a route runs
    // as the signal's primary run, which has none of them.
    anyhow::ensure!(
        !(calls_dispatcher(wf) && (metadata.get("route").is_some() || metadata.get("agent").is_some())),
        "a workflow that calls the dispatcher's functions (primary_loop, route_signal, …) is a `signal` override: it declares no metadata.route and no metadata.agent; a route is a workflow the dispatcher starts as the signal's primary run (a copy of agent-loop that declares the route, for one)"
    );
    agent_start(wf)?;
    route(wf)?;
    Ok(())
}

/// A name's active version as the engine holds it, for routing and the loop
/// catalog: its text parsed, and whether its breaker or an operator set it
/// aside.
pub struct ActiveVersion {
    pub name: String,
    pub version: u64,
    pub wf: Workflow,
    pub set_aside: bool,
}

/// The active version of every name the agent has one of, built-ins first
/// (`guard` left out: it is never routed to or run as a loop).
pub async fn active_versions(engine: &Engine) -> anyhow::Result<Vec<ActiveVersion>> {
    let mut active = Vec::new();
    for name in names(&engine.deps.db)? {
        if name == GUARD_NAME {
            continue;
        }
        let definitions = engine.runtime.definitions(&name).await?;
        let Some(d) = definitions.active.and_then(|v| definitions.versions.get(&v)) else { continue };
        let Ok(wf) = ouro_runtime::ows::model::parse_yaml(&d.yaml) else { continue };
        active.push(ActiveVersion { name, version: d.version, wf, set_aside: definitions.tripped.is_some() });
    }
    Ok(active)
}

/// The routes a signal of `source` with `payload` matches, the one to start
/// first first: the most specific (most keys), then the highest priority,
/// then the name. Activation refuses ties at equal specificity; two
/// concurrent activations can both pass (R-L7), and the name breaks that tie.
pub async fn signal_routes(engine: &Engine, source: &str, payload: &Value) -> anyhow::Result<Vec<(String, u64)>> {
    let mut matches = Vec::new();
    for active in active_versions(engine).await? {
        if active.name == crate::supervisor::SIGNAL_NAME {
            continue;
        }
        let Ok(Some(route)) = route(&active.wf) else { continue };
        if route.matches(source, payload) {
            matches.push((route.when.len(), route.priority, active.name, active.version));
        }
    }
    matches.sort_by(|a, b| (b.0, b.1, &b.2).cmp(&(a.0, a.1, &a.2)));
    Ok(matches.into_iter().map(|(_, _, name, version)| (name, version)).collect())
}

// Two routes at the same specificity whose values overlap need distinct
// priorities: the dispatcher never decides a tie by a model.
async fn require_route_priority(engine: &Engine, name: &str, wf: &Workflow) -> anyhow::Result<()> {
    let Some(candidate) = route(wf)? else { return Ok(()) };
    for active in active_versions(engine).await? {
        if active.name == name {
            continue;
        }
        let Some(existing) = route(&active.wf)? else { continue };
        let overlaps = candidate.when.iter().all(|(key, values)| {
            existing.when.get(key).is_none_or(|others| values.iter().any(|value| others.contains(value)))
        });
        anyhow::ensure!(
            candidate.when.len() != existing.when.len() || !overlaps || candidate.priority != existing.priority,
            "route `{name}` conflicts with `{}` at equal specificity; set distinct metadata.route.priority values to choose a winner",
            active.name
        );
    }
    Ok(())
}

/// The loops a worker or the primary may run, one line each
/// (`metadata.loop.description`), from the active versions (R2-L7): the
/// built-in `agent-loop` (an active override replaces its line) and every
/// name whose active version declares a loop; one its breaker or an
/// operator set aside is marked. A draft never hides an active version.
pub async fn loop_catalog(engine: &Engine) -> anyhow::Result<String> {
    let mut lines = Vec::new();
    for active in active_versions(engine).await? {
        let described = workflow_metadata(&active.wf)["loop"]["description"].as_str().map(str::to_owned);
        let description = match described {
            Some(description) => description,
            None if active.name == crate::agent::AGENT_LOOP_NAME => built_in(&active.name)
                .and_then(|yaml| ouro_runtime::ows::model::parse_yaml(yaml).ok())
                .and_then(|wf| workflow_metadata(&wf)["loop"]["description"].as_str().map(str::to_owned))
                .unwrap_or_default(),
            None => continue,
        };
        let aside = if active.set_aside { " (set aside: the built-in agent-loop runs in its place)" } else { "" };
        lines.push(format!("{}: {description}{aside}", active.name));
    }
    Ok(lines.join("\n"))
}

// ── 5. authoring tools ──────────────────────────────────────────────────────
// Synthetic tools of the agent loop for writing workflows. Saving checks a
// definition (the gate) and stores it as a draft version in the engine's
// catalog; only activation (after the user agreed in the chat, and after a
// dry run tried it) makes it run.

pub const TOOLS: &[&str] = &["save_workflow", "activate_workflow", "list_workflows", "read_workflow"];

// The workflow-author worker's own tools.
pub const AUTHOR_TOOLS: &[&str] = &["workflow_functions", "probe_function", "try_workflow"];

/// The never-activated versions an agent may keep of one name (design 04,
/// decision 8, R2-M3): every version's text is in the engine's one catalog
/// row per name, read by every admission and finish of that name. Activated
/// ones are bounded by the user's confirmations and not counted.
pub const MAX_DRAFTS: usize = 20;

/// The engine issue for pruning never-activated versions.
pub const PRUNING_ISSUE: &str = "Ch4mpl00/ouro#36";

/// How long a dry run may take before it is stopped (`try_workflow`, ported
/// from `main`).
pub const DRY_RUN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

// What a probe returns at most: enough to see a reply's shape, never a
// whole store.
const PROBE_MAX_CHARS: usize = 6000;

/// The answer of a tool whose feature returns in a later step of the engine
/// migration (design 03, decision 9).
pub fn unavailable(tool: &str, step: &str) -> String {
    json!({"ok": false, "error": format!("{tool} is unavailable until the engine migration's {step} step")}).to_string()
}

// How a call of `function` is delivered: at most once for a write the engine
// cannot repeat safely (an MCP tool not marked read-only, the host's own
// at-most-once functions), or as the task's `metadata.ouro.delivery` says.
fn delivery(engine: &Engine, function: &str, declared_once: bool) -> &'static str {
    let once = declared_once
        || crate::functions::AT_MOST_ONCE.contains(&function)
        || (crate::functions::host_function(function).is_none()
            && effect(&*engine.deps.mcp, function) == Some(Effect::Write));
    if once { "at_most_once" } else { "at_least_once" }
}

fn created_at(ms: i64) -> Option<String> {
    chrono::DateTime::from_timestamp_millis(ms).map(iso)
}

pub async fn tool(engine: &Engine, name: &str, args: &Map<String, Value>) -> anyhow::Result<String> {
    let text = |key: &str| {
        args.get(key)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow::anyhow!("{key}: Expected nonempty string"))
    };
    let note = args.get("note").and_then(Value::as_str).filter(|s| !s.is_empty()).map(str::to_owned);
    let version = || match args.get("version") {
        None | Some(Value::Null) => Ok(None),
        Some(v) => {
            v.as_u64().filter(|v| *v > 0).map(Some).ok_or_else(|| anyhow::anyhow!("version: Expected positive integer"))
        }
    };
    let db = &engine.deps.db;
    match name {
        "save_workflow" => {
            let (name, definition) = (text("name")?.to_owned(), text("definition")?.to_owned());
            let gated = gate(engine, &name, &definition)?;
            let digest = ouro_runtime::durable::digest_of(&definition);
            let definitions = engine.runtime.definitions(&name).await?;
            let existing = definitions.versions.values().find(|d| d.digest == digest).map(|d| d.version);
            if existing.is_none() {
                let mut drafts = 0;
                for d in definitions.versions.values().filter(|d| !definitions.activations.contains_key(&d.version)) {
                    if !WorkflowMeta::get(db, &name, &d.digest)?.is_some_and(|m| m.builtin) {
                        drafts += 1;
                    }
                }
                anyhow::ensure!(
                    drafts < MAX_DRAFTS,
                    "`{name}` already holds {drafts} versions that were never activated, the most it keeps; activate one of them, or ask the maintainer to prune them (engine pruning of never-activated versions: {PRUNING_ISSUE})"
                );
            }
            let (record_name, record_digest, record_note) = (name.clone(), digest.clone(), note.clone());
            db.write(move |db| WorkflowMeta::record(db, &record_name, &record_digest, record_note.as_deref(), false))
                .await?;
            let version = match existing {
                Some(version) => version,
                None => engine.runtime.save(&name, &definition).await?.version,
            };
            let wf = &gated.wf;
            let calls: Vec<Value> = ouro_runtime::ows::model::call_sites(wf)
                .into_iter()
                .map(|c| {
                    let once = c.delivery == ouro_runtime::Delivery::AtMostOnce;
                    json!({"reference": c.reference, "function": c.function, "delivery": delivery(engine, &c.function, once)})
                })
                .collect();
            let then = "before the user sees it, then describe in words what it will do, the at-most-once calls above all; activate this version only after they confirm it in the chat.";
            let next = if crate::reflexes::declaration(wf).is_some() {
                format!(
                    "Dry-run it on input its declaration accepts (try_workflow with `input`, the parameters by name; `drafts` for reflexes it starts) {then}"
                )
            } else if is_runtime_workflow(&name, wf) {
                format!(
                    "Dry-run it on a signal of the type it handles (try_workflow with `signal`, and `drafts` for the other drafts of the chain) {then}"
                )
            } else if takes_turns(wf) {
                format!(
                    "Dry-run it on a fire of its task (try_workflow with signal {{source: scheduler, content: the task's prompt, payload: {{workflow: \"{name}\"}}}}) {then}"
                )
            } else {
                format!("Dry-run it (try_workflow) {then}")
            };
            let latest = engine.runtime.definitions(&name).await?;
            let mut reply = json!({
                "ok": true, "name": name, "version": version, "status": status(&latest, version),
                "calls": calls, "next": next,
            });
            if existing.is_some() {
                // The same text is the same version (decision 8, R-M6).
                reply["reused"] = json!("This text is already saved as this version: no new version was made.");
            }
            if !gated.warnings.is_empty() {
                reply["warnings"] = json!(gated.warnings);
            }
            Ok(reply.to_string())
        }
        "activate_workflow" => {
            let name = text("name")?;
            refuse_reserved(name)?;
            let version = version()?.ok_or_else(|| anyhow::anyhow!("version: Required"))?;
            let definitions = engine.runtime.definitions(name).await?;
            let d = definitions
                .versions
                .get(&version)
                .ok_or_else(|| anyhow::anyhow!("no version {version} of workflow `{name}`"))?;
            // An activation lifts an operator's pause: the agent never does
            // (design 04, decision 5).
            if let Some(manual) = definitions.tripped.as_ref().and_then(|t| t.manual.as_ref()) {
                anyhow::bail!(
                    "`{name}` is paused by an operator ({}): ask them to resume it; activating a version would lift the pause",
                    manual.note
                );
            }
            let meta = WorkflowMeta::get(db, name, &d.digest)?;
            let note = note.or_else(|| meta.as_ref().and_then(|m| m.note.clone()));
            // The current built-in text is how the agent undoes an override:
            // always allowed, without the gate or a dry run (decision 3).
            let builtin = built_in(name).is_some_and(|yaml| ouro_runtime::durable::digest_of(yaml) == d.digest);
            let mut runs = "A matching signal";
            if !builtin {
                // Tools may have changed since it was saved.
                let gated = gate(engine, name, &d.yaml)?;
                require_route_priority(engine, name, &gated.wf).await?;
                anyhow::ensure!(
                    meta.as_ref().is_some_and(|m| m.tried_ms.is_some()),
                    "version {version} of `{name}` has not completed a dry run; the workflow-author worker tries it (try_workflow) before it is shown to the user"
                );
                runs = match (is_loop_definition(name, &d.yaml), route(&gated.wf)?.is_some()) {
                    (true, true) => "A matching signal, a loop selecting this name or a task scheduled with it,",
                    (true, false) => "A loop selecting this name, or a task scheduled with it,",
                    (false, true) => "A matching signal",
                    (false, false) => "A task scheduled with it",
                };
            }
            engine.runtime.activate_as(name, version, "agent", note.as_deref()).await?;
            let next = if builtin {
                format!("The built-in `{name}` runs again from now on.")
            } else if built_in(name).is_some() {
                format!(
                    "Every `{name}` run started from now runs this version, the one the dry run tried. Its breaker counts all its runs: three faults in a row set it aside, and the built-in one runs in its place until a version is activated again."
                )
            } else {
                format!(
                    "{runs} started from now runs this version, the one the dry run tried. Three faults of its runs in a row set it aside until a version is activated again."
                )
            };
            Ok(json!({
                "ok": true, "name": name, "version": version, "builtin": builtin,
                "next": format!("{next} Activating clears a tripped breaker and starts a fresh count, also for a version that tripped before."),
            })
            .to_string())
        }
        "list_workflows" => {
            let mut workflows = Vec::new();
            for name in names(db)? {
                let definitions = engine.runtime.definitions(&name).await?;
                // A meta row whose engine save never happened is an orphan.
                let Some((&latest, d)) = definitions.versions.iter().next_back() else { continue };
                let meta = WorkflowMeta::get(db, &name, &d.digest)?;
                let trip = definitions.tripped.as_ref();
                // A reflex's declaration and what it does, the active
                // version's apart from the latest's (design 10, R6).
                let active = definitions.active.and_then(|v| definitions.versions.get(&v));
                let active_reflex = match active {
                    Some(a) => crate::reflexes::info(engine, &name, &a.yaml).await,
                    None => None,
                };
                let latest_reflex = crate::reflexes::info(engine, &name, &d.yaml).await;
                workflows.push(json!({
                    "name": name, "latest_version": latest, "status": status(&definitions, latest),
                    "active_version": definitions.active,
                    "builtin": meta.as_ref().is_some_and(|m| m.builtin)
                        || built_in(&name).is_some_and(|yaml| ouro_runtime::durable::digest_of(yaml) == d.digest),
                    "tried": meta.as_ref().is_some_and(|m| m.tried_ms.is_some()),
                    "tripped": trip.is_some_and(|t| t.breaker), "paused": trip.is_some_and(|t| t.manual.is_some()),
                    "note": meta.as_ref().and_then(|m| m.note.clone()),
                    "created_at": meta.as_ref().and_then(|m| created_at(m.created_ms)),
                    "active_reflex": active_reflex, "latest_reflex": latest_reflex,
                }));
            }
            Ok(json!({"count": workflows.len(), "workflows": workflows}).to_string())
        }
        "read_workflow" => {
            let name = text("name")?;
            if name == GUARD_NAME {
                return Ok(json!({"name": name, "found": true, "version": null, "status": "built-in",
                    "builtin": true, "definition": GUARD})
                .to_string());
            }
            let definitions = engine.runtime.definitions(name).await?;
            // The version asked for; else the one that runs; else the newest.
            let chosen = match version()? {
                Some(version) => definitions.versions.get(&version),
                None => definitions
                    .active
                    .and_then(|v| definitions.versions.get(&v))
                    .or_else(|| definitions.versions.values().next_back()),
            };
            let Some(d) = chosen else { return Ok(json!({"name": name, "found": false}).to_string()) };
            let meta = WorkflowMeta::get(db, name, &d.digest)?;
            let trip = definitions.tripped.as_ref();
            Ok(json!({
                "name": name, "found": true, "version": d.version, "status": status(&definitions, d.version),
                "builtin": meta.as_ref().is_some_and(|m| m.builtin)
                    || built_in(name).is_some_and(|yaml| ouro_runtime::durable::digest_of(yaml) == d.digest),
                "tried": meta.as_ref().is_some_and(|m| m.tried_ms.is_some()),
                "note": meta.as_ref().and_then(|m| m.note.clone()),
                "active_version": definitions.active, "latest_version": definitions.versions.keys().next_back(),
                "tripped": trip.is_some_and(|t| t.breaker), "paused": trip.is_some_and(|t| t.manual.is_some()),
                "definition": d.yaml,
            })
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
    let tools: Vec<ToolDef> =
        model_tools(mcp).into_iter().filter(|t| !crate::functions::WITHHELD.contains(&t.name.as_str())).collect();
    let effect_of = |name: &str| match effect(mcp, name) {
        Some(Effect::Read) => "read",
        _ => "write",
    };
    let runtime_functions = || {
        crate::functions::HOST_FUNCTIONS
            .iter()
            .chain(crate::functions::ENGINE_FUNCTIONS)
            .filter(|f| !crate::functions::RESERVED_CONTEXTS.contains(&f.context))
            .map(|f| (*f, f.context))
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
        let crate::functions::HostFunction { name, takes, needs, description, .. } = function;
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
        crate::functions::host_function(name).is_none_or(|f| f.context == "workflow"),
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
        crate::functions::host_function(name).is_none(),
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

// ── 6. the evals workbench's adapter ────────────────────────────────────────
// The optimizer (crates/evals) runs candidate workflows in memory on the
// engine's interpreter (`ows::run::run`, design 07): the workflow steps and
// MCP calls of this engine, with no admitted run behind them (no agent
// turns, no children, no events). Not the agent's running path.

/// The workflow steps and MCP calls of an engine, for an isolated in-memory
/// host such as the optimizer workbench. It grants no admitted context: the
/// caller restricts what may run for real.
pub struct Standalone {
    engine: std::sync::Arc<Engine>,
    root: Scope,
}

/// `engine`'s steps for an in-memory host, traced under `root`.
pub fn standalone(
    engine: &std::sync::Arc<Engine>,
    root: &std::sync::Arc<dyn crate::tracing::Span>,
) -> std::sync::Arc<Standalone> {
    std::sync::Arc::new(Standalone { engine: engine.clone(), root: root.clone() })
}

impl Standalone {
    /// What `name` does, or None when a standalone run cannot call it: a host
    /// function outside the `workflow` context needs an admitted run.
    pub fn effect(&self, name: &str) -> Option<Effect> {
        let function = crate::functions::host_function(name);
        if function.is_some_and(|f| f.context != "workflow") {
            return None;
        }
        effect(&*self.engine.deps.mcp, name)
    }
}

// How far a step's error says it got, as the interpreter records it.
fn failure(error: ouro_runtime::FunctionError) -> ows::durable::CallFailure {
    let error = serde_json::to_value(&error)
        .ok()
        .and_then(|v| serde_json::from_value(v).ok())
        .unwrap_or_else(|| ows::run::WorkflowError::runtime(error.to_string()));
    ows::durable::CallFailure::answer(error)
}

#[async_trait::async_trait]
impl ows::run::Functions for Standalone {
    fn has(&self, name: &str) -> bool {
        self.effect(name).is_some()
    }

    // A write is bound at most once, as the agent's registry binds an MCP
    // write (design 01, decision 8).
    fn at_most_once(&self, name: &str, _: &Value) -> bool {
        self.effect(name) == Some(Effect::Write)
    }

    async fn invoke(
        &self,
        name: &str,
        args: Value,
        info: &ows::run::CallInfo<'_>,
    ) -> Result<Value, ows::durable::CallFailure> {
        use ows::durable::CallFailure;
        use ows::run::WorkflowError;
        if crate::functions::host_function(name).is_some() {
            if info.delivery == Delivery::AtMostOnce && name != "set_memory" {
                return Err(CallFailure::NotExecuted(WorkflowError::validation(
                    "model adapters do not yet support at_most_once",
                )));
            }
            let label = info.reference.rsplit('/').next().unwrap_or(name);
            return crate::functions::run_step(&self.engine, name, &args, &self.root, label, info.cancel)
                .await
                .map_err(failure);
        }
        let Value::Object(args) = args else {
            return Err(CallFailure::NotExecuted(WorkflowError::validation(format!(
                "{name}: arguments must be an object"
            ))));
        };
        let raw =
            self.engine.deps.mcp.call_tool_once(name, args, info.cancel).await.map_err(|failure| match failure {
                crate::mcp::ToolCallFailure::NotExecuted(e) => {
                    CallFailure::NotExecuted(WorkflowError::communication(503, e.to_string()))
                }
                crate::mcp::ToolCallFailure::Unknown(e) => {
                    CallFailure::Unknown(WorkflowError::communication(503, e.to_string()))
                }
            })?;
        if raw.starts_with("[tool error]") {
            return Err(CallFailure::Response(WorkflowError::communication(500, raw)));
        }
        Ok(parse_tool_json(&raw))
    }

    async fn call(
        &self,
        name: &str,
        args: Value,
        info: &ows::run::CallInfo<'_>,
    ) -> Result<Value, ows::run::WorkflowError> {
        self.invoke(name, args, info).await.map_err(|failure| failure.error().clone())
    }
}
