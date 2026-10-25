// The workflows the agent runs and the tools for authoring them:
// 1. the built-in definitions (engine definitions at boot, runs.rs §7);
// 2. model steps (composition, choice options, input rendering);
// 3. what a workflow can call, checked before saving; 4. authoring tools:
// saving, activating and dry runs come back with the versions and sandbox
// steps (design 03, decision 9); 5. the evals workbench's in-memory adapter.
//
// Every run is an engine run: the registry the runs call is functions.rs,
// the runtime wiring runs.rs.
use crate::errors::is_aborted;
use crate::{
    agent::{Effect, Engine},
    generation::{Observation, generation},
    mcp::{Mcp, model_tools},
    models::PresetName,
    providers::{self, CompletionParams, ToolDef},
    session::{EnvData, iso},
    skills::append_patch,
    tracing::{GenerationEnd, Scope},
};
use owf::durable::Delivery;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tokio_util::sync::CancellationToken;

// ── 1. built-in definitions ─────────────────────────────────────────────────

pub const GUARD_NAME: &str = "guard";
pub const GUARD: &str = include_str!("../workflows/guard.yaml");

/// The definitions the runtime itself runs, saved and activated at boot.
pub fn builtins() -> Vec<(&'static str, &'static str)> {
    vec![
        (crate::supervisor::SIGNAL_NAME, crate::supervisor::SIGNAL),
        (crate::agent::AGENT_LOOP_NAME, crate::agent::AGENT_LOOP),
        (GUARD_NAME, GUARD),
    ]
}

pub fn built_in(name: &str) -> Option<&'static str> {
    builtins().into_iter().find(|(n, _)| *n == name).map(|(_, yaml)| yaml)
}

/// The names a workflow the agent saves can never take (design 01, decision 17).
pub const RESERVED: &[&str] = &["signal", "agent-loop", "guard", "recovery"];

/// The loops a worker may run, one line each (`metadata.loop.description`):
/// the built-in ones until the versions step.
pub fn loop_catalog() -> String {
    builtins()
        .into_iter()
        .filter_map(|(name, yaml)| {
            let wf = ouro_runtime::ows::model::parse_yaml(yaml).ok()?;
            let description = wf.definition["document"]["metadata"]["loop"]["description"].as_str()?.to_owned();
            Some(format!("{name}: {description}"))
        })
        .collect::<Vec<_>>()
        .join("\n")
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
            !matches!(name, "confirm_request" | "review" | "resolve_review") && effect(self.0, name).is_some()
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

// ── 4. authoring tools ──────────────────────────────────────────────────────

pub const TOOLS: &[&str] = &["save_workflow", "activate_workflow", "list_workflows", "read_workflow"];

// The workflow-author worker's own tools.
pub const AUTHOR_TOOLS: &[&str] = &["workflow_functions", "probe_function", "try_workflow"];

// What a probe returns at most: enough to see a reply's shape, never a
// whole store.
const PROBE_MAX_CHARS: usize = 6000;

/// The answer of a tool whose feature returns in a later step of the engine
/// migration (design 03, decision 9).
pub fn unavailable(tool: &str, step: &str) -> String {
    json!({"ok": false, "unavailable": true,
        "error": format!("{tool} is unavailable until the engine migration's {step} step")})
    .to_string()
}

pub async fn tool(engine: &Engine, name: &str, args: &Map<String, Value>) -> anyhow::Result<String> {
    let text = |key: &str| {
        args.get(key)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow::anyhow!("{key}: Expected nonempty string"))
    };
    match name {
        "save_workflow" | "activate_workflow" => Ok(unavailable(name, "versions")),
        // The engine's catalog: the built-ins until the versions step.
        "list_workflows" => {
            let mut workflows = Vec::new();
            for (name, _) in builtins() {
                let definitions = engine.runtime.definitions(name).await?;
                workflows.push(json!({"name": name, "latest_version": definitions.versions.keys().max(),
                    "active_version": definitions.active, "status": "built-in"}));
            }
            Ok(json!({"count": workflows.len(), "workflows": workflows}).to_string())
        }
        "read_workflow" => {
            let name = text("name")?;
            Ok(match built_in(name) {
                Some(definition) => {
                    let active = engine.runtime.definitions(name).await?.active;
                    json!({"name":name,"found":true,"version":active,"status":"built-in","definition":definition})
                }
                None => json!({"name": name, "found": false}),
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
            .filter(|f| f.context != "guard")
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

// ── 5. the evals workbench's adapter ────────────────────────────────────────
// The optimizer (crates/evals) runs candidate workflows in memory on the
// `owf` interpreter until the step that removes it: the workflow steps and
// MCP calls of this engine, with no admitted run behind them (no agent
// turns, no children, no events). Not the agent's running path.

struct Standalone {
    engine: std::sync::Arc<Engine>,
    root: Scope,
}

fn owf_error(error: ouro_runtime::FunctionError) -> owf::run::WorkflowError {
    serde_json::to_value(&error)
        .ok()
        .and_then(|v| serde_json::from_value(v).ok())
        .unwrap_or_else(|| owf::run::WorkflowError::runtime(error.to_string()))
}

// How far a step's error says it got, as the interpreter records it.
fn owf_failure(error: owf::run::WorkflowError) -> owf::durable::CallFailure {
    use owf::durable::CallFailure;
    match error.type_.as_str() {
        ouro_runtime::CALL_NOT_EXECUTED => CallFailure::NotExecuted(error),
        ouro_runtime::CALL_UNKNOWN => CallFailure::Unknown(error),
        ouro_runtime::CALL_INTERRUPTED => CallFailure::Interrupted(error),
        _ => CallFailure::Response(error),
    }
}

#[async_trait::async_trait]
impl owf::run::Functions for Standalone {
    fn effect(&self, name: &str) -> Option<owf::run::Effect> {
        let function = crate::functions::host_function(name);
        if function.is_some_and(|f| f.context != "workflow") {
            return None;
        }
        match effect(&*self.engine.deps.mcp, name)? {
            Effect::Read => Some(owf::run::Effect::Read),
            Effect::Write => Some(owf::run::Effect::Write),
        }
    }

    async fn invoke(
        &self,
        name: &str,
        args: Value,
        info: &owf::run::CallInfo<'_>,
    ) -> Result<Value, owf::durable::CallFailure> {
        use owf::durable::CallFailure;
        if crate::functions::host_function(name).is_some() {
            if info.delivery == Delivery::AtMostOnce && name != "set_memory" {
                return Err(CallFailure::NotExecuted(owf::run::WorkflowError::validation(
                    "model adapters do not yet support at_most_once",
                )));
            }
            let label = info.reference.rsplit('/').next().unwrap_or(name);
            return crate::functions::run_step(&self.engine, name, &args, &self.root, label, info.cancel)
                .await
                .map_err(|e| owf_failure(owf_error(e)));
        }
        let Value::Object(args) = args else {
            return Err(CallFailure::NotExecuted(owf::run::WorkflowError::validation(format!(
                "{name}: arguments must be an object"
            ))));
        };
        let raw =
            self.engine.deps.mcp.call_tool_once(name, args, info.cancel).await.map_err(|failure| match failure {
                crate::mcp::ToolCallFailure::NotExecuted(e) => {
                    CallFailure::NotExecuted(owf::run::WorkflowError::communication(503, e.to_string()))
                }
                crate::mcp::ToolCallFailure::Unknown(e) => {
                    CallFailure::Unknown(owf::run::WorkflowError::communication(503, e.to_string()))
                }
            })?;
        if raw.starts_with("[tool error]") {
            return Err(CallFailure::Response(owf::run::WorkflowError::communication(500, raw)));
        }
        Ok(parse_tool_json(&raw))
    }

    async fn call(
        &self,
        name: &str,
        args: Value,
        info: &owf::run::CallInfo<'_>,
    ) -> Result<Value, owf::run::WorkflowError> {
        self.invoke(name, args, info).await.map_err(|failure| match failure {
            owf::durable::CallFailure::NotExecuted(e)
            | owf::durable::CallFailure::Response(e)
            | owf::durable::CallFailure::Unknown(e)
            | owf::durable::CallFailure::Interrupted(e) => e,
        })
    }
}

/// The workflow steps of `engine` for an isolated in-memory host such as the
/// optimizer workbench, traced under `root`. It grants no admitted context:
/// the caller restricts what may run for real.
pub fn standalone_host(
    engine: &std::sync::Arc<Engine>,
    root: &std::sync::Arc<dyn crate::tracing::Span>,
) -> owf::run::Host {
    owf::run::Host {
        functions: std::sync::Arc::new(Standalone { engine: engine.clone(), root: root.clone() }),
        observer: std::sync::Arc::new(owf::run::Silent),
        clock: std::sync::Arc::new(owf::run::SystemClock),
        limits: owf::run::Limits::default(),
        runtime: json!({"name":"agent-supervisor","version":env!("CARGO_PKG_VERSION")}),
    }
}
