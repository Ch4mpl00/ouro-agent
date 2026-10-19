// Dynamic workflow domain: 1. DSL/validation; 2. variable store;
// 3. compiler; 4. executor; 5. compile/execute/replan facade.
// Only scheduler signals compile. Execution failures never replay a plan.
use crate::{
    agent::{Engine, LoopOptions, synthetic_definitions},
    errors::{check, is_aborted},
    generation::{Observation, generation, messages_json, trace_generation},
    models::PresetName,
    providers::{self, CompletionParams, ToolDef},
    session::{EnvData, Session, iso},
    skills::append_patch,
    tracing::{ContextUpdate, GenerationEnd, GenerationStart, Scope, Span, SpanEnd, SpanKind, SpanStart},
};
use futures::{FutureExt, StreamExt, future::BoxFuture, stream};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::HashSet;
use std::sync::{Arc, LazyLock, Mutex};
use tokio_util::sync::CancellationToken;

// ── 1. DSL / validation ──────────────────────────────────────────────────
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Workflow {
    pub version: u8,
    pub steps: Vec<Step>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum Step {
    #[serde(rename = "tool")]
    Tool {
        tool: String,
        args: Map<String, Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        bind: Option<String>,
    },
    #[serde(rename = "llm_compose")]
    Compose {
        preset: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        skill: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prompt: Option<String>,
        input: Map<String, Value>,
        bind: String,
    },
    #[serde(rename = "llm_agent")]
    Agent {
        preset: String,
        skill: String,
        prompt: String,
        tools: Vec<String>,
        #[serde(rename = "maxIterations")]
        max_iterations: u32,
        bind: String,
    },
    #[serde(rename = "code_agent")]
    Code {
        task: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        data: Option<String>,
        bind: String,
    },
    #[serde(rename = "decide")]
    Decide {
        preset: String,
        items: String,
        prompt: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        threshold: Option<f64>,
        bind: String,
    },
    #[serde(rename = "parallel")]
    Parallel { steps: Vec<Step> },
    #[serde(rename = "terminal")]
    Terminal,
    #[serde(rename = "replan")]
    Replan {
        context: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        note: Option<String>,
    },
}
impl Step {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Tool { .. } => "tool",
            Self::Compose { .. } => "llm_compose",
            Self::Agent { .. } => "llm_agent",
            Self::Code { .. } => "code_agent",
            Self::Decide { .. } => "decide",
            Self::Parallel { .. } => "parallel",
            Self::Terminal => "terminal",
            Self::Replan { .. } => "replan",
        }
    }
    pub fn bind(&self) -> Option<&str> {
        match self {
            Self::Tool { bind, .. } => bind.as_deref(),
            Self::Compose { bind, .. }
            | Self::Agent { bind, .. }
            | Self::Code { bind, .. }
            | Self::Decide { bind, .. } => Some(bind),
            _ => None,
        }
    }
    fn texts(&self) -> Vec<Value> {
        match self {
            Self::Tool { args, .. } => vec![Value::Object(args.clone())],
            Self::Compose { input, prompt, .. } => vec![Value::Object(input.clone()), json!(prompt)],
            Self::Agent { prompt, .. } => vec![json!(prompt)],
            Self::Code { task, data, .. } => vec![json!(task), json!(data)],
            Self::Decide { items, prompt, .. } => vec![json!(items), json!(prompt)],
            _ => Vec::new(),
        }
    }
}
pub struct WorkflowSchema {
    pub known_tools: Vec<String>,
    pub known_skills: Vec<String>,
}
impl WorkflowSchema {
    pub fn new(known_tools: Vec<String>, known_skills: Vec<String>) -> anyhow::Result<Self> {
        anyhow::ensure!(!known_tools.is_empty(), "createWorkflowSchema: knownTools must be non-empty");
        anyhow::ensure!(!known_skills.is_empty(), "createWorkflowSchema: knownSkills must be non-empty");
        Ok(Self { known_tools, known_skills })
    }
    pub fn parse(&self, input: Value, seeds: Option<&[&str]>) -> Result<Workflow, Vec<String>> {
        let mut input = input;
        let mut shape_errors = Vec::new();
        validate_shape(&mut input, self, &mut shape_errors);
        if !shape_errors.is_empty() {
            return Err(shape_errors);
        }
        let serialized = input.to_string();
        let mut de = serde_json::Deserializer::from_str(&serialized);
        let workflow: Workflow = serde_path_to_error::deserialize(&mut de).map_err(|e| {
            vec![format!(
                "at {}: {}",
                if e.path().to_string() == "." { "workflow root".into() } else { e.path().to_string() },
                e.inner()
            )]
        })?;
        let mut errors = Vec::new();
        if workflow.version != 1 {
            errors.push("at version: Invalid literal value, expected 1".into());
        }
        if workflow.steps.is_empty() {
            errors.push("at steps: Array must contain at least 1 element(s)".into());
        }
        for (i, step) in workflow.steps.iter().enumerate() {
            self.validate_step(step, &format!("steps[{i}]"), false, &mut errors);
        }
        if let Some(seeds) = seeds {
            check_bindings(&workflow, seeds, &mut errors);
        }
        if errors.is_empty() { Ok(workflow) } else { Err(errors) }
    }
    fn validate_step(&self, step: &Step, path: &str, leaf: bool, errors: &mut Vec<String>) {
        if leaf && matches!(step, Step::Parallel { .. } | Step::Terminal | Step::Replan { .. }) {
            errors.push(format!("at {path}.kind: only leaf steps allowed inside parallel"));
        }
        let mut nonempty = |value: &str, key: &str| {
            if value.is_empty() {
                errors.push(format!("at {path}.{key}: String must contain at least 1 character(s)"));
            }
        };
        if let Some(bind) = step.bind() {
            nonempty(bind, "bind");
        }
        match step {
            Step::Tool { tool, .. } => {
                if !self.known_tools.contains(tool) {
                    errors.push(format!("at {path}.tool: Invalid enum value. Received '{tool}'"));
                }
            }
            Step::Compose { preset, skill, prompt, .. } => {
                if let Some(prompt) = prompt {
                    nonempty(prompt, "prompt");
                }
                if skill.is_none() && prompt.is_none() {
                    errors.push(format!("at {path}: llm_compose requires either `skill` or `prompt` (or both)"));
                }
                self.check_preset_skill(preset, skill.as_deref(), path, errors);
            }
            Step::Agent { preset, skill, prompt, tools, max_iterations, .. } => {
                nonempty(prompt, "prompt");
                self.check_preset_skill(preset, Some(skill), path, errors);
                if tools.is_empty() {
                    errors.push(format!("at {path}.tools: Array must contain at least 1 element(s)"));
                }
                for (i, tool) in tools.iter().enumerate() {
                    if !self.known_tools.contains(tool) {
                        errors.push(format!("at {path}.tools[{i}]: Invalid enum value. Received '{tool}'"));
                    }
                }
                if !(1..=20).contains(max_iterations) {
                    errors.push(format!("at {path}.maxIterations: Number must be between 1 and 20"));
                }
            }
            Step::Code { task, .. } => nonempty(task, "task"),
            Step::Decide { preset, items, prompt, threshold, .. } => {
                nonempty(items, "items");
                nonempty(prompt, "prompt");
                if preset != "filter" {
                    errors
                        .push(format!("at {path}.preset: Invalid enum value. Expected 'filter', received '{preset}'"));
                }
                if threshold.is_some_and(|t| !t.is_finite() || t <= 0.0 || t >= 1.0) {
                    errors.push(format!("at {path}.threshold: Number must be greater than 0 and less than 1"));
                }
            }
            Step::Parallel { steps } => {
                if steps.len() < 2 {
                    errors.push(format!("at {path}.steps: Array must contain at least 2 element(s)"));
                }
                for (i, child) in steps.iter().enumerate() {
                    self.validate_step(child, &format!("{path}.steps[{i}]"), true, errors);
                }
            }
            Step::Replan { context, note } => {
                if let Some(note) = note {
                    nonempty(note, "note");
                }
                if context.is_empty() {
                    errors.push(format!("at {path}.context: Array must contain at least 1 element(s)"));
                }
                for (i, name) in context.iter().enumerate() {
                    if name.is_empty() {
                        errors.push(format!("at {path}.context[{i}]: String must contain at least 1 character(s)"));
                    }
                }
            }
            Step::Terminal => {}
        }
    }
    fn check_preset_skill(&self, preset: &str, skill: Option<&str>, path: &str, errors: &mut Vec<String>) {
        if PresetName::selectable(preset).is_none() {
            errors.push(format!("at {path}.preset: Invalid enum value. Received '{preset}'"));
        }
        if let Some(skill) = skill
            && !self.known_skills.iter().any(|s| s == skill)
        {
            errors.push(format!("at {path}.skill: Invalid enum value. Received '{skill}'"));
        }
    }
}
// Shape validation precedes deserialization: Option must not accept explicit
// null where Zod permits only an omitted field, and unit variants are strict.
fn validate_shape(input: &mut Value, schema: &WorkflowSchema, errors: &mut Vec<String>) {
    fn kind(v: &Value) -> &'static str {
        match v {
            Value::Null => "null",
            Value::Bool(_) => "boolean",
            Value::Number(_) => "number",
            Value::String(_) => "string",
            Value::Array(_) => "array",
            Value::Object(_) => "object",
        }
    }
    fn issue(errors: &mut Vec<String>, path: &str, message: String) {
        errors.push(format!("at {}: {message}", if path.is_empty() { "workflow root" } else { path }));
    }
    fn field<'a>(
        object: &'a Map<String, Value>,
        key: &str,
        path: &str,
        ty: &str,
        optional: bool,
        nonempty: bool,
        errors: &mut Vec<String>,
    ) -> Option<&'a Value> {
        let Some(value) = object.get(key) else {
            if !optional {
                issue(errors, &format!("{path}.{key}"), "Required".into());
            }
            return None;
        };
        let valid = match ty {
            "string" => value.is_string(),
            "object" => value.is_object(),
            "array" => value.is_array(),
            "number" => value.is_number(),
            _ => false,
        };
        if !valid {
            issue(errors, &format!("{path}.{key}"), format!("Expected {ty}, received {}", kind(value)));
            return None;
        }
        if nonempty && value.as_str().is_some_and(str::is_empty) {
            issue(errors, &format!("{path}.{key}"), "String must contain at least 1 character(s)".into());
        }
        Some(value)
    }
    fn strict(object: &Map<String, Value>, allowed: &[&str], path: &str, errors: &mut Vec<String>) {
        let unknown =
            object.keys().filter(|k| !allowed.contains(&k.as_str())).map(|k| format!("'{k}'")).collect::<Vec<_>>();
        if !unknown.is_empty() {
            issue(errors, path, format!("Unrecognized key(s) in object: {}", unknown.join(", ")));
        }
    }
    fn step(value: &mut Value, path: &str, leaf: bool, schema: &WorkflowSchema, errors: &mut Vec<String>) {
        let Some(object) = value.as_object_mut() else {
            issue(errors, path, format!("Expected object, received {}", kind(value)));
            return;
        };
        let tag = object.get("kind").and_then(Value::as_str).unwrap_or_default().to_owned();
        let allowed_kinds = if leaf {
            vec!["tool", "llm_compose", "llm_agent", "code_agent", "decide"]
        } else {
            vec!["tool", "llm_compose", "llm_agent", "code_agent", "decide", "terminal", "parallel", "replan"]
        };
        if !allowed_kinds.contains(&tag.as_str()) {
            issue(
                errors,
                &format!("{path}.kind"),
                format!(
                    "Invalid discriminator value. Expected {}",
                    allowed_kinds.iter().map(|s| format!("'{s}'")).collect::<Vec<_>>().join(" | ")
                ),
            );
            return;
        }
        let allowed = match tag.as_str() {
            "tool" => vec!["kind", "tool", "args", "bind"],
            "llm_compose" => vec!["kind", "preset", "skill", "prompt", "input", "bind"],
            "llm_agent" => vec!["kind", "preset", "skill", "prompt", "tools", "maxIterations", "bind"],
            "code_agent" => vec!["kind", "task", "data", "bind"],
            "decide" => vec!["kind", "preset", "items", "prompt", "threshold", "bind"],
            "parallel" => vec!["kind", "steps"],
            "replan" => vec!["kind", "context", "note"],
            _ => vec!["kind"],
        };
        for key in ["tool", "preset", "skill", "prompt", "bind", "task", "data", "items", "note"] {
            if !allowed.contains(&key) {
                continue;
            }
            let optional = matches!(
                (tag.as_str(), key),
                ("tool", "bind") | ("llm_compose", "skill" | "prompt") | ("code_agent", "data") | ("replan", "note")
            );
            field(object, key, path, "string", optional, key != "data", errors);
        }
        for key in ["args", "input"] {
            if allowed.contains(&key) {
                field(object, key, path, "object", false, false, errors);
            }
        }
        for (key, options) in [
            ("tool", schema.known_tools.clone()),
            ("skill", schema.known_skills.clone()),
            (
                "preset",
                if tag == "decide" {
                    vec!["filter".into()]
                } else {
                    vec!["base".into(), "smart".into(), "smartest".into()]
                },
            ),
        ] {
            if !allowed.contains(&key) {
                continue;
            }
            if let Some(text) = object.get(key).and_then(Value::as_str)
                && !options.iter().any(|s| s == text)
            {
                issue(
                    errors,
                    &format!("{path}.{key}"),
                    format!(
                        "Invalid enum value. Expected {}, received '{text}'",
                        options.iter().map(|s| format!("'{s}'")).collect::<Vec<_>>().join(" | ")
                    ),
                );
            }
        }
        for key in ["tools", "context"] {
            if !allowed.contains(&key) {
                continue;
            }
            if let Some(values) = field(object, key, path, "array", false, false, errors).and_then(Value::as_array) {
                if values.is_empty() {
                    issue(errors, &format!("{path}.{key}"), "Array must contain at least 1 element(s)".into());
                }
                for (i, v) in values.iter().enumerate() {
                    let p = format!("{path}.{key}[{i}]");
                    match v.as_str() {
                        None => issue(errors, &p, format!("Expected string, received {}", kind(v))),
                        Some("") => issue(errors, &p, "String must contain at least 1 character(s)".into()),
                        Some(t) if key == "tools" && !schema.known_tools.iter().any(|s| s == t) => {
                            issue(errors, &p, format!("Invalid enum value. Received '{t}'"))
                        }
                        _ => {}
                    }
                }
            }
        }
        if tag == "llm_agent"
            && let Some(number) =
                field(object, "maxIterations", path, "number", false, false, errors).and_then(Value::as_f64)
        {
            if number.fract() != 0.0 {
                issue(errors, &format!("{path}.maxIterations"), "Expected integer, received float".into());
            }
            if number < 1.0 {
                issue(errors, &format!("{path}.maxIterations"), "Number must be greater than or equal to 1".into());
            }
            if number > 20.0 {
                issue(errors, &format!("{path}.maxIterations"), "Number must be less than or equal to 20".into());
            }
            if (1.0..=20.0).contains(&number) && number.fract() == 0.0 {
                object.insert("maxIterations".into(), json!(number as u32));
            }
        }
        if tag == "decide"
            && let Some(number) =
                field(object, "threshold", path, "number", true, false, errors).and_then(Value::as_f64)
        {
            if number <= 0.0 {
                issue(errors, &format!("{path}.threshold"), "Number must be greater than 0".into());
            }
            if number >= 1.0 {
                issue(errors, &format!("{path}.threshold"), "Number must be less than 1".into());
            }
        }
        if tag == "parallel" {
            if let Some(values) = field(object, "steps", path, "array", false, false, errors).and_then(Value::as_array)
                && values.len() < 2
            {
                issue(errors, &format!("{path}.steps"), "Array must contain at least 2 element(s)".into());
            }
            if let Some(values) = object.get_mut("steps").and_then(Value::as_array_mut) {
                for (i, v) in values.iter_mut().enumerate() {
                    step(v, &format!("{path}.steps[{i}]"), true, schema, errors);
                }
            }
        }
        strict(object, &allowed, path, errors);
    }
    let Some(object) = input.as_object_mut() else {
        issue(errors, "", format!("Expected object, received {}", kind(input)));
        return;
    };
    if object.get("version").and_then(Value::as_f64) != Some(1.0) {
        issue(errors, "version", "Invalid literal value, expected 1".into());
    } else {
        object.insert("version".into(), json!(1));
    }
    if !object.get("steps").is_some_and(Value::is_array) {
        issue(
            errors,
            "steps",
            object
                .get("steps")
                .map(|v| format!("Expected array, received {}", kind(v)))
                .unwrap_or_else(|| "Required".into()),
        );
    }
    if let Some(values) = object.get_mut("steps").and_then(Value::as_array_mut) {
        if values.is_empty() {
            issue(errors, "steps", "Array must contain at least 1 element(s)".into());
        }
        for (i, v) in values.iter_mut().enumerate() {
            step(v, &format!("steps[{i}]"), false, schema, errors);
        }
    }
    strict(object, &["version", "steps"], "", errors);
}

fn check_bindings(workflow: &Workflow, seeds: &[&str], errors: &mut Vec<String>) {
    let mut bound: Vec<String> = seeds.iter().map(|s| s.to_string()).collect();
    let read = |step: &Step, path: &str, visible: &[String], siblings: &HashSet<String>, errors: &mut Vec<String>| {
        for reference in placeholder_paths(&Value::Array(step.texts())) {
            let root = reference.split('.').next().unwrap_or_default();
            if visible.iter().any(|n| n == root) {
                continue;
            }
            errors.push(if siblings.contains(root){format!("at {path}: ${{{reference}}} reads \"{root}\", which another step of the same parallel binds — they run at once; bind it in an earlier step")}else{format!("at {path}: ${{{reference}}} reads \"{root}\", which no earlier step binds (available: {})",visible.join(", "))});
        }
    };
    let bind = |step: &Step, path: &str, bound: &mut Vec<String>, errors: &mut Vec<String>| {
        if let Some(name) = step.bind() {
            if bound.iter().any(|n| n == name) {
                errors.push(format!(
                    "at {path}: bind \"{name}\" is already {}; choose another name",
                    if seeds.contains(&name) { "a reserved name" } else { "bound by an earlier step" }
                ));
            } else {
                bound.push(name.into());
            }
        }
    };
    for (i, step) in workflow.steps.iter().enumerate() {
        let path = format!("steps[{i}]");
        match step {
            Step::Terminal | Step::Replan { .. } => break,
            Step::Parallel { steps } => {
                let before = bound.clone();
                let siblings = steps.iter().filter_map(Step::bind).map(str::to_owned).collect();
                for (j, child) in steps.iter().enumerate() {
                    read(child, &format!("{path}.steps[{j}]"), &before, &siblings, errors);
                }
                for (j, child) in steps.iter().enumerate() {
                    bind(child, &format!("{path}.steps[{j}]"), &mut bound, errors);
                }
            }
            _ => {
                read(step, &path, &bound, &HashSet::new(), errors);
                bind(step, &path, &mut bound, errors);
            }
        }
    }
}

// ── 2. variables ─────────────────────────────────────────────────────────
#[derive(Debug, thiserror::Error)]
pub enum WorkflowError {
    #[error("unbound substitution: ${{{0}}}")]
    Missing(String),
    #[error("duplicate binding: {0}")]
    Duplicate(String),
    #[error("skill not found: {0}")]
    Skill(String),
    #[error("tool {tool} failed: {message}")]
    Tool { tool: String, message: String },
    #[error("llm call failed: {0}")]
    Llm(String),
    #[error("decide failed: {0}")]
    Decision(String),
}
pub fn classify_error(error: &anyhow::Error) -> &'static str {
    match error.downcast_ref::<WorkflowError>() {
        Some(WorkflowError::Missing(_)) => "missing_binding",
        Some(WorkflowError::Duplicate(_)) => "duplicate_binding",
        Some(WorkflowError::Skill(_)) => "skill_not_found",
        Some(WorkflowError::Tool { .. }) => "tool_error",
        Some(WorkflowError::Llm(_)) => "llm_error",
        Some(WorkflowError::Decision(_)) => "decision_error",
        None => "step_failed",
    }
}
#[derive(Default)]
pub struct VariableStore {
    data: Mutex<Map<String, Value>>,
}
impl VariableStore {
    pub fn new(initial: Map<String, Value>) -> Arc<Self> {
        Arc::new(Self { data: Mutex::new(initial) })
    }
    pub fn get(&self, path: &str) -> Result<Value, WorkflowError> {
        let mut parts = path.split('.');
        let data = self.data.lock().unwrap_or_else(|e| e.into_inner());
        let first = parts.next().unwrap_or_default();
        let mut value = data.get(first).cloned().ok_or_else(|| WorkflowError::Missing(path.into()))?;
        for segment in parts {
            value = match &value {
                Value::Object(map) => map.get(segment).cloned(),
                Value::Array(array) if segment == "length" => Some(json!(array.len())),
                Value::Array(array) => segment
                    .parse::<usize>()
                    .ok()
                    .filter(|i| i.to_string() == segment)
                    .and_then(|i| array.get(i).cloned()),
                _ => None,
            }
            .ok_or_else(|| WorkflowError::Missing(path.into()))?;
        }
        Ok(value)
    }
    pub fn set(&self, name: &str, value: Value) -> Result<(), WorkflowError> {
        let mut data = self.data.lock().unwrap_or_else(|e| e.into_inner());
        if data.contains_key(name) {
            return Err(WorkflowError::Duplicate(name.into()));
        }
        data.insert(name.into(), value);
        Ok(())
    }
    pub fn has(&self, path: &str) -> bool {
        self.get(path).is_ok()
    }
    pub fn snapshot(&self) -> Map<String, Value> {
        self.data.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}
static PLACEHOLDER: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\$\{([^}]+)\}").unwrap());
pub fn placeholder_paths(value: &Value) -> Vec<String> {
    match value {
        Value::String(s) => PLACEHOLDER.captures_iter(s).map(|c| c[1].to_owned()).collect(),
        Value::Array(a) => a.iter().flat_map(placeholder_paths).collect(),
        Value::Object(m) => m.values().flat_map(placeholder_paths).collect(),
        _ => Vec::new(),
    }
}
pub fn substitute(value: &Value, store: &VariableStore) -> anyhow::Result<Value> {
    match value {
        Value::String(s) => {
            if let Some(found) = PLACEHOLDER.captures(s)
                && found.get(0).is_some_and(|m| m.start() == 0 && m.end() == s.len())
            {
                return Ok(store.get(&found[1])?);
            }
            let mut out = String::new();
            let mut from = 0;
            for found in PLACEHOLDER.captures_iter(s) {
                let m = found.get(0).unwrap();
                out.push_str(&s[from..m.start()]);
                out.push_str(&text_value(&store.get(&found[1])?));
                from = m.end();
            }
            out.push_str(&s[from..]);
            Ok(out.into())
        }
        Value::Array(a) => Ok(Value::Array(a.iter().map(|v| substitute(v, store)).collect::<anyhow::Result<_>>()?)),
        Value::Object(m) => Ok(Value::Object(
            m.iter().map(|(k, v)| substitute(v, store).map(|v| (k.clone(), v))).collect::<anyhow::Result<_>>()?,
        )),
        _ => Ok(value.clone()),
    }
}
pub fn text_value(value: &Value) -> String {
    value.as_str().map(str::to_owned).unwrap_or_else(|| value.to_string())
}
pub fn substitute_text(text: &str, store: &VariableStore) -> anyhow::Result<String> {
    let value = substitute(&json!(text), store)?;
    Ok(value.as_str().map(str::to_owned).unwrap_or(serde_json::to_string_pretty(&value)?))
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

// ── 3. compiler ──────────────────────────────────────────────────────────
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkflowSignal {
    pub id: i64,
    pub source: String,
    pub content: String,
    #[serde(rename = "envContext")]
    pub env_context: Option<String>,
}
#[derive(Clone, Debug)]
pub struct PriorContext {
    pub pass: usize,
    pub last_pass: bool,
    pub data: Map<String, Value>,
    pub note: Option<String>,
}
#[derive(Clone, Debug)]
pub enum CompileResult {
    Ok { workflow: Workflow, attempts: usize },
    Failed { reason: &'static str, errors: Vec<String>, attempts: usize },
}
pub fn render_tool_signature(tool: &ToolDef) -> String {
    fn short_type(schema: &Value) -> String {
        if let Some(options) = schema["enum"].as_array().filter(|a| !a.is_empty()) {
            return options.iter().map(Value::to_string).collect::<Vec<_>>().join("|");
        }
        match schema["type"].as_str() {
            Some("string") => "string".into(),
            Some("number" | "integer") => "number".into(),
            Some("boolean") => "boolean".into(),
            Some("array") => format!("{}[]", short_type(&schema["items"])),
            Some("object") => "object".into(),
            _ => "any".into(),
        }
    }
    let required = tool.parameters["required"].as_array();
    let params = tool.parameters["properties"]
        .as_object()
        .map(|props| {
            props
                .iter()
                .map(|(name, schema)| {
                    format!(
                        "{name}{}: {}",
                        if required.is_some_and(|r| r.iter().any(|v| v.as_str() == Some(name))) { "" } else { "?" },
                        short_type(schema)
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_default();
    let desc = tool
        .description
        .as_deref()
        .filter(|d| !d.is_empty())
        .map(|d| {
            format!(
                " — {}",
                if d.encode_utf16().count() <= 200 {
                    d.to_owned()
                } else {
                    format!("{}…", crate::activity::head_utf16(d, 199).trim_end())
                }
            )
        })
        .unwrap_or_default();
    format!("- {}({params}){desc}", tool.name)
}
pub fn render_signal_prompt(signal: &WorkflowSignal, env: &EnvData, prior: Option<&PriorContext>) -> String {
    let mut lines = vec![
        "<signal>".into(),
        format!("Source: {}", signal.source),
        "Content:".into(),
        signal.content.clone(),
        "</signal>".into(),
        "".into(),
        "<env>".into(),
        format!("Timezone: {}", env.timezone),
        format!("Now: {}", iso(env.now)),
    ];
    if let Some(email) = env.user_email.as_ref().filter(|s| !s.is_empty()) {
        lines.push(format!("User email: {email}"));
    }
    lines.extend([
        format!(
            "News last read at: {}",
            env.news_last_read_at.as_deref().unwrap_or("never (bootstrap with now - 24h)")
        ),
        format!(
            "Tech last read at: {}",
            env.tech_last_read_at.as_deref().unwrap_or("never (bootstrap with now - 24h)")
        ),
        "</env>".into(),
        "".into(),
    ]);
    if let Some(context) = signal.env_context.as_ref().filter(|s| !s.is_empty()) {
        lines.extend(["<envContext>".into(), context.clone(), "</envContext>".into(), "".into()]);
    }
    if let Some(pc) = prior {
        lines.extend(["<context>".into(),format!("You already ran a gather pass for this signal (replan pass {}). Use what you gathered to decide, and emit the ACTING workflow now.",pc.pass),if pc.last_pass{"This is your LAST pass — you MUST act now; do NOT emit another `replan`."}else{"Emit another `replan` ONLY if you still genuinely lack data to proceed."}.into()]);
        if let Some(note) = pc.note.as_ref().filter(|s| !s.is_empty()) {
            lines.extend(["".into(), format!("Note from your previous pass: {note}")]);
        }
        lines.extend([
            "".into(),
            "Gathered data (also in the store as ${context.<name>} for your steps to reference):".into(),
            serde_json::to_string_pretty(&pc.data).unwrap(),
            "</context>".into(),
            "".into(),
        ]);
    }
    lines.push("Emit a Workflow as JSON matching the DSL. Return ONLY the JSON, no markdown wrapper.".into());
    lines.join("\n")
}
// What a run starts from: the session's environment and the signal. A saved
// workflow (`flows.rs`) gets the same object as its input. `localNow` is the
// same instant on the user's clock, with its offset (UTC when the zone is
// unknown), for the dates a run shows the user.
pub fn signal_input(signal: &WorkflowSignal, env: &EnvData) -> Map<String, Value> {
    let local_now = match env.timezone.parse::<chrono_tz::Tz>() {
        Ok(zone) => env.now.with_timezone(&zone).to_rfc3339_opts(chrono::SecondsFormat::Secs, false),
        Err(_) => env.now.to_rfc3339_opts(chrono::SecondsFormat::Secs, false),
    };
    json!({"env":{"timezone":env.timezone,"now":iso(env.now),"localNow":local_now,"newsLastReadAt":env.news_last_read_at,"techLastReadAt":env.tech_last_read_at,"userEmail":env.user_email,"telegramChatId":env.telegram_chat_id},"signal":{"source":signal.source,"content":signal.content,"id":signal.id}}).as_object().unwrap().clone()
}
pub struct WorkflowRunner {
    pub engine: Arc<Engine>,
    pub schema: WorkflowSchema,
    tools: Vec<ToolDef>,
    pub max_attempts: usize,
    pub max_passes: usize,
}
pub fn create_workflow_runner(engine: Arc<Engine>, known_skills: Vec<String>) -> anyhow::Result<Arc<WorkflowRunner>> {
    let mut tools = crate::mcp::model_tools(&*engine.deps.mcp);
    tools.extend(
        synthetic_definitions()
            .iter()
            .filter(|t| t.name == "set_memory" || crate::schedule::TOOLS.contains(&t.name.as_str()))
            .cloned(),
    );
    let schema = WorkflowSchema::new(tools.iter().map(|t| t.name.clone()).collect(), known_skills)?;
    Ok(Arc::new(WorkflowRunner { engine, schema, tools, max_attempts: 3, max_passes: 3 }))
}
impl WorkflowRunner {
    pub async fn compile(
        &self,
        signal: &WorkflowSignal,
        env: &EnvData,
        parent: &Scope,
        prior: Option<&PriorContext>,
        cancel: &CancellationToken,
    ) -> anyhow::Result<CompileResult> {
        check(cancel)?;
        let Some(skill) = self.engine.deps.skill_store.read_skill("planner").await? else {
            return Ok(CompileResult::Failed {
                reason: "skill_not_found",
                errors: vec!["compiler skill \"planner\" not found".into()],
                attempts: 0,
            });
        };
        let preset = self.engine.deps.presets.compiler.clone();
        let provider = self.engine.resolve_provider(&preset.model)?;
        let mut reference=vec!["<tools>".into(),"Signature format: name(arg: type, opt?: type) — description. Use the EXACT parameter names listed; do not invent aliases.".into()];
        reference.extend(self.tools.iter().map(render_tool_signature));
        reference.extend(["</tools>".into(), "".into(), "<skills>".into()]);
        reference.extend(self.schema.known_skills.iter().map(|name| format!("- {name}")));
        reference.push("</skills>".into());
        let patch = self.engine.deps.skill_store.read_patch("planner").await?.unwrap_or_default();
        let system = append_patch(&format!("{}\n\n{}", skill.body, reference.join("\n")), &patch);
        let mut messages = vec![providers::system(&system), providers::user(&render_signal_prompt(signal, env, prior))];
        let span = parent.span(
            SpanStart::new("planner", SpanKind::Chain)
                .input(json!({"preset":"compiler","model":preset.model}))
                .metadata(json!({"signal_source":signal.source}).as_object().unwrap().clone()),
        );
        let scope: Scope = span.clone();
        let mut last_errors = Vec::new();
        for attempt in 1..=self.max_attempts {
            let start = GenerationStart {
                name: format!("attempt-{attempt}"),
                model: preset.model.clone(),
                input: Some(messages_json(&messages)),
                model_parameters: Some(json!({"response_format":"json_object"}).as_object().unwrap().clone()),
                ..Default::default()
            };
            let result = trace_generation(
                &scope,
                start,
                None,
                cancel,
                |operation_cancel| async {
                    let mut params = CompletionParams {
                        model: preset.model.clone(),
                        messages: &mut messages,
                        reasoning_effort: preset.reasoning_effort,
                        tools: Vec::new(),
                        response_format: Some(json!({"type":"json_object"})),
                        trace: Some(scope.clone()),
                        cancel: operation_cancel,
                    };
                    let completion = provider.complete(&mut params).await?;
                    let text = providers::content(&completion.message).unwrap_or_default().to_owned();
                    let parsed = serde_json::from_str::<Value>(&text)
                        .map_err(|e| vec![format!("invalid JSON: {e}")])
                        .and_then(|v| {
                            self.schema.parse(
                                v,
                                Some(if prior.is_some() { &["env", "signal", "context"] } else { &["env", "signal"] }),
                            )
                        });
                    Ok((text, completion.usage, parsed))
                },
                |(text, usage, parsed)| GenerationEnd {
                    output: Some(json!(text)),
                    usage: usage.clone(),
                    metadata: if parsed.is_ok() {
                        Some(json!({"judge_node":"planner"}).as_object().unwrap().clone())
                    } else {
                        None
                    },
                    ..Default::default()
                },
            )
            .await;
            match result {
                Err(error) => {
                    span.end(
                        SpanEnd::error(error.to_string()).with_output(json!({"reason":"llm_error","attempts":attempt})),
                    );
                    check(cancel)?;
                    return Ok(CompileResult::Failed {
                        reason: "llm_error",
                        errors: vec![error.to_string()],
                        attempts: attempt,
                    });
                }
                Ok((_, _, Ok(workflow))) => {
                    span.end(SpanEnd::output(json!({"attempts":attempt,"ok":true})));
                    return Ok(CompileResult::Ok { workflow, attempts: attempt });
                }
                Ok((text, _, Err(errors))) => {
                    messages.push(providers::assistant(&text));
                    let mut feedback = vec!["Your previous workflow failed validation. Errors:".into()];
                    feedback.extend(errors.iter().map(|e| format!("  - {e}")));
                    feedback.extend([
                        "".into(),
                        "Emit a corrected workflow. Return ONLY the JSON, no markdown wrapper.".into(),
                    ]);
                    messages.push(providers::user(&feedback.join("\n")));
                    last_errors = errors;
                }
            }
        }
        span.end(
            SpanEnd::error("exhausted retries")
                .with_output(json!({"reason":"max_retries","lastErrors":last_errors,"attempts":self.max_attempts})),
        );
        Ok(CompileResult::Failed {
            reason: if last_errors.first().is_some_and(|e| e.starts_with("invalid JSON")) {
                "invalid_json"
            } else {
                "schema_invalid"
            },
            errors: last_errors,
            attempts: self.max_attempts,
        })
    }
}

// ── 4. executor ──────────────────────────────────────────────────────────
#[derive(Clone)]
pub struct ExecContext {
    pub session: Session,
    pub store: Arc<VariableStore>,
    pub scope: Scope,
    pub signal_label: String,
    pub cancel: CancellationToken,
}
#[derive(Clone, Debug)]
pub struct ReplanRequest {
    pub context: Map<String, Value>,
    pub note: Option<String>,
}
pub enum ExecResult {
    Ok { store: Arc<VariableStore>, replan: Option<ReplanRequest> },
    Failed { reason: &'static str, error: anyhow::Error, step_index: usize, step: Step, store: Arc<VariableStore> },
}
struct DispatchResult {
    stop: bool,
    replan: Option<ReplanRequest>,
    output: Value,
}
fn step_metadata(step: &Step) -> Map<String, Value> {
    let value = match step {
        Step::Tool { tool, bind, .. } => json!({"tool":tool,"bind":bind}),
        Step::Compose { preset, skill, bind, .. } => json!({"preset":preset,"skill":skill,"bind":bind}),
        Step::Agent { preset, skill, bind, tools, max_iterations, .. } => {
            json!({"preset":preset,"skill":skill,"bind":bind,"tool_count":tools.len(),"max_iterations":max_iterations})
        }
        Step::Code { bind, data, .. } => json!({"tool":"code_agent","bind":bind,"has_data":data.is_some()}),
        Step::Decide { preset, bind, threshold, .. } => json!({"preset":preset,"bind":bind,"threshold":threshold}),
        Step::Parallel { steps } => json!({"child_count":steps.len()}),
        Step::Replan { context, note } => json!({"context":context,"has_note":note.is_some()}),
        Step::Terminal => json!({}),
    };
    value.as_object().unwrap().clone()
}
fn step_span_kind(step: &Step) -> SpanKind {
    match step {
        Step::Tool { .. } => SpanKind::Tool,
        Step::Agent { .. } | Step::Code { .. } => SpanKind::Agent,
        Step::Terminal => SpanKind::Span,
        _ => SpanKind::Chain,
    }
}
impl WorkflowRunner {
    pub async fn execute(&self, workflow: &Workflow, ctx: &ExecContext) -> anyhow::Result<ExecResult> {
        let root = ctx.scope.span(
            SpanStart::new("runner", SpanKind::Chain)
                .input(json!({"stepCount":workflow.steps.len()}))
                .metadata(json!({"workflow_version":workflow.version}).as_object().unwrap().clone()),
        );
        for (i, step) in workflow.steps.iter().enumerate() {
            if let Err(error) = check(&ctx.cancel) {
                root.end(SpanEnd::error(error.to_string()));
                return Err(error);
            }
            let span = root.span(
                SpanStart::new(format!("step[{i}]:{}", step.kind()), step_span_kind(step))
                    .metadata(step_metadata(step)),
            );
            match self.dispatch(step, ctx, span.clone()).await {
                Ok(result) => {
                    span.end(SpanEnd::output(result.output));
                    if let Some(replan) = result.replan {
                        root.end(SpanEnd::output(
                            json!({"replanAtIndex":i,"carried":replan.context.keys().collect::<Vec<_>>()}),
                        ));
                        return Ok(ExecResult::Ok { store: ctx.store.clone(), replan: Some(replan) });
                    }
                    if result.stop {
                        root.end(SpanEnd::output(json!({"stoppedAtIndex":i,"stoppedBy":step.kind()})));
                        return Ok(ExecResult::Ok { store: ctx.store.clone(), replan: None });
                    }
                }
                Err(error) => {
                    let reason = classify_error(&error);
                    span.end(SpanEnd::error(error.to_string()).with_output(json!({"reason":reason})));
                    root.end(SpanEnd::error(error.to_string()).with_output(json!({"failedAtIndex":i,"reason":reason})));
                    check(&ctx.cancel)?;
                    return Ok(ExecResult::Failed {
                        reason,
                        error,
                        step_index: i,
                        step: step.clone(),
                        store: ctx.store.clone(),
                    });
                }
            }
        }
        root.end(SpanEnd::output(json!({"ranToEnd":true})));
        Ok(ExecResult::Ok { store: ctx.store.clone(), replan: None })
    }
    fn dispatch<'a>(
        &'a self,
        step: &'a Step,
        ctx: &'a ExecContext,
        span: Arc<dyn Span>,
    ) -> BoxFuture<'a, anyhow::Result<DispatchResult>> {
        async move {
            check(&ctx.cancel)?;
            let store = &ctx.store;
            let mut replan = None;
            let mut stop = false;
            let output = match step {
                Step::Tool { tool, args, bind } => {
                    let args = substitute(&Value::Object(args.clone()), store)?
                        .as_object()
                        .cloned()
                        .ok_or_else(|| anyhow::anyhow!("tool args must resolve to an object"))?;
                    span.update(ContextUpdate::input(json!({"tool":tool,"args":args})));
                    let output = if tool == "set_memory" {
                        let key =
                            args.get("key").and_then(Value::as_str).filter(|s| !s.is_empty()).ok_or_else(|| {
                                WorkflowError::Tool {
                                    tool: tool.clone(),
                                    message: "key: Expected nonempty string".into(),
                                }
                            })?;
                        let value = args.get("value").and_then(Value::as_str).ok_or_else(|| WorkflowError::Tool {
                            tool: tool.clone(),
                            message: "value: Expected string".into(),
                        })?;
                        self.engine.deps.memory.set(key, value)?;
                        json!({"ok":true,"key":key})
                    } else if crate::schedule::TOOLS.contains(&tool.as_str()) {
                        let deps = &self.engine.deps;
                        let tz = &ctx.session.env.timezone;
                        let raw = crate::schedule::tool(
                            &*deps.schedule,
                            &*deps.workflows,
                            tz,
                            tool,
                            &args,
                            chrono::Utc::now(),
                        )
                        .await
                        .map_err(|e| WorkflowError::Tool { tool: tool.clone(), message: e.to_string() })?;
                        parse_tool_json(&raw)
                    } else {
                        let raw = self.engine.deps.mcp.call_tool(tool, args, &ctx.cancel).await.map_err(|e| {
                            if is_aborted(&e) {
                                e
                            } else {
                                WorkflowError::Tool { tool: tool.clone(), message: e.to_string() }.into()
                            }
                        })?;
                        check(&ctx.cancel)?;
                        if raw.starts_with("[tool error]") {
                            return Err(WorkflowError::Tool { tool: tool.clone(), message: raw }.into());
                        }
                        parse_tool_json(&raw)
                    };
                    check(&ctx.cancel)?;
                    if let Some(bind) = bind {
                        store.set(bind, output.clone())?;
                    }
                    output
                }
                Step::Compose { preset, skill, prompt, input, bind } => {
                    let resolved =
                        substitute(&Value::Object(input.clone()), store)?.as_object().cloned().unwrap_or_default();
                    let mut parts = Vec::new();
                    if let Some(prompt) = prompt {
                        parts.push(substitute_text(prompt, store)?);
                    }
                    let xml = render_input_xml(&resolved);
                    if !xml.is_empty() {
                        parts.push(xml);
                    }
                    let user = parts.join("\n\n");
                    span.update(ContextUpdate::input(json!({"skill":skill,"prompt":user})));
                    let scope: Scope = span.clone();
                    let output =
                        compose(&self.engine, preset, skill.as_deref(), &user, false, bind, &scope, &ctx.cancel)
                            .await?;
                    store.set(bind, output.clone())?;
                    output
                }
                Step::Agent { preset, skill, prompt, tools, max_iterations, bind } => {
                    let prompt = substitute_text(prompt, store)?;
                    span.update(ContextUpdate::input(json!({"skill":skill,"prompt":prompt})));
                    let id = format!("{}__agent:{bind}", ctx.signal_label);
                    let mut opts = LoopOptions::new(&id, ctx.session.clone());
                    opts.cancel = ctx.cancel.clone();
                    opts.skills = vec![skill.clone()];
                    opts.include_engine_skills = false;
                    opts.preset = PresetName::selectable(preset).ok_or_else(|| anyhow::anyhow!("unknown preset"))?;
                    opts.max_iterations = *max_iterations as usize;
                    opts.parent_id = Some(ctx.signal_label.clone());
                    opts.tool_whitelist = Some(tools.iter().cloned().collect());
                    opts.trace_scope = Some(span.clone());
                    let child = self.engine.start_loop(opts).await?;
                    let result = child.send(prompt).await;
                    self.engine.end_loop(&id).await;
                    let result = result?;
                    check(&ctx.cancel)?;
                    store.set(bind, json!(result))?;
                    json!(result)
                }
                Step::Code { task, data, bind } => {
                    let task = substitute_text(task, store)?;
                    let data = data.as_ref().map(|d| substitute_text(d, store)).transpose()?;
                    span.update(ContextUpdate::input(json!({"task":task,"has_data":data.is_some()})));
                    let result =
                        crate::codex::code_agent(&self.engine.deps.codex, &task, data.as_deref(), &ctx.cancel).await?;
                    check(&ctx.cancel)?;
                    store.set(bind, json!(result))?;
                    json!(result)
                }
                Step::Decide { preset, items, prompt, threshold, bind } => {
                    let decisions = self.engine.deps.decisions.as_ref().ok_or_else(|| {
                        WorkflowError::Decision("no decision model configured (OPEN_ROUTER_API_KEY)".into())
                    })?;
                    let items = substitute(&json!(items), store)?;
                    let items = items.as_array().ok_or_else(|| {
                        // Named as JS `typeof` names it, like the TS message.
                        let got = match &items {
                            Value::Null => "null",
                            Value::Bool(_) => "boolean",
                            Value::Number(_) => "number",
                            Value::String(_) => "string",
                            _ => "object",
                        };
                        WorkflowError::Decision(format!("items must resolve to an array, got {got}"))
                    })?;
                    let prompt = substitute_text(prompt, store)?;
                    span.update(ContextUpdate::input(
                        json!({"preset":preset,"items":items.len(),"threshold":threshold,"prompt":prompt}),
                    ));
                    let scope: Scope = span.clone();
                    let filtered = crate::decisions::filter(decisions, items, &prompt, *threshold, &scope, &ctx.cancel)
                        .await
                        .map_err(|e| if is_aborted(&e) { e } else { WorkflowError::Decision(e.to_string()).into() })?;
                    check(&ctx.cancel)?;
                    let kept = filtered.kept.len();
                    store.set(bind, json!(filtered.kept))?;
                    json!({"kept":kept,"of":items.len()})
                }
                Step::Parallel { steps } => {
                    let cancel = ctx.cancel.child_token();
                    let _guard = cancel.clone().drop_guard();
                    let results: Vec<_> = stream::iter(steps.clone().into_iter().enumerate().map(|(i, child)| {
                        let ctx = ExecContext { cancel: cancel.clone(), ..ctx.clone() };
                        let span = span.clone();
                        async move {
                            let child_span = span.span(
                                SpanStart::new(format!("parallel[{i}]:{}", child.kind()), step_span_kind(&child))
                                    .metadata(step_metadata(&child)),
                            );
                            let result = self.dispatch(&child, &ctx, child_span.clone()).await;
                            match &result {
                                Ok(result) => child_span.end(SpanEnd::output(result.output.clone())),
                                Err(error) => {
                                    ctx.cancel.cancel();
                                    child_span.end(SpanEnd::error(error.to_string()));
                                }
                            }
                            result
                        }
                    }))
                    .buffer_unordered(4)
                    .collect()
                    .await;
                    // Drain child finalizers before ending the parent. Preserve the
                    // actual failure rather than a sibling's cancellation marker.
                    let mut failure = None;
                    for result in results {
                        if let Err(error) = result
                            && (failure.is_none() || !is_aborted(&error))
                        {
                            failure = Some(error);
                        }
                    }
                    if let Some(error) = failure {
                        return Err(error);
                    }
                    json!({"ok":true})
                }
                Step::Terminal => {
                    stop = true;
                    json!({"stop":true})
                }
                Step::Replan { context, note } => {
                    let mut carried = Map::new();
                    let mut missing = Vec::new();
                    for name in context {
                        if store.has(name) {
                            carried.insert(name.clone(), store.get(name)?);
                        } else {
                            missing.push(name.clone());
                        }
                    }
                    span.update(ContextUpdate::input(json!({"context":context,"note":note})));
                    let output = json!({"carried":carried.keys().collect::<Vec<_>>(),"missing":missing});
                    replan = Some(ReplanRequest { context: carried, note: note.clone() });
                    stop = true;
                    output
                }
            };
            Ok(DispatchResult { stop, replan, output })
        }
        .boxed()
    }
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
            .ok_or_else(|| WorkflowError::Skill(skill.to_owned()))?
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
    .map_err(|e| if is_aborted(&e) { e } else { WorkflowError::Llm(e.to_string()).into() })?;
    check(cancel)?;
    let reply = providers::content(&result.message).unwrap_or_default();
    if json {
        return match serde_json::from_str::<Value>(reply.trim()) {
            Ok(object @ Value::Object(_)) => Ok(object),
            _ => Err(WorkflowError::Llm(format!(
                "the reply is not a JSON object: {}",
                crate::activity::clip(reply, 200)
            ))
            .into()),
        };
    }
    Ok(parse_tool_json(reply))
}

// ── 5. runner facade ─────────────────────────────────────────────────────
pub enum RunResult {
    Ok { attempts: usize, step_count: usize, store: Arc<VariableStore> },
    CompileFailed { reason: &'static str, errors: Vec<String>, attempts: usize },
    ExecuteFailed { reason: &'static str, error: anyhow::Error, step_index: usize, step: Step },
    ReplanExhausted { passes: usize, attempts: usize },
}
#[async_trait::async_trait]
pub trait WorkflowRun: Send + Sync {
    async fn run_signal(
        &self,
        signal: &WorkflowSignal,
        session: Session,
        scope: Scope,
        cancel: &CancellationToken,
    ) -> anyhow::Result<RunResult>;
}
#[async_trait::async_trait]
impl WorkflowRun for WorkflowRunner {
    async fn run_signal(
        &self,
        signal: &WorkflowSignal,
        session: Session,
        scope: Scope,
        cancel: &CancellationToken,
    ) -> anyhow::Result<RunResult> {
        let mut prior = None;
        let mut last_attempts = 0;
        for pass in 0..self.max_passes {
            check(cancel)?;
            let (workflow, attempts) = match self.compile(signal, &session.env, &scope, prior.as_ref(), cancel).await? {
                CompileResult::Failed { reason, errors, attempts } => {
                    return Ok(RunResult::CompileFailed { reason, errors, attempts });
                }
                CompileResult::Ok { workflow, attempts } => (workflow, attempts),
            };
            last_attempts = attempts;
            let mut initial = signal_input(signal, &session.env);
            if let Some(prior) = &prior {
                initial.insert("context".into(), json!(prior.data));
            }
            let ctx = ExecContext {
                session: session.clone(),
                store: VariableStore::new(initial),
                scope: scope.clone(),
                signal_label: format!("{}:{}", signal.source, signal.id),
                cancel: cancel.clone(),
            };
            let executed = self.execute(&workflow, &ctx).await?;
            check(cancel)?;
            match executed {
                ExecResult::Failed { reason, error, step_index, step, .. } => {
                    return Ok(RunResult::ExecuteFailed { reason, error, step_index, step });
                }
                ExecResult::Ok { replan: Some(replan), .. } => {
                    prior = Some(PriorContext {
                        pass: pass + 1,
                        last_pass: pass + 1 == self.max_passes - 1,
                        data: replan.context,
                        note: replan.note,
                    })
                }
                ExecResult::Ok { store, replan: None } => {
                    return Ok(RunResult::Ok { attempts, step_count: workflow.steps.len(), store });
                }
            }
        }
        Ok(RunResult::ReplanExhausted { passes: self.max_passes, attempts: last_attempts })
    }
}
