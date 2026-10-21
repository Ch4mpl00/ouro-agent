// The workflow definition: 1. types; 2. parsing (YAML/JSON → types, with
// path-qualified errors); 3. validation against a function catalog
// (expressions compile, calls resolve, flow targets exist); 4. static
// analysis (the call sites, for manifests and previews).
//
// Supported subset of DSL 1.0: do, call, set, for, switch, try/catch with
// retry, raise, fork, wait; input/output/export transforms and schemas;
// if/then/timeout; use.functions/errors/retries/timeouts. Everything else
// (listen, emit, run, schedule, catalogs, authentication, secrets,
// extensions) is rejected at parse time so a definition never fails later
// on a construct this runtime does not implement.
use crate::expr::{self, Args};
use serde_json::{Map, Value};
use std::sync::OnceLock;
use std::time::Duration;

// ── 1. types ───────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct Workflow {
    pub document: Document,
    pub input: Option<Input>,
    pub output: Option<Output>,
    pub uses: Uses,
    pub tasks: TaskList,
    pub timeout: Option<DurationSpec>,
    /// The definition as parsed, exposed as `$workflow.definition`.
    pub definition: Value,
}

#[derive(Clone, Debug)]
pub struct Document {
    pub dsl: String,
    pub namespace: String,
    pub name: String,
    pub version: String,
    pub title: Option<String>,
    pub summary: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct Uses {
    pub functions: Vec<(String, Task)>,
    pub errors: Vec<(String, ErrorDef)>,
    pub retries: Vec<(String, Retry)>,
    pub timeouts: Vec<(String, DurationSpec)>,
}

pub type TaskList = Vec<(String, Task)>;

#[derive(Clone, Debug)]
pub struct Task {
    pub delivery: crate::durable::Delivery,
    pub when: Option<String>,
    pub input: Option<Input>,
    pub output: Option<Output>,
    pub export: Option<Export>,
    pub timeout: Option<DurationSpec>,
    pub then: Option<Flow>,
    pub kind: Kind,
    /// The task's own definition, exposed as `$task.definition`.
    pub definition: Value,
}

#[derive(Clone, Debug)]
pub enum Kind {
    Call {
        function: String,
        with: Option<Value>,
    },
    Do(TaskList),
    /// `concurrency` (`metadata.concurrency`, an extension): iterations run
    /// at once, at most that many, each on the `for`'s input and a copy of
    /// the context; the output is their outputs in item order.
    For {
        each: String,
        at: String,
        items: Items,
        condition: Option<String>,
        concurrency: Option<usize>,
        tasks: TaskList,
    },
    Fork {
        branches: TaskList,
        compete: bool,
    },
    Set(Value),
    Switch(Vec<(String, Case)>),
    Try {
        tasks: TaskList,
        catch: Box<Catch>,
    },
    Raise(Raise),
    Wait(DurationSpec),
    Listen(Vec<Value>),
}

impl Kind {
    pub fn name(&self) -> &'static str {
        match self {
            Kind::Call { .. } => "call",
            Kind::Do(_) => "do",
            Kind::For { .. } => "for",
            Kind::Fork { .. } => "fork",
            Kind::Set(_) => "set",
            Kind::Switch(_) => "switch",
            Kind::Try { .. } => "try",
            Kind::Raise(_) => "raise",
            Kind::Wait(_) => "wait",
            Kind::Listen(_) => "listen",
        }
    }
}

#[derive(Clone, Debug)]
pub enum Items {
    Expr(String),
    Inline(Vec<Value>),
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Flow {
    Continue,
    Exit,
    End,
    Goto(String),
}

#[derive(Clone, Debug, Default)]
pub struct Input {
    pub schema: Option<Value>,
    /// A bare/`${}` expression string, or an object template.
    pub from: Option<Value>,
}

#[derive(Clone, Debug, Default)]
pub struct Output {
    pub schema: Option<Value>,
    pub as_: Option<Value>,
}

pub type Export = Output;

#[derive(Clone, Debug)]
pub struct Case {
    pub when: Option<String>,
    pub then: Flow,
}

#[derive(Clone, Debug)]
pub struct Catch {
    /// Static filter: each listed property must equal the error's.
    pub errors: Option<Map<String, Value>>,
    pub as_: String,
    pub when: Option<String>,
    pub except_when: Option<String>,
    pub retry: Option<Retry>,
    pub tasks: Option<TaskList>,
    pub then: Option<Flow>,
}

#[derive(Clone, Debug, Default)]
pub struct Retry {
    pub when: Option<String>,
    pub except_when: Option<String>,
    pub delay: Option<DurationSpec>,
    pub backoff: Backoff,
    pub attempts: Option<u32>,
    pub duration: Option<DurationSpec>,
    pub jitter: Option<(DurationSpec, DurationSpec)>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Backoff {
    #[default]
    Constant,
    Linear,
    Exponential,
}

#[derive(Clone, Debug)]
pub enum Raise {
    Inline(ErrorDef),
    Named(String),
}

#[derive(Clone, Debug)]
pub struct ErrorDef {
    pub type_: String,
    pub status: u16,
    /// Literal or `${}` expression.
    pub title: Option<String>,
    pub detail: Option<String>,
    pub instance: Option<String>,
}

#[derive(Clone, Debug)]
pub enum DurationSpec {
    Fixed(Duration),
    /// An expression producing an ISO 8601 duration string.
    Expr(String),
}

// ── 2. parsing ─────────────────────────────────────────────────────────────

/// A definition problem, located by JSON pointer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Problem {
    pub at: String,
    pub message: String,
}

impl std::fmt::Display for Problem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", if self.at.is_empty() { "/" } else { &self.at }, self.message)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid workflow:\n{}", .0.iter().map(|p| format!("  - {p}")).collect::<Vec<_>>().join("\n"))]
pub struct Invalid(pub Vec<Problem>);

/// Parses a YAML (or JSON, which is YAML) definition.
pub fn parse_yaml(text: &str) -> Result<Workflow, Invalid> {
    let value: Value =
        serde_yaml_ng::from_str(text).map_err(|e| Invalid(vec![problem("", format!("not YAML: {e}"))]))?;
    parse(value)
}

/// Our parser runs first because its errors name the exact property and the
/// fix; the official schema then guarantees that whatever we accept is a
/// valid definition by the specification too.
pub fn parse(definition: Value) -> Result<Workflow, Invalid> {
    let mut p = Parser::default();
    let wf = p.workflow(&definition);
    match (wf, p.problems.is_empty()) {
        (Some(wf), true) => {
            conforms(&definition)?;
            Ok(wf)
        }
        _ => Err(Invalid(p.problems)),
    }
}

/// Validates against the official JSON Schema (`schema/workflow.yaml`).
pub fn conforms(definition: &Value) -> Result<(), Invalid> {
    static SCHEMA: OnceLock<jsonschema::Validator> = OnceLock::new();
    let validator = SCHEMA.get_or_init(|| {
        let schema: Value =
            serde_yaml_ng::from_str(include_str!("../schema/workflow.yaml")).expect("the bundled schema is YAML");
        jsonschema::validator_for(&schema).expect("the bundled schema compiles")
    });
    let problems: Vec<Problem> = validator
        .iter_errors(definition)
        .take(5)
        .map(|e| problem(&e.instance_path().to_string(), format!("not valid by the specification's schema: {e}")))
        .collect();
    if problems.is_empty() { Ok(()) } else { Err(Invalid(problems)) }
}

fn problem(at: &str, message: impl Into<String>) -> Problem {
    Problem { at: at.to_string(), message: message.into() }
}

const COMMON: &[&str] = &["if", "input", "output", "export", "timeout", "then", "metadata"];
const UNSUPPORTED_TASKS: &[(&str, &str)] = &[
    ("emit", "emitting events is not supported yet"),
    ("run", "run (container/script/shell/workflow) is not supported; call a function instead"),
];

#[derive(Default)]
struct Parser {
    problems: Vec<Problem>,
    /// Named retries and timeouts, resolved where tasks refer to them.
    retries: Vec<(String, Retry)>,
    timeouts: Vec<(String, DurationSpec)>,
}

impl Parser {
    fn fail(&mut self, at: &str, message: impl Into<String>) {
        self.problems.push(problem(at, message));
    }

    fn object<'v>(&mut self, v: &'v Value, at: &str) -> Option<&'v Map<String, Value>> {
        match v {
            Value::Object(m) => Some(m),
            _ => {
                self.fail(at, "expected an object");
                None
            }
        }
    }

    fn only(&mut self, m: &Map<String, Value>, at: &str, allowed: &[&str]) {
        for k in m.keys() {
            if !allowed.contains(&k.as_str()) {
                self.fail(&format!("{at}/{k}"), format!("unknown property (allowed: {})", allowed.join(", ")));
            }
        }
    }

    fn string(&mut self, m: &Map<String, Value>, key: &str, at: &str) -> Option<String> {
        match m.get(key) {
            None => None,
            Some(Value::String(s)) => Some(s.clone()),
            Some(_) => {
                self.fail(&format!("{at}/{key}"), "expected a string");
                None
            }
        }
    }

    fn required_string(&mut self, m: &Map<String, Value>, key: &str, at: &str) -> String {
        match self.string(m, key, at) {
            Some(s) => s,
            None => {
                if !m.contains_key(key) {
                    self.fail(at, format!("missing `{key}`"));
                }
                String::new()
            }
        }
    }

    fn workflow(&mut self, v: &Value) -> Option<Workflow> {
        let m = self.object(v, "")?;
        self.only(m, "", &["document", "input", "output", "use", "do", "timeout", "schedule", "evaluate"]);
        if m.contains_key("schedule") {
            self.fail("/schedule", "schedules live in the scheduler (a cron row), not in the definition");
        }
        if let Some(ev) = m.get("evaluate").and_then(|e| self.object(e, "/evaluate")) {
            self.only(ev, "/evaluate", &["language", "mode"]);
            if ev.get("language").is_some_and(|l| l != "jq") {
                self.fail("/evaluate/language", "only jq is supported");
            }
            if ev.get("mode").is_some_and(|l| l != "strict") {
                self.fail("/evaluate/mode", "only strict mode is supported");
            }
        }
        let document = match m.get("document") {
            Some(d) => self.document(d),
            None => {
                self.fail("", "missing `document`");
                None
            }
        };
        let mut uses = m.get("use").map(|u| self.uses(u)).unwrap_or_default();
        self.retries = uses.retries.clone();
        self.timeouts = uses.timeouts.clone();
        for (name, def) in m
            .get("use")
            .and_then(Value::as_object)
            .and_then(|u| u.get("functions"))
            .and_then(Value::as_object)
            .into_iter()
            .flatten()
        {
            // Functions are parsed again now that named retries/timeouts are known.
            if let Some(t) = self.task(def, &format!("/use/functions/{name}"))
                && let Some(slot) = uses.functions.iter_mut().find(|(n, _)| n == name)
            {
                slot.1 = t;
            }
        }
        let tasks = match m.get("do") {
            Some(d) => self.task_list(d, "/do"),
            None => {
                self.fail("", "missing `do`");
                Vec::new()
            }
        };
        let input = m.get("input").map(|i| self.input(i, "/input"));
        let output = m.get("output").map(|o| self.output(o, "/output"));
        let timeout = m.get("timeout").and_then(|t| self.timeout(t, "/timeout"));
        Some(Workflow { document: document?, input, output, uses, tasks, timeout, definition: v.clone() })
    }

    fn document(&mut self, v: &Value) -> Option<Document> {
        let m = self.object(v, "/document")?;
        self.only(m, "/document", &["dsl", "namespace", "name", "version", "title", "summary", "tags", "metadata"]);
        let dsl = self.required_string(m, "dsl", "/document");
        if !dsl.is_empty() && !dsl.starts_with("1.") {
            self.fail("/document/dsl", format!("DSL {dsl} is not supported (1.x only)"));
        }
        Some(Document {
            dsl,
            namespace: self.required_string(m, "namespace", "/document"),
            name: self.required_string(m, "name", "/document"),
            version: self.required_string(m, "version", "/document"),
            title: self.string(m, "title", "/document"),
            summary: self.string(m, "summary", "/document"),
        })
    }

    fn uses(&mut self, v: &Value) -> Uses {
        let mut uses = Uses::default();
        let Some(m) = self.object(v, "/use") else { return uses };
        for (k, why) in [
            ("authentications", "authentication is not supported; functions carry their own credentials"),
            ("catalogs", "external catalogs are not supported; functions come from the runtime's catalog"),
            ("extensions", "extensions are not supported"),
            ("secrets", "secrets are not supported; functions carry their own credentials"),
        ] {
            if m.contains_key(k) {
                self.fail(&format!("/use/{k}"), why);
            }
        }
        self.only(
            m,
            "/use",
            &["functions", "errors", "retries", "timeouts", "authentications", "catalogs", "extensions", "secrets"],
        );
        if let Some(e) = m.get("errors").and_then(|e| self.object(e, "/use/errors")) {
            for (name, def) in e {
                if let Some(d) = self.error_def(def, &format!("/use/errors/{name}")) {
                    uses.errors.push((name.clone(), d));
                }
            }
        }
        if let Some(r) = m.get("retries").and_then(|r| self.object(r, "/use/retries")) {
            for (name, def) in r {
                if let Some(d) = self.retry(def, &format!("/use/retries/{name}")) {
                    uses.retries.push((name.clone(), d));
                }
            }
        }
        if let Some(t) = m.get("timeouts").and_then(|t| self.object(t, "/use/timeouts")) {
            for (name, def) in t {
                let at = format!("/use/timeouts/{name}");
                if let Some(d) = self.timeout_object(def, &at) {
                    uses.timeouts.push((name.clone(), d));
                }
            }
        }
        if let Some(f) = m.get("functions").and_then(|f| self.object(f, "/use/functions")) {
            // Placeholders keep declaration order; `workflow` parses the bodies
            // once the named retries/timeouts are in place.
            for name in f.keys() {
                let placeholder = Task {
                    delivery: crate::durable::Delivery::default(),
                    when: None,
                    input: None,
                    output: None,
                    export: None,
                    timeout: None,
                    then: None,
                    kind: Kind::Do(Vec::new()),
                    definition: Value::Null,
                };
                uses.functions.push((name.clone(), placeholder));
            }
        }
        uses
    }

    fn task_list(&mut self, v: &Value, at: &str) -> TaskList {
        let Value::Array(items) = v else {
            self.fail(at, "expected a list of `- name: task` entries");
            return Vec::new();
        };
        let mut out: TaskList = Vec::new();
        for (i, item) in items.iter().enumerate() {
            let at = format!("{at}/{i}");
            let Some(m) = self.object(item, &at) else { continue };
            if m.len() != 1 {
                self.fail(&at, "each entry must be a single `name: task` pair");
                continue;
            }
            let (name, def) = m.iter().next().expect("one entry");
            if out.iter().any(|(n, _)| n == name) {
                self.fail(&at, format!("duplicate task name `{name}` in this list"));
            }
            if let Some(t) = self.task(def, &format!("{at}/{name}")) {
                out.push((name.clone(), t));
            }
        }
        out
    }

    fn task(&mut self, v: &Value, at: &str) -> Option<Task> {
        let m = self.object(v, at)?;
        for (k, why) in UNSUPPORTED_TASKS {
            if m.contains_key(*k) {
                self.fail(&format!("{at}/{k}"), *why);
                return None;
            }
        }
        let kind = if m.contains_key("for") {
            self.only(m, at, &[COMMON, &["for", "while", "do"]].concat());
            self.for_task(m, at)
        } else if m.contains_key("try") {
            self.only(m, at, &[COMMON, &["try", "catch"]].concat());
            self.try_task(m, at)
        } else if let Some(f) = m.get("fork") {
            self.only(m, at, &[COMMON, &["fork"]].concat());
            self.fork_task(f, &format!("{at}/fork"))
        } else if m.contains_key("call") {
            self.only(m, at, &[COMMON, &["call", "with"]].concat());
            let function = self.required_string(m, "call", at);
            let with = m.get("with").cloned();
            if let Some(w) = &with
                && !w.is_object()
            {
                self.fail(&format!("{at}/with"), "expected an object of arguments");
            }
            Some(Kind::Call { function, with })
        } else if let Some(s) = m.get("set") {
            self.only(m, at, &[COMMON, &["set"]].concat());
            if !(s.is_object() || s.as_str().is_some_and(|t| expr::strip(t).is_some())) {
                self.fail(
                    &format!("{at}/set"),
                    "expected an object or a `${ … }` expression (a fixed text is `set: '${ \"…\" }'`)",
                );
            }
            Some(Kind::Set(s.clone()))
        } else if let Some(s) = m.get("switch") {
            self.only(m, at, &[COMMON, &["switch"]].concat());
            self.switch_task(s, &format!("{at}/switch"))
        } else if let Some(r) = m.get("raise") {
            self.only(m, at, &[COMMON, &["raise"]].concat());
            self.raise_task(r, &format!("{at}/raise"))
        } else if let Some(v) = m.get("listen") {
            for key in ["read", "foreach"] {
                if m.contains_key(key) { self.fail(&format!("{at}/{key}"), "listen read/foreach is not supported"); return None; }
            }
            self.only(m, at, &[COMMON, &["listen"]].concat());
            self.listen(v, &format!("{at}/listen"))
        } else if let Some(w) = m.get("wait") {
            self.only(m, at, &[COMMON, &["wait"]].concat());
            self.duration(w, &format!("{at}/wait")).map(Kind::Wait)
        } else if let Some(d) = m.get("do") {
            self.only(m, at, &[COMMON, &["do"]].concat());
            Some(Kind::Do(self.task_list(d, &format!("{at}/do"))))
        } else {
            self.fail(at, "unknown task: expected one of call, do, for, fork, set, switch, try, raise, wait");
            None
        };
        let when = self.string(m, "if", at);
        let input = m.get("input").map(|i| self.input(i, &format!("{at}/input")));
        let output = m.get("output").map(|o| self.output(o, &format!("{at}/output")));
        let export = m.get("export").map(|o| self.output(o, &format!("{at}/export")));
        let timeout = m.get("timeout").and_then(|t| self.timeout(t, &format!("{at}/timeout")));
        let then = m.get("then").and_then(|t| self.flow(t, &format!("{at}/then")));
        if m.get("metadata").and_then(|m| m.get("concurrency")).is_some() && !m.contains_key("for") {
            self.fail(&format!("{at}/metadata/concurrency"), "concurrency belongs on a for task");
        }
        let delivery = match m.get("metadata").and_then(|m| m.get("delivery")) {
            None => crate::durable::Delivery::AtLeastOnce,
            Some(v) => match serde_json::from_value(v.clone()) {
                Ok(policy) if m.contains_key("call") => policy,
                _ => {
                    self.fail(
                        &format!("{at}/metadata/delivery"),
                        "expected at_least_once or at_most_once on a call task",
                    );
                    crate::durable::Delivery::AtLeastOnce
                }
            },
        };
        Some(Task { delivery, when, input, output, export, timeout, then, kind: kind?, definition: v.clone() })
    }

    fn listen(&mut self, v: &Value, at: &str) -> Option<Kind> {
        let m = self.object(v, at)?;
        for key in m.keys().filter(|k| k.as_str() != "to") {
            self.fail(&format!("{at}/{key}"), "listen option not supported");
        }
        let to = self.object(m.get("to").unwrap_or(&Value::Null), &format!("{at}/to"))?;
        if to.len() != 1 || !(to.contains_key("one") || to.contains_key("any")) {
            self.fail(at, "only listen.to.one and listen.to.any are supported; all/until/correlate are not supported");
            return None;
        }
        let filters = if let Some(one) = to.get("one") {
            vec![one]
        } else if let Some(Value::Array(any)) = to.get("any") {
            any.iter().collect()
        } else {
            self.fail(at, "listen.to.any requires an array");
            return None;
        };
        if filters.is_empty() {
            self.fail(at, "listening without an event type is not supported");
        }
        let mut types = Vec::new();
        for filter in filters {
            let f = self.object(filter, at)?;
            for key in f.keys().filter(|k| k.as_str() != "with") {
                self.fail(&format!("{at}/{key}"), "event filter option not supported");
            }
            let with = self.object(f.get("with").unwrap_or(&Value::Null), at)?;
            for key in with.keys().filter(|k| k.as_str() != "type") {
                self.fail(&format!("{at}/{key}"), "event filter option not supported");
            }
            let t = self.required_string(with, "type", at);
            if t.is_empty() {
                self.fail(at, "event type must not be empty");
            }
            types.push(Value::String(t));
        }
        Some(Kind::Listen(types))
    }

    fn for_task(&mut self, m: &Map<String, Value>, at: &str) -> Option<Kind> {
        let f = self.object(&m["for"], &format!("{at}/for"))?;
        self.only(f, &format!("{at}/for"), &["each", "in", "at"]);
        let items = match f.get("in") {
            Some(Value::String(s)) => Items::Expr(expr::bare(s).to_string()),
            Some(Value::Array(a)) => Items::Inline(a.clone()),
            Some(_) => {
                self.fail(&format!("{at}/for/in"), "expected an expression or an inline array");
                return None;
            }
            None => {
                self.fail(&format!("{at}/for"), "missing `in`");
                return None;
            }
        };
        let each = self.string(f, "each", &format!("{at}/for")).unwrap_or_else(|| "item".into());
        let index = self.string(f, "at", &format!("{at}/for")).unwrap_or_else(|| "index".into());
        let condition = self.string(m, "while", at).map(|s| expr::bare(&s).to_string());
        let concurrency = match m.get("metadata").and_then(|m| m.get("concurrency")) {
            None => None,
            Some(n) => match n.as_u64().filter(|n| *n > 0).and_then(|n| usize::try_from(n).ok()) {
                Some(_) if condition.is_some() => {
                    self.fail(&format!("{at}/metadata/concurrency"), "a concurrent for has no `while`");
                    None
                }
                Some(n) => Some(n),
                None => {
                    self.fail(&format!("{at}/metadata/concurrency"), "expected a positive integer");
                    None
                }
            },
        };
        let tasks = match m.get("do") {
            Some(d) => self.task_list(d, &format!("{at}/do")),
            None => {
                self.fail(at, "a for task needs `do`");
                Vec::new()
            }
        };
        Some(Kind::For { each, at: index, items, condition, concurrency, tasks })
    }

    fn try_task(&mut self, m: &Map<String, Value>, at: &str) -> Option<Kind> {
        let tasks = self.task_list(&m["try"], &format!("{at}/try"));
        let Some(c) = m.get("catch") else {
            self.fail(at, "a try task needs `catch`");
            return None;
        };
        let cat = format!("{at}/catch");
        let c = self.object(c, &cat)?;
        self.only(c, &cat, &["errors", "as", "when", "exceptWhen", "retry", "do", "then"]);
        let errors = match c.get("errors") {
            None => None,
            Some(e) => {
                let e = self.object(e, &format!("{cat}/errors"))?;
                self.only(e, &format!("{cat}/errors"), &["with"]);
                e.get("with").and_then(|w| self.object(w, &format!("{cat}/errors/with"))).cloned()
            }
        };
        let retry = match c.get("retry") {
            None => None,
            Some(Value::String(name)) => match self.retries.iter().find(|(n, _)| n == name) {
                Some((_, r)) => Some(r.clone()),
                None => {
                    self.fail(&format!("{cat}/retry"), format!("no retry policy `{name}` under use.retries"));
                    None
                }
            },
            Some(r) => self.retry(r, &format!("{cat}/retry")),
        };
        Some(Kind::Try {
            tasks,
            catch: Box::new(Catch {
                errors,
                as_: self.string(c, "as", &cat).unwrap_or_else(|| "error".into()),
                when: self.string(c, "when", &cat).map(|s| expr::bare(&s).to_string()),
                except_when: self.string(c, "exceptWhen", &cat).map(|s| expr::bare(&s).to_string()),
                retry,
                tasks: c.get("do").map(|d| self.task_list(d, &format!("{cat}/do"))),
                then: c.get("then").and_then(|t| self.flow(t, &format!("{cat}/then"))),
            }),
        })
    }

    fn fork_task(&mut self, v: &Value, at: &str) -> Option<Kind> {
        let f = self.object(v, at)?;
        self.only(f, at, &["branches", "compete"]);
        let branches = match f.get("branches") {
            Some(b) => self.task_list(b, &format!("{at}/branches")),
            None => Vec::new(),
        };
        let compete = match f.get("compete") {
            None => false,
            Some(Value::Bool(b)) => *b,
            Some(_) => {
                self.fail(&format!("{at}/compete"), "expected a boolean");
                false
            }
        };
        Some(Kind::Fork { branches, compete })
    }

    fn switch_task(&mut self, v: &Value, at: &str) -> Option<Kind> {
        let Value::Array(items) = v else {
            self.fail(at, "expected a list of `- name: case` entries");
            return None;
        };
        let mut cases = Vec::new();
        let mut defaults = 0;
        for (i, item) in items.iter().enumerate() {
            let cat = format!("{at}/{i}");
            let Some(m) = self.object(item, &cat) else { continue };
            let Some((name, case)) = m.iter().next().filter(|_| m.len() == 1) else {
                self.fail(&cat, "each entry must be a single `name: case` pair");
                continue;
            };
            let cat = format!("{cat}/{name}");
            let Some(c) = self.object(case, &cat) else { continue };
            self.only(c, &cat, &["when", "then"]);
            let when = self.string(c, "when", &cat).map(|s| expr::bare(&s).to_string());
            if when.is_none() {
                defaults += 1;
            }
            let Some(then) = c.get("then").and_then(|t| self.flow(t, &format!("{cat}/then"))) else {
                if !c.contains_key("then") {
                    self.fail(&cat, "a switch case needs `then`");
                }
                continue;
            };
            cases.push((name.clone(), Case { when, then }));
        }
        if defaults > 1 {
            self.fail(at, "only one case may omit `when` (the default)");
        }
        Some(Kind::Switch(cases))
    }

    fn raise_task(&mut self, v: &Value, at: &str) -> Option<Kind> {
        let r = self.object(v, at)?;
        self.only(r, at, &["error"]);
        match r.get("error") {
            Some(Value::String(name)) => Some(Kind::Raise(Raise::Named(name.clone()))),
            Some(e) => self.error_def(e, &format!("{at}/error")).map(|d| Kind::Raise(Raise::Inline(d))),
            None => {
                self.fail(at, "missing `error`");
                None
            }
        }
    }

    fn error_def(&mut self, v: &Value, at: &str) -> Option<ErrorDef> {
        let e = self.object(v, at)?;
        self.only(e, at, &["type", "status", "title", "detail", "instance"]);
        let status = match e.get("status") {
            Some(Value::Number(n)) if n.as_u64().is_some_and(|s| (100..=599).contains(&s)) => {
                n.as_u64().unwrap_or(500) as u16
            }
            Some(_) => {
                self.fail(&format!("{at}/status"), "expected an HTTP-style status between 100 and 599");
                500
            }
            None => {
                self.fail(at, "missing `status`");
                500
            }
        };
        Some(ErrorDef {
            type_: self.required_string(e, "type", at),
            status,
            title: self.string(e, "title", at),
            detail: self.string(e, "detail", at),
            instance: self.string(e, "instance", at),
        })
    }

    fn retry(&mut self, v: &Value, at: &str) -> Option<Retry> {
        let r = self.object(v, at)?;
        self.only(r, at, &["when", "exceptWhen", "delay", "backoff", "limit", "jitter"]);
        let mut retry = Retry {
            when: self.string(r, "when", at).map(|s| expr::bare(&s).to_string()),
            except_when: self.string(r, "exceptWhen", at).map(|s| expr::bare(&s).to_string()),
            delay: r.get("delay").and_then(|d| self.duration(d, &format!("{at}/delay"))),
            ..Retry::default()
        };
        if let Some(b) = r.get("backoff").and_then(|b| self.object(b, &format!("{at}/backoff"))) {
            self.only(b, &format!("{at}/backoff"), &["constant", "linear", "exponential"]);
            retry.backoff = if b.contains_key("exponential") {
                Backoff::Exponential
            } else if b.contains_key("linear") {
                Backoff::Linear
            } else {
                Backoff::Constant
            };
        }
        if let Some(l) = r.get("limit").and_then(|l| self.object(l, &format!("{at}/limit"))) {
            let lat = format!("{at}/limit");
            self.only(l, &lat, &["attempt", "duration"]);
            if let Some(a) = l.get("attempt").and_then(|a| self.object(a, &format!("{lat}/attempt"))) {
                self.only(a, &format!("{lat}/attempt"), &["count", "duration"]);
                if a.contains_key("duration") {
                    // The specification leaves open whether it bounds each
                    // attempt or all of them; a task `timeout` says either.
                    self.fail(
                        &format!("{lat}/attempt/duration"),
                        "limit.attempt.duration is not supported; put a `timeout` on the task inside `try`",
                    );
                }
                retry.attempts = match a.get("count") {
                    None => None,
                    Some(Value::Number(n)) if n.as_u64().is_some() => {
                        Some(n.as_u64().unwrap_or(0).min(u32::MAX as u64) as u32)
                    }
                    Some(_) => {
                        self.fail(&format!("{lat}/attempt/count"), "expected a non-negative integer");
                        None
                    }
                };
            }
            retry.duration = l.get("duration").and_then(|d| self.duration(d, &format!("{lat}/duration")));
        }
        if let Some(j) = r.get("jitter").and_then(|j| self.object(j, &format!("{at}/jitter"))) {
            let from = j.get("from").and_then(|d| self.duration(d, &format!("{at}/jitter/from")));
            let to = j.get("to").and_then(|d| self.duration(d, &format!("{at}/jitter/to")));
            match (from, to) {
                (Some(f), Some(t)) => retry.jitter = Some((f, t)),
                _ => self.fail(&format!("{at}/jitter"), "jitter needs `from` and `to`"),
            }
        }
        Some(retry)
    }

    fn input(&mut self, v: &Value, at: &str) -> Input {
        let Some(m) = self.object(v, at) else { return Input::default() };
        self.only(m, at, &["schema", "from"]);
        Input {
            schema: m.get("schema").and_then(|s| self.schema(s, &format!("{at}/schema"))),
            from: self.transform(m.get("from"), &format!("{at}/from")),
        }
    }

    fn output(&mut self, v: &Value, at: &str) -> Output {
        let Some(m) = self.object(v, at) else { return Output::default() };
        self.only(m, at, &["schema", "as"]);
        Output {
            schema: m.get("schema").and_then(|s| self.schema(s, &format!("{at}/schema"))),
            as_: self.transform(m.get("as"), &format!("{at}/as")),
        }
    }

    /// `from`/`as`: an expression string (bare or `${}`) or an object template.
    fn transform(&mut self, v: Option<&Value>, at: &str) -> Option<Value> {
        match v? {
            Value::String(s) => Some(Value::String(expr::bare(s).to_string())),
            o @ Value::Object(_) => Some(o.clone()),
            _ => {
                self.fail(at, "expected an expression or an object");
                None
            }
        }
    }

    fn schema(&mut self, v: &Value, at: &str) -> Option<Value> {
        let m = self.object(v, at)?;
        self.only(m, at, &["format", "document", "resource"]);
        if m.get("format").is_some_and(|f| f != "json") {
            self.fail(&format!("{at}/format"), "only JSON Schema (`json`) is supported");
        }
        if m.contains_key("resource") {
            self.fail(&format!("{at}/resource"), "external schemas are not supported; inline the `document`");
            return None;
        }
        match m.get("document") {
            Some(d) => match jsonschema::validator_for(d) {
                Ok(_) => Some(d.clone()),
                Err(e) => {
                    self.fail(&format!("{at}/document"), format!("invalid JSON Schema: {e}"));
                    None
                }
            },
            None => {
                self.fail(at, "missing `document`");
                None
            }
        }
    }

    fn timeout(&mut self, v: &Value, at: &str) -> Option<DurationSpec> {
        match v {
            Value::String(name) => match self.timeouts.iter().find(|(n, _)| n == name) {
                Some((_, d)) => Some(d.clone()),
                None => {
                    self.fail(at, format!("no timeout `{name}` under use.timeouts"));
                    None
                }
            },
            _ => self.timeout_object(v, at),
        }
    }

    fn timeout_object(&mut self, v: &Value, at: &str) -> Option<DurationSpec> {
        let m = self.object(v, at)?;
        self.only(m, at, &["after"]);
        match m.get("after") {
            Some(d) => self.duration(d, &format!("{at}/after")),
            None => {
                self.fail(at, "missing `after`");
                None
            }
        }
    }

    fn duration(&mut self, v: &Value, at: &str) -> Option<DurationSpec> {
        match v {
            Value::String(s) => match expr::strip(s) {
                Some(code) => Some(DurationSpec::Expr(code.to_string())),
                None => match parse_iso_duration(s) {
                    Some(d) => Some(DurationSpec::Fixed(d)),
                    None => {
                        self.fail(at, format!("`{s}` is not an ISO 8601 duration (e.g. PT30S)"));
                        None
                    }
                },
            },
            Value::Object(m) => {
                self.only(m, at, &["days", "hours", "minutes", "seconds", "milliseconds"]);
                let mut ms: u64 = 0;
                for (k, unit) in [
                    ("days", 86_400_000u64),
                    ("hours", 3_600_000),
                    ("minutes", 60_000),
                    ("seconds", 1_000),
                    ("milliseconds", 1),
                ] {
                    match m.get(k) {
                        None => {}
                        Some(Value::Number(n)) if n.as_u64().is_some() => {
                            ms = ms.saturating_add(n.as_u64().unwrap_or(0).saturating_mul(unit))
                        }
                        Some(_) => self.fail(&format!("{at}/{k}"), "expected a non-negative integer"),
                    }
                }
                Some(DurationSpec::Fixed(Duration::from_millis(ms)))
            }
            _ => {
                self.fail(at, "expected a duration object or an ISO 8601 string");
                None
            }
        }
    }

    fn flow(&mut self, v: &Value, at: &str) -> Option<Flow> {
        match v.as_str() {
            Some("continue") => Some(Flow::Continue),
            Some("exit") => Some(Flow::Exit),
            Some("end") => Some(Flow::End),
            Some(name) if !name.is_empty() => Some(Flow::Goto(name.to_string())),
            _ => {
                self.fail(at, "expected continue, exit, end or a task name");
                None
            }
        }
    }
}

/// `P[nD][T[nH][nM][n[.n]S]]` (weeks/months/years are not durations here).
pub fn parse_iso_duration(s: &str) -> Option<Duration> {
    let rest = s.trim().strip_prefix('P')?;
    if rest.is_empty() {
        return None;
    }
    let (date, time) = match rest.split_once('T') {
        Some((d, t)) if !t.is_empty() => (d, Some(t)),
        Some(_) => return None,
        None => (rest, None),
    };
    let mut ms: f64 = 0.0;
    let mut take = |part: &str, units: &[(char, f64)]| -> Option<()> {
        let mut num = String::new();
        let mut unit_idx = 0;
        for c in part.chars() {
            if c.is_ascii_digit() || c == '.' {
                num.push(c);
            } else {
                let pos = units[unit_idx..].iter().position(|(u, _)| *u == c)? + unit_idx;
                ms += num.parse::<f64>().ok()? * units[pos].1;
                num.clear();
                unit_idx = pos + 1;
            }
        }
        num.is_empty().then_some(())
    };
    take(date, &[('D', 86_400_000.0)])?;
    if let Some(t) = time {
        take(t, &[('H', 3_600_000.0), ('M', 60_000.0), ('S', 1_000.0)])?;
    }
    Some(Duration::from_millis(ms.round() as u64))
}

// ── 3. validation ──────────────────────────────────────────────────────────

/// What validation needs to know about the runtime's functions.
pub trait Catalog {
    /// Whether `name` is callable here.
    fn has(&self, name: &str) -> bool;
    fn events(&self) -> bool {
        false
    }
}

/// Normalises `name:version@default` (the runtime's default catalog) to
/// `name`; any other catalog suffix is kept and fails to resolve.
pub fn function_name(call: &str) -> &str {
    match call.strip_suffix("@default") {
        Some(rest) => rest.split_once(':').map_or(rest, |(n, _)| n),
        None => call,
    }
}

/// The runtime arguments every expression may use.
pub const BASE_VARS: &[&str] = &["context", "input", "output", "task", "workflow", "runtime"];

fn base_args() -> Args {
    BASE_VARS.iter().fold(Args::new(), |a, n| a.with(n, Value::Null))
}

/// Checks what parsing cannot: expressions compile with the variables in
/// scope, calls resolve, flow directives target tasks of their own list,
/// named errors exist, value strings do not embed `${`.
pub fn validate(wf: &Workflow, catalog: &dyn Catalog) -> Result<(), Invalid> {
    let mut v = Validator { wf, catalog, problems: Vec::new() };
    let args = base_args();
    if let Some(i) = &wf.input {
        v.transform(i.from.as_ref(), "/input/from", &args);
    }
    if let Some(o) = &wf.output {
        v.transform(o.as_.as_ref(), "/output/as", &args);
    }
    for (name, f) in &wf.uses.functions {
        v.task(f, &format!("/use/functions/{name}"), &args, &[]);
    }
    v.list(&wf.tasks, "/do", &args);
    if v.problems.is_empty() { Ok(()) } else { Err(Invalid(v.problems)) }
}

struct Validator<'a> {
    wf: &'a Workflow,
    catalog: &'a dyn Catalog,
    problems: Vec<Problem>,
}

impl Validator<'_> {
    fn expr(&mut self, code: &str, at: &str, args: &Args) {
        if let Err(e) = expr::check(code, args) {
            self.problems.push(problem(at, e));
        }
    }

    fn template(&mut self, t: &Value, at: &str, args: &Args) {
        let mut exprs = Vec::new();
        expr::template_exprs(t, &mut exprs);
        for e in exprs {
            self.expr(&e, at, args);
        }
        let mut embedded = Vec::new();
        expr::embedded_strings(t, &mut embedded);
        for s in embedded {
            self.problems.push(problem(at, format!("`{s}` embeds `${{` inside text; make the whole value one expression, e.g. \"${{ \\\"Hello \\(.name)\\\" }}\"")));
        }
    }

    fn transform(&mut self, t: Option<&Value>, at: &str, args: &Args) {
        match t {
            Some(Value::String(code)) => self.expr(code, at, args),
            Some(o) => self.template(o, at, args),
            None => {}
        }
    }

    fn flow(&mut self, f: &Option<Flow>, at: &str, siblings: &[&str]) {
        if let Some(Flow::Goto(name)) = f
            && !siblings.contains(&name.as_str())
        {
            self.problems.push(problem(
                at,
                format!("`then: {name}` must name a task in the same list (flow directives cannot leave their scope)"),
            ));
        }
    }

    fn list(&mut self, tasks: &TaskList, at: &str, args: &Args) {
        let names: Vec<&str> = tasks.iter().map(|(n, _)| n.as_str()).collect();
        for (i, (name, t)) in tasks.iter().enumerate() {
            self.task(t, &format!("{at}/{i}/{name}"), args, &names);
        }
    }

    fn task(&mut self, t: &Task, at: &str, args: &Args, siblings: &[&str]) {
        if let Some(c) = &t.when {
            self.expr(expr::bare(c), &format!("{at}/if"), args);
        }
        if let Some(i) = &t.input {
            self.transform(i.from.as_ref(), &format!("{at}/input/from"), args);
        }
        if let Some(o) = &t.output {
            self.transform(o.as_.as_ref(), &format!("{at}/output/as"), args);
        }
        if let Some(e) = &t.export {
            self.transform(e.as_.as_ref(), &format!("{at}/export/as"), args);
        }
        if let Some(DurationSpec::Expr(e)) = &t.timeout {
            self.expr(e, &format!("{at}/timeout"), args);
        }
        self.flow(&t.then, &format!("{at}/then"), siblings);
        match &t.kind {
            Kind::Call { function, with } => {
                let name = function_name(function);
                let local = self.wf.uses.functions.iter().any(|(n, _)| n == name);
                if local && t.delivery != crate::durable::Delivery::AtLeastOnce {
                    self.problems.push(problem(
                        &format!("{at}/metadata/delivery"),
                        "put delivery on the external call inside the reusable function",
                    ));
                }
                if !local && !self.catalog.has(name) {
                    self.problems.push(problem(&format!("{at}/call"), format!("unknown function `{function}`")));
                }
                if let Some(w) = with {
                    self.template(w, &format!("{at}/with"), args);
                }
            }
            Kind::Do(tasks) => self.list(tasks, &format!("{at}/do"), args),
            Kind::For { each, at: index, items, condition, tasks, .. } => {
                let inner = args.clone().with(each, Value::Null).with(index, Value::Null);
                if let Items::Expr(e) = items {
                    self.expr(e, &format!("{at}/for/in"), args);
                }
                if let Some(c) = condition {
                    self.expr(c, &format!("{at}/while"), &inner);
                }
                self.list(tasks, &format!("{at}/do"), &inner);
            }
            Kind::Fork { branches, .. } => {
                for (i, (name, b)) in branches.iter().enumerate() {
                    // A branch is alone in its scope: it cannot jump to a sibling branch.
                    self.task(b, &format!("{at}/fork/branches/{i}/{name}"), args, &[]);
                }
            }
            Kind::Set(s) => match s {
                Value::String(code) => self.expr(expr::bare(code), &format!("{at}/set"), args),
                other => self.template(other, &format!("{at}/set"), args),
            },
            Kind::Switch(cases) => {
                for (i, (name, c)) in cases.iter().enumerate() {
                    let cat = format!("{at}/switch/{i}/{name}");
                    if let Some(w) = &c.when {
                        self.expr(w, &format!("{cat}/when"), args);
                    }
                    self.flow(&Some(c.then.clone()), &format!("{cat}/then"), siblings);
                }
            }
            Kind::Try { tasks, catch } => {
                self.list(tasks, &format!("{at}/try"), args);
                let caught = args.clone().with(&catch.as_, Value::Null);
                for (code, key) in [(&catch.when, "when"), (&catch.except_when, "exceptWhen")] {
                    if let Some(c) = code {
                        self.expr(c, &format!("{at}/catch/{key}"), &caught);
                    }
                }
                if let Some(r) = &catch.retry {
                    for (code, key) in [(&r.when, "when"), (&r.except_when, "exceptWhen")] {
                        if let Some(c) = code {
                            self.expr(c, &format!("{at}/catch/retry/{key}"), &caught);
                        }
                    }
                }
                if let Some(handler) = &catch.tasks {
                    self.list(handler, &format!("{at}/catch/do"), &caught);
                }
                self.flow(&catch.then, &format!("{at}/catch/then"), siblings);
            }
            Kind::Raise(r) => {
                let def = match r {
                    Raise::Inline(d) => Some(d),
                    Raise::Named(name) => {
                        let found = self.wf.uses.errors.iter().find(|(n, _)| n == name).map(|(_, d)| d);
                        if found.is_none() {
                            self.problems.push(problem(
                                &format!("{at}/raise/error"),
                                format!("no error `{name}` under use.errors"),
                            ));
                        }
                        found
                    }
                };
                for text in def.into_iter().flat_map(|d| [&d.title, &d.detail]).flatten() {
                    if let Some(code) = expr::strip(text) {
                        self.expr(code, &format!("{at}/raise/error"), args);
                    }
                }
            }
            Kind::Listen(types) => {
                if !self.catalog.events() {
                    self.problems.push(problem(&format!("{at}/listen"), "events are not supported by this host"));
                }
                for t in types {
                    self.template(t, &format!("{at}/listen"), args);
                }
            }
            Kind::Wait(d) => {
                if let DurationSpec::Expr(e) = d {
                    self.expr(e, &format!("{at}/wait"), args);
                }
            }
        }
    }
}

// ── 4. static analysis ─────────────────────────────────────────────────────

/// One `call` in the definition: where it is and what it calls.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CallSite {
    pub reference: String,
    pub function: String,
    /// The arguments as written: a template, or one expression.
    pub with: Option<Value>,
    pub delivery: crate::durable::Delivery,
}

/// Every call site, including those inside reusable functions, in
/// definition order. Callers derive manifests and previews from it.
pub fn call_sites(wf: &Workflow) -> Vec<CallSite> {
    fn list(tasks: &TaskList, at: &str, out: &mut Vec<CallSite>) {
        for (i, (name, t)) in tasks.iter().enumerate() {
            task(t, &format!("{at}/{i}/{name}"), out);
        }
    }
    fn task(t: &Task, at: &str, out: &mut Vec<CallSite>) {
        match &t.kind {
            Kind::Call { function, with } => out.push(CallSite {
                reference: at.to_string(),
                function: function_name(function).to_string(),
                with: with.clone(),
                delivery: t.delivery,
            }),
            Kind::Do(tasks) | Kind::For { tasks, .. } => list(tasks, &format!("{at}/do"), out),
            Kind::Fork { branches, .. } => list(branches, &format!("{at}/fork/branches"), out),
            Kind::Try { tasks, catch } => {
                list(tasks, &format!("{at}/try"), out);
                if let Some(h) = &catch.tasks {
                    list(h, &format!("{at}/catch/do"), out);
                }
            }
            Kind::Set(_) | Kind::Switch(_) | Kind::Raise(_) | Kind::Wait(_) | Kind::Listen(_) => {}
        }
    }
    let mut out = Vec::new();
    for (name, f) in &wf.uses.functions {
        task(f, &format!("/use/functions/{name}"), &mut out);
    }
    list(&wf.tasks, "/do", &mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Names(&'static [&'static str]);
    impl Catalog for Names {
        fn has(&self, name: &str) -> bool {
            self.0.contains(&name)
        }
    }

    const HEAD: &str = "document: { dsl: '1.0.4', namespace: t, name: w, version: '0.1.0' }\n";

    fn wf(body: &str) -> Result<Workflow, Invalid> {
        parse_yaml(&format!("{HEAD}{body}"))
    }

    fn problems(body: &str, catalog: &[&'static str]) -> Vec<String> {
        let leaked: &'static [&'static str] = Box::leak(catalog.to_vec().into_boxed_slice());
        match wf(body) {
            Err(Invalid(p)) => p.iter().map(|p| p.to_string()).collect(),
            Ok(w) => match validate(&w, &Names(leaked)) {
                Ok(()) => Vec::new(),
                Err(Invalid(p)) => p.iter().map(|p| p.to_string()).collect(),
            },
        }
    }

    #[test]
    fn parses_every_supported_kind() {
        let w = wf(r#"
do:
  - a: { call: list_news, with: { since: '${ .s }' } }
  - b: { set: { x: 1 } }
  - c: { for: { each: n, in: '.items' }, do: [ { d: { set: '${ $n }' } } ] }
  - e: { switch: [ { hit: { when: '.x == 1', then: f } }, { other: { then: end } } ] }
  - f: { fork: { compete: true, branches: [ { g: { set: { y: 2 } } } ] } }
  - h: { try: [ { i: { call: list_news } } ], catch: { errors: { with: { status: 503 } }, retry: { delay: { seconds: 1 }, backoff: { exponential: {} }, limit: { attempt: { count: 3 } } } } }
  - j: { raise: { error: { type: 'https://x/e', status: 400, title: Bad } } }
  - k: { wait: PT1M30S }
  - l: { do: [ { m: { set: { z: 3 } } } ] }
"#)
        .unwrap();
        let kinds: Vec<&str> = w.tasks.iter().map(|(_, t)| t.kind.name()).collect();
        assert_eq!(kinds, ["call", "set", "for", "switch", "fork", "try", "raise", "wait", "do"]);
        assert!(matches!(&w.tasks[7].1.kind, Kind::Wait(DurationSpec::Fixed(d)) if d.as_secs() == 90));
        validate(&w, &Names(&["list_news"])).unwrap();
    }

    #[test]
    fn unsupported_and_unknown_constructs_fail_at_parse_time() {
        let p = problems("do:\n  - a: { listen: { to: { one: { with: { type: x } } } } }\n", &[]);
        assert!(p[0].contains("/do/0/a/listen") && p[0].contains("not supported"), "{p:?}");
        let p = problems("schedule: { cron: '0 * * * *' }\ndo: [ { a: { set: { x: 1 } } } ]\n", &[]);
        assert!(p.iter().any(|p| p.contains("scheduler")), "{p:?}");
        let p = problems("do: [ { a: { set: { x: 1 }, color: red } } ]\n", &[]);
        assert!(p.iter().any(|p| p.contains("/do/0/a/color") && p.contains("unknown property")), "{p:?}");
        let p = problems("do: [ { a: { frobnicate: 1 } } ]\n", &[]);
        assert!(p.iter().any(|p| p.contains("unknown task")), "{p:?}");
        let p = problems("use: { secrets: [ token ] }\ndo: [ { a: { set: { x: 1 } } } ]\n", &[]);
        assert!(p.iter().any(|p| p.contains("/use/secrets")), "{p:?}");
    }

    #[test]
    fn validation_reports_expressions_calls_and_flow() {
        let p = problems("do: [ { a: { call: nope } } ]\n", &["list_news"]);
        assert_eq!(p, ["/do/0/a/call: unknown function `nope`"]);
        let p = problems("do: [ { a: { set: { x: '${ .[ }' } } } ]\n", &[]);
        assert!(p[0].starts_with("/do/0/a/set: invalid jq"), "{p:?}");
        let p = problems("do: [ { a: { set: { x: '${ $nowhere }' } } } ]\n", &[]);
        assert!(p[0].contains("invalid jq"), "{p:?}");
        let p = problems("do: [ { a: { set: { x: 'Hi ${ .name }' } } } ]\n", &[]);
        assert!(p[0].contains("embeds"), "{p:?}");
        let p =
            problems("do: [ { a: { do: [ { b: { set: { x: 1 }, then: c } } ] } }, { c: { set: { y: 1 } } } ]\n", &[]);
        assert!(p[0].contains("same list"), "{p:?}");
        let p = problems("do: [ { a: { raise: { error: missing } } } ]\n", &[]);
        assert!(p[0].contains("use.errors"), "{p:?}");
    }

    #[test]
    fn loop_and_catch_variables_are_in_scope_only_inside() {
        assert!(
            problems(
                "do: [ { l: { for: { each: c, in: '.xs' }, do: [ { s: { set: '${ [$c, $index] }' } } ] } } ]\n",
                &[]
            )
            .is_empty()
        );
        let p = problems("do: [ { l: { for: { each: c, in: '.xs' }, do: [] } }, { s: { set: '${ $c }' } } ]\n", &[]);
        assert!(p[0].contains("/do/1/s"), "{p:?}");
        assert!(
            problems("do: [ { t: { try: [], catch: { as: err, do: [ { s: { set: '${ $err }' } } ] } } } ]\n", &[])
                .is_empty()
        );
    }

    #[test]
    fn default_catalog_names_and_call_sites() {
        assert_eq!(function_name("log:1.0.0@default"), "log");
        assert_eq!(function_name("log"), "log");
        assert_eq!(function_name("log:1.0.0@global"), "log:1.0.0@global");
        let w = wf("use: { functions: { wrap: { call: send } } }\ndo: [ { a: { call: list_news } }, { b: { for: { in: '.x' }, do: [ { c: { call: 'wrap' } } ] } } ]\n").unwrap();
        let sites: Vec<(String, String)> = call_sites(&w).into_iter().map(|c| (c.reference, c.function)).collect();
        assert_eq!(
            sites,
            [
                ("/use/functions/wrap".to_string(), "send".to_string()),
                ("/do/0/a".to_string(), "list_news".to_string()),
                ("/do/1/b/do/0/c".to_string(), "wrap".to_string()),
            ]
        );
    }

    #[test]
    fn named_retries_and_timeouts_resolve_in_nested_tasks() {
        let w = wf(r#"
use:
  retries: { thrice: { limit: { attempt: { count: 3 } } } }
  timeouts: { short: { after: PT5S } }
  functions: { guarded: { call: list_news, timeout: short } }
do:
  - outer:
      do:
        - t: { try: [ { a: { call: list_news, timeout: short } } ], catch: { retry: thrice } }
"#)
        .unwrap();
        let Kind::Do(inner) = &w.tasks[0].1.kind else { panic!() };
        let Kind::Try { tasks, catch } = &inner[0].1.kind else { panic!() };
        assert_eq!(catch.retry.as_ref().and_then(|r| r.attempts), Some(3));
        assert!(matches!(tasks[0].1.timeout, Some(DurationSpec::Fixed(d)) if d.as_secs() == 5));
        assert!(matches!(w.uses.functions[0].1.timeout, Some(DurationSpec::Fixed(d)) if d.as_secs() == 5));
        assert!(
            problems("do: [ { a: { set: { x: 1 }, timeout: nope } } ]\n", &[])
                .iter()
                .any(|p| p.contains("use.timeouts"))
        );
        let p =
            problems("do: [ { t: { try: [], catch: { retry: { limit: { attempt: { duration: PT1S } } } } } } ]\n", &[]);
        assert!(p.iter().any(|p| p.contains("/limit/attempt/duration") && p.contains("not supported")), "{p:?}");
    }

    #[test]
    fn iso_durations() {
        assert_eq!(parse_iso_duration("PT30S"), Some(Duration::from_secs(30)));
        assert_eq!(parse_iso_duration("P1DT2H"), Some(Duration::from_secs(86_400 + 7_200)));
        assert_eq!(parse_iso_duration("PT0.5S"), Some(Duration::from_millis(500)));
        assert_eq!(parse_iso_duration("PT1M30S"), Some(Duration::from_secs(90)));
        assert_eq!(parse_iso_duration("P"), None);
        assert_eq!(parse_iso_duration("PT"), None);
        assert_eq!(parse_iso_duration("30S"), None);
        assert_eq!(parse_iso_duration("PT30X"), None);
        assert_eq!(parse_iso_duration("PT1S2M"), None);
    }
}
