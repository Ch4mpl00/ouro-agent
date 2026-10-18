// The reference model of the control-flow core: what a workflow run means,
// with nothing else in the way — no async, no timers, no observer, no
// parser. It reads the generator's program tree (support/program.rs)
// directly and calls `owf::expr::eval` only for the jq leaves.
//
// This file is the specification for the differential test (main.rs);
// properties.rs checks the model itself. It is written from the DSL text
// (dsl.md "Task Flow", "Data Flow", "Fault Tolerance"; dsl-reference.md
// Task, Do, For, Set, Switch, Try/Catch, Retry, Raise, Flow Directive) and
// the CTK scenarios (ctk/features), plus the choices this runtime made
// where the specification is silent, each marked CHOICE.
//
//   1. outcome  — what a run produces, and the steps it took
//   2. errors   — Problem Details: type, status, the innermost task
//   3. a run    — the machine, task lists and flow directives
//   4. one task — `if` → input.from → body → output.as → export.as
//   5. bodies   — set, call, do, for, switch, try, raise

use owf::expr::{self, Args};
use serde_json::{Map, Value, json};
use std::collections::HashMap;

use crate::support::host::{behave, effect};
use crate::support::program::{
    Body, Catch, Cond, DEFAULT_AT, DEFAULT_EACH, DEFAULT_ERROR, Data, Flow, Items, Num, Program, RAISED, Task, Template,
};
use owf::run::Effect;

// ── 1. outcome ─────────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
pub struct Outcome {
    pub result: Result<Value, Error>,
    /// `start r` when a task's body begins, `skip r` when its `if` is false,
    /// `retry r #k` when a try runs its body again after attempt k.
    pub trace: Vec<String>,
    /// Task executions, skipped ones included: what the ceiling counts.
    pub tasks_run: usize,
    pub calls: Vec<(String, Value)>,
    pub effects_started: bool,
}

pub struct Limits {
    pub max_tasks: usize,
    pub max_attempts: u32,
}

// ── 2. errors ──────────────────────────────────────────────────────────────

const STANDARD: &str = "https://open-workflow-specification.org/spec/1.0.0/errors/";

#[derive(Clone, Debug, PartialEq)]
pub struct Error {
    pub type_: String,
    pub status: u16,
    /// The innermost task that raised it; `None` outside every task (the
    /// workflow's own input.from / output.as).
    pub instance: Option<String>,
}

impl Error {
    fn standard(kind: &str, status: u16) -> Self {
        Error { type_: format!("{STANDARD}{kind}"), status, instance: None }
    }
    /// What `catch.as` binds; the generated expressions read `status`.
    fn value(&self) -> Value {
        json!({ "type": self.type_, "status": self.status, "instance": self.instance })
    }
}

/// Why a step stopped. `Caught`-able errors are workflow errors; the
/// host's ceiling is not one, so no `try` sees it (CHOICE).
enum Stop {
    Error(Error),
    Ceiling(Error),
}

impl Stop {
    /// An error belongs to the innermost task it happened in.
    fn at(self, reference: &str) -> Stop {
        let locate = |mut e: Error| {
            e.instance.get_or_insert_with(|| reference.to_string());
            e
        };
        match self {
            Stop::Error(e) => Stop::Error(locate(e)),
            Stop::Ceiling(e) => Stop::Ceiling(locate(e)),
        }
    }
}

/// "When the evaluation of an expression fails, runtimes must raise an
/// error with type …/expression and status 400."
fn expression() -> Stop {
    Stop::Error(Error::standard("expression", 400))
}

// ── 3. a run ───────────────────────────────────────────────────────────────

/// The variables a scope adds: loop item/index, the caught error.
type Scope = Vec<(String, Value)>;

/// How a task list ended: it ran out or `exit`ed (`Done`), or a task said
/// `end`, which ends the whole workflow (`End`).
enum Ended {
    Done(Value),
    End(Value),
}

struct Machine<'p> {
    program: &'p Program,
    limits: &'p Limits,
    /// `$context`: starts as the transformed workflow input; `export.as`
    /// replaces it.
    context: Value,
    tasks_run: usize,
    /// Host calls so far, per function (the fakes answer by call count).
    counts: HashMap<String, usize>,
    writes: usize,
    trace: Vec<String>,
    calls: Vec<(String, Value)>,
}

pub fn run(program: &Program, input: Value, limits: &Limits) -> Outcome {
    let mut m = Machine {
        program,
        limits,
        context: Value::Null,
        tasks_run: 0,
        counts: HashMap::new(),
        writes: 0,
        trace: Vec::new(),
        calls: Vec::new(),
    };
    let result = m.workflow(input).map_err(|s| match s {
        Stop::Error(e) | Stop::Ceiling(e) => e,
    });
    Outcome { result, trace: m.trace, tasks_run: m.tasks_run, calls: m.calls, effects_started: m.writes > 0 }
}

impl Machine<'_> {
    fn workflow(&mut self, raw: Value) -> Result<Value, Stop> {
        // "Transform Workflow Input": input.from on the raw input; the
        // result is the first task's input and the initial context.
        let input = match &self.program.input_from {
            Some(d) => self.data(d, &raw, &Scope::new())?,
            None => raw,
        };
        self.context = input.clone();
        let out = match self.list(&self.program.tasks, "/do", input, &Scope::new())? {
            Ended::Done(v) | Ended::End(v) => v,
        };
        match &self.program.output_as {
            Some(d) => self.data(d, &out, &Scope::new()),
            None => Ok(out),
        }
    }

    /// Tasks run in declaration order; `then` redirects within the list:
    /// `continue` the next, `exit` ends this list, `end` the workflow, a
    /// name jumps to that sibling.
    fn list(&mut self, tasks: &[Task], at: &str, input: Value, scope: &Scope) -> Result<Ended, Stop> {
        let mut data = input;
        let mut pc = 0;
        let mut visits = vec![0usize; tasks.len()];
        while pc < tasks.len() {
            let t = &tasks[pc];
            let suffix = if visits[pc] == 0 { String::new() } else { format!("[{}]", visits[pc]) };
            let reference = format!("{at}/{pc}/{}{suffix}", t.name);
            visits[pc] += 1;
            let (out, flow) = self.task(t, &reference, data, scope)?;
            data = out;
            pc = match flow {
                Flow::Continue => pc + 1,
                Flow::Exit => return Ok(Ended::Done(data)),
                Flow::End => return Ok(Ended::End(data)),
                Flow::Goto(i) => i,
                Flow::Named(n) => panic!("the model runs valid programs only (`then: {n}`)"),
            };
        }
        Ok(Ended::Done(data))
    }

    // ── 4. one task ────────────────────────────────────────────────────────

    fn task(&mut self, t: &Task, reference: &str, raw: Value, scope: &Scope) -> Result<(Value, Flow), Stop> {
        // CHOICE: every task entered counts, skipped or not; one past the
        // ceiling stops the run.
        self.tasks_run += 1;
        if self.tasks_run > self.limits.max_tasks {
            let mut e = Error::standard("runtime", 500);
            e.instance = Some(reference.to_string());
            return Err(Stop::Ceiling(e));
        }
        self.steps(t, reference, raw, scope).map_err(|s| s.at(reference))
    }

    fn steps(&mut self, t: &Task, reference: &str, raw: Value, scope: &Scope) -> Result<(Value, Flow), Stop> {
        // `if` on the raw input: "The task is considered skipped if not
        // run, and the raw task input becomes the task's output." CHOICE:
        // a skipped task's `then` does not apply; the next task runs.
        if let Some(c) = &t.when
            && !self.cond(c, &raw, scope)?
        {
            self.trace.push(format!("skip {reference}"));
            return Ok((raw, Flow::Continue));
        }
        let input = match &t.input_from {
            Some(d) => self.data(d, &raw, scope)?,
            None => raw,
        };
        self.trace.push(format!("start {reference}"));
        let (out, directive) = self.body(&t.body, reference, &input, scope)?;
        let out = match &t.output_as {
            Some(d) => self.data(d, &out, scope)?,
            None => out,
        };
        // "export.as … evaluates the transformed task output … replaces
        // the workflow's current context."
        if let Some(d) = &t.export_as {
            self.context = self.data(d, &out, scope)?;
        }
        // A directive chosen inside (a switch case, catch.then, `end`
        // from a nested list) wins over the task's own `then`, which
        // "defaults to continue".
        let flow = directive.or_else(|| t.then.clone()).unwrap_or(Flow::Continue);
        Ok((out, flow))
    }

    // ── 5. bodies ──────────────────────────────────────────────────────────

    fn body(&mut self, b: &Body, reference: &str, input: &Value, scope: &Scope) -> Result<(Value, Option<Flow>), Stop> {
        match b {
            Body::Set(Template::Expr(d)) => Ok((self.data(d, input, scope)?, None)),
            Body::Set(Template::Obj(n)) => Ok((json!({ "n": self.num(n, input, scope)? }), None)),
            Body::Call(f, n) => {
                let args = json!({ "n": self.num(n, input, scope)? });
                Ok((self.call(f, args, reference)?, None))
            }
            Body::Do(tasks) => Ok(ended(self.list(tasks, &format!("{reference}/do"), input.clone(), scope)?)),
            Body::For { each, at, items, cond, body } => {
                let items = self.items(items, input, scope)?;
                // CHOICE: the first pass gets the task's input, each next
                // pass the previous one's output; `while` is checked
                // before each pass, with that pass's item bound.
                let mut data = input.clone();
                for (i, item) in items.into_iter().enumerate() {
                    let mut inner = scope.clone();
                    inner.push((each.clone().unwrap_or_else(|| DEFAULT_EACH.into()), item));
                    inner.push((at.clone().unwrap_or_else(|| DEFAULT_AT.into()), json!(i)));
                    if let Some(c) = cond
                        && !self.cond(c, &data, &inner)?
                    {
                        break;
                    }
                    match self.list(body, &format!("{reference}[{i}]/do"), data, &inner)? {
                        Ended::Done(v) => data = v,
                        Ended::End(v) => return Ok((v, Some(Flow::End))),
                    }
                }
                Ok((data, None))
            }
            Body::Switch(cases) => {
                // The first case whose `when` holds; the default (no
                // `when`) "will be matched by default if no other case
                // match". No match at all: the task's own `then`
                // (switch.feature, implicit default). The output is the input.
                for c in cases {
                    if let Some(w) = &c.when
                        && self.cond(w, input, scope)?
                    {
                        return Ok((input.clone(), Some(c.then.clone())));
                    }
                }
                let default = cases.iter().find(|c| c.when.is_none()).map(|c| c.then.clone());
                Ok((input.clone(), default))
            }
            Body::Try { body, catch } => self.attempt(body, catch, reference, input, scope),
            Body::Raise(status) => {
                Err(Stop::Error(Error { type_: RAISED.into(), status: *status, instance: Some(reference.into()) }))
            }
            Body::Fork { .. } | Body::Wait(_) => panic!("fork and wait are outside the model's profile"),
        }
    }

    /// A `use.functions` entry runs as a task on the call's arguments, at
    /// `<caller>/use/functions/<name>`, outside the caller's scope (CHOICE: its
    /// flow directive does not leave it). Anything else is the host's.
    fn call(&mut self, name: &str, args: Value, caller: &str) -> Result<Value, Stop> {
        if let Some(f) = self.program.functions.iter().find(|f| f.name == name) {
            let (out, _) = self.task(f, &format!("{caller}/use/functions/{name}"), args, &Scope::new())?;
            return Ok(out);
        }
        let before = self.counts.entry(name.to_string()).or_default();
        let n = *before;
        *before += 1;
        if effect(name) == Some(Effect::Write) {
            self.writes += 1;
        }
        self.calls.push((name.to_string(), args.clone()));
        behave(name, &args, n).map_err(|e| Stop::Error(Error { type_: e.type_, status: e.status, instance: None }))
    }

    /// try/catch: an error the catch filters in is retried by its policy,
    /// else handled by `catch.do` and `catch.then`; any other error, and
    /// the ceiling, pass through.
    fn attempt(
        &mut self,
        body: &[Task],
        catch: &Catch,
        reference: &str,
        input: &Value,
        scope: &Scope,
    ) -> Result<(Value, Option<Flow>), Stop> {
        let context = self.context.clone();
        let mut attempt: u32 = 1;
        loop {
            let writes = self.writes;
            let error = match self.list(body, &format!("{reference}/try"), input.clone(), scope) {
                Ok(e) => return Ok(ended(e)),
                Err(Stop::Error(e)) => e,
                Err(ceiling) => return Err(ceiling),
            };
            let mut caught = scope.clone();
            caught.push((catch.as_.clone().unwrap_or_else(|| DEFAULT_ERROR.into()), error.value()));
            // `errors.with`: "catch only the errors whose properties match
            // the specified values"; then `when`.
            let filtered = catch.status.is_none_or(|s| s == error.status);
            let wanted = filtered && catch.when.as_ref().map_or(Ok(true), |w| self.cond(w, input, &caught))?;
            if !wanted {
                return Err(Stop::Error(error));
            }
            if let Some(retry) = &catch.retry {
                // CHOICE: an attempt that started a Write is never
                // replayed; the count includes the first attempt and the
                // host caps it; a retry starts from the context the body
                // began with. The policy's `when` is read before the count.
                let cap = retry.count.unwrap_or(self.limits.max_attempts).min(self.limits.max_attempts);
                if self.writes == writes
                    && retry.when.as_ref().map_or(Ok(true), |w| self.cond(w, input, &caught))?
                    && attempt < cap
                {
                    self.trace.push(format!("retry {reference} #{attempt}"));
                    self.context = context.clone();
                    attempt += 1;
                    continue;
                }
            }
            // "then: the flow directive … after the error has been caught
            // (and after executing any do tasks)". Without a handler the
            // try's input passes through.
            return match &catch.handler {
                Some(h) => {
                    let (out, flow) = ended(self.list(h, &format!("{reference}/catch/do"), input.clone(), &caught)?);
                    Ok((out, flow.or_else(|| catch.then.clone())))
                }
                None => Ok((input.clone(), catch.then.clone())),
            };
        }
    }

    fn items(&self, items: &Items, input: &Value, scope: &Scope) -> Result<Vec<Value>, Stop> {
        match items {
            Items::Inline(xs) => Ok(xs.iter().map(|x| json!(x)).collect()),
            range => match self.eval(&range.code().unwrap_or_default(), input, scope)? {
                Value::Array(xs) => Ok(xs),
                Value::Null => Ok(Vec::new()),
                _ => Err(expression()),
            },
        }
    }

    // The jq leaves.

    fn eval(&self, code: &str, on: &Value, scope: &Scope) -> Result<Value, Stop> {
        let mut args = Args::new().with("context", self.context.clone());
        for (name, value) in scope {
            args.set(name, value.clone());
        }
        expr::eval(code, on, &args).map_err(|_| expression())
    }

    fn data(&self, d: &Data, on: &Value, scope: &Scope) -> Result<Value, Stop> {
        self.eval(&d.code(), on, scope)
    }

    fn num(&self, n: &Num, on: &Value, scope: &Scope) -> Result<Value, Stop> {
        match n {
            Num::Lit(k) => Ok(json!(k)),
            other => self.eval(&other.code(), on, scope),
        }
    }

    fn cond(&self, c: &Cond, on: &Value, scope: &Scope) -> Result<bool, Stop> {
        self.eval(&c.code(), on, scope).map(|v| expr::truthy(&v))
    }
}

fn ended(e: Ended) -> (Value, Option<Flow>) {
    match e {
        Ended::Done(v) => (v, None),
        Ended::End(v) => (v, Some(Flow::End)),
    }
}

/// Every reference the program can run at: what a trace may name.
pub fn references(p: &Program) -> Vec<String> {
    fn list(tasks: &[Task], at: &str, out: &mut Vec<String>) {
        for (i, t) in tasks.iter().enumerate() {
            one(t, &format!("{at}/{i}/{}", t.name), out);
        }
    }
    fn one(t: &Task, r: &str, out: &mut Vec<String>) {
        out.push(r.to_string());
        match &t.body {
            Body::Do(ts) | Body::For { body: ts, .. } => list(ts, &format!("{r}/do"), out),
            Body::Fork { branches, .. } => list(branches, &format!("{r}/fork/branches"), out),
            Body::Try { body, catch } => {
                list(body, &format!("{r}/try"), out);
                if let Some(h) = &catch.handler {
                    list(h, &format!("{r}/catch/do"), out);
                }
            }
            _ => {}
        }
    }
    let mut out = Vec::new();
    for f in &p.functions {
        one(f, &format!("/use/functions/{}", f.name), &mut out);
    }
    list(&p.tasks, "/do", &mut out);
    out
}

/// The model's result in the comparable shape: the value, or the error's
/// type, status and instance.
pub fn shape(r: &Result<Value, Error>) -> Value {
    match r {
        Ok(v) => json!({ "completed": v }),
        Err(e) => Value::Object(Map::from_iter([(
            "faulted".to_string(),
            json!({ "type": e.type_, "status": e.status, "instance": e.instance }),
        )])),
    }
}

/// Static task identity, without visit/iteration suffixes used by the runtime.
pub fn task_reference(invocation: &str) -> String {
    let invocation = invocation.rfind("/use/functions/").map(|at| &invocation[at..]).unwrap_or(invocation);
    invocation.split('/').map(|part| part.split('[').next().unwrap_or(part)).collect::<Vec<_>>().join("/")
}
