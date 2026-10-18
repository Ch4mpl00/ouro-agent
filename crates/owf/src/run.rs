// The interpreter: 1. the host's seams (functions, observer, clock) and the
// outcome; 2. errors (Problem Details with the standard types); 3. the run
// (data flow, flow directives, limits); 4. task kinds; 5. try, retries and
// durations.
//
// Each step is reported to the observer under its task reference
// (`/do/1/fetch/try/0/get`), so a trace names exactly which part of the
// definition ran. Only `call` reaches the host; everything else is evaluated
// here.
use crate::expr::{self, Args};
use crate::model::{
    self, Backoff, Catalog, Catch, DurationSpec, Flow, Invalid, Items, Kind, Raise, Retry, Task, TaskList, Workflow,
};
use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use futures::future::{BoxFuture, try_join_all};
use futures::stream::{FuturesUnordered, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

// ── 1. host seams and outcome ──────────────────────────────────────────────

/// What a host function does to the world (MCP's `readOnlyHint`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Effect {
    Read,
    Write,
}

/// Where a call comes from, for the host's tracing and cancellation.
pub struct CallInfo<'a> {
    pub run_id: &'a str,
    pub reference: &'a str,
    pub cancel: &'a CancellationToken,
}

/// The functions `call` reaches (MCP tools, model steps, …).
#[async_trait]
pub trait Functions: Send + Sync {
    /// The function's effect class; `None` when the host has no such function.
    fn effect(&self, name: &str) -> Option<Effect>;
    async fn call(&self, name: &str, args: Value, info: &CallInfo<'_>) -> Result<Value, WorkflowError>;
}

/// One step of a run, keyed by task reference.
#[derive(Debug)]
pub enum Event<'a> {
    /// `parent` is the task this one runs inside (a reusable function's
    /// caller), `None` at the top level.
    TaskStarted {
        reference: &'a str,
        parent: Option<&'a str>,
        kind: &'static str,
        input: &'a Value,
    },
    /// `if` was false; the task's input passes through.
    TaskSkipped {
        reference: &'a str,
        parent: Option<&'a str>,
    },
    TaskCompleted {
        reference: &'a str,
        output: &'a Value,
    },
    TaskFaulted {
        reference: &'a str,
        error: &'a WorkflowError,
    },
    /// `try` caught `error` and runs its body again after `delay`.
    TaskRetrying {
        reference: &'a str,
        attempt: u32,
        delay: Duration,
        error: &'a WorkflowError,
    },
}

pub trait Observer: Send + Sync {
    fn event(&self, event: &Event<'_>);
}

/// An observer that ignores every event.
pub struct Silent;

impl Observer for Silent {
    fn event(&self, _: &Event<'_>) {}
}

// A timeout or a fork winner can drop a pending child without returning a
// fault through `task`; its observer span must still close before its parent.
struct TaskSpan<'a> {
    observer: &'a dyn Observer,
    reference: &'a str,
    active: bool,
}

impl Drop for TaskSpan<'_> {
    fn drop(&mut self) {
        if self.active {
            let error = WorkflowError {
                instance: Some(self.reference.into()),
                ..WorkflowError::standard(
                    "runtime",
                    499,
                    "Task Interrupted",
                    "the task was cancelled or its enclosing task stopped",
                )
            };
            self.observer.event(&Event::TaskFaulted { reference: self.reference, error: &error });
        }
    }
}

pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// Host-side ceilings no definition can lift.
#[derive(Clone, Debug)]
pub struct Limits {
    /// Task executions per run; every loop pass and `then` jump counts.
    pub max_tasks: usize,
    /// Attempts per `try`, the first included, whatever the policy says.
    pub max_attempts: u32,
}

impl Default for Limits {
    fn default() -> Self {
        Self { max_tasks: 10_000, max_attempts: 10 }
    }
}

pub struct Host {
    pub functions: Arc<dyn Functions>,
    pub observer: Arc<dyn Observer>,
    pub clock: Arc<dyn Clock>,
    pub limits: Limits,
    /// `$runtime`: `{ name, version, metadata }`.
    pub runtime: Value,
}

#[derive(Debug)]
pub enum Status {
    Completed(Value),
    Faulted(WorkflowError),
    Cancelled,
}

#[derive(Debug)]
pub struct Outcome {
    pub status: Status,
    /// Whether a `Write` call started. Until one has, a failed run changed
    /// nothing and can be handed over or rerun as if it had never happened.
    pub effects_started: bool,
    pub tasks_run: usize,
}

/// `model::validate` against the host's functions.
pub fn validate(wf: &Workflow, functions: &dyn Functions) -> Result<(), Invalid> {
    struct Known<'a>(&'a dyn Functions);
    impl Catalog for Known<'_> {
        fn has(&self, name: &str) -> bool {
            self.0.effect(name).is_some()
        }
    }
    model::validate(wf, &Known(functions))
}

// ── 2. errors ──────────────────────────────────────────────────────────────

const STANDARD_ERRORS: &str = "https://open-workflow-specification.org/spec/1.0.0/errors/";

/// A workflow error as RFC 7807 Problem Details; `instance` is the
/// reference of the task that raised it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowError {
    #[serde(rename = "type")]
    pub type_: String,
    pub status: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<String>,
}

impl WorkflowError {
    /// A standard error: `kind` is the last segment of its type URI.
    pub fn standard(kind: &str, status: u16, title: &str, detail: impl Into<String>) -> Self {
        Self {
            type_: format!("{STANDARD_ERRORS}{kind}"),
            status,
            title: Some(title.into()),
            detail: Some(detail.into()),
            instance: None,
        }
    }
    pub fn validation(detail: impl Into<String>) -> Self {
        Self::standard("validation", 400, "Validation Error", detail)
    }
    pub fn expression(detail: impl Into<String>) -> Self {
        Self::standard("expression", 400, "Expression Error", detail)
    }
    pub fn timeout(detail: impl Into<String>) -> Self {
        Self::standard("timeout", 408, "Timeout", detail)
    }
    pub fn communication(status: u16, detail: impl Into<String>) -> Self {
        Self::standard("communication", status, "Communication Error", detail)
    }
    pub fn runtime(detail: impl Into<String>) -> Self {
        Self::standard("runtime", 500, "Runtime Error", detail)
    }
    /// Whether this is the standard error `kind` (`"timeout"`, …).
    pub fn is(&self, kind: &str) -> bool {
        self.type_.strip_prefix(STANDARD_ERRORS) == Some(kind)
    }
    fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
}

impl std::fmt::Display for WorkflowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.title.as_deref().unwrap_or(&self.type_))?;
        if let Some(d) = &self.detail {
            write!(f, ": {d}")?;
        }
        if let Some(i) = &self.instance {
            write!(f, " (at {i})")?;
        }
        Ok(())
    }
}

impl std::error::Error for WorkflowError {}

/// Why a step stopped. Only `Error` can be caught by `try`.
enum Fault {
    Error(WorkflowError),
    /// A host limit was hit: the run stops whatever the definition says.
    Fatal(WorkflowError),
    Cancelled,
}

impl From<WorkflowError> for Fault {
    fn from(e: WorkflowError) -> Self {
        Fault::Error(e)
    }
}

fn expression(e: String) -> Fault {
    Fault::Error(WorkflowError::expression(e))
}

fn conform(schema: &Value, value: &Value, what: &str) -> Result<(), Fault> {
    let validator =
        jsonschema::validator_for(schema).map_err(|e| WorkflowError::validation(format!("{what} schema: {e}")))?;
    let errors: Vec<String> =
        validator.iter_errors(value).take(3).map(|e| format!("{}: {e}", e.instance_path())).collect();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(WorkflowError::validation(format!("{what} does not match its schema: {}", errors.join("; "))).into())
    }
}

// ── 3. the run ─────────────────────────────────────────────────────────────

/// Variables a scope adds on top of the runtime arguments (`for`'s item
/// and index, `catch.as`); a later entry shadows an earlier one.
type Scope = Vec<(String, Value)>;

/// How a task list finished: on its own (or `exit`), or by `then: end`,
/// which ends the whole workflow.
enum Next {
    Done(Value),
    End(Value),
}

fn ended(next: Next) -> (Value, Option<Flow>) {
    match next {
        Next::Done(v) => (v, None),
        Next::End(v) => (v, Some(Flow::End)),
    }
}

fn datetime(t: DateTime<Utc>) -> Value {
    json!({
        "iso8601": t.to_rfc3339_opts(SecondsFormat::Millis, true),
        "epoch": { "seconds": t.timestamp(), "milliseconds": t.timestamp_millis() },
    })
}

/// Runs `wf` on `input` to completion, a fault, or `cancel`.
pub async fn run(host: &Host, wf: &Workflow, run_id: &str, input: Value, cancel: &CancellationToken) -> Outcome {
    let run = Run {
        host,
        wf,
        run_id,
        cancel,
        tasks_run: AtomicUsize::new(0),
        writes: AtomicUsize::new(0),
        workflow: json!({ "id": run_id, "definition": wf.definition, "input": input, "startedAt": datetime(host.clock.now()) }),
    };
    let result = match &wf.timeout {
        None => run.workflow(input).await,
        Some(spec) => match run.duration(
            spec,
            &input,
            &run.args(&Value::Null, &Value::Null, &Value::Null, &Value::Null, &Scope::new()),
        ) {
            Err(f) => Err(f),
            Ok(limit) => match tokio::time::timeout(limit, run.workflow(input)).await {
                Ok(r) => r,
                Err(_) => {
                    Err(Fault::Fatal(WorkflowError::timeout(format!("the workflow did not finish within {limit:?}"))))
                }
            },
        },
    };
    let status = match result {
        Ok(v) => Status::Completed(v),
        Err(Fault::Error(e) | Fault::Fatal(e)) => Status::Faulted(e),
        Err(Fault::Cancelled) => Status::Cancelled,
    };
    Outcome {
        status,
        effects_started: run.writes.load(Ordering::SeqCst) > 0,
        tasks_run: run.tasks_run.load(Ordering::SeqCst),
    }
}

struct Run<'a> {
    host: &'a Host,
    wf: &'a Workflow,
    run_id: &'a str,
    cancel: &'a CancellationToken,
    tasks_run: AtomicUsize,
    /// `Write` calls started so far.
    writes: AtomicUsize,
    /// `$workflow`.
    workflow: Value,
}

impl Run<'_> {
    fn observe(&self, event: Event<'_>) {
        self.host.observer.event(&event);
    }

    /// The runtime arguments, then the scope's variables. Arguments an
    /// expression has no access to are bound to null, so validation and
    /// evaluation see the same names.
    fn args(&self, ctx: &Value, input: &Value, output: &Value, task: &Value, scope: &Scope) -> Args {
        let mut args = Args::new()
            .with("context", ctx.clone())
            .with("input", input.clone())
            .with("output", output.clone())
            .with("task", task.clone())
            .with("workflow", self.workflow.clone())
            .with("runtime", self.host.runtime.clone());
        for (name, value) in scope {
            args.set(name, value.clone());
        }
        args
    }

    /// `input.from` / `output.as` / `export.as`: an expression or a template.
    fn transform(&self, t: &Value, on: &Value, args: &Args) -> Result<Value, Fault> {
        match t {
            Value::String(code) => expr::eval(code, on, args),
            template => expr::render(template, on, args),
        }
        .map_err(expression)
    }

    async fn workflow(&self, raw: Value) -> Result<Value, Fault> {
        let null = Value::Null;
        let none = Scope::new();
        if let Some(s) = self.wf.input.as_ref().and_then(|i| i.schema.as_ref()) {
            conform(s, &raw, "the workflow input")?;
        }
        let input = match self.wf.input.as_ref().and_then(|i| i.from.as_ref()) {
            Some(t) => self.transform(t, &raw, &self.args(&null, &null, &null, &null, &none))?,
            None => raw,
        };
        let mut ctx = input.clone();
        let (out, _) = ended(self.list(&self.wf.tasks, "/do".into(), None, input, &mut ctx, &none).await?);
        let out = match self.wf.output.as_ref().and_then(|o| o.as_.as_ref()) {
            Some(t) => self.transform(t, &out, &self.args(&ctx, &null, &null, &null, &none))?,
            None => out,
        };
        if let Some(s) = self.wf.output.as_ref().and_then(|o| o.schema.as_ref()) {
            conform(s, &out, "the workflow output")?;
        }
        Ok(out)
    }

    /// Runs `tasks` in order, following each task's flow directive.
    fn list<'s>(
        &'s self,
        tasks: &'s TaskList,
        at: String,
        parent: Option<&'s str>,
        input: Value,
        ctx: &'s mut Value,
        scope: &'s Scope,
    ) -> BoxFuture<'s, Result<Next, Fault>> {
        Box::pin(async move {
            let mut data = input;
            let mut i = 0;
            let mut visits = vec![0usize; tasks.len()];
            while let Some((name, task)) = tasks.get(i) {
                let suffix = if visits[i] == 0 { String::new() } else { format!("[{}]", visits[i]) };
                let reference = format!("{at}/{i}/{name}{suffix}");
                visits[i] += 1;
                let (out, flow) = self.task(task, name, &reference, parent, data, ctx, scope).await?;
                data = out;
                i = match flow {
                    Flow::Continue => i + 1,
                    Flow::Exit => break,
                    Flow::End => return Ok(Next::End(data)),
                    Flow::Goto(target) => tasks.iter().position(|(n, _)| *n == target).ok_or_else(|| {
                        Fault::Fatal(WorkflowError::runtime(format!("`then: {target}` names no task in {at}")))
                    })?,
                };
            }
            Ok(Next::Done(data))
        })
    }

    /// One task: the limit and cancellation checks, then the data flow,
    /// with a fault located at this task unless an inner one already is.
    #[allow(clippy::too_many_arguments)]
    fn task<'s>(
        &'s self,
        task: &'s Task,
        name: &'s str,
        reference: &'s str,
        parent: Option<&'s str>,
        raw: Value,
        ctx: &'s mut Value,
        scope: &'s Scope,
    ) -> BoxFuture<'s, Result<(Value, Flow), Fault>> {
        Box::pin(async move {
            if self.cancel.is_cancelled() {
                return Err(Fault::Cancelled);
            }
            let max = self.host.limits.max_tasks;
            if self.tasks_run.fetch_add(1, Ordering::SeqCst) >= max {
                let e =
                    WorkflowError::runtime(format!("more than {max} tasks ran; a loop or a `then` cycle does not end"));
                let e = WorkflowError { instance: Some(reference.into()), ..e };
                self.observe(Event::TaskFaulted { reference, error: &e });
                return Err(Fault::Fatal(e));
            }
            let mut span = TaskSpan { observer: self.host.observer.as_ref(), reference, active: false };
            let result = self.steps(task, name, reference, parent, raw, ctx, scope, &mut span).await;
            if !matches!(result, Err(Fault::Cancelled)) {
                span.active = false;
            }
            match result {
                Err(Fault::Error(mut e)) => {
                    e.instance.get_or_insert_with(|| reference.into());
                    self.observe(Event::TaskFaulted { reference, error: &e });
                    Err(Fault::Error(e))
                }
                Err(Fault::Fatal(mut e)) => {
                    e.instance.get_or_insert_with(|| reference.into());
                    self.observe(Event::TaskFaulted { reference, error: &e });
                    Err(Fault::Fatal(e))
                }
                other => other,
            }
        })
    }

    /// if → input.schema → input.from → the task (under its timeout) →
    /// output.as → output.schema → export.as → export.schema.
    #[allow(clippy::too_many_arguments)]
    async fn steps(
        &self,
        task: &Task,
        name: &str,
        reference: &str,
        parent: Option<&str>,
        raw: Value,
        ctx: &mut Value,
        scope: &Scope,
        span: &mut TaskSpan<'_>,
    ) -> Result<(Value, Flow), Fault> {
        let null = Value::Null;
        let mut desc = json!({
            "name": name,
            "reference": reference,
            "definition": task.definition,
            "input": raw,
            "startedAt": datetime(self.host.clock.now()),
        });
        if let Some(cond) = &task.when {
            let v =
                expr::eval(expr::bare(cond), &raw, &self.args(ctx, &null, &null, &desc, scope)).map_err(expression)?;
            if !expr::truthy(&v) {
                self.observe(Event::TaskSkipped { reference, parent });
                return Ok((raw, Flow::Continue));
            }
        }
        if let Some(s) = task.input.as_ref().and_then(|i| i.schema.as_ref()) {
            conform(s, &raw, "the task input")?;
        }
        let input = match task.input.as_ref().and_then(|i| i.from.as_ref()) {
            Some(t) => self.transform(t, &raw, &self.args(ctx, &null, &null, &desc, scope))?,
            None => raw,
        };
        let limit = match &task.timeout {
            Some(spec) => Some(self.duration(spec, &input, &self.args(ctx, &input, &null, &desc, scope))?),
            None => None,
        };
        self.observe(Event::TaskStarted { reference, parent, kind: task.kind.name(), input: &input });
        span.active = true;
        let body = self.kind(task, reference, &input, ctx, scope, &desc);
        let (out, flow) = match limit {
            None => body.await?,
            Some(limit) => match tokio::time::timeout(limit, body).await {
                Ok(r) => r?,
                Err(_) => {
                    return Err(WorkflowError::timeout(format!("{reference} did not finish within {limit:?}")).into());
                }
            },
        };
        desc["output"] = out.clone();
        let out = match task.output.as_ref().and_then(|o| o.as_.as_ref()) {
            Some(t) => self.transform(t, &out, &self.args(ctx, &input, &null, &desc, scope))?,
            None => out,
        };
        if let Some(s) = task.output.as_ref().and_then(|o| o.schema.as_ref()) {
            conform(s, &out, "the task output")?;
        }
        if let Some(export) = &task.export {
            if let Some(t) = &export.as_ {
                *ctx = self.transform(t, &out, &self.args(ctx, &input, &out, &desc, scope))?;
            }
            if let Some(s) = &export.schema {
                conform(s, ctx, "the exported context")?;
            }
        }
        self.observe(Event::TaskCompleted { reference, output: &out });
        Ok((out, flow.or_else(|| task.then.clone()).unwrap_or(Flow::Continue)))
    }

    // ── 4. task kinds ──────────────────────────────────────────────────────

    /// The task's raw output and the directive it chose, if any (a switch
    /// case, `catch.then`, or `end` from inside).
    async fn kind(
        &self,
        task: &Task,
        reference: &str,
        input: &Value,
        ctx: &mut Value,
        scope: &Scope,
        desc: &Value,
    ) -> Result<(Value, Option<Flow>), Fault> {
        let args = |ctx: &Value| self.args(ctx, input, &Value::Null, desc, scope);
        match &task.kind {
            Kind::Call { function, with } => {
                let call_args = match with {
                    Some(w) => expr::render(w, input, &args(ctx)).map_err(expression)?,
                    None => Value::Object(Map::new()),
                };
                Ok((self.call(model::function_name(function), call_args, reference, ctx).await?, None))
            }
            Kind::Do(tasks) => Ok(ended(
                self.list(tasks, format!("{reference}/do"), Some(reference), input.clone(), ctx, scope).await?,
            )),
            Kind::For { each, at, items, condition, tasks } => {
                let items = match items {
                    Items::Inline(items) => items.clone(),
                    Items::Expr(code) => match expr::eval(code, input, &args(ctx)).map_err(expression)? {
                        Value::Array(items) => items,
                        Value::Null => Vec::new(),
                        other => {
                            return Err(WorkflowError::expression(format!(
                                "`for.in` must produce an array, got {other}"
                            ))
                            .into());
                        }
                    },
                };
                // Each pass gets the previous pass's output; the first gets the task's input.
                let mut data = input.clone();
                for (i, item) in items.into_iter().enumerate() {
                    let mut inner = scope.clone();
                    inner.push((each.clone(), item));
                    inner.push((at.clone(), json!(i)));
                    if let Some(c) = condition {
                        let go = expr::eval(c, &data, &self.args(ctx, input, &Value::Null, desc, &inner))
                            .map_err(expression)?;
                        if !expr::truthy(&go) {
                            break;
                        }
                    }
                    match self.list(tasks, format!("{reference}[{i}]/do"), Some(reference), data, ctx, &inner).await? {
                        Next::Done(v) => data = v,
                        Next::End(v) => return Ok((v, Some(Flow::End))),
                    }
                }
                Ok((data, None))
            }
            Kind::Fork { branches, compete } => {
                Ok((self.fork(branches, *compete, reference, input, ctx, scope).await?, None))
            }
            Kind::Set(value) => {
                let out = match value {
                    Value::String(code) => expr::eval(expr::bare(code), input, &args(ctx)),
                    template => expr::render(template, input, &args(ctx)),
                }
                .map_err(expression)?;
                Ok((out, None))
            }
            Kind::Switch(cases) => {
                let a = args(ctx);
                for (_, case) in cases {
                    let hit = match &case.when {
                        None => false,
                        Some(w) => expr::truthy(&expr::eval(w, input, &a).map_err(expression)?),
                    };
                    if hit {
                        return Ok((input.clone(), Some(case.then.clone())));
                    }
                }
                let default = cases.iter().find(|(_, case)| case.when.is_none()).map(|(_, case)| case.then.clone());
                // Without a default, the task's own `then` applies.
                Ok((input.clone(), default))
            }
            Kind::Try { tasks, catch } => self.attempt(tasks, catch, reference, input, ctx, scope, desc).await,
            Kind::Raise(r) => Err(self.raise(r, reference, input, &args(ctx))?.into()),
            Kind::Wait(spec) => {
                let d = self.duration(spec, input, &args(ctx))?;
                self.sleep(d).await?;
                Ok((input.clone(), None))
            }
        }
    }

    async fn call(&self, name: &str, args: Value, reference: &str, ctx: &mut Value) -> Result<Value, Fault> {
        if let Some((_, function)) = self.wf.uses.functions.iter().find(|(n, _)| n == name) {
            // A reusable function is a task whose input is the call's
            // arguments; it sees none of the caller's loop or catch variables.
            let none = Scope::new();
            let (out, _) = self
                .task(function, name, &format!("{reference}/use/functions/{name}"), Some(reference), args, ctx, &none)
                .await?;
            return Ok(out);
        }
        let effect = self
            .host
            .functions
            .effect(name)
            .ok_or_else(|| Fault::Fatal(WorkflowError::runtime(format!("unknown function `{name}`"))))?;
        if effect == Effect::Write {
            self.writes.fetch_add(1, Ordering::SeqCst);
        }
        let info = CallInfo { run_id: self.run_id, reference, cancel: self.cancel };
        tokio::select! {
            _ = self.cancel.cancelled() => Err(Fault::Cancelled),
            r = self.host.functions.call(name, args, &info) => r.map_err(Fault::Error),
        }
    }

    /// Branches run concurrently, each on the fork's input and a copy of the
    /// context as it was at the fork: what a branch exports stays in it.
    async fn fork(
        &self,
        branches: &TaskList,
        compete: bool,
        fork: &str,
        input: &Value,
        ctx: &Value,
        scope: &Scope,
    ) -> Result<Value, Fault> {
        let runs = branches.iter().enumerate().map(|(i, (name, branch))| {
            let reference = format!("{fork}/fork/branches/{i}/{name}");
            let mut ctx = ctx.clone();
            let input = input.clone();
            async move { self.task(branch, name, &reference, Some(fork), input, &mut ctx, scope).await.map(|(v, _)| v) }
        });
        if !compete {
            // In declaration order; the first fault cancels the rest.
            return Ok(Value::Array(try_join_all(runs).await?));
        }
        // The first branch to complete wins and the others are dropped; the
        // fork faults only when every branch has.
        let mut racing: FuturesUnordered<_> = runs.collect();
        let mut last = None;
        while let Some(r) = racing.next().await {
            match r {
                Ok(v) => return Ok(v),
                Err(Fault::Error(e)) => last = Some(e),
                Err(stop) => return Err(stop),
            }
        }
        Err(last.unwrap_or_else(|| WorkflowError::runtime("a competing fork needs a branch")).into())
    }

    fn raise(&self, r: &Raise, reference: &str, input: &Value, args: &Args) -> Result<WorkflowError, Fault> {
        let def = match r {
            Raise::Inline(d) => d,
            Raise::Named(name) => {
                self.wf.uses.errors.iter().find(|(n, _)| n == name).map(|(_, d)| d).ok_or_else(|| {
                    Fault::Fatal(WorkflowError::runtime(format!("no error `{name}` under use.errors")))
                })?
            }
        };
        let text = |t: &Option<String>| -> Result<Option<String>, Fault> {
            let Some(s) = t else { return Ok(None) };
            let Some(code) = expr::strip(s) else { return Ok(Some(s.clone())) };
            Ok(match expr::eval(code, input, args).map_err(expression)? {
                Value::Null => None,
                Value::String(s) => Some(s),
                other => Some(other.to_string()),
            })
        };
        Ok(WorkflowError {
            type_: def.type_.clone(),
            status: def.status,
            title: text(&def.title)?,
            detail: text(&def.detail)?,
            instance: Some(def.instance.clone().unwrap_or_else(|| reference.to_string())),
        })
    }

    // ── 5. try, retries and durations ──────────────────────────────────────

    /// `try`: run the body; a caught error is retried by the policy, then
    /// handled by `catch.do` and `catch.then`. A retry restores the context
    /// the body started with, and is never made once the failed attempt
    /// started a `Write` call: a side effect is not replayed automatically.
    #[allow(clippy::too_many_arguments)]
    async fn attempt(
        &self,
        tasks: &TaskList,
        catch: &Catch,
        reference: &str,
        input: &Value,
        ctx: &mut Value,
        scope: &Scope,
        desc: &Value,
    ) -> Result<(Value, Option<Flow>), Fault> {
        let began = Instant::now();
        let snapshot = ctx.clone();
        let mut attempt: u32 = 1;
        loop {
            let writes = self.writes.load(Ordering::SeqCst);
            let error =
                match self.list(tasks, format!("{reference}/try"), Some(reference), input.clone(), ctx, scope).await {
                    Ok(next) => return Ok(ended(next)),
                    Err(Fault::Error(e)) => e,
                    Err(stop) => return Err(stop),
                };
            let mut caught = scope.clone();
            caught.push((catch.as_.clone(), error.to_value()));
            let args = self.args(ctx, input, &Value::Null, desc, &caught);
            if !self.catches(catch, &error, input, &args)? {
                return Err(error.into());
            }
            if let Some(retry) = &catch.retry
                && self.writes.load(Ordering::SeqCst) == writes
                && let Some(delay) = self.retry_delay(retry, attempt, began, input, &args)?
            {
                self.observe(Event::TaskRetrying { reference, attempt, delay, error: &error });
                self.sleep(delay).await?;
                *ctx = snapshot.clone();
                attempt += 1;
                continue;
            }
            return match &catch.tasks {
                Some(handler) => {
                    let (out, flow) = ended(
                        self.list(
                            handler,
                            format!("{reference}/catch/do"),
                            Some(reference),
                            input.clone(),
                            ctx,
                            &caught,
                        )
                        .await?,
                    );
                    Ok((out, flow.or_else(|| catch.then.clone())))
                }
                None => Ok((input.clone(), catch.then.clone())),
            };
        }
    }

    /// `catch.errors.with` (each listed property equals the error's), then
    /// `when` / `exceptWhen`.
    fn catches(&self, catch: &Catch, error: &WorkflowError, input: &Value, args: &Args) -> Result<bool, Fault> {
        if let Some(filter) = &catch.errors {
            let e = error.to_value();
            if !filter.iter().all(|(k, v)| e.get(k) == Some(v)) {
                return Ok(false);
            }
        }
        guards(catch.when.as_deref(), catch.except_when.as_deref(), input, args)
    }

    /// The delay before attempt `attempt + 1`, or `None` when the policy
    /// (or the host's ceiling) says to stop. Jitter adds the middle of its
    /// range, which keeps runs reproducible.
    fn retry_delay(
        &self,
        retry: &Retry,
        attempt: u32,
        began: Instant,
        input: &Value,
        args: &Args,
    ) -> Result<Option<Duration>, Fault> {
        if !guards(retry.when.as_deref(), retry.except_when.as_deref(), input, args)? {
            return Ok(None);
        }
        let ceiling = self.host.limits.max_attempts;
        if attempt >= retry.attempts.unwrap_or(ceiling).min(ceiling) {
            return Ok(None);
        }
        let base = match &retry.delay {
            Some(d) => self.duration(d, input, args)?,
            None => Duration::ZERO,
        };
        let factor = match retry.backoff {
            Backoff::Constant => 1,
            Backoff::Linear => attempt,
            Backoff::Exponential => 2u32.saturating_pow(attempt - 1),
        };
        let mut delay = base.saturating_mul(factor);
        if let Some((from, to)) = &retry.jitter {
            delay += (self.duration(from, input, args)? + self.duration(to, input, args)?) / 2;
        }
        if let Some(limit) = &retry.duration
            && began.elapsed() + delay > self.duration(limit, input, args)?
        {
            return Ok(None);
        }
        Ok(Some(delay))
    }

    fn duration(&self, spec: &DurationSpec, input: &Value, args: &Args) -> Result<Duration, Fault> {
        match spec {
            DurationSpec::Fixed(d) => Ok(*d),
            DurationSpec::Expr(code) => match expr::eval(code, input, args).map_err(expression)? {
                Value::String(s) => model::parse_iso_duration(&s)
                    .ok_or_else(|| expression(format!("`{s}` is not an ISO 8601 duration (e.g. PT30S)"))),
                other => Err(expression(format!("a duration expression must produce an ISO 8601 string, got {other}"))),
            },
        }
    }

    async fn sleep(&self, d: Duration) -> Result<(), Fault> {
        tokio::select! {
            _ = self.cancel.cancelled() => Err(Fault::Cancelled),
            _ = tokio::time::sleep(d) => Ok(()),
        }
    }
}

/// `when` must hold and `exceptWhen` must not.
fn guards(when: Option<&str>, except_when: Option<&str>, input: &Value, args: &Args) -> Result<bool, Fault> {
    if let Some(w) = when
        && !expr::truthy(&expr::eval(w, input, args).map_err(expression)?)
    {
        return Ok(false);
    }
    if let Some(x) = except_when
        && expr::truthy(&expr::eval(x, input, args).map_err(expression)?)
    {
        return Ok(false);
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    type Behaviour = Box<dyn Fn(&Value, usize) -> Result<Value, WorkflowError> + Send + Sync>;

    /// Functions by name: an effect and a behaviour given the arguments and
    /// how many times the function was called before.
    #[derive(Default)]
    struct Fake {
        functions: HashMap<&'static str, (Effect, Behaviour)>,
        calls: Mutex<Vec<(String, Value, String)>>,
    }

    impl Fake {
        fn with(
            mut self,
            name: &'static str,
            effect: Effect,
            f: impl Fn(&Value, usize) -> Result<Value, WorkflowError> + Send + Sync + 'static,
        ) -> Self {
            self.functions.insert(name, (effect, Box::new(f)));
            self
        }
    }

    #[async_trait]
    impl Functions for Fake {
        fn effect(&self, name: &str) -> Option<Effect> {
            self.functions.get(name).map(|(e, _)| *e)
        }
        async fn call(&self, name: &str, args: Value, info: &CallInfo<'_>) -> Result<Value, WorkflowError> {
            let before = {
                let mut calls = self.calls.lock().unwrap();
                let n = calls.iter().filter(|(c, _, _)| c == name).count();
                calls.push((name.to_string(), args.clone(), info.reference.to_string()));
                n
            };
            (self.functions[name].1)(&args, before)
        }
    }

    #[derive(Default)]
    struct Log(Mutex<Vec<String>>);

    impl Observer for Log {
        fn event(&self, event: &Event<'_>) {
            let line = match event {
                Event::TaskStarted { reference, .. } => format!("start {reference}"),
                Event::TaskSkipped { reference, .. } => format!("skip {reference}"),
                Event::TaskCompleted { reference, .. } => format!("done {reference}"),
                Event::TaskFaulted { reference, .. } => format!("fault {reference}"),
                Event::TaskRetrying { reference, attempt, delay, .. } => {
                    format!("retry {reference} #{attempt} after {}s", delay.as_secs())
                }
            };
            self.0.lock().unwrap().push(line);
        }
    }

    impl Log {
        fn lines(&self, prefix: &str) -> Vec<String> {
            self.0.lock().unwrap().iter().filter_map(|l| l.strip_prefix(prefix).map(str::to_string)).collect()
        }
    }

    struct Fixed;
    impl Clock for Fixed {
        fn now(&self) -> DateTime<Utc> {
            DateTime::from_timestamp(1_700_000_000, 0).unwrap()
        }
    }

    struct Setup {
        host: Host,
        fake: Arc<Fake>,
        log: Arc<Log>,
    }

    fn setup(fake: Fake) -> Setup {
        let fake = Arc::new(fake);
        let log = Arc::new(Log::default());
        let host = Host {
            functions: fake.clone(),
            observer: log.clone(),
            clock: Arc::new(Fixed),
            limits: Limits::default(),
            runtime: json!({ "name": "owf-test" }),
        };
        Setup { host, fake, log }
    }

    const HEAD: &str = "document: { dsl: '1.0.4', namespace: test, name: w, version: '0.1.0' }\n";

    async fn exec(s: &Setup, definition: &str, input: Value) -> Outcome {
        let text = if definition.trim_start().starts_with("document:") {
            definition.to_string()
        } else {
            format!("{HEAD}{definition}")
        };
        let wf = model::parse_yaml(&text).unwrap_or_else(|e| panic!("{e}"));
        validate(&wf, &*s.host.functions).unwrap_or_else(|e| panic!("{e}"));
        run(&s.host, &wf, "run-1", input, &CancellationToken::new()).await
    }

    fn completed(o: &Outcome) -> &Value {
        match &o.status {
            Status::Completed(v) => v,
            other => panic!("expected completion, got {other:?}"),
        }
    }

    fn faulted(o: &Outcome) -> &WorkflowError {
        match &o.status {
            Status::Faulted(e) => e,
            other => panic!("expected a fault, got {other:?}"),
        }
    }

    fn yaml(text: &str) -> Value {
        serde_yaml_ng::from_str(text).unwrap()
    }

    // The CTK scenarios (ctk/features/*.feature), the HTTP calls replaced by
    // a fake `http` function.

    #[tokio::test]
    async fn ctk_flow_implicit_and_explicit_sequence() {
        let s = setup(Fake::default());
        let o = exec(&s, "do:\n  - setRed: { set: { colors: '${ .colors + [ \"red\" ] }' } }\n  - setGreen: { set: { colors: '${ .colors + [ \"green\" ] }' } }\n  - setBlue: { set: { colors: '${ .colors + [ \"blue\" ] }' } }\n", json!({})).await;
        assert_eq!(completed(&o), &yaml("colors: [ red, green, blue ]"));

        let s = setup(Fake::default());
        let o = exec(
            &s,
            r#"
do:
  - setRed: { set: { colors: '${ .colors + [ "red" ] }' }, then: setGreen }
  - setBlue: { set: { colors: '${ .colors + [ "blue" ] }' }, then: end }
  - setGreen: { set: { colors: '${ .colors + [ "green" ] }' }, then: setBlue }
"#,
            json!({}),
        )
        .await;
        assert_eq!(completed(&o), &yaml("colors: [ red, green, blue ]"));
        assert_eq!(s.log.lines("start "), ["/do/0/setRed", "/do/2/setGreen", "/do/1/setBlue"]);
    }

    #[tokio::test]
    async fn ctk_set_and_do() {
        let s = setup(Fake::default());
        let o = exec(&s, "do:\n  - setShape: { set: { shape: circle, size: '${ .configuration.size }', fill: '${ .configuration.fill }' } }\n", yaml("configuration: { size: { width: 6, height: 6 }, fill: { red: 69, green: 69, blue: 69 } }")).await;
        assert_eq!(
            completed(&o),
            &yaml("shape: circle\nsize: { width: 6, height: 6 }\nfill: { red: 69, green: 69, blue: 69 }")
        );

        let o = exec(&s, "do:\n  - compositeExample:\n      do:\n        - setRed: { set: { colors: '${ .colors + [\"red\"] }' } }\n        - setGreen: { set: { colors: '${ .colors + [\"green\"] }' } }\n", json!({})).await;
        assert_eq!(completed(&o), &yaml("colors: [ red, green ]"));
    }

    #[tokio::test]
    async fn ctk_for_accumulates_over_iterations() {
        let s = setup(Fake::default());
        let o = exec(
            &s,
            r#"
do:
  - loopColors:
      for: { each: color, in: '.colors' }
      do:
        - markProcessed:
            set:
              processed: '${ { colors: (.processed.colors + [ $color ]), indexes: (.processed.indexes + [ $index ])} }'
"#,
            yaml("colors: [ red, green, blue ]"),
        )
        .await;
        assert_eq!(completed(&o), &yaml("processed: { colors: [ red, green, blue ], indexes: [ 0, 1, 2 ] }"));
        assert_eq!(s.log.lines("start ").len(), 4);
    }

    const SWITCH: &str = r#"
do:
  - switchColor:
      switch:
        - red: { when: '.color == "red"', then: setRed }
        - green: { when: '.color == "green"', then: setGreen }
        - other: { then: setCustomColor }
  - setRed: { set: { colors: '${ .colors + [ "red" ] }' }, then: end }
  - setGreen: { set: { colors: '${ .colors + [ "green" ] }' }, then: end }
  - setCustomColor: { set: { colors: '${ .colors + [ $input.color ] }' } }
"#;

    #[tokio::test]
    async fn ctk_switch_matching_default_and_no_match() {
        let s = setup(Fake::default());
        assert_eq!(completed(&exec(&s, SWITCH, json!({"color": "red"})).await), &json!({"colors": ["red"]}));
        assert_eq!(completed(&exec(&s, SWITCH, json!({"color": "yellow"})).await), &json!({"colors": ["yellow"]}));

        let s = setup(Fake::default());
        let implicit = "do:\n  - switchColor:\n      switch:\n        - red: { when: '.color == \"red\"', then: setRed }\n      then: end\n  - setRed: { set: { colors: [ red ] } }\n";
        assert_eq!(completed(&exec(&s, implicit, json!({"color": "yellow"})).await), &json!({"color": "yellow"}));
        assert_eq!(s.log.lines("start "), ["/do/0/switchColor"]);
    }

    fn petstore() -> Fake {
        Fake::default().with("http", Effect::Read, |args, _| {
            let uri = args["endpoint"]["uri"].as_str().unwrap_or_default();
            Ok(json!({ "id": if uri.ends_with("/2") { 2 } else { 1 } }))
        })
    }

    #[tokio::test]
    async fn ctk_data_flow_filters_input_and_output() {
        let s = setup(Fake::default());
        let o = exec(
            &s,
            "do:\n  - setPlayerId: { input: { from: .user.claims.subject }, set: { playerId: '${ . }' } }\n",
            yaml("user: { claims: { subject: 6AsnRgGEB0q2O7ux9JXFAw } }"),
        )
        .await;
        assert_eq!(completed(&o), &json!({"playerId": "6AsnRgGEB0q2O7ux9JXFAw"}));

        let s = setup(petstore());
        let o = exec(
            &s,
            r#"
do:
  - getPetById1:
      call: http
      with: { method: get, endpoint: { uri: 'https://petstore.swagger.io/v2/pet/{petId}' } }
      output: { as: .id }
  - getPetById2:
      call: http
      with: { method: get, endpoint: { uri: 'https://petstore.swagger.io/v2/pet/2' } }
      output: { as: '{ ids: [ $input, .id ] }' }
"#,
            json!({"petId": 1}),
        )
        .await;
        assert_eq!(completed(&o), &json!({"ids": [1, 2]}));
        assert_eq!(s.fake.calls.lock().unwrap()[1].2, "/do/1/getPetById2");
    }

    const TRY_PET: &str = r#"
do:
  - tryGetPet:
      try:
        - getPet:
            call: http
            with: { method: get, endpoint: { uri: 'https://petstore.swagger.io/v2/pet/getPetByName/{petName}' } }
      catch:
        errors: { with: { type: 'https://open-workflow-specification.org/spec/1.0.0/errors/communication', status: 404 } }
        as: err
        do:
          - setError: { set: { error: '${ $err }' } }
"#;

    #[tokio::test]
    async fn ctk_try_catches_a_matching_error_and_faults_on_another() {
        let s = setup(
            Fake::default().with("http", Effect::Read, |_, _| Err(WorkflowError::communication(404, "no such pet"))),
        );
        let o = exec(&s, TRY_PET, json!({"petName": "Milou"})).await;
        let error = &completed(&o)["error"];
        assert_eq!(error["instance"], "/do/0/tryGetPet/try/0/getPet");
        assert_eq!(error["status"], 404);
        assert!(error["type"].is_string() && error["title"].is_string());

        let s =
            setup(Fake::default().with("http", Effect::Read, |_, _| Err(WorkflowError::communication(503, "down"))));
        let o = exec(&s, TRY_PET, json!({"petName": "Milou"})).await;
        let e = faulted(&o);
        assert_eq!((e.status, e.instance.as_deref()), (503, Some("/do/0/tryGetPet/try/0/getPet")));
        assert_eq!(s.log.lines("fault "), ["/do/0/tryGetPet/try/0/getPet", "/do/0/tryGetPet"]);
    }

    #[tokio::test]
    async fn ctk_fork_compete_has_one_winner_and_join_keeps_order() {
        let s = setup(Fake::default());
        let branches = "branches:\n          - setRed: { set: { colors: '${ .colors + [\"red\"] }' } }\n          - setGreen: { set: { colors: '${ .colors + [\"green\"] }' } }\n          - setBlue: { set: { colors: '${ .colors + [\"blue\"] }' } }\n";
        let o = exec(
            &s,
            &format!("do:\n  - branchWithCompete:\n      fork:\n        compete: true\n        {branches}"),
            json!({}),
        )
        .await;
        assert_eq!(completed(&o)["colors"].as_array().map(Vec::len), Some(1));
        let o = exec(&s, &format!("do:\n  - join:\n      fork:\n        {branches}"), json!({})).await;
        assert_eq!(completed(&o), &json!([{"colors": ["red"]}, {"colors": ["green"]}, {"colors": ["blue"]}]));
    }

    #[tokio::test]
    async fn ctk_raise_inline_error_at_its_reference() {
        let s = setup(Fake::default());
        let o = exec(&s, "do:\n  - raiseError: { raise: { error: { status: 400, type: 'https://open-workflow-specification.org/errors/types/compliance', title: Compliance Error } } }\n", json!({})).await;
        let e = faulted(&o);
        assert_eq!(
            serde_json::to_value(e).unwrap(),
            yaml(
                "status: 400\ntype: https://open-workflow-specification.org/errors/types/compliance\ntitle: Compliance Error\ninstance: /do/0/raiseError"
            )
        );
    }

    // Beyond the CTK.

    #[tokio::test]
    async fn context_export_if_and_workflow_transforms() {
        let s = setup(Fake::default());
        let o = exec(
            &s,
            r#"
input: { from: '{ n: .n }', schema: { document: { type: object, required: [ n ] } } }
output: { as: '{ total: ., seen: $context.seen }' }
do:
  - remember: { set: { v: '${ .n * 2 }' }, export: { as: '{ seen: .v }' } }
  - skipped: { if: '${ .v > 100 }', set: { v: 0 } }
  - sum: { set: '${ .v + $context.seen }' }
"#,
            json!({"n": 3, "noise": true}),
        )
        .await;
        assert_eq!(completed(&o), &json!({"total": 12, "seen": 6}));
        assert_eq!(s.log.lines("skip "), ["/do/1/skipped"]);

        let o = exec(
            &s,
            "input: { schema: { document: { type: object, required: [ n ] } } }\ndo: [ { a: { set: { x: 1 } } } ]\n",
            json!({}),
        )
        .await;
        assert!(faulted(&o).is("validation"));
    }

    #[tokio::test]
    async fn reusable_functions_run_as_tasks_on_the_call_arguments() {
        let s = setup(Fake::default().with("send", Effect::Write, |args, _| Ok(json!({ "sent": args["text"] }))));
        let o = exec(&s, "use:\n  functions:\n    notify: { call: send, with: { text: '${ \"hi \" + .name }' } }\ndo:\n  - greet: { call: notify, with: { name: '${ .user }' } }\n", json!({"user": "ann"})).await;
        assert_eq!(completed(&o), &json!({"sent": "hi ann"}));
        assert!(o.effects_started);
        assert_eq!(s.fake.calls.lock().unwrap()[0].2, "/do/0/greet/use/functions/notify");
    }

    const RETRYING: &str = r#"
do:
  - fetch:
      try: [ { get: { call: flaky } } ]
      catch:
        retry: { delay: { seconds: 1 }, backoff: { exponential: {} }, limit: { attempt: { count: 3 } } }
"#;

    #[tokio::test(start_paused = true)]
    async fn retries_back_off_and_stop_at_the_attempt_limit() {
        let s = setup(Fake::default().with("flaky", Effect::Read, |_, n| {
            if n < 2 { Err(WorkflowError::communication(503, "busy")) } else { Ok(json!("ok")) }
        }));
        assert_eq!(completed(&exec(&s, RETRYING, Value::Null).await), &json!("ok"));
        assert_eq!(s.log.lines("retry "), ["/do/0/fetch #1 after 1s", "/do/0/fetch #2 after 2s"]);

        // Three attempts fail: the error is handled (no catch.do), the input passes through.
        let s =
            setup(Fake::default().with("flaky", Effect::Read, |_, _| Err(WorkflowError::communication(503, "busy"))));
        assert_eq!(completed(&exec(&s, RETRYING, json!("in")).await), &json!("in"));
        assert_eq!(s.fake.calls.lock().unwrap().len(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn an_attempt_that_started_a_write_is_not_retried() {
        let s =
            setup(Fake::default().with("flaky", Effect::Write, |_, _| Err(WorkflowError::communication(503, "busy"))));
        exec(&s, RETRYING, Value::Null).await;
        assert_eq!(s.fake.calls.lock().unwrap().len(), 1);
        assert!(s.log.lines("retry ").is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn task_timeouts_are_catchable_errors() {
        let s = setup(Fake::default());
        let o = exec(&s, "do:\n  - slow: { wait: PT1M, timeout: { after: PT1S } }\n", Value::Null).await;
        let e = faulted(&o);
        assert!(e.is("timeout") && e.instance.as_deref() == Some("/do/0/slow"), "{e:?}");
        let o = exec(&s, "do:\n  - t: { try: [ { slow: { wait: PT1M, timeout: { after: PT1S } } } ], catch: { errors: { with: { status: 408 } }, do: [ { h: { set: { r: handled } } } ] } }\n", Value::Null).await;
        assert_eq!(completed(&o), &json!({"r": "handled"}));
    }

    #[tokio::test]
    async fn a_cycle_hits_the_task_limit_which_try_cannot_catch() {
        let mut s = setup(Fake::default());
        s.host.limits.max_tasks = 50;
        let o = exec(&s, "do:\n  - t:\n      try:\n        - a: { set: { x: 1 }, then: b }\n        - b: { set: { x: 2 }, then: a }\n      catch: { do: [ { h: { set: { r: caught } } } ] }\n", Value::Null).await;
        assert!(faulted(&o).detail.as_deref().is_some_and(|d| d.contains("more than 50 tasks")));
        assert_eq!(o.tasks_run, 51);
    }

    #[tokio::test]
    async fn cancellation_stops_a_run_and_reports_started_effects() {
        let s = setup(Fake::default().with("send", Effect::Write, |_, _| Ok(Value::Null)));
        let wf = model::parse_yaml(&format!(
            "{HEAD}do:\n  - a: {{ call: send }}\n  - b: {{ wait: PT1H }}\n  - c: {{ call: send }}\n"
        ))
        .unwrap();
        let cancel = CancellationToken::new();
        let stop = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            stop.cancel();
        });
        let o = run(&s.host, &wf, "run-1", Value::Null, &cancel).await;
        assert!(matches!(o.status, Status::Cancelled), "{o:?}");
        assert!(o.effects_started);
        assert_eq!(s.fake.calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn runtime_arguments_describe_the_task_and_the_workflow() {
        let s = setup(Fake::default());
        let o = exec(&s, "do:\n  - probe: { set: { task: '${ $task.reference }', name: '${ $task.name }', run: '${ $workflow.id }', at: '${ $task.startedAt.epoch.seconds }', rt: '${ $runtime.name }', raw: '${ $workflow.input }' } }\n", json!(7)).await;
        assert_eq!(
            completed(&o),
            &json!({"task": "/do/0/probe", "name": "probe", "run": "run-1", "at": 1_700_000_000, "rt": "owf-test", "raw": 7})
        );
    }
}
