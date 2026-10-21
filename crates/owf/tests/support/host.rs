// The host the properties run programs on: 1. the fake functions (pure
// behaviours the reference model shares); 2. the journal, one ordered log of
// observer events and function calls; 3. running a program on a paused
// clock, so waits, retries and timeouts take no real time.
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use owf::model::{self, Workflow};
use owf::run::{
    self, CallInfo, Clock, Effect, Event, Functions, Host, Limits, Observer, Outcome, Status, WorkflowError,
};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use super::program::{Program, definition};

// ── 1. fake functions ──────────────────────────────────────────────────────

/// The host's functions and their effect class.
pub fn effect(name: &str) -> Option<Effect> {
    match name {
        "inc" | "down" | "flaky" => Some(Effect::Read),
        "send" | "reject" => Some(Effect::Write),
        _ => None,
    }
}

/// What `name` answers to `args` when it was called `before` times already
/// in this run. Deterministic, so the model can answer the same.
pub fn behave(name: &str, args: &Value, before: usize) -> Result<Value, WorkflowError> {
    let n = args["n"].as_i64().unwrap_or(0);
    match name {
        "inc" => Ok(json!({ "n": n.saturating_add(1) })),
        "send" => Ok(json!({ "n": n })),
        "down" => Err(WorkflowError::communication(503, "the service is down")),
        "reject" => Err(WorkflowError::communication(400, "the request was rejected")),
        "flaky" if before < 2 => Err(WorkflowError::communication(503, "busy, try again")),
        "flaky" => Ok(json!({ "n": n.saturating_add(10) })),
        other => panic!("no host function `{other}`"),
    }
}

struct Fake {
    journal: Arc<Journal>,
    counts: Mutex<HashMap<String, usize>>,
    latency: Duration,
}

#[async_trait]
impl Functions for Fake {
    fn effect(&self, name: &str) -> Option<Effect> {
        effect(name)
    }

    async fn call(&self, name: &str, args: Value, info: &CallInfo<'_>) -> Result<Value, WorkflowError> {
        let before = {
            let mut counts = self.counts.lock().unwrap();
            let c = counts.entry(name.to_string()).or_default();
            *c += 1;
            *c - 1
        };
        self.journal.push(Entry::Call {
            name: name.into(),
            args: args.clone(),
            reference: info.reference.into(),
            write: effect(name) == Some(Effect::Write),
            after_cancel: info.cancel.is_cancelled(),
        });
        if !self.latency.is_zero() {
            tokio::time::sleep(self.latency).await;
        }
        behave(name, &args, before)
    }
}

// ── 2. the journal ─────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
pub enum Entry {
    Started { reference: String, parent: Option<String>, kind: String },
    Skipped { reference: String, parent: Option<String> },
    Completed { reference: String, output: Value },
    Faulted { reference: String, error: WorkflowError },
    Retrying { reference: String, attempt: u32 },
    Call { name: String, args: Value, reference: String, write: bool, after_cancel: bool },
}

#[derive(Default)]
pub struct Journal(Mutex<Vec<Entry>>);

impl Journal {
    fn push(&self, e: Entry) {
        self.0.lock().unwrap().push(e);
    }
}

impl Observer for Journal {
    fn event(&self, event: &Event<'_>) {
        self.push(match event {
            Event::TaskStarted { reference, parent, kind, .. } => Entry::Started {
                reference: reference.to_string(),
                parent: parent.map(str::to_string),
                kind: kind.to_string(),
            },
            Event::TaskSkipped { reference, parent } => {
                Entry::Skipped { reference: reference.to_string(), parent: parent.map(str::to_string) }
            }
            Event::TaskCompleted { reference, output } => {
                Entry::Completed { reference: reference.to_string(), output: (*output).clone() }
            }
            Event::TaskSuspended { .. } => panic!("ephemeral test host suspended"),
            Event::SwitchCase { .. } => return,
            Event::TaskFaulted { reference, error } => {
                Entry::Faulted { reference: reference.to_string(), error: (*error).clone() }
            }
            Event::TaskRetrying { reference, attempt, .. } => {
                Entry::Retrying { reference: reference.to_string(), attempt: *attempt }
            }
        });
    }
}

/// The steps a run took, as the model writes them: `start r`, `skip r`,
/// `retry r #k`.
pub fn trace(journal: &[Entry]) -> Vec<String> {
    journal
        .iter()
        .filter_map(|e| match e {
            Entry::Started { reference, .. } => Some(format!("start {reference}")),
            Entry::Skipped { reference, .. } => Some(format!("skip {reference}")),
            Entry::Retrying { reference, attempt } => Some(format!("retry {reference} #{attempt}")),
            _ => None,
        })
        .collect()
}

/// The host calls, by name and arguments.
pub fn calls(journal: &[Entry]) -> Vec<(String, Value)> {
    journal
        .iter()
        .filter_map(|e| match e {
            Entry::Call { name, args, .. } => Some((name.clone(), args.clone())),
            _ => None,
        })
        .collect()
}

// ── 3. running ─────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct Options {
    pub limits: Limits,
    /// How long every host call takes (virtual time). Every generated
    /// duration is an even number of milliseconds (the timer's resolution
    /// is one), so a cancel at an odd millisecond never ties a timer.
    pub latency: Duration,
    /// Cancel the run at this virtual instant.
    pub cancel_at: Option<Duration>,
}

impl Default for Options {
    fn default() -> Self {
        Self { limits: limits(), latency: Duration::from_millis(2), cancel_at: None }
    }
}

/// Small enough that a cycle ends fast, large enough for real programs.
pub fn limits() -> Limits {
    Limits { max_tasks: 60, max_attempts: 3, max_sleep: None, max_listen: None }
}

#[derive(Debug)]
pub struct Ran {
    pub outcome: Outcome,
    pub journal: Vec<Entry>,
    /// Virtual time from start to finish.
    pub elapsed: Duration,
}

struct Fixed;

impl Clock for Fixed {
    fn now(&self) -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).expect("a valid timestamp")
    }
}

/// Parses and validates the program's definition; a generated program that
/// fails either is a generator bug.
pub fn workflow(p: &Program) -> Workflow {
    let def = definition(p);
    let wf = model::parse(def.clone()).unwrap_or_else(|e| panic!("{e}\n{}", pretty(&def)));
    run::validate(&wf, &Known).unwrap_or_else(|e| panic!("{e}\n{}", pretty(&def)));
    wf
}

pub fn pretty(v: &Value) -> String {
    serde_yaml_ng::to_string(v).unwrap_or_else(|_| v.to_string())
}

/// The host's functions as a catalog, for `run::validate`.
pub struct Known;

#[async_trait]
impl Functions for Known {
    fn effect(&self, name: &str) -> Option<Effect> {
        effect(name)
    }
    async fn call(&self, _: &str, _: Value, _: &CallInfo<'_>) -> Result<Value, WorkflowError> {
        unreachable!("validation only")
    }
}

pub fn execute(p: &Program, input: Value, opts: &Options) -> Ran {
    execute_workflow(&workflow(p), input, opts)
}

/// Runs on a fresh paused runtime. A run that does not finish within a
/// virtual day hangs: nothing generated waits that long.
pub fn execute_workflow(wf: &Workflow, input: Value, opts: &Options) -> Ran {
    try_execute_workflow(wf, input, opts).unwrap_or_else(|| panic!("the run hangs"))
}

/// `None` when the run does not finish within a virtual day.
pub fn try_execute_workflow(wf: &Workflow, input: Value, opts: &Options) -> Option<Ran> {
    let rt = tokio::runtime::Builder::new_current_thread().enable_time().start_paused(true).build().expect("a runtime");
    rt.block_on(async {
        let journal = Arc::new(Journal::default());
        let fake = Arc::new(Fake { journal: journal.clone(), counts: Mutex::default(), latency: opts.latency });
        let host = Host {
            functions: fake,
            observer: journal.clone(),
            clock: Arc::new(Fixed),
            limits: opts.limits.clone(),
            runtime: json!({ "name": "owf-properties" }),
        };
        let cancel = CancellationToken::new();
        if let Some(at) = opts.cancel_at {
            let stop = cancel.clone();
            tokio::spawn(async move {
                tokio::time::sleep(at).await;
                stop.cancel();
            });
        }
        let began = tokio::time::Instant::now();
        let outcome = tokio::time::timeout(Duration::from_secs(86_400), run::run(&host, wf, "run-1", input, &cancel))
            .await
            .ok()?;
        let elapsed = began.elapsed();
        let journal = journal.0.lock().unwrap().clone();
        Some(Ran { outcome, journal, elapsed })
    })
}

/// The outcome without references: what a rewrite must preserve.
#[derive(Clone, Debug, PartialEq)]
pub enum Observed {
    Completed(Value),
    Faulted { type_: String, status: u16 },
    Cancelled,
}

pub fn observed(o: &Outcome) -> Observed {
    match &o.status {
        Status::Completed(v) => Observed::Completed(v.clone()),
        Status::Faulted(e) => Observed::Faulted { type_: e.type_.clone(), status: e.status },
        other => {
            assert!(matches!(other, Status::Cancelled), "a non-durable run suspended: {other:?}");
            Observed::Cancelled
        }
    }
}

/// Whether the run stopped at the host's task ceiling.
pub fn hit_limit(o: &Outcome) -> bool {
    matches!(&o.status, Status::Faulted(e) if is_limit(e))
}

pub fn is_limit(e: &WorkflowError) -> bool {
    e.is("runtime") && e.detail.as_deref().is_some_and(|d| d.contains("tasks ran"))
}

/// Case counts: small by default, `PROPERTY_RUNS` raises them.
pub fn cases(default: u32) -> u32 {
    std::env::var("PROPERTY_RUNS").ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// Keep failures reproducible and shrinking bounded even for nested programs.
pub fn config(default: u32, source: &'static str) -> proptest::test_runner::Config {
    proptest::test_runner::Config {
        cases: cases(default),
        source_file: Some(source),
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::WithSource(
            "proptest-regressions",
        ))),
        max_shrink_time: 10_000,
        ..proptest::test_runner::Config::default()
    }
}
