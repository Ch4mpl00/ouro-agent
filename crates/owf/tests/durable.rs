use async_trait::async_trait;
use chrono::{DateTime, Utc};
use owf::{
    durable::{CallFailure, Journal, Record, check_transition},
    model,
    run::{self, CallInfo, Clock, Effect, Functions, Host, Limits, Silent, Status, WorkflowError},
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, AtomicUsize, Ordering},
    },
};
use tokio_util::sync::CancellationToken;

mod support;

#[derive(Default)]
struct Store {
    records: Mutex<BTreeMap<String, Record>>,
    saves: AtomicUsize,
    fail_at: AtomicUsize,
    after_commit: bool,
}
impl Journal for Store {
    fn load(&self) -> anyhow::Result<BTreeMap<String, Record>> {
        Ok(self.records.lock().unwrap().clone())
    }
    fn save(&self, key: &str, record: &Record) -> anyhow::Result<()> {
        let n = self.saves.fetch_add(1, Ordering::SeqCst) + 1;
        let fail = n == self.fail_at.load(Ordering::SeqCst);
        if fail && !self.after_commit {
            anyhow::bail!("injected crash before commit {n}");
        }
        let mut records = self.records.lock().unwrap();
        if let Some(old) = records.get(key) {
            check_transition(old, record)?;
        }
        records.insert(key.into(), record.clone());
        if fail {
            anyhow::bail!("injected crash after commit {n}");
        }
        Ok(())
    }
}

struct Time(AtomicI64);
impl Clock for Time {
    fn now(&self) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(self.0.load(Ordering::SeqCst)).unwrap()
    }
}

#[derive(Default)]
struct World(Mutex<Vec<(String, String)>>);
#[async_trait]
impl Functions for World {
    fn effect(&self, _: &str) -> Option<Effect> {
        Some(Effect::Write)
    }
    async fn call(&self, _: &str, args: Value, _: &CallInfo<'_>) -> Result<Value, WorkflowError> {
        Ok(args)
    }
    async fn invoke(&self, name: &str, args: Value, info: &CallInfo<'_>) -> Result<Value, CallFailure> {
        let mut calls = self.0.lock().unwrap();
        let before = calls.iter().filter(|(n, _)| n == name).count();
        calls.push((name.into(), info.reference.into()));
        let error = || WorkflowError::communication(503, "injected failure");
        match name {
            "unknown" => Err(CallFailure::Unknown(error())),
            "not_sent" if before == 0 => Err(CallFailure::NotExecuted(error())),
            "flaky" if !info.reference.contains("try[2]") => Err(CallFailure::Response(error())),
            _ => Ok(args),
        }
    }
}

fn workflow(body: &str) -> model::Workflow {
    model::parse_yaml(&format!("document: {{ dsl: '1.0.4', namespace: t, name: durable, version: '0.1.0' }}\n{body}"))
        .unwrap()
}
fn host(world: Arc<World>, time: Arc<Time>) -> Host {
    Host {
        functions: world,
        clock: time,
        observer: Arc::new(Silent),
        limits: Limits::default(),
        runtime: json!({"name":"test"}),
    }
}
async fn drive(host: &Host, time: &Time, wf: &model::Workflow, store: &Store) -> Status {
    for _ in 0..100 {
        match run::resume(host, wf, "run", json!({}), &CancellationToken::new(), store).await.status {
            Status::Waiting { wake_at_ms } => time.0.store(wake_at_ms, Ordering::SeqCst),
            Status::Interrupted(_) => {}
            terminal => return terminal,
        }
    }
    panic!("recovery did not finish");
}

const BODY: &str = r#"
use:
  functions:
    wrap: { call: echo, with: { n: '${ .n }' } }
do:
  - init: { set: { n: 0 }, export: { as: '${ . }' } }
  - loop:
      for: { each: x, in: [1, 2] }
      do:
        - add: { call: wrap, with: { n: '${ $context.n + $x }' }, export: { as: '${ . }' } }
        - pause: { wait: PT0.001S }
  - retry:
      try: [ { fetch: { call: flaky } } ]
      catch:
        retry: { delay: PT0.001S, limit: { attempt: { count: 2 } } }
  - parallel:
      fork:
        branches:
          - a: { do: [ { delay: { wait: PT0.002S } }, { fetch: { call: echo, with: { branch: a } } } ] }
          - b: { call: echo, with: { branch: b } }
  - race:
      fork:
        compete: true
        branches:
          - slow: { do: [ { delay: { wait: PT1S } }, { fetch: { call: echo, with: { winner: slow } } } ] }
          - fast: { call: echo, with: { winner: fast } }
  - done: { set: '${ $context }' }
"#;

#[tokio::test]
async fn every_checkpoint_recovers_before_and_after_commit() {
    let wf = workflow(BODY);
    let baseline = Store::default();
    let time = Arc::new(Time(AtomicI64::new(1_000)));
    let h = host(Arc::new(World::default()), time.clone());
    assert_eq!(drive(&h, &time, &wf, &baseline).await, Status::Completed(json!({"n":3})));
    let count = baseline.saves.load(Ordering::SeqCst);
    for after_commit in [false, true] {
        for fail_at in 1..=count {
            let store = Store { fail_at: AtomicUsize::new(fail_at), after_commit, ..Default::default() };
            let time = Arc::new(Time(AtomicI64::new(1_000)));
            let h = host(Arc::new(World::default()), time.clone());
            assert_eq!(
                drive(&h, &time, &wf, &store).await,
                Status::Completed(json!({"n":3})),
                "crash {fail_at}, after={after_commit}"
            );
            // A terminal result does not execute anything or rewrite checkpoints.
            let saves = store.saves.load(Ordering::SeqCst);
            assert_eq!(drive(&h, &time, &wf, &store).await, Status::Completed(json!({"n":3})));
            assert_eq!(store.saves.load(Ordering::SeqCst), saves);
        }
    }
}

#[tokio::test]
async fn at_most_once_never_dispatches_twice_at_any_crash_boundary() {
    let wf = workflow(
        r#"do:
  - loop:
      for: { in: [1, 2, 3] }
      do:
        - send: { call: echo, metadata: { delivery: at_most_once } }
"#,
    );
    for after_commit in [false, true] {
        for fail_at in 1..=30 {
            let store = Store { fail_at: AtomicUsize::new(fail_at), after_commit, ..Default::default() };
            let world = Arc::new(World::default());
            let time = Arc::new(Time(AtomicI64::new(1_000)));
            let result = drive(&host(world.clone(), time.clone()), &time, &wf, &store).await;
            assert!(
                matches!(result, Status::Completed(_))
                    || matches!(result, Status::Faulted(ref e) if e.is("outcome_unknown")),
                "{result:?}"
            );
            let mut counts = BTreeMap::new();
            for (_, reference) in world.0.lock().unwrap().iter() {
                *counts.entry(reference.clone()).or_insert(0) += 1;
            }
            assert!(counts.values().all(|n| *n == 1), "crash {fail_at}, after={after_commit}: {counts:?}");
        }
    }
}

#[tokio::test]
async fn unknown_bypasses_catch_but_proven_non_dispatch_can_retry() {
    for name in ["unknown", "not_sent"] {
        let wf = workflow(&format!(
            r#"do:
  - guarded:
      try: [ {{ send: {{ call: {name}, metadata: {{ delivery: at_most_once }} }} }} ]
      catch:
        retry: {{ limit: {{ attempt: {{ count: 3 }} }} }}
        do: [ {{ fallback: {{ call: echo }} }} ]
"#
        ));
        let world = Arc::new(World::default());
        let time = Arc::new(Time(AtomicI64::new(1_000)));
        let result = drive(&host(world.clone(), time.clone()), &time, &wf, &Store::default()).await;
        let calls = world.0.lock().unwrap();
        if name == "unknown" {
            assert!(matches!(result, Status::Faulted(ref e) if e.is("outcome_unknown")));
            assert_eq!(calls.len(), 1);
        } else {
            assert!(matches!(result, Status::Completed(_)), "{result:?}");
            assert_eq!(calls.len(), 2);
        }
    }
}

#[tokio::test]
async fn invalid_identity_and_corrupt_state_fail_without_dispatch() {
    let wf = workflow("do: [ { send: { call: echo } } ]");
    let world = Arc::new(World::default());
    let time = Arc::new(Time(AtomicI64::new(1_000)));
    let h = host(world.clone(), time.clone());
    let store = Store::default();
    drive(&h, &time, &wf, &store).await;
    let before = world.0.lock().unwrap().len();
    let different = workflow("do: [ { send: { call: echo, with: { changed: true } } } ]");
    assert!(matches!(drive(&h, &time, &different, &store).await, Status::Faulted(_)));
    store.records.lock().unwrap().remove("execution");
    let result = run::resume(&h, &wf, "run", json!({}), &CancellationToken::new(), &store).await;
    assert!(matches!(result.status, Status::Interrupted(_)));
    assert_eq!(world.0.lock().unwrap().len(), before);
}

#[tokio::test]
async fn then_visits_keep_their_identity_across_durable_waits() {
    let wf = workflow(
        r#"do:
  - init: { set: { n: 0 } }
  - send: { call: echo, with: { n: '${ .n + 1 }' }, metadata: { delivery: at_most_once } }
  - wait: { wait: PT0.001S }
  - again: { switch: [ { more: { when: '.n < 3', then: send } }, { done: { then: end } } ] }
"#,
    );
    let world = Arc::new(World::default());
    let time = Arc::new(Time(AtomicI64::new(1_000)));
    assert_eq!(
        drive(&host(world.clone(), time.clone()), &time, &wf, &Store::default()).await,
        Status::Completed(json!({"n":3}))
    );
    let calls = world.0.lock().unwrap();
    assert_eq!(
        calls.iter().map(|(_, reference)| reference.as_str()).collect::<Vec<_>>(),
        ["/do/1/send", "/do/1/send[1]", "/do/1/send[2]"]
    );
}

#[tokio::test]
async fn saved_call_response_is_reused_when_the_tool_disappears_before_task_commit() {
    struct Gone;
    #[async_trait]
    impl Functions for Gone {
        fn effect(&self, _: &str) -> Option<Effect> {
            None
        }
        async fn call(&self, _: &str, _: Value, _: &CallInfo<'_>) -> Result<Value, WorkflowError> {
            panic!("must use saved response")
        }
    }
    let wf = workflow("do: [ { send: { call: echo, with: { n: 7 }, metadata: { delivery: at_most_once } } } ]");
    // execution, task start, intent, response, then crash before task commit.
    let store = Store { fail_at: AtomicUsize::new(5), ..Default::default() };
    let time = Arc::new(Time(AtomicI64::new(1_000)));
    let mut h = host(Arc::new(World::default()), time.clone());
    assert!(matches!(
        run::resume(&h, &wf, "run", json!({}), &CancellationToken::new(), &store).await.status,
        Status::Interrupted(_)
    ));
    h.functions = Arc::new(Gone);
    assert_eq!(drive(&h, &time, &wf, &store).await, Status::Completed(json!({"n":7})));
}

#[test]
fn delivery_is_validated_as_call_metadata() {
    let parse = |task: &str| {
        model::parse_yaml(&format!(
            "document: {{ dsl: '1.0.4', namespace: t, name: w, version: '0.1.0' }}\ndo: [ {{ t: {task} }} ]"
        ))
    };
    assert!(parse("{ call: echo, metadata: { delivery: at_most_once } }").is_ok());
    assert!(parse("{ call: echo, metadata: { delivery: exactly_once } }").is_err());
    assert!(parse("{ set: { x: 1 }, metadata: { delivery: at_most_once } }").is_err());
}

#[tokio::test(start_paused = true)]
async fn a_timeout_cannot_turn_an_unknown_dispatch_into_a_catchable_retry() {
    struct Hanging(AtomicUsize);
    #[async_trait]
    impl Functions for Hanging {
        fn effect(&self, _: &str) -> Option<Effect> {
            Some(Effect::Read)
        }
        async fn call(&self, _: &str, _: Value, _: &CallInfo<'_>) -> Result<Value, WorkflowError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            std::future::pending().await
        }
    }
    let wf = workflow(
        r#"do:
  - guarded:
      try: [ { send: { call: slow, timeout: { after: PT0.001S }, metadata: { delivery: at_most_once } } } ]
      catch: { retry: { limit: { attempt: { count: 5 } } }, do: [ { fallback: { set: { fallback: true } } } ] }
"#,
    );
    let calls = Arc::new(Hanging(AtomicUsize::new(0)));
    let time = Arc::new(Time(AtomicI64::new(1_000)));
    let mut h = host(Arc::new(World::default()), time.clone());
    h.functions = calls.clone();
    let store = Store::default();
    let result = drive(&h, &time, &wf, &store).await;
    assert!(matches!(result, Status::Faulted(ref error) if error.is("outcome_unknown")));
    assert_eq!(drive(&h, &time, &wf, &store).await, result);
    assert_eq!(calls.0.load(Ordering::SeqCst), 1);
    let crashed = Store { fail_at: AtomicUsize::new(4), after_commit: true, ..Default::default() };
    assert!(matches!(
        run::resume(&h, &wf, "run", json!({}), &CancellationToken::new(), &crashed).await.status,
        Status::Interrupted(_)
    ));
    // Recovery after the timeout still reports the unresolved dispatch,
    // instead of letting catch turn the elapsed deadline into a retry.
    time.0.store(2_000, Ordering::SeqCst);
    assert!(matches!(drive(&h, &time, &wf, &crashed).await,
        Status::Faulted(ref error) if error.is("outcome_unknown")));
    assert_eq!(calls.0.load(Ordering::SeqCst), 1);
}

// Reuse the interpreter's shrinkable DSL generator. Only the crash schedule,
// delivery policies and durable assertions belong to this test target.
#[derive(Default)]
struct PropertyWorld(Mutex<Vec<(String, owf::durable::Delivery)>>);

#[async_trait]
impl Functions for PropertyWorld {
    fn effect(&self, name: &str) -> Option<Effect> {
        support::host::effect(name)
    }
    async fn call(&self, name: &str, args: Value, info: &CallInfo<'_>) -> Result<Value, WorkflowError> {
        self.0.lock().unwrap().push((info.reference.into(), info.delivery));
        // Stable responses isolate recovery from changing external data.
        // "flaky" succeeds here; down/reject still return real DSL errors.
        support::host::behave(name, &args, 2)
    }
    async fn invoke(&self, name: &str, args: Value, info: &CallInfo<'_>) -> Result<Value, CallFailure> {
        self.call(name, args, info).await.map_err(CallFailure::Response)
    }
}

#[derive(Debug)]
struct Recovered {
    status: Status,
    completed: BTreeMap<String, owf::durable::TaskResult>,
    crashes: usize,
}

fn recover_generated(
    wf: &model::Workflow,
    input: Value,
    crashes: &[(u8, bool)],
    cancelled: bool,
) -> Result<Recovered, String> {
    use owf::durable::{CallState, Delivery};
    let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
    rt.block_on(async {
        let time = Arc::new(Time(AtomicI64::new(1_000)));
        let world = Arc::new(PropertyWorld::default());
        let mut store = Store::default();
        let mut next_crash = 0;
        let mut armed = false;
        for _ in 0..512 {
            if !armed && let Some(&(distance, after)) = crashes.get(next_crash) {
                store.fail_at.store(store.saves.load(Ordering::SeqCst) + usize::from(distance), Ordering::SeqCst);
                store.after_commit = after;
                armed = true;
            }
            let before = world.0.lock().unwrap().len();
            let saved = store.load().unwrap();
            let host = Host {
                functions: world.clone(),
                clock: time.clone(),
                observer: Arc::new(Silent),
                limits: support::host::limits(),
                runtime: json!({"name":"durable-properties"}),
            };
            let cancel = CancellationToken::new();
            if cancelled {
                cancel.cancel();
            }
            let status = run::resume(&host, wf, "property-run", input.clone(), &cancel, &store).await.status;
            let calls = world.0.lock().unwrap().clone();
            for (reference, _) in &calls[before..] {
                if matches!(
                    saved.get(&format!("{reference}#call")),
                    Some(Record::Call { state: CallState::Done(_), .. })
                ) {
                    return Err(format!("a checkpointed call ran again: {reference}"));
                }
            }
            let mut dispatched = std::collections::BTreeSet::new();
            for (reference, delivery) in &calls {
                if *delivery == Delivery::AtMostOnce && !dispatched.insert(reference) {
                    return Err(format!("at-most-once invocation dispatched twice: {reference}"));
                }
            }
            match status {
                Status::Waiting { wake_at_ms } => time.0.store(wake_at_ms, Ordering::SeqCst),
                Status::Interrupted(ref error) => {
                    if !armed || store.saves.load(Ordering::SeqCst) != store.fail_at.load(Ordering::SeqCst) {
                        return Err(format!("unexpected storage interruption: {error}"));
                    }
                    next_crash += 1;
                    armed = false;
                    store.fail_at.store(0, Ordering::SeqCst);
                }
                terminal => {
                    let saves = store.saves.load(Ordering::SeqCst);
                    // Late cancellation and repeated recovery cannot reopen a
                    // terminal run, dispatch anything, or rewrite its records.
                    for late_cancel in [false, true, false] {
                        let cancel = CancellationToken::new();
                        if late_cancel {
                            cancel.cancel();
                        }
                        let replay = run::resume(&host, wf, "property-run", input.clone(), &cancel, &store).await;
                        if replay.status != terminal
                            || store.saves.load(Ordering::SeqCst) != saves
                            || *world.0.lock().unwrap() != calls
                        {
                            return Err(format!("terminal run changed: {terminal:?} -> {replay:?}"));
                        }
                    }
                    let completed = store
                        .load()
                        .unwrap()
                        .into_iter()
                        .filter_map(|(key, record)| match record {
                            Record::Task { result: Some(result), .. } => Some((key, result)),
                            _ => None,
                        })
                        .collect();
                    return Ok(Recovered { status: terminal, completed, crashes: next_crash });
                }
            }
            // A new worker sees only serialized state, not the old Run/Host
            // or any in-memory interpreter context. External effects persist.
            let records = serde_json::from_slice(&serde_json::to_vec(&store.load().unwrap()).unwrap()).unwrap();
            store = Store {
                records: Mutex::new(records),
                saves: AtomicUsize::new(store.saves.load(Ordering::SeqCst)),
                fail_at: AtomicUsize::new(store.fail_at.load(Ordering::SeqCst)),
                after_commit: store.after_commit,
            };
        }
        Err("recovery exceeded 512 resumptions".into())
    })
}

fn delivery_policies(value: &mut Value, policies: &[bool], index: &mut usize) {
    match value {
        Value::Object(object) => {
            if object.get("call").and_then(Value::as_str).is_some_and(|name| support::program::HOST.contains(&name)) {
                let policy = if policies[*index % policies.len()] { "at_most_once" } else { "at_least_once" };
                object.insert("metadata".into(), json!({"delivery":policy}));
                *index += 1;
            }
            for child in object.values_mut() {
                delivery_policies(child, policies, index);
            }
        }
        Value::Array(array) => {
            for child in array {
                delivery_policies(child, policies, index);
            }
        }
        _ => {}
    }
}

#[test]
fn generated_workflows_preserve_output_context_and_errors_across_multiple_crashes() {
    use proptest::{
        prelude::*,
        test_runner::{TestCaseError, TestRunner},
    };
    use support::program::{Profile, input, program};
    // Sequential deterministic programs give an exact output/context oracle.
    // Fork scheduling and mixed delivery are exercised by the safety property.
    let profile = Profile { wait: true, ..Profile::CORE };
    let strategy = (program(profile), input(), prop::collection::vec((1u8..=16, any::<bool>()), 1..=8));
    TestRunner::new(support::host::config(64, file!()))
        .run(&strategy, |(p, input, crashes)| {
            let wf = support::host::workflow(&p);
            let baseline = recover_generated(&wf, input.clone(), &[], false).map_err(TestCaseError::fail)?;
            let recovered = recover_generated(&wf, input, &crashes, false).map_err(TestCaseError::fail)?;
            prop_assert_eq!(recovered.status, baseline.status);
            prop_assert_eq!(recovered.completed, baseline.completed);
            Ok(())
        })
        .unwrap();
}

#[test]
fn generated_forks_and_mixed_delivery_never_repeat_protected_calls_or_reopen_terminal_runs() {
    use proptest::{
        prelude::*,
        test_runner::{TestCaseError, TestRunner},
    };
    use support::program::{Profile, input, program};
    let profile = Profile { timeouts: false, ..Profile::FULL };
    let strategy = (
        program(profile),
        input(),
        prop::collection::vec((1u8..=16, any::<bool>()), 1..=8),
        prop::collection::vec(any::<bool>(), 1..=16),
        prop::bool::weighted(0.1),
    );
    TestRunner::new(support::host::config(64, file!()))
        .run(&strategy, |(p, input, crashes, policies, cancelled)| {
            let mut definition = support::program::definition(&p);
            delivery_policies(&mut definition, &policies, &mut 0);
            let wf = model::parse(definition).unwrap();
            run::validate(&wf, &support::host::Known).unwrap();
            let result = recover_generated(&wf, input, &crashes, cancelled).map_err(TestCaseError::fail)?;
            if cancelled {
                prop_assert_eq!(result.status, Status::Cancelled);
            }
            Ok(())
        })
        .unwrap();
}

#[test]
fn repeated_crashes_can_hit_the_same_pending_checkpoint_before_progress() {
    use support::program::{Body, Num, Program, task};
    let p = Program {
        input_from: None,
        output_as: None,
        functions: vec![],
        bare: false,
        tasks: vec![task("send", Body::Call("inc".into(), Num::N))],
    };
    let wf = support::host::workflow(&p);
    let crashes = [(1, false), (1, true), (1, false), (1, true), (1, false), (1, true)];
    let result = recover_generated(&wf, json!({"n":0}), &crashes, false).unwrap();
    assert_eq!(result.crashes, crashes.len());
    assert_eq!(result.status, Status::Completed(json!({"n":1})));
}

// ── retries of protected calls ─────────────────────────────────────────────
//
// A DSL retry attempt gives a write (or any at-most-once call) the first
// attempt's identity: one that succeeded is reused, not sent again; one that
// failed is sent again; other arguments under that identity are a conflict.
// Reads and model steps run afresh. Every scenario runs twice: `PT0S` retries
// in the same interpreter, `PT0.001S` suspends between attempts, so each
// attempt starts in a fresh interpreter replaying the journal.

type Script = dyn Fn(&str, &Value, usize) -> Result<Value, CallFailure> + Send + Sync;

// `send*` are writes, everything else reads. The script answers from the
// function, its arguments and how many times it was dispatched before.
struct Tools {
    script: Box<Script>,
    dispatched: Mutex<Vec<(String, Value, bool)>>,
}
impl Tools {
    fn new(script: impl Fn(&str, &Value, usize) -> Result<Value, CallFailure> + Send + Sync + 'static) -> Arc<Self> {
        Arc::new(Tools { script: Box::new(script), dispatched: Mutex::default() })
    }
    fn count(&self, name: &str) -> usize {
        self.dispatched.lock().unwrap().iter().filter(|(n, ..)| n == name).count()
    }
    // Dispatches the tool answered with success.
    fn succeeded(&self, name: &str) -> Vec<Value> {
        self.dispatched
            .lock()
            .unwrap()
            .iter()
            .filter(|(n, _, ok)| n == name && *ok)
            .map(|(_, a, _)| a.clone())
            .collect()
    }
}
#[async_trait]
impl Functions for Tools {
    fn effect(&self, name: &str) -> Option<Effect> {
        Some(if name.starts_with("send") { Effect::Write } else { Effect::Read })
    }
    async fn call(&self, name: &str, args: Value, info: &CallInfo<'_>) -> Result<Value, WorkflowError> {
        self.invoke(name, args, info).await.map_err(|failure| match failure {
            CallFailure::NotExecuted(e) | CallFailure::Response(e) | CallFailure::Unknown(e) => e,
        })
    }
    async fn invoke(&self, name: &str, args: Value, _: &CallInfo<'_>) -> Result<Value, CallFailure> {
        let before = self.count(name);
        let result = (self.script)(name, &args, before);
        self.dispatched.lock().unwrap().push((name.into(), args, result.is_ok()));
        result
    }
}

fn rejected() -> CallFailure {
    CallFailure::Response(WorkflowError::communication(500, "rejected"))
}

// Every send succeeds; `read_check` fails on its first dispatch, so the try
// around it runs a second attempt.
fn check_fails_once(name: &str, args: &Value, before: usize) -> Result<Value, CallFailure> {
    match (name, before) {
        ("read_check", 0) => Err(rejected()),
        _ => Ok(args.clone()),
    }
}

fn tools_host(tools: Arc<Tools>, time: Arc<Time>) -> Host {
    Host {
        functions: tools,
        clock: time,
        observer: Arc::new(Silent),
        limits: Limits::default(),
        runtime: json!({"name":"test"}),
    }
}

const DELAYS: [&str; 2] = ["PT0S", "PT0.001S"];

// Runs `body` (with `DELAY` replaced) durably to its end.
async fn retried(tools: &Arc<Tools>, body: &str, delay: &str) -> Status {
    let time = Arc::new(Time(AtomicI64::new(1_000)));
    let h = tools_host(tools.clone(), time.clone());
    drive(&h, &time, &workflow(&body.replace("DELAY", delay)), &Store::default()).await
}

const SEND_THEN_CHECK: &str = r#"
do:
  - guard:
      try:
        - send: { call: send, with: { text: hi }, export: { as: '${ $context + { sent: . } }' } }
        - check: { call: read_check }
      catch:
        retry: { delay: DELAY, limit: { attempt: { count: 3 } } }
  - done: { set: '${ $context }' }
"#;

#[tokio::test]
async fn a_write_that_succeeded_is_not_sent_again_when_a_later_step_retries() {
    for delay in DELAYS {
        let tools = Tools::new(check_fails_once);
        let status = retried(&tools, SEND_THEN_CHECK, delay).await;
        // The second attempt sees the first one's response as its own.
        assert_eq!(status, Status::Completed(json!({"sent":{"text":"hi"}})), "{delay}");
        assert_eq!(tools.count("send"), 1, "{delay}");
        assert_eq!(tools.count("read_check"), 2, "{delay}: the read did run again");
    }
}

#[tokio::test]
async fn a_write_that_failed_is_sent_again_by_the_retry() {
    let body = r#"
do:
  - guard:
      try: [ { send: { call: send, with: { text: hi } } } ]
      catch:
        retry: { delay: DELAY, limit: { attempt: { count: 3 } } }
"#;
    fn lost() -> CallFailure {
        CallFailure::Unknown(WorkflowError::communication(503, "connection reset"))
    }
    type Failure = fn() -> CallFailure;
    let failures: [(&str, Failure); 2] = [("a tool error", rejected), ("a lost response", lost)];
    for delay in DELAYS {
        for (case, failure) in failures {
            let tools = Tools::new(move |_, args, before| if before == 0 { Err(failure()) } else { Ok(args.clone()) });
            let status = retried(&tools, body, delay).await;
            assert_eq!(status, Status::Completed(json!({"text":"hi"})), "{case} {delay}");
            assert_eq!(tools.count("send"), 2, "{case} {delay}: the failed send is retried");
            assert_eq!(tools.succeeded("send").len(), 1, "{case} {delay}");
        }
    }
}

#[tokio::test]
async fn reads_run_afresh_on_every_attempt() {
    let body = r#"
do:
  - guard:
      try:
        - data: { call: read_data, export: { as: '${ $context + { data: . } }' } }
        - check: { call: read_check }
      catch:
        retry: { delay: DELAY, limit: { attempt: { count: 3 } } }
  - done: { set: '${ $context }' }
"#;
    for delay in DELAYS {
        let tools = Tools::new(|name, args, before| match name {
            "read_data" => Ok(json!({"version": before})),
            _ => check_fails_once(name, args, before),
        });
        let status = retried(&tools, body, delay).await;
        assert_eq!(status, Status::Completed(json!({"data":{"version":1}})), "{delay}: the second read's answer");
        assert_eq!(tools.count("read_data"), 2, "{delay}");
    }
}

#[tokio::test]
async fn other_arguments_for_a_sent_write_are_a_conflict_and_nothing_is_sent() {
    // The text comes from a step that answers differently each time, as a
    // model regenerating it would.
    let body = r#"
do:
  - guard:
      try:
        - compose: { call: compose }
        - send: { call: send, with: { text: '${ .text }' } }
        - check: { call: read_check }
      catch:
        retry: { delay: DELAY, limit: { attempt: { count: 3 } } }
"#;
    for delay in DELAYS {
        let tools = Tools::new(|name, args, before| match name {
            "compose" => Ok(json!({"text": format!("draft {before}")})),
            _ => check_fails_once(name, args, before),
        });
        let status = retried(&tools, body, delay).await;
        let Status::Faulted(error) = &status else { panic!("{delay}: {status:?}") };
        assert!(error.is("conflict"), "{delay}: {error:?}");
        assert_eq!(error.instance.as_deref(), Some("/do/0/guard/try[2]/1/send"), "{delay}");
        assert_eq!(tools.succeeded("send"), [json!({"text":"draft 0"})], "{delay}: only the first text went out");
        assert_eq!(tools.count("compose"), 2, "{delay}");
    }
}

#[tokio::test]
async fn loop_iterations_stay_distinct_writes_across_retries() {
    // Each iteration is its own write, even with equal arguments and even in
    // a loop named `try`, whose iterations' paths look like attempts.
    for name in ["each", "try"] {
        let body = format!(
            r#"
do:
  - guard:
      try:
        - {name}:
            for: {{ each: x, in: [1, 2, 3] }}
            do: [ {{ send: {{ call: send, with: {{ text: same }} }} }} ]
        - check: {{ call: read_check }}
      catch:
        retry: {{ delay: DELAY, limit: {{ attempt: {{ count: 3 }} }} }}
"#
        );
        for delay in DELAYS {
            let tools = Tools::new(check_fails_once);
            let status = retried(&tools, &body, delay).await;
            assert!(matches!(status, Status::Completed(_)), "{name} {delay}: {status:?}");
            assert_eq!(tools.count("send"), 3, "{name} {delay}: one per iteration, none repeated");
        }
    }
}

#[tokio::test]
async fn then_visits_stay_distinct_writes_across_retries() {
    // A `then` loop sends once per visit; the retry repeats none of them.
    let body = r#"
do:
  - guard:
      try:
        - init: { set: { n: 0 } }
        - send:
            call: send
            with: { text: same }
            export: { as: '${ { n: ($context.n // 0) + 1 } }' }
        - again:
            switch:
              - more: { when: '${ $context.n < 3 }', then: send }
              - stop: { then: continue }
        - check: { call: read_check }
      catch:
        retry: { delay: DELAY, limit: { attempt: { count: 3 } } }
"#;
    for delay in DELAYS {
        let tools = Tools::new(check_fails_once);
        let status = retried(&tools, body, delay).await;
        assert!(matches!(status, Status::Completed(_)), "{delay}: {status:?}");
        assert_eq!(tools.count("send"), 3, "{delay}");
    }
}

const NESTED: &str = r#"
do:
  - outer:
      try:
        - inner:
            try: [ { send: { call: send, with: { text: hi } } } ]
            catch:
              retry: { delay: DELAY, limit: { attempt: { count: 3 } } }
        - check: { call: read_check }
      catch:
        retry: { delay: DELAY, limit: { attempt: { count: 3 } } }
"#;

#[tokio::test]
async fn nested_retries_reuse_a_write_the_inner_retry_got_through() {
    // The inner try needs two attempts to send; then the outer one retries.
    // Its inner try finds the send already done, in whichever inner attempt.
    for delay in DELAYS {
        let tools = Tools::new(|name, args, before| match (name, before) {
            ("send", 0) => Err(rejected()),
            _ => check_fails_once(name, args, before),
        });
        let status = retried(&tools, NESTED, delay).await;
        assert!(matches!(status, Status::Completed(_)), "{delay}: {status:?}");
        assert_eq!(tools.count("send"), 2, "{delay}: one rejected, one sent, none in the outer retry");
        assert_eq!(tools.succeeded("send").len(), 1, "{delay}");
    }
}

#[tokio::test]
async fn an_at_most_once_write_that_succeeded_no_longer_blocks_the_retry() {
    let body =
        SEND_THEN_CHECK.replace("with: { text: hi },", "with: { text: hi }, metadata: { delivery: at_most_once },");
    for delay in DELAYS {
        let tools = Tools::new(check_fails_once);
        let status = retried(&tools, &body, delay).await;
        assert_eq!(status, Status::Completed(json!({"sent":{"text":"hi"}})), "{delay}");
        assert_eq!(tools.count("send"), 1, "{delay}");
    }
}

#[tokio::test]
async fn an_at_most_once_write_the_tool_rejected_is_not_retried() {
    // A tool error does not prove the call did nothing.
    let body = r#"
do:
  - guard:
      try: [ { send: { call: send, with: { text: hi }, metadata: { delivery: at_most_once } } } ]
      catch:
        retry: { delay: DELAY, limit: { attempt: { count: 3 } } }
        do: [ { handled: { set: { handled: true } } } ]
"#;
    for delay in DELAYS {
        let tools = Tools::new(|_, _, _| Err(rejected()));
        let status = retried(&tools, body, delay).await;
        assert_eq!(status, Status::Completed(json!({"handled":true})), "{delay}");
        assert_eq!(tools.count("send"), 1, "{delay}");
    }
}

#[tokio::test]
async fn an_ephemeral_run_still_never_retries_an_attempt_that_wrote() {
    let tools = Tools::new(check_fails_once);
    let time = Arc::new(Time(AtomicI64::new(1_000)));
    let h = tools_host(tools.clone(), time);
    let wf = workflow(&SEND_THEN_CHECK.replace("DELAY", "PT0S"));
    let outcome = run::run(&h, &wf, "run", json!({}), &CancellationToken::new()).await;
    assert!(matches!(outcome.status, Status::Completed(_)), "{:?}", outcome.status);
    assert_eq!((tools.count("send"), tools.count("read_check")), (1, 1), "caught, not retried");
}

// A crash at any checkpoint, before or after its commit, then recovery. An
// at-most-once send goes out at most once, and exactly once when the run
// completes. An at-least-once send may go out twice only because of the
// crash, never because of the retry; the run always completes.
#[tokio::test]
async fn retried_writes_survive_a_crash_at_every_checkpoint() {
    for (delivery, extra) in [("at_most_once", ", metadata: { delivery: at_most_once }"), ("at_least_once", "")] {
        let body = NESTED.replace("with: { text: hi } }", &format!("with: {{ text: hi }}{extra} }}"));
        // An at-most-once send cannot be retried after a rejection, so the
        // inner retry is exercised by the at-least-once pass only.
        let first_send_fails = delivery == "at_least_once";
        let script = move |name: &str, args: &Value, before: usize| match (name, before) {
            ("send", 0) if first_send_fails => Err(rejected()),
            _ => check_fails_once(name, args, before),
        };
        for delay in DELAYS {
            let wf = workflow(&body.replace("DELAY", delay));
            let baseline = Store::default();
            let time = Arc::new(Time(AtomicI64::new(1_000)));
            let tools = Tools::new(script);
            drive(&tools_host(tools.clone(), time.clone()), &time, &wf, &baseline).await;
            assert_eq!(tools.succeeded("send").len(), 1, "{delivery} {delay}: baseline");
            let saves = baseline.saves.load(Ordering::SeqCst);
            for after_commit in [false, true] {
                for at in 1..=saves {
                    let store = Store { after_commit, ..Store::default() };
                    store.fail_at.store(at, Ordering::SeqCst);
                    let time = Arc::new(Time(AtomicI64::new(1_000)));
                    let tools = Tools::new(script);
                    let status = drive(&tools_host(tools.clone(), time.clone()), &time, &wf, &store).await;
                    let sent = tools.succeeded("send").len();
                    let case = format!("{delivery} {delay} crash at {at} (after commit: {after_commit})");
                    if delivery == "at_most_once" {
                        assert!(sent <= 1, "{case}: sent {sent}");
                        match &status {
                            Status::Completed(_) => assert_eq!(sent, 1, "{case}"),
                            Status::Faulted(e) => assert!(e.is("outcome_unknown"), "{case}: {e:?}"),
                            other => panic!("{case}: {other:?}"),
                        }
                    } else {
                        assert!(matches!(status, Status::Completed(_)), "{case}: {status:?}");
                        assert!((1..=2).contains(&sent), "{case}: sent {sent}");
                    }
                }
            }
        }
    }
}

// A host that dispatches by an argument: `tool { name }` is a write (and at
// most once) for `send`, a read otherwise, and a `send` that may have run
// comes back as a marker instead of faulting the run.
#[derive(Default)]
struct Dispatch(Mutex<Vec<(String, String)>>);
#[async_trait]
impl Functions for Dispatch {
    fn effect(&self, name: &str) -> Option<Effect> {
        (name == "tool").then_some(Effect::Read)
    }
    fn effect_of(&self, name: &str, args: &Value) -> Option<Effect> {
        self.effect(name).map(|read| if args["name"] == "send" { Effect::Write } else { read })
    }
    fn at_most_once(&self, _: &str, args: &Value) -> bool {
        args["name"] == "send"
    }
    fn unknown_outcome(&self, _: &str, args: &Value) -> Option<Value> {
        Some(json!({"unknown": args["name"]}))
    }
    async fn call(&self, _: &str, args: Value, info: &CallInfo<'_>) -> Result<Value, WorkflowError> {
        self.0.lock().unwrap().push((args["name"].as_str().unwrap_or_default().into(), info.reference.into()));
        Ok(json!({"did": args["name"]}))
    }
}

fn dispatch_host(functions: Arc<Dispatch>, time: Arc<Time>) -> Host {
    Host {
        functions,
        clock: time,
        observer: Arc::new(Silent),
        limits: Limits::default(),
        runtime: json!({"name":"test"}),
    }
}

const ROUND: &str = r#"do:
  - round:
      for: { each: call, in: [ read, send, read ] }
      metadata: { concurrency: 3 }
      do:
        - one: { call: tool, with: { name: '${ $call }' } }
"#;

#[tokio::test]
async fn a_write_classified_per_call_is_never_sent_twice_and_an_unknown_one_is_a_marker() {
    let wf = workflow(ROUND);
    let mut markers = 0;
    for after_commit in [false, true] {
        for fail_at in 1..=20 {
            let store = Store { fail_at: AtomicUsize::new(fail_at), after_commit, ..Default::default() };
            let calls = Arc::new(Dispatch::default());
            let time = Arc::new(Time(AtomicI64::new(1_000)));
            let result = drive(&dispatch_host(calls.clone(), time.clone()), &time, &wf, &store).await;
            let Status::Completed(Value::Array(out)) = result else { panic!("crash {fail_at}: {result:?}") };
            assert_eq!(out.len(), 3, "crash {fail_at}");
            assert!(out[1] == json!({"did":"send"}) || out[1] == json!({"unknown":"send"}), "{out:?}");
            markers += usize::from(out[1] == json!({"unknown":"send"}));
            let sends = calls.0.lock().unwrap().iter().filter(|(name, _)| name == "send").count();
            assert!(sends <= 1, "crash {fail_at}, after={after_commit}: sent {sends} times");
            // The marker is the call's saved result: a later replay keeps it.
            assert_eq!(
                drive(&dispatch_host(calls, time.clone()), &time, &wf, &store).await,
                Status::Completed(json!(out))
            );
        }
    }
    assert!(markers > 0, "some crash leaves the send's outcome unknown");
}

#[tokio::test]
async fn a_concurrent_for_runs_its_iterations_at_once_on_its_input() {
    struct Slow(AtomicUsize, AtomicUsize);
    #[async_trait]
    impl Functions for Slow {
        fn effect(&self, _: &str) -> Option<Effect> {
            Some(Effect::Read)
        }
        async fn call(&self, _: &str, args: Value, _: &CallInfo<'_>) -> Result<Value, WorkflowError> {
            let now = self.0.fetch_add(1, Ordering::SeqCst) + 1;
            self.1.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            self.0.fetch_sub(1, Ordering::SeqCst);
            Ok(args)
        }
    }
    let wf = workflow(
        r#"do:
  - init: { set: { base: 10 }, export: { as: '${ . }' } }
  - round:
      for: { each: n, in: [ 3, 1, 2 ] }
      metadata: { concurrency: 2 }
      do:
        - echo: { call: slow, with: { v: '${ $n + .base }' }, export: { as: '${ { leaked: true } }' } }
  - after: { set: { round: '${ . }', context: '${ $context }' } }
"#,
    );
    let slow = Arc::new(Slow(AtomicUsize::new(0), AtomicUsize::new(0)));
    let time = Arc::new(Time(AtomicI64::new(1_000)));
    let h = Host {
        functions: slow.clone(),
        clock: time.clone(),
        observer: Arc::new(Silent),
        limits: Limits::default(),
        runtime: json!({"name":"test"}),
    };
    assert_eq!(
        drive(&h, &time, &wf, &Store::default()).await,
        Status::Completed(json!({"round": [{"v":13}, {"v":11}, {"v":12}], "context": {"base":10}})),
        "outputs in item order, each iteration on the for's input, exports kept inside"
    );
    assert_eq!(slow.1.load(Ordering::SeqCst), 2, "at most `concurrency` at once, and more than one");
}

#[test]
fn concurrency_is_validated_as_for_metadata() {
    let parse = |task: &str| {
        model::parse_yaml(&format!(
            "document: {{ dsl: '1.0.4', namespace: t, name: w, version: '0.1.0' }}\ndo: [ {{ t: {task} }} ]"
        ))
    };
    let each = "for: { in: [1] }, do: [ { e: { call: echo } } ]";
    assert!(parse(&format!("{{ {each}, metadata: {{ concurrency: 4 }} }}")).is_ok());
    assert!(parse(&format!("{{ {each}, metadata: {{ concurrency: 0 }} }}")).is_err());
    assert!(parse(&format!("{{ {each}, while: '.go', metadata: {{ concurrency: 2 }} }}")).is_err());
    assert!(parse("{ call: echo, metadata: { concurrency: 2 } }").is_err());
}
