// Invariants of the interpreter over random valid workflows
// (support/program.rs): 1. termination and the task ceiling; 2. events are
// well-formed spans; 3. effects and retries; 4. cancellation; 5. validation
// soundness; 6. determinism; 7. the parser never panics.
//
//   cargo test -p owf --test invariants
//   PROPERTY_RUNS=2000 cargo test -p owf --test invariants
//
// A failure prints the smallest program that breaks the property (proptest
// shrinks it); `pretty(&definition(&p))` shows it as YAML.
mod support;

use owf::model::{self, Invalid};
use owf::run::{self, Limits, Status};
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::{TestCaseError, TestRunner};
use serde_json::{Map, Value, json};
use std::collections::HashSet;
use std::time::Duration;
use support::host::{
    Entry, Known, Options, execute, execute_workflow, hit_limit, is_limit, limits, observed, pretty,
    try_execute_workflow,
};
use support::program::{
    Body, Data, Flow, GHOST, Profile, Program, Task, Template, definition, input, list_at, lists, program, task,
};

fn check<S: Strategy>(default: u32, strategy: S, test: impl Fn(S::Value) -> Result<(), TestCaseError>)
where
    S::Value: std::fmt::Debug,
{
    TestRunner::new(support::host::config(default, file!())).run(&strategy, test).unwrap();
}

/// Fails the case with the program attached.
fn ensure(ok: bool, p: &Program, what: impl std::fmt::Display) -> Result<(), TestCaseError> {
    if ok { Ok(()) } else { Err(TestCaseError::fail(format!("{what}\n{}", pretty(&definition(p))))) }
}

// ── 1. termination and the task ceiling ────────────────────────────────────

#[test]
fn every_run_ends_completed_or_faulted_within_the_task_ceiling() {
    check(30, (program(Profile::FULL), input()), |(p, input)| {
        let ran = execute(&p, input, &Options::default());
        let max = support::host::limits().max_tasks;
        ensure(
            matches!(ran.outcome.status, Status::Completed(_) | Status::Faulted(_)),
            &p,
            "a non-durable run did not finish",
        )?;
        ensure(ran.outcome.tasks_run <= max + 1, &p, format!("{} tasks ran", ran.outcome.tasks_run))?;
        ensure(
            (ran.outcome.tasks_run > max) == hit_limit(&ran.outcome),
            &p,
            format!("tasks_run {} vs outcome {:?}", ran.outcome.tasks_run, ran.outcome.status),
        )?;
        // The ceiling cannot be caught: once a task reports it, the run ends with it.
        let limit_fault = ran.journal.iter().any(|e| matches!(e, Entry::Faulted { error, .. } if is_limit(error)));
        ensure(!limit_fault || hit_limit(&ran.outcome), &p, format!("a caught ceiling: {:?}", ran.outcome.status))
    });
}

/// `try: [a → b → a]` catching everything, with a retry and a handler.
fn cycle() -> Task {
    let step = |name: &str, to: usize| Task {
        then: Some(Flow::Goto(to)),
        ..task(name, Body::Set(Template::Expr(Data::Same)))
    };
    let catch = support::program::Catch {
        status: None,
        as_: None,
        when: None,
        retry: Some(support::program::Retry { count: Some(3), delay_ms: 0, when: None }),
        handler: Some(vec![task("h", Body::Set(Template::Obj(support::program::Num::Lit(-1))))]),
        then: None,
    };
    task("cycle", Body::Try { body: vec![step("a", 1), step("b", 0)], catch: Box::new(catch) })
}

#[test]
fn a_then_cycle_hits_the_ceiling_which_no_try_catches() {
    check(
        30,
        (program(Profile::FULL), input(), any::<prop::sample::Index>(), any::<prop::sample::Index>()),
        |(mut p, input, l, at)| {
            let paths = lists(&p);
            let list = list_at(&mut p, &paths[l.index(paths.len())]);
            let at = at.index(list.len() + 1);
            list.insert(at, cycle());
            shift_gotos(list, at);
            let ran = execute(&p, input, &Options::default());
            let reached = ran
                .journal
                .iter()
                .any(|e| matches!(e, Entry::Started { reference, .. } if reference.ends_with("/cycle")));
            ensure(
                !reached || hit_limit(&ran.outcome),
                &p,
                format!("the cycle ran and the run {:?}", ran.outcome.status),
            )
        },
    );
}

/// After inserting a task at `at`, jumps to tasks at or after it move by one.
fn shift_gotos(list: &mut [Task], at: usize) {
    let shift = |f: &mut Flow| {
        if let Flow::Goto(i) = f
            && *i >= at
        {
            *i += 1;
        }
    };
    for (j, t) in list.iter_mut().enumerate() {
        if j == at {
            continue;
        }
        if let Some(f) = &mut t.then {
            shift(f);
        }
        match &mut t.body {
            Body::Switch(cases) => cases.iter_mut().for_each(|c| shift(&mut c.then)),
            Body::Try { catch, .. } => {
                if let Some(f) = &mut catch.then {
                    shift(f);
                }
            }
            _ => {}
        }
    }
}

// ── 2. events ──────────────────────────────────────────────────────────────

/// Spans: every start is closed once, by its own reference; a parent is
/// open while its children run and closes after them; a fault names the
/// innermost task, which started or faulted before starting.
fn well_formed(journal: &[Entry], status: &Status) -> Result<(), String> {
    let mut open: Vec<(String, Option<String>)> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut faulted: HashSet<String> = HashSet::new();
    let is_open = |open: &[(String, Option<String>)], r: &str| open.iter().any(|(o, _)| o == r);
    for (i, e) in journal.iter().enumerate() {
        match e {
            Entry::Started { reference, parent, .. } => {
                if let Some(p) = parent
                    && !is_open(&open, p)
                {
                    return Err(format!("#{i}: {reference} starts inside {p}, which is not open"));
                }
                seen.insert(reference.clone());
                open.push((reference.clone(), parent.clone()));
            }
            Entry::Skipped { reference, parent } => {
                if let Some(p) = parent
                    && !is_open(&open, p)
                {
                    return Err(format!("#{i}: {reference} is skipped inside {p}, which is not open"));
                }
            }
            Entry::Completed { reference, .. } | Entry::Faulted { reference, .. } => {
                let error = match e {
                    Entry::Faulted { error, .. } => Some(error),
                    _ => None,
                };
                match open.iter().rposition(|(o, _)| o == reference) {
                    Some(at) => {
                        if let Some((child, _)) = open.iter().find(|(_, p)| p.as_deref() == Some(reference)) {
                            return Err(format!("#{i}: {reference} closes while its child {child} is open"));
                        }
                        open.remove(at);
                    }
                    None => match error {
                        // Faulted before it started: its `if`, input or the ceiling.
                        Some(err) if err.instance.as_deref() == Some(reference) => {}
                        _ => return Err(format!("#{i}: {reference} closes but never started")),
                    },
                }
                if let Some(err) = error {
                    match err.instance.as_deref() {
                        Some(inst) if inst == reference || faulted.contains(inst) => {}
                        other => return Err(format!("#{i}: {reference} faults with instance {other:?}, not inner")),
                    }
                    faulted.insert(reference.clone());
                }
            }
            Entry::Retrying { reference, .. } => {
                if !is_open(&open, reference) {
                    return Err(format!("#{i}: {reference} retries while not open"));
                }
            }
            Entry::Call { reference, .. } => {
                if !is_open(&open, reference) {
                    return Err(format!("#{i}: a call from {reference}, which is not open"));
                }
            }
        }
    }
    if let Some((r, _)) = open.first() {
        return Err(format!("{r} never closes"));
    }
    if let Status::Faulted(e) = status
        && let Some(inst) = &e.instance
        && !seen.contains(inst)
        && !faulted.contains(inst)
    {
        return Err(format!("the run faults at {inst}, which never ran"));
    }
    Ok(())
}

#[test]
fn events_are_well_formed_spans() {
    check(30, (program(Profile::FULL), input()), |(p, input)| {
        let ran = execute(&p, input, &Options::default());
        let r = well_formed(&ran.journal, &ran.outcome.status);
        ensure(r.is_ok(), &p, format!("{}\n{:#?}", r.err().unwrap_or_default(), ran.journal))
    });
}

// ── 3. effects and retries ─────────────────────────────────────────────────

/// For each call, the references of the open tasks it runs inside,
/// innermost first (a function's caller included, through `parent`).
fn ancestry(journal: &[Entry]) -> Vec<(usize, bool, Vec<String>)> {
    let mut open: Vec<(String, Option<String>)> = Vec::new();
    let mut out = Vec::new();
    for (i, e) in journal.iter().enumerate() {
        match e {
            Entry::Started { reference, parent, .. } => open.push((reference.clone(), parent.clone())),
            Entry::Completed { reference, .. } | Entry::Faulted { reference, .. } => {
                if let Some(at) = open.iter().rposition(|(o, _)| o == reference) {
                    open.remove(at);
                }
            }
            Entry::Call { reference, write, .. } => {
                let mut chain = Vec::new();
                let mut at = Some(reference.clone());
                while let Some(r) = at {
                    at = open.iter().rev().find(|(o, _)| *o == r).and_then(|(_, p)| p.clone());
                    chain.push(r);
                }
                out.push((i, *write, chain));
            }
            _ => {}
        }
    }
    out
}

#[test]
fn effects_started_means_a_write_was_called_and_no_retry_replays_one() {
    check(30, (program(Profile::FULL), input()), |(p, input)| {
        let ran = execute(&p, input, &Options::default());
        let wrote = ran.journal.iter().any(|e| matches!(e, Entry::Call { write: true, .. }));
        ensure(ran.outcome.effects_started == wrote, &p, format!("effects_started {}", ran.outcome.effects_started))?;
        let calls = ancestry(&ran.journal);
        for (k, e) in ran.journal.iter().enumerate() {
            let Entry::Retrying { reference, .. } = e else { continue };
            // The attempt began at the try's start or at its previous retry.
            let began = ran.journal[..k]
                .iter()
                .rposition(|e| {
                    matches!(e, Entry::Started { reference: r, .. } | Entry::Retrying { reference: r, .. } if r == reference)
                })
                .unwrap_or(0);
            let replayed =
                calls.iter().find(|(i, write, chain)| *i > began && *i < k && *write && chain.contains(reference));
            ensure(replayed.is_none(), &p, format!("{reference} retries an attempt that wrote: {replayed:?}"))?;
        }
        Ok(())
    });
}

// ── 4. cancellation ────────────────────────────────────────────────────────

#[test]
fn cancelling_stops_the_run_and_no_call_starts_after() {
    check(30, (program(Profile::FULL), input(), 0u64..40), |(p, input, ms)| {
        let whole = execute(&p, input.clone(), &Options::default());
        // Timers fire on even milliseconds (support/host.rs): an odd one never ties.
        let at = Duration::from_millis(ms * 2 + 1);
        let ran = execute(&p, input, &Options { cancel_at: Some(at), ..Options::default() });
        if at < whole.elapsed {
            ensure(matches!(ran.outcome.status, Status::Cancelled), &p, format!("{:?} at {at:?}", ran.outcome.status))?;
        } else {
            ensure(
                observed(&ran.outcome) == observed(&whole.outcome),
                &p,
                "a cancel after the end changed the outcome",
            )?;
        }
        let late = ran.journal.iter().find(|e| matches!(e, Entry::Call { after_cancel: true, .. }));
        ensure(late.is_none(), &p, format!("a call started after the cancel: {late:?}"))?;
        let spans = well_formed(&ran.journal, &ran.outcome.status);
        ensure(spans.is_ok(), &p, spans.err().unwrap_or_default())?;
        let wrote = ran.journal.iter().any(|e| matches!(e, Entry::Call { write: true, .. }));
        ensure(ran.outcome.effects_started == wrote, &p, "effects_started disagrees with the calls")
    });
}

// ── 5. validation soundness ────────────────────────────────────────────────

/// Whether a `then` names no task of its own list, or a call names no
/// function: what validation must reject.
fn dangling(p: &Program) -> bool {
    fn flow(f: &Flow, siblings: &[&str]) -> bool {
        matches!(f, Flow::Named(n) if !siblings.contains(&n.as_str()))
    }
    fn list(tasks: &[Task]) -> bool {
        let names: Vec<&str> = tasks.iter().map(|t| t.name.as_str()).collect();
        tasks.iter().any(|t| one(t, &names))
    }
    fn one(t: &Task, siblings: &[&str]) -> bool {
        t.then.as_ref().is_some_and(|f| flow(f, siblings))
            || match &t.body {
                Body::Call(f, _) => f == GHOST,
                Body::Do(ts) | Body::For { body: ts, .. } => list(ts),
                Body::Fork { branches, .. } => branches.iter().any(|b| one(b, &[])),
                Body::Switch(cases) => cases.iter().any(|c| flow(&c.then, siblings)),
                Body::Try { body, catch } => {
                    list(body)
                        || catch.handler.as_deref().is_some_and(list)
                        || catch.then.as_ref().is_some_and(|f| flow(f, siblings))
                }
                _ => false,
            }
    }
    list(&p.tasks) || p.functions.iter().any(|f| one(f, &[]))
}

#[test]
fn a_validated_workflow_never_fails_on_a_name_at_runtime() {
    check(30, (program(Profile::SLOPPY), input()), |(p, input)| {
        let wf = model::parse(definition(&p)).map_err(|e| TestCaseError::fail(e.to_string()))?;
        match run::validate(&wf, &Known) {
            Err(Invalid(problems)) => ensure(dangling(&p), &p, format!("rejected a sound program: {problems:?}")),
            Ok(()) => {
                ensure(!dangling(&p), &p, "accepted a dangling name")?;
                let ran = execute_workflow(&wf, input, &Options::default());
                let named = ran.journal.iter().find_map(|e| match e {
                    Entry::Faulted { error, .. }
                        if error
                            .detail
                            .as_deref()
                            .is_some_and(|d| d.contains("unknown function") || d.contains("names no task")) =>
                    {
                        Some(error.clone())
                    }
                    _ => None,
                });
                ensure(named.is_none(), &p, format!("a validated workflow failed on a name: {named:?}"))
            }
        }
    });
}

// ── 6. determinism ─────────────────────────────────────────────────────────

#[test]
fn the_same_workflow_on_the_same_host_runs_the_same_way() {
    check(30, (program(Profile::DETERMINISTIC), input()), |(p, input)| {
        let a = execute(&p, input.clone(), &Options::default());
        let b = execute(&p, input, &Options::default());
        ensure(a.journal == b.journal, &p, "the journals differ")?;
        ensure(observed(&a.outcome) == observed(&b.outcome), &p, "the outcomes differ")?;
        ensure(format!("{:?}", a.outcome) == format!("{:?}", b.outcome), &p, "the outcome details differ")?;
        ensure(a.elapsed == b.elapsed, &p, "the virtual durations differ")
    });
}

// ── 7. the parser ──────────────────────────────────────────────────────────

const WORDS: &[&str] = &[
    "document",
    "dsl",
    "namespace",
    "name",
    "version",
    "do",
    "set",
    "call",
    "with",
    "for",
    "each",
    "in",
    "at",
    "while",
    "try",
    "catch",
    "errors",
    "retry",
    "limit",
    "attempt",
    "count",
    "delay",
    "backoff",
    "jitter",
    "exponential",
    "fork",
    "branches",
    "compete",
    "switch",
    "when",
    "then",
    "raise",
    "error",
    "type",
    "status",
    "wait",
    "if",
    "input",
    "output",
    "export",
    "as",
    "from",
    "schema",
    "timeout",
    "after",
    "use",
    "functions",
    "retries",
    "timeouts",
    "seconds",
    "milliseconds",
    "x",
];

fn json_value() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::from),
        any::<i64>().prop_map(Value::from),
        (0u64..700).prop_map(Value::from),
        any::<f64>().prop_map(Value::from),
        prop::sample::select(WORDS).prop_map(Value::from),
        prop::sample::select(vec!["${ . }", "${ .n + 1 }", "${", "}", "1.0.0", "PT1S", "P", "end", "continue", "exit"])
            .prop_map(Value::from),
        ".{0,12}".prop_map(Value::from),
    ];
    leaf.prop_recursive(5, 48, 5, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
            prop::collection::vec((prop::sample::select(WORDS), inner), 0..5)
                .prop_map(|kv| Value::Object(kv.into_iter().map(|(k, v)| (k.to_string(), v)).collect::<Map<_, _>>())),
        ]
    })
}

#[test]
fn parse_yaml_never_panics_on_any_text() {
    let text = prop_oneof![
        any::<String>(),
        prop::collection::vec(
            prop::sample::select(vec![
                "document:",
                " dsl: '1.0.0'",
                "do:",
                "- a:",
                "  set:",
                "{",
                "}",
                "[",
                "]",
                ": ",
                "\n",
                "  ",
                "'${ . }'",
                "then: end",
                "- ",
                "call: x",
                "&a",
                "*a",
                "!!str",
                "<<: *a",
                "---",
                "~",
                "1e999",
                "\t",
            ]),
            0..40,
        )
        .prop_map(|parts| parts.concat()),
    ];
    check(500, text, |t| {
        let _ = model::parse_yaml(&t);
        Ok(())
    });
}

#[test]
fn parse_never_panics_on_any_json() {
    check(500, json_value(), |v| {
        let _ = model::parse(v.clone());
        let _ = model::parse(
            json!({ "document": { "dsl": "1.0.0", "namespace": "t", "name": "w", "version": "0.1.0" }, "do": v }),
        );
        Ok(())
    });
}

/// Replaces the value at a random position (a pre-order index) with `with`.
fn mutate(v: &mut Value, mut at: usize, with: Value) -> bool {
    fn walk(v: &mut Value, at: &mut usize, with: &mut Option<Value>) -> bool {
        if *at == 0 {
            *v = with.take().unwrap_or(Value::Null);
            return true;
        }
        *at -= 1;
        match v {
            Value::Array(items) => items.iter_mut().any(|x| walk(x, at, with)),
            Value::Object(m) => m.values_mut().any(|x| walk(x, at, with)),
            _ => false,
        }
    }
    walk(v, &mut at, &mut Some(with))
}

#[test]
fn a_mutated_definition_parses_validates_and_runs_without_panicking() {
    check(30, (program(Profile::FULL), 0usize..200, json_value(), input()), |(p, at, with, input)| {
        let mut def = definition(&p);
        mutate(&mut def, at, with);
        if let Ok(wf) = model::parse(def)
            && run::validate(&wf, &Known).is_ok()
        {
            // Whatever it does, it must not panic; a mutation may wait for days.
            let opts = Options {
                limits: Limits { max_tasks: 60, max_attempts: 3, max_sleep: None, max_listen: None },
                ..Options::default()
            };
            let _ = try_execute_workflow(&wf, input, &opts);
        }
        Ok(())
    });
}

/// A guard against a degenerate generator: random programs complete, fault
/// and hit the ceiling, each often enough to matter.
#[test]
fn the_generator_reaches_every_kind_of_outcome() {
    let mut runner = TestRunner::deterministic();
    let strategy = (program(Profile::FULL), input());
    let (mut completed, mut faulted, mut limited) = (0, 0, 0);
    for _ in 0..200 {
        let (p, input) = strategy.new_tree(&mut runner).unwrap().current();
        let ran = execute(&p, input, &Options::default());
        match &ran.outcome.status {
            _ if hit_limit(&ran.outcome) => limited += 1,
            Status::Completed(_) => completed += 1,
            _ => faulted += 1,
        }
    }
    assert!(
        completed > 20 && faulted > 20 && limited > 5,
        "completed {completed}, faulted {faulted}, ceiling {limited}"
    );
}

#[test]
fn the_task_ceiling_reports_a_fault_after_skipped_visits() {
    let wf = model::parse(json!({
        "document": { "dsl": "1.0.0", "namespace": "test", "name": "ceiling", "version": "0.1.0" },
        "do": [
            { "skip": { "if": "false", "set": "${ . }" } },
            { "again": { "set": "${ . }", "then": "skip" } }
        ]
    }))
    .unwrap();
    let opts = Options {
        limits: Limits { max_tasks: 2, max_attempts: 1, max_sleep: None, max_listen: None },
        ..Options::default()
    };
    let ran = execute_workflow(&wf, json!({ "n": 1 }), &opts);
    assert!(hit_limit(&ran.outcome));
    assert!(ran.journal.iter().any(|e| matches!(e, Entry::Faulted { error, .. } if is_limit(error))));
    well_formed(&ran.journal, &ran.outcome.status).unwrap();
}

#[test]
fn interrupted_children_close_before_their_parent() {
    for task in [
        json!({ "fork": { "compete": true, "branches": [
            { "slow": { "call": "inc", "with": { "n": 0 } } },
            { "winner": { "set": "${ . }" } }
        ] } }),
        json!({ "fork": { "branches": [
            { "slow": { "call": "inc", "with": { "n": 0 } } },
            { "failed": { "raise": { "error": { "type": "https://example.com/failure", "status": 400 } } } }
        ] } }),
        json!({ "do": [ { "slow": { "wait": { "seconds": 10 } } } ], "timeout": { "after": { "milliseconds": 2 } } }),
    ] {
        let wf = model::parse(json!({
            "document": { "dsl": "1.0.0", "namespace": "test", "name": "spans", "version": "0.1.0" },
            "do": [ { "parent": task } ]
        }))
        .unwrap();
        let ran = execute_workflow(&wf, json!({ "n": 0 }), &Options::default());
        well_formed(&ran.journal, &ran.outcome.status).unwrap();
    }
}

#[test]
fn a_sleep_ceiling_cuts_waits_and_retry_delays() {
    let wf = model::parse(json!({
        "document": { "dsl": "1.0.0", "namespace": "test", "name": "sleeps", "version": "0.1.0" },
        "do": [
            { "pause": { "wait": { "hours": 1 } } },
            { "retried": {
                "try": [ { "fail": { "raise": { "error": { "type": "https://example.com/failure", "status": 503 } } } } ],
                "catch": { "retry": { "delay": { "minutes": 10 }, "limit": { "attempt": { "count": 3 } } } }
            } }
        ]
    }))
    .unwrap();
    let uncut = execute_workflow(&wf, json!({}), &Options::default());
    assert!(uncut.elapsed >= Duration::from_secs(3600 + 600), "{:?}", uncut.elapsed);
    let opts = Options { limits: Limits { max_sleep: Some(Duration::ZERO), ..limits() }, ..Options::default() };
    let cut = execute_workflow(&wf, json!({}), &opts);
    assert_eq!(cut.elapsed, Duration::ZERO);
    assert_eq!(observed(&cut.outcome), observed(&uncut.outcome));
}

#[test]
fn concurrent_calls_to_one_reusable_function_have_distinct_spans() {
    let wf = model::parse(json!({
        "document": { "dsl": "1.0.0", "namespace": "test", "name": "functions", "version": "0.1.0" },
        "use": { "functions": { "shared": { "call": "inc", "with": { "n": "${ .n }" } } } },
        "do": [ { "fork": { "fork": { "branches": [
            { "first": { "call": "shared", "with": { "n": 0 } } },
            { "second": { "call": "shared", "with": { "n": 1 } } }
        ] } } } ]
    }))
    .unwrap();
    let ran = execute_workflow(&wf, json!({}), &Options::default());
    assert!(matches!(ran.outcome.status, Status::Completed(_)));
    well_formed(&ran.journal, &ran.outcome.status).unwrap();
    let references: Vec<_> = ran
        .journal
        .iter()
        .filter_map(|e| match e {
            Entry::Call { reference, .. } => Some(reference),
            _ => None,
        })
        .collect();
    assert_eq!(references.len(), 2);
    assert_ne!(references[0], references[1]);
}
