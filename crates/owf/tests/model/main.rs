// Differential test: the interpreter (owf::run) against the reference model.
// The model (model.rs) is the specification — read that, not this file.
// This file only generates programs (support/program.rs, the CORE profile:
// no fork, wait or timeout), runs each on both and compares:
//
//   1. the comparison — outcome (value, or error type/status/instance), the
//                       steps (start/skip/retry by reference), the tasks
//                       counted, the host calls and effects_started
//   2. random programs, then the regressions they found, kept as plain tests
//
//   cargo test -p owf --test model
//   PROPERTY_RUNS=2000 cargo test -p owf --test model
//
// properties.rs checks the model itself. A failure prints the smallest
// program that still disagrees (proptest shrinks it), as YAML, with both
// sides' steps.

#[path = "../support/mod.rs"]
mod support;

mod model;
mod properties;

use model::Limits;
use owf::run::Status;
use proptest::test_runner::{TestCaseError, TestRunner};
use serde_json::{Value, json};
use support::host::{Options, calls, execute, limits, pretty, trace};
use support::program::{Profile, Program, definition, input, program};

// ── 1. the comparison ──────────────────────────────────────────────────────

pub fn model_limits() -> Limits {
    let l = limits();
    Limits { max_tasks: l.max_tasks, max_attempts: l.max_attempts }
}

fn real_shape(status: &Status) -> Value {
    match status {
        Status::Completed(v) => json!({ "completed": v }),
        Status::Faulted(e) => json!({ "faulted": { "type": e.type_, "status": e.status, "instance": e.instance } }),
        other => {
            assert!(matches!(other, Status::Cancelled), "a non-durable run suspended: {other:?}");
            json!("cancelled")
        }
    }
}

/// `Err` with a transcript when the interpreter and the model disagree.
fn agree(p: &Program, input: Value) -> Result<(), String> {
    let real = execute(p, input.clone(), &Options::default());
    let expected = model::run(p, input.clone(), &model_limits());
    let mut diffs = Vec::new();
    let (got, want) = (real_shape(&real.outcome.status), model::shape(&expected.result));
    if got != want {
        diffs.push(format!("outcome: interpreter {got}\n         model       {want}"));
    }
    let steps = trace(&real.journal);
    if steps != expected.trace {
        diffs.push(format!("steps: interpreter {steps:#?}\n       model       {:#?}", expected.trace));
    }
    if real.outcome.tasks_run != expected.tasks_run {
        diffs.push(format!("tasks_run: interpreter {} model {}", real.outcome.tasks_run, expected.tasks_run));
    }
    if calls(&real.journal) != expected.calls {
        diffs.push(format!("calls: interpreter {:?}\n       model       {:?}", calls(&real.journal), expected.calls));
    }
    if real.outcome.effects_started != expected.effects_started {
        diffs.push(format!("effects_started: interpreter {}", real.outcome.effects_started));
    }
    if diffs.is_empty() {
        Ok(())
    } else {
        Err(format!("input {input}\n{}\n{}", pretty(&definition(p)), diffs.join("\n")))
    }
}

// ── 2. programs ────────────────────────────────────────────────────────────

#[test]
fn the_interpreter_agrees_with_the_model() {
    TestRunner::new(support::host::config(50, file!()))
        .run(&(program(Profile::CORE), input()), |(p, input)| agree(&p, input).map_err(TestCaseError::fail))
        .unwrap();
}

#[test]
fn a_default_switch_case_does_not_preempt_a_later_match() {
    use support::program::{Body, Case, Cond, Flow, Num, Template, task};
    let p = Program {
        input_from: None,
        output_as: None,
        functions: vec![],
        bare: false,
        tasks: vec![
            task(
                "choose",
                Body::Switch(vec![
                    Case { name: "default".into(), when: None, then: Flow::Exit },
                    Case { name: "matched".into(), when: Some(Cond::True), then: Flow::Continue },
                ]),
            ),
            task("next", Body::Set(Template::Obj(Num::Lit(7)))),
        ],
    };
    agree(&p, json!({ "n": 0 })).unwrap();
    let ran = execute(&p, json!({ "n": 0 }), &Options::default());
    assert!(matches!(ran.outcome.status, Status::Completed(v) if v == json!({ "n": 7 })));
}
