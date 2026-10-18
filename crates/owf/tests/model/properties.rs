// What must hold of the model itself, over random programs; if one fails,
// the model is wrong before the interpreter is compared with it.
//
//   1. a run counts at most one task past the ceiling, and the ceiling is
//      never caught
//   2. every step names a task of the program; every retry names a try,
//      and its attempt number stays under the host's cap
//   3. `then: continue` is the same as no `then`
//   4. a task list wrapped in a `do` runs to the same result
//   5. a program with nothing that can fail never faults, ceiling aside

use proptest::prelude::*;
use proptest::test_runner::{TestCaseError, TestRunner};

use crate::model::{self, references};
use crate::model_limits;
use crate::support::program::{Body, Data, Flow, Num, Profile, Program, Task, each_task, input, program, task};

fn check(test: impl Fn(Program, serde_json::Value) -> Result<(), TestCaseError>) {
    TestRunner::new(crate::support::host::config(50, file!()))
        .run(&(program(Profile::CORE), input()), |(p, i)| test(p, i))
        .unwrap();
}

/// The model raises a runtime error only at the ceiling.
fn ceiling(e: &model::Error) -> bool {
    e.type_.ends_with("/errors/runtime") && e.status == 500
}

#[test]
fn p1_the_ceiling_stops_the_run_and_is_never_caught() {
    check(|p, input| {
        let l = model_limits();
        let o = model::run(&p, input, &l);
        prop_assert!(o.tasks_run <= l.max_tasks + 1);
        if o.tasks_run > l.max_tasks {
            let e = o.result.as_ref().expect_err("a run past the ceiling faults");
            prop_assert!(ceiling(e) && e.instance.is_some(), "{e:?}");
        }
        Ok(())
    });
}

#[test]
fn p2_steps_name_tasks_of_the_program() {
    check(|p, input| {
        let l = model_limits();
        let known = references(&p);
        for step in model::run(&p, input, &l).trace {
            let (kind, rest) = step.split_once(' ').expect("`kind reference`");
            let invocation = rest.split(" #").next().unwrap_or(rest);
            let reference = model::task_reference(invocation);
            prop_assert!(known.iter().any(|k| k == &reference), "{step} is not in {known:?}");
            if kind == "retry" {
                let attempt: u32 = rest.rsplit('#').next().and_then(|n| n.parse().ok()).unwrap_or(0);
                prop_assert!(attempt >= 1 && attempt < l.max_attempts, "{step}");
                prop_assert!(try_at(&p, &reference), "{step} is not a try");
            }
        }
        Ok(())
    });
}

fn try_at(p: &Program, reference: &str) -> bool {
    let mut found = false;
    let mut probe = p.clone();
    // A try's reference ends in its name; the model's trace only names tries
    // it ran, so a try with that name exists.
    let name = reference.rsplit('/').next().unwrap_or_default().to_string();
    each_task(&mut probe, &mut |t| found |= t.name == name && matches!(t.body, Body::Try { .. }));
    found
}

#[test]
fn p3_an_explicit_continue_is_no_then() {
    check(|p, input| {
        let mut explicit = p.clone();
        each_task(&mut explicit, &mut |t| {
            t.then.get_or_insert(Flow::Continue);
        });
        let l = model_limits();
        let (a, b) = (model::run(&p, input.clone(), &l), model::run(&explicit, input, &l));
        prop_assert_eq!(a, b);
        Ok(())
    });
}

#[test]
fn p4_wrapping_the_tasks_in_a_do_keeps_the_result() {
    check(|p, input| {
        let mut wrapped = p.clone();
        wrapped.tasks = vec![task("wrap", Body::Do(p.tasks.clone()))];
        let l = model_limits();
        let (a, b) = (model::run(&p, input.clone(), &l), model::run(&wrapped, input, &l));
        // One more task counts, so a run at the ceiling may stop elsewhere.
        if a.tasks_run >= l.max_tasks || b.tasks_run > l.max_tasks {
            return Ok(());
        }
        prop_assert_eq!(
            a.result.clone().map_err(|e| (e.type_, e.status)),
            b.result.clone().map_err(|e| (e.type_, e.status))
        );
        prop_assert_eq!(a.calls, b.calls);
        Ok(())
    });
}

/// Whether anything in the program can raise: a raise, a failing function,
/// an expression that indexes a number.
fn can_fail(p: &Program) -> bool {
    let mut fails = false;
    let mut probe = p.clone();
    each_task(&mut probe, &mut |t: &mut Task| {
        let bad = |d: &Option<Data>| matches!(d, Some(Data::Obj(Num::Bad)));
        fails |= bad(&t.input_from) || bad(&t.output_as) || bad(&t.export_as);
        fails |= match &t.body {
            Body::Raise(_) => true,
            Body::Call(f, n) => matches!(f.as_str(), "down" | "reject" | "flaky") || *n == Num::Bad,
            Body::Set(s) => format!("{s:?}").contains("Bad"),
            _ => false,
        };
    });
    fails || [&p.input_from, &p.output_as].iter().any(|d| matches!(d, Some(Data::Obj(Num::Bad))))
}

#[test]
fn p5_nothing_that_can_fail_never_faults() {
    check(|p, input| {
        if can_fail(&p) {
            return Ok(());
        }
        match model::run(&p, input, &model_limits()).result {
            Ok(_) => Ok(()),
            Err(e) if ceiling(&e) => Ok(()),
            Err(e) => Err(TestCaseError::fail(format!("{e:?}"))),
        }
    });
}
