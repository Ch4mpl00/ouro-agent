// Metamorphic relations: rewrites of a random workflow that must not change
// what it computes — the outcome (value, or error type and status) and the
// host calls; task references may differ. 1. structure (wrapping a list,
// renaming); 2. identity tasks and transforms; 3. flow directives; 4. fork
// against its branches run alone.
//
//   cargo test -p owf --test metamorphic
//   PROPERTY_RUNS=2000 cargo test -p owf --test metamorphic
//
// A rewrite adds or removes tasks, so a run that reaches the host's task
// ceiling may stop elsewhere; those runs are left out of the comparison.
mod support;

use proptest::prelude::*;
use proptest::test_runner::{TestCaseError, TestRunner};
use serde_json::{Value, json};
use support::host::{Entry, Observed, Options, Ran, calls, execute, hit_limit, observed, pretty};
use support::program::{
    Body, Catch, Cond, Data, Flow, Items, Num, Profile, Program, Task, Template, definition, each_task, fix, input,
    list_at, lists, program, task,
};

fn check<S: Strategy>(strategy: S, test: impl Fn(S::Value) -> Result<(), TestCaseError>)
where
    S::Value: std::fmt::Debug,
{
    TestRunner::new(support::host::config(30, file!())).run(&strategy, test).unwrap();
}

/// The two programs compute the same, unless either ran into the ceiling.
fn same(a: &Program, b: &Program, input: Value) -> Result<(), TestCaseError> {
    let (x, y) = (execute(a, input.clone(), &Options::default()), execute(b, input, &Options::default()));
    if hit_limit(&x.outcome) || hit_limit(&y.outcome) {
        return Ok(());
    }
    let (ox, oy) = (observed(&x.outcome), observed(&y.outcome));
    if ox == oy && calls(&x.journal) == calls(&y.journal) {
        return Ok(());
    }
    Err(TestCaseError::fail(format!(
        "{ox:?}\n  vs\n{oy:?}\n--- original\n{}--- rewritten\n{}",
        pretty(&definition(a)),
        pretty(&definition(b))
    )))
}

/// Inserting a task at `at` moves the jumps to tasks at or after it.
fn insert(list: &mut Vec<Task>, at: usize, t: Task) {
    let shift = |f: &mut Flow| {
        if let Flow::Goto(i) = f
            && *i >= at
        {
            *i += 1;
        }
    };
    for t in list.iter_mut() {
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
    list.insert(at, t);
}

fn pick(p: &Program, which: prop::sample::Index) -> Vec<usize> {
    let paths = lists(p);
    paths[which.index(paths.len())].clone()
}

fn faulted_anywhere(r: &Ran) -> bool {
    r.journal.iter().any(|e| matches!(e, Entry::Faulted { .. }))
}

// ── 1. structure ───────────────────────────────────────────────────────────

#[test]
fn wrapping_a_task_list_in_a_do_changes_nothing() {
    check((program(Profile::DETERMINISTIC), input(), any::<prop::sample::Index>()), |(p, input, which)| {
        let mut q = p.clone();
        let list = list_at(&mut q, &pick(&p, which));
        let inner = std::mem::take(list);
        list.push(task("wrap", Body::Do(inner)));
        same(&p, &q, input)
    });
}

#[test]
fn a_try_around_a_body_that_cannot_fail_is_the_body() {
    check((program(Profile::DETERMINISTIC), input(), any::<prop::sample::Index>()), |(p, input, which)| {
        // Only programs in which nothing faulted, caught or not.
        if faulted_anywhere(&execute(&p, input.clone(), &Options::default())) {
            return Ok(());
        }
        let mut q = p.clone();
        let list = list_at(&mut q, &pick(&p, which));
        let inner = std::mem::take(list);
        let catch = Catch {
            status: None,
            as_: None,
            when: None,
            retry: None,
            handler: Some(vec![task("h", Body::Set(Template::Obj(Num::Lit(-1))))]),
            then: Some(Flow::End),
        };
        list.push(task("guard", Body::Try { body: inner, catch: Box::new(catch) }));
        same(&p, &q, input)
    });
}

#[test]
fn renaming_tasks_changes_nothing() {
    check((program(Profile::DETERMINISTIC), input()), |(p, input)| {
        // Siblings swap names (the last becomes `n0`), so a jump that went
        // by position instead of by name would land elsewhere.
        let mut q = p.clone();
        for path in lists(&p) {
            let list = list_at(&mut q, &path);
            let len = list.len();
            for (i, t) in list.iter_mut().enumerate() {
                t.name = format!("n{}", len - 1 - i);
            }
        }
        same(&p, &q, input)
    });
}

// ── 2. identity tasks and transforms ───────────────────────────────────────

/// Tasks that pass their input through and touch nothing else.
fn identities() -> Vec<Task> {
    let same = || Body::Set(Template::Expr(Data::Same));
    let raise = || vec![task("never", Body::Raise(500))];
    vec![
        task("identity", same()),
        Task {
            when: Some(Cond::False),
            then: Some(Flow::End),
            ..task("identity", Body::Set(Template::Obj(Num::Lit(999))))
        },
        Task { input_from: Some(Data::Same), output_as: Some(Data::Same), ..task("identity", same()) },
        task("identity", Body::For { each: None, at: None, items: Items::Inline(vec![]), cond: None, body: raise() }),
        task(
            "identity",
            Body::For {
                each: None,
                at: None,
                items: Items::Inline(vec![1, 2]),
                cond: Some(Cond::False),
                body: raise(),
            },
        ),
        task("identity", Body::Do(vec![])),
        task(
            "identity",
            Body::Try {
                body: vec![task("s", same())],
                catch: Box::new(Catch {
                    status: None,
                    as_: None,
                    when: None,
                    retry: None,
                    handler: Some(raise()),
                    then: Some(Flow::End),
                }),
            },
        ),
    ]
}

#[test]
fn inserting_an_identity_task_changes_nothing() {
    let strategy = (
        program(Profile::DETERMINISTIC),
        input(),
        any::<prop::sample::Index>(),
        any::<prop::sample::Index>(),
        0..identities().len(),
    );
    check(strategy, |(p, input, which, at, kind)| {
        let mut q = p.clone();
        let list = list_at(&mut q, &pick(&p, which));
        let at = at.index(list.len() + 1);
        insert(list, at, identities().swap_remove(kind));
        same(&p, &q, input)
    });
}

#[test]
fn identity_transforms_change_nothing() {
    check((program(Profile::DETERMINISTIC), input()), |(p, input)| {
        let mut q = p.clone();
        each_task(&mut q, &mut |t| {
            t.input_from.get_or_insert(Data::Same);
            t.output_as.get_or_insert(Data::Same);
            t.when.get_or_insert(Cond::True);
        });
        q.input_from.get_or_insert(Data::Same);
        q.output_as.get_or_insert(Data::Same);
        same(&p, &q, input)
    });
}

#[test]
fn set_as_an_expression_or_a_template_is_the_same() {
    check((program(Profile::DETERMINISTIC), input()), |(p, input)| {
        let mut q = p.clone();
        each_task(&mut q, &mut |t| {
            if let Body::Set(Template::Obj(n)) = &t.body {
                t.body = Body::Set(Template::Expr(Data::Obj(n.clone())));
            }
        });
        same(&p, &q, input)
    });
}

#[test]
fn bare_and_wrapped_expressions_are_the_same() {
    check((program(Profile::DETERMINISTIC), input()), |(p, input)| {
        let q = Program { bare: !p.bare, ..p.clone() };
        same(&p, &q, input)
    });
}

// ── 3. flow directives ─────────────────────────────────────────────────────

#[test]
fn an_explicit_then_continue_is_no_then() {
    check((program(Profile::DETERMINISTIC), input()), |(p, input)| {
        let mut q = p.clone();
        each_task(&mut q, &mut |t| {
            t.then.get_or_insert(Flow::Continue);
        });
        same(&p, &q, input)
    });
}

// ── 4. fork ────────────────────────────────────────────────────────────────

#[test]
fn a_fork_outputs_what_each_branch_outputs_alone() {
    check((program(Profile::DETERMINISTIC), input()), |(p, input)| {
        let mut forked = Program {
            input_from: None,
            output_as: None,
            functions: p.functions.clone(),
            tasks: vec![task("f", Body::Fork { branches: p.tasks.clone(), compete: false })],
            bare: p.bare,
        };
        // Alone in their scope, the branches lose their jumps.
        fix(&mut forked, Profile::DETERMINISTIC);
        // A function failing the first calls would answer by interleaving.
        each_task(&mut forked, &mut |t| {
            if let Body::Call(f, _) = &mut t.body
                && f == "flaky"
            {
                *f = "inc".into();
            }
        });
        let Body::Fork { branches, .. } = &forked.tasks[0].body else { unreachable!() };
        let fork = execute(&forked, input.clone(), &Options::default());
        let alone: Vec<Ran> = branches
            .iter()
            .map(|b| execute(&Program { tasks: vec![b.clone()], ..forked.clone() }, input.clone(), &Options::default()))
            .collect();
        if hit_limit(&fork.outcome) || alone.iter().any(|r| hit_limit(&r.outcome)) {
            return Ok(());
        }
        let results: Vec<Observed> = alone.iter().map(|r| observed(&r.outcome)).collect();
        let outputs: Option<Vec<Value>> = results
            .iter()
            .map(|o| match o {
                Observed::Completed(v) => Some(v.clone()),
                _ => None,
            })
            .collect();
        let ok = match (observed(&fork.outcome), outputs) {
            (Observed::Completed(v), Some(outs)) => v == json!(outs),
            (fault @ Observed::Faulted { .. }, None) => results.contains(&fault),
            _ => false,
        };
        prop_assert!(ok, "fork {:?}\nalone {results:?}\n{}", observed(&fork.outcome), pretty(&definition(&forked)));
        Ok(())
    });
}
