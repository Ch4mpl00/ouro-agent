// Dry runs on a signal (`try_workflow`) return with the engine migration's
// sandbox step (design 01, step 4: a second in-memory Runtime with the drafts
// activated and a dry registry). Until then the tool answers that it is
// unavailable (design 03, decision 9); the sandbox suite is parked in
// tests/pending/sandbox.rs, to run again in step 4.
mod support;
use serde_json::json;
use support::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn try_workflow_answers_that_it_returns_with_the_sandbox_step() {
    let (tracer, _written) = recording();
    let h = Harness::new(
        task_context("try"),
        vec![
            turn(|_| call("try_workflow", json!({"name":"digest","signal":{"source":"scheduler","content":"x"}}))),
            turn(|p| answer(&last_result(&p.messages))),
        ],
        "ok",
        tracer,
    );
    let mut opts = h.options();
    opts.skills = vec!["workflow-author".into()];
    let out = h.engine.start_loop(opts).await.unwrap().send("try it".into()).await.unwrap();
    assert!(out.contains("unavailable until the engine migration's sandbox step"), "{out}");
    assert!(h.mcp.names().is_empty(), "a dry run makes no call");
}

#[ignore = "step 4 (sandbox): the parked suite in tests/pending/sandbox.rs runs again on the second Runtime"]
#[test]
fn the_sandbox_suite_returns_with_step_4() {}
