// Runs on the engine (docs/designs/03-runs-on-the-engine.md): the design's
// tests 1–18, one section each, on the real engine store.
mod support;
use serde_json::json;
use support::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_loop_answers_as_an_engine_run() {
    let (tracer, _written) = recording();
    let h = Harness::new(task_context("t1"), vec![turn(|_| answer("hello"))], "ok", tracer);
    let live = h.start().await;
    assert_eq!(live.send("hi".into()).await.unwrap(), "hello");
    let run = h.engine.runtime.get("parent").await.unwrap().unwrap();
    assert_eq!(run.admission.name, "agent-loop");
    assert_eq!(run.admission.host().unwrap()["kind"], "loop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_loop_calls_a_read_and_answers() {
    let (tracer, _written) = recording();
    let h = Harness::new(
        task_context("t2"),
        vec![turn(|_| call("search_news", json!({"query":"x"}))), turn(|p| answer(&last_result(&p.messages)))],
        "found",
        tracer,
    );
    let live = h.start().await;
    let out = live.send("find".into()).await.unwrap();
    assert!(out.contains("found"), "{out}");
    assert_eq!(h.mcp.names(), vec!["search_news"]);
}

fn signal(id: i64, source: &str, content: &str) -> agent::supervisor::PendingSignal {
    agent::supervisor::PendingSignal {
        id,
        source: source.into(),
        content: content.into(),
        env_context: None,
        created_at: "2026-09-06 12:00:00".into(),
        payload: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_signal_runs_its_primary_loop_as_a_child_of_its_run() {
    let provider = std::sync::Arc::new(Scripted::new(vec![agent::providers::assistant("done")]));
    let f = fixture(provider, &[], &[("scheduler", "---\ntools: *\n---\nscheduled")]);
    let supervisor = agent::supervisor::create_supervisor(f.engine.clone());
    let out = supervisor.run_signal(&signal(7, "scheduler", "do it")).await.unwrap();
    assert_eq!(out, "done");
    let run = f.engine.runtime.get("signal:7").await.unwrap().unwrap();
    assert!(run.ended_ms.is_some());
    supervisor.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_runs_as_a_host_child_and_its_answer_reaches_the_parent() {
    let (tracer, _written) = recording();
    let h = Harness::new(
        task_context("t3"),
        vec![
            turn(|_| call("invoke_sub_agent", json!({"prompt":"sub task"}))),
            turn(|_| answer("worker answer")),
            turn(|p| answer(&format!("parent got {}", last_result(&p.messages)))),
        ],
        "ok",
        tracer,
    );
    let live = h.start().await;
    let out = live.send("delegate".into()).await.unwrap();
    assert!(out.contains("worker answer"), "{out}");
    assert!(out.contains("w1.0"), "{out}");
}
