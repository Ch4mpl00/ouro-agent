// Port of src/agent-loop.test.ts, one test per case: a real engine, scripted
// model turns, a recording MCP and KV, a skill store that knows every skill.
// Where the TS test spies on startAgentLoop / endAgentLoop, this one reads
// the trace the loops leave and the engine's loop registry. The tests after
// "Rust-only" run the same runtime against real skill files and agent.db.
mod support;
use agent::{
    agent::{EngineDeps, LoopOptions, Providers, create_engine},
    db::{MemoryStore, TraceStore},
    errors::is_aborted,
    langfuse::{Attr, FinishedSpan, LangfuseTracer, SpanSink},
    mcp::Mcp,
    models::default_presets,
    providers::{ChatProvider, CompletionParams, CompletionResult, ProviderKind, assistant},
    session::{EnvDeps, Format},
    skills::{FsSkillStore, SkillStore, SkillTools},
    supervisor::{PendingSignal, create_supervisor},
    trace_model::ObservationType,
    tracing::{NullTracer, Tracer, tee_tracer},
};
use async_trait::async_trait;
use serde_json::{Map, Value, json};
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use support::*;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

fn null() -> Arc<dyn Tracer> {
    Arc::new(NullTracer)
}

fn mentions(messages: &[agent::providers::Message], text: &str) -> bool {
    agent::generation::messages_json(messages).to_string().contains(text)
}

// ── AgentLoop working memory ────────────────────────────────────────────────

#[tokio::test]
async fn saves_small_mcp_and_synthetic_outputs_without_changing_mcp_schemas_or_arguments() {
    let session = task_context("task");
    let (s1, s2) = (session.clone(), session.clone());
    let h = Harness::new(
        session.clone(),
        vec![
            turn(|p| {
                assert_eq!(p.tools.iter().find(|t| t.name == "search_news"), Some(&search_tool()));
                call("search_news", json!({"query":"today"}))
            }),
            turn(move |p| {
                let reply = last_json(&p.messages);
                assert_eq!((&reply["content"], &reply["truncated"]), (&json!("found"), &json!(false)));
                assert_eq!(s1.memory.get(reply["memory_key"].as_str().unwrap()).unwrap(), "found");
                // Not the loop's own skill, which would come back as a note.
                call("read_skill", json!({"name":"other"}))
            }),
            turn(move |p| {
                let reply = last_json(&p.messages);
                let content: Value = serde_json::from_str(reply["content"].as_str().unwrap()).unwrap();
                assert_eq!(content["content"], "test skill");
                assert_eq!(reply["format"], "json");
                assert_eq!(s2.memory.get(reply["memory_key"].as_str().unwrap()).unwrap(), reply["content"]);
                answer("done")
            }),
        ],
        "found",
        null(),
    );
    assert_eq!(h.start().await.send("search".into()).await.unwrap(), "done");
    assert_eq!(*h.mcp.calls.lock().unwrap(), [("search_news".to_owned(), obj(json!({"query":"today"})))]);
    assert_eq!(session.memory.list().len(), 2);
}

fn obj(v: Value) -> Map<String, Value> {
    v.as_object().unwrap().clone()
}

#[tokio::test]
async fn keeps_large_results_out_of_the_next_llm_turn_and_allows_an_explicit_full_read() {
    let payload = "news ".repeat(2_000) + "secret tail";
    let session = task_context("task");
    let (s1, expected, expected2) = (session.clone(), payload.clone(), payload.clone());
    let h = Harness::new(
        session.clone(),
        vec![
            turn(|_| call("search_news", json!({"query":"today"}))),
            turn(move |p| {
                let reply = last_json(&p.messages);
                assert_eq!(reply["truncated"], true);
                assert!(!mentions(&p.messages, "secret tail"));
                assert_eq!(s1.memory.get(reply["memory_key"].as_str().unwrap()).unwrap(), expected);
                call("working_memory_get", json!({"key": reply["memory_key"]}))
            }),
            turn(move |p| {
                assert_eq!(last_result(&p.messages), expected2);
                answer("read")
            }),
        ],
        &payload,
        null(),
    );
    h.start().await.send("search".into()).await.unwrap();
    assert_eq!(session.memory.list().len(), 1);
}

#[tokio::test]
async fn supports_put_get_list_delete_without_creating_recursive_memory_entries() {
    let session = task_context("task");
    let h = Harness::new(
        session.clone(),
        vec![
            turn(|_| call("working_memory_put", json!({"key":"mem.news","value":"","format":"json"}))),
            turn(|p| {
                assert_eq!(last_json(&p.messages), json!({"memory_key":"mem.news","format":"json","size_bytes":0}));
                call("working_memory_put", json!({"key":"mem.news","value":"overwrite"}))
            }),
            turn(|p| {
                assert!(last_result(&p.messages).contains("Key \"mem.news\" already exists"));
                call("working_memory_get", json!({"key":"mem.news"}))
            }),
            turn(|p| {
                assert_eq!(last_result(&p.messages), "");
                call("working_memory_list", json!({}))
            }),
            turn(|p| {
                assert_eq!(last_json(&p.messages), json!([{"key":"mem.news","format":"json","sizeBytes":0}]));
                call("working_memory_delete", json!({"key":"mem.news"}))
            }),
            turn(|p| {
                assert_eq!(last_json(&p.messages)["deleted"], true);
                call("working_memory_delete", json!({"key":"mem.news"}))
            }),
            turn(|p| {
                assert_eq!(last_json(&p.messages)["deleted"], false);
                call("working_memory_get", json!({"key":"mem.news"}))
            }),
            turn(|p| {
                assert!(last_result(&p.messages).contains("Key \"mem.news\" not found"));
                answer("done")
            }),
        ],
        "found",
        null(),
    );
    h.start().await.send("use memory".into()).await.unwrap();
    assert!(session.memory.list().is_empty());
    assert!(h.mcp.calls.lock().unwrap().is_empty());
    assert!(h.persist.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn validates_memory_tool_arguments_before_writing() {
    for args in
        [json!({"key":"","value":"x"}), json!({"key":"x","value":42}), json!({"key":"x","value":"x","format":"yaml"})]
    {
        let session = task_context("task");
        let h = Harness::new(
            session.clone(),
            vec![
                turn(move |_| call("working_memory_put", args)),
                turn(|p| {
                    assert!(last_result(&p.messages).contains("[working_memory_put error]"));
                    answer("recovered")
                }),
            ],
            "found",
            null(),
        );
        h.start().await.send("store".into()).await.unwrap();
        assert!(session.memory.list().is_empty());
    }
}

#[tokio::test]
async fn passes_references_directly_to_a_child_and_shares_memory_without_copying_parent_history() {
    let payload = "source ".repeat(2_000) + "source tail";
    let session = task_context("task");
    let s1 = session.clone();
    let (tracer, written) = recording();
    let h = Harness::new(
        session.clone(),
        vec![
            turn(|_| call("search_news", json!({}))),
            turn(|p| {
                let key = last_json(&p.messages)["memory_key"].clone();
                call("invoke_sub_agent", json!({"skills":["test"],"prompt":"summarize","input_refs":[key]}))
            }),
            turn(|p| {
                assert!(mentions(&p.messages, "source tail"));
                assert!(!mentions(&p.messages, "parent-only instruction"));
                assert!(!p.tools.iter().any(|t| t.name == "invoke_sub_agent"));
                call("working_memory_put", json!({"key":"summary","value":"short summary"}))
            }),
            turn(|_| answer("summary saved")),
            turn(move |p| {
                assert!(!mentions(&p.messages, "source tail"));
                assert_eq!(last_json(&p.messages)["content"], "summary saved");
                assert_eq!(s1.memory.get("summary").unwrap(), "short summary");
                answer("done")
            }),
        ],
        &payload,
        tracer,
    );
    h.start().await.send("parent-only instruction".into()).await.unwrap();
    // Two loops: the parent and one worker, which ran on the parent's session.
    let observations = &written.all()[0].observations;
    let worker = find(observations, "invoke_sub_agent");
    assert_eq!(worker.meta("agent_id"), Some(&json!("parent__sub1")));
    assert_eq!(
        observations
            .iter()
            .filter(|o| o.parent_observation_id.as_ref() == Some(&worker.id))
            .filter(|o| o.kind == ObservationType::Generation)
            .count(),
        2
    );
    assert_eq!(session.memory.list().len(), 3);
}

#[tokio::test]
async fn reports_a_missing_input_ref_without_starting_a_child() {
    let (tracer, written) = recording();
    let h = Harness::new(
        task_context("task"),
        vec![
            turn(|_| {
                call("invoke_sub_agent", json!({"skills":["test"],"prompt":"summarize","input_refs":["missing"]}))
            }),
            turn(|p| {
                assert!(last_json(&p.messages)["content"].as_str().unwrap().contains("Key \"missing\" not found"));
                answer("recovered")
            }),
        ],
        "found",
        tracer,
    );
    h.start().await.send("delegate".into()).await.unwrap();
    let observations = &written.all()[0].observations;
    let worker = find(observations, "invoke_sub_agent");
    assert!(!observations.iter().any(|o| o.parent_observation_id.as_ref() == Some(&worker.id)));
    assert!(worker.meta("agent_id").is_none_or(|id| id == "parent"));
    assert_eq!(h.requests(), 2);
}

#[tokio::test]
async fn stores_a_large_sub_agent_answer_without_adding_it_to_the_parents_context() {
    let payload = "child answer ".repeat(1_000) + "child tail";
    let session = task_context("task");
    let (s1, child, expected) = (session.clone(), payload.clone(), payload.clone());
    let h = Harness::new(
        session,
        vec![
            turn(|_| call("invoke_sub_agent", json!({"skills":["test"],"prompt":"report"}))),
            turn(move |_| answer(&child)),
            turn(move |p| {
                let reply = last_json(&p.messages);
                assert_eq!(reply["truncated"], true);
                assert!(!mentions(&p.messages, "child tail"));
                assert_eq!(s1.memory.get(reply["memory_key"].as_str().unwrap()).unwrap(), expected);
                answer("done")
            }),
        ],
        "found",
        null(),
    );
    h.start().await.send("delegate".into()).await.unwrap();
}

#[tokio::test]
async fn returns_invalid_arguments_as_a_recoverable_tool_reply() {
    for raw in ["null", "[]", "{broken"] {
        let h = Harness::new(
            task_context("task"),
            vec![
                turn(move |_| {
                    json!({"role":"assistant","content":"","tool_calls":[{"id":"bad_call","type":"function","function":{"name":"search_news","arguments":raw}}]})
                        .as_object()
                        .unwrap()
                        .clone()
                }),
                turn(|p| {
                    let content = last_json(&p.messages)["content"].as_str().unwrap().to_owned();
                    assert!(content.contains("[tool error] arguments must be a JSON object"), "{content}");
                    answer("recovered")
                }),
            ],
            "found",
            null(),
        );
        h.start().await.send("search".into()).await.unwrap();
        assert!(h.mcp.calls.lock().unwrap().is_empty(), "{raw}");
    }
}

#[tokio::test]
async fn keeps_independent_tasks_isolated_even_on_the_same_engine() {
    let session = task_context("task");
    let h = Harness::new(session.clone(), Vec::new(), "found", null());
    session.memory.put("private", "first task", Format::Text).unwrap();
    let second_context = task_context("second");
    let first = h.start().await;
    let second = h.engine.start_loop(LoopOptions::new("second-loop", second_context.clone())).await.unwrap();
    assert!(Arc::ptr_eq(&first.opts.session, &session));
    assert!(Arc::ptr_eq(&second.opts.session, &second_context));
    assert!(second.opts.session.memory.list().is_empty());
}

#[tokio::test]
async fn keeps_persistent_set_memory_writes_separate_from_temporary_result_storage() {
    let session = task_context("task");
    let h = Harness::new(
        session.clone(),
        vec![turn(|_| call("set_memory", json!({"key":"watermark","value":"today"}))), turn(|_| answer("done"))],
        "found",
        null(),
    );
    h.start().await.send("save".into()).await.unwrap();
    assert_eq!(*h.persist.0.lock().unwrap(), [("watermark".to_owned(), "today".to_owned())]);
    assert!(session.memory.get("watermark").unwrap_err().to_string().contains("not found"));
    assert_eq!(session.memory.list().len(), 1);
}

#[tokio::test]
async fn stores_parallel_calls_under_distinct_keys_and_preserves_message_order() {
    let session = task_context("task");
    let h = Harness::new(
        session.clone(),
        vec![
            turn(|_| {
                calls_of(&[
                    call("search_news", json!({"query":"first"})),
                    call("search_news", json!({"query":"second"})),
                ])
            }),
            turn(|p| {
                let replies = tool_replies(&p.messages);
                assert_eq!(
                    replies.iter().map(|r| r["content"].clone()).collect::<Vec<_>>(),
                    [json!("first result"), json!("second result")]
                );
                assert_ne!(replies[0]["memory_key"], replies[1]["memory_key"]);
                answer("done")
            }),
        ],
        "found",
        null(),
    );
    // The first call finishes only once the second has run.
    let (release, released) = oneshot::channel::<String>();
    h.mcp.once(Box::new(move |_, _, _| Box::pin(async move { Ok(released.await.unwrap()) })));
    h.mcp.once(Box::new(move |_, _, _| {
        Box::pin(async move {
            release.send("first result".into()).unwrap();
            Ok("second result".into())
        })
    }));
    h.start().await.send("parallel".into()).await.unwrap();
    assert_eq!(session.memory.list().len(), 2);
}

#[tokio::test]
async fn returns_parallel_successes_and_failures_to_the_model_for_its_next_decision() {
    let session = task_context("task");
    let h = Harness::new(
        session.clone(),
        vec![
            turn(|_| {
                calls_of(&[
                    call_id("search_news", json!({"query":"first"}), "first"),
                    call_id("search_news", json!({"query":"second"}), "second"),
                ])
            }),
            turn(|p| {
                let replies = tool_replies(&p.messages);
                assert_eq!(replies[0]["content"], "completed result");
                assert!(replies[1]["content"].as_str().unwrap().contains("connection lost"));
                answer("handled failure")
            }),
        ],
        "found",
        null(),
    );
    h.mcp.once(Box::new(|_, _, _| Box::pin(async { Ok("completed result".into()) })));
    h.mcp.once(Box::new(|_, _, _| Box::pin(async { Err(anyhow::anyhow!("connection lost")) })));
    assert_eq!(h.start().await.send("parallel".into()).await.unwrap(), "handled failure");
    let entries = session.memory.list();
    assert_eq!(entries.len(), 2);
    assert_eq!(session.memory.get(&entries[0].key).unwrap(), "completed result");
}

#[tokio::test]
async fn supports_a_general_worker_without_a_domain_skill() {
    let (tracer, written) = recording();
    let h = Harness::new(
        task_context("task"),
        vec![
            turn(|_| call("invoke_sub_agent", json!({"prompt":"extract the answer"}))),
            turn(|_| answer("42")),
            turn(|p| {
                assert_eq!(last_json(&p.messages)["content"], "42");
                answer("done")
            }),
        ],
        "found",
        tracer,
    );
    h.start().await.send("extract".into()).await.unwrap();
    let worker = find(&written.all()[0].observations, "invoke_sub_agent").clone();
    assert_eq!(worker.meta("skills"), Some(&json!(["worker"])));
}

// ── AgentLoop lifecycle ─────────────────────────────────────────────────────

#[tokio::test]
async fn aborts_a_pending_generation_and_ignores_its_late_tool_calls_after_close() {
    let (started, signal) = oneshot::channel::<CancellationToken>();
    let (late, pending) = oneshot::channel::<agent::providers::Message>();
    let (tracer, written) = recording();
    let session = task_context("task");
    let h = Harness::new(
        session.clone(),
        vec![turn_async(move |p| async move {
            started.send(p.cancel.clone()).unwrap();
            // Deliberately ignores cancellation, like an uncooperative SDK.
            Ok(pending.await?)
        })],
        "unused",
        tracer,
    );
    let loop_ = h.start().await;
    let sent = tokio::spawn(loop_.send("work".into()));
    let signal = signal.await.unwrap();
    assert!(loop_.send("overlap".into()).await.unwrap_err().to_string().contains("already running"));
    loop_.close().await;
    assert!(is_aborted(&sent.await.unwrap().unwrap_err()));
    assert!(signal.is_cancelled());
    let _ = late.send(call("search_news", json!({"query":"late"})));
    tokio::task::yield_now().await;
    assert!(h.mcp.calls.lock().unwrap().is_empty());
    assert_eq!(h.requests(), 1);
    assert!(session.memory.list().is_empty());
    assert_eq!(loop_.messages.lock().await.iter().filter(|m| m["role"] == "user").count(), 1);
    loop_.close().await;
    let traces = written.all();
    assert_eq!(traces.len(), 1);
    assert_eq!(traces[0].observations.iter().find(|o| o.kind == ObservationType::Generation).unwrap().level, "ERROR");
    assert!(is_aborted(&loop_.send("closed".into()).await.unwrap_err()));
}

#[tokio::test]
async fn cancels_a_child_loop_and_closes_its_observations_before_the_parent_trace() {
    let (started, signal) = oneshot::channel::<CancellationToken>();
    let (tracer, written) = recording();
    let session = task_context("task");
    let h = Harness::new(
        session.clone(),
        vec![
            turn(|_| call("invoke_sub_agent", json!({"prompt":"work"}))),
            turn_async(move |p| async move {
                started.send(p.cancel.clone()).unwrap();
                std::future::pending::<anyhow::Result<agent::providers::Message>>().await
            }),
        ],
        "unused",
        tracer,
    );
    let loop_ = h.start().await;
    let sent = tokio::spawn(loop_.send("delegate".into()));
    let child_signal = signal.await.unwrap();
    assert!(h.engine.loop_ids().await.contains(&"parent__sub1".to_owned()));
    loop_.close().await;
    assert!(is_aborted(&sent.await.unwrap().unwrap_err()));
    assert!(child_signal.is_cancelled());
    assert!(!h.engine.loop_ids().await.contains(&"parent__sub1".to_owned()));
    assert_eq!(h.requests(), 2);
    let traces = written.all();
    assert_eq!(traces.len(), 1);
    assert_eq!(find(&traces[0].observations, "invoke_sub_agent").level, "ERROR");
    assert!(session.memory.list().is_empty());
}

#[tokio::test]
async fn aborts_the_active_mcp_call_and_never_starts_queued_tools() {
    let (started, signal) = oneshot::channel::<CancellationToken>();
    let started = Mutex::new(Some(started));
    let session = task_context("task");
    let h = Harness::new(
        session.clone(),
        vec![turn(|_| calls_of(&[call_id("search_news", json!({}), "a"), call_id("search_news", json!({}), "b")]))],
        "unused",
        null(),
    );
    h.mcp.always(Arc::new(move |_, _, cancel| {
        if let Some(started) = started.lock().unwrap().take() {
            started.send(cancel.clone()).unwrap();
        }
        Box::pin(async move {
            cancel.cancelled().await;
            Err(agent::errors::aborted())
        })
    }));
    let mut opts = h.options();
    opts.max_concurrent_tools = 1;
    let loop_ = h.engine.start_loop(opts).await.unwrap();
    let sent = tokio::spawn(loop_.send("tools".into()));
    let signal = signal.await.unwrap();
    loop_.close().await;
    assert!(is_aborted(&sent.await.unwrap().unwrap_err()));
    assert!(signal.is_cancelled());
    assert_eq!(h.mcp.calls.lock().unwrap().len(), 1);
    assert!(session.memory.list().is_empty());
}

#[tokio::test]
async fn bounds_tool_concurrency_and_preserves_reply_order_when_completions_arrive_out_of_order() {
    let h = Harness::new(
        task_context("task"),
        vec![
            turn(|_| {
                calls_of(
                    &(0..3)
                        .map(|i| call_id("search_news", json!({"query": i.to_string()}), &i.to_string()))
                        .collect::<Vec<_>>(),
                )
            }),
            turn(|p| {
                let replies: Vec<_> = tool_replies(&p.messages).into_iter().map(|r| r["content"].clone()).collect();
                assert_eq!(replies, [json!("first"), json!("second"), json!("third")]);
                answer("done")
            }),
        ],
        "unused",
        null(),
    );
    let mut senders = Vec::new();
    let mut receivers = Vec::new();
    for _ in 0..3 {
        let (tx, rx) = oneshot::channel::<String>();
        senders.push(Some(tx));
        receivers.push(Some(rx));
    }
    let receivers = Arc::new(Mutex::new(receivers));
    let (second_started_tx, second_started) = oneshot::channel::<()>();
    let (third_started_tx, third_started) = oneshot::channel::<()>();
    let starts = Arc::new(Mutex::new([None, Some(second_started_tx), Some(third_started_tx)]));
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    {
        let (active, peak) = (active.clone(), peak.clone());
        h.mcp.always(Arc::new(move |_, args, _| {
            let i: usize = args["query"].as_str().unwrap().parse().unwrap();
            let now = active.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            if let Some(tx) = starts.lock().unwrap()[i].take() {
                tx.send(()).unwrap();
            }
            let rx = receivers.lock().unwrap()[i].take().unwrap();
            let active = active.clone();
            Box::pin(async move {
                let out = rx.await.unwrap();
                active.fetch_sub(1, Ordering::SeqCst);
                Ok(out)
            })
        }));
    }
    let mut opts = h.options();
    opts.max_concurrent_tools = 2;
    let loop_ = h.engine.start_loop(opts).await.unwrap();
    let sent = tokio::spawn(loop_.send("parallel".into()));
    second_started.await.unwrap();
    assert_eq!(h.mcp.calls.lock().unwrap().len(), 2);
    senders[1].take().unwrap().send("second".into()).unwrap();
    // The third starts while the first still runs: a slot frees as soon as
    // any call finishes.
    tokio::time::timeout(std::time::Duration::from_secs(5), third_started)
        .await
        .expect("the third call waited for the first")
        .unwrap();
    senders[2].take().unwrap().send("third".into()).unwrap();
    senders[0].take().unwrap().send("first".into()).unwrap();
    assert_eq!(sent.await.unwrap().unwrap(), "done");
    assert_eq!(peak.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn times_out_a_generation_aborts_its_request_and_records_the_failure() {
    let (started, signal) = oneshot::channel::<CancellationToken>();
    let (tracer, written) = recording();
    let h = Harness::new(
        task_context("task"),
        vec![turn_async(move |p| async move {
            started.send(p.cancel.clone()).unwrap();
            std::future::pending::<anyhow::Result<agent::providers::Message>>().await
        })],
        "unused",
        tracer,
    );
    let mut opts = h.options();
    opts.generation_timeout = Some(std::time::Duration::from_millis(10));
    let loop_ = h.engine.start_loop(opts).await.unwrap();
    assert_eq!(loop_.send("timeout".into()).await.unwrap_err().to_string(), "Generation timed out after 10ms");
    assert!(signal.await.unwrap().is_cancelled());
    let generation =
        written.all()[0].observations.iter().find(|o| o.kind == ObservationType::Generation).unwrap().clone();
    assert_eq!(generation.level, "ERROR");
    assert_eq!(generation.status_message.as_deref(), Some("Generation timed out after 10ms"));
}

// ── primary AgentLoop and tracing ───────────────────────────────────────────

fn signal(source: &str, content: &str) -> PendingSignal {
    PendingSignal {
        id: 1,
        source: source.into(),
        content: content.into(),
        env_context: Some("chatId=42".into()),
        created_at: "2026-09-06T12:00:00Z".into(),
        payload: None,
    }
}

const SIGNAL_CONTENT: &str = "chatId=42: summarize";

fn supervisor(h: &Harness) -> Arc<agent::supervisor::Supervisor> {
    activate_telegram_loop(&h.engine);
    let timezone: Arc<dyn Mcp> = ToolMcp::returning(Vec::new(), "{\"timezone\":\"UTC\"}");
    create_supervisor(
        h.engine.clone(),
        EnvDeps { mcp: timezone, memory: h.persist.clone(), user_email: None, telegram_chat_id: None },
        agent::flows::create_flows(h.engine.clone()),
    )
}

#[tokio::test]
async fn waits_for_cancellation_and_trace_cleanup_on_shutdown_without_starting_recovery() {
    for source in ["telegram", "scheduler"] {
        let (started, has_started) = oneshot::channel::<()>();
        let (local, written) = recording();
        let probe = Arc::new(ShutdownProbe {
            inner: local,
            written: written.clone(),
            shutdowns: AtomicUsize::new(0),
            written_at_shutdown: Mutex::default(),
        });
        let h = Harness::new(
            task_context("task"),
            vec![turn_async(move |_| async move {
                started.send(()).unwrap();
                std::future::pending::<anyhow::Result<agent::providers::Message>>().await
            })],
            "unused",
            probe.clone(),
        );
        let runner = supervisor(&h);
        let running = {
            let runner = runner.clone();
            let s = signal(source, SIGNAL_CONTENT);
            tokio::spawn(async move { runner.run_signal(&s).await })
        };
        has_started.await.unwrap();
        runner.shutdown().await;
        assert!(is_aborted(&running.await.unwrap().unwrap_err()), "{source}");
        runner.shutdown().await;
        let observations = &written.all()[0].observations;
        assert!(observations.iter().any(|o| o.name == "agent_loop"), "{source}");
        assert!(!observations.iter().any(|o| o.name == "recovery"), "{source}");
        assert_eq!(probe.shutdowns.load(Ordering::SeqCst), 1);
        // The trace was written before the tracer shut down.
        assert_eq!(*probe.written_at_shutdown.lock().unwrap(), [1]);
        assert_eq!(h.mcp.closed.load(Ordering::SeqCst), 1);
        assert!(is_aborted(&h.engine.start_loop(h.options()).await.err().unwrap()));
    }
}

#[tokio::test]
async fn loads_telegram_history_before_the_first_generation_and_shares_its_reference_with_workers() {
    let (tracer, written) = recording();
    let history = json!({"messages":[
        {"id":1,"chat_id":42,"thread_id":null,"role":"assistant","text":"Put together a weekly summary?","created_at":"2026-09-06 11:59:00"},
    ]});
    let mcp_calls: Arc<Mutex<Option<Arc<ToolMcp>>>> = Arc::default();
    let seen = mcp_calls.clone();
    let h = Harness::new(
        task_context("task"),
        vec![
            turn(move |p| {
                let mcp = seen.lock().unwrap().clone().unwrap();
                assert_eq!(
                    *mcp.calls.lock().unwrap(),
                    [
                        ("get_telegram_chat_history".to_owned(), obj(json!({"chatId":"42","limit":20}))),
                        ("journal_tail".to_owned(), obj(json!({"stream":"ouro/activity","n":10}))),
                        ("read_doc".to_owned(), obj(json!({"project":"agent-system","doc":"skill-index.md"}))),
                        (
                            "telegram_send_status".to_owned(),
                            obj(json!({"id":"status:1","text":"👀","chatId":"42","messageThreadId":null}))
                        ),
                    ]
                );
                assert!(mentions(&p.messages, "Put together a weekly summary?"));
                call("invoke_sub_agent", json!({"prompt":"summarize","input_refs":["telegram.history"]}))
            }),
            turn(|p| {
                assert!(mentions(&p.messages, "Put together a weekly summary?"));
                answer("summary")
            }),
            turn(|_| answer("done")),
            turn(|_| answer("done")),
        ],
        &history.to_string(),
        tracer,
    );
    *mcp_calls.lock().unwrap() = Some(h.mcp.clone());
    let runner = supervisor(&h);
    let content = "Telegram message in chat 42.\nText: \"go ahead\"";
    assert_eq!(runner.run_signal(&signal("telegram", content)).await.unwrap(), "done");
    // History, activity, skill-index lookup and the loop's status bubble,
    // opened and cleared; no worker fetch or extra LLM retrieval turn.
    assert_eq!(h.mcp.calls.lock().unwrap().len(), 5);
    let trace = &written.all()[0];
    assert_eq!(trace.input, Some(json!(content)));
    let fetch = find(&trace.observations, "get_telegram_chat_history");
    assert_eq!(fetch.kind, ObservationType::Tool);
    assert_eq!(fetch.meta("automatic"), Some(&json!(true)));
    assert_eq!(fetch.meta("memory_key"), Some(&json!("telegram.history")));
    assert_eq!(trace.observations.iter().filter(|o| o.kind == ObservationType::Generation).count(), 4);
}

#[tokio::test]
async fn guard_gets_only_preloaded_user_messages_and_raw_signal_for_parent_and_worker() {
    let history = json!({"messages":[
        {"id":1,"chat_id":42,"thread_id":null,"role":"user","text":"Yesterday: keep a weekly summary","created_at":"2026-09-06 11:58:00"},
        {"id":2,"chat_id":42,"thread_id":null,"role":"assistant","text":"BOT_HISTORY_MUST_STAY_OUT","created_at":"2026-09-06 11:59:00"},
    ]});
    let mut h = Harness::new(
        task_context("task"),
        vec![
            turn(|_| call("invoke_sub_agent", json!({"prompt":"prepare notes","input_refs":["telegram.history"]}))),
            turn(|_| call("set_memory", json!({"key":"worker_notes","value":"summary"}))),
            turn(|_| answer("notes ready")),
            turn(|_| call("set_memory", json!({"key":"parent_notes","value":"summary"}))),
            turn(|_| answer("done")),
            turn(|_| answer("done")),
        ],
        &history.to_string(),
        null(),
    );
    let api = Arc::new(GuardApi::default());
    Arc::get_mut(&mut h.engine).unwrap().deps.decisions =
        Some(agent::decisions::decision_client("guard-test".into(), api.clone()));
    let content = "Telegram message in chat 42.\nText: \"do what I asked yesterday\"";
    assert_eq!(supervisor(&h).run_signal(&signal("telegram", content)).await.unwrap(), "done");
    let requests = api.requests.lock().unwrap();
    let guards: Vec<_> = requests.iter().filter(|r| r["questions"].get("call_0").is_some()).collect();
    assert_eq!(guards.len(), 2);
    assert_eq!(guards[0]["state"]["task"], "prepare notes");
    assert_eq!(guards[0]["state"]["root_request"], content);
    assert_eq!(guards[1]["state"]["task"], content);
    for request in guards {
        assert!(
            request["state"]["earlier_user_messages"].as_str().unwrap().contains("Yesterday: keep a weekly summary")
        );
        assert!(!request.to_string().contains("BOT_HISTORY_MUST_STAY_OUT"));
        assert!(!request.to_string().contains("Working-memory inputs"));
    }
}

#[tokio::test]
async fn runs_a_signal_directly_and_records_parent_worker_tool_io_memory_references_and_per_iteration_usage() {
    let (primary, primary_written) = recording();
    let (mirror, mirror_written) = recording();
    let payload = "article ".repeat(2_000) + "source tail";
    let mut h = Harness::new(
        task_context("task"),
        vec![
            turn(|_| call("search_news", json!({"query":"latest"}))),
            turn(|p| {
                let key = last_json(&p.messages)["memory_key"].clone();
                call("invoke_sub_agent", json!({"skills":["news-query"],"prompt":"summarize","input_refs":[key]}))
            }),
            turn(|_| answer("short summary")),
            turn(|_| answer("finished")),
        ],
        &payload,
        Arc::new(tee_tracer(primary, mirror)),
    );
    Arc::get_mut(&mut h.engine).unwrap().deps.skills = vec!["orchestrator".into()];
    let runner = supervisor(&h);
    assert_eq!(runner.run_signal(&signal("telegram", SIGNAL_CONTENT)).await.unwrap(), "finished");
    assert_eq!(primary_written.len(), 1);
    assert_eq!(mirror_written.len(), 1);
    let trace = &mirror_written.all()[0];
    assert_eq!(trace.input, Some(json!(SIGNAL_CONTENT)));
    assert_eq!(trace.output, Some(json!("finished")));
    assert_eq!(trace.session_id.as_deref(), Some("telegram:1"));
    assert_eq!(trace.tags, ["telegram", "agent-loop"]);
    let agent = find(&trace.observations, "agent_loop");
    assert_eq!(agent.meta("preset"), Some(&json!("smart")));
    assert_eq!(agent.meta("skills"), Some(&json!(["orchestrator", "telegram"])));
    let worker = find(&trace.observations, "invoke_sub_agent");
    let search = find(&trace.observations, "search_news");
    let generations: Vec<_> = trace
        .observations
        .iter()
        .filter(|o| o.kind == ObservationType::Generation && o.name.starts_with("iter-"))
        .collect();
    assert_eq!(worker.kind, ObservationType::Agent);
    assert_eq!(worker.parent_observation_id.as_ref(), Some(&agent.id));
    assert_eq!(worker.output, json!("short summary"));
    assert_eq!(worker.meta("skill"), Some(&json!("news-query")));
    assert!(worker.input.to_string().contains("source tail"));
    assert_eq!(search.kind, ObservationType::Tool);
    assert_eq!(search.output, json!(payload));
    assert_eq!(search.meta("result_truncated"), Some(&json!(true)));
    assert_eq!(search.meta("result_size_bytes"), Some(&json!(payload.len())));
    assert!(search.meta("memory_key").is_some_and(Value::is_string));
    assert_eq!(search.meta("guard").unwrap()["label"], "allow");
    assert!(
        trace
            .observations
            .iter()
            .any(|o| o.name == "guard" && o.meta("verdicts").and_then(Value::as_array).is_some_and(|v| !v.is_empty()))
    );
    assert_eq!(generations.len(), 4);
    assert_eq!(generations.iter().filter(|o| o.parent_observation_id.as_ref() == Some(&worker.id)).count(), 1);
    for generation in &generations {
        assert_eq!(generation.usage_details, Some(obj(json!({"input":10,"output":5,"total":15,"cached":2}))));
    }
    // A snapshot before any tool calls.
    assert_eq!(generations[0].input.as_array().unwrap().len(), 2);
    assert!(!generations[1].input.to_string().contains("source tail"));
    let primary_ids: Vec<_> = primary_written.all()[0].observations[1..].iter().map(|o| o.id.clone()).collect();
    assert_eq!(trace.observations[1..].iter().map(|o| o.id.clone()).collect::<Vec<_>>(), primary_ids);
    let compiler = regex::Regex::new("compiler|workflow|planner").unwrap();
    assert!(!trace.observations.iter().any(|o| compiler.is_match(&o.name)));
}

#[tokio::test]
async fn records_tool_and_child_failures_as_errors_then_lets_the_parent_continue() {
    let (tracer, written) = recording();
    let h = Harness::new(
        task_context("task"),
        vec![
            turn(|_| call("search_news", json!({}))),
            turn(|_| call("invoke_sub_agent", json!({"prompt":"work"}))),
            fail("model offline"),
            turn(|_| answer("reported failure")),
        ],
        "[tool error] timeout",
        tracer,
    );
    h.start().await.send("do work".into()).await.unwrap();
    let observations = &written.all()[0].observations;
    assert_eq!(find(observations, "search_news").level, "ERROR");
    assert_eq!(find(observations, "invoke_sub_agent").level, "ERROR");
    let failed = observations.iter().find(|o| o.status_message.as_deref() == Some("model offline")).unwrap();
    assert_eq!(failed.kind, ObservationType::Generation);
}

#[tokio::test]
async fn recovers_a_failed_primary_loop_in_the_same_trace_and_releases_both_loops() {
    let (tracer, written) = recording();
    let h = Harness::new(
        task_context("task"),
        vec![
            fail("primary model offline"),
            turn(|p| {
                assert!(mentions(&p.messages, SIGNAL_CONTENT));
                answer("recovery report")
            }),
        ],
        "unused",
        tracer,
    );
    let runner = supervisor(&h);
    let err = runner.run_signal(&signal("telegram", SIGNAL_CONTENT)).await.unwrap_err();
    assert!(err.to_string().contains("primary model offline"), "{err}");
    assert!(h.engine.loop_ids().await.is_empty());
    let traces = written.all();
    assert_eq!(traces.len(), 1);
    assert_eq!(traces[0].output, Some(json!({"error":"primary model offline"})));
    let recovery = find(&traces[0].observations, "recovery");
    assert_eq!(recovery.kind, ObservationType::Agent);
    assert_eq!(recovery.output, json!("recovery report"));
    assert_eq!(recovery.meta("skills"), Some(&json!(["recovery"])));
    assert_eq!(recovery.meta("max_iterations"), Some(&json!(5)));
    // The recovery loop runs on the primary loop's session.
    assert_eq!(recovery.meta("agent_id"), Some(&json!("telegram:1__recovery")));
    assert_eq!(recovery.meta("parent_id"), Some(&json!("telegram:1")));
}

#[tokio::test]
async fn discovers_domain_skills_without_preloading_them_into_the_parent() {
    let (tracer, written) = recording();
    let h = Harness::new(task_context("task"), vec![turn(|_| answer("done"))], "unused", tracer);
    supervisor(&h).run_signal(&signal("news-digest", SIGNAL_CONTENT)).await.unwrap();
    assert_eq!(find(&written.all()[0].observations, "agent_loop").meta("skills"), Some(&json!([])));
    let system = h.model.requests.lock().unwrap()[0][0]["content"].as_str().unwrap().to_owned();
    assert!(system.contains("Call find_skills only when the task needs something specific"));
}

#[tokio::test]
async fn loads_the_shipped_orchestration_and_worker_prompts_with_valid_frontmatter() {
    let repo = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let skills =
        FsSkillStore { live_dir: "/nonexistent-agent-test-skills".into(), defaults_dir: repo.join("skills.default") };
    let root = skills.read_skill("orchestrator").await.unwrap().unwrap();
    let worker = skills.read_skill("worker").await.unwrap().unwrap();
    assert_eq!(root.tools, SkillTools::All);
    assert!(root.body.contains("input_refs"));
    assert_eq!(worker.tools, SkillTools::All);
}

#[derive(Default)]
struct Exported(Mutex<Vec<FinishedSpan>>);

impl SpanSink for Exported {
    fn finished(&self, span: FinishedSpan) {
        self.0.lock().unwrap().push(span);
    }
}

fn attr<'a>(span: &'a FinishedSpan, key: &str) -> Option<&'a str> {
    span.attributes.iter().find(|(k, _)| k == key).and_then(|(_, v)| match v {
        Attr::Str(s) => Some(s.as_str()),
        Attr::StrArray(_) => None,
    })
}

#[tokio::test]
async fn exports_the_primary_loop_and_nested_worker_through_the_langfuse_adapter() {
    let (local, local_written) = recording();
    let exported = Arc::new(Exported::default());
    let langfuse = Arc::new(LangfuseTracer::with_sink(exported.clone()));
    let tracer = Arc::new(tee_tracer(langfuse, local));
    let mut h = Harness::new(
        task_context("task"),
        vec![
            turn(|_| call("search_news", json!({"query":"latest"}))),
            turn(|p| {
                let key = last_json(&p.messages)["memory_key"].clone();
                call("invoke_sub_agent", json!({"prompt":"summarize","input_refs":[key]}))
            }),
            turn(|_| answer("summary")),
            turn(|_| answer("done")),
        ],
        &"large result ".repeat(1_000),
        tracer.clone(),
    );
    Arc::get_mut(&mut h.engine).unwrap().deps.skills = vec!["orchestrator".into()];
    supervisor(&h).run_signal(&signal("telegram", SIGNAL_CONTENT)).await.unwrap();
    let spans = exported.0.lock().unwrap().clone();
    let by_name = |name: &str| spans.iter().find(|s| s.name == name).unwrap().clone();
    let root = by_name("signal:telegram");
    let primary = by_name("agent_loop");
    let worker = by_name("invoke_sub_agent");
    let tool = by_name("search_news");
    assert_eq!(attr(&root, "session.id"), Some("telegram:1"));
    assert!(attr(&root, "langfuse.observation.output").unwrap().contains("done"));
    assert_eq!(attr(&worker, "langfuse.observation.type"), Some("agent"));
    assert_eq!(worker.parent_span_id.as_ref(), Some(&primary.span_id));
    assert!(attr(&worker, "langfuse.observation.input").unwrap().contains("large result"));
    assert_eq!(attr(&tool, "langfuse.observation.type"), Some("tool"));
    assert!(attr(&tool, "langfuse.observation.metadata.memory_key").is_some());
    assert!(attr(&tool, "langfuse.observation.output").unwrap().contains("large result"));
    let generations: Vec<_> = spans
        .iter()
        .filter(|s| attr(s, "langfuse.observation.type") == Some("generation") && s.name.starts_with("iter-"))
        .collect();
    assert_eq!(generations.len(), 4);
    assert!(generations.iter().any(|s| s.parent_span_id.as_ref() == Some(&worker.span_id)));
    let usage: Value =
        serde_json::from_str(attr(generations[0], "langfuse.observation.usage_details").unwrap()).unwrap();
    assert_eq!(usage, json!({"input":10,"output":5,"total":15,"cached":2}));
    let local = &local_written.all()[0];
    assert_eq!(local.id, root.trace_id);
    assert_eq!(find(&local.observations, "invoke_sub_agent").id, worker.span_id);
    tracer.shutdown().await;
}

// ── engine provider routing ─────────────────────────────────────────────────

struct Stub(ProviderKind);

#[async_trait]
impl ChatProvider for Stub {
    fn kind(&self) -> ProviderKind {
        self.0
    }
    async fn complete(&self, _: &mut CompletionParams<'_>) -> anyhow::Result<CompletionResult> {
        anyhow::bail!("Unexpected LLM call")
    }
}

fn engine_with(openrouter: Option<Arc<dyn ChatProvider>>) -> Arc<agent::agent::Engine> {
    create_engine(EngineDeps {
        providers: Providers {
            openai: Arc::new(Stub(ProviderKind::Openai)),
            gemini: Arc::new(Stub(ProviderKind::Gemini)),
            deepseek: Arc::new(Stub(ProviderKind::Deepseek)),
            openrouter,
        },
        mcp: ToolMcp::returning(Vec::new(), ""),
        presets: default_presets(),
        skills: Vec::new(),
        skill_store: Arc::new(AnySkill),
        memory: Arc::new(PersistLog::default()),
        tracer: null(),
        codex: Arc::new(NoCodex),
        decisions: None,
        workflows: std::sync::Arc::new(agent::flows::MemoryWorkflows::default()),
        schedule: std::sync::Arc::new(agent::schedule::MemorySchedule::default()),
    })
}

#[test]
fn routes_only_an_explicit_openrouter_prefix_to_openrouter_ahead_of_the_vendor_prefixes() {
    let engine = engine_with(Some(Arc::new(Stub(ProviderKind::Openrouter))));
    let kind = |model: &str| engine.resolve_provider(model).unwrap().kind();
    assert_eq!(kind("openrouter/deepseek/deepseek-v4-pro"), ProviderKind::Openrouter);
    assert_eq!(kind("openrouter/anthropic/claude-sonnet-5.5"), ProviderKind::Openrouter);
    assert_eq!(kind("deepseek-v4-pro"), ProviderKind::Deepseek);
    assert_eq!(kind("gemini-3.7-flash"), ProviderKind::Gemini);
    assert_eq!(kind("gpt-5.4"), ProviderKind::Openai);
}

#[test]
fn fails_loudly_when_an_openrouter_model_has_no_provider_configured() {
    let err = engine_with(None).resolve_provider("openrouter/qwen/qwen3").err().unwrap();
    assert!(err.to_string().contains("no OpenRouter provider"), "{err}");
}

#[test]
fn exposes_no_decision_client_unless_one_is_configured() {
    assert!(engine_with(None).deps.decisions.is_none());
}

// ── Rust-only: the same runtime on real skill files and agent.db ───────────

fn text(reply: &str) -> String {
    let value: Value = serde_json::from_str(reply).unwrap();
    value["content"].as_str().unwrap_or_default().to_owned()
}

#[tokio::test]
async fn whitelist_intersects_skill_tools_and_blocks_hallucinated_calls() {
    for (fm, whitelist, expected) in [
        ("*", Some(vec!["a"]), vec!["a"]),
        ("[a, b]", Some(vec!["b", "c"]), vec!["b"]),
        ("[a]", None, vec!["a"]),
        ("[]", None, vec![]),
    ] {
        let model =
            Arc::new(Scripted::new(vec![calls(&[("c", json!({})), ("missing", json!({}))]), assistant("done")]));
        let body = format!("---\ntools: {fm}\n---\n# domain\n");
        let f = fixture(model.clone(), &["a", "b", "c"], &[("domain", &body)]);
        let mut opts = LoopOptions::new("loop", context());
        opts.skills = vec!["domain".into()];
        opts.tool_whitelist = whitelist.map(|v| v.into_iter().map(str::to_owned).collect::<HashSet<_>>());
        let loop_ = f.engine.start_loop(opts).await.unwrap();
        assert_eq!(loop_.send("go".into()).await.unwrap(), "done");
        let seen = model.seen.lock().unwrap().clone();
        let offered: Vec<_> = seen[0]
            .tools
            .iter()
            .filter(|t| ["a", "b", "c"].contains(&t.name.as_str()))
            .map(|t| t.name.as_str())
            .collect();
        assert_eq!(offered, expected);
        assert!(f.mcp.called.lock().unwrap().is_empty());
        let replies: Vec<_> = seen[1].messages.iter().filter(|m| m["role"] == "tool").collect();
        assert!(replies.iter().all(|m| text(m["content"].as_str().unwrap()).contains("unavailable")));
        f.engine.shutdown().await;
    }
}
#[tokio::test]
async fn bounded_tools_keep_call_order_and_trace_raw_outputs() {
    let model = Arc::new(Scripted::new(vec![
        calls(&[
            ("echo", json!({"output":"first","delay":40})),
            ("echo", json!({"output":"second","delay":1})),
            ("echo", json!({"output":"third","delay":2})),
        ]),
        assistant("done"),
    ]));
    let f = fixture(model.clone(), &["echo"], &[("worker", "---\ntools: *\n---\nworker\n")]);
    let mut opts = LoopOptions::new("loop", context());
    opts.skills = vec!["worker".into()];
    opts.max_concurrent_tools = 2;
    let loop_ = f.engine.start_loop(opts).await.unwrap();
    loop_.send("go".into()).await.unwrap();
    let replies: Vec<_> = loop_
        .messages
        .lock()
        .await
        .iter()
        .filter(|m| m["role"] == "tool")
        .map(|m| text(m["content"].as_str().unwrap()))
        .collect();
    assert_eq!(replies, ["first", "second", "third"]);
    assert_eq!(f.mcp.peak.load(Ordering::SeqCst), 2);
    let (_, observations) = f.db.get_trace("loop").unwrap().unwrap();
    let tools: Vec<_> = observations.iter().filter(|o| o.name == "echo").collect();
    assert_eq!(tools.len(), 3);
    for t in tools {
        assert_eq!(t.metadata.as_ref().unwrap()["tool"], "echo");
        assert!(t.metadata.as_ref().unwrap().get("memory_key").is_some());
        assert!(t.output.is_string());
    }
    f.engine.shutdown().await;
}
#[tokio::test]
async fn working_memory_reads_stay_full_and_persistent_state_is_separate() {
    let big = "я🦊".repeat(2000);
    let model = Arc::new(Scripted::new(vec![
        calls(&[
            ("working_memory_put", json!({"key":"data","value":big,"format":"json"})),
            ("set_memory", json!({"key":"watermark","value":"today"})),
        ]),
        calls(&[("working_memory_get", json!({"key":"data"})), ("working_memory_list", json!({}))]),
        assistant("done"),
    ]));
    let f = fixture(model.clone(), &[], &[]);
    let context = context();
    let loop_ = f.engine.start_loop(LoopOptions::new("loop", context.clone())).await.unwrap();
    loop_.send("go".into()).await.unwrap();
    assert_eq!(context.memory.get("data").unwrap(), big);
    assert_eq!(f.db.get("watermark").unwrap().as_deref(), Some("today"));
    let seen = model.seen.lock().unwrap().clone();
    let replies: Vec<_> = seen[2].messages.iter().filter(|m| m["role"] == "tool").collect();
    assert_eq!(replies[2]["content"], big);
    // put/get/list are inline: only set_memory is itself saved.
    assert_eq!(context.memory.list().len(), 2);
    drop(seen);
    f.engine.shutdown().await;
}
#[tokio::test]
async fn workers_receive_refs_and_isolated_history_and_cannot_delegate() {
    let model = Arc::new(Scripted::new(vec![
        calls(&[("invoke_sub_agent", json!({"prompt":"process","input_refs":["raw"]}))]),
        calls(&[("invoke_sub_agent", json!({"prompt":"recurse"}))]),
        assistant("child result"),
        assistant("parent result"),
    ]));
    let f = fixture(model.clone(), &[], &[("worker", "---\ntools: *\n---\nworker\n")]);
    let context = context();
    context.memory.put("raw", "secret ${path}", agent::session::Format::Text).unwrap();
    let loop_ = f.engine.start_loop(LoopOptions::new("parent", context)).await.unwrap();
    assert_eq!(loop_.send("parent input".into()).await.unwrap(), "parent result");
    let seen = model.seen.lock().unwrap().clone();
    assert!(seen[0].tools.iter().any(|t| t.name == "invoke_sub_agent"));
    assert!(!seen[1].tools.iter().any(|t| t.name == "invoke_sub_agent"));
    assert!(seen[1].messages.iter().any(|m| m["content"].as_str().is_some_and(|s| s.contains("secret ${path}"))));
    assert!(!seen[1].messages.iter().any(|m| m["content"] == "parent input"));
    assert!(
        seen[2]
            .messages
            .iter()
            .filter(|m| m["role"] == "tool")
            .any(|m| text(m["content"].as_str().unwrap()).contains("unavailable"))
    );
    drop(seen);
    let (_, obs) = f.db.get_trace("parent").unwrap().unwrap();
    let worker = obs.iter().find(|o| o.name == "invoke_sub_agent").unwrap();
    assert!(obs.iter().any(|o| o.name == "iter-0" && o.parent_observation_id.as_deref() == Some(&worker.id)));
    f.engine.shutdown().await;
}
// The workflow-author worker alone is offered the function catalog, the
// probe and the dry run: every other loop would pay for their schemas on
// every turn. It runs on `smart` at least, whatever preset the parent left.
#[tokio::test]
async fn only_the_workflow_author_is_offered_the_catalog_and_the_probe() {
    let model = Arc::new(Scripted::new(vec![
        calls(&[("invoke_sub_agent", json!({"skills":["workflow-author"],"prompt":"draft"}))]),
        calls(&[("probe_function", json!({"name":"nope"}))]),
        assistant("child"),
        assistant("parent"),
    ]));
    let skills = [("parent", "---\ntools: *\n---\nparent\n"), ("workflow-author", "---\ntools: []\n---\nauthor\n")];
    let f = fixture(model.clone(), &["search_news"], &skills);
    let mut opts = LoopOptions::new("parent", context());
    opts.skills = vec!["parent".into()];
    let loop_ = f.engine.start_loop(opts).await.unwrap();
    loop_.send("go".into()).await.unwrap();
    let seen = model.seen.lock().unwrap().clone();
    let offered = |i: usize, name: &str| seen[i].tools.iter().any(|t| t.name == name);
    let author_tools = ["workflow_functions", "probe_function", "try_workflow", "save_workflow"];
    assert!(author_tools.iter().all(|t| !offered(0, t)), "the parent");
    assert!(offered(0, "search_news"), "the parent keeps its MCP tools");
    assert!(author_tools.iter().all(|t| offered(1, t)), "the author");
    assert!(!offered(1, "search_news"), "no MCP tool of its own");
    assert_eq!(seen[1].model, default_presets().smartest.model, "the author runs on smartest");
    assert_eq!(seen[1].reasoning_effort, agent::models::ReasoningEffort::Max);
    drop(seen);
    f.engine.shutdown().await;
}
// A non-writing worker asking for an already loaded skill gets a note.
#[tokio::test]
async fn reading_a_skill_the_loop_already_runs_returns_a_note() {
    let model = Arc::new(Scripted::new(vec![
        calls(&[("read_skill", json!({"name":"author"})), ("read_skill", json!({"name":"other"}))]),
        assistant("done"),
    ]));
    let skills = [("author", "---\ntools: []\n---\nauthor body\n"), ("other", "---\ntools: []\n---\nother body\n")];
    let f = fixture(model.clone(), &[], &skills);
    let mut opts = LoopOptions::new("loop", context());
    opts.skills = vec!["author".into()];
    opts.parent_id = Some("parent".into());
    f.engine.start_loop(opts).await.unwrap().send("go".into()).await.unwrap();
    let seen = model.seen.lock().unwrap().clone();
    let replies: Vec<String> = seen[1]
        .messages
        .iter()
        .filter(|m| m["role"] == "tool")
        .map(|m| m["content"].as_str().unwrap().to_owned())
        .collect();
    assert!(
        replies[0].contains("Already part of your instructions") && !replies[0].contains("author body"),
        "{}",
        replies[0]
    );
    assert!(replies[1].contains("other body"), "{}", replies[1]);
    drop(seen);
    f.engine.shutdown().await;
}
#[tokio::test]
async fn skill_write_authorization_cannot_be_created_by_model_arguments() {
    for authorized in [false, true] {
        let content = "---\ntools: []\n---\nrevised\n";
        let model = Arc::new(Scripted::new(vec![
            calls(&[("invoke_sub_agent", json!({"skills":["dreaming"],"prompt":"rewrite","allowSkillWrites":true}))]),
            calls(&[("write_skill", json!({"name":"telegram","content":content}))]),
            assistant("child"),
            assistant("parent"),
        ]));
        let f = fixture(
            model.clone(),
            &[],
            &[("dreaming", "---\ntools: []\n---\ndreaming\n"), ("telegram", "---\ntools: []\n---\noriginal\n")],
        );
        let mut opts = LoopOptions::new("parent", context());
        opts.skills = vec!["telegram".into()];
        opts.allow_skill_writes = authorized;
        let loop_ = f.engine.start_loop(opts).await.unwrap();
        loop_.send("rewrite".into()).await.unwrap();
        assert_eq!(model.seen.lock().unwrap()[1].tools.iter().any(|t| t.name == "write_skill"), authorized);
        assert_eq!(
            f.skills.read_skill("telegram").await.unwrap().unwrap().body,
            if authorized { "revised\n" } else { "original\n" }
        );
        f.engine.shutdown().await;
    }
}
#[tokio::test]
async fn writes_reject_broken_frontmatter_unknown_tools_and_escaping_names() {
    for (name, body) in [
        ("telegram", "no frontmatter"),
        ("telegram", "---\ntools: [unknown]\n---\ntext\n"),
        ("../escape", "---\ntools: []\n---\ntext\n"),
    ] {
        let model = Arc::new(Scripted::new(vec![
            calls(&[("write_skill", json!({"name":name,"content":body}))]),
            assistant("done"),
        ]));
        let f = fixture(model.clone(), &[], &[("dreaming", "---\ntools: []\n---\ndreaming\n")]);
        let mut opts = LoopOptions::new("loop", context());
        opts.skills = vec!["dreaming".into()];
        opts.allow_skill_writes = true;
        let loop_ = f.engine.start_loop(opts).await.unwrap();
        loop_.send("go".into()).await.unwrap();
        assert!(!f.root.path().join("skills/telegram.md").exists());
        let (_, obs) = f.db.get_trace("loop").unwrap().unwrap();
        let write = obs.iter().find(|o| o.name == "write_skill").unwrap();
        assert_eq!(write.level, "ERROR");
        assert!(write.output.as_str().unwrap().starts_with("[write_skill error]"));
        f.engine.shutdown().await;
    }
}
#[tokio::test]
async fn malformed_arguments_still_leave_error_spans_and_never_call_mcp() {
    for args in ["{broken", "null", "[]", "42"] {
        let message=json!({"role":"assistant","content":null,"tool_calls":[{"id":"bad","type":"function","function":{"name":"echo","arguments":args}}]}).as_object().unwrap().clone();
        let model = Arc::new(Scripted::new(vec![message, assistant("done")]));
        let f = fixture(model.clone(), &["echo"], &[]);
        let loop_ = f.engine.start_loop(LoopOptions::new("loop", context())).await.unwrap();
        loop_.send("go".into()).await.unwrap();
        assert!(f.mcp.called.lock().unwrap().is_empty());
        let (_, obs) = f.db.get_trace("loop").unwrap().unwrap();
        let span = obs.iter().find(|o| o.name == "echo").unwrap();
        assert_eq!(span.level, "ERROR");
        assert_eq!(span.input["raw_arguments"], args);
        f.engine.shutdown().await;
    }
}
#[tokio::test]
async fn budget_duplicate_ids_and_missing_skills_fail_loudly() {
    let model = Arc::new(Scripted::new(vec![calls(&[("working_memory_list", json!({}))])]));
    let f = fixture(model, &[], &[]);
    let mut missing = LoopOptions::new("missing", context());
    missing.skills = vec!["no-such-skill".into()];
    assert!(f.engine.start_loop(missing).await.is_err());
    let mut opts = LoopOptions::new("loop", context());
    opts.max_iterations = 1;
    let loop_ = f.engine.start_loop(opts.clone()).await.unwrap();
    assert!(f.engine.start_loop(opts).await.is_err());
    assert!(loop_.send("go".into()).await.unwrap_err().to_string().contains("maxIterations=1"));
    f.engine.end_loop("loop").await;
    assert!(loop_.run().await.is_err());
    f.engine.shutdown().await;
    f.engine.shutdown().await;
    assert_eq!(f.mcp.closed.load(Ordering::SeqCst), 1);
    assert!(f.engine.start_loop(LoopOptions::new("later", context())).await.is_err());
}
#[tokio::test]
async fn close_cancels_active_round_joins_tools_and_closes_trace_once() {
    let model =
        Arc::new(Scripted::new(vec![calls(&[("wait", json!({"delay":60000})), ("wait", json!({"delay":60000}))])]));
    let f = fixture(model, &["wait"], &[("worker", "---\ntools: *\n---\nworker\n")]);
    let mut opts = LoopOptions::new("loop", context());
    opts.skills = vec!["worker".into()];
    let loop_ = f.engine.start_loop(opts).await.unwrap();
    let running = tokio::spawn(loop_.send("go".into()));
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while f.mcp.active.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(loop_.run().await.unwrap_err().to_string().contains("already running"));
    loop_.close().await;
    assert!(running.await.unwrap().is_err());
    assert_eq!(f.mcp.active.load(Ordering::SeqCst), 0);
    let (trace, obs) = f.db.get_trace("loop").unwrap().unwrap();
    assert!(trace.output.get("error").is_some());
    assert_eq!(obs.iter().filter(|o| o.name == "wait" && o.level == "ERROR").count(), 2);
    f.engine.shutdown().await;
}

// ── Rust-only: the supervisor's own queue tool stays out of a model's reach ──

// wait_for_signal blocks on the signal queue; a model that called it would
// stall its run. It is never offered, even under `tools: *`, and a call to it
// by name is refused without reaching MCP.
#[tokio::test]
async fn a_model_is_never_offered_or_allowed_the_supervisors_wait_for_signal() {
    let model = Arc::new(Scripted::new(vec![calls(&[("wait_for_signal", json!({}))]), assistant("done")]));
    let f = fixture(model.clone(), &["a", "wait_for_signal"], &[("domain", "---\ntools: *\n---\n# domain\n")]);
    let mut opts = LoopOptions::new("loop", context());
    opts.skills = vec!["domain".into()];
    let loop_ = f.engine.start_loop(opts).await.unwrap();
    assert_eq!(loop_.send("go".into()).await.unwrap(), "done");
    let seen = model.seen.lock().unwrap().iter().map(|s| (s.tools.clone(), s.messages.clone())).collect::<Vec<_>>();
    assert!(seen[0].0.iter().any(|t| t.name == "a"));
    assert!(!seen[0].0.iter().any(|t| t.name == "wait_for_signal"));
    assert!(f.mcp.called.lock().unwrap().is_empty(), "the call never reached MCP");
    let reply = seen[1].1.iter().find(|m| m["role"] == "tool").unwrap();
    assert!(text(reply["content"].as_str().unwrap()).contains("unavailable"));
    f.engine.shutdown().await;
}

#[tokio::test]
async fn primary_discovers_and_reads_effective_skill_instructions_in_the_same_loop() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("skills.default")).unwrap();
    std::fs::create_dir(root.path().join("skills")).unwrap();
    let repo = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../skills.default");
    std::fs::copy(repo.join("orchestrator.md"), root.path().join("skills.default/orchestrator.md")).unwrap();
    std::fs::write(root.path().join("skills.default/domain.md"), "---\ntools: []\n---\nDOMAIN RULES").unwrap();
    std::fs::write(root.path().join("skills/domain.patch.md"), "EFFECTIVE PATCH").unwrap();
    let store = Arc::new(FsSkillStore::new(root.path()));
    let snapshot = agent::skills::catalog_snapshot(&*store).await.unwrap();
    let hash = snapshot["skills"].as_array().unwrap().iter().find(|s| s["name"] == "domain").unwrap()["content_hash"]
        .as_str()
        .unwrap()
        .to_owned();
    let expected = hash.clone();
    let mut h = Harness::new(
        task_context("discovery"),
        vec![
            turn(|p| {
                assert!(mentions(&p.messages, "follow them without searching again"));
                assert!(!mentions(&p.messages, "Typical routes:"));
                call("find_skills", json!({"query":"do domain task"}))
            }),
            turn(move |p| {
                assert!(last_json(&p.messages)["content"].as_str().unwrap().contains("domain"));
                call("read_skill", json!({"name":"domain","expected_hash":expected}))
            }),
            turn(|p| {
                let result: Value = serde_json::from_str(last_json(&p.messages)["content"].as_str().unwrap()).unwrap();
                assert!(result["content"].as_str().unwrap().contains("DOMAIN RULES"));
                assert!(result["content"].as_str().unwrap().contains("EFFECTIVE PATCH"));
                answer("task completed")
            }),
        ],
        "unused",
        null(),
    );
    struct Scorer;
    #[async_trait]
    impl agent::providers::JsonApi for Scorer {
        async fn post(&self, _: &str, _: Value, _: &CancellationToken) -> anyhow::Result<Value> {
            Ok(json!({"model":"jev-test","answers":{"skill_0":{"type":"noul","noul":0.9}}}))
        }
    }
    let mcp = ToolMcp::returning(vec![], &json!({"ok":false,"error":"doc_not_found"}).to_string());
    let engine = Arc::get_mut(&mut h.engine).unwrap();
    engine.deps.mcp = mcp.clone();
    engine.deps.decisions = Some(agent::decisions::decision_client("jev-test".into(), Arc::new(Scorer)));
    engine.deps.skill_store = store;
    engine.deps.skills = vec!["orchestrator".into()];
    let mut opts = h.options();
    opts.skills = vec![];
    let loop_ = h.engine.start_loop(opts).await.unwrap();
    assert_eq!(loop_.send("do domain task".into()).await.unwrap(), "task completed");
    assert_eq!(mcp.calls.lock().unwrap().iter().map(|(name, _)| name.as_str()).collect::<Vec<_>>(), ["read_doc"]);
    assert_eq!(h.engine.loop_ids().await.len(), 1, "no worker needed to consult a skill");
    h.engine.shutdown().await;
}

// ── finish: the round's decision ────────────────────────────────────────────

fn harness(turns: Vec<Turn>) -> Harness {
    Harness::new(task_context("finish"), turns, "found", null())
}

#[tokio::test]
async fn finish_alongside_the_last_action_ends_the_session_without_another_turn() {
    let h = harness(vec![turn(|_| calls(&[("search_news", json!({})), ("finish", json!({"result":"sent"}))]))]);
    let loop_ = h.start().await;
    assert_eq!(loop_.send("go".into()).await.unwrap(), "sent");
    assert_eq!(h.model.requests.lock().unwrap().len(), 1);
    assert_eq!(h.mcp.calls.lock().unwrap().iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(), ["search_news"]);
    // Every call of the round is answered, so the transcript stays valid.
    let messages = loop_.messages.lock().await;
    assert_eq!(messages.iter().filter(|m| m["role"] == "tool").count(), 2);
    drop(messages);
    h.engine.shutdown().await;
}

#[tokio::test]
async fn finish_alone_or_without_a_result_ends_with_an_empty_answer() {
    let h = harness(vec![turn(|_| call("finish", json!({})))]);
    assert_eq!(h.start().await.send("go".into()).await.unwrap(), "");
    h.engine.shutdown().await;
}

#[tokio::test]
async fn a_failed_call_in_the_round_keeps_the_session_going() {
    let h = harness(vec![
        turn(|_| calls(&[("search_news", json!({})), ("finish", json!({"result":"sent"}))])),
        turn(|p| {
            assert!(mentions(&p.messages, "[finish error] not finished: search_news failed"));
            answer("recovered")
        }),
    ]);
    *h.mcp.handler.lock().unwrap() = Arc::new(|_, _, _| Box::pin(async { Err(anyhow::anyhow!("down")) }));
    assert_eq!(h.start().await.send("go".into()).await.unwrap(), "recovered");
    h.engine.shutdown().await;
}

#[tokio::test]
async fn two_finishes_in_one_round_end_nothing() {
    let h = harness(vec![
        turn(|_| calls(&[("finish", json!({"result":"a"})), ("finish", json!({"result":"b"}))])),
        turn(|p| {
            let replies = p.messages.iter().filter(|m| m["role"] == "tool").count();
            assert_eq!(replies, 2);
            assert!(mentions(&p.messages, "[finish error] call finish once per round"));
            answer("one at a time")
        }),
    ]);
    assert_eq!(h.start().await.send("go".into()).await.unwrap(), "one at a time");
    h.engine.shutdown().await;
}

#[tokio::test]
async fn an_invalid_finish_is_an_error_the_model_reads() {
    let h = harness(vec![
        turn(|_| call("finish", json!({"result": 5}))),
        turn(|p| {
            assert!(mentions(&p.messages, "[finish error]"));
            answer("fixed")
        }),
    ]);
    assert_eq!(h.start().await.send("go".into()).await.unwrap(), "fixed");
    h.engine.shutdown().await;
}

#[tokio::test]
async fn a_worker_that_finishes_hands_its_result_to_the_parent() {
    let h = harness(vec![
        turn(|_| call("invoke_sub_agent", json!({"skills":["test"],"prompt":"do it"}))),
        turn(|p| {
            assert!(p.tools.iter().any(|t| t.name == "finish"), "workers may finish too");
            call("finish", json!({"result":"worker result"}))
        }),
        turn(|p| {
            assert_eq!(last_json(&p.messages)["content"], "worker result");
            answer("done")
        }),
    ]);
    assert_eq!(h.start().await.send("go".into()).await.unwrap(), "done");
    h.engine.shutdown().await;
}
