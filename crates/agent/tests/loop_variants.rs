// Loop variants (design 04, R-M9): an agent's own loops, chosen by a worker
// (the YAML's `start_worker { loop }`, the call's `loop`, a skill's
// preference) or started as a route; their breaker; the reference channel
// loop (tests/fixtures/telegram-loop.yaml) and the loop's generic outcomes.
// Restored from tests/pending on the engine: a loop runs as an engine run
// (`AgentLoop::send`), its version chosen at its start (runs.rs §10).
mod support;
use agent::{
    agent::{AGENT_LOOP, ConfirmTarget, Engine, LoopOptions},
    db::TraceStore,
    flows,
    providers::assistant,
};
use ouro_runtime::durable::SavedRun;
use serde_json::{Value, json};
use std::sync::Arc;
use support::*;

const WORKER: &[(&str, &str)] = &[("worker", "---\ntools: *\n---\nworker")];

async fn activate(f: &Fixture, name: &str, definition: &str) -> u64 {
    activate_definition(&f.engine, name, definition).await
}

// A loop of `name` the host runs on "reply to me": its answer, or its fault.
async fn run(f: &Fixture, id: &str, name: &str, conversation: bool) -> anyhow::Result<String> {
    f.engine.ready().await.unwrap();
    if name == "telegram-loop" && f.engine.runtime.definitions(name).await.unwrap().active.is_none() {
        activate_telegram_loop(&f.engine).await;
    }
    let mut opts = LoopOptions::new(id, context());
    opts.workflow = name.into();
    opts.guard_request = Some("Send to chat 42 and chat 43".into());
    opts.skills = vec!["worker".into()];
    if conversation {
        opts.conversation = Some(ConfirmTarget { chat_id: "42".into(), message_thread_id: Some(7) });
        opts.confirm = opts.conversation.clone();
    }
    let result = f.engine.start_loop(opts).await.unwrap().send("reply to me".into()).await;
    f.engine.end_loop(id).await;
    result
}

fn sent(chat: &str, thread: Option<i64>, text: &str) -> Value {
    json!({"chatId":chat,"messageThreadId":thread,"text":text,"output":"{\"delivered\":true}"})
}

// The worker the loop `parent` started in round `i`, as the runtime reads it.
async fn worker(engine: &Engine, parent: &str, i: u64) -> SavedRun {
    let filter = agent::runs::Runs {
        parent: Some(parent.into()),
        since_ms: Some(0),
        before_ms: Some(i64::MAX / 4),
        ..Default::default()
    };
    let ended = engine.control.history(&filter, None, 50).await.unwrap().items;
    let live = agent::runs::Runs { parent: Some(parent.into()), ..Default::default() };
    let listed = engine.control.list(&live, None, 50).await.unwrap().items;
    let key = format!("/{}", agent::agent::worker_key(i, 0));
    let found = ended.into_iter().chain(listed).find(|r| r.child.as_ref().is_some_and(|l| l.key.ends_with(&key)));
    engine.runtime.get(&found.expect("the worker").admission.id).await.unwrap().unwrap()
}

// agent-loop with its YAML choosing the workers' loop.
fn choosing(name: &str) -> String {
    let parent = without_breaker(AGENT_LOOP).replace(
        "call: start_worker\n                    with: { call:",
        &format!("call: start_worker\n                    with: {{ loop: {name}, call:"),
    );
    assert_ne!(parent, without_breaker(AGENT_LOOP));
    parent
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reply_ends_the_telegram_variant_in_one_turn_and_is_the_trace_output() {
    let model =
        Arc::new(Scripted::new(vec![calls(&[("send_telegram_message", sent("42", Some(7), "delivered reply"))])]));
    let f = fixture(model.clone(), &["send_telegram_message"], WORKER);
    activate_telegram_loop(&f.engine).await;
    let mut opts = LoopOptions::new("reply", context());
    opts.workflow = "telegram-loop".into();
    opts.skills = vec!["worker".into()];
    opts.conversation = Some(ConfirmTarget { chat_id: "42".into(), message_thread_id: Some(7) });
    let live = f.engine.start_loop(opts).await.unwrap();
    assert_eq!(live.send("Reply in chat 42".into()).await.unwrap(), "delivered reply");
    assert_eq!(model.seen.lock().unwrap().len(), 1);
    let (trace, _) = f.db.get_trace("reply").unwrap().unwrap();
    assert_eq!(trace.output, json!("delivered reply"));
}

// The rule decides on what the call asked for and whether it succeeded: the
// reply is never read, so a dry run's mock ends the loop as a real send does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_successful_sends_to_the_exact_conversation_count() {
    for (name, args, delivered) in [
        ("send_telegram_message", sent("43", Some(7), "wrong chat"), false),
        (
            "send_telegram_message",
            json!({"chatId":43,"messageThreadId":7,"text":"wrong numeric chat","output":"{\"delivered\":true}"}),
            false,
        ),
        ("send_telegram_message", sent("42", None, "wrong topic"), false),
        ("telegram_send_status", sent("42", Some(7), "progress"), false),
        (
            "send_telegram_message",
            json!({"chatId":"42","messageThreadId":7,"text":"failed","output":"[tool error] no delivery"}),
            false,
        ),
        (
            "send_telegram_message",
            json!({"chatId":"43","messageThreadId":7,"text":"elsewhere","output":json!({"delivered":true,"chatId":42,"messageThreadId":7}).to_string()}),
            false,
        ),
        ("send_telegram_message", sent("42", Some(7), "reply"), true),
        (
            "send_telegram_message",
            json!({"chatId":42,"messageThreadId":7,"text":"reply","output":"{\"delivered\":true}"}),
            true,
        ),
        (
            "send_telegram_message",
            json!({"chatId":"42","messageThreadId":7,"text":"reply","output":json!({"delivered":false,"chatId":43,"messageThreadId":null}).to_string()}),
            true,
        ),
        ("send_telegram_message", json!({"chatId":"42","messageThreadId":7,"text":"reply","output":MOCK}), true),
    ] {
        let model = Arc::new(Scripted::new(vec![
            calls(&[(name, args)]),
            assistant("ending"),
            calls(&[("finish", json!({"result":"done"}))]),
        ]));
        let f = fixture(model.clone(), &["send_telegram_message", "telegram_send_status"], WORKER);
        let result = run(&f, "conversation", "telegram-loop", true).await;
        let expected = if delivered { "reply" } else { "done" };
        assert!(matches!(result, Ok(ref output) if output == expected), "{name}: {result:?}");
        let seen = model.seen.lock().unwrap();
        assert_eq!(seen.len(), if delivered { 1 } else { 3 });
        if !delivered {
            assert_eq!(
                seen[2]
                    .messages
                    .iter()
                    .filter(|m| m["role"] == "user"
                        && m["content"].as_str().unwrap_or_default().contains("Nothing was sent"))
                    .count(),
                1
            );
        }
    }
}

const MOCK: &str = agent::sandbox::MOCK_REPLY;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_guard_blocked_send_does_not_end_the_loop() {
    let args = sent("42", Some(7), "sk-abcdefghijklmnopqrstuvwxyz123456");
    let model = Arc::new(Scripted::new(vec![
        calls(&[("send_telegram_message", args)]),
        assistant("ending"),
        assistant("done"),
    ]));
    let f = fixture(model.clone(), &["send_telegram_message"], WORKER);
    assert!(matches!(run(&f, "blocked", "telegram-loop", true).await, Ok(ref output) if output == "done"));
    assert!(f.mcp.called.lock().unwrap().is_empty());
    assert_eq!(model.seen.lock().unwrap().len(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_silent_end_nudges_once_for_text_or_finish_and_workers_have_no_conversation() {
    for first in [assistant("silent"), calls(&[("finish", json!({"result":"silent"}))])] {
        let model =
            Arc::new(Scripted::new(vec![first.clone(), calls(&[("finish", json!({"result":"no reply needed"}))])]));
        let f = fixture(model.clone(), &[], WORKER);
        assert!(matches!(run(&f, "nudge", "telegram-loop", true).await, Ok(ref v) if v == "no reply needed"));
        {
            let seen = model.seen.lock().unwrap();
            assert_eq!(seen.len(), 2);
            assert_eq!(seen[1].messages.last().unwrap()["role"], "user");
            assert!(seen[1].messages.last().unwrap()["content"].as_str().unwrap().contains("Nothing was sent"));
        }
        let model = Arc::new(Scripted::new(vec![first]));
        let f = fixture(model.clone(), &[], WORKER);
        assert!(matches!(run(&f, "worker", "telegram-loop", false).await, Ok(ref v) if v == "silent"));
        assert_eq!(model.seen.lock().unwrap().len(), 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_named_loop_requires_the_guard_at_save_and_activation() {
    let f = fixture(Arc::new(Scripted::new(vec![])), &[], WORKER);
    f.engine.ready().await.unwrap();
    for name in ["telegram-loop", "research-loop"] {
        let def = format!(
            "document: {{dsl: '1.0.4', namespace: t, name: {name}, version: '1.0.0', metadata: {{loop: {{description: Research}}}}}}\ndo: [{{turn: {{call: agent_turn, with: {{messages: '${{ .messages }}'}}}}}}]\n"
        );
        let save = json!({"name":name,"definition":def});
        assert!(
            flows::tool(&f.engine, "save_workflow", save.as_object().unwrap())
                .await
                .unwrap_err()
                .to_string()
                .contains("requires a `call: start_guard`")
        );
        let version = f.engine.runtime.save(name, &def).await.unwrap().version;
        let digest = ouro_runtime::durable::digest_of(&def);
        flows::WorkflowMeta::record(&f.engine.deps.db, name, &digest, None, false).unwrap();
        mark_tried(&f.engine, name, &def);
        let args = json!({"name":name,"version":version});
        assert!(
            flows::tool(&f.engine, "activate_workflow", args.as_object().unwrap())
                .await
                .unwrap_err()
                .to_string()
                .contains("requires a `call: start_guard`")
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_named_variants_breaker_falls_back_after_three_faults_and_keeps_others_closed() {
    let model = Arc::new(Scripted::new(vec![assistant("fallback"), assistant("telegram fallback")]));
    let f = fixture(model, &[], WORKER);
    f.engine.ready().await.unwrap();
    for name in ["research-loop", "telegram-loop"] {
        let broken = faulting(&variant(name));
        activate(&f, name, &broken).await;
        for n in 0..3 {
            assert!(run(&f, &format!("{name}-{n}"), name, false).await.is_err());
        }
        assert!(run(&f, &format!("{name}-fallback"), name, false).await.is_ok());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workers_choose_tool_or_skill_loop_and_catalog_lists_active_versions() {
    for explicit in [false, true] {
        let args = if explicit {
            json!({"prompt":"research","skills":["research"],"loop":"explicit-loop"})
        } else {
            json!({"prompt":"research","skills":["research"]})
        };
        let model = Arc::new(Scripted::new(vec![
            calls(&[("invoke_sub_agent", args)]),
            assistant("worker answer"),
            assistant("parent answer"),
        ]));
        let f = fixture(
            model.clone(),
            &[],
            &[("worker", "---\ntools: *\n---\nworker"), ("research", "---\ntools: *\nloop: skill-loop\n---\nresearch")],
        );
        f.engine.ready().await.unwrap();
        let catalog = flows::loop_catalog(&f.engine).await.unwrap();
        assert!(catalog.contains("agent-loop:"));
        assert!(!catalog.contains("telegram-loop:"));
        activate_telegram_loop(&f.engine).await;
        for name in ["explicit-loop", "skill-loop"] {
            let def = variant(name).replace("output: .content", &format!("output: (\"{name}: \" + .content)"));
            activate(&f, name, &def).await;
        }
        // A new draft must not hide an active version from the catalog.
        let draft = variant("skill-loop").replace("General ReAct loop", "A draft loop");
        let args = json!({"name":"skill-loop","definition":draft});
        flows::tool(&f.engine, "save_workflow", args.as_object().unwrap()).await.unwrap();
        assert!(matches!(run(&f, "parent", "agent-loop", false).await, Ok(ref v) if v == "parent answer"));
        let child = worker(&f.engine, "parent", 1).await;
        let expected = if explicit { "explicit-loop" } else { "skill-loop" };
        assert_eq!(child.admission.name, expected);
        assert_eq!(child.outcome, Some(agent::runs::Status::Completed(json!(format!("{expected}: worker answer")))));
        assert!(child.admission.input["conversation"].is_null(), "workers have no conversation");
        let seen = model.seen.lock().unwrap();
        let description =
            seen[0].tools.iter().find(|t| t.name == "invoke_sub_agent").unwrap().description.as_ref().unwrap();
        for name in ["agent-loop:", "telegram-loop:", "explicit-loop:", "skill-loop:"] {
            assert!(description.contains(name), "{description}");
        }
        assert!(!description.contains("A draft loop"), "{description}");
    }
}

fn route_definition(name: &str, when: &str, priority: Option<i64>) -> String {
    let priority = priority.map(|n| format!(", priority: {n}")).unwrap_or_default();
    format!(
        "document: {{dsl: '1.0.4', namespace: t, name: {name}, version: '1.0.0', metadata: {{route: {{when: {{{when}}}{priority}}}}}}}\ndo: [{{done: {{set: {{output: routed}}}}}}]\noutput: {{as: '${{ .output }}'}}\n"
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn routing_uses_source_payload_specificity_priority_and_active_overrides() {
    let f = fixture(Arc::new(Scripted::new(vec![])), &[], WORKER);
    f.engine.ready().await.unwrap();
    let pick = async |source: &str, payload: &Value| {
        flows::signal_routes(&f.engine, source, payload).await.unwrap().into_iter().next().map(|r| r.0)
    };
    // The built-in Telegram route (design 08) at priority -1: an agent's
    // route of the same specificity wins without a tie.
    assert_eq!(pick("telegram", &Value::Null).await.as_deref(), Some("telegram"));
    assert_eq!(pick("telegram_button", &Value::Null).await.as_deref(), Some("telegram"));
    assert!(flows::built_in("telegram-loop").is_none());
    activate_telegram_loop(&f.engine).await;
    assert_eq!(pick("telegram", &Value::Null).await.as_deref(), Some("telegram-loop"));
    assert_eq!(pick("telegram_button", &Value::Null).await.as_deref(), Some("telegram-loop"));
    assert_eq!(pick("peer", &Value::Null).await, None);
    assert_eq!(pick("scheduler", &Value::Null).await, None);
    let specific = route_definition("topic-route", "source: [telegram, telegram_button], topic: research", None);
    activate(&f, "topic-route", &specific).await;
    assert_eq!(pick("telegram", &json!({"topic":"research"})).await.as_deref(), Some("topic-route"));
    let tie = route_definition("tied-route", "source: telegram", None);
    let args = json!({"name":"tied-route","definition":tie});
    let saved = flows::tool(&f.engine, "save_workflow", args.as_object().unwrap()).await.unwrap();
    let version = serde_json::from_str::<Value>(&saved).unwrap()["version"].clone();
    mark_tried(&f.engine, "tied-route", &tie);
    let args = json!({"name":"tied-route","version":version});
    let error = flows::tool(&f.engine, "activate_workflow", args.as_object().unwrap()).await.unwrap_err().to_string();
    // A route workflow and a loop route conflict by the same rules.
    assert!(error.contains("telegram-loop") && error.contains("priority"), "{error}");
    activate(&f, "tied-route", &route_definition("tied-route", "source: telegram", Some(1))).await;
    assert_eq!(pick("telegram", &Value::Null).await.as_deref(), Some("tied-route"));
    assert_eq!(pick("telegram", &json!({"topic":"research"})).await.as_deref(), Some("topic-route"));
    let moved = TELEGRAM_LOOP.replace("source: [telegram, telegram_button]", "source: [peer, scheduler]");
    activate(&f, "telegram-loop", &moved).await;
    assert_eq!(pick("peer", &Value::Null).await.as_deref(), Some("telegram-loop"));
    assert_eq!(pick("telegram", &Value::Null).await.as_deref(), Some("tied-route"));
}

// A worker's loop chosen in the YAML; a run admitted at a version keeps it
// when another is activated.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_start_child_can_choose_a_loop_and_admitted_versions_stay_pinned() {
    let release = Arc::new(tokio::sync::Notify::new());
    let reached = Arc::new(tokio::sync::Notify::new());
    let (wait, hit) = (release.clone(), reached.clone());
    let turns = Turns::new(vec![
        turn(|_| call("invoke_sub_agent", json!({"prompt":"research"}))),
        turn(|_| answer("worker result")),
        turn(|_| answer("parent result")),
        turn_async(move |_| async move {
            hit.notify_one();
            wait.notified().await;
            Ok(answer("pinned result"))
        }),
    ]);
    let f = fixture(turns, &[], WORKER);
    f.engine.ready().await.unwrap();
    let old = variant("research-loop").replace("output: .content", "output: (\"old: \" + .content)");
    let version = activate(&f, "research-loop", &old).await;
    activate(&f, "agent-loop", &choosing("research-loop")).await;
    assert!(run(&f, "named-child", "agent-loop", false).await.is_ok());
    let child = worker(&f.engine, "named-child", 1).await;
    assert_eq!(child.admission.name, "research-loop");
    assert_eq!(child.outcome, Some(agent::runs::Status::Completed(json!("old: worker result"))));

    // Admitted at `version`; a newer version activated meanwhile is not its.
    let pinned = {
        let engine = f.engine.clone();
        tokio::spawn(async move {
            let mut opts = LoopOptions::new("pinned", context());
            opts.workflow = "research-loop".into();
            opts.skills = vec!["worker".into()];
            engine.start_loop(opts).await.unwrap().send("go".into()).await
        })
    };
    reached.notified().await;
    activate(&f, "research-loop", &variant("research-loop")).await;
    release.notify_one();
    assert_eq!(pinned.await.unwrap().unwrap(), "old: pinned result");
    let saved = f.engine.runtime.get("pinned").await.unwrap().unwrap();
    assert_eq!((saved.admission.version, saved.admission.definition.as_str()), (version, old.as_str()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_silent_end_nudge_gets_one_turn_beyond_the_iteration_limit() {
    let model = Arc::new(Scripted::new(vec![
        assistant("nothing yet"),
        calls(&[("finish", json!({"result":"no reply needed"}))]),
    ]));
    let f = fixture(model.clone(), &[], WORKER);
    activate_telegram_loop(&f.engine).await;
    let mut opts = LoopOptions::new("limit", context());
    opts.workflow = "telegram-loop".into();
    opts.skills = vec!["worker".into()];
    opts.max_iterations = 1;
    opts.conversation = Some(ConfirmTarget { chat_id: "42".into(), message_thread_id: None });
    let live = f.engine.start_loop(opts).await.unwrap();
    assert_eq!(live.send("Nothing to do".into()).await.unwrap(), "no reply needed");
    assert_eq!(model.seen.lock().unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_in_process_worker_start_child_respects_the_yaml_loop_selection() {
    let model = Arc::new(Scripted::new(vec![
        calls(&[("invoke_sub_agent", json!({"prompt":"research"}))]),
        assistant("worker answer"),
        assistant("done"),
    ]));
    let f = fixture(model.clone(), &[], WORKER);
    f.engine.ready().await.unwrap();
    let research = variant("research-loop").replace("output: .content", "output: (\"research: \" + .content)");
    activate(&f, "research-loop", &research).await;
    activate(&f, "agent-loop", &choosing("research-loop")).await;
    let mut opts = LoopOptions::new("ephemeral", context());
    opts.skills = vec!["worker".into()];
    let live = f.engine.start_loop(opts).await.unwrap();
    assert_eq!(live.send("research".into()).await.unwrap(), "done");
    let seen = model.seen.lock().unwrap();
    assert!(seen[2].messages.last().unwrap()["content"].as_str().unwrap().contains("research: worker answer"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_explicit_base_loop_in_yaml_overrides_a_skills_variant_preference() {
    for durable in [false, true] {
        let model = Arc::new(Scripted::new(vec![
            calls(&[("invoke_sub_agent", json!({"prompt":"research","skills":["research"]}))]),
            assistant("worker answer"),
            assistant("done"),
        ]));
        let f = fixture(
            model.clone(),
            &[],
            &[
                ("worker", "---\ntools: *\n---\nworker"),
                ("research", "---\ntools: *\nloop: research-loop\n---\nresearch"),
            ],
        );
        f.engine.ready().await.unwrap();
        let research = variant("research-loop").replace("output: .content", "output: (\"research: \" + .content)");
        activate(&f, "research-loop", &research).await;
        activate(&f, "agent-loop", &choosing("agent-loop")).await;
        if durable {
            assert!(matches!(run(&f, "explicit-base", "agent-loop", false).await, Ok(ref v) if v == "done"));
            assert_eq!(worker(&f.engine, "explicit-base", 1).await.admission.name, "agent-loop");
        } else {
            let mut opts = LoopOptions::new("explicit-base", context());
            opts.skills = vec!["worker".into()];
            assert_eq!(f.engine.start_loop(opts).await.unwrap().send("research".into()).await.unwrap(), "done");
        }
        let seen = model.seen.lock().unwrap();
        let reply = seen[2].messages.last().unwrap()["content"].as_str().unwrap();
        assert!(reply.contains("worker answer") && !reply.contains("research: worker answer"), "{reply}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settle_exposes_ordered_generic_outcomes_before_model_truncation() {
    let f = fixture(Arc::new(Scripted::new(vec![])), &["t_ok", "t_err", "t_read"], WORKER);
    let mut opts = LoopOptions::new("outcomes", context());
    opts.skills = vec!["worker".into()];
    let live = f.engine.start_loop(opts).await.unwrap();
    live.admit_task("inspect tool facts").await;
    let full = json!({"answer": "evidence".repeat(1500)});
    let huge = "🦀".repeat(40_000);
    let calls = vec![
        json!({"id":"json","name":"t_read","arguments":json!({"output":full.to_string()}).to_string()}),
        json!({"id":"text","name":"t_read","arguments":json!({"output":huge}).to_string()}),
        json!({"id":"error","name":"t_err","arguments":"{}"}),
        json!({"id":"invalid","name":"t_ok","arguments":"not JSON"}),
        json!({"id":"unknown","name":"t_ok","arguments":"{}"}),
    ];
    // t_read uses the same injected MCP as any other tool, with no channel contract.
    let mut results = Vec::new();
    for call in &calls[..4] {
        let result =
            support::loop_tool(&f.engine, &live, &json!({"call":call}), "outcomes", "invocation", true).await.unwrap();
        results.push(result);
    }
    assert!(results[0]["presented"].as_str().unwrap().contains("\"truncated\":true"));
    // A round admitted before outcomes existed only has the full stored reply.
    results[1].as_object_mut().unwrap().remove("result");
    results.push(agent::agent::unknown_tool_outcome("t_ok"));
    let settled = agent::agent::loop_settle(&f.engine, &live, "outcomes", &json!({"calls":calls,"results":results}))
        .await
        .unwrap();
    assert!(settled.get("replied").is_none() && settled.get("reply").is_none());
    let outcomes = settled["outcomes"].as_array().unwrap();
    assert_eq!(outcomes.len(), calls.len());
    assert_eq!(outcomes[0], json!({"name":"t_read","arguments":{"output":full.to_string()},"ok":true,"result":full}));
    assert_eq!(outcomes[1]["ok"], true);
    let clipped = outcomes[1]["result"].as_str().unwrap();
    assert!(clipped.starts_with('🦀') && clipped.contains("[clipped, 80000 chars]"));
    assert!(clipped.encode_utf16().count() < 66_000);
    for (outcome, name) in outcomes[2..].iter().zip(["t_err", "t_ok", "t_ok"]) {
        assert_eq!(outcome["name"], name);
        assert_eq!(outcome["ok"], false);
        assert!(outcome["result"].as_str().unwrap().starts_with("[tool error]"));
    }
    assert_eq!(outcomes[3]["arguments"], "not JSON");
    assert!(outcomes[4]["result"].as_str().unwrap().contains("outcome unknown"));
    f.engine.end_loop("outcomes").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workflow_authors_use_maximum_effort_for_every_requested_worker_preset() {
    // Every loop is an engine run: the parked suite's in-process and durable
    // paths are one.
    for preset in ["instant", "base", "smart", "smartest"] {
        let model = Arc::new(Scripted::new(vec![
            calls(&[("invoke_sub_agent", json!({"skills":["workflow-author"],"preset":preset,"prompt":"draft"}))]),
            assistant("draft"),
            assistant("done"),
        ]));
        let f = fixture(
            model.clone(),
            &[],
            &[("worker", "---\ntools: *\n---\nworker"), ("workflow-author", "---\ntools: []\n---\nauthor")],
        );
        let mut opts = LoopOptions::new("author", context());
        opts.skills = vec!["worker".into()];
        assert_eq!(f.engine.start_loop(opts).await.unwrap().send("draft".into()).await.unwrap(), "done");
        let seen = model.seen.lock().unwrap();
        assert_eq!(seen[1].model, agent::models::default_presets().smartest.model, "{preset}");
        assert_eq!(seen[1].reasoning_effort, agent::models::ReasoningEffort::Max, "{preset}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn another_channels_loop_can_exit_from_generic_outcomes_without_host_changes() {
    let model = Arc::new(Scripted::new(vec![calls(&[(
        "reply_to_peer",
        json!({"threadId":9,"text":"peer answer","output":"{\"sent\":true}"}),
    )])]));
    let f = fixture(model.clone(), &["reply_to_peer"], WORKER);
    f.engine.ready().await.unwrap();
    let definition = variant("peer-loop")
        .replace("        - finished:", "        - sent: { when: '${ any(.outcomes[]; .ok and .name == \"reply_to_peer\" and .result.sent == true) }', then: peer_reply }\n        - finished:")
        .replace("  - answer:", "  - peer_reply:\n      set: '${ { output: ([ .outcomes[] | select(.ok and .name == \"reply_to_peer\") | .arguments.text ] | last) } }'\n      then: end\n  - answer:");
    assert_ne!(definition, variant("peer-loop"));
    activate(&f, "peer-loop", &definition).await;
    assert!(matches!(run(&f, "peer", "peer-loop", false).await, Ok(ref v) if v == "peer answer"));
    assert_eq!(model.seen.lock().unwrap().len(), 1);
}

// Fixed steps of a loop are the author's, trusted as a saved workflow's
// calls: a direct write runs without a guard review and, recorded, is not
// sent again when the run resumes. The model's calls still go through the
// guarded round.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_loops_own_write_runs_unreviewed_and_once_while_the_models_calls_stay_guarded() {
    let (started, has_started) = tokio::sync::oneshot::channel::<()>();
    let turns = Turns::new(vec![
        turn_async(move |_| async move {
            started.send(()).unwrap();
            std::future::pending::<anyhow::Result<agent::providers::Message>>().await
        }),
        turn(|_| calls(&[("send_telegram_message", json!({"chatId":"43","text":"elsewhere"}))])),
        turn(|_| assistant("done")),
    ]);
    let api = Arc::new(GuardApi::default());
    *api.label.lock().unwrap() = Some("misaligned".into());
    let decisions = agent::decisions::decision_client("guard-test".into(), api.clone());
    let f = fixture_on(turns.clone(), &["send_telegram_message"], WORKER, |deps| {
        deps.decisions = Some(decisions.clone());
    });
    f.engine.ready().await.unwrap();
    let direct = variant("direct-loop").replacen(
        "do:\n",
        "do:\n  - note:\n      call: send_telegram_message\n      with: { chatId: '42', text: fixed step }\n",
        1,
    );
    activate(&f, "direct-loop", &direct).await;
    let running = {
        let engine = f.engine.clone();
        tokio::spawn(async move {
            let mut opts = LoopOptions::new("direct", context());
            opts.workflow = "direct-loop".into();
            opts.skills = vec!["worker".into()];
            engine.start_loop(opts).await.unwrap().send("go".into()).await
        })
    };
    has_started.await.unwrap();
    // A restart: this process stops in the first turn, the next resumes the run.
    f.engine.halt().await;
    running.abort();
    let resumed = reopen(&f, turns.clone(), Arc::new(ouro_runtime::durable::SystemClock), |deps| {
        deps.decisions = Some(decisions.clone());
    });
    resumed.ready().await.unwrap();
    let ended = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        agent::runs::wait_end(&resumed, "direct", &tokio_util::sync::CancellationToken::new()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(ended, agent::runs::Status::Completed(json!("done")));
    let sent: Vec<_> =
        f.mcp.called.lock().unwrap().iter().filter(|(n, _)| n == "send_telegram_message").cloned().collect();
    assert_eq!(sent.len(), 1, "the direct write once, the model's blocked: {sent:?}");
    assert_eq!(sent[0].1["text"], "fixed step");
    // The guard blocked the model's send to another chat; nothing reviewed the direct one.
    assert!(!api.requests.lock().unwrap().iter().any(|r| r.to_string().contains("fixed step")));
    let last = turns.requests.lock().unwrap().last().unwrap().clone();
    assert!(serde_json::to_string(&last).unwrap().contains("[guard] blocked"), "{last:?}");
    resumed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn loop_setup_runs_once_and_only_before_the_first_turn() {
    for (name, step, at) in [
        ("late-loop", "  - late: { call: loop_setup, with: { system: [late] } }\n  - route:\n", "  - route:\n"),
        (
            "twice-loop",
            "do:\n  - one: { call: loop_setup, with: { system: [one] } }\n  - two: { call: loop_setup, with: { system: [two] } }\n",
            "do:\n",
        ),
    ] {
        let model = Arc::new(Scripted::new(vec![assistant("answer")]));
        let f = fixture(model, &[], WORKER);
        f.engine.ready().await.unwrap();
        activate(&f, name, &variant(name).replacen(at, step, 1)).await;
        let error = run(&f, name, name, false).await.expect_err("it ran").to_string();
        let expected = if name == "late-loop" { "before the loop's first agent_turn" } else { "runs once per loop" };
        assert!(error.contains(expected), "{name}: {error}");
    }
}

// The same loop a route starts runs as a worker: no signal, so its context
// steps are skipped and it opens no status.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_running_the_routed_loop_skips_its_context() {
    let model = Arc::new(Scripted::new(vec![
        calls(&[("invoke_sub_agent", json!({"prompt":"research","loop":"telegram-loop"}))]),
        assistant("worker answer"),
        assistant("parent answer"),
    ]));
    let f = fixture(
        model.clone(),
        &[],
        &[("worker", "---\ntools: *\n---\nworker"), ("telegram", "---\ntools: *\n---\nTELEGRAM")],
    );
    activate_telegram_loop(&f.engine).await;
    assert!(matches!(run(&f, "parent", "agent-loop", false).await, Ok(ref v) if v == "parent answer"));
    let child = worker(&f.engine, "parent", 1).await;
    assert_eq!(child.admission.name, "telegram-loop");
    assert!(child.admission.input.get("signal").is_none());
    assert_eq!(child.outcome, Some(agent::runs::Status::Completed(json!("worker answer"))));
    assert!(f.mcp.called.lock().unwrap().is_empty(), "no context reads, no status");
    let seen = model.seen.lock().unwrap();
    assert!(!seen[1].messages[0]["content"].as_str().unwrap().contains("TELEGRAM"));
}
