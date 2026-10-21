mod support;
use agent::{
    agent::{AGENT_LOOP, ConfirmTarget, LoopOptions},
    db::TraceStore,
    flows::{self, FlowResult, FlowRun},
    providers::assistant,
    tracing::null_scope,
};
use serde_json::{Value, json};
use std::sync::Arc;
use support::*;
use tokio_util::sync::CancellationToken;

fn activate(f: &Fixture, name: &str, definition: &str) -> i64 {
    let saved = f.engine.deps.workflows.save(name, definition, None).unwrap();
    let args = json!({"name":name,"version":saved.version});
    flows::tool(&f.engine, "activate_workflow", args.as_object().unwrap()).unwrap();
    saved.version
}
fn variant(name: &str) -> String {
    AGENT_LOOP.replace("name: agent-loop,", &format!("name: {name},"))
}
async fn run(f: &Fixture, id: &str, name: &str, conversation: bool) -> FlowResult {
    let mut opts = LoopOptions::new(id, context());
    opts.workflow = name.into();
    opts.guard_request = Some("Send to chat 42 and chat 43".into());
    opts.skills = vec!["worker".into()];
    if conversation {
        opts.conversation = Some(ConfirmTarget { chat_id: "42".into(), message_thread_id: Some(7) });
        opts.confirm = opts.conversation.clone();
    }
    let input = f.engine.start_loop(opts).await.unwrap().workflow_input("reply to me").await;
    let result = flows::create_durable_flows(f.engine.clone(), (*f.db).clone())
        .run_loop(&format!("{id}/loop"), input, &null_scope(), &CancellationToken::new())
        .await;
    f.engine.end_loop(id).await;
    result
}
const WORKER: &[(&str, &str)] = &[("worker", "---\ntools: *\n---\nworker")];
fn sent(chat: &str, thread: Option<i64>, text: &str) -> Value {
    json!({"chatId":chat,"messageThreadId":thread,"text":text,"output":"{\"delivered\":true}"})
}

#[tokio::test]
async fn a_reply_ends_the_telegram_variant_in_one_turn_and_is_the_trace_output() {
    let model =
        Arc::new(Scripted::new(vec![calls(&[("send_telegram_message", sent("42", Some(7), "delivered reply"))])]));
    let f = fixture(model.clone(), &["send_telegram_message"], WORKER);
    let mut opts = LoopOptions::new("reply", context());
    opts.workflow = "agent-loop-telegram".into();
    opts.skills = vec!["worker".into()];
    opts.conversation = Some(ConfirmTarget { chat_id: "42".into(), message_thread_id: Some(7) });
    let live = f.engine.start_loop(opts).await.unwrap();
    assert_eq!(live.send("Reply in chat 42".into()).await.unwrap(), "delivered reply");
    assert_eq!(model.seen.lock().unwrap().len(), 1);
    let (trace, _) = f.db.get_trace("reply").unwrap().unwrap();
    assert_eq!(trace.output, json!("delivered reply"));
}

#[tokio::test]
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
            json!({"chatId":"42","messageThreadId":7,"text":"unconfirmed result","output":"{\"delivered\":false}"}),
            false,
        ),
        ("send_telegram_message", sent("42", Some(7), "reply"), true),
    ] {
        let model = Arc::new(Scripted::new(vec![
            calls(&[(name, args)]),
            assistant("ending"),
            calls(&[("finish", json!({"result":"done"}))]),
        ]));
        let f = fixture(model.clone(), &["send_telegram_message", "telegram_send_status"], WORKER);
        let result = run(&f, "conversation", "agent-loop-telegram", true).await;
        let expected = if delivered { "reply" } else { "done" };
        assert!(matches!(result, FlowResult::Completed(ref output) if output == expected), "{name}: {result:?}");
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

#[tokio::test]
async fn a_guard_blocked_send_does_not_end_the_loop() {
    let args = sent("42", Some(7), "sk-abcdefghijklmnopqrstuvwxyz123456");
    let model = Arc::new(Scripted::new(vec![
        calls(&[("send_telegram_message", args)]),
        assistant("ending"),
        assistant("done"),
    ]));
    let f = fixture(model.clone(), &["send_telegram_message"], WORKER);
    assert!(
        matches!(run(&f,"blocked","agent-loop-telegram",true).await, FlowResult::Completed(ref output) if output == "done")
    );
    assert!(f.mcp.called.lock().unwrap().is_empty());
    assert_eq!(model.seen.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn a_silent_end_nudges_once_for_text_or_finish_and_workers_have_no_conversation() {
    for first in [assistant("silent"), calls(&[("finish", json!({"result":"silent"}))])] {
        let model =
            Arc::new(Scripted::new(vec![first.clone(), calls(&[("finish", json!({"result":"no reply needed"}))])]));
        let f = fixture(model.clone(), &[], WORKER);
        assert!(
            matches!(run(&f,"nudge","agent-loop-telegram",true).await,FlowResult::Completed(ref v) if v=="no reply needed")
        );
        {
            let seen = model.seen.lock().unwrap();
            assert_eq!(seen.len(), 2);
            assert_eq!(seen[1].messages.last().unwrap()["role"], "user");
            assert!(seen[1].messages.last().unwrap()["content"].as_str().unwrap().contains("Nothing was sent"));
        }
        let model = Arc::new(Scripted::new(vec![first]));
        let f = fixture(model.clone(), &[], WORKER);
        assert!(
            matches!(run(&f,"worker","agent-loop-telegram",false).await,FlowResult::Completed(ref v) if v=="silent")
        );
        assert_eq!(model.seen.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn every_named_loop_requires_the_guard_at_save_and_activation() {
    let f = fixture(Arc::new(Scripted::new(vec![])), &[], WORKER);
    for name in ["agent-loop-telegram", "research-loop"] {
        let def = format!(
            "document: {{dsl: '1.0.4', namespace: t, name: {name}, version: '1.0.0', metadata: {{loop: {{description: Research}}}}}}\ndo: [{{done: {{set: {{output: done}}}}}}]\n"
        );
        let save = json!({"name":name,"definition":def});
        assert!(
            flows::tool(&f.engine, "save_workflow", save.as_object().unwrap())
                .unwrap_err()
                .to_string()
                .contains("requires a `call: start_child`")
        );
        let saved = f.engine.deps.workflows.save(name, &def, None).unwrap();
        let args = json!({"name":name,"version":saved.version});
        assert!(
            flows::tool(&f.engine, "activate_workflow", args.as_object().unwrap())
                .unwrap_err()
                .to_string()
                .contains("requires a `call: start_child`")
        );
    }
}

#[tokio::test]
async fn a_named_variants_breaker_falls_back_after_three_faults_and_keeps_others_closed() {
    let model = Arc::new(Scripted::new(vec![assistant("fallback"), assistant("telegram fallback")]));
    let f = fixture(model, &[], WORKER);
    for name in ["research-loop", "agent-loop-telegram"] {
        let broken = variant(name).replacen(
            "do:\n",
            "do:\n  - broken: { raise: { error: { type: 'https://example.com/fault', status: 500 } } }\n",
            1,
        );
        activate(&f, name, &broken);
        for n in 0..3 {
            assert!(matches!(run(&f, &format!("{name}-{n}"), name, false).await, FlowResult::Faulted { .. }));
        }
        assert!(matches!(run(&f, &format!("{name}-fallback"), name, false).await, FlowResult::Completed(_)));
    }
}

#[tokio::test]
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
        for name in ["explicit-loop", "skill-loop"] {
            let def = variant(name).replace("output: .content", &format!("output: (\"{name}: \" + .content)"));
            activate(&f, name, &def);
        }
        // A new draft must not hide an active version from the catalog.
        f.engine.deps.workflows.save("skill-loop", &variant("skill-loop"), None).unwrap();
        assert!(matches!(run(&f,"parent","agent-loop",false).await,FlowResult::Completed(ref v) if v=="parent answer"));
        let runs = agent::durable::RunStore::new((*f.db).clone());
        let child = runs.get("parent__sub1.0/loop").unwrap().unwrap();
        let expected = if explicit { "explicit-loop" } else { "skill-loop" };
        assert_eq!(child.name, expected);
        assert_eq!(child.outcome, Some(owf::run::Status::Completed(json!(format!("{expected}: worker answer")))));
        assert!(child.input["loop"]["conversation"].is_null());
        let seen = model.seen.lock().unwrap();
        let description =
            seen[0].tools.iter().find(|t| t.name == "invoke_sub_agent").unwrap().description.as_ref().unwrap();
        for name in ["agent-loop:", "agent-loop-telegram:", "explicit-loop:", "skill-loop:"] {
            assert!(description.contains(name), "{description}");
        }
    }
}

fn route_definition(name: &str, when: &str, priority: Option<i64>) -> String {
    let priority = priority.map(|n| format!(", priority: {n}")).unwrap_or_default();
    format!(
        "document: {{dsl: '1.0.4', namespace: t, name: {name}, version: '1.0.0', metadata: {{route: {{when: {{{when}}}{priority}}}}}}}\ndo: [{{done: {{set: {{output: routed}}}}}}]\noutput: {{as: '${{ .output }}'}}\n"
    )
}

#[test]
fn routing_uses_source_payload_specificity_priority_and_active_overrides() {
    let f = fixture(Arc::new(Scripted::new(vec![])), &[], WORKER);
    let pick = |source: &str, payload: &Value| flows::signal_route(&f.engine, source, payload).unwrap().map(|r| r.0);
    assert_eq!(pick("telegram", &Value::Null).as_deref(), Some("signal-telegram"));
    assert_eq!(pick("telegram_button", &Value::Null).as_deref(), Some("signal-telegram"));
    assert_eq!(pick("peer", &Value::Null), None);
    assert_eq!(pick("scheduler", &Value::Null), None);
    let specific = route_definition("topic-route", "source: [telegram, telegram_button], topic: research", None);
    activate(&f, "topic-route", &specific);
    assert_eq!(pick("telegram", &json!({"topic":"research"})).as_deref(), Some("topic-route"));
    let tie = route_definition("tied-route", "source: telegram", None);
    let saved = f.engine.deps.workflows.save("tied-route", &tie, None).unwrap();
    let args = json!({"name":"tied-route","version":saved.version});
    let error = flows::tool(&f.engine, "activate_workflow", args.as_object().unwrap()).unwrap_err().to_string();
    assert!(error.contains("signal-telegram") && error.contains("priority"), "{error}");
    activate(&f, "tied-route", &route_definition("tied-route", "source: telegram", Some(1)));
    assert_eq!(pick("telegram", &Value::Null).as_deref(), Some("tied-route"));
    assert_eq!(pick("telegram", &json!({"topic":"research"})).as_deref(), Some("topic-route"));
    activate(&f, "signal-telegram", &route_definition("signal-telegram", "source: [peer, scheduler]", None));
    assert_eq!(pick("peer", &Value::Null).as_deref(), Some("signal-telegram"));
}

#[tokio::test]
async fn a_worker_start_child_can_choose_a_loop_and_admitted_versions_stay_pinned() {
    let model = Arc::new(Scripted::new(vec![
        calls(&[("invoke_sub_agent", json!({"prompt":"research"}))]),
        assistant("worker result"),
        assistant("parent result"),
        assistant("pinned result"),
    ]));
    let f = fixture(model, &[], WORKER);
    let old = variant("research-loop").replace("output: .content", "output: (\"old: \" + .content)");
    let version = activate(&f, "research-loop", &old);
    let parent = AGENT_LOOP
        .replace("with: { workflow: agent-loop, call:", "with: { workflow: agent-loop, loop: research-loop, call:");
    activate(&f, "agent-loop", &parent);
    assert!(matches!(run(&f, "named-child", "agent-loop", false).await, FlowResult::Completed(_)));
    let runs = agent::durable::RunStore::new((*f.db).clone());
    let child = runs.get("named-child__sub1.0/loop").unwrap().unwrap();
    assert_eq!(child.name, "research-loop");
    assert_eq!(child.outcome, Some(owf::run::Status::Completed(json!("old: worker result"))));
    let mut opts = LoopOptions::new("pinned", context());
    opts.workflow = "research-loop".into();
    opts.skills = vec!["worker".into()];
    let mut input = f.engine.start_loop(opts).await.unwrap().workflow_input("go").await;
    input["override_version"] = json!(version);
    runs.admit("pinned/loop", "research-loop", &old, &input).unwrap();
    f.engine.end_loop("pinned").await;
    activate(&f, "research-loop", &variant("research-loop"));
    let result = flows::create_durable_flows(f.engine.clone(), (*f.db).clone())
        .run_loop("pinned/loop", Value::Null, &null_scope(), &CancellationToken::new())
        .await;
    assert!(matches!(result,FlowResult::Completed(ref v) if v=="old: pinned result"), "{result:?}");
    let saved = runs.get("pinned/loop").unwrap().unwrap();
    assert_eq!(saved.input["override_version"], version);
    assert_eq!(saved.definition, old);
}

#[tokio::test]
async fn the_silent_end_nudge_gets_one_turn_beyond_the_iteration_limit() {
    let model = Arc::new(Scripted::new(vec![
        assistant("nothing yet"),
        calls(&[("finish", json!({"result":"no reply needed"}))]),
    ]));
    let f = fixture(model.clone(), &[], WORKER);
    let mut opts = LoopOptions::new("limit", context());
    opts.workflow = "agent-loop-telegram".into();
    opts.skills = vec!["worker".into()];
    opts.max_iterations = 1;
    opts.conversation = Some(ConfirmTarget { chat_id: "42".into(), message_thread_id: None });
    let live = f.engine.start_loop(opts).await.unwrap();
    assert_eq!(live.send("Nothing to do".into()).await.unwrap(), "no reply needed");
    assert_eq!(model.seen.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn an_in_process_worker_start_child_respects_the_yaml_loop_selection() {
    let model = Arc::new(Scripted::new(vec![
        calls(&[("invoke_sub_agent", json!({"prompt":"research"}))]),
        assistant("worker answer"),
        assistant("done"),
    ]));
    let f = fixture(model.clone(), &[], WORKER);
    activate(
        &f,
        "research-loop",
        &variant("research-loop").replace("output: .content", "output: (\"research: \" + .content)"),
    );
    let parent = AGENT_LOOP
        .replace("with: { workflow: agent-loop, call:", "with: { workflow: agent-loop, loop: research-loop, call:");
    activate(&f, "agent-loop", &parent);
    let mut opts = LoopOptions::new("ephemeral", context());
    opts.skills = vec!["worker".into()];
    let live = f.engine.start_loop(opts).await.unwrap();
    assert_eq!(live.send("research".into()).await.unwrap(), "done");
    let seen = model.seen.lock().unwrap();
    assert!(seen[2].messages.last().unwrap()["content"].as_str().unwrap().contains("research: worker answer"));
}

#[tokio::test]
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
        activate(
            &f,
            "research-loop",
            &variant("research-loop").replace("output: .content", "output: (\"research: \" + .content)"),
        );
        let parent = AGENT_LOOP
            .replace("with: { workflow: agent-loop, call:", "with: { workflow: agent-loop, loop: agent-loop, call:");
        activate(&f, "agent-loop", &parent);
        if durable {
            assert!(
                matches!(run(&f,"explicit-base","agent-loop",false).await,FlowResult::Completed(ref v) if v=="done")
            );
            let runs = agent::durable::RunStore::new((*f.db).clone());
            assert_eq!(runs.get("explicit-base__sub1.0/loop").unwrap().unwrap().name, "agent-loop");
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
