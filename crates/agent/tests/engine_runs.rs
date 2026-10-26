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

// ── the world of the design's tests ─────────────────────────────────────────

use agent::{
    agent::{Engine, EngineDeps, LoopOptions, Providers, create_engine_with},
    db::AgentDb,
    supervisor::{PendingSignal, create_supervisor},
};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

const CHAT: &str = "42";

// What outlives an engine: its stores and clients. A restart builds another
// engine on the same parts.
#[derive(Clone)]
struct Parts {
    store: Arc<dyn ouro_runtime::durable::RunStore>,
    dry_store: Arc<dyn ouro_runtime::durable::RunStore>,
    db: AgentDb,
    mcp: Arc<ToolMcp>,
    model: Arc<Turns>,
    guard: Arc<GuardApi>,
}

fn tools() -> Vec<agent::providers::ToolDef> {
    let open = json!({"type":"object"});
    ["send_telegram_message", "edit_telegram_message", "append_doc", "search_news", "get_telegram_chat_history"]
        .iter()
        .map(|name| agent::providers::ToolDef::new(name, name, open.clone()))
        .collect()
}

// Telegram answers a send with its message id; the rest answer "ok".
fn parts(turns: Vec<Turn>) -> Parts {
    let (store, dry_store) = agent::runs::memory_store_with_view();
    let mcp = ToolMcp::returning(tools(), "ok");
    mcp.always(Arc::new(|name, args, _| {
        Box::pin(async move {
            Ok(match name.as_str() {
                "send_telegram_message" => json!({"messageId": 100, "echo": args}).to_string(),
                "get_telegram_chat_history" => json!({"messages": []}).to_string(),
                _ => "ok".into(),
            })
        })
    }));
    Parts {
        store,
        dry_store,
        db: AgentDb::open(":memory:").unwrap(),
        mcp,
        model: Turns::new(turns),
        guard: Arc::new(GuardApi::default()),
    }
}

fn engine(parts: &Parts) -> Arc<Engine> {
    engine_with(parts, agent::runs::config())
}

fn engine_with(parts: &Parts, config: ouro_runtime::Config) -> Arc<Engine> {
    let provider: Arc<dyn agent::providers::ChatProvider> = parts.model.clone();
    create_engine_with(
        EngineDeps {
            providers: Providers {
                openai: provider.clone(),
                gemini: provider.clone(),
                deepseek: provider.clone(),
                openrouter: Some(provider),
            },
            mcp: parts.mcp.clone(),
            presets: agent::models::default_presets(),
            // The orchestrator's skill allows every tool, as the shipped one.
            skills: vec!["orchestrator".into()],
            skill_store: Arc::new(AnySkill),
            memory: Arc::new(parts.db.clone()),
            tracer: Arc::new(agent::local_recorder::local_recorder_tracer(Arc::new(parts.db.clone()))),
            codex: Arc::new(NoCodex),
            decisions: Some(agent::decisions::decision_client("guard-test".into(), parts.guard.clone())),
            schedule: Arc::new(agent::schedule::Schedules::in_memory()),
            env: agent::session::EnvConfig { user_email: None, telegram_chat_id: Some(CHAT.into()) },
            db: parts.db.clone(),
            store: parts.store.clone(),
            dry_store: parts.dry_store.clone(),
        },
        config,
    )
}

fn telegram(id: i64, text: &str) -> PendingSignal {
    PendingSignal {
        id,
        source: "telegram".into(),
        content: format!("Telegram message in chat {CHAT}.\nText: {}", json!(text)),
        env_context: None,
        created_at: "2026-09-06 12:00:00".into(),
        payload: None,
    }
}

fn press(id: i64, chat: &str, message: i64, token: &str) -> PendingSignal {
    PendingSignal {
        id,
        source: "telegram_button".into(),
        content: json!({"chatId": chat.parse::<i64>().unwrap(), "messageThreadId": null, "messageId": message,
            "data": token, "text": "Confirm"})
        .to_string(),
        env_context: None,
        created_at: "2026-09-06 12:00:00".into(),
        payload: None,
    }
}

// The confirm and decline tokens of the card the guard sent.
fn card_tokens(mcp: &ToolMcp) -> (String, String) {
    let calls = mcp.calls.lock().unwrap();
    let card = calls
        .iter()
        .find(|(name, args)| name == "send_telegram_message" && args.contains_key("buttons"))
        .expect("a confirmation card was sent");
    let buttons = &card.1["buttons"][0];
    (buttons[0]["data"].as_str().unwrap().into(), buttons[1]["data"].as_str().unwrap().into())
}

fn count(mcp: &ToolMcp, tool: &str) -> usize {
    mcp.names().iter().filter(|n| *n == tool).count()
}

async fn until_ended(engine: &Engine, run: &str) -> agent::runs::Status {
    tokio::time::timeout(
        Duration::from_secs(30),
        agent::runs::wait_end(engine, run, &tokio_util::sync::CancellationToken::new()),
    )
    .await
    .expect("the run ends")
    .unwrap()
}

// A write the guard asks the user about: the phrase makes the deterministic
// rule ask for a confirmation, without the decision model.
const ASKED: &str = "guard test confirm: save the note";

fn save_then_answer(after: &'static str) -> Vec<Turn> {
    vec![
        turn(|_| call("append_doc", json!({"text":"note"}))),
        turn(move |p| answer(&format!("{after}: {}", last_result(&p.messages)))),
    ]
}

// ── 1. invariant 1: a confirmation is honoured only from its own press ──────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_press_arriving_as_the_next_signal_is_honoured_within_seconds() {
    let parts = parts(save_then_answer("done"));
    let engine = engine(&parts);
    let supervisor = create_supervisor(engine.clone());
    // The tree waits for the press: the supervisor is free again.
    let raw = supervisor.run_signal(&telegram(1, ASKED)).await.unwrap();
    let waiting: Value = serde_json::from_str(&raw).unwrap_or_else(|_| panic!("{raw}"));
    assert_eq!(waiting["resumes"], true);
    assert_eq!(count(&parts.mcp, "append_doc"), 0, "nothing is written before the press");
    let (yes, _) = card_tokens(&parts.mcp);
    let started = std::time::Instant::now();
    assert_eq!(supervisor.run_signal(&press(2, CHAT, 100, &yes)).await.unwrap(), "Button delivered");
    let ended = until_ended(&engine, "signal:1").await;
    assert!(started.elapsed() < Duration::from_secs(10), "{:?}", started.elapsed());
    assert!(matches!(ended, agent::runs::Status::Completed(ref v) if v.as_str().unwrap().starts_with("done")));
    assert_eq!(count(&parts.mcp, "append_doc"), 1, "the confirmed write, once: {ended:?}");
    supervisor.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_press_from_another_chat_or_message_is_not_honoured() {
    for (chat, message) in [("43", 100), (CHAT, 101)] {
        let parts = parts(save_then_answer("blocked"));
        let engine = engine(&parts);
        let supervisor = create_supervisor(engine.clone());
        supervisor.run_signal(&telegram(1, ASKED)).await.unwrap();
        let (yes, _) = card_tokens(&parts.mcp);
        // Delivered to the guard round (the token is its card's), and refused there.
        assert_eq!(supervisor.run_signal(&press(2, chat, message, &yes)).await.unwrap(), "Button delivered");
        let ended = until_ended(&engine, "signal:1").await;
        let agent::runs::Status::Completed(output) = ended else { panic!("{ended:?}") };
        assert!(output.as_str().unwrap().contains("unverified_confirmation"), "{output}");
        assert_eq!(count(&parts.mcp, "append_doc"), 0, "{chat}/{message}: never written");
        supervisor.shutdown().await;
    }
}

// A press that arrives while the card is still being sent, before the
// guard listens, waits in the round's mailbox and is honoured once; the
// same press again changes nothing. (Another run's token is no evidence:
// confirmations.rs, confirmation_tokens_are_bound_to_one_run_*.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_press_before_the_guard_listens_is_honoured_once() {
    let parts = parts(save_then_answer("done"));
    let release = Arc::new(tokio::sync::Notify::new());
    {
        let release = release.clone();
        parts.mcp.always(Arc::new(move |name, args, _| {
            let release = release.clone();
            Box::pin(async move {
                if name == "send_telegram_message" && args.contains_key("buttons") {
                    release.notified().await;
                }
                Ok(if name == "send_telegram_message" { json!({"messageId": 100}).to_string() } else { "ok".into() })
            })
        }));
    }
    let engine = engine(&parts);
    let live = loop_of(&parts, &engine, "loop-early", ASKED).await;
    let sent = tokio::spawn(async move { live.send(ASKED.into()).await });
    // The card's row exists before its send answers.
    let (yes, guard) = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let row = parts.db.conn().query_row("SELECT yes_type, run_id FROM confirmations", [], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            });
            if let Ok(row) = row {
                return row;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let press = |id: &str| {
        json!({"source":"telegram","id":id,"type":yes,
        "data":{"chatId":42,"messageThreadId":null,"messageId":100,"data":yes}})
    };
    assert_eq!(engine.runtime.deliver(press("early-1"), Some(&guard)).await.unwrap().as_deref(), Some(guard.as_str()));
    assert_eq!(engine.runtime.deliver(press("early-2"), Some(&guard)).await.unwrap().as_deref(), Some(guard.as_str()));
    release.notify_one();
    let out = sent.await.unwrap().unwrap();
    assert!(out.starts_with("done"), "{out}");
    assert_eq!(count(&parts.mcp, "append_doc"), 1, "the confirmed write, once");
    let (_, answer) = agent::confirmation::Store::by_token(&parts.db, &yes).unwrap().unwrap();
    assert_eq!(answer.as_deref(), Some("confirmed"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_press_of_an_unknown_card_falls_through_to_the_agent_with_its_reason() {
    let parts = parts(vec![turn(|p| answer(p.messages[0]["content"].as_str().unwrap()))]);
    let engine = engine(&parts);
    let supervisor = create_supervisor(engine.clone());
    let out = supervisor.run_signal(&press(3, CHAT, 100, "cf:forged")).await.unwrap();
    assert!(out.contains("No confirmation card matches the pressed button"), "{out}");
    supervisor.shutdown().await;
}

// ── 2. invariant 2: a model write is never executed unreviewed ──────────────

async fn loop_of(parts: &Parts, engine: &Arc<Engine>, id: &str, task: &str) -> Arc<agent::agent::AgentLoop> {
    let _ = parts;
    let mut opts = LoopOptions::new(id, task_context(id));
    opts.skills = vec!["any".into()];
    opts.include_engine_skills = false;
    opts.confirm = Some(agent::agent::ConfirmTarget { chat_id: CHAT.into(), message_thread_id: None });
    let live = engine.start_loop(opts).await.unwrap();
    live.admit_task(task).await;
    live
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_without_a_guard_child_is_reviewed_inline_and_a_foreign_guard_run_is_refused() {
    let parts = parts(Vec::new());
    let engine = engine(&parts);
    let live = loop_of(&parts, &engine, "loop-2", "save the note").await;
    let write = json!({"id":"c1","name":"append_doc","arguments":"{\"text\":\"x\"}"});
    // No guard child: the host reviews the call itself before dispatch.
    support::loop_tool(&engine, &live, &json!({"call":write,"i":1,"index":0}), "loop-2", "k1", true).await.unwrap();
    assert_eq!(parts.guard.requests.lock().unwrap().len(), 1, "one review");
    assert_eq!(count(&parts.mcp, "append_doc"), 1);
    // A guard run that is not this run's own guard child is refused.
    let foreign = json!({"call":write,"guard_run":"signal:9","i":1,"index":0});
    let error = support::loop_tool(&engine, &live, &foreign, "loop-2", "k2", true).await.unwrap_err();
    assert!(error.downcast_ref::<agent::agent::Refusal>().is_some(), "{error}");
    assert_eq!(count(&parts.mcp, "append_doc"), 1, "never dispatched");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pending_card_cannot_be_bypassed_by_omitting_the_guard_child() {
    use agent::confirmation::Store;
    let parts = parts(Vec::new());
    let engine = engine(&parts);
    let live = loop_of(&parts, &engine, "loop-3", "save the note").await;
    let call = agent::providers::ToolCall { id: "c1".into(), name: "append_doc".into(), arguments: "{}".into() };
    parts
        .db
        .insert(&agent::confirmation::Pending {
            run: "guard-run".into(),
            loop_run: Some("loop-3".into()),
            key: agent::guard::key(&call),
            yes: "cf:y".into(),
            no: "cf:n".into(),
            target: agent::agent::ConfirmTarget { chat_id: CHAT.into(), message_thread_id: None },
            message: Some(100),
            card: "card".into(),
        })
        .unwrap();
    let args = json!({"call":{"id":"c1","name":"append_doc","arguments":"{}"},"i":1,"index":0});
    let out = support::loop_tool(&engine, &live, &args, "loop-3", "k", true).await.unwrap();
    assert!(out["presented"].as_str().unwrap().contains("unverified_confirmation"), "{out}");
    assert_eq!(count(&parts.mcp, "append_doc"), 0);
}

// ── 3. invariant 3: model parameters come only from the host ────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runs_without_host_data_are_refused_by_host_functions() {
    let parts = parts(Vec::new());
    let engine = engine(&parts);
    engine.ready().await.unwrap();
    // A run a workflow (or anyone) starts without host data takes no turn,
    // opens no session and reviews nothing.
    for (definition, input) in [
        ("agent-loop", json!({"messages":[{"role":"system","content":"s"},{"role":"user","content":"u"}],"memory":[]})),
        ("signal", json!({"signal":{"id":1,"source":"telegram","content":"x","created_at":"2026-09-06 12:00:00"}})),
        ("guard", json!({"calls":[],"round":1,"history":[[],0]})),
    ] {
        let id = format!("forged-{definition}");
        engine.runtime.start(&id, definition, input).await.unwrap();
        let ended = until_ended(&engine, &id).await;
        let agent::runs::Status::Faulted(error) = &ended else { panic!("{definition}: {ended:?}") };
        let detail = error.detail.clone().unwrap_or_default();
        assert!(
            detail.contains("host") || detail.contains("dispatcher") || detail.contains("guard"),
            "{definition}: {detail}"
        );
    }
    assert_eq!(parts.model.requests.lock().unwrap().len(), 0, "no model call");
}

// A workflow that starts `agent-loop` itself (the engine's `start_child`)
// gets a run with no host data: the loop takes no turn (invariant 3).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_loop_a_workflow_starts_itself_is_never_admitted() {
    let parts = parts(vec![turn(|_| answer("never asked"))]);
    let engine = engine(&parts);
    engine.ready().await.unwrap();
    let spawner = r#"document: { dsl: '1.0.4', namespace: t, name: spawner, version: '1.0.0' }
do:
  - spawn:
      call: start_child
      with:
        name: agent-loop
        input: { messages: [ { role: system, content: s }, { role: user, content: u } ], memory: [] }
      export: { as: '${ $context + { child: . } }' }
  - wait:
      listen: { to: { one: { with: { type: '${ $context.child.done }' } } } }
  - outcome:
      call: child_outcome
      with: { id: '${ $context.child.id }', key: '${ $context.child.key }' }
"#;
    let version = engine.runtime.save("spawner", spawner).await.unwrap().version;
    engine.runtime.activate("spawner", version).await.unwrap();
    engine.runtime.start("spawner-1", "spawner", json!({})).await.unwrap();
    until_ended(&engine, "spawner-1").await;
    let child = children(&engine, "spawner-1").await.pop().expect("the started loop");
    let run = engine.runtime.get(&child).await.unwrap().unwrap();
    assert!(run.admission.host().is_none(), "a workflow's child has no host data");
    let Some(agent::runs::Status::Faulted(error)) = run.outcome else { panic!("{:?}", run.outcome) };
    assert!(error.detail.unwrap_or_default().contains("host"), "refused for want of host data");
    assert!(parts.model.requests.lock().unwrap().is_empty(), "no model call");
}

// A `guard` version an operator activated never stands in for the reserved
// round: the loop's rounds run the built-in one, and a verdict of another
// definition authorizes nothing (invariants 1 and 2).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_activated_guard_version_never_reviews_a_round() {
    let parts =
        parts(vec![turn(|_| call("append_doc", json!({"text":"note"}))), turn(|p| answer(&last_result(&p.messages)))]);
    *parts.guard.label.lock().unwrap() = Some("misaligned".into());
    let engine = engine(&parts);
    engine.ready().await.unwrap();
    let forged = "document: { dsl: '1.0.4', namespace: agent, name: guard, version: '9.0.0', metadata: { ouro: { breaker: 'off' } } }
do: [ { allow: { set: { verdicts: [] } } } ]
";
    let version = engine.runtime.save("guard", forged).await.unwrap().version;
    engine.runtime.activate("guard", version).await.unwrap();
    let live = loop_of(&parts, &engine, "loop-g", "save the note").await;
    let out = live.send("save the note".into()).await.unwrap();
    assert!(out.contains("[guard] blocked: misaligned"), "the built-in round reviewed it: {out}");
    assert_eq!(count(&parts.mcp, "append_doc"), 0);
    let guard = children(&engine, "loop-g").await.into_iter().next().expect("the round's guard");
    let run = engine.runtime.get(&guard).await.unwrap().unwrap();
    assert_eq!(run.admission.definition, agent::flows::GUARD);
}

// ── 4. invariant 4: an at-most-once call is never resent after a crash ──────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_cut_by_a_restart_is_unknown_to_the_model_and_sent_once() {
    let parts =
        parts(vec![turn(|_| call("append_doc", json!({"text":"note"}))), turn(|p| answer(&last_result(&p.messages)))]);
    let sent = Arc::new(tokio::sync::Notify::new());
    {
        let sent = sent.clone();
        parts.mcp.once(Box::new(move |_, _, _| {
            Box::pin(async move {
                sent.notify_one();
                std::future::pending::<anyhow::Result<String>>().await
            })
        }));
    }
    let first = engine(&parts);
    let live = loop_of(&parts, &first, "loop-4", "save the note").await;
    let running = tokio::spawn(async move { live.send("save the note".into()).await });
    tokio::time::timeout(Duration::from_secs(30), sent.notified()).await.unwrap();
    first.halt().await;
    let _ = running.await;
    // The next process resumes the run: the write's outcome is unknown.
    let second = engine(&parts);
    second.ready().await.unwrap();
    let ended = until_ended(&second, "loop-4").await;
    let agent::runs::Status::Completed(output) = ended else { panic!("{ended:?}") };
    assert!(output.as_str().unwrap().contains("outcome unknown"), "{output}");
    assert_eq!(count(&parts.mcp, "append_doc"), 1, "never sent again");
    second.shutdown().await;
}

// ── 5. invariant 5: a worker inherits the confirmation target ───────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_confirms_where_its_parent_does() {
    let parts = parts(vec![
        turn(|_| call("invoke_sub_agent", json!({"prompt":"sub task"}))),
        turn(|_| answer("worker answer")),
        turn(|_| answer("parent answer")),
    ]);
    let engine = engine(&parts);
    let live = loop_of(&parts, &engine, "loop-5", "delegate").await;
    assert_eq!(live.send("delegate".into()).await.unwrap(), "parent answer");
    let workers = children(&engine, "loop-5").await;
    assert_eq!(workers.len(), 1);
    let worker = engine.runtime.get(&workers[0]).await.unwrap().unwrap();
    let host = worker.admission.host().unwrap();
    assert_eq!(host["kind"], "worker");
    assert_eq!(host["confirm"], json!({"chatId": CHAT, "messageThreadId": null}));
    assert_eq!(host["parent_id"], "loop-5");
}

// What a stuck tree looks like: each run's state and its calls (a test's
// diagnostics on a timeout).
async fn dump(engine: &Engine, root: &str) -> String {
    let mut out = String::new();
    let mut pending = vec![root.to_owned()];
    while let Some(id) = pending.pop() {
        if let Some(run) = engine.runtime.get(&id).await.unwrap() {
            out += &format!(
                "{id} {} {:?} wake={} outcome={:?}\n",
                run.admission.name, run.state, run.wake_at_ms, run.outcome
            );
            for call in engine.runtime.calls(&id).await.unwrap_or_default() {
                out += &format!("  {call:?}\n");
            }
        }
        pending.extend(children(engine, &id).await);
    }
    out
}

async fn children(engine: &Engine, run: &str) -> Vec<String> {
    let filter = agent::runs::Runs { parent: Some(run.into()), ..Default::default() };
    engine.control.list(&filter, None, 100).await.unwrap().items.into_iter().map(|r| r.admission.id).collect()
}

// ── 6. invariant 6: a card with an unknown send is never resent ─────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_card_whose_send_is_unknown_is_not_resent_and_the_round_goes_on() {
    let parts = parts(vec![
        turn(|_| {
            calls_of(&[
                call_id("append_doc", json!({"text":"note"}), "a"),
                call_id("send_telegram_message", json!({"chatId":CHAT,"text":"hello"}), "b"),
            ])
        }),
        turn(|p| answer(&json!(tool_replies(&p.messages)).to_string())),
    ]);
    parts.mcp.always(Arc::new(|name, args, _| {
        Box::pin(async move {
            if name == "send_telegram_message" && args.contains_key("buttons") {
                anyhow::bail!("connection reset after the request was sent");
            }
            Ok(if name == "send_telegram_message" { json!({"messageId": 7}).to_string() } else { "ok".into() })
        })
    }));
    let engine = engine(&parts);
    let live = loop_of(&parts, &engine, "loop-6", &format!("{ASKED}; reply in chat {CHAT}")).await;
    let sent = tokio::time::timeout(Duration::from_secs(30), live.send(format!("{ASKED}; reply in chat {CHAT}"))).await;
    let Ok(out) = sent else { panic!("the round never settled:\n{}", dump(&engine, "loop-6").await) };
    let out = out.unwrap();
    let cards = parts
        .mcp
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(n, a)| n == "send_telegram_message" && a.contains_key("buttons"))
        .count();
    assert_eq!(cards, 1, "the card is never sent again");
    assert_eq!(count(&parts.mcp, "append_doc"), 0, "an unverified write is not made");
    assert!(out.contains("unverified_confirmation"), "{out}");
    let plain = parts
        .mcp
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(n, a)| n == "send_telegram_message" && !a.contains_key("buttons"))
        .count();
    assert_eq!(plain, 1, "the round's other call stands");
}

// ── 7. capacity: a primary with 16 workers and their guards completes ───────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sixteen_workers_complete_under_one_parent() {
    let mut turns = vec![turn(|_| {
        calls_of(
            &(0..16)
                .map(|i| call_id("invoke_sub_agent", json!({"prompt": format!("part {i}")}), &format!("w{i}")))
                .collect::<Vec<_>>(),
        )
    })];
    for _ in 0..16 {
        turns.push(turn(|_| answer("part done")));
    }
    turns.push(turn(|p| answer(&format!("{} replies", tool_replies(&p.messages).len()))));
    let parts = parts(turns);
    let engine = engine(&parts);
    let live = loop_of(&parts, &engine, "loop-7", "split it").await;
    // A liveness bound, not a latency one: in a debug build under the whole
    // suite's load the round takes tens of seconds (docs/operations.md).
    let out = tokio::time::timeout(Duration::from_secs(240), live.send("split it".into())).await.unwrap().unwrap();
    assert_eq!(out, "16 replies");
    assert_eq!(children(&engine, "loop-7").await.len(), 16);
}

// ── 8. the built-ins never trip ─────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn faulted_signals_never_trip_the_built_ins() {
    let parts = parts(Vec::new());
    let engine = engine(&parts);
    let supervisor = create_supervisor(engine.clone());
    for id in 1..=4 {
        // The model is down: the primary faults, and so does the signal.
        let error = supervisor.run_signal(&telegram(id, "hello")).await.unwrap_err().to_string();
        assert!(error.contains("Unexpected LLM call"), "signal {id}: {error}");
    }
    for name in ["signal", "agent-loop"] {
        assert!(engine.runtime.definitions(name).await.unwrap().tripped.is_none(), "{name}");
    }
    supervisor.shutdown().await;
}

// ── 9. a signal id is one run ───────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_same_signal_started_twice_is_one_run() {
    let parts = parts(vec![turn(|_| answer("once"))]);
    let engine = engine(&parts);
    let supervisor = create_supervisor(engine.clone());
    let signal = telegram(9, "hello");
    assert_eq!(supervisor.run_signal(&signal).await.unwrap(), "once");
    assert_eq!(supervisor.run_signal(&signal).await.unwrap(), "once");
    assert_eq!(parts.model.requests.lock().unwrap().len(), 1, "one run, one turn");
    supervisor.shutdown().await;
}

// ── 10. host data stays small: evidence goes by digest ──────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forty_kilobyte_skill_set_starts_fine() {
    let skills: Vec<(String, String)> =
        (0..4).map(|i| (format!("big{i}"), format!("---\ntools: *\n---\n{}", "x".repeat(10 * 1024)))).collect();
    let skills: Vec<(&str, &str)> = skills.iter().map(|(n, b)| (n.as_str(), b.as_str())).collect();
    let f = fixture(Arc::new(Scripted::new(vec![agent::providers::assistant("fine")])), &[], &skills);
    let mut opts = LoopOptions::new("big", context());
    opts.skills = (0..4).map(|i| format!("big{i}")).collect();
    let live = f.engine.start_loop(opts).await.unwrap();
    assert_eq!(live.send("go".into()).await.unwrap(), "fine");
    let run = f.engine.runtime.get("big").await.unwrap().unwrap();
    let host = run.admission.host().unwrap().to_string();
    assert!(host.len() < 4 * 1024, "{} bytes of host data", host.len());
    let digest = run.admission.host().unwrap()["evidence"]["skills"].as_str().unwrap().to_owned();
    assert!(f.db.evidence(&digest).unwrap().unwrap().len() > 40 * 1024);
}

// ── 11. a child continues across a restart, whatever changed meanwhile ──────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_cut_by_a_restart_continues_as_the_same_child_after_its_skill_changed() {
    let reached = Arc::new(tokio::sync::Notify::new());
    let blocked = reached.clone();
    let parts = parts(vec![
        turn(|_| call("invoke_sub_agent", json!({"prompt":"sub task","skills":["any"]}))),
        turn_async(move |_| async move {
            blocked.notify_one();
            std::future::pending::<anyhow::Result<agent::providers::Message>>().await
        }),
        turn(|_| answer("worker answer")),
        turn(|p| answer(&last_result(&p.messages))),
    ]);
    let first = engine(&parts);
    let live = loop_of(&parts, &first, "loop-11", "delegate").await;
    let running = tokio::spawn(async move { live.send("delegate".into()).await });
    tokio::time::timeout(Duration::from_secs(30), reached.notified()).await.unwrap();
    let before = children(&first, "loop-11").await;
    first.halt().await;
    let _ = running.await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let second = engine(&parts);
    second.ready().await.unwrap();
    let ended = until_ended(&second, "loop-11").await;
    assert!(matches!(ended, agent::runs::Status::Completed(ref v) if v.as_str().unwrap().contains("worker answer")));
    assert_eq!(children(&second, "loop-11").await, before, "the same child, not a second one");
    second.shutdown().await;
}

// ── 12. what signal_input captured survives a restart before primary_loop ───

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_primarys_guard_evidence_is_the_same_after_a_restart_before_it_starts() {
    async fn primary_evidence(restart: bool) -> Value {
        let parts = parts(vec![turn(|_| answer("done"))]);
        parts.mcp.always(Arc::new(|name, _, _| {
            Box::pin(async move {
                Ok(match name.as_str() {
                    "get_telegram_chat_history" => json!({"messages":[
                        {"id":1,"chat_id":42,"thread_id":null,"role":"user","text":"earlier ask","created_at":"2026-09-06 11:00:00"}
                    ]})
                    .to_string(),
                    _ => "ok".into(),
                })
            })
        }));
        // The restart lands between signal_input and primary_loop: in the
        // activity read, which comes after signal_input.
        let reached = Arc::new(tokio::sync::Notify::new());
        if restart {
            let reached = reached.clone();
            parts.mcp.once(Box::new(move |_, _, _| {
                Box::pin(async move { Ok(json!({"messages":[
                    {"id":1,"chat_id":42,"thread_id":null,"role":"user","text":"earlier ask","created_at":"2026-09-06 11:00:00"}
                ]}).to_string()) })
            }));
            parts.mcp.once(Box::new(move |_, _, _| {
                Box::pin(async move {
                    reached.notify_one();
                    std::future::pending::<anyhow::Result<String>>().await
                })
            }));
        }
        let first = engine(&parts);
        let supervisor = create_supervisor(first.clone());
        let signal = telegram(12, "now do it");
        let engine = if restart {
            let running = {
                let supervisor = supervisor.clone();
                let signal = signal.clone();
                tokio::spawn(async move { supervisor.run_signal(&signal).await })
            };
            tokio::time::timeout(Duration::from_secs(30), reached.notified()).await.unwrap();
            first.halt().await;
            running.abort();
            let second = engine(&parts);
            second.ready().await.unwrap();
            until_ended(&second, "signal:12").await;
            second
        } else {
            supervisor.run_signal(&signal).await.unwrap();
            first
        };
        let primary = children(&engine, "signal:12").await.pop().unwrap();
        let run = engine.runtime.get(&primary).await.unwrap().unwrap();
        let earlier = run.admission.host().unwrap()["evidence"]["earlier"].as_str().unwrap().to_owned();
        serde_json::from_str(&parts.db.evidence(&earlier).unwrap().unwrap()).unwrap()
    }
    let plain = primary_evidence(false).await;
    assert_eq!(plain, json!(["earlier ask"]));
    assert_eq!(primary_evidence(true).await, plain);
}

// ── 13. settle_child: done, raise, cancelled ────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_signal_settles_its_primary_by_how_it_ended() {
    // done: the primary's answer is the signal's.
    let world = self::parts(vec![turn(|_| answer("answered"))]);
    let engine = self::engine(&world);
    let supervisor = create_supervisor(engine.clone());
    assert_eq!(supervisor.run_signal(&telegram(1, "hi")).await.unwrap(), "answered");
    supervisor.shutdown().await;
    // raise: the primary's fault is the signal's, with its detail.
    let world = self::parts(Vec::new());
    let engine = self::engine(&world);
    let supervisor = create_supervisor(engine.clone());
    let error = supervisor.run_signal(&telegram(2, "hi")).await.unwrap_err().to_string();
    assert!(error.contains("Unexpected LLM call"), "{error}");
    supervisor.shutdown().await;
    // cancelled: the signal ends without a report.
    let world = self::parts(save_then_answer("unused"));
    let engine = self::engine(&world);
    let supervisor = create_supervisor(engine.clone());
    supervisor.run_signal(&telegram(3, ASKED)).await.unwrap();
    let primary = children(&engine, "signal:3").await.pop().unwrap();
    engine.runtime.cancel(&primary).await.unwrap();
    let ended = until_ended(&engine, "signal:3").await;
    assert_eq!(ended, agent::runs::Status::Completed(Value::Null));
    assert!(engine.runtime.get("recovery:3").await.unwrap().is_none(), "no report");
    supervisor.shutdown().await;
}

// ── 14. an exhausted budget still gets its report ───────────────────────────

fn mentions(messages: &[agent::providers::Message], text: &str) -> bool {
    agent::generation::messages_json(messages).to_string().contains(text)
}

// A model that keeps working (a local read every turn) until it is asked
// for the failure report, which it sends once and ends.
fn busy_until_reporting(n: usize) -> Vec<Turn> {
    (0..n)
        .map(|_| {
            turn(|p| {
                if !mentions(&p.messages, "Report the failure only") {
                    call("working_memory_list", json!({}))
                } else if p.messages.last().is_some_and(|m| m["role"] == "tool") {
                    answer("reported")
                } else {
                    call("send_telegram_message", json!({"chatId":CHAT,"text":"report: it failed"}))
                }
            })
        })
        .collect()
}

fn reported(parts: &Parts) -> bool {
    parts
        .mcp
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|(n, a)| n == "send_telegram_message" && a["text"] == "report: it failed")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_primary_exhausting_its_iterations_still_gets_the_failure_reported() {
    // The primary's limit is 100 rounds; the report runs outside its tree.
    let parts = parts(busy_until_reporting(110));
    let engine = engine(&parts);
    let supervisor = create_supervisor(engine.clone());
    let error = tokio::time::timeout(Duration::from_secs(120), supervisor.run_signal(&telegram(15, "work")))
        .await
        .expect("the signal ends")
        .unwrap_err()
        .to_string();
    assert!(error.contains("exceeded maxIterations=100"), "{error}");
    let report = engine.runtime.get("recovery:15").await.unwrap().expect("the report runs");
    assert!(matches!(report.outcome, Some(agent::runs::Status::Completed(_))), "{:?}", report.outcome);
    assert!(reported(&parts), "the user got the report");
    supervisor.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_exhausting_the_trees_budget_still_gets_the_failure_reported() {
    let mut turns = vec![turn(|_| call("invoke_sub_agent", json!({"prompt":"loop forever"})))];
    turns.extend(busy_until_reporting(20));
    let parts = parts(turns);
    let mut config = agent::runs::config();
    config.limits.max_jumps = 6;
    let engine = engine_with(&parts, config);
    let supervisor = create_supervisor(engine.clone());
    let error = supervisor.run_signal(&telegram(14, "work")).await.unwrap_err().to_string();
    // The engine's jump budget ended the tree.
    assert!(error.contains("jumps in this run tree"), "{error}");
    let report = engine.runtime.get("recovery:14").await.unwrap().expect("the report runs outside the tree");
    assert!(report.outcome.is_some(), "the report ran to its end");
    assert!(reported(&parts), "the user got the report");
    supervisor.shutdown().await;
}

// ── 15. the registry is a superset; nothing resumes before MCP answers ──────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tool_mcp_no_longer_offers_answers_not_executed_and_runs_wait_for_the_worker() {
    let parts = parts(Vec::new());
    parts.db.snapshot_tools(&[("gone_tool".into(), false)]).unwrap();
    let engine = engine(&parts);
    agent::runs::boot(&engine).await.unwrap();
    let yaml = "document: { dsl: '1.0.4', namespace: agent, name: probe, version: '1.0.0' }\ndo:\n  - go: { call: gone_tool, with: {} }\n";
    let version = engine.runtime.save("probe", yaml).await.unwrap().version;
    engine.runtime.activate("probe", version).await.unwrap();
    engine.runtime.start("probe-1", "probe", json!({})).await.unwrap();
    // Not started: no worker drives anything yet.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(engine.runtime.get("probe-1").await.unwrap().unwrap().ended_ms.is_none());
    engine.ready().await.unwrap();
    let ended = until_ended(&engine, "probe-1").await;
    let agent::runs::Status::Faulted(error) = ended else { panic!("{ended:?}") };
    assert_eq!(error.type_, ouro_runtime::CALL_NOT_EXECUTED, "{error:?}");
}

// ── 16. the reconciler records what the observer missed ─────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_reconciler_records_ended_runs_closes_orphans_and_is_idempotent() {
    use agent::runs::RunTrace;
    let parts = parts(Vec::new());
    let engine = engine(&parts);
    agent::runs::boot(&engine).await.unwrap();
    let yaml = "document: { dsl: '1.0.4', namespace: agent, name: plain, version: '1.0.0' }\ndo:\n  - a: { set: { ok: true } }\n";
    let version = engine.runtime.save("plain", yaml).await.unwrap().version;
    engine.runtime.activate("plain", version).await.unwrap();
    let tracer = agent::local_recorder::local_recorder_tracer(Arc::new(parts.db.clone()));
    let trace = agent::tracing::Tracer::trace(
        &tracer,
        agent::tracing::TraceStart { id: "t16".into(), name: "signal:test".into(), ..Default::default() },
    );
    let anchor = trace.anchor().unwrap();
    trace.end();
    let row = |run: &str, created_ms: i64| RunTrace {
        run_id: run.into(),
        trace_id: anchor.trace_id.clone(),
        session_id: None,
        parent_span_id: Some(anchor.parent_span_id.clone()),
        recorded: false,
        created_ms,
    };
    parts.db.insert_run_trace(&row("plain-1", agent::runs::now_ms())).unwrap();
    parts.db.insert_run_trace(&row("never-started", agent::runs::now_ms() - 11 * 60_000)).unwrap();
    // Driven without the background: no observer event reaches the agent.
    engine.runtime.start("plain-1", "plain", json!({})).await.unwrap();
    engine.runtime.drive("plain-1", &tokio_util::sync::CancellationToken::new()).await.unwrap();
    assert!(!parts.db.run_trace("plain-1").unwrap().unwrap().recorded);
    let later = agent::runs::now_ms() + agent::runs::RECONCILE_LAG_MS + 1_000;
    assert_eq!(agent::runs::reconcile_once(&engine, later).await.unwrap(), 2);
    assert!(parts.db.run_trace("plain-1").unwrap().unwrap().recorded);
    assert!(parts.db.run_trace("never-started").unwrap().unwrap().recorded);
    let stored = agent::db::TraceStore::get_trace(&parts.db, &anchor.trace_id).unwrap().unwrap().0;
    assert_eq!(stored.output, json!({"ok": true}), "the root's final output");
    // Overlapping windows change nothing twice.
    assert_eq!(agent::runs::reconcile_once(&engine, later + 1_000).await.unwrap(), 0);
}

// ── 17. no live loop outlives its run ───────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_live_loops_end_with_the_signal_and_its_workers() {
    let parts = parts(vec![
        turn(|_| call("invoke_sub_agent", json!({"prompt":"sub task"}))),
        turn(|_| answer("worker answer")),
        turn(|_| answer("done")),
    ]);
    let engine = engine(&parts);
    let supervisor = create_supervisor(engine.clone());
    assert_eq!(supervisor.run_signal(&telegram(17, "delegate")).await.unwrap(), "done");
    assert!(engine.loop_ids().await.is_empty(), "{:?}", engine.loop_ids().await);
    supervisor.shutdown().await;
}

// ── 18. boot: built-ins reused by digest, an operator's version kept ────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boot_reuses_built_ins_by_digest_and_keeps_an_operators_active_version() {
    let parts = parts(Vec::new());
    let engine = engine(&parts);
    agent::runs::boot(&engine).await.unwrap();
    agent::runs::boot(&engine).await.unwrap();
    let loops = engine.runtime.definitions("agent-loop").await.unwrap();
    assert_eq!(loops.versions.len(), 1, "the built-in is reused by its digest");
    // An operator activates a version of their own: boot leaves it active.
    let own = agent::agent::AGENT_LOOP.replace("General ReAct loop", "An operator loop");
    let version = engine.runtime.save("agent-loop", &own).await.unwrap().version;
    engine.runtime.activate("agent-loop", version).await.unwrap();
    let restarted = self::engine(&parts);
    agent::runs::boot(&restarted).await.unwrap();
    assert_eq!(restarted.runtime.definitions("agent-loop").await.unwrap().active, Some(version));
}
