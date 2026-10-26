// Confirmations on the engine (docs/designs/03-runs-on-the-engine.md;
// invariants 1, 2, 4, 5 and 6 of design 01): the confirmation properties of
// the old run store, rewritten on engine runs. Real agent-loop and guard runs
// with scripted models and Jev; the calls MCP saw, the cards, the events a
// guard run consumed and its stored outcome are the evidence, never a YAML
// claim. Presses arrive as `telegram_button` signals through the supervisor,
// as in production; a restart is a new engine on the same stores.
mod support;
use agent::{
    agent::{AgentLoop, ConfirmTarget, Engine, EngineDeps, LoopOptions, Providers, create_engine_on},
    confirmation::{Consumed, Pending, Store},
    db::AgentDb,
    guard,
    mcp::Mcp,
    providers::{Message, ToolCall, ToolDef, assistant},
    supervisor::{PendingSignal, Supervisor, create_supervisor},
    tracing::null_scope,
};
use async_trait::async_trait;
use proptest::{
    prelude::*,
    test_runner::{Config, TestRunner},
};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use support::{AnySkill, GuardApi, NoCodex, Scripted, calls, properties::Witnesses};
use tokio_util::sync::CancellationToken;

// ── the world ───────────────────────────────────────────────────────────────

#[derive(Default)]
struct World {
    calls: Mutex<Vec<(String, Value)>>,
    // The process dies inside the next call of this tool, after it reached
    // the world: the token is cancelled and the call never answers.
    crash: Mutex<Option<(&'static str, CancellationToken)>>,
}

#[async_trait]
impl Mcp for World {
    fn tools(&self) -> Vec<ToolDef> {
        ["write", "local_read", "send_telegram_message", "edit_telegram_message"]
            .into_iter()
            .map(|s| ToolDef::new(s, s, json!({"type":"object"})))
            .collect()
    }
    fn read_only(&self, name: &str) -> bool {
        name == "local_read"
    }
    fn open_world(&self, name: &str) -> bool {
        name != "local_read"
    }
    async fn call_tool(&self, name: &str, args: Map<String, Value>, _: &CancellationToken) -> anyhow::Result<String> {
        let result = {
            let mut calls = self.calls.lock().unwrap();
            calls.push((name.into(), json!(args)));
            if name == "send_telegram_message" {
                json!({"messageId": 700 + calls.iter().filter(|(n, _)| n == "send_telegram_message").count()})
            } else {
                json!({"ok": true})
            }
            .to_string()
        };
        let crash = {
            let mut crash = self.crash.lock().unwrap();
            if crash.as_ref().is_some_and(|(tool, _)| *tool == name) { crash.take() } else { None }
        };
        if let Some((_, died)) = crash {
            died.cancel();
            std::future::pending::<()>().await;
        }
        Ok(result)
    }
}

impl World {
    fn cards(&self) -> Vec<Value> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(n, a)| n == "send_telegram_message" && a.get("buttons").is_some())
            .map(|(_, a)| a.clone())
            .collect()
    }
    fn writes(&self) -> Vec<Value> {
        self.calls.lock().unwrap().iter().filter(|(n, _)| n == "write").map(|(_, a)| a.clone()).collect()
    }
    fn called(&self, tool: &str) -> bool {
        self.calls.lock().unwrap().iter().any(|(n, _)| n == tool)
    }
}

// The engine's time: the system clock plus an offset a test moves forward
// (a 24-hour confirmation expiring).
#[derive(Default)]
struct Clock(AtomicI64);

impl ouro_runtime::durable::Clock for Clock {
    fn now_ms(&self) -> i64 {
        chrono::Utc::now().timestamp_millis() + self.0.load(Ordering::SeqCst)
    }
}

// What outlives an engine: its stores, the world and the model.
#[derive(Clone)]
struct Parts {
    store: Arc<dyn ouro_runtime::durable::RunStore>,
    dry_store: Arc<dyn ouro_runtime::durable::RunStore>,
    db: AgentDb,
    world: Arc<World>,
    model: Arc<Scripted>,
    api: Arc<GuardApi>,
    clock: Arc<Clock>,
}

fn parts(script: Vec<Message>, label: &str) -> Parts {
    let (store, dry_store) = agent::runs::memory_store_with_view();
    let api = Arc::new(GuardApi::default());
    *api.label.lock().unwrap() = Some(label.into());
    Parts {
        store,
        dry_store,
        db: AgentDb::open(":memory:").unwrap(),
        world: Arc::new(World::default()),
        model: Arc::new(Scripted::new(script)),
        api,
        clock: Arc::new(Clock::default()),
    }
}

fn engine(p: &Parts) -> Arc<Engine> {
    let provider: Arc<dyn agent::providers::ChatProvider> = p.model.clone();
    create_engine_on(
        EngineDeps {
            providers: Providers {
                openai: provider.clone(),
                gemini: provider.clone(),
                deepseek: provider.clone(),
                openrouter: Some(provider),
            },
            mcp: p.world.clone(),
            presets: agent::models::default_presets(),
            skills: Vec::new(),
            skill_store: Arc::new(AnySkill),
            memory: Arc::new(p.db.clone()),
            tracer: Arc::new(agent::tracing::NullTracer),
            codex: Arc::new(NoCodex),
            decisions: Some(agent::decisions::decision_client("test".into(), p.api.clone())),
            schedule: Arc::new(agent::schedule::MemorySchedule::default()),
            env: agent::session::EnvConfig { user_email: None, telegram_chat_id: Some("42".into()) },
            db: p.db.clone(),
            store: p.store.clone(),
            dry_store: p.dry_store.clone(),
        },
        agent::runs::config(),
        p.clock.clone(),
    )
}

const LOOP: &str = "primary";
const TASK: &str = "Perform the requested operation in conversation 42";

async fn start(engine: &Arc<Engine>) -> Arc<AgentLoop> {
    let mut options = LoopOptions::new(LOOP, support::task_context(LOOP));
    options.skills = vec!["s".into()];
    options.include_engine_skills = false;
    options.confirm = Some(ConfirmTarget { chat_id: "42".into(), message_thread_id: Some(7) });
    engine.start_loop(options).await.unwrap()
}

async fn until<T>(what: &str, limit: Duration, mut probe: impl AsyncFnMut() -> Option<T>) -> T {
    let deadline = tokio::time::Instant::now() + limit;
    loop {
        if let Some(found) = probe().await {
            return found;
        }
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// The loop run's end, from the store.
async fn ended(engine: &Engine) -> ouro_runtime::ows::run::Status {
    until("the loop's end", Duration::from_secs(60), async || {
        engine.runtime.get(LOOP).await.unwrap().and_then(|r| r.outcome)
    })
    .await
}

// A card the guard sent: its tokens, guard run, call key and message.
#[derive(Clone, Debug)]
struct Card {
    yes: String,
    no: String,
    run: String,
    key: String,
    message: i64,
}

fn cards(db: &AgentDb) -> Vec<Card> {
    let conn = db.conn();
    let mut statement = conn
        .prepare(
            "SELECT yes_type, no_type, run_id, call_key, message_id FROM confirmations
             WHERE message_id IS NOT NULL ORDER BY call_key",
        )
        .unwrap();
    statement
        .query_map([], |r| {
            Ok(Card { yes: r.get(0)?, no: r.get(1)?, run: r.get(2)?, key: r.get(3)?, message: r.get(4)? })
        })
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

// A press of `card` arriving as signal `id`: the confirm or the decline
// button, from the card's own message (`foreign` 0) or another chat (1),
// thread (2) or message (3).
async fn press(supervisor: &Arc<Supervisor>, id: i64, card: &Card, decline: bool, foreign: u8) -> String {
    let token = if decline { &card.no } else { &card.yes };
    let content = json!({
        "chatId": if foreign == 1 { 43 } else { 42 },
        "messageThreadId": if foreign == 2 { 8 } else { 7 },
        "messageId": if foreign == 3 { 9999 } else { card.message },
        "data": token,
        "text": if decline { "No" } else { "Yes" },
    });
    let signal = PendingSignal {
        id,
        source: "telegram_button".into(),
        content: content.to_string(),
        env_context: None,
        created_at: "2026-09-06 12:00:00".into(),
        payload: None,
    };
    supervisor.run_signal(&signal).await.unwrap()
}

// Every write the world saw has its own consumed confirmation: a card of a
// guard run that consumed that card's confirm press from the card's own chat,
// message and thread; whose stored outcome allows exactly that call; and
// whose verdict authorized one invocation. Nothing is written twice.
async fn audit(engine: &Engine, db: &AgentDb, world: &World) {
    let mut unique = BTreeMap::new();
    for args in world.writes() {
        assert_eq!(unique.insert(args.to_string(), ()), None, "a held call was dispatched twice: {args}");
        let call = ToolCall { id: String::new(), name: "write".into(), arguments: args.to_string() };
        let hash = guard::key(&call)[1..].to_owned();
        let rows: Vec<(String, String, String, String, Option<i64>, i64)> = {
            let conn = db.conn();
            let mut statement = conn
                .prepare(
                    "SELECT run_id, call_key, yes_type, chat_id, thread_id, message_id FROM confirmations
                     WHERE outcome = 'confirmed' AND call_key LIKE '%' || ?1",
                )
                .unwrap();
            statement
                .query_map([&hash], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?)))
                .unwrap()
                .map(Result::unwrap)
                .collect()
        };
        let mut authorized = false;
        for (run, key, yes, chat, thread, message) in rows {
            let pressed = engine.runtime.records(&run).await.unwrap().values().any(|record| match record {
                ouro_runtime::ows::durable::Record::Event { event } => {
                    let data = &event["data"];
                    event["type"] == yes
                        && data["chatId"].to_string().trim_matches('"') == chat
                        && data["messageId"].as_i64() == Some(message)
                        && data["messageThreadId"].as_i64() == thread
                }
                _ => false,
            });
            let allowed = match engine.runtime.get(&run).await.unwrap().and_then(|r| r.outcome) {
                Some(ouro_runtime::ows::run::Status::Completed(v)) => v["verdicts"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|pair| pair[0] == key.as_str() && pair[1]["label"] == "allow"),
                _ => false,
            };
            let uses: i64 = db
                .conn()
                .query_row(
                    "SELECT count(*) FROM guard_verdict_uses WHERE child_run = ?1 AND call_key = ?2",
                    rusqlite::params![run, key],
                    |r| r.get(0),
                )
                .unwrap();
            authorized |= pressed && allowed && uses == 1;
        }
        assert!(authorized, "a write without its own consumed confirmation: {args}");
    }
}

fn transcript(model: &Scripted) -> String {
    model
        .seen
        .lock()
        .unwrap()
        .iter()
        .map(|s| serde_json::to_string(&s.messages).unwrap())
        .collect::<Vec<_>>()
        .join("\n")
}

// ── 1. confirmation rounds ──────────────────────────────────────────────────

// One round of writes the guard holds for confirmation, beside a local read
// that is not held. `decisions[i]` answers the card of write `i`: 0 confirm,
// 1 decline, 2 expire (24 h without a press), 3–5 a confirm press from
// another chat, thread or message. `restart`: the process dies between the
// cards and the presses; `worker`: the round is a worker's. `noise`: presses
// of tokens no card holds.
async fn confirmation_case(decisions: Vec<u8>, restart: bool, worker: bool, noise: Vec<u8>, w: &Witnesses) {
    assert!(restart || !decisions.contains(&2), "expiry is exercised across a restart");
    let original: Vec<Value> =
        decisions.iter().enumerate().map(|(i, _)| json!({"operation":i,"payload":format!("original-{i}-λ")})).collect();
    let mut mixed: Vec<_> = original.iter().map(|a| ("write", a.clone())).collect();
    mixed.push(("local_read", json!({"q": noise.len()})));
    let mut script = Vec::new();
    if worker {
        script.push(calls(&[("invoke_sub_agent", json!({"prompt":"perform the operation","skills":["s"]}))]));
    }
    script.push(calls(&mixed));
    script.push(assistant(if worker { "worker answer" } else { "done" }));
    if worker {
        script.push(assistant("parent answer"));
    }
    let p = parts(script, "needs_confirmation");
    let mut engine = engine(&p);
    let live = start(&engine).await;
    let mut sent = Some(tokio::spawn(live.send(TASK.into())));
    let card = until("the cards", Duration::from_secs(30), async || {
        let card = cards(&p.db);
        (card.len() == decisions.len()).then_some(card)
    })
    .await;
    assert!(p.world.writes().is_empty(), "nothing is written before a press");
    until("the unguarded read", Duration::from_secs(10), async || p.world.called("local_read").then_some(())).await;
    w.hit("unguarded call before press");
    if restart {
        // Lose the loop, its guard and every call in flight between the
        // cards and the presses, once every card's send is committed (a
        // send cut in flight is never resent: its call resolves unverified,
        // engine_runs.rs, invariant 6).
        until("the cards' sends committed", Duration::from_secs(30), async || {
            let calls = engine.runtime.calls(&card[0].run).await.unwrap();
            let sends = calls.iter().filter(|c| c.function == "confirm_request").collect::<Vec<_>>();
            (sends.len() == decisions.len() && sends.iter().all(|c| c.state == ouro_runtime::CallStatus::Completed))
                .then_some(())
        })
        .await;
        engine.shutdown().await;
        assert!(sent.take().unwrap().await.unwrap().is_err(), "the cut send answers nothing");
        engine = self::engine(&p);
        engine.ready().await.unwrap();
        w.hit("crash between card and press");
    } else {
        w.hit("press during hold");
    }
    let supervisor = create_supervisor(engine.clone());
    let mut signal = 100;
    for n in noise {
        let stray = Card { yes: format!("cf:stray{n}"), no: format!("cf:stray{n}:no"), ..card[0].clone() };
        signal += 1;
        // A token no card holds, sent to a guard round: it waits in the
        // mailbox, no listen takes it, and it authorizes nothing.
        let taken = engine
            .runtime
            .deliver(
                json!({"source":"telegram","id":format!("stray:{signal}"),"type":stray.yes,"data":{}}),
                Some(&card[0].run),
            )
            .await
            .unwrap();
        assert_eq!(taken.as_deref(), Some(card[0].run.as_str()));
        audit(&engine, &p.db, &p.world).await;
    }
    // Presses in generated order; a card's second press changes nothing.
    for (i, decision) in decisions.iter().enumerate().rev() {
        if *decision == 2 {
            continue;
        }
        let foreign = if *decision >= 3 { *decision - 2 } else { 0 };
        signal += 1;
        assert_eq!(press(&supervisor, signal, &card[i], *decision == 1, foreign).await, "Button delivered");
        // The same press again, straight to the guard round (a press of an
        // answered card goes to the agent instead, design 03 decision 8).
        let token = if *decision == 1 { &card[i].no } else { &card[i].yes };
        let again = json!({"source":"telegram","id":format!("duplicate:{signal}"),"type":token,
            "data":{"chatId":42,"messageThreadId":7,"messageId":card[i].message,"data":token}});
        engine.runtime.deliver(again, Some(&card[i].run)).await.unwrap();
        w.hit("duplicate press");
        audit(&engine, &p.db, &p.world).await;
    }
    if decisions.contains(&2) {
        // The pressed cards resolve first; then 24 hours pass.
        until("the pressed cards' answers", Duration::from_secs(30), async || {
            decisions
                .iter()
                .zip(&card)
                .filter(|(d, _)| **d != 2)
                .all(|(_, c)| p.db.by_token(&c.yes).unwrap().is_some_and(|(_, o)| o.is_some()))
                .then_some(())
        })
        .await;
        for (c, _) in card.iter().zip(&decisions).filter(|(_, d)| **d == 2) {
            p.db.conn()
                .execute(
                    "UPDATE confirmations SET created_at_ms = created_at_ms - 86400001 WHERE yes_type = ?1",
                    [&c.yes],
                )
                .unwrap();
        }
        p.clock.0.fetch_add(86_400_000 + 60_000, Ordering::SeqCst);
    }
    let outcome = ended(&engine).await;
    if let Some(sent) = sent {
        assert!(sent.await.unwrap().is_ok(), "the held round finishes in its send");
    }
    audit(&engine, &p.db, &p.world).await;
    let want = if worker { "parent answer" } else { "done" };
    assert_eq!(outcome, ouro_runtime::ows::run::Status::Completed(json!(want)));
    let expected: Vec<_> = original.iter().zip(&decisions).filter(|(_, d)| **d == 0).map(|(a, _)| a.clone()).collect();
    let mut actual = p.world.writes();
    actual.sort_by_key(|a| a["operation"].as_i64());
    assert_eq!(actual, expected, "confirmed calls ran with their original arguments");
    for (c, d) in card.iter().zip(&decisions) {
        let (_, answer) = p.db.by_token(&c.yes).unwrap().unwrap();
        let want = match d {
            0 => "confirmed",
            1 => "declined",
            2 => "expired",
            _ => "unverified",
        };
        assert_eq!(answer.as_deref(), Some(want), "card {}", c.key);
        w.hit(match d {
            0 => "confirmed and executed",
            1 => "declined",
            2 => "expired",
            _ => "foreign card rejected",
        });
    }
    let said = transcript(&p.model);
    if decisions.contains(&1) {
        assert!(said.contains("declined by the user"));
    }
    if decisions.contains(&2) {
        assert!(said.contains("not confirmed within 24 h"));
    }
    if decisions.iter().any(|d| *d >= 3) {
        assert!(said.contains("no consumed confirmation matches this exact call"));
    }
    if worker {
        assert!(said.contains("worker answer"));
        w.hit("worker answer joined");
    }
    assert_eq!(p.world.cards().len(), decisions.len(), "no card is sent again");
    supervisor.shutdown().await;
}

fn confirmation_witnesses() -> Witnesses {
    Witnesses::new(&[
        "crash between card and press",
        "press during hold",
        "duplicate press",
        "confirmed and executed",
        "declined",
        "expired",
        "foreign card rejected",
        "worker answer joined",
        "unguarded call before press",
    ])
}

// Engine runs take the worker's ticks: fewer cases than the store-only
// properties by default; PROPERTY_RUNS raises them.
fn cases() -> u32 {
    std::env::var("PROPERTY_RUNS").ok().and_then(|v| v.parse().ok()).unwrap_or(6)
}

#[test]
fn confirmation_rounds_finish_with_exact_original_calls_or_explicit_refusals() {
    let w = confirmation_witnesses();
    TestRunner::new(Config { cases: cases(), ..Config::default() })
        .run(
            &(
                any::<bool>(),
                any::<bool>(),
                prop::collection::vec(0u8..=5, 1..=4),
                prop::collection::vec(any::<u8>(), 0..=3),
            ),
            |(restart, worker, mut decisions, noise)| {
                if !restart {
                    decisions.retain(|d| *d != 2);
                    if decisions.is_empty() {
                        decisions.push(0);
                    }
                }
                let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
                rt.block_on(async {
                    confirmation_case(decisions, restart, worker, noise, &w).await;
                    confirmation_case(vec![0, 1, 3, 4, 5], false, false, vec![], &w).await;
                    confirmation_case(vec![2, 0], true, true, vec![1], &w).await;
                });
                Ok(())
            },
        )
        .unwrap();
    w.finish();
}

// ── 2. after the guard suspended ────────────────────────────────────────────

// Two confirmed cards of a worker's round, pressed only after the guard's
// in-memory hold ended and it suspended: the parent, the worker and the
// guard are rebuilt from the store, and both writes run once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_confirmed_cards_rebuild_the_same_loop_after_suspension() {
    let script = vec![
        calls(&[("invoke_sub_agent", json!({"prompt":"perform the operation","skills":["s"]}))]),
        calls(&[("write", json!({"operation":0})), ("write", json!({"operation":1}))]),
        assistant("worker answer"),
        assistant("parent answer"),
    ];
    let p = parts(script, "needs_confirmation");
    let engine = engine(&p);
    let live = start(&engine).await;
    let sent = tokio::spawn(live.send(TASK.into()));
    let card = until("the cards", Duration::from_secs(30), async || {
        let card = cards(&p.db);
        (card.len() == 2).then_some(card)
    })
    .await;
    // The guard's hold for a press is five seconds; then it suspends.
    until("the guard's suspension", Duration::from_secs(30), async || {
        let run = engine.runtime.get(&card[0].run).await.unwrap()?;
        (run.state == ouro_runtime::durable::RunState::Waiting).then_some(())
    })
    .await;
    let supervisor = create_supervisor(engine.clone());
    for (n, c) in card.iter().enumerate() {
        assert_eq!(press(&supervisor, 200 + n as i64, c, false, 0).await, "Button delivered");
    }
    assert_eq!(ended(&engine).await, ouro_runtime::ows::run::Status::Completed(json!("parent answer")));
    assert_eq!(sent.await.unwrap().unwrap(), "parent answer");
    let mut writes = p.world.writes();
    writes.sort_by_key(|a| a["operation"].as_i64());
    assert_eq!(writes, vec![json!({"operation":0}), json!({"operation":1})]);
    audit(&engine, &p.db, &p.world).await;
    assert!(transcript(&p.model).contains("worker answer"));
    supervisor.shutdown().await;
}

// ── 3. a local read does not wait for a press ───────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_local_read_in_a_mixed_round_runs_before_a_confirmation_press() {
    let script =
        vec![calls(&[("write", json!({"held":true})), ("local_read", json!({"q":"ready"}))]), assistant("done")];
    let p = parts(script, "needs_confirmation");
    let engine = engine(&p);
    let live = start(&engine).await;
    let _sent = tokio::spawn(live.send(TASK.into()));
    until("the card", Duration::from_secs(30), async || (cards(&p.db).len() == 1).then_some(())).await;
    until("the unguarded read", Duration::from_secs(10), async || p.world.called("local_read").then_some(())).await;
    assert!(p.world.writes().is_empty());
    engine.shutdown().await;
}

// ── 4. reviewed rounds ──────────────────────────────────────────────────────

// A round's writes run only on the verdicts the round's own guard child
// stored: all of them when it allows, none when it blocks; the local read
// runs either way, with one decision request, and the replies keep the
// round's order.
#[test]
fn reviewed_rounds_dispatch_only_the_owned_allow_verdicts_and_keep_reply_order() {
    let w = Witnesses::new(&["allow executed", "block refused", "unguarded progressed", "loop finished"]);
    TestRunner::new(Config { cases: cases(), ..Config::default() })
        .run(&prop::collection::vec(".{0,20}", 1..=4), |texts| {
            let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
            rt.block_on(async {
                for label in ["allow", "misaligned"] {
                    let args: Vec<_> = texts.iter().enumerate().map(|(i, t)| json!({"i":i,"text":t})).collect();
                    let mut round: Vec<_> = args.iter().map(|a| ("write", a.clone())).collect();
                    round.insert(0, ("local_read", json!({"q":"ready"})));
                    let p = parts(vec![calls(&round), assistant("done")], label);
                    let engine = engine(&p);
                    let out = start(&engine).await.send(TASK.into()).await.unwrap();
                    assert_eq!(out, "done");
                    let mut writes = p.world.writes();
                    writes.sort_by_key(|v| v["i"].as_i64());
                    assert_eq!(writes, if label == "allow" { args } else { vec![] });
                    assert!(p.world.cards().is_empty());
                    assert_eq!(p.api.requests.lock().unwrap().len(), 1, "one review for the round");
                    assert!(p.world.called("local_read"));
                    {
                        let seen = p.model.seen.lock().unwrap();
                        let replies: Vec<_> = seen
                            .last()
                            .unwrap()
                            .messages
                            .iter()
                            .filter(|m| m["role"] == "tool")
                            .map(|m| m["tool_call_id"].as_str().unwrap().to_owned())
                            .collect();
                        assert_eq!(replies, (0..round.len()).map(|i| format!("call-{i}")).collect::<Vec<_>>());
                    }
                    // The verdicts are the guard child's own stored outcome.
                    let filter = agent::runs::Runs { parent: Some(LOOP.into()), ..Default::default() };
                    let guards = engine.control.list(&filter, None, 10).await.unwrap().items;
                    let guard = guards.iter().find(|r| r.admission.name == "guard").expect("the round's guard child");
                    let Some(ouro_runtime::ows::run::Status::Completed(outcome)) = &guard.outcome else {
                        panic!("the guard did not complete: {:?}", guard.outcome)
                    };
                    assert!(outcome["verdicts"].as_array().unwrap().iter().all(|v| v[1]["label"] == label));
                    w.hit(if label == "allow" { "allow executed" } else { "block refused" });
                    w.hit("unguarded progressed");
                    w.hit("loop finished");
                    engine.shutdown().await;
                }
            });
            Ok(())
        })
        .unwrap();
    w.finish();
}

// ── 5. tokens and their consumption ─────────────────────────────────────────

// The store's resolution of a card (`Store::resolve`) takes only an event
// the guard run consumed: a token of another run's card, a press from
// another message, a second resolution and an early "expired" claim are all
// unverified; a real expiry (24 h after the card) is expired.
#[test]
fn confirmation_tokens_are_bound_to_one_run_call_card_and_consumption() {
    let w = Witnesses::new(&[
        "cross-run token",
        "confirm spent once",
        "decline spent once",
        "foreign card",
        "early forged expiry",
        "real expiry",
    ]);
    TestRunner::new(Config { cases: support::properties::runs(), ..Config::default() })
        .run(&(1i64..1_000_000, any::<bool>()), |(chat, no)| {
            let db = AgentDb::open(":memory:").unwrap();
            for id in ["a", "b"] {
                for n in 0..6 {
                    let key = format!("key{n}");
                    let p = Pending {
                        run: id.into(),
                        loop_run: Some(format!("{id}-loop")),
                        key: key.clone(),
                        yes: format!("cf:{id}:{n}:yes"),
                        no: format!("cf:{id}:{n}:no"),
                        target: ConfirmTarget { chat_id: chat.to_string(), message_thread_id: Some(7) },
                        message: None,
                        card: "exact arguments".into(),
                    };
                    db.insert(&p).unwrap();
                    db.sent(id, &key, n).unwrap();
                }
            }
            let consumed = |type_: &str, message: i64| Consumed {
                type_: type_.into(),
                data: json!({"chatId": chat, "messageThreadId": 7, "messageId": message}),
            };
            // b's press, consumed by b, is no evidence for a.
            let b_press = [consumed("cf:b:0:yes", 0)];
            assert_eq!(db.resolve("a", "key0", "confirmed", &b_press).unwrap(), "unverified");
            assert_eq!(db.resolve("b", "key0", "confirmed", &b_press).unwrap(), "confirmed");
            w.hit("cross-run token");
            for (n, foreign) in [(1, false), (2, true)] {
                let type_ = format!("cf:a:{n}:{}", if no { "no" } else { "yes" });
                let events = [consumed(&type_, if foreign { 99 } else { n })];
                let key = format!("key{n}");
                let want = if foreign {
                    "unverified"
                } else if no {
                    "declined"
                } else {
                    "confirmed"
                };
                assert_eq!(db.resolve("a", &key, "confirmed", &events).unwrap(), want);
                assert_eq!(db.resolve("a", &key, "confirmed", &events).unwrap(), "unverified", "spent once");
                w.hit(if foreign {
                    "foreign card"
                } else if no {
                    "decline spent once"
                } else {
                    "confirm spent once"
                });
            }
            assert_eq!(db.resolve("a", "key3", "expired", &[]).unwrap(), "unverified");
            w.hit("early forged expiry");
            db.conn()
                .execute(
                    "UPDATE confirmations SET created_at_ms = created_at_ms - 86400001 WHERE run_id = 'a' AND call_key = 'key4'",
                    [],
                )
                .unwrap();
            assert_eq!(db.resolve("a", "key4", "expired", &[]).unwrap(), "expired");
            w.hit("real expiry");
            Ok(())
        })
        .unwrap();
    w.finish();
}

// ── 6. a crash around a resolution ──────────────────────────────────────────

// The process dies (0) while the guard edits the answered card, after the
// confirmation was resolved; (1) after that edit answered, before the guard
// completed; (2) inside the confirmed write itself. The next process
// finishes the round on the same verdict: the write runs at most once, the
// card is never sent again, and a write cut in flight reaches the model as
// an outcome that may have happened.
async fn resolution_crash(args: Value, mode: u8, w: &Witnesses) {
    let p = parts(vec![calls(&[("write", args.clone())]), assistant("done")], "needs_confirmation");
    let first = engine(&p);
    let live = start(&first).await;
    let sent = tokio::spawn(live.send(TASK.into()));
    let card = until("the card", Duration::from_secs(30), async || {
        let card = cards(&p.db);
        (card.len() == 1).then_some(card)
    })
    .await;
    let died = CancellationToken::new();
    *p.world.crash.lock().unwrap() = Some((if mode == 2 { "write" } else { "edit_telegram_message" }, died.clone()));
    if mode == 1 {
        // Dies just after the edit answered: the guard may or may not have
        // committed its completion.
        *p.world.crash.lock().unwrap() = None;
    }
    let supervisor = create_supervisor(first.clone());
    assert_eq!(press(&supervisor, 300, &card[0], false, 0).await, "Button delivered");
    if mode == 1 {
        until("the card's edit", Duration::from_secs(30), async || {
            p.world.called("edit_telegram_message").then_some(())
        })
        .await;
    } else {
        tokio::time::timeout(Duration::from_secs(30), died.cancelled()).await.expect("the crash point is reached");
    }
    supervisor.shutdown().await;
    let _ = sent.await;
    assert_eq!(p.db.by_token(&card[0].yes).unwrap().unwrap().1.as_deref(), Some("confirmed"));
    audit(&first, &p.db, &p.world).await;
    let next = engine(&p);
    next.ready().await.unwrap();
    assert_eq!(ended(&next).await, ouro_runtime::ows::run::Status::Completed(json!("done")));
    audit(&next, &p.db, &p.world).await;
    assert_eq!(p.world.writes(), vec![args], "the confirmed write, once");
    assert_eq!(p.world.cards().len(), 1, "no card is sent again");
    if mode == 2 {
        assert!(transcript(&p.model).contains("outcome unknown: write"));
    }
    w.hit(match mode {
        0 => "crash after consumed confirm",
        1 => "crash at guard completion",
        _ => "crash after confirmed effect",
    });
    next.shutdown().await;
}

#[test]
fn confirmation_resolution_crashes_preserve_authority_and_at_most_once_execution() {
    let w =
        Witnesses::new(&["crash after consumed confirm", "crash at guard completion", "crash after confirmed effect"]);
    TestRunner::new(Config { cases: cases(), ..Config::default() })
        .run(&(".{0,30}", any::<u16>()), |(text, n)| {
            let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
            rt.block_on(async {
                for mode in 0..3 {
                    resolution_crash(json!({"text":text,"n":n}), mode, &w).await;
                }
            });
            Ok(())
        })
        .unwrap();
    w.finish();
}

// ── 7. forged authority ─────────────────────────────────────────────────────

// A workflow that is not a loop the host started (no host data) calls the
// loop's functions with forged evidence: a "confirmed" outcome, another
// parent's guard child, an "allow" verdict. Nothing is written, no card is
// sent: the functions serve only runs whose host data names them.
async fn forged_case(args: Value, mode: u8, w: &Witnesses) {
    let p = parts(vec![assistant("seed done")], "needs_confirmation");
    let engine = engine(&p);
    engine.ready().await.unwrap();
    // Another parent's guard child, whose stored outcome allows the call.
    let seeded = start(&engine).await.send(TASK.into()).await.unwrap();
    assert_eq!(seeded, "seed done");
    let call = json!({"id":"c","name":"write","arguments":args.to_string()});
    let mut with = json!({"call": call, "i": 1, "index": 0});
    match mode {
        0 => with["outcome"] = json!("confirmed"),
        1 => with["guard_run"] = json!(format!("hostchild:{}", "0".repeat(32))),
        _ => {
            with["verdict"] = json!({"label":"allow","outcome":"confirmed"});
            with["outcome"] = json!("expired");
        }
    }
    let mut tasks = Vec::new();
    for function in ["tool_once", "tool_repeat", "classify", "start_guard", "confirm_request", "resolve_review"] {
        tasks.push(json!({ format!("forged_{function}"): {
            "try": [{ "call": { "call": function, "with": with } }],
            "catch": { "do": [{ "refused": { "set": { "refused": true } } }] }
        }}));
    }
    let yaml = json!({"document":{"dsl":"1.0.4","namespace":"agent","name":"forger","version":"1.0.0"},"do":tasks})
        .to_string();
    let version = engine.runtime.save("forger", &yaml).await.unwrap().version;
    engine.runtime.activate("forger", version).await.unwrap();
    engine.runtime.start("forger-1", "forger", json!({})).await.unwrap();
    until("the forger's end", Duration::from_secs(30), async || {
        engine.runtime.get("forger-1").await.unwrap().and_then(|r| r.outcome)
    })
    .await;
    assert!(p.world.writes().is_empty(), "a forged call wrote");
    assert!(p.world.cards().is_empty(), "a forged call sent a card");
    for call in engine.runtime.calls("forger-1").await.unwrap() {
        assert_ne!(call.state, ouro_runtime::CallStatus::Completed, "{} served a run without host data", call.function);
    }
    audit(&engine, &p.db, &p.world).await;
    w.hit(match mode {
        0 => "forged confirmed outcome",
        1 => "another parents guard",
        _ => "forged allow verdict",
    });
    engine.shutdown().await;
}

#[test]
fn workflows_cannot_forge_authority_or_borrow_another_parents_guard() {
    let w = Witnesses::new(&["forged confirmed outcome", "another parents guard", "forged allow verdict"]);
    TestRunner::new(Config { cases: cases(), ..Config::default() })
        // Text without `${`, which a definition reads as an expression.
        .run(&("[ -#%-~]{0,40}", any::<i32>()), |(text, n)| {
            let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
            rt.block_on(async {
                for mode in 0..3 {
                    forged_case(json!({"text":text,"n":n}), mode, &w).await;
                }
            });
            Ok(())
        })
        .unwrap();
    w.finish();
}

// ── 8. the guard's judgement of replies ─────────────────────────────────────

async fn judge_case(text: String, chat: i64, variant: u8, test_confirm: bool) {
    let p = parts(vec![], "misaligned");
    let engine = engine(&p);
    let destination = if variant == 0 { chat } else { chat + 1 };
    let context = guard::Context {
        task: format!("reply in chat {chat}{}", if test_confirm { "; guard test confirm" } else { "" }),
        default_chat: Some(chat.to_string()),
        ..Default::default()
    };
    let call = ToolCall {
        id: "generated".into(),
        name: "send_telegram_message".into(),
        arguments: json!({"chatId":destination,"text":text}).to_string(),
    };
    let verdict =
        guard::judge(&engine, &context, &[call], &[], 0, &null_scope(), &CancellationToken::new()).await.unwrap();
    let requests = p.api.requests.lock().unwrap().len();
    if variant == 1 {
        assert_eq!(requests, 0);
        assert_eq!(verdict[0].1.label, "unknown_chat");
    } else {
        assert!(requests > 0 || !verdict[0].1.allowed(), "a link bypassed Jev: {text:?}");
        if test_confirm {
            assert!(!verdict[0].1.allowed());
        }
    }
    engine.shutdown().await;
}

#[test]
fn links_and_foreign_destinations_never_get_the_local_reply_exception() {
    let w = Witnesses::new(&[
        "scheme URL",
        "www URL",
        "bare domain",
        "unicode domain",
        "current plain reply",
        "foreign destination",
        "confirmation cannot allow a secret",
    ]);
    TestRunner::new(Config { cases: support::properties::runs(), ..Config::default() })
        .run(&("[a-z]{1,10}", 1i64..100_000, any::<bool>(), any::<bool>()), |(host, n, test_confirm, numeric)| {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(async {
                for (text, kind) in [
                    (format!("custom+v1://{host}.test/p"), "scheme URL"),
                    (format!("www.{host}.test"), "www URL"),
                    (format!("{host}.test"), "bare domain"),
                    (format!("{host}.рф"), "unicode domain"),
                ] {
                    judge_case(text.clone(), -n, 0, test_confirm).await;
                    w.hit(kind);
                    judge_case(text, -n, 1, test_confirm).await;
                    w.hit("foreign destination");
                }
                let p = parts(vec![], "misaligned");
                let engine = engine(&p);
                let context = guard::Context {
                    task: format!("Reply in conversation -{n}; guard test confirm"),
                    default_chat: Some(format!("-{n}")),
                    ..Default::default()
                };
                let destination = if numeric { json!(-n) } else { json!(format!("-{n}")) };
                for (text, label) in [
                    (format!("Plain answer λ {host}"), "allow"),
                    ("sk-abcdefghijklmnop1234567890".into(), "secret_pattern"),
                ] {
                    let call = ToolCall {
                        id: format!("call-{host}"),
                        name: "send_telegram_message".into(),
                        arguments: json!({"chatId":destination,"text":text}).to_string(),
                    };
                    let verdict =
                        guard::judge(&engine, &context, &[call], &[], 0, &null_scope(), &CancellationToken::new())
                            .await
                            .unwrap();
                    assert_eq!(verdict[0].1.label, label);
                    assert!(p.api.requests.lock().unwrap().is_empty());
                }
                w.hit("current plain reply");
                w.hit("confirmation cannot allow a secret");
                engine.shutdown().await;
            });
            Ok(())
        })
        .unwrap();
    w.finish();
}

#[tokio::test]
async fn a_bare_domain_in_a_current_conversation_reply_reaches_jev() {
    judge_case("example.com".into(), 42, 0, false).await;
}
