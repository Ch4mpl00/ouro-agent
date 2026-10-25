//! The real agent-loop/guard workflows run with scripted models and Jev.
//! Calls, cards, consumed events and owned guard outcomes are the evidence;
//! no YAML claim is treated as authority. Paused Tokio time orders presses,
//! hold expiration and crashes without wall-clock polling.
mod support;
use agent::{
    agent::{ConfirmTarget, Engine, LoopOptions},
    db::AgentDb,
    durable::RunStore,
    flows::{FlowResult, FlowRun, Flows, create_durable_flows},
    guard,
    mcp::Mcp,
    providers::{ToolCall, ToolDef, assistant},
    tracing::null_scope,
};
use async_trait::async_trait;
use futures::poll;
use proptest::{
    prelude::*,
    test_runner::{Config, TestRunner},
};
use serde_json::{Map, Value, json};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use support::{
    Fixture, GuardApi, Scripted, calls, fixture,
    properties::{Witnesses, runs},
};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct World {
    calls: Mutex<Vec<(String, Value)>>,
    stop_after_write: Mutex<Option<CancellationToken>>,
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
                json!({"messageId":700+calls.iter().filter(|(n,_)|n=="send_telegram_message").count()})
            } else {
                json!({"ok":true})
            }
            .to_string()
        };
        let stop = if name == "write" { self.stop_after_write.lock().unwrap().take() } else { None };
        if let Some(stop) = stop {
            stop.cancel();
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
}
fn setup(model: Arc<Scripted>, world: Arc<World>, label: &str) -> (Fixture, Arc<GuardApi>) {
    let mut f = fixture(model, &[], &[("s", "---\ntools: *\n---\nskill"), ("worker", "---\ntools: *\n---\nworker")]);
    let api = Arc::new(GuardApi::default());
    *api.label.lock().unwrap() = Some(label.into());
    let deps = &mut Arc::get_mut(&mut f.engine).unwrap().deps;
    deps.mcp = world;
    deps.decisions = Some(agent::decisions::decision_client("test".into(), api.clone()));
    (f, api)
}
async fn input(engine: &Arc<Engine>, id: &str) -> Value {
    let mut options = LoopOptions::new(id, support::task_context(id));
    options.skills = vec!["s".into()];
    options.include_engine_skills = false;
    options.confirm = Some(ConfirmTarget { chat_id: "42".into(), message_thread_id: Some(7) });
    engine.start_loop(options).await.unwrap().workflow_input("Perform the requested operation in conversation 42").await
}
fn audit(db: &AgentDb, world: &World) {
    let mut unique = BTreeMap::new();
    for args in world.writes() {
        assert_eq!(unique.insert(args.to_string(), 1), None, "held call dispatched twice");
        let conn = db.conn();
        let mut stmt=conn.prepare("SELECT c.call_key,c.outcome,g.parent_run,g.outcome FROM confirmations c JOIN workflow_runs g ON g.id=c.run_id WHERE EXISTS (SELECT 1 FROM workflow_events e WHERE e.run_id=c.run_id AND e.type=c.yes_type AND e.consumed_at_ms IS NOT NULL AND cast(json_extract(e.data,'$.chatId') AS TEXT)=c.chat_id AND json_extract(e.data,'$.messageId')=c.message_id AND json_extract(e.data,'$.messageThreadId') IS c.thread_id)").unwrap();
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Option<String>>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, Option<String>>(3)?,
                ))
            })
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>();
        let matched=rows.into_iter().any(|(key,outcome,parent,guard_outcome)| {
            let owned=conn.query_row("SELECT count(*) FROM guard_verdict_uses u JOIN workflow_runs g ON g.id=u.child_run WHERE g.parent_run=?1 AND u.call_key=?2",rusqlite::params![parent,key],|r|r.get::<_,i64>(0)).unwrap()==1;
            let allowed=guard_outcome.and_then(|s|serde_json::from_str::<owf::run::Status>(&s).ok()).is_some_and(|status| match status {
                owf::run::Status::Completed(v)=>v["verdicts"].as_array().unwrap().iter().any(|pair|pair[0]==key && pair[1]["label"]=="allow"),_=>false
            });
            outcome.as_deref()==Some("confirmed") && owned && allowed && key.ends_with(&guard::key(&ToolCall{id:String::new(),name:"write".into(),arguments:args.to_string()})[1..])
        });
        assert!(matched, "write without its own consumed confirmation: {args}");
    }
    assert_eq!(db.conn().query_row("SELECT count(*) FROM (SELECT run_id,consumed_key FROM workflow_events WHERE consumed_at_ms IS NOT NULL GROUP BY run_id,consumed_key HAVING count(*)>1)",[],|r|r.get::<_,i64>(0)).unwrap(),0);
    assert_eq!(db.conn().query_row("SELECT count(*) FROM workflow_awaits a JOIN workflow_runs r ON r.id=a.run_id WHERE r.ended_ms IS NOT NULL",[],|r|r.get::<_,i64>(0)).unwrap(),0);
}
fn press(runs: &RunStore, card: &Value, index: usize, foreign: u8) -> bool {
    let token = card["buttons"][0][index]["data"].as_str().unwrap();
    runs.deliver(token,&json!({"chatId":if foreign==1 {43} else {42},"messageThreadId":if foreign==2 {8} else {7},"messageId":if foreign==3 {9999} else {card["messageId"].as_i64().unwrap()}})).unwrap()
}

fn cards(db: &AgentDb, world: &World) -> Vec<Value> {
    let mut cards: Vec<_> = world
        .cards()
        .into_iter()
        .map(|mut card| {
            let token = card["buttons"][0][0]["data"].as_str().unwrap();
            let (message, key): (i64, String) = db
                .conn()
                .query_row("SELECT message_id,call_key FROM confirmations WHERE yes_type=?1", [token], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })
                .unwrap();
            card["messageId"] = message.into();
            card["callKey"] = key.into();
            card
        })
        .collect();
    cards.sort_by_key(|c| c["callKey"].as_str().unwrap().to_owned());
    cards
}
fn expire(db: &AgentDb, card: &Value) {
    let token = card["buttons"][0][0]["data"].as_str().unwrap();
    let (run, key): (String, String) = db
        .conn()
        .query_row("SELECT run_id,call_key FROM confirmations WHERE yes_type=?1", [token], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    let review = RunStore::new(db.clone())
        .records(&run)
        .unwrap()
        .into_iter()
        .find_map(|r| match r {
            owf::durable::Record::Call { function, state: owf::durable::CallState::Done(Ok(output)), .. }
                if function == "review" =>
            {
                Some(output)
            }
            _ => None,
        })
        .unwrap();
    let i = review["items"].as_array().unwrap().iter().position(|item| item["key"] == key).unwrap();
    let conn = db.conn();
    conn.execute("UPDATE confirmations SET created_at_ms=created_at_ms-86400001 WHERE yes_type=?1", [token]).unwrap();
    assert_eq!(conn.execute("UPDATE workflow_records SET record=json_set(record,'$.started_ms',json_extract(record,'$.started_ms')-86400001) WHERE run_id=?1 AND key LIKE ?2",rusqlite::params![run,format!("%/calls[{i}]/%/listen#task")]).unwrap(),1);
    conn.execute("UPDATE workflow_runs SET wake_at_ms=0 WHERE id=?1", [&run]).unwrap();
}
async fn reopen(f: &Fixture, model: Arc<Scripted>, world: Arc<World>) -> (Fixture, Arc<Flows>) {
    let (mut next, _) = setup(model, world, "needs_confirmation");
    next.db = Arc::new(AgentDb::open(f.root.path().join("agent.db")).unwrap());
    let deps = &mut Arc::get_mut(&mut next.engine).unwrap().deps;
    deps.memory = next.db.clone();
    deps.tracer = Arc::new(agent::local_recorder::local_recorder_tracer(next.db.clone()));
    let flows = create_durable_flows(next.engine.clone(), (*next.db).clone());
    (next, flows)
}

async fn confirmation_case(
    decisions: Vec<u8>,
    during_hold: bool,
    restart: bool,
    worker: bool,
    noise: Vec<u8>,
    w: &Witnesses,
) {
    let original: Vec<Value> =
        decisions.iter().enumerate().map(|(i, _)| json!({"operation":i,"payload":format!("original-{i}-λ")})).collect();
    let mut mixed: Vec<_> = original.iter().map(|a| ("write", a.clone())).collect();
    mixed.push(("local_read", json!({"q":noise.len()})));
    let round = calls(&mixed);
    let mut script = Vec::new();
    if worker {
        script.push(calls(&[("invoke_sub_agent", json!({"prompt":"perform the operation","skills":["s"]}))]));
    }
    script.push(round);
    script.push(assistant(if worker { "worker answer" } else { "done" }));
    if worker {
        script.push(assistant("parent answer"));
    }
    let model = Arc::new(Scripted::new(script));
    let world = Arc::new(World::default());
    let (f, _) = setup(model.clone(), world.clone(), "needs_confirmation");
    f.db.conn().execute_batch("CREATE TABLE waiting_saved(id TEXT); CREATE TRIGGER record_waiting AFTER UPDATE OF state ON workflow_runs WHEN NEW.state='waiting' BEGIN INSERT INTO waiting_saved VALUES(NEW.id); END;").unwrap();
    let flows = create_durable_flows(f.engine.clone(), (*f.db).clone());
    let input = input(&f.engine, "primary").await;
    let scope = null_scope();
    let token = CancellationToken::new();
    let mut future = Some(Box::pin(flows.run_loop("primary/loop", input, &scope, &token)));
    // Poll and yield only. Paused time cannot jump over a hold while this
    // controller is runnable, so observing the cards is an ordering barrier.
    for _ in 0..200 {
        assert!(poll!(future.as_mut().unwrap().as_mut()).is_pending(), "loop finished before the press");
        tokio::task::yield_now().await;
        if world.cards().len() == decisions.len() {
            break;
        }
    }
    let card = cards(&f.db, &world);
    assert_eq!(card.len(), decisions.len(), "cards not reached");
    assert!(world.writes().is_empty());
    assert!(world.calls.lock().unwrap().iter().any(|(name, _)| name == "local_read"));
    w.hit("unguarded call before press");
    let runs = RunStore::new((*f.db).clone());
    for n in noise {
        assert!(!runs.deliver(&format!("foreign-token:{n}"), &json!({})).unwrap());
        audit(&f.db, &world);
    }
    let mut next = None;
    let resumed = if restart {
        // Lose the root and all owned futures between card and press.
        drop(future.take());
        f.engine.shutdown().await;
        f.db.conn()
            .execute_batch("UPDATE workflow_runs SET lease_until_ms=0,wake_at_ms=0 WHERE ended_ms IS NULL")
            .unwrap();
        let (restarted, resumed) = reopen(&f, model.clone(), world.clone()).await;
        next = Some(restarted);
        w.hit("crash between card and press");
        Some(resumed)
    } else if !during_hold {
        let status = future.take().unwrap().await;
        assert!(matches!(status, FlowResult::Waiting { .. }), "{status:?}");
        w.hit("press after suspension");
        Some(flows.clone())
    } else {
        None
    };
    // All input arrives before recovery starts. Order is generated; separate
    // cards retain their own token and argument binding.
    for (i, decision) in decisions.iter().enumerate().rev() {
        if *decision == 2 {
            continue;
        }
        let index = usize::from(*decision == 1);
        let foreign = if *decision >= 3 { *decision - 2 } else { 0 };
        assert!(press(&runs, &card[i], index, foreign));
        assert!(!press(&runs, &card[i], index, foreign), "duplicate pending press accepted");
        w.hit("duplicate press");
        audit(&f.db, &world);
    }
    for (i, d) in decisions.iter().enumerate() {
        if *d == 2 {
            expire(&f.db, &card[i]);
            assert!(press(&runs, &card[i], 0, 0), "stale press is queued, then rejected by timeout");
        }
    }
    if let Some(resumed) = resumed {
        // Child guard → worker (if any) → parent. Bounded recovery is a
        // liveness assertion: an indefinitely waiting run fails here.
        for _ in 0..8 {
            resumed.recover_once(&token).await.unwrap();
            audit(&f.db, &world);
            if runs.get("primary/loop").unwrap().unwrap().outcome.is_some() {
                break;
            }
        }
    } else {
        assert!(!decisions.contains(&2), "expiry is exercised after suspension");
        let status = future.take().unwrap().await;
        assert!(matches!(status, FlowResult::Completed(_)), "held press stalled: {status:?}");
        assert_eq!(f.db.conn().query_row("SELECT count(*) FROM waiting_saved", [], |r| r.get::<_, i64>(0)).unwrap(), 0);
        w.hit("press during hold");
    }
    let expected: Vec<_> = original.iter().zip(&decisions).filter(|(_, d)| **d == 0).map(|(a, _)| a.clone()).collect();
    let mut actual = world.writes();
    actual.sort_by_key(|a| a["operation"].as_i64());
    assert_eq!(actual, expected, "confirmed calls ran with original arguments");
    assert_eq!(
        runs.get("primary/loop").unwrap().unwrap().outcome,
        Some(owf::run::Status::Completed(json!(if worker { "parent answer" } else { "done" })))
    );
    for (i, d) in decisions.iter().enumerate() {
        assert!(!press(&runs, &card[i], 0, 0), "resolved token still active");
        w.hit(match d {
            0 => "confirmed and executed",
            1 => "declined",
            2 => "expired",
            _ => "foreign card rejected",
        });
    }
    let transcript = model
        .seen
        .lock()
        .unwrap()
        .iter()
        .map(|s| serde_json::to_string(&s.messages).unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    if decisions.contains(&1) {
        assert!(transcript.contains("declined by the user"));
    }
    if decisions.contains(&2) {
        assert!(transcript.contains("not confirmed within 24 h"));
    }
    if worker {
        assert!(transcript.contains("worker answer"));
        w.hit("worker answer joined");
    }
    assert_eq!(world.cards().len(), decisions.len(), "restart resent a card");
    if let Some(f) = next {
        f.engine.shutdown().await;
    }
    f.engine.shutdown().await;
}

fn confirmation_witnesses() -> Witnesses {
    Witnesses::new(&[
        "crash between card and press",
        "press after suspension",
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

#[test]
fn confirmation_rounds_finish_with_exact_original_calls_or_explicit_refusals() {
    let w = confirmation_witnesses();
    TestRunner::new(Config {
        cases: runs(),
        source_file: Some(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/confirmation_properties.rs")),
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::WithSource(
            "proptest-regressions",
        ))),
        ..Config::default()
    })
    .run(
        &(
            any::<bool>(),
            any::<bool>(),
            prop::collection::vec(0u8..=5, 1..=4),
            prop::collection::vec(any::<u8>(), 0..=5),
        ),
        |(restart, worker, decisions, noise)| {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap();
            rt.block_on(async {
                confirmation_case(decisions, false, restart, worker, noise, &w).await;
                confirmation_case(vec![0, 1, 3, 4, 5], true, false, false, vec![], &w).await;
                confirmation_case(vec![2], false, false, false, vec![], &w).await;
            });
            Ok(())
        },
    )
    .unwrap();
    w.finish();
}

async fn judge_case(text: String, chat: i64, variant: u8, test_confirm: bool) {
    let model = Arc::new(Scripted::new(vec![]));
    let world = Arc::new(World::default());
    let (f, api) = setup(model, world, "misaligned");
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
        guard::judge(&f.engine, &context, &[call], &[], 0, &null_scope(), &CancellationToken::new()).await.unwrap();
    let requests = api.requests.lock().unwrap().len();
    if variant == 1 {
        assert_eq!(requests, 0);
        assert_eq!(verdict[0].1.label, "unknown_chat");
    } else {
        assert!(requests > 0 || !verdict[0].1.allowed(), "link bypassed Jev: {text:?}");
        if test_confirm {
            assert!(!verdict[0].1.allowed());
        }
    }
    f.engine.shutdown().await;
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
    TestRunner::new(Config {
        cases: runs(),
        source_file: Some(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/confirmation_properties.rs")),
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::WithSource(
            "proptest-regressions",
        ))),
        ..Config::default()
    })
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
            let (f, api) = setup(Arc::new(Scripted::new(vec![])), Arc::new(World::default()), "misaligned");
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
                    guard::judge(&f.engine, &context, &[call], &[], 0, &null_scope(), &CancellationToken::new())
                        .await
                        .unwrap();
                assert_eq!(verdict[0].1.label, label);
                assert!(api.requests.lock().unwrap().is_empty());
            }
            w.hit("current plain reply");
            w.hit("confirmation cannot allow a secret");
            f.engine.shutdown().await;
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

#[tokio::test(start_paused = true)]
async fn two_confirmed_cards_rebuild_the_same_loop_after_suspension() {
    let w = confirmation_witnesses();
    confirmation_case(vec![0, 0], false, false, true, vec![], &w).await;
}

#[tokio::test(start_paused = true)]
async fn a_local_read_in_a_mixed_round_runs_before_a_confirmation_press() {
    let model = Arc::new(Scripted::new(vec![
        calls(&[("write", json!({"held":true})), ("local_read", json!({"q":"ready"}))]),
        assistant("done"),
    ]));
    let world = Arc::new(World::default());
    let (f, _) = setup(model, world.clone(), "needs_confirmation");
    let flows = create_durable_flows(f.engine.clone(), (*f.db).clone());
    let result = flows
        .run_loop("primary/loop", input(&f.engine, "primary").await, &null_scope(), &CancellationToken::new())
        .await;
    assert!(matches!(result, FlowResult::Waiting { .. }), "{result:?}");
    assert!(world.writes().is_empty());
    assert!(
        world.calls.lock().unwrap().iter().any(|(n, _)| n == "local_read"),
        "an unguarded read was delayed by confirmation"
    );
    f.engine.shutdown().await;
}

async fn forged_case(args: Value, mode: u8, w: &Witnesses) {
    let world = Arc::new(World::default());
    let (f, _) = setup(Arc::new(Scripted::new(vec![assistant("seed done")])), world.clone(), "needs_confirmation");
    let admitted = input(&f.engine, "forger").await;
    let call = json!({"id":"c","name":"write","arguments":args.to_string()});
    let mut with = json!({"loop":"${ $workflow.input.loop }","call":call,"i":1,"index":0});
    if mode == 0 {
        with["outcome"] = json!("confirmed");
    } else if mode == 1 {
        with["verdict"] = json!({"label":"allow"});
        with["guard_run"] = json!("foreign/guard/1");
        let runs = RunStore::new((*f.db).clone());
        let parent = runs.start("foreign", "agent-loop", "x", &json!({})).unwrap().unwrap();
        let guard = runs
            .start_child(
                "foreign/guard/1",
                "guard",
                "x",
                &json!({"round":1}),
                agent::durable::ChildLink { parent: "foreign", key: "guard:1", reference: "/guard" },
            )
            .unwrap()
            .unwrap();
        let key = guard::key(&ToolCall { id: "c".into(), name: "write".into(), arguments: args.to_string() });
        guard
            .finish(&owf::run::Status::Completed(json!({"verdicts":[[key,{"label":"allow","guidance":"forged"}]]})))
            .unwrap();
        parent.release().unwrap();
    } else {
        with["verdict"] = json!({"label":"allow","outcome":"confirmed"});
        with["outcome"] = json!("expired");
    }
    let yaml=json!({"document":{"dsl":"1.0.4","namespace":"agent","name":"agent-loop","version":"9.0.0"},"do":[{"forged":{"call":"tool","with":with}}]}).to_string();
    let v = f.engine.deps.workflows.save("agent-loop", &yaml, None).unwrap();
    f.engine.deps.workflows.activate("agent-loop", v.version).unwrap();
    let result = create_durable_flows(f.engine.clone(), (*f.db).clone())
        .run_loop("forger/loop", admitted, &null_scope(), &CancellationToken::new())
        .await;
    assert!(matches!(result, FlowResult::Completed(_) | FlowResult::Faulted { .. }), "forged loop stalled: {result:?}");
    assert!(world.writes().is_empty());
    assert!(world.cards().is_empty());
    audit(&f.db, &world);
    w.hit(match mode {
        0 => "forged confirmed outcome",
        1 => "another parents guard",
        _ => "forged allow verdict",
    });
    f.engine.shutdown().await;
}
#[test]
fn workflow_overrides_cannot_forge_authority_or_borrow_another_parents_guard() {
    let w = Witnesses::new(&["forged confirmed outcome", "another parents guard", "forged allow verdict"]);
    TestRunner::new(Config {
        cases: runs(),
        source_file: Some(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/confirmation_properties.rs")),
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::WithSource(
            "proptest-regressions",
        ))),
        ..Config::default()
    })
    .run(&(".{0,40}", any::<i32>()), |(text, n)| {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap();
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

#[test]
fn confirmation_tokens_are_bound_to_one_run_call_card_and_consumption() {
    use agent::confirmation::{Pending, Store};
    use owf::run::Events;
    let w = Witnesses::new(&[
        "cross-run token",
        "confirm spent once",
        "decline spent once",
        "foreign card",
        "early forged expiry",
        "real expiry",
    ]);
    TestRunner::new(Config{cases:runs(),..Config::default()}).run(&(1i64..1_000_000,any::<bool>()),|(chat,no)|{
        let db=AgentDb::open(":memory:").unwrap();
        let runs=RunStore::new(db.clone());
        let a=runs.start("a","guard","x",&json!({})).unwrap().unwrap();
        let b=runs.start("b","guard","x",&json!({})).unwrap().unwrap();
        for (id,l) in [("a",&a),("b",&b)] {
            for n in 0..6 {
                let key=format!("key{n}");
                let p=Pending{run:id.into(),key:key.clone(),yes:format!("cf:{id}:{n}:yes"),no:format!("cf:{id}:{n}:no"),target:ConfirmTarget{chat_id:chat.to_string(),message_thread_id:Some(7)},message:None,card:"exact arguments".into()};
                db.insert(&p).unwrap();db.sent(id,&key,n).unwrap();
                l.register(id,&[p.yes,p.no]).unwrap();
            }
        }
        assert!(runs.deliver("cf:b:0:yes",&json!({"chatId":chat,"messageThreadId":7,"messageId":0})).unwrap());
        assert!(b.take("b","other",&["cf:b:0:yes".into()]).unwrap().is_some());
        assert_eq!(db.resolve("a","key0","confirmed").unwrap(),"unverified");
        assert_eq!(db.resolve("b","key0","confirmed").unwrap(),"confirmed");
        w.hit("cross-run token");
        for (n,foreign) in [(1,false),(2,true)] {
            let type_=format!("cf:a:{n}:{}",if no {"no"} else {"yes"});
            assert!(runs.deliver(&type_,&json!({"chatId":chat,"messageThreadId":7,"messageId":if foreign {99} else {n}})).unwrap());
            assert!(a.take("a",&format!("take{n}"),&[type_]).unwrap().is_some());
            assert_eq!(db.resolve("a",&format!("key{n}"),"confirmed").unwrap(),if foreign {"unverified"} else if no {"declined"} else {"confirmed"});
            assert_eq!(db.resolve("a",&format!("key{n}"),"confirmed").unwrap(),"unverified");
            w.hit(if foreign {"foreign card"} else if no {"decline spent once"} else {"confirm spent once"});
        }
        assert_eq!(db.resolve("a","key3","expired").unwrap(),"unverified");w.hit("early forged expiry");
        db.conn().execute("UPDATE confirmations SET created_at_ms=created_at_ms-86400001 WHERE run_id='a' AND call_key='key4'",[]).unwrap();
        assert_eq!(db.resolve("a","key4","expired").unwrap(),"expired");w.hit("real expiry");
        assert!(!runs.deliver("cf:a:4:yes",&json!({})).unwrap());
        a.finish(&owf::run::Status::Completed(json!({}))).unwrap();b.finish(&owf::run::Status::Completed(json!({}))).unwrap();
        Ok(())
    }).unwrap();
    w.finish();
}

async fn resolution_crash(args: Value, mode: u8, w: &Witnesses) {
    let model = Arc::new(Scripted::new(vec![calls(&[("write", args.clone())]), assistant("done")]));
    let world = Arc::new(World::default());
    let (f, _) = setup(model.clone(), world.clone(), "needs_confirmation");
    let flows = create_durable_flows(f.engine.clone(), (*f.db).clone());
    let result = flows
        .run_loop("primary/loop", input(&f.engine, "primary").await, &null_scope(), &CancellationToken::new())
        .await;
    assert!(matches!(result, FlowResult::Waiting { .. }), "{result:?}");
    let runs = RunStore::new((*f.db).clone());
    let card = cards(&f.db, &world);
    assert!(press(&runs, &card[0], 0, 0));
    let stop = CancellationToken::new();
    match mode {
        0=>f.db.conn().execute_batch("CREATE TRIGGER crash_resolution AFTER UPDATE ON workflow_records WHEN json_extract(NEW.record,'$.function')='resolve_review' AND json_type(NEW.record,'$.state.Done.Ok') IS NOT NULL BEGIN SELECT RAISE(ABORT,'crash after confirmation resolution'); END;").unwrap(),
        1=>f.db.conn().execute_batch("CREATE TRIGGER crash_resolution AFTER UPDATE ON workflow_runs WHEN NEW.name='guard' AND NEW.state='completed' BEGIN SELECT RAISE(ABORT,'crash during guard completion'); END;").unwrap(),
        2=>*world.stop_after_write.lock().unwrap()=Some(stop.clone()),
        _=>unreachable!(),
    }
    assert_eq!(flows.recover_once(&stop).await.unwrap(), 1);
    if mode < 2 {
        assert_eq!(
            f.db.conn()
                .query_row("SELECT count(*) FROM confirmations WHERE outcome='confirmed'", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1,
            "crash point was not reached"
        );
        f.db.conn().execute_batch("DROP TRIGGER crash_resolution").unwrap();
        assert!(world.writes().is_empty());
    } else {
        assert_eq!(flows.recover_once(&stop).await.unwrap(), 1);
        assert_eq!(world.writes(), vec![args.clone()], "crash happened after the external effect");
    }
    audit(&f.db, &world);
    f.engine.shutdown().await;
    f.db.conn().execute_batch("UPDATE workflow_runs SET lease_until_ms=0,wake_at_ms=0 WHERE ended_ms IS NULL").unwrap();
    let (next, resumed) = reopen(&f, model.clone(), world.clone()).await;
    for _ in 0..6 {
        resumed.recover_once(&CancellationToken::new()).await.unwrap();
        audit(&next.db, &world);
        if runs.get("primary/loop").unwrap().unwrap().outcome.is_some() {
            break;
        }
    }
    assert_eq!(runs.get("primary/loop").unwrap().unwrap().outcome, Some(owf::run::Status::Completed(json!("done"))));
    assert_eq!(world.writes(), vec![args]);
    assert_eq!(world.cards().len(), 1);
    assert!(!press(&runs, &card[0], 0, 0));
    if mode == 2 {
        assert!(
            serde_json::to_string(&model.seen.lock().unwrap().last().unwrap().messages)
                .unwrap()
                .contains("outcome unknown: write")
        );
    }
    w.hit(match mode {
        0 => "crash after consumed confirm",
        1 => "crash at guard completion",
        _ => "crash after confirmed effect",
    });
    next.engine.shutdown().await;
}
#[test]
fn confirmation_resolution_crashes_preserve_authority_and_at_most_once_execution() {
    let w =
        Witnesses::new(&["crash after consumed confirm", "crash at guard completion", "crash after confirmed effect"]);
    TestRunner::new(Config {
        cases: runs(),
        source_file: Some(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/confirmation_properties.rs")),
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::WithSource(
            "proptest-regressions",
        ))),
        ..Config::default()
    })
    .run(&(".{0,30}", any::<u16>()), |(text, n)| {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap();
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

#[test]
fn reviewed_rounds_dispatch_only_the_owned_allow_verdicts_and_keep_reply_order() {
    let w = Witnesses::new(&["allow executed", "block refused", "unguarded progressed", "loop finished"]);
    TestRunner::new(Config { cases: runs(), ..Config::default() })
        .run(&prop::collection::vec(".{0,20}", 1..=4), |texts| {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap();
            rt.block_on(async {
                for label in ["allow", "misaligned"] {
                    let args: Vec<_> = texts.iter().enumerate().map(|(i, t)| json!({"i":i,"text":t})).collect();
                    let mut round: Vec<_> = args.iter().map(|a| ("write", a.clone())).collect();
                    round.insert(0, ("local_read", json!({"q":"ready"})));
                    let model = Arc::new(Scripted::new(vec![calls(&round), assistant("done")]));
                    let world = Arc::new(World::default());
                    let (f, api) = setup(model.clone(), world.clone(), label);
                    let flows = create_durable_flows(f.engine.clone(), (*f.db).clone());
                    assert!(matches!(
                        flows
                            .run_loop(
                                "primary/loop",
                                input(&f.engine, "primary").await,
                                &null_scope(),
                                &CancellationToken::new()
                            )
                            .await,
                        FlowResult::Completed(_)
                    ));
                    let mut writes = world.writes();
                    writes.sort_by_key(|v| v["i"].as_i64());
                    assert_eq!(writes, if label == "allow" { args } else { vec![] });
                    assert!(world.cards().is_empty());
                    assert_eq!(api.requests.lock().unwrap().len(), 1);
                    assert!(world.calls.lock().unwrap().iter().any(|(n, _)| n == "local_read"));
                    {
                        let seen = model.seen.lock().unwrap();
                        let replies: Vec<_> = seen
                            .last()
                            .unwrap()
                            .messages
                            .iter()
                            .filter(|m| m["role"] == "tool")
                            .map(|m| m["tool_call_id"].as_str().unwrap())
                            .collect();
                        assert_eq!(replies, (0..round.len()).map(|i| format!("call-{i}")).collect::<Vec<_>>());
                    }
                    let runs = RunStore::new((*f.db).clone());
                    let outcome =
                        runs.child_outcome("primary/loop", "guard:1", "primary/loop/guard/1").unwrap().unwrap();
                    let owf::run::Status::Completed(outcome) = outcome else { panic!("guard did not complete") };
                    assert!(outcome["verdicts"].as_array().unwrap().iter().all(|v| v[1]["label"] == label));
                    w.hit(if label == "allow" { "allow executed" } else { "block refused" });
                    w.hit("unguarded progressed");
                    w.hit("loop finished");
                    f.engine.shutdown().await;
                }
            });
            Ok(())
        })
        .unwrap();
    w.finish();
}
