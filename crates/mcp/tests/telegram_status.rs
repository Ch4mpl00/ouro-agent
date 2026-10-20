// The live status bubble (telegram_send_status) and the typing keep-alive
// over MCP, against the fake Bot API. A port of
// packages/mcp/test/telegram-status.test.ts. Generated sequences of status
// calls — some fired at once, as an agent's parallel tool calls are — with
// pauses that let the animation run: the chat holds at most one bubble per
// status id, showing the latest text (plus its animation dots), and an
// emptied status leaves nothing behind.

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use mcp_tools::telegram::TelegramConfig;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use serde_json::json;
use support::fake_telegram::{self, Delivered, FakeTelegram};
use support::{Client, Outcome, Overrides, Server, TestDatabase, call, connect, runtime, start_test_server_with};

const CHAT: i64 = 4242;

struct Env {
    _db: TestDatabase,
    _server: Server,
    client: Client,
    fake: FakeTelegram,
}

fn env() -> &'static Env {
    static ENV: OnceLock<Env> = OnceLock::new();
    ENV.get_or_init(|| {
        runtime().block_on(async {
            let db = support::test_database().await;
            let fake = fake_telegram::start("test-token").await;
            let telegram = TelegramConfig {
                bot_token: Some(fake.token.clone()),
                default_chat_id: Some(CHAT.to_string()),
                api_root: Some(fake.root.clone()),
                ..TelegramConfig::default()
            };
            let server =
                start_test_server_with(&db, None, Overrides { telegram: Some(telegram), ..Overrides::default() })
                    .await
                    .expect("server starts");
            let client = connect(&server.url).await;
            Env { _db: db, _server: server, client, fake }
        })
    })
}

fn serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

#[derive(Debug, Clone)]
enum Op {
    Status { id: &'static str, text: String, parallel: usize },
    Wait { ms: u64 },
}

fn text() -> impl Strategy<Value = String> {
    prop_oneof![
        5 => proptest::sample::select(vec!["Searching the news", "Reading the PDF", "Collecting the digest", "step 2"]).prop_map(str::to_owned),
        2 => proptest::sample::select(vec!["", "   "]).prop_map(str::to_owned),
        1 => Just("я".repeat(4096)),
    ]
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        5 => (proptest::sample::select(vec!["status:1", "status:2"]), text(), proptest::sample::select(vec![1usize, 1, 1, 2, 3]))
            .prop_map(|(id, text, parallel)| Op::Status { id, text, parallel }),
        1 => proptest::sample::select(vec![300u64, 1_300]).prop_map(|ms| Op::Wait { ms }),
    ]
}

// The bubbles in the chat, with their current text.
fn bubbles(fake: &FakeTelegram) -> Vec<Delivered> {
    fake.lock().chat.iter().filter(|m| m.chat_id == CHAT).cloned().collect()
}

fn undotted(s: &str) -> String {
    let dots = s.chars().rev().take_while(|c| *c == '.').count().min(3);
    s[..s.len() - dots].to_owned()
}

#[test]
fn keeps_one_bubble_per_status_id_showing_the_latest_text_and_removes_it_when_emptied() {
    let _serial = serial();
    let env = env();
    let witnessed = Mutex::new(BTreeSet::new());
    TestRunner::new(Config::with_cases(25))
        .run(&proptest::collection::vec(op(), 1..=10), |ops| {
            runtime().block_on(async {
                // Start from no status at all: empty both ids, then clear the chat.
                for id in ["status:1", "status:2"] {
                    call(&env.client, "telegram_send_status", json!({ "id": id, "text": "" })).await;
                }
                env.fake.lock().chat.clear();
                let mut latest: BTreeMap<&str, String> = BTreeMap::new();
                for o in &ops {
                    match o {
                        Op::Wait { ms } => {
                            tokio::time::sleep(Duration::from_millis(*ms)).await;
                            if *ms > 1_000 {
                                witnessed.lock().unwrap().insert("the animation ran");
                            }
                        }
                        Op::Status { id, text, parallel } => {
                            let calls = (0..*parallel)
                                .map(|_| call(&env.client, "telegram_send_status", json!({ "id": id, "text": text })));
                            let outcomes = futures::future::join_all(calls).await;
                            if *parallel > 1 {
                                witnessed.lock().unwrap().insert("parallel calls for one id");
                            }
                            for out in &outcomes {
                                assert!(matches!(out, Outcome::Ok(_)), "{o:?}: {:.200}", format!("{out:?}"));
                            }
                            if text.trim().is_empty() {
                                latest.remove(id);
                            } else {
                                latest.insert(id, text.trim().to_owned());
                            }
                        }
                    }
                    // Let in-flight animation edits land before looking.
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    let shown = bubbles(&env.fake);
                    assert_eq!(shown.len(), latest.len(), "{o:?}: one bubble per live status, no orphans");
                    let mut texts: Vec<String> = shown.iter().map(|b| undotted(&b.text)).collect();
                    texts.sort();
                    let mut want: Vec<String> = latest.values().cloned().collect();
                    want.sort();
                    assert_eq!(texts, want, "{o:?}: each bubble shows its latest text");
                    for b in &shown {
                        assert!(b.text.encode_utf16().count() <= 4096, "within Telegram's limit");
                    }
                }
            });
            Ok(())
        })
        .unwrap();
    assert_eq!(
        witnessed.into_inner().unwrap().into_iter().collect::<Vec<_>>(),
        ["parallel calls for one id", "the animation ran"]
    );
}

#[test]
fn animates_a_long_status_without_going_over_the_limit() {
    let _serial = serial();
    let env = env();
    runtime().block_on(async {
        call(&env.client, "telegram_send_status", json!({ "id": "status:long", "text": "я".repeat(4096) })).await;
        let before = env.fake.lock().sent.len();
        tokio::time::sleep(Duration::from_millis(2_300)).await;
        let bubble = bubbles(&env.fake).into_iter().find(|b| b.text.starts_with('я')).expect("the bubble");
        assert!(bubble.text.encode_utf16().count() <= 4096);
        assert_eq!(env.fake.lock().sent.len(), before);
        call(&env.client, "telegram_send_status", json!({ "id": "status:long", "text": "" })).await;
    });
}

#[test]
fn start_typing_keeps_the_indicator_alive_until_a_message_goes_to_that_chat_then_stops() {
    let _serial = serial();
    let env = env();
    runtime().block_on(async {
        env.fake.lock().actions.clear();
        assert!(matches!(call(&env.client, "start_typing", json!({ "chatId": CHAT })).await, Outcome::Ok(_)));
        tokio::time::sleep(Duration::from_millis(4_500)).await;
        let kept = env.fake.lock().actions.iter().filter(|a| a.0 == CHAT).count();
        assert!(kept >= 2, "sent at start and refreshed: {kept}");
        assert!(matches!(call(&env.client, "send_telegram_message", json!({ "text": "done" })).await, Outcome::Ok(_)));
        let after = env.fake.lock().actions.len();
        tokio::time::sleep(Duration::from_millis(4_500)).await;
        assert_eq!(env.fake.lock().actions.len(), after, "no refresh after the message");
    });
}
