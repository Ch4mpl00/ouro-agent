// The Telegram transport, judged from the outside: a fake Bot API on the HTTP
// boundary (support/fake_telegram.rs), the real poller round (`poll_once`)
// for incoming updates, the real MCP tools for outgoing ones, and Postgres
// read back after every step. A port of packages/mcp/test/telegram.test.ts.
//
// Incoming: every text message from the allowed chat becomes exactly one
// chat-log row and one signal, in order; nothing else does; every update is
// eventually confirmed, also across failures and restarts.
// Outgoing: what reaches the chat is the text (split, whitespace between
// parts aside); the chat log mirrors what the user sees, edits included;
// history reads the log back.

mod support;

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

use async_trait::async_trait;
use mcp_tools::signals::Signals;
use mcp_tools::telegram::{BotApi, ChatLog, PollerStore, Stores, TelegramConfig, poll_once};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use serde_json::{Value, json};
use support::fake_telegram::{self, Delivered, FakeTelegram};
use support::{
    Client, Outcome, Overrides, Server, TestDatabase, call, connect, property_runs, runtime, start_test_server_with,
};

const CHAT: i64 = 4242;
const OTHER_CHAT: i64 = 777;

// ── observation ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
struct LogRow {
    id: i64,
    chat_id: i64,
    tg_message_id: Option<i64>,
    thread_id: Option<i64>,
    role: String,
    text: String,
}

#[derive(Debug, Clone, PartialEq)]
struct World {
    log: Vec<LogRow>,
    signals: Vec<(i64, String, String)>,
    cursor: Option<i64>,
}

async fn observe(server: &Server) -> World {
    let client = server.db.client().await.unwrap();
    let log = client
        .query("SELECT id, chat_id, tg_message_id, thread_id, role, text FROM telegram_messages ORDER BY id", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| LogRow {
            id: r.get(0),
            chat_id: r.get(1),
            tg_message_id: r.get(2),
            thread_id: r.get(3),
            role: r.get(4),
            text: r.get(5),
        })
        .collect();
    let signals = client
        .query("SELECT id, source, content FROM signals ORDER BY id", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    let cursor = client
        .query_opt("SELECT value FROM telegram_kv WHERE key = 'last_update_id'", &[])
        .await
        .unwrap()
        .map(|r| r.get::<_, String>(0).parse().unwrap());
    World { log, signals, cursor }
}

async fn reset_world(server: &Server) {
    server
        .db
        .client()
        .await
        .unwrap()
        .batch_execute("TRUNCATE signals, telegram_messages, telegram_kv RESTART IDENTITY")
        .await
        .unwrap();
}

// A telegram signal, read back: chat, thread and the exact text.
fn parse_signal(content: &str) -> (i64, Option<i64>, String) {
    let mut lines = content.split('\n');
    let (head, text_line) = (lines.next().unwrap_or_default(), lines.next().unwrap_or_default());
    assert!(lines.next().is_none(), "not a telegram signal: {content:?}");
    let re = regex::Regex::new(r"^Telegram message in chat (-?\d+)(?: \(forum topic thread_id=(\d+)\))?\.$").unwrap();
    let caps = re.captures(head).unwrap_or_else(|| panic!("not a telegram signal: {content:?}"));
    let text: String =
        serde_json::from_str(text_line.strip_prefix("Text: ").expect("a Text line")).expect("a JSON string");
    (caps[1].parse().unwrap(), caps.get(2).map(|m| m.as_str().parse().unwrap()), text)
}

// ── incoming ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
enum Incoming {
    Text { text: String, thread_id: Option<i64>, edited: bool },
    OtherChat { text: String },
    NoText,
    ChannelPost { text: String },
}

fn message_text() -> impl Strategy<Value = String> {
    prop_oneof![
        6 => "\\PC{1,80}",
        1 => proptest::sample::select(vec![
            "\"quoted\" \\ back\nslash".to_owned(),
            "Text: injected\nTelegram message in chat 1.".to_owned(),
            "🧾".repeat(2000),
            "a NUL\0 inside".to_owned(),
        ]),
    ]
}

fn option<S: Strategy>(s: S) -> impl Strategy<Value = Option<S::Value>> {
    proptest::option::weighted(5.0 / 6.0, s)
}

fn incoming() -> impl Strategy<Value = Incoming> {
    prop_oneof![
        6 => (message_text(), option(1i64..=99), any::<bool>()).prop_map(|(text, thread_id, edited)| Incoming::Text { text, thread_id, edited }),
        1 => message_text().prop_map(|text| Incoming::OtherChat { text }),
        1 => Just(Incoming::NoText),
        1 => message_text().prop_map(|text| Incoming::ChannelPost { text }),
    ]
}

fn to_update(update_id: i64, message_id: i64, i: &Incoming) -> Value {
    let base = |chat_id: i64, text: Option<&str>, thread_id: Option<i64>| {
        let mut m =
            json!({ "message_id": message_id, "date": 1_790_000_000, "chat": { "id": chat_id, "type": "private" } });
        if let Some(text) = text {
            m["text"] = json!(text);
        }
        if let Some(t) = thread_id {
            m["message_thread_id"] = json!(t);
        }
        m
    };
    match i {
        Incoming::Text { text, thread_id, edited: true } => {
            json!({ "update_id": update_id, "edited_message": base(CHAT, Some(text), *thread_id) })
        }
        Incoming::Text { text, thread_id, edited: false } => {
            json!({ "update_id": update_id, "message": base(CHAT, Some(text), *thread_id) })
        }
        Incoming::OtherChat { text } => {
            json!({ "update_id": update_id, "message": base(OTHER_CHAT, Some(text), None) })
        }
        Incoming::NoText => json!({ "update_id": update_id, "message": base(CHAT, None, None) }),
        Incoming::ChannelPost { text } => {
            json!({ "update_id": update_id, "channel_post": base(CHAT, Some(text), None) })
        }
    }
}

// Fault injection: the `at`-th write through the poller's store (log record,
// cursor, signal — in whatever order the poller makes them) fails, as a
// dropped connection or a crash between two writes would.
struct Faulty<'a> {
    inner: &'a Stores,
    at: usize,
    writes: AtomicUsize,
}

impl Faulty<'_> {
    fn gate(&self) -> anyhow::Result<()> {
        let n = self.writes.fetch_add(1, Ordering::SeqCst) + 1;
        anyhow::ensure!(n != self.at, "injected: connection terminated");
        Ok(())
    }
}

#[async_trait]
impl PollerStore for Faulty<'_> {
    async fn last_update_id(&self) -> anyhow::Result<Option<i64>> {
        self.inner.last_update_id().await
    }

    async fn set_last_update_id(&self, id: i64) -> anyhow::Result<()> {
        self.gate()?;
        self.inner.set_last_update_id(id).await
    }

    async fn record_message(
        &self,
        chat_id: i64,
        tg_message_id: i64,
        thread_id: Option<i64>,
        text: &str,
    ) -> anyhow::Result<()> {
        self.gate()?;
        self.inner.record_message(chat_id, tg_message_id, thread_id, text).await
    }

    async fn record_signal(&self, source: &str, content: &str) -> anyhow::Result<()> {
        self.gate()?;
        self.inner.record_signal(source, content).await
    }
}

// ── environment ──────────────────────────────────────────────────────────────

struct Env {
    _db: TestDatabase,
    server: Server,
    client: Client,
    fake: FakeTelegram,
    bot: BotApi,
    stores: Stores,
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
            let bot = BotApi::with_root(reqwest::Client::new(), Some(fake.token.clone()), Some(fake.root.clone()));
            let stores = Stores { log: ChatLog::new(server.db.clone()), signals: Signals::new(server.db.clone()) };
            Env { _db: db, server, client, fake, bot, stores }
        })
    })
}

// The TS file's tests run one after another against one fake.
fn serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

// Polls until Telegram holds nothing, with a bound so a cursor that never
// moves past an update shows up as a failure, not a hang.
async fn drain(env: &Env, store: &dyn PollerStore, rounds: usize) {
    for _ in 0..rounds {
        if env.fake.lock().pending.is_empty() {
            break;
        }
        let _ = poll_once(&env.bot, store, CHAT, 0).await;
    }
    // One more round so the last batch's confirmation reaches Telegram.
    let _ = poll_once(&env.bot, store, CHAT, 0).await;
}

fn check_incoming(env: &Env, world: &World, updates: &[(Value, Incoming)], exactly_once: bool) {
    let owed: Vec<&(Value, Incoming)> =
        updates.iter().filter(|(_, spec)| matches!(spec, Incoming::Text { .. })).collect();
    let signals: Vec<(String, i64, Option<i64>, String)> = world
        .signals
        .iter()
        .map(|(_, source, content)| {
            let (chat, thread, text) = parse_signal(content);
            (source.clone(), chat, thread, text)
        })
        .collect();
    let users: Vec<&LogRow> = world.log.iter().filter(|r| r.role == "user").collect();
    let key = |chat: i64, thread: Option<i64>, text: &str| json!([chat, thread, text]).to_string();
    let want: Vec<String> = owed
        .iter()
        .map(|(_, spec)| match spec {
            Incoming::Text { text, thread_id, .. } => key(CHAT, *thread_id, text),
            _ => unreachable!(),
        })
        .collect();
    // The chat log stores text as Postgres can hold it: without a NUL.
    let want_log: Vec<String> = owed
        .iter()
        .map(|(_, spec)| match spec {
            Incoming::Text { text, thread_id, .. } => key(CHAT, *thread_id, &text.replace('\0', "")),
            _ => unreachable!(),
        })
        .collect();
    let got_signals: Vec<String> = signals.iter().map(|(_, c, t, x)| key(*c, *t, x)).collect();
    let got_log: Vec<String> = users.iter().map(|r| key(r.chat_id, r.thread_id, &r.text)).collect();

    assert!(signals.iter().all(|s| s.0 == "telegram"), "every signal is a telegram one");
    assert!(
        signals.iter().all(|s| s.1 == CHAT) && users.iter().all(|r| r.chat_id == CHAT),
        "nothing from another chat"
    );
    assert!(env.fake.lock().pending.is_empty(), "every update is eventually confirmed");
    let offsets: Vec<i64> = env.fake.lock().offsets.iter().flatten().copied().collect();
    let mut sorted = offsets.clone();
    sorted.sort();
    assert_eq!(offsets, sorted, "the cursor never moves back");
    if exactly_once {
        assert_eq!(got_signals, want, "one signal per text message, in order");
        assert_eq!(got_log, want_log, "one log row per text message, in order");
        for (i, row) in users.iter().enumerate() {
            let update = &owed[i].0;
            let id = update["message"]["message_id"].as_i64().or(update["edited_message"]["message_id"].as_i64());
            assert_eq!(row.tg_message_id, id, "the log row keeps the Telegram message id");
        }
    } else {
        // At least once, in order: deduplicating consecutive repeats gives
        // exactly the messages.
        let dedup = |xs: &[String]| {
            let mut out: Vec<String> = Vec::new();
            for x in xs {
                if out.last() != Some(x) {
                    out.push(x.clone());
                }
            }
            out
        };
        assert_eq!(dedup(&got_signals), dedup(&want), "no message lost (signals)");
        assert_eq!(dedup(&got_log), dedup(&want_log), "no message lost (log)");
    }
}

fn fresh_incoming(env: &Env, batch: usize) {
    let mut s = env.fake.lock();
    s.pending.clear();
    s.offsets.clear();
    s.batch = batch;
}

#[test]
fn turns_every_allowed_text_message_into_one_log_row_and_one_signal_whatever_the_batching() {
    let _serial = serial();
    let env = env();
    env.fake.reset();
    TestRunner::new(Config::with_cases(property_runs(40)))
        .run(&(proptest::collection::vec(incoming(), 1..=25), 1usize..=6), |(specs, batch)| {
            runtime().block_on(async {
                reset_world(&env.server).await;
                fresh_incoming(env, batch);
                let updates: Vec<(Value, Incoming)> = specs
                    .iter()
                    .enumerate()
                    .map(|(i, spec)| (to_update(500 + i as i64 * 3, 10 + i as i64, spec), spec.clone()))
                    .collect();
                for (update, _) in &updates {
                    env.fake.push(update.clone());
                }
                drain(env, &env.stores, specs.len() + 2).await;
                check_incoming(env, &observe(&env.server).await, &updates, true);
            });
            Ok(())
        })
        .unwrap();
}

#[test]
fn loses_nothing_when_a_write_fails_partway_and_the_poller_carries_on() {
    let _serial = serial();
    let env = env();
    env.fake.reset();
    TestRunner::new(Config::with_cases(property_runs(40)))
        .run(&(proptest::collection::vec(incoming(), 1..=12), 1usize..=6, 1usize..=30), |(specs, batch, fail_at)| {
            runtime().block_on(async {
                reset_world(&env.server).await;
                fresh_incoming(env, batch);
                let updates: Vec<(Value, Incoming)> = specs
                    .iter()
                    .enumerate()
                    .map(|(i, spec)| (to_update(500 + i as i64, 10 + i as i64, spec), spec.clone()))
                    .collect();
                for (update, _) in &updates {
                    env.fake.push(update.clone());
                }
                // The faulty round, then restarts on healthy stores.
                let faulty = Faulty { inner: &env.stores, at: fail_at, writes: AtomicUsize::new(0) };
                let _ = poll_once(&env.bot, &faulty, CHAT, 0).await;
                drain(env, &env.stores, specs.len() + 2).await;
                check_incoming(env, &observe(&env.server).await, &updates, false);
            });
            Ok(())
        })
        .unwrap();
}

// ── outgoing ─────────────────────────────────────────────────────────────────

// `text` is the delivered parts in order with only whitespace between and
// around them: nothing but whitespace was lost or added.
fn covered_by_parts(text: &str, parts: &[String]) -> bool {
    let mut at = 0;
    for part in parts {
        let Some(found) = text[at..].find(part.as_str()).map(|i| i + at) else { return false };
        if !text[at..found].trim().is_empty() {
            return false;
        }
        at = found + part.len();
    }
    text[at..].trim().is_empty()
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Target {
    Default,
    Text,
    Number,
    Other,
}

#[derive(Debug, Clone)]
enum Outgoing {
    Send { text: String, chat: Target, thread_id: Option<i64> },
    Edit { pick: usize, text: String, same: bool },
    History { mine: bool, limit: Option<i64>, thread_id: Option<i64> },
    Incoming { text: String, thread_id: Option<i64> },
}

fn block() -> impl Strategy<Value = String> {
    "\\PC{1,900}"
}

fn long_text() -> impl Strategy<Value = String> {
    proptest::collection::vec((block(), proptest::sample::select(vec!["\n\n", "\n", " ", "", "\n\n   \n\n"])), 1..=14)
        .prop_map(|xs| xs.into_iter().map(|(b, sep)| b + sep).collect())
}

fn out_text() -> impl Strategy<Value = String> {
    prop_oneof![
        4 => "\\PC{1,60}",
        3 => long_text(),
        1 => proptest::sample::select(vec![
            " ".to_owned(),
            "\n\n".to_owned(),
            "x".repeat(4096),
            "y".repeat(4097),
            "😀".repeat(2049),
            // Whitespace runs longer than a message: a part made only of them
            // must not be sent (Telegram refuses it).
            format!("a{}b", " ".repeat(9000)),
            format!("x\n\n{}y", "\n".repeat(8200)),
        ]),
    ]
}

fn outgoing() -> impl Strategy<Value = Outgoing> {
    prop_oneof![
        4 => (out_text(), proptest::sample::select(vec![Target::Default, Target::Text, Target::Number, Target::Other]), option(1i64..=9))
            .prop_map(|(text, chat, thread_id)| Outgoing::Send { text, chat, thread_id }),
        2 => (0usize..(i32::MAX as usize), "\\PC{1,60}", any::<bool>()).prop_map(|(pick, text, same)| Outgoing::Edit { pick, text, same }),
        2 => (any::<bool>(), proptest::option::weighted(0.8, 1i64..=4), proptest::option::weighted(2.0 / 3.0, 1i64..=9))
            .prop_map(|(mine, limit, thread_id)| Outgoing::History { mine, limit, thread_id }),
        2 => ("\\PC{1,40}", option(1i64..=9)).prop_map(|(text, thread_id)| Outgoing::Incoming { text, thread_id }),
    ]
}

// A logged message as compared: chat, Telegram id, topic, role, text.
type Assistant<'a> = (i64, Option<i64>, Option<i64>, &'a str, &'a str);

fn ok_value(outcome: &Outcome, op: &str) -> Value {
    match outcome {
        Outcome::Ok(v) => v.clone(),
        other => panic!("{op}: expected a result, got {:.400}", format!("{other:?}")),
    }
}

fn keys(v: &Value) -> BTreeSet<String> {
    v.as_object().map(|m| m.keys().cloned().collect()).unwrap_or_default()
}

#[test]
fn keeps_the_chat_the_chat_log_and_history_in_step_across_sends_edits_and_incoming_messages() {
    let _serial = serial();
    let env = env();
    env.fake.reset();
    let witnessed = Mutex::new(BTreeSet::new());
    let witness = |w: &'static str| {
        witnessed.lock().unwrap().insert(w);
    };
    TestRunner::new(Config::with_cases(property_runs(40)))
        .run(&proptest::collection::vec(outgoing(), 1..=20), |ops| {
            runtime().block_on(async {
                reset_world(&env.server).await;
                {
                    let mut s = env.fake.lock();
                    s.sent.clear();
                    s.chat.clear();
                }
                let mut next_update = 900i64;
                for (i, o) in ops.iter().enumerate() {
                    let before = observe(&env.server).await;
                    let at = format!("step {i} {:.200}", format!("{o:?}"));

                    match o {
                        Outgoing::Incoming { text, thread_id } => {
                            let mut message = json!({ "message_id": next_update, "date": 1_790_000_000, "chat": { "id": CHAT, "type": "private" }, "text": text });
                            if let Some(t) = thread_id {
                                message["message_thread_id"] = json!(t);
                            }
                            env.fake.push(json!({ "update_id": next_update, "message": message }));
                            next_update += 1;
                            drain(env, &env.stores, 3).await;
                        }
                        Outgoing::Send { text, chat, thread_id } => {
                            let sent_before = env.fake.lock().sent.len();
                            let target = if *chat == Target::Other { OTHER_CHAT } else { CHAT };
                            let mut args = json!({ "text": text });
                            match chat {
                                Target::Default => {}
                                Target::Text => args["chatId"] = json!(CHAT.to_string()),
                                Target::Number => args["chatId"] = json!(CHAT),
                                Target::Other => args["chatId"] = json!(OTHER_CHAT),
                            }
                            if let Some(t) = thread_id {
                                args["messageThreadId"] = json!(t);
                            }
                            let outcome = call(&env.client, "send_telegram_message", args).await;
                            let after = observe(&env.server).await;
                            let delivered: Vec<Delivered> = env.fake.lock().sent[sent_before..].to_vec();
                            let new_rows = &after.log[before.log.len()..];
                            assert_eq!(after.signals.len(), before.signals.len(), "{at}: a send queued a signal");
                            assert_eq!(after.log[..before.log.len()], before.log[..], "{at}: a send changed earlier log rows");
                            if text.trim().is_empty() {
                                witness("a whitespace-only send is refused");
                                assert!(!matches!(outcome, Outcome::Ok(_)) && delivered.is_empty() && new_rows.is_empty(), "{at}: a whitespace-only text must be refused with nothing sent");
                                continue;
                            }
                            if text.encode_utf16().count() > 4 * 4096 {
                                assert!(!matches!(outcome, Outcome::Ok(_)) && delivered.is_empty(), "{at}: an over-long text must be refused with nothing sent");
                                continue;
                            }
                            let reply = ok_value(&outcome, "send_telegram_message");
                            let allowed: BTreeSet<String> = ["delivered", "logError", "chatId", "messageId", "messageIds", "parts", "messageThreadId", "date"].map(str::to_owned).into();
                            assert!(keys(&reply).is_subset(&allowed) && reply["delivered"] == true, "{at}: reply shape {reply}");
                            if delivered.len() > 1 {
                                witness("a long text goes out in several messages");
                            }
                            let texts: Vec<String> = delivered.iter().map(|d| d.text.clone()).collect();
                            assert!(covered_by_parts(text, &texts), "{at}: the delivered parts are not the text {:?}", texts.iter().map(String::len).collect::<Vec<_>>());
                            assert!(delivered.iter().all(|d| d.chat_id == target && d.thread_id == *thread_id), "{at}: a part went to the wrong chat or topic");
                            assert_eq!(reply["messageIds"], json!(delivered.iter().map(|d| d.message_id).collect::<Vec<_>>()), "{at}: messageIds");
                            assert_eq!(reply["messageId"], json!(delivered[0].message_id), "{at}: messageId");
                            assert_eq!(reply["parts"], json!(delivered.len()), "{at}: parts");
                            assert_eq!(reply["chatId"], json!(target.to_string()), "{at}: reply chat");
                            assert_eq!(reply["messageThreadId"], json!(thread_id), "{at}: reply topic");
                            let want: Vec<Assistant> =
                                delivered.iter().map(|d| (d.chat_id, Some(d.message_id), d.thread_id, "assistant", d.text.as_str())).collect();
                            let got: Vec<Assistant> =
                                new_rows.iter().map(|r| (r.chat_id, r.tg_message_id, r.thread_id, r.role.as_str(), r.text.as_str())).collect();
                            assert_eq!(got, want, "{at}: the log must gain one assistant row per delivered message");
                        }
                        Outgoing::Edit { pick, text, same } => {
                            let mine: Vec<Delivered> = env.fake.lock().chat.clone();
                            let Some(target) = mine.get(pick % mine.len().max(1)).cloned() else { continue };
                            let text = if *same { target.text.clone() } else { text.clone() };
                            let unchanged = text == target.text;
                            let outcome = call(&env.client, "edit_telegram_message", json!({ "chatId": target.chat_id, "messageId": target.message_id, "text": text })).await;
                            let after = observe(&env.server).await;
                            if unchanged || text.trim().is_empty() {
                                witness("an edit that changes nothing is refused");
                                assert!(!matches!(outcome, Outcome::Ok(_)), "{at}: an edit that changes nothing, or empties the message, must be refused");
                                assert_eq!(after, before, "{at}: a refused edit changed state");
                                continue;
                            }
                            let reply = ok_value(&outcome, "edit_telegram_message");
                            let allowed: BTreeSet<String> = ["logError", "edited", "chatId", "messageId", "date"].map(str::to_owned).into();
                            assert!(keys(&reply).is_subset(&allowed) && reply["edited"] == true, "{at}: reply shape {reply}");
                            assert_eq!(reply["messageId"], json!(target.message_id), "{at}: edited the wrong message");
                            witness("a message is edited");
                            let row = after.log.iter().find(|r| r.chat_id == target.chat_id && r.tg_message_id == Some(target.message_id) && r.role == "assistant");
                            assert_eq!(row.map(|r| r.text.as_str()), Some(text.as_str()), "{at}: the chat log must show the edited text");
                        }
                        Outgoing::History { mine, limit, thread_id } => {
                            let chat_id = if *mine { CHAT } else { OTHER_CHAT };
                            let mut args = json!({ "chatId": chat_id });
                            if let Some(l) = limit {
                                args["limit"] = json!(l);
                            }
                            if let Some(t) = thread_id {
                                args["threadId"] = json!(t);
                            }
                            let reply = ok_value(&call(&env.client, "get_telegram_chat_history", args).await, "get_telegram_chat_history");
                            let after = observe(&env.server).await;
                            assert_eq!(after, before, "{at}: reading history changed state");
                            let scope: Vec<&LogRow> = before.log.iter().filter(|r| r.chat_id == chat_id && thread_id.is_none_or(|t| r.thread_id == Some(t))).collect();
                            let n = limit.unwrap_or(50) as usize;
                            let want: Vec<i64> = scope[scope.len().saturating_sub(n)..].iter().map(|r| r.id).collect();
                            let messages = reply["messages"].as_array().expect("messages");
                            let got: Vec<i64> = messages.iter().map(|m| m["id"].as_i64().unwrap()).collect();
                            assert_eq!(got, want, "{at}: history is the last N rows of the chat (and topic), oldest first");
                            for m in messages {
                                let r = before.log.iter().find(|x| Some(x.id) == m["id"].as_i64()).expect("history row as stored");
                                assert!(
                                    m["text"] == json!(r.text) && m["role"] == json!(r.role) && m["thread_id"] == json!(r.thread_id) && m["tg_message_id"] == json!(r.tg_message_id),
                                    "{at}: history row as stored {m}"
                                );
                            }
                            if limit.is_some_and(|l| scope.len() as i64 > l) {
                                witness("history is cut to its limit");
                            }
                            if scope.iter().any(|r| r.role == "user") && scope.iter().any(|r| r.role == "assistant") {
                                witness("history interleaves both sides");
                            }
                        }
                    }
                }
            });
            Ok(())
        })
        .unwrap();
    assert_eq!(
        witnessed.into_inner().unwrap().into_iter().collect::<Vec<_>>(),
        [
            "a long text goes out in several messages",
            "a message is edited",
            "a whitespace-only send is refused",
            "an edit that changes nothing is refused",
            "history interleaves both sides",
            "history is cut to its limit",
        ]
    );
}

// A send that fails partway: the delivered parts are in the chat, so they are
// in the log and the error names them; nothing after the failure is.
#[test]
fn a_send_that_stops_partway_logs_exactly_the_delivered_parts() {
    let _serial = serial();
    let env = env();
    env.fake.reset();
    runtime().block_on(async {
        reset_world(&env.server).await;
        let text = ["first", "second", "third"].map(|b| format!("{b:.<3000}")).join("\n\n");
        env.fake.fail("sendMessage", 1, 1); // the second part fails
        let outcome = call(&env.client, "send_telegram_message", json!({ "text": text })).await;
        let world = observe(&env.server).await;
        let sent = env.fake.lock().sent.clone();
        assert_eq!(sent.iter().map(|d| d.text[..5].to_owned()).collect::<Vec<_>>(), ["first"]);
        let Outcome::ToolError(message) = outcome else { panic!("{outcome:?}") };
        assert!(message.contains(&sent[0].message_id.to_string()), "{message}");
        let logged: Vec<(Option<i64>, String)> = world.log.iter().map(|r| (r.tg_message_id, r.text.clone())).collect();
        let wanted: Vec<(Option<i64>, String)> = sent.iter().map(|d| (Some(d.message_id), d.text.clone())).collect();
        assert_eq!(logged, wanted);
    });
}

// Telegram accepted the message; the chat log write then failed. The tool
// must not report a failure — the model would send the message again.
#[test]
fn reports_a_delivered_message_as_delivered_even_when_the_chat_log_write_fails() {
    let _serial = serial();
    let env = env();
    env.fake.reset();
    runtime().block_on(async {
        let client = env.server.db.client().await.unwrap();
        client.batch_execute("ALTER TABLE telegram_messages RENAME TO telegram_messages_away").await.unwrap();
        let outcome = call(&env.client, "send_telegram_message", json!({ "text": "your bill is paid" })).await;
        client.batch_execute("ALTER TABLE telegram_messages_away RENAME TO telegram_messages").await.unwrap();
        let sent = env.fake.lock().sent.clone();
        assert_eq!(sent.iter().map(|d| d.text.as_str()).collect::<Vec<_>>(), ["your bill is paid"]);
        let reply = ok_value(&outcome, "send_telegram_message");
        assert_eq!(reply["messageIds"], json!(sent.iter().map(|d| d.message_id).collect::<Vec<_>>()));
        assert!(reply["logError"].as_str().is_some_and(|e| e.contains("chat log")), "{reply}");
    });
}

// ── send_telegram_message, long text (tools/telegram.test.ts) ────────────────

fn digest() -> String {
    format!("{}\n\n{}", "• headline ".repeat(300), "• another headline ".repeat(200))
}

#[test]
fn sends_a_text_over_4096_characters_as_consecutive_messages_and_logs_each() {
    let _serial = serial();
    let env = env();
    env.fake.reset();
    runtime().block_on(async {
        reset_world(&env.server).await;
        let text = digest();
        assert!(text.encode_utf16().count() > 4096);
        let reply = ok_value(
            &call(&env.client, "send_telegram_message", json!({ "chatId": CHAT, "text": text })).await,
            "send",
        );
        let sent = env.fake.lock().sent.clone();
        assert_eq!(reply["delivered"], true);
        assert_eq!(reply["messageId"], json!(sent[0].message_id));
        assert_eq!(reply["messageIds"], json!(sent.iter().map(|d| d.message_id).collect::<Vec<_>>()));
        assert_eq!(reply["parts"], 2);
        assert_eq!(sent.iter().map(|d| d.text.as_str()).collect::<String>(), text);
        assert!(sent.iter().all(|d| d.text.encode_utf16().count() <= 4096));
        let logged: Vec<(Option<i64>, String)> =
            observe(&env.server).await.log.into_iter().map(|r| (r.tg_message_id, r.text)).collect();
        assert_eq!(logged, sent.iter().map(|d| (Some(d.message_id), d.text.clone())).collect::<Vec<_>>());
    });
}

#[test]
fn keeps_a_short_text_a_single_message() {
    let _serial = serial();
    let env = env();
    env.fake.reset();
    runtime().block_on(async {
        let reply = ok_value(
            &call(&env.client, "send_telegram_message", json!({ "chatId": CHAT, "text": "a short message" })).await,
            "send",
        );
        let first = env.fake.lock().sent[0].message_id;
        assert_eq!(
            (reply["messageId"].clone(), reply["messageIds"].clone(), reply["parts"].clone()),
            (json!(first), json!([first]), json!(1))
        );
    });
}

#[test]
fn reports_the_parts_already_delivered_when_a_later_part_fails_and_logs_them() {
    let _serial = serial();
    let env = env();
    env.fake.reset();
    runtime().block_on(async {
        reset_world(&env.server).await;
        env.fake.fail("sendMessage", 1, 1);
        let outcome = call(&env.client, "send_telegram_message", json!({ "chatId": CHAT, "text": digest() })).await;
        let first = env.fake.lock().sent[0].message_id;
        let Outcome::ToolError(message) = outcome else { panic!("{outcome:?}") };
        assert!(message.contains(&format!("after 1 of 2 parts (delivered message ids: {first})")), "{message}");
        let logged: Vec<Option<i64>> = observe(&env.server).await.log.into_iter().map(|r| r.tg_message_id).collect();
        assert_eq!(logged, [Some(first)]);
    });
}

#[tokio::test]
async fn inline_buttons_split_edit_and_callback_signals() {
    use mcp_tools::telegram::{Button, send_long_message_buttons};
    let fake = fake_telegram::start("buttons").await;
    let bot = BotApi::with_root(reqwest::Client::new(), Some(fake.token.clone()), Some(fake.root.clone()));
    let buttons = vec![vec![Button { text: "Confirm".into(), data: "cf:yes".into() }]];
    let (_, sent) = send_long_message_buttons(&bot, "4242", &"x".repeat(5000), Some(7), Some(&buttons)).await.unwrap();
    let requests = fake.lock().requests.clone();
    let sends: Vec<_> = requests.iter().filter(|(m, _)| m == "sendMessage").collect();
    assert!(sends[0].1.get("reply_markup").is_none());
    assert_eq!(sends[1].1["reply_markup"]["inline_keyboard"][0][0], json!({"text":"Confirm","callback_data":"cf:yes"}));
    bot.edit_message_buttons("4242", sent[1].message_id, "Confirmed", None).await.unwrap();
    assert_eq!(fake.lock().requests.last().unwrap().1["reply_markup"], json!({"inline_keyboard":[]}));

    #[derive(Default)]
    struct Memory {
        cursor: Mutex<Option<i64>>,
        signals: Mutex<Vec<(String, Value)>>,
        log: Mutex<Vec<String>>,
    }
    #[async_trait]
    impl PollerStore for Memory {
        async fn last_update_id(&self) -> anyhow::Result<Option<i64>> {
            Ok(*self.cursor.lock().unwrap())
        }
        async fn set_last_update_id(&self, id: i64) -> anyhow::Result<()> {
            *self.cursor.lock().unwrap() = Some(id);
            Ok(())
        }
        async fn record_message(&self, _: i64, _: i64, _: Option<i64>, text: &str) -> anyhow::Result<()> {
            self.log.lock().unwrap().push(text.into());
            Ok(())
        }
        async fn record_signal(&self, source: &str, content: &str) -> anyhow::Result<()> {
            self.signals.lock().unwrap().push((source.into(), serde_json::from_str(content)?));
            Ok(())
        }
    }
    let press = |id, chat, data| {
        json!({"update_id":id,"callback_query":{"id":format!("q{id}"),"data":data,"message":{
        "message_id":1001,"chat":{"id":chat,"type":"private"},"message_thread_id":7,
        "reply_markup":{"inline_keyboard":[[{"text":"Confirm","callback_data":"cf:yes"}]]}}}})
    };
    fake.push(press(1, CHAT, "cf:yes"));
    fake.push(press(2, OTHER_CHAT, "cf:yes"));
    fake.push(press(3, CHAT, "missing"));
    fake.fail("answerCallbackQuery", 1, 0);
    let store = Memory::default();
    assert_eq!(poll_once(&bot, &store, CHAT, 0).await.unwrap(), 3);
    assert_eq!(*store.cursor.lock().unwrap(), Some(3));
    assert_eq!(*store.log.lock().unwrap(), ["[button] Confirm", "[button] missing"]);
    let signals = store.signals.lock().unwrap();
    assert_eq!(
        signals[0],
        (
            "telegram_button".into(),
            json!({"chatId":CHAT,"messageThreadId":7,"messageId":1001,"data":"cf:yes","text":"Confirm"})
        )
    );
    assert_eq!(signals[1].1["text"], Value::Null);
    assert_eq!(fake.lock().requests.iter().filter(|(m, _)| m == "answerCallbackQuery").count(), 2);
}
