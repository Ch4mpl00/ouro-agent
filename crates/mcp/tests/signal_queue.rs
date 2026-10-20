// The signal queue, judged from the outside: pollers record signals through
// the Signals module (their real boundary), the agent pops and lists them
// over MCP, and every step is checked against what Postgres actually holds
// before and after. A port of packages/mcp/test/signal-queue.test.ts.
//
// Rules are independent statements about one observed transition, named so
// a failure says which requirement broke:
//   - outcome: which reply is legal, decided from the request and the
//     pre-state only;
//   - effect: what the reply and the changed rows must look like;
//   - frame: what must not change, refusals included.
//
// PROPERTY_RUNS raises the number of generated sequences.

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use chrono::{TimeZone, Utc};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use serde_json::{Value, json};
use support::{Client, Outcome, Server, TestDatabase, call, connect, property_runs, runtime, start_test_server};

const DEFAULT_LIST_LIMIT: i64 = 200;

// ── observation ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
struct Stored {
    id: i64,
    source: String,
    content: String,
    created_us: i64,
    consumed_us: Option<i64>,
}

type World = BTreeMap<i64, Stored>;

async fn observe(server: &Server) -> World {
    server
        .db
        .client()
        .await
        .unwrap()
        .query(
            "SELECT id, source, content,
                    (extract(epoch FROM created_at) * 1000000)::bigint AS created_us,
                    (extract(epoch FROM consumed_at) * 1000000)::bigint AS consumed_us
               FROM signals ORDER BY id",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| {
            let s = Stored {
                id: r.get(0),
                source: r.get(1),
                content: r.get(2),
                created_us: r.get(3),
                consumed_us: r.get(4),
            };
            (s.id, s)
        })
        .collect()
}

fn pending(w: &World) -> Vec<&Stored> {
    w.values().filter(|s| s.consumed_us.is_none()).collect()
}

fn second(us: i64) -> i64 {
    us.div_euclid(1_000_000)
}

// The documented wire shape: "YYYY-MM-DD HH:MM:SS", UTC, truncated.
fn visible(us: i64) -> String {
    Utc.timestamp_opt(second(us), 0).unwrap().format("%Y-%m-%d %H:%M:%S").to_string()
}

fn keys(v: &Value) -> BTreeSet<String> {
    v.as_object().map(|m| m.keys().cloned().collect()).unwrap_or_default()
}

fn set(keys: &[&str]) -> BTreeSet<String> {
    keys.iter().map(|k| (*k).to_owned()).collect()
}

fn ok_value(outcome: &Outcome, op: &str) -> Value {
    match outcome {
        Outcome::Ok(v) => v.clone(),
        other => panic!("{op}: expected a result, got {other:?}"),
    }
}

// ── operations ───────────────────────────────────────────────────────────────

// `since` is generated relative to whatever the queue holds when the step
// runs: a stored signal's second, shifted, in either accepted spelling.
#[derive(Debug, Clone)]
enum Since {
    Around { pick: usize, shift: i64, iso: bool, ms: i64 },
    Garbage(&'static str),
}

#[derive(Debug, Clone)]
enum Limit {
    Int(i64),
    Fraction,
}

#[derive(Debug, Clone)]
enum Op {
    Record { source: &'static str, content: String },
    Next,
    List { source: Option<&'static str>, since: Option<Since>, limit: Option<Limit> },
}

const SOURCES: [&str; 5] = ["telegram", "gmail", "scheduler", "nashdom-bill", "dreaming"];

fn content() -> impl Strategy<Value = String> {
    prop_oneof![
        8 => "\\PC{0,120}",
        1 => Just(String::new()),
        1 => Just("🧾 Bill №42 — «NashDim»\n".repeat(800)),
    ]
}

fn since() -> impl Strategy<Value = Since> {
    prop_oneof![
        9 => (0usize..(i32::MAX as usize), proptest::sample::select(vec![-1i64, 0, 1]), any::<bool>(), 0i64..=999)
            .prop_map(|(pick, shift, iso, ms)| Since::Around { pick, shift, iso, ms }),
        1 => proptest::sample::select(vec!["", "yesterday", "2026-13-01 00:00:00", "2026-10-02 25:00:00", "not a date"]).prop_map(Since::Garbage),
    ]
}

fn limit() -> impl Strategy<Value = Option<Limit>> {
    prop_oneof![
        4 => Just(None),
        4 => (1i64..=6).prop_map(|n| Some(Limit::Int(n))),
        1 => proptest::sample::select(vec![DEFAULT_LIST_LIMIT, 2000]).prop_map(|n| Some(Limit::Int(n))),
        1 => prop_oneof![proptest::sample::select(vec![0i64, -1, 2001]).prop_map(Limit::Int), Just(Limit::Fraction)].prop_map(Some),
    ]
}

fn op() -> impl Strategy<Value = Op> {
    let mut sources = SOURCES.to_vec();
    sources.push("unknown-source");
    prop_oneof![
        5 => (proptest::sample::select(&SOURCES[..]), content()).prop_map(|(source, content)| Op::Record { source, content }),
        3 => Just(Op::Next),
        // An unfiltered listing with a small limit, so a cut listing is common.
        1 => (1i64..=3).prop_map(|n| Op::List { source: None, since: None, limit: Some(Limit::Int(n)) }),
        3 => (proptest::option::weighted(5.0 / 6.0, proptest::sample::select(sources)), proptest::option::weighted(5.0 / 6.0, since()), limit())
            .prop_map(|(source, since, limit)| Op::List { source, since, limit }),
    ]
}

// ── rules ────────────────────────────────────────────────────────────────────

#[derive(Default)]
struct Violations(Vec<String>);

impl Violations {
    fn check(&mut self, rule: &str, holds: bool, detail: impl FnOnce() -> String) {
        if !holds {
            self.0.push(format!("{rule}: {}", detail()));
        }
    }
}

// Every row of `before` other than `except` is still there, untouched.
fn frame(v: &mut Violations, rule: &str, before: &World, after: &World, except: Option<i64>) {
    for (id, row) in before {
        if Some(*id) == except {
            continue;
        }
        v.check(rule, after.get(id) == Some(row), || format!("{id}: {row:?} → {:?}", after.get(id)));
    }
}

fn no_new_rows(v: &mut Violations, rule: &str, before: &World, after: &World) {
    let added: Vec<&i64> = after.keys().filter(|id| !before.contains_key(id)).collect();
    v.check(rule, added.is_empty(), || format!("{added:?}"));
}

fn is_refusal(o: &Outcome) -> bool {
    matches!(o, Outcome::ToolError(_) | Outcome::Rejected { .. })
}

// Witnesses: interesting situations that must actually occur across a run, so
// a generator drifting into trivia is noticed.
static WITNESSED: Mutex<BTreeSet<&'static str>> = Mutex::new(BTreeSet::new());

fn witness(w: &'static str) {
    WITNESSED.lock().unwrap().insert(w);
}

const NEXT_KEYS: [&str; 5] = ["id", "source", "content", "created_at", "envContext"];
const LISTED_KEYS: [&str; 5] = ["id", "source", "content", "created_at", "consumed_at"];

async fn step(env: &Env, o: &Op) -> Vec<String> {
    let mut v = Violations::default();
    let before = observe(&env.server).await;

    match o {
        Op::Record { source, content } => {
            let id = env.server.signals.record(source, content).await.unwrap();
            let after = observe(&env.server).await;
            let row = after.get(&id);
            v.check("record: the returned id names a new row", row.is_some() && !before.contains_key(&id), || {
                id.to_string()
            });
            v.check(
                "record: the row holds exactly what was recorded",
                row.is_some_and(|r| r.source == *source && r.content == *content),
                || format!("{row:?}"),
            );
            v.check("record: a new signal is pending", row.is_some_and(|r| r.consumed_us.is_none()), || {
                format!("{row:?}")
            });
            v.check("record: ids only grow", before.keys().all(|old| *old < id), || {
                format!("{id} vs {:?}", before.keys().max())
            });
            v.check("record: exactly one row is added", after.len() == before.len() + 1, || {
                format!("{} → {}", before.len(), after.len())
            });
            frame(&mut v, "record frame: existing signals are untouched", &before, &after, None);
            if content.encode_utf16().count() > 10_000 {
                witness("a large signal round-trips");
            }
        }
        Op::Next => {
            let reply = ok_value(&call(&env.client, "get_next_signal", json!({})).await, "get_next_signal");
            let after = observe(&env.server).await;
            v.check("next: reply shape", keys(&reply) == set(&["signal", "pendingAfter"]), || reply.to_string());
            let waiting = pending(&before);
            let oldest = waiting.iter().min_by_key(|s| s.id);
            let Some(oldest) = oldest else {
                witness("an empty queue is asked");
                v.check("next on empty: replies null", reply["signal"].is_null(), || reply.to_string());
                v.check("next on empty: pendingAfter is 0", reply["pendingAfter"] == 0, || reply.to_string());
                frame(&mut v, "next on empty frame: nothing changes", &before, &after, None);
                no_new_rows(&mut v, "next on empty frame: nothing is added", &before, &after);
                return v.0;
            };
            if waiting.len() >= 2 {
                witness("a signal is taken while others wait");
            }
            let got = &reply["signal"];
            v.check("next: a waiting queue hands out a signal", !got.is_null(), || {
                format!("{} waiting", waiting.len())
            });
            if got.is_null() {
                return v.0;
            }
            v.check("next: signal shape", keys(got) == set(&NEXT_KEYS), || got.to_string());
            let id = got["id"].as_i64().unwrap();
            v.check("next: the oldest waiting signal comes out", id == oldest.id, || format!("{id} vs {}", oldest.id));
            let was = before.get(&id);
            v.check(
                "next: the signal comes out as recorded",
                was.is_some_and(|w| got["source"] == json!(w.source) && got["content"] == json!(w.content)),
                || got.to_string(),
            );
            v.check(
                "next: created_at is the stored time on the wire format",
                was.is_some_and(|w| got["created_at"] == json!(visible(w.created_us))),
                || got["created_at"].to_string(),
            );
            v.check("next: with nothing configured, envContext is null", got["envContext"].is_null(), || {
                got["envContext"].to_string()
            });
            v.check(
                "next: the handed-out signal is now consumed",
                after.get(&id).is_some_and(|r| r.consumed_us.is_some()),
                || format!("{:?}", after.get(&id)),
            );
            v.check(
                "next: pendingAfter counts what is still waiting",
                reply["pendingAfter"] == json!(pending(&after).len()),
                || reply["pendingAfter"].to_string(),
            );
            frame(&mut v, "next frame: every other signal is untouched", &before, &after, Some(id));
            no_new_rows(&mut v, "next frame: nothing is added", &before, &after);
        }
        Op::List { source, since, limit } => {
            let rows: Vec<&Stored> = before.values().collect();
            let pick = match since {
                Some(Since::Around { pick, .. }) => *pick,
                _ => 0,
            };
            let anchor_us = rows.get(pick % rows.len().max(1)).map_or(Utc::now().timestamp_micros(), |r| r.created_us);
            let (mut since_text, mut since_us) = (None, None);
            match since {
                Some(Since::Garbage(text)) => since_text = Some((*text).to_owned()),
                Some(Since::Around { shift, iso, ms, .. }) => {
                    let base = second(anchor_us) + shift;
                    if *iso {
                        let at = Utc.timestamp_millis_opt(base * 1000 + ms).unwrap();
                        since_text = Some(at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true));
                        since_us = Some((base * 1000 + ms) * 1000);
                    } else {
                        since_text = Some(visible(base * 1_000_000));
                        since_us = Some(base * 1_000_000);
                    }
                }
                None => {}
            }
            let mut args = json!({});
            if let Some(s) = source {
                args["source"] = json!(s);
            }
            if let Some(t) = &since_text {
                args["since"] = json!(t);
            }
            match limit {
                Some(Limit::Int(n)) => args["limit"] = json!(n),
                Some(Limit::Fraction) => args["limit"] = json!(1.5),
                None => {}
            }
            let outcome = call(&env.client, "list_signals", args.clone()).await;
            let after = observe(&env.server).await;
            frame(&mut v, "list frame: listing never changes a signal", &before, &after, None);
            no_new_rows(&mut v, "list frame: listing adds nothing", &before, &after);

            let valid_limit = match limit {
                None => true,
                Some(Limit::Int(n)) => (1..=2000).contains(n),
                Some(Limit::Fraction) => false,
            };
            if !valid_limit || matches!(since, Some(Since::Garbage(_))) {
                witness("an invalid list request is refused");
                v.check("list: an invalid limit or since is refused", is_refusal(&outcome), || {
                    format!("{args} {outcome:?}")
                });
                return v.0;
            }
            let reply = ok_value(&outcome, "list_signals");
            v.check("list: reply shape", keys(&reply) == set(&["count", "signals"]), || reply.to_string());
            let cap = match limit {
                Some(Limit::Int(n)) => *n as usize,
                _ => DEFAULT_LIST_LIMIT as usize,
            };
            let listed = reply["signals"].as_array().cloned().unwrap_or_default();
            let ids: Vec<i64> = listed.iter().map(|s| s["id"].as_i64().unwrap()).collect();
            v.check("list: count is the number of rows returned", reply["count"] == json!(listed.len()), || {
                reply["count"].to_string()
            });
            v.check("list: at most limit rows", listed.len() <= cap, || format!("{cap} vs {}", listed.len()));
            v.check("list: oldest first", ids.windows(2).all(|w| w[0] < w[1]), || format!("{ids:?}"));
            for s in &listed {
                v.check("list: row shape", keys(s) == set(&LISTED_KEYS), || s.to_string());
                let stored = before.get(&s["id"].as_i64().unwrap());
                v.check(
                    "list: every row is a stored signal, as stored",
                    stored.is_some_and(|st| s["source"] == json!(st.source) && s["content"] == json!(st.content)),
                    || s.to_string(),
                );
                let Some(stored) = stored else { continue };
                v.check(
                    "list: created_at on the wire format",
                    s["created_at"] == json!(visible(stored.created_us)),
                    || s["created_at"].to_string(),
                );
                v.check(
                    "list: consumed_at is set exactly when consumed",
                    s["consumed_at"].is_null() == stored.consumed_us.is_none(),
                    || s["consumed_at"].to_string(),
                );
                if let Some(c) = stored.consumed_us {
                    v.check("list: consumed_at on the wire format", s["consumed_at"] == json!(visible(c)), || {
                        s["consumed_at"].to_string()
                    });
                }
                v.check("list: the source filter holds", source.is_none_or(|f| s["source"] == json!(f)), || {
                    s["source"].to_string()
                });
                // Sub-second ordering inside since's own second is
                // unspecified; earlier seconds never come back.
                v.check(
                    "list: nothing from before since's second",
                    since_us.is_none_or(|u| second(stored.created_us) >= second(u)),
                    || format!("{since_text:?} vs {}", visible(stored.created_us)),
                );
            }
            // Completeness: every matching signal from a later second is
            // listed, up to the limit, and the limit cuts the newest ones off,
            // not the oldest.
            let owed: Vec<&&Stored> = rows
                .iter()
                .filter(|s| {
                    source.is_none_or(|f| s.source == f) && since_us.is_none_or(|u| second(s.created_us) > second(u))
                })
                .collect();
            let missing: Vec<i64> = owed.iter().map(|s| s.id).filter(|id| !ids.contains(id)).collect();
            let truncated = listed.len() == cap;
            let last_listed = ids.last().copied().unwrap_or(i64::MIN);
            v.check(
                "list: no matching signal is skipped",
                if truncated { missing.iter().all(|id| *id > last_listed) } else { missing.is_empty() },
                || format!("{missing:?} truncated={truncated}"),
            );
            if truncated && owed.len() > cap {
                witness("the limit cuts a listing");
            }
            if source.is_some_and(|f| rows.iter().any(|s| s.source != f)) {
                witness("the source filter excludes something");
            }
            if since_us.is_some_and(|u| rows.iter().any(|s| second(s.created_us) == second(u))) {
                witness("since falls inside a second that holds signals");
            }
        }
    }
    v.0
}

// ── suites ───────────────────────────────────────────────────────────────────

struct Env {
    db: TestDatabase,
    server: Server,
    client: Client,
}

fn env() -> &'static Env {
    static ENV: OnceLock<Env> = OnceLock::new();
    ENV.get_or_init(|| {
        runtime().block_on(async {
            let db = support::test_database().await;
            let server = start_test_server(&db, None).await.expect("server starts");
            let client = connect(&server.url).await;
            Env { db, server, client }
        })
    })
}

fn serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

async fn truncate(env: &Env) {
    env.server.db.client().await.unwrap().batch_execute("TRUNCATE signals RESTART IDENTITY").await.unwrap();
}

#[test]
fn keeps_every_rule_across_generated_sequences_of_record_next_list() {
    let _serial = serial();
    let env = env();
    TestRunner::new(Config::with_cases(property_runs(60)))
        .run(&proptest::collection::vec(op(), 1..=40), |ops| {
            runtime().block_on(async {
                truncate(env).await;
                for (i, o) in ops.iter().enumerate() {
                    let violations = step(env, o).await;
                    assert!(violations.is_empty(), "step {i} {:.300}\n  {}", format!("{o:?}"), violations.join("\n  "));
                }
            });
            Ok(())
        })
        .unwrap();
    assert_eq!(
        WITNESSED.lock().unwrap().iter().copied().collect::<Vec<_>>(),
        [
            "a large signal round-trips",
            "a signal is taken while others wait",
            "an empty queue is asked",
            "an invalid list request is refused",
            "since falls inside a second that holds signals",
            "the limit cuts a listing",
            "the source filter excludes something",
        ]
    );
}

struct Race<'a> {
    t0: Instant,
    recorded: &'a Mutex<BTreeMap<i64, (String, f64)>>,
    pops: &'a Mutex<Vec<(i64, String, f64, f64)>>,
    producing: &'a AtomicUsize,
}

impl Race<'_> {
    fn now(&self) -> f64 {
        self.t0.elapsed().as_secs_f64()
    }
}

async fn produce(env: &Env, race: &Race<'_>, p: usize, count: usize) {
    for i in 0..count {
        let content = format!("p{p}-{i}");
        let id = env.server.signals.record("telegram", &content).await.unwrap();
        race.recorded.lock().unwrap().insert(id, (content, race.now()));
    }
    race.producing.fetch_sub(1, Ordering::SeqCst);
}

async fn consume(client: &Client, race: &Race<'_>) {
    loop {
        let start = race.now();
        let reply = ok_value(&call(client, "get_next_signal", json!({})).await, "get_next_signal");
        let end = race.now();
        let s = &reply["signal"];
        if !s.is_null() {
            race.pops.lock().unwrap().push((
                s["id"].as_i64().unwrap(),
                s["content"].as_str().unwrap().to_owned(),
                start,
                end,
            ));
        } else if race.producing.load(Ordering::SeqCst) == 0 {
            return;
        }
    }
}

// Records and pops racing on separate sessions. Exactly once: every recorded
// signal is handed out once, to one consumer, as recorded. No overtaking: a
// pop never returns a newer signal while an older one, whose record had
// already returned when the pop started, is still waiting for a pop that began
// after this one ended.
#[test]
fn hands_out_every_signal_exactly_once_and_in_order_while_producers_and_consumers_race() {
    let _serial = serial();
    let env = env();
    const PRODUCERS: usize = 3;
    const PER_PRODUCER: usize = 30;
    const CONSUMERS: usize = 4;
    runtime().block_on(async {
        truncate(env).await;
        // A full instance is single-session (one supervisor), so the extra
        // consumers come from a second, signals-only instance on the same
        // database — the shape of two MCP processes sharing one queue. Each
        // session also runs two loops at once.
        let scoped = start_test_server(&env.db, Some("signals")).await.expect("scoped instance");
        let mut sessions = Vec::new();
        for _ in 0..CONSUMERS - 1 {
            sessions.push(connect(&scoped.url).await);
        }
        let clients: Vec<&Client> =
            [&env.client, &env.client].into_iter().chain(sessions.iter()).chain(sessions.iter()).collect();
        let t0 = Instant::now();
        let recorded: Mutex<BTreeMap<i64, (String, f64)>> = Mutex::default();
        let pops: Mutex<Vec<(i64, String, f64, f64)>> = Mutex::default();
        let producing = AtomicUsize::new(PRODUCERS);
        let race = Race { t0, recorded: &recorded, pops: &pops, producing: &producing };
        let producers = futures::future::join_all((0..PRODUCERS).map(|p| produce(env, &race, p, PER_PRODUCER)));
        let consumers = futures::future::join_all(clients.iter().map(|c| consume(c, &race)));
        futures::join!(producers, consumers);

        let recorded = recorded.into_inner().unwrap();
        let pops = pops.into_inner().unwrap();
        let mut delivered: Vec<i64> = pops.iter().map(|p| p.0).collect();
        delivered.sort();
        assert_eq!(delivered, recorded.keys().copied().collect::<Vec<_>>());
        assert_eq!(delivered.len(), PRODUCERS * PER_PRODUCER);
        for p in &pops {
            assert_eq!(Some(&p.1), recorded.get(&p.0).map(|r| &r.0));
        }
        let pop_of: BTreeMap<i64, &(i64, String, f64, f64)> = pops.iter().map(|p| (p.0, p)).collect();
        let mut overtakes = Vec::new();
        for p in &pops {
            for (older, rec) in &recorded {
                if *older >= p.0 || rec.1 >= p.2 {
                    continue;
                }
                if pop_of.get(older).is_none_or(|taker| taker.2 > p.3) {
                    overtakes.push(format!("{} handed out while {older} still waited", p.0));
                }
            }
        }
        assert_eq!(overtakes, Vec::<String>::new());
        drop(sessions);
    });
}
