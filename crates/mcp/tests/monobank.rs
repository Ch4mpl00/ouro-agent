// list_monobank_transactions over MCP against a stand-in for the Monobank
// Personal API, playing its documented rules: a statement covers at most 31
// days + 1 hour, comes newest first, holds at most 500 transactions (more need
// another request with an earlier `to`), and an account may be asked once per
// 60 s. A port of packages/mcp/test/monobank.test.ts.
//
// Rules, from the account's history and the tool's own window:
//   - every transaction it returns is one of the account's, inside the
//     window, newest first, money in major units with its currency;
//   - when it says the list is complete, it is exactly the window's
//     transactions — and it never says so when Monobank cut the list at 500;
//   - asking again within 60 s is a tool error that names the rate limit.

mod support;

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use axum::Json;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use mcp_tools::monobank::Monobank;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use serde_json::{Value, json};
use support::{
    Client, Outcome, Overrides, Server, TestDatabase, call, connect, property_runs, runtime, start_test_server_with,
};

const TOKEN: &str = "test-monobank-token";
const PAGE: usize = 500;
const MAX_RANGE_S: i64 = 31 * 24 * 3600 + 3600;

#[derive(Debug, Clone)]
struct Item {
    id: String,
    time: i64,
    amount: i64,
    currency_code: i64,
    cashback: i64,
}

// Monobank, as far as the tool can tell. Its rate limit runs on a clock the
// test advances, so "a minute later" costs nothing.
#[derive(Default)]
struct Fake {
    accounts: HashMap<String, Vec<Item>>,
    last_asked: HashMap<String, i64>,
    requests: Vec<(String, i64, i64)>,
    clock: i64,
}

type Shared = Arc<Mutex<Fake>>;

async fn statement(
    State(fake): State<Shared>,
    headers: HeaderMap,
    Path((account, from, to)): Path<(String, String, String)>,
) -> (StatusCode, Json<Value>) {
    let error =
        |code: u16, text: &str| (StatusCode::from_u16(code).unwrap(), Json(json!({ "errorDescription": text })));
    if headers.get("x-token").and_then(|v| v.to_str().ok()) != Some(TOKEN) {
        return error(403, "Unknown 'X-Token'");
    }
    let mut f = fake.lock().unwrap();
    let Some(history) = f.accounts.get(&account).cloned() else { return error(400, "Account not found") };
    let (Ok(from), Ok(to)) = (from.parse::<i64>(), to.parse::<i64>()) else {
        return error(400, "Period must be no more than 31 days");
    };
    if to < from || to - from > MAX_RANGE_S {
        return error(400, "Period must be no more than 31 days");
    }
    let now = f.clock;
    if f.last_asked.get(&account).is_some_and(|last| now - last < 60) {
        return error(429, "Too many requests");
    }
    f.last_asked.insert(account.clone(), now);
    f.requests.push((account, from, to));
    let mut items: Vec<&Item> = history.iter().filter(|i| i.time >= from && i.time <= to).collect();
    items.sort_by_key(|i| std::cmp::Reverse(i.time));
    let body: Vec<Value> = items
        .into_iter()
        .take(PAGE)
        .map(|i| {
            json!({
                "id": i.id, "time": i.time, "description": "Purchase", "mcc": 5411, "originalMcc": 5411, "hold": false,
                "amount": i.amount, "operationAmount": i.amount, "currencyCode": i.currency_code,
                "commissionRate": 0, "cashbackAmount": i.cashback, "balance": 1_000_000,
            })
        })
        .collect();
    (StatusCode::OK, Json(Value::Array(body)))
}

struct Env {
    _db: TestDatabase,
    _server: Server,
    client: Client,
    fake: Shared,
}

fn env() -> &'static Env {
    static ENV: OnceLock<Env> = OnceLock::new();
    ENV.get_or_init(|| {
        runtime().block_on(async {
            let fake: Shared = Arc::default();
            let app = axum::Router::new()
                .route("/personal/statement/{account}/{from}/{to}", axum::routing::get(statement))
                .with_state(fake.clone());
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let root = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
            tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            });
            let db = support::test_database().await;
            let monobank = Monobank::with_api(reqwest::Client::new(), Some(TOKEN.into()), &root);
            let server = start_test_server_with(
                &db,
                Some("monobank"),
                Overrides { monobank: Some(monobank), ..Overrides::default() },
            )
            .await
            .expect("server starts");
            let client = connect(&server.url).await;
            Env { _db: db, _server: server, client, fake }
        })
    })
}

fn currency(code: i64) -> &'static str {
    match code {
        980 => "UAH",
        840 => "USD",
        _ => "EUR",
    }
}

#[test]
fn returns_the_windows_transactions_says_when_monobank_cut_them_short_and_surfaces_the_rate_limit() {
    let env = env();
    let witnessed = Mutex::new(BTreeSet::new());
    let run = AtomicUsize::new(0);
    // An account's history: how many transactions, spread over how many days
    // back from now.
    let history = (prop_oneof![3 => 0usize..=40, 1 => 480usize..=800], 1i64..=45, 0i64..(i32::MAX as i64));
    // Usually a minute after the previous request; sometimes right away.
    let ask = (proptest::option::weighted(5.0 / 6.0, 1i64..=31), prop_oneof![4 => Just(true), 1 => Just(false)]);
    TestRunner::new(Config::with_cases(property_runs(80)))
        .run(&(history, proptest::collection::vec(ask, 1..=4)), |((count, span_days, seed), asks)| {
            runtime().block_on(async {
                let account = format!("acc-{}", run.fetch_add(1, Ordering::SeqCst) + 1);
                let now_s = chrono::Utc::now().timestamp();
                let items: Vec<Item> = (0..count as i64)
                    .map(|i| Item {
                        id: format!("{account}-tx{i}"),
                        time: now_s - ((i * 7919 + seed) % (span_days * 86400) + 1),
                        amount: -((i * 37 + seed) % 100_000) - 1,
                        currency_code: [980, 980, 840, 978][((seed + i) % 4) as usize],
                        cashback: i % 3,
                    })
                    .collect();
                env.fake.lock().unwrap().accounts.insert(account.clone(), items.clone());
                for (days, wait) in &asks {
                    env.fake.lock().unwrap().clock += if *wait { 61 } else { 1 };
                    let mut args = json!({ "accountId": account });
                    if let Some(d) = days {
                        args["days"] = json!(d);
                    }
                    let out = call(&env.client, "list_monobank_transactions", args).await;
                    let at = format!("count={count} span={span_days} {asks:?}");
                    let asked_before = env.fake.lock().unwrap().requests.iter().any(|r| r.0 == account);
                    if !wait && asked_before && !matches!(out, Outcome::Ok(_)) {
                        witnessed.lock().unwrap().insert("rate limited");
                        let Outcome::ToolError(message) = &out else { panic!("{at}: a 429 is a tool error: {out:?}") };
                        assert!(message.to_lowercase().contains("rate limit"), "{at}: {message}");
                        continue;
                    }
                    let Outcome::Ok(value) = out else { panic!("{at}: {out:?}") };
                    let ms = |k: &str| {
                        chrono::DateTime::parse_from_rfc3339(value[k].as_str().unwrap())
                            .unwrap()
                            .timestamp_millis()
                            .div_euclid(1000)
                    };
                    let (from_s, to_s) = (ms("from"), ms("to"));
                    let mut in_window: Vec<&Item> =
                        items.iter().filter(|i| i.time >= from_s && i.time <= to_s).collect();
                    in_window.sort_by_key(|i| std::cmp::Reverse(i.time));
                    assert_eq!(to_s - from_s, days.unwrap_or(7) * 86400, "{at}: the window is the days asked for");

                    // Each returned transaction is the account's own, rendered
                    // right, newest first.
                    let got = value["transactions"].as_array().expect("transactions");
                    let ids: Vec<&str> = got.iter().map(|t| t["id"].as_str().unwrap()).collect();
                    assert_eq!(
                        ids,
                        in_window.iter().take(ids.len()).map(|i| i.id.as_str()).collect::<Vec<_>>(),
                        "{at}: newest first, from the window"
                    );
                    for t in got {
                        let src = items.iter().find(|i| t["id"] == json!(i.id)).unwrap();
                        assert_eq!(t["amount"].as_f64(), Some(src.amount as f64 / 100.0), "{at}");
                        assert_eq!(t["currency"], json!(currency(src.currency_code)), "{at}");
                        let time = chrono::DateTime::from_timestamp(src.time, 0)
                            .unwrap()
                            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
                        assert_eq!(t["time"], json!(time), "{at}");
                    }
                    // Complete means complete.
                    let complete = value["complete"].as_bool();
                    if in_window.len() >= PAGE {
                        witnessed.lock().unwrap().insert("more than a page in the window");
                        assert_eq!(
                            complete,
                            Some(false),
                            "{at}: {} in the window, Monobank sent {PAGE} — not complete",
                            in_window.len()
                        );
                    }
                    if complete == Some(true) {
                        assert_eq!(ids.len(), in_window.len(), "{at}: complete means all of them");
                    } else {
                        assert!(in_window.len() >= PAGE, "{at}: only a cut list is incomplete");
                    }
                }
            });
            Ok(())
        })
        .unwrap();
    assert_eq!(
        witnessed.into_inner().unwrap().into_iter().collect::<Vec<_>>(),
        ["more than a page in the window", "rate limited"]
    );
}
