// The news store as the agent reads it: list_news and search_news over MCP
// against Postgres, on a generated corpus (sources, channels, dates, bodies
// that are often near-duplicates of each other, some rows whose embedding
// failed). A port of packages/mcp/test/news.test.ts. Expectations are
// relations over the stored rows — filters, ordering, distances recomputed
// from the stored vectors, no near-duplicate pairs, and nothing better left
// out — not a re-run of the ranking code.
//
// PROPERTY_RUNS raises the number of generated corpora.

mod support;

use std::collections::{BTreeMap, HashSet};
use std::sync::{Mutex, OnceLock};

use chrono::{DateTime, TimeZone, Utc};
use mcp_tools::news::{NewsItem, NewsRepository};
use mcp_tools::pg::{PgPool, parse_vector};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use serde_json::{Map, Value, json};
use support::{
    Client, Outcome, Server, TestDatabase, call, connect, property_runs, runtime, start_test_server, word_embedder,
    word_vectors,
};

const DEDUP: f64 = 0.03; // embeddings::DEFAULT_DEDUP_THRESHOLD
const EPS: f64 = 1e-6;

// ── observation ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
struct Row {
    id: i64,
    source: String,
    external_id: String,
    title: Option<String>,
    body: String,
    channel: Option<String>,
    posted_at: Option<i64>,
    fetched_at: i64,
    embedded_at: Option<i64>,
    embedding: Option<Vec<f64>>,
}

async fn observe(pool: &PgPool) -> Vec<Row> {
    let rows = pool
        .get()
        .await
        .unwrap()
        .query(
            "SELECT id, source, external_id, title, body, metadata ->> 'chat_username' AS channel,
                    posted_at, fetched_at, embedded_at, embedding::text AS embedding
               FROM news_items",
            &[],
        )
        .await
        .unwrap();
    let ms = |t: Option<DateTime<Utc>>| t.map(|t| t.timestamp_millis());
    rows.iter()
        .map(|r| Row {
            id: r.get(0),
            source: r.get(1),
            external_id: r.get(2),
            title: r.get(3),
            body: r.get(4),
            channel: r.get(5),
            posted_at: ms(r.get(6)),
            fetched_at: r.get::<_, DateTime<Utc>>(7).timestamp_millis(),
            embedded_at: ms(r.get(8)),
            embedding: r
                .get::<_, Option<String>>(9)
                .and_then(|v| parse_vector(&v))
                .map(|v| v.into_iter().map(f64::from).collect()),
        })
        .collect()
}

fn cosine_distance(a: &[f64], b: &[f64]) -> f64 {
    let (mut dot, mut na, mut nb) = (0.0, 0.0, 0.0);
    for (x, y) in a.iter().zip(b) {
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    1.0 - dot / (na.sqrt() * nb.sqrt())
}

// ── corpus and queries ───────────────────────────────────────────────────────

const WORDS: [&str; 12] =
    ["rust", "postgres", "llm", "agents", "kotlin", "tax", "ukraine", "startup", "gpu", "privacy", "ai", "compiler"];
const DAY: i64 = 24 * 3_600_000;
const SOURCES: [&str; 3] = ["hackernews", "habr", "channel"];
const CHANNELS: [&str; 2] = ["fop_news", "devua"];

fn t0() -> i64 {
    Utc.with_ymd_and_hms(2026, 10, 1, 0, 0, 0).unwrap().timestamp_millis()
}

fn word() -> impl Strategy<Value = String> {
    proptest::sample::select(&WORDS[..]).prop_map(str::to_owned)
}

fn sentence() -> impl Strategy<Value = String> {
    proptest::collection::vec(proptest::sample::select(&WORDS[..]), 1..=5).prop_map(|ws| ws.join(" "))
}

#[derive(Debug, Clone)]
struct Item {
    source: &'static str,
    external_id: String,
    title: Option<String>,
    body: String,
    channel: &'static str,
    posted_at: Option<i64>,
    embeds: bool,
}

fn item() -> impl Strategy<Value = Item> {
    (
        proptest::sample::select(&SOURCES[..]),
        (1u32..=40).prop_map(|n| n.to_string()),
        // Mostly untitled, short bodies: plenty of near-duplicates to dedup.
        prop_oneof![3 => Just(None), 1 => sentence().prop_map(Some)],
        prop_oneof![sentence(), word(), word().prop_map(|w| format!("{w} {w}"))],
        proptest::sample::select(&CHANNELS[..]),
        prop_oneof![1 => Just(None), 7 => (0..=10 * DAY).prop_map(|d| Some(t0() + d))],
        prop_oneof![5 => Just(true), 1 => Just(false)],
    )
        .prop_map(|(source, external_id, title, body, channel, posted_at, embeds)| Item {
            source,
            external_id,
            title,
            body,
            channel,
            posted_at,
            embeds,
        })
}

fn iso(ms: i64) -> String {
    Utc.timestamp_millis_opt(ms).unwrap().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

#[derive(Debug, Clone, Default)]
struct Filters {
    source: Option<&'static str>,
    since: Option<i64>,
    until: Option<i64>,
    channel: Option<&'static str>,
}

impl Filters {
    fn args(&self) -> Map<String, Value> {
        let mut out = Map::new();
        if let Some(s) = self.source {
            out.insert("source".into(), s.into());
        }
        if let Some(t) = self.since {
            out.insert("sinceISO".into(), iso(t).into());
        }
        if let Some(t) = self.until {
            out.insert("untilISO".into(), iso(t).into());
        }
        if let Some(c) = self.channel {
            out.insert("channel".into(), c.into());
        }
        out
    }
}

// Each filter present about a quarter of the time, so most queries still
// have candidates.
fn sometimes<S: Strategy + 'static>(s: S) -> impl Strategy<Value = Option<S::Value>>
where
    S::Value: Clone,
{
    prop_oneof![3 => Just(None), 1 => s.prop_map(Some)]
}

fn filters() -> impl Strategy<Value = Filters> {
    let instant = (-DAY..=11 * DAY).prop_map(|d| t0() + d);
    (
        sometimes(proptest::sample::select(&SOURCES[..])),
        sometimes(instant.clone()),
        sometimes(instant),
        sometimes(proptest::sample::select(&CHANNELS[..])),
    )
        .prop_map(|(source, since, until, channel)| Filters { source, since, until, channel })
}

// The channel filter only means something for channel posts; on other
// sources it is about chat_username, which they don't have.
fn matches(r: &Row, f: &Filters) -> bool {
    if f.source.is_some_and(|s| r.source != s) {
        return false;
    }
    if f.since.is_some_and(|t| r.posted_at.is_none_or(|p| p <= t)) {
        return false;
    }
    if f.until.is_some_and(|t| r.posted_at.is_none_or(|p| p > t)) {
        return false;
    }
    if f.channel.is_some_and(|c| r.channel.as_deref() != Some(c)) {
        return false;
    }
    true
}

fn value(o: Outcome, op: &str) -> Value {
    match o {
        Outcome::Ok(v) => v,
        other => panic!("{op}: {other:?}"),
    }
}

// ── suite ────────────────────────────────────────────────────────────────────

struct Env {
    _db: TestDatabase,
    server: Server,
    client: Client,
    repo: NewsRepository,
}

fn env() -> &'static Env {
    static ENV: OnceLock<Env> = OnceLock::new();
    ENV.get_or_init(|| {
        runtime().block_on(async {
            let db = support::test_database().await;
            let server = start_test_server(&db, Some("news-read")).await.expect("server starts");
            let client = connect(&server.url).await;
            let repo = NewsRepository::new(server.pool.clone(), word_embedder());
            Env { _db: db, server, client, repo }
        })
    })
}

fn serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn news_item(i: &Item) -> NewsItem {
    let mut metadata = Map::new();
    if i.source == "channel" {
        metadata.insert("chat_username".into(), i.channel.into());
    }
    NewsItem {
        source: i.source.into(),
        external_id: i.external_id.clone(),
        title: i.title.clone(),
        url: None,
        body: i.body.clone(),
        metadata,
        posted_at: i.posted_at.map(|ms| Utc.timestamp_millis_opt(ms).unwrap()),
    }
}

async fn seed(env: &Env, items: &[Item]) -> Vec<Row> {
    let pool = &env.server.pool;
    pool.get().await.unwrap().batch_execute("TRUNCATE news_items RESTART IDENTITY").await.unwrap();
    env.repo.save(&items.iter().map(news_item).collect::<Vec<_>>()).await.unwrap();
    // The embedder was down for some of them.
    let unembedded: Vec<String> = items
        .iter()
        .filter(|i| !i.embeds)
        .map(|i| format!("{}:{}", i.source, i.external_id))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    pool.get()
        .await
        .unwrap()
        .execute(
            "UPDATE news_items SET embedding = NULL, embedded_at = NULL WHERE source || ':' || external_id = ANY($1)",
            &[&unembedded],
        )
        .await
        .unwrap();
    observe(pool).await
}

fn sorted_by_id(mut rows: Vec<Row>) -> Vec<Row> {
    rows.sort_by_key(|r| r.id);
    rows
}

#[test]
fn save_keeps_the_first_copy_of_each_source_external_id_and_embeds_what_it_stored() {
    let _serial = serial();
    let env = env();
    TestRunner::new(Config::with_cases(property_runs(30)))
        .run(&proptest::collection::vec(item(), 1..=30), |items| {
            runtime().block_on(async {
                let all_embed: Vec<Item> = items.iter().cloned().map(|i| Item { embeds: true, ..i }).collect();
                let rows = seed(env, &all_embed).await;
                let mut firsts: BTreeMap<String, &Item> = BTreeMap::new();
                for i in &items {
                    firsts.entry(format!("{}:{}", i.source, i.external_id)).or_insert(i);
                }
                let mut keys: Vec<String> = rows.iter().map(|r| format!("{}:{}", r.source, r.external_id)).collect();
                keys.sort();
                assert_eq!(keys, firsts.keys().cloned().collect::<Vec<_>>());
                for r in &rows {
                    assert_eq!(
                        Some(&r.body),
                        firsts.get(&format!("{}:{}", r.source, r.external_id)).map(|i| &i.body),
                        "the first copy wins"
                    );
                    assert!(r.embedding.is_some(), "embedded on save");
                }
                // Saving the same batch again changes nothing.
                let again: Vec<NewsItem> = items
                    .iter()
                    .map(|i| NewsItem {
                        source: i.source.into(),
                        external_id: i.external_id.clone(),
                        title: i.title.clone(),
                        url: None,
                        body: format!("{} changed", i.body),
                        metadata: Map::new(),
                        posted_at: None,
                    })
                    .collect();
                assert_eq!(env.repo.save(&again).await.unwrap().saved, 0);
                assert_eq!(sorted_by_id(observe(&env.server.pool).await), sorted_by_id(rows));
            });
            Ok(())
        })
        .unwrap();
}

// Postgres text and jsonb can't hold a NUL — and one such item would fail the
// whole batch's insert, every tick. Each is stored as close as it can be, and
// the rest of the batch with it. (The TS test also feeds an Invalid Date; a
// DateTime here can't be one.)
#[test]
fn save_stores_a_batch_whole_even_when_items_carry_nuls() {
    let _serial = serial();
    let env = env();
    let taint = proptest::sample::select(vec!["none", "none", "nulBody", "nulTitle", "nulMetadata"]);
    TestRunner::new(Config::with_cases(property_runs(30)))
        .run(&proptest::collection::vec((item(), taint), 1..=20), |pairs| {
            runtime().block_on(async {
                let pool = &env.server.pool;
                pool.get().await.unwrap().batch_execute("TRUNCATE news_items RESTART IDENTITY").await.unwrap();
                let batch: Vec<NewsItem> = pairs
                    .iter()
                    .map(|(i, t)| {
                        let mut item = news_item(i);
                        match *t {
                            "nulBody" => item.body = format!("{}\0tail", item.body),
                            "nulTitle" => item.title = Some("head\0line".into()),
                            "nulMetadata" => {
                                item.metadata = json!({ "author": "a\0b", "tags": ["x\0"], "k\0": 1 })
                                    .as_object()
                                    .cloned()
                                    .unwrap_or_default()
                            }
                            _ => {}
                        }
                        item
                    })
                    .collect();
                env.repo.save(&batch).await.unwrap();
                let mut firsts: BTreeMap<String, &NewsItem> = BTreeMap::new();
                for b in &batch {
                    firsts.entry(format!("{}:{}", b.source, b.external_id)).or_insert(b);
                }
                let rows = pool
                    .get()
                    .await
                    .unwrap()
                    .query("SELECT source || ':' || external_id, title, body, metadata FROM news_items", &[])
                    .await
                    .unwrap();
                let at = format!("{:?}", pairs.iter().map(|(_, t)| *t).collect::<Vec<_>>());
                let mut keys: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
                keys.sort();
                assert_eq!(keys, firsts.keys().cloned().collect::<Vec<_>>(), "{at}: the whole batch is stored");
                let strip = |x: &str| x.replace('\0', "");
                for r in &rows {
                    let b = firsts[&r.get::<_, String>(0)];
                    assert_eq!(r.get::<_, String>(2), strip(&b.body), "{at}");
                    assert_eq!(r.get::<_, Option<String>>(1), b.title.as_deref().map(strip), "{at}");
                    let want =
                        serde_json::to_string(&Value::Object(b.metadata.clone())).unwrap().replace("\\u0000", "");
                    assert_eq!(r.get::<_, Value>(3), serde_json::from_str::<Value>(&want).unwrap(), "{at}");
                }
            });
            Ok(())
        })
        .unwrap();
}

#[test]
fn list_news_returns_matching_rows_in_posted_order_without_near_duplicates_leaving_nothing_out() {
    let _serial = serial();
    let env = env();
    let strategy =
        (proptest::collection::vec(item(), 1..=40), filters(), proptest::option::weighted(5.0 / 6.0, 1usize..=12));
    TestRunner::new(Config::with_cases(property_runs(30)))
        .run(&strategy, |(items, f, limit)| {
            runtime().block_on(async {
                let rows = seed(env, &items).await;
                let mut args = f.args();
                if let Some(limit) = limit {
                    args.insert("limit".into(), limit.into());
                }
                let reply = value(call(&env.client, "list_news", Value::Object(args)).await, "list_news");
                let cap = limit.unwrap_or(500);
                let got: Vec<&Row> = reply["items"]
                    .as_array()
                    .expect("items")
                    .iter()
                    .map(|it| {
                        rows.iter()
                            .find(|x| it["source"] == json!(x.source) && it["externalId"] == json!(x.external_id))
                            .unwrap_or_else(|| panic!("list_news returned a row not in the store: {it}"))
                    })
                    .collect();
                let at = format!("{f:?} limit={limit:?}");
                assert_eq!(reply["count"], json!(got.len()), "{at}");
                assert!(got.len() <= cap, "{at}: at most limit");
                assert_eq!(got.iter().map(|r| r.id).collect::<HashSet<_>>().len(), got.len(), "{at}: no row twice");
                for r in &got {
                    assert!(matches(r, &f), "{at}: {}:{} passes the filters", r.source, r.external_id);
                }
                // Order by posted_at: ascending with sinceISO, descending
                // otherwise (undated rows have no place in it — F11).
                let dated: Vec<i64> = got.iter().filter_map(|r| r.posted_at).collect();
                let mut sorted = dated.clone();
                if f.since.is_some() {
                    sorted.sort();
                } else {
                    sorted.sort_by(|a, b| b.cmp(a));
                }
                assert_eq!(dated, sorted, "{at}: posted order");
                let vecs: Vec<&Vec<f64>> = got.iter().filter_map(|r| r.embedding.as_ref()).collect();
                for i in 0..vecs.len() {
                    for j in i + 1..vecs.len() {
                        assert!(cosine_distance(vecs[i], vecs[j]) >= DEDUP - EPS, "{at}: two near-duplicates listed");
                    }
                }
                // Nothing left out: a matching row is missing only if the list
                // is full, or it duplicates a listed row.
                if got.len() < cap {
                    for r in rows.iter().filter(|x| matches(x, &f)) {
                        if got.iter().any(|g| g.id == r.id) {
                            continue;
                        }
                        let dup = r.embedding.as_ref().is_some_and(|e| {
                            got.iter().any(|g| g.embedding.as_ref().is_some_and(|ge| cosine_distance(ge, e) < DEDUP))
                        });
                        assert!(dup, "{at}: {}:{} left out of a short list", r.source, r.external_id);
                    }
                }
            });
            Ok(())
        })
        .unwrap();
}

#[test]
fn search_news_returns_the_closest_matching_rows_best_first_without_near_duplicates() {
    let _serial = serial();
    let env = env();
    let queries = prop_oneof![sentence().prop_map(|q| vec![q]), proptest::collection::vec(sentence(), 2..=3)];
    let strategy = (
        proptest::collection::vec(item(), 1..=40),
        filters(),
        queries,
        1usize..=12,
        proptest::option::weighted(5.0 / 6.0, 2usize..=5),
    );
    TestRunner::new(Config::with_cases(property_runs(30)))
        .run(&strategy, |(items, f, queries, k, chunks)| {
            runtime().block_on(async {
                let rows = seed(env, &items).await;
                let mut args = f.args();
                args.insert("k".into(), k.into());
                if queries.len() == 1 {
                    args.insert("query".into(), queries[0].clone().into());
                } else {
                    args.insert("queries".into(), json!(queries));
                }
                let flat = value(call(&env.client, "search_news", Value::Object(args.clone())).await, "search_news");
                // Without sinceISO a search covers the day before untilISO (or now).
                let f =
                    Filters { since: f.since.or(Some(f.until.unwrap_or(Utc::now().timestamp_millis()) - DAY)), ..f };
                let at = format!("{f:?} {queries:?} k={k}");
                let qv: Vec<Vec<f64>> =
                    word_vectors(&queries).into_iter().map(|v| v.into_iter().map(f64::from).collect()).collect();
                let candidates: Vec<&Row> = rows.iter().filter(|r| r.embedding.is_some() && matches(r, &f)).collect();
                let distance_of = |r: &Row| {
                    qv.iter().map(|v| cosine_distance(v, r.embedding.as_ref().unwrap())).fold(f64::INFINITY, f64::min)
                };

                let results = flat["results"].as_array().expect("results");
                let got: Vec<(&Row, &Value)> = results
                    .iter()
                    .map(|res| {
                        let id = res["id"].as_i64().expect("id");
                        (
                            rows.iter().find(|x| x.id == id).unwrap_or_else(|| panic!("an id not in the store: {id}")),
                            res,
                        )
                    })
                    .collect();
                assert_eq!(flat["count"], json!(got.len()), "{at}");
                assert!(got.len() <= k, "{at}: at most k");
                for (r, res) in &got {
                    assert!(r.embedding.is_some(), "{at}: only searchable rows");
                    assert!(matches(r, &f), "{at}: {} passes the filters", r.id);
                    let d = res["distance"].as_f64().expect("distance");
                    assert!(
                        (d - distance_of(r)).abs() < 1e-4,
                        "{at}: {}'s distance is its best over the queries",
                        r.id
                    );
                    if queries.len() > 1 {
                        assert!(
                            res["matchedQueries"].as_array().is_some_and(|m| !m.is_empty()),
                            "{at}: matchedQueries names a query"
                        );
                    }
                }
                let ds: Vec<f64> = got.iter().map(|(_, res)| res["distance"].as_f64().unwrap()).collect();
                let mut sorted = ds.clone();
                sorted.sort_by(f64::total_cmp);
                assert_eq!(ds, sorted, "{at}: best first");
                for i in 0..got.len() {
                    for j in i + 1..got.len() {
                        let (a, b) = (got[i].0.embedding.as_ref().unwrap(), got[j].0.embedding.as_ref().unwrap());
                        assert!(cosine_distance(a, b) >= DEDUP - EPS, "{at}: near-duplicates returned");
                    }
                }
                // Nothing better left out: a missing candidate is no closer
                // than the last result (when k are back), duplicates a
                // returned row, or lies beyond each query's candidate pool.
                let pool_size = (k * 2).max(30);
                let rank_of = |r: &Row| {
                    qv.iter()
                        .map(|v| {
                            let dr = cosine_distance(v, r.embedding.as_ref().unwrap());
                            candidates
                                .iter()
                                .filter(|c| cosine_distance(v, c.embedding.as_ref().unwrap()) < dr - EPS)
                                .count()
                        })
                        .min()
                        .unwrap_or(0)
                };
                let worst = ds.last().copied().unwrap_or(f64::INFINITY);
                for c in &candidates {
                    if got.iter().any(|(g, _)| g.id == c.id) {
                        continue;
                    }
                    let dc = distance_of(c);
                    let full_and_no_closer = got.len() == k && dc >= worst - 1e-4;
                    let dup = got.iter().any(|(g, _)| {
                        cosine_distance(g.embedding.as_ref().unwrap(), c.embedding.as_ref().unwrap()) < DEDUP
                    });
                    let outside_pool = rank_of(c) >= pool_size;
                    assert!(full_and_no_closer || dup || outside_pool, "{at}: {} (distance {dc}) left out", c.id);
                }
                if !candidates.is_empty() {
                    assert!(!got.is_empty(), "{at}: something matches, so something comes back");
                }

                if let Some(n) = chunks {
                    args.insert("chunks".into(), n.into());
                    let split =
                        value(call(&env.client, "search_news", Value::Object(args)).await, "search_news chunks");
                    let parts = split["chunks"].as_array().expect("chunks");
                    assert_eq!(parts.len(), n, "{at}: exactly n chunks");
                    let flat_ids: Vec<&Value> = results.iter().map(|x| &x["id"]).collect();
                    let split_ids: Vec<&Value> =
                        parts.iter().flat_map(|c| c.as_array().unwrap()).map(|x| &x["id"]).collect();
                    assert_eq!(split_ids, flat_ids, "{at}: chunks are the results, in order");
                    let sizes: Vec<usize> = parts.iter().map(|c| c.as_array().unwrap().len()).collect();
                    assert!(sizes.iter().max().unwrap() - sizes.iter().min().unwrap() <= 1, "{at}: balanced");
                    let mut desc = sizes.clone();
                    desc.sort_by(|a, b| b.cmp(a));
                    assert_eq!(sizes, desc, "{at}: trailing chunks are the short ones");
                }
            });
            Ok(())
        })
        .unwrap();
}

// Near-duplicates at the top must not crowd out the next distinct match: k
// results come back as long as k distinct ones exist.
#[test]
fn dedup_at_the_top_leaves_room_for_the_next_distinct_match() {
    let _serial = serial();
    let env = env();
    runtime().block_on(async {
        let item = |id: &str, body: &str| Item {
            source: "habr",
            external_id: id.into(),
            title: None,
            body: body.into(),
            channel: "devua",
            posted_at: Some(t0()),
            embeds: true,
        };
        seed(env, &[item("1", "rust"), item("2", "rust rust"), item("3", "rust tax")]).await;
        let reply = value(
            call(&env.client, "search_news", json!({ "query": "rust", "k": 2, "sinceISO": iso(t0() - DAY) })).await,
            "search_news",
        );
        assert_eq!(reply["results"].as_array().unwrap().len(), 2);
    });
}

#[test]
fn an_invalid_date_filter_is_refused_not_a_crash() {
    let _serial = serial();
    let env = env();
    runtime().block_on(async {
        for tool in ["list_news", "search_news"] {
            let outcome = call(&env.client, tool, json!({ "query": "rust", "sinceISO": "yesterday" })).await;
            let ok = matches!(&outcome, Outcome::Ok(v) if v.get("error").is_none());
            assert!(!ok, "{tool}: {outcome:?}");
        }
    });
}
