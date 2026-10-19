// Unified memory: facts and documents against their search projection
// (memory_index), on a real Postgres. A port of packages/mcp/test/
// memory-index.test.ts.
//
// 1. Over MCP: generated remember / update_fact / get_fact / recall
//    sequences; after every step the index mirrors every fact exactly (text,
//    state, tags), and recall only ever hands out refs that resolve.
// 2. Through the memory service with an embedder of random latency: writes
//    racing on one fact or one document leave the index describing the
//    last write, not whichever write finished embedding last.
//
// PROPERTY_RUNS raises the number of generated sequences.

mod support;

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use mcp_tools::embeddings::Embedder;
use mcp_tools::memory::{MemoryService, MemoryState, PgMemoryStore, WriteDoc};
use mcp_tools::pg::PgPool;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use serde_json::{Value, json};
use support::{
    Client, Outcome, Server, TestDatabase, call, connect, property_runs, runtime, start_test_server, word_vectors,
};

// ── environment ──────────────────────────────────────────────────────────────

struct Env {
    _db: TestDatabase,
    server: Server,
    client: Client,
}

fn env() -> &'static Env {
    static ENV: OnceLock<Env> = OnceLock::new();
    ENV.get_or_init(|| {
        runtime().block_on(async {
            let db = support::test_database().await;
            let server = start_test_server(&db, Some("memory")).await.expect("server starts");
            let client = connect(&server.url).await;
            Env { _db: db, server, client }
        })
    })
}

// The TS file's tests run one after another on one database.
fn serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

async fn sql(pool: &PgPool, statement: &str) {
    pool.get().await.unwrap().batch_execute(statement).await.unwrap();
}

// ── observation ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
struct StoredFact {
    id: i64,
    body: String,
    tags: Vec<String>,
    state: String,
}

#[derive(Debug, Clone)]
struct IndexRow {
    source_ref: String,
    r#ref: String,
    text: String,
    tags: Vec<String>,
    state: String,
}

struct World {
    facts: BTreeMap<i64, StoredFact>,
    index: Vec<IndexRow>,
}

async fn observe(pool: &PgPool) -> World {
    let client = pool.get().await.unwrap();
    let facts = client
        .query("SELECT id, body, tags, state FROM memory_facts ORDER BY id", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| {
            let f = StoredFact { id: r.get(0), body: r.get(1), tags: r.get(2), state: r.get(3) };
            (f.id, f)
        })
        .collect();
    let index = client
        .query("SELECT source_ref, ref, text, tags, state FROM memory_index ORDER BY source_ref, ref", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| IndexRow { source_ref: r.get(0), r#ref: r.get(1), text: r.get(2), tags: r.get(3), state: r.get(4) })
        .collect();
    World { facts, index }
}

// The projection of a fact is one row carrying exactly its body, tags and
// state.
fn index_mirrors_facts(world: &World) -> Vec<String> {
    let mut problems = Vec::new();
    for fact in world.facts.values() {
        let owner = format!("fact:{}", fact.id);
        let rows: Vec<&IndexRow> = world.index.iter().filter(|r| r.source_ref == owner).collect();
        if rows.len() != 1 {
            problems.push(format!("{owner} has {} index rows", rows.len()));
            continue;
        }
        let row = rows[0];
        if row.r#ref != owner || row.text != fact.body || row.state != fact.state || row.tags != fact.tags {
            problems.push(format!("{owner} index {row:?} ≠ fact {fact:?}"));
        }
    }
    for row in &world.index {
        if let Some(id) = row.source_ref.strip_prefix("fact:").and_then(|id| id.parse::<i64>().ok())
            && !world.facts.contains_key(&id)
        {
            problems.push(format!("index row for a fact that doesn't exist: {}", row.source_ref));
        }
    }
    problems
}

// ── replies ──────────────────────────────────────────────────────────────────

fn value(outcome: Outcome, op: &str) -> Value {
    match outcome {
        Outcome::Ok(v) => v,
        other => panic!("{op}: expected a result, got {other:?}"),
    }
}

fn reply_fact(v: &Value) -> StoredFact {
    assert_eq!(v["ok"], true, "{v}");
    let f = &v["fact"];
    StoredFact {
        id: f["id"].as_i64().expect("fact.id"),
        body: f["body"].as_str().expect("fact.body").to_owned(),
        tags: f["tags"].as_array().expect("fact.tags").iter().map(|t| t.as_str().unwrap().to_owned()).collect(),
        state: f["state"].as_str().expect("fact.state").to_owned(),
    }
}

fn refused(o: &Outcome) -> bool {
    match o {
        Outcome::Ok(v) => v.get("ok") == Some(&Value::Bool(false)),
        _ => true,
    }
}

// ── operations ───────────────────────────────────────────────────────────────

const WORDS: [&str; 10] =
    ["router", "password", "internet", "bill", "alex", "tram", "lisbon", "pills", "monday", "garage"];

fn sentence() -> impl Strategy<Value = String> {
    proptest::collection::vec(proptest::sample::select(&WORDS[..]), 2..=6).prop_map(|ws| ws.join(" "))
}

fn body() -> impl Strategy<Value = String> {
    prop_oneof![
        6 => sentence(),
        1 => proptest::sample::select(vec!["  padded  body  ", "   ", "\n\t"]).prop_map(str::to_owned),
    ]
}

fn tags() -> impl Strategy<Value = Vec<String>> {
    proptest::collection::vec(
        proptest::sample::select(vec!["home", "money", "travel", " home ", "health"]).prop_map(str::to_owned),
        0..=4,
    )
}

// fast-check's fc.option: absent one time in six.
fn option<S: Strategy>(s: S) -> impl Strategy<Value = Option<S::Value>> {
    proptest::option::weighted(5.0 / 6.0, s)
}

#[derive(Debug, Clone)]
enum Query {
    Text(String),
    ExactOf(usize),
}

#[derive(Debug, Clone)]
enum Op {
    Remember { body: String, tags: Option<Vec<String>> },
    Update { pick: usize, body: Option<String>, tags: Option<Vec<String>>, state: Option<&'static str> },
    Get { pick: usize },
    Recall { query: Query, tags: Option<Vec<String>>, limit: Option<i64> },
}

fn nat() -> impl Strategy<Value = usize> {
    0usize..(i32::MAX as usize)
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (body(), option(tags())).prop_map(|(body, tags)| Op::Remember { body, tags }),
        // Blank more often than in `remember`, so a refused update shows up
        // in every run.
        3 => (
            nat(),
            option(prop_oneof![3 => body(), 1 => proptest::sample::select(vec!["   ", "\n\t"]).prop_map(str::to_owned)]),
            option(tags()),
            option(proptest::sample::select(vec!["active", "archived"])),
        )
            .prop_map(|(pick, body, tags, state)| Op::Update { pick, body, tags, state }),
        1 => nat().prop_map(|pick| Op::Get { pick }),
        // Asking for an active fact word for word, no filters.
        2 => nat().prop_map(|n| Op::Recall { query: Query::ExactOf(n), tags: None, limit: Some(50) }),
        3 => (
            prop_oneof![sentence().prop_map(Query::Text), nat().prop_map(Query::ExactOf)],
            option(proptest::collection::vec(
                proptest::sample::select(vec!["home", "money", "travel"]).prop_map(str::to_owned),
                1..=2
            )),
            option(1i64..=50),
        )
            .prop_map(|(query, tags, limit)| Op::Recall { query, tags, limit }),
    ]
}

fn trimmed_tags(t: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for x in t.iter().map(|x| x.trim()).filter(|x| !x.is_empty()) {
        if !out.iter().any(|y| y == x) {
            out.push(x.to_owned());
        }
    }
    out
}

async fn step(env: &Env, i: usize, o: &Op, witnessed: &Mutex<HashSet<&'static str>>) {
    let pool = &env.server.pool;
    let client = &env.client;
    let before = observe(pool).await;
    let ids: Vec<i64> = before.facts.keys().copied().collect();
    let at = format!("step {i} {o:?}");

    match o {
        Op::Remember { body, tags } => {
            let mut args = json!({ "body": body });
            if let Some(tags) = tags {
                args["tags"] = json!(tags);
            }
            let outcome = call(client, "remember", args).await;
            let after = observe(pool).await;
            if body.trim().is_empty() {
                assert!(refused(&outcome), "{at}: a blank fact is refused");
                assert_eq!(after.facts.len(), before.facts.len(), "{at}: nothing stored");
            } else {
                let got = reply_fact(&value(outcome, "remember"));
                assert_eq!(
                    after.facts.get(&got.id),
                    Some(&StoredFact {
                        id: got.id,
                        body: body.trim().to_owned(),
                        tags: trimmed_tags(tags.as_deref().unwrap_or_default()),
                        state: "active".into(),
                    }),
                    "{at}: stored as returned"
                );
            }
        }
        Op::Update { pick, body, tags, state } => {
            if ids.is_empty() {
                return;
            }
            let id = ids[pick % ids.len()];
            let was = before.facts[&id].clone();
            let mut args = json!({ "id": id });
            if let Some(body) = body {
                args["body"] = json!(body);
            }
            if let Some(tags) = tags {
                args["tags"] = json!(tags);
            }
            if let Some(state) = state {
                args["state"] = json!(state);
            }
            let outcome = call(client, "update_fact", args).await;
            let after = observe(pool).await;
            if body.as_deref().is_some_and(|b| b.trim().is_empty()) {
                witnessed.lock().unwrap().insert("a blank update is refused");
                assert!(refused(&outcome), "{at}: a fact can't be blanked by an update either");
                assert_eq!(after.facts.get(&id), Some(&was), "{at}: a refused update changes nothing");
            } else {
                reply_fact(&value(outcome, "update_fact"));
                assert_eq!(
                    after.facts.get(&id),
                    Some(&StoredFact {
                        id,
                        body: body.as_deref().map_or(was.body.clone(), |b| b.trim().to_owned()),
                        tags: tags.as_deref().map_or(was.tags.clone(), trimmed_tags),
                        state: state.map_or(was.state.clone(), str::to_owned),
                    }),
                    "{at}: exactly the given fields change"
                );
                if *state == Some("archived") {
                    witnessed.lock().unwrap().insert("a fact is archived");
                }
            }
            for (other, f) in &before.facts {
                if *other != id {
                    assert_eq!(after.facts.get(other), Some(f), "{at}: other facts untouched");
                }
            }
        }
        Op::Get { pick } => {
            if ids.is_empty() {
                return;
            }
            let id = ids[pick % ids.len()];
            let got = reply_fact(&value(call(client, "get_fact", json!({ "id": id })).await, "get_fact"));
            assert_eq!(Some(&got), before.facts.get(&id), "{at}: get_fact returns the stored fact");
        }
        Op::Recall { query, tags, limit } => {
            let active: Vec<&StoredFact> = before.facts.values().filter(|f| f.state == "active").collect();
            let exact = match query {
                Query::ExactOf(n) => active.get(n % active.len().max(1)).copied(),
                Query::Text(_) => None,
            };
            let text = match query {
                Query::Text(t) => t.clone(),
                Query::ExactOf(_) => exact.map_or("router".to_owned(), |f| f.body.clone()),
            };
            let mut args = json!({ "query": text });
            if let Some(tags) = tags {
                args["tags"] = json!(tags);
            }
            if let Some(limit) = limit {
                args["limit"] = json!(limit);
            }
            let reply = value(call(client, "recall", args).await, "recall");
            assert_eq!(reply["ok"], true, "{at}: {reply}");
            let hits = reply["hits"].as_array().expect("hits");
            assert!(hits.len() as i64 <= limit.unwrap_or(10), "{at}: at most limit hits");
            for hit in hits {
                let r = hit["ref"].as_str().expect("ref");
                let id: i64 = r
                    .strip_prefix("fact:")
                    .and_then(|id| id.parse().ok())
                    .unwrap_or_else(|| panic!("{at}: only facts exist here, got {r}"));
                let f = before.facts.get(&id).unwrap_or_else(|| panic!("{at}: {r} resolves"));
                assert_eq!(f.state, "active", "{at}: archived facts stay out of recall");
                if let Some(tags) = tags {
                    assert!(f.tags.iter().any(|t| tags.contains(t)), "{at}: {r} carries a requested tag");
                }
                let resolved = reply_fact(&value(call(client, "get_fact", json!({ "id": id })).await, "get_fact"));
                assert_eq!(resolved.body, hit["text"].as_str().unwrap(), "{at}: the hit's text is the fact");
            }
            // Progress: an active fact asked for word for word is found.
            if let Some(exact) = exact
                && tags.is_none()
                && limit.unwrap_or(10) >= before.facts.len() as i64
            {
                witnessed.lock().unwrap().insert("recall finds a fact by its own words");
                let refs: Vec<&str> = hits.iter().map(|h| h["ref"].as_str().unwrap()).collect();
                assert!(
                    refs.contains(&format!("fact:{}", exact.id).as_str()),
                    "{at}: recall finds fact:{} by its own body",
                    exact.id
                );
            }
        }
    }

    assert_eq!(index_mirrors_facts(&observe(pool).await), Vec::<String>::new(), "{at}: index ≠ facts");
}

#[test]
fn keeps_the_index_a_mirror_of_the_facts_and_recalls_refs_resolvable() {
    let _serial = serial();
    let env = env();
    let witnessed = Mutex::new(HashSet::new());
    let mut runner = TestRunner::new(Config::with_cases(property_runs(30)));
    runner
        .run(&proptest::collection::vec(op(), 1..=20), |ops| {
            runtime().block_on(async {
                sql(&env.server.pool, "TRUNCATE memory_facts, memory_index RESTART IDENTITY").await;
                for (i, o) in ops.iter().enumerate() {
                    step(env, i, o, &witnessed).await;
                }
            });
            Ok(())
        })
        .unwrap();
    let mut seen: Vec<&str> = witnessed.into_inner().unwrap().into_iter().collect();
    seen.sort();
    assert_eq!(seen, ["a blank update is refused", "a fact is archived", "recall finds a fact by its own words"]);
}

// ── racing writes, slow embedder ─────────────────────────────────────────────

// Bag-of-words vectors, as in the spec environment, after a random delay —
// so a write's embedding can finish after a later write's.
struct SlowEmbedder;

#[async_trait]
impl Embedder for SlowEmbedder {
    async fn embed_batch(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
        let delay = rand::random::<u64>() % 30;
        tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
        Ok(word_vectors(texts))
    }
}

fn slow_memory(env: &Env) -> MemoryService {
    MemoryService::new(Arc::new(PgMemoryStore::new(env.server.pool.clone())), Arc::new(SlowEmbedder))
}

#[test]
fn the_index_describes_the_last_update_of_a_fact_whatever_order_the_embeddings_finish_in() {
    let _serial = serial();
    let env = env();
    let memory = slow_memory(env);
    let update = (sentence(), proptest::sample::select(vec![MemoryState::Active, MemoryState::Archived]));
    let mut runner = TestRunner::new(Config::with_cases(25));
    runner
        .run(&proptest::collection::vec(update, 2..=5), |updates| {
            runtime().block_on(async {
                sql(&env.server.pool, "TRUNCATE memory_facts, memory_index RESTART IDENTITY").await;
                let f = memory.remember("the starting point", None, None).await.unwrap();
                futures::future::join_all(
                    updates.iter().map(|(body, state)| memory.update_fact(f.id, Some(body), None, Some(*state))),
                )
                .await;
                assert_eq!(index_mirrors_facts(&observe(&env.server.pool).await), Vec::<String>::new(), "{updates:?}");
            });
            Ok(())
        })
        .unwrap();
}

// Re-indexing one unchanged source from several places at once (a write and
// the backfill, two instances) must not interleave delete and insert.
#[test]
fn concurrent_reindexes_of_one_source_leave_one_set_of_rows() {
    let _serial = serial();
    let env = env();
    let memory = slow_memory(env);
    runtime().block_on(async {
        sql(
            &env.server.pool,
            "TRUNCATE memory_facts, memory_projects, memory_project_docs, memory_doc_patches, memory_index RESTART IDENTITY CASCADE",
        )
        .await;
        let f = memory.remember("router password on the sticker", None, None).await.unwrap();
        memory.create_project("p", "P").await.unwrap();
        let written = memory
            .write_doc(WriteDoc { searchable: None,
                project: "p",
                doc: "notes.md",
                body: "## A\n\nalpha\n\n## B\n\nbeta",
                summary: None,
                expected_version: None,
                actor: "a",
                rationale: None,
            })
            .await
            .unwrap();
        let doc = memory.read_doc("p", "notes.md").await.unwrap();
        let project = memory.list_projects().await.unwrap().into_iter().find(|x| x.slug == "p").expect("project p");
        let indexer = memory.indexer();
        let facts = (0..8).map(|_| async { indexer.index_fact(&f).await.unwrap() });
        let docs = (0..8).map(|_| async { indexer.index_doc(&project, &doc).await.unwrap() });
        futures::future::join(futures::future::join_all(facts), futures::future::join_all(docs)).await;
        let world = observe(&env.server.pool).await;
        let owner = format!("fact:{}", f.id);
        assert_eq!(world.index.iter().filter(|r| r.source_ref == owner).count(), 1);
        let chunks: Vec<&str> =
            world.index.iter().filter(|r| r.source_ref == "doc:p/notes.md").map(|r| r.r#ref.as_str()).collect();
        let unique: HashSet<&str> = chunks.iter().copied().collect();
        assert_eq!(chunks.len(), unique.len(), "doc v{}", written.version);
    });
}

#[test]
fn the_index_describes_the_last_write_of_a_document_whatever_order_the_embeddings_finish_in() {
    let _serial = serial();
    let env = env();
    let memory = slow_memory(env);
    let mut runner = TestRunner::new(Config::with_cases(25));
    runner
        .run(&proptest::collection::vec(sentence(), 2..=5), |lines| {
            runtime().block_on(async {
                sql(
                    &env.server.pool,
                    "TRUNCATE memory_projects, memory_project_docs, memory_doc_patches, memory_index RESTART IDENTITY CASCADE",
                )
                .await;
                memory.create_project("p", "P").await.unwrap();
                memory
                    .write_doc(WriteDoc { searchable: None,
                        project: "p",
                        doc: "notes.md",
                        body: "start",
                        summary: None,
                        expected_version: None,
                        actor: "a",
                        rationale: Some("seed"),
                    })
                    .await
                    .unwrap();
                // Appends retry a lost compare-and-swap a few times; one that
                // still loses answers "raced" and changes nothing (the model
                // allows it — see F9). Whatever landed, the index is the final
                // document's.
                let texts: Vec<String> = lines.iter().enumerate().map(|(i, line)| format!("{line} #{i}")).collect();
                let settled = futures::future::join_all(
                    texts.iter().map(|text| memory.append_doc("p", "notes.md", text, None, "a", Some("race"))),
                )
                .await;
                let doc = memory.read_doc("p", "notes.md").await.unwrap();
                let world = observe(&env.server.pool).await;
                let indexed: Vec<&str> =
                    world.index.iter().filter(|r| r.source_ref == "doc:p/notes.md").map(|r| r.text.as_str()).collect();
                let indexed = indexed.join("\n");
                for (i, text) in texts.iter().enumerate() {
                    let landed = settled[i].is_ok();
                    assert_eq!(doc.body.contains(text.as_str()), landed, "append {i} is in the document iff it succeeded");
                    assert_eq!(indexed.contains(text.as_str()), landed, "index of the final document: {lines:?}");
                }
            });
            Ok(())
        })
        .unwrap();
}
