// Port of test/agent-db.test.ts: the agent's sqlite state on a real database
// file under generated operation sequences — including closing and reopening
// the file, as a restart does. Expectations are the plain store semantics:
// last write wins, reads see exactly what was written (as JSON), filters and
// orderings hold. The migration-bytes and example tests at the end are
// Rust-only.
use agent::db::*;
use proptest::prelude::*;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};

fn runs() -> u32 {
    std::env::var("PROPERTY_RUNS").ok().and_then(|v| v.parse().ok()).unwrap_or(60)
}

// ── memory KV ───────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
enum KvOp {
    Set(String, String),
    Get(String),
    Reopen,
}

fn kv_key() -> impl Strategy<Value = String> {
    prop::sample::select(vec![
        "news_digest.last_read_at".to_owned(),
        "tech_digest.last_read_at".to_owned(),
        "a".to_owned(),
        "ключ".to_owned(),
        "x".repeat(300),
    ])
}

fn kv_op() -> impl Strategy<Value = KvOp> {
    prop_oneof![
        (kv_key(), ".{0,50}").prop_map(|(k, v)| KvOp::Set(k, v)),
        kv_key().prop_map(KvOp::Get),
        Just(KvOp::Reopen),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(runs()))]
    #[test]
    fn returns_the_last_value_written_for_each_key_across_reopening_the_file(ops in prop::collection::vec(kv_op(), 1..=30)) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent.db");
        let mut db = AgentDb::open(&path).unwrap();
        let mut model = HashMap::new();
        for op in ops {
            match op {
                KvOp::Set(key, value) => {
                    db.set(&key, &value).unwrap();
                    model.insert(key, value);
                }
                KvOp::Get(key) => prop_assert_eq!(db.get(&key).unwrap(), model.get(&key).cloned(), "get({})", key),
                KvOp::Reopen => {
                    drop(db);
                    db = AgentDb::open(&path).unwrap();
                }
            }
        }
        for (key, value) in model {
            prop_assert_eq!(db.get(&key).unwrap(), Some(value), "final {}", key);
        }
    }
}

// ── trace mirror and judgements ─────────────────────────────────────────────

#[derive(Clone, Debug)]
enum TraceOp {
    Trace {
        id: &'static str,
        input: Option<Value>,
        output: Option<Value>,
        tags: Vec<&'static str>,
        name: &'static str,
    },
    Judge {
        pick: usize,
        observation_id: &'static str,
        provider: &'static str,
        version: &'static str,
        skill: &'static str,
        score: Option<u8>,
    },
    Recent {
        limit: u32,
        filter: Option<(&'static str, &'static str)>,
    },
    Judgements {
        skill: &'static str,
        provider: &'static str,
        version: &'static str,
    },
}

// fc.jsonValue({ maxDepth: 2 }): JSON values nested at most two levels.
fn json_value() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::from),
        any::<i32>().prop_map(Value::from),
        ".{0,10}".prop_map(Value::from),
    ];
    leaf.prop_recursive(2, 16, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
            prop::collection::btree_map(".{0,6}", inner, 0..4).prop_map(|m| Value::Object(m.into_iter().collect())),
        ]
    })
}

fn trace_op() -> impl Strategy<Value = TraceOp> {
    let trace_id = prop::sample::select(vec!["telegram:1", "scheduler:2", "gmail:3", "t4", "t5"]);
    let json = || prop::option::of(json_value());
    let provider = || prop::sample::select(vec!["codex", "openai"]);
    let version = || prop::sample::select(vec!["n1", "n2"]);
    let skill = || prop::sample::select(vec!["planner", "news-digest", "telegram"]);
    prop_oneof![
        3 => (
            trace_id,
            json(),
            json(),
            prop::collection::vec(prop::sample::select(vec!["telegram", "agent-loop", "planner-mode"]), 0..=3),
            prop::sample::select(vec!["signal:telegram", "signal:scheduler"]),
        )
            .prop_map(|(id, input, output, tags, name)| TraceOp::Trace { id, input, output, tags, name }),
        3 => (any::<usize>(), prop::sample::select(vec!["o1", "o2"]), provider(), version(), skill(), prop::option::of(0u8..=5))
            .prop_map(|(pick, observation_id, provider, version, skill, score)| TraceOp::Judge {
                pick, observation_id, provider, version, skill, score,
            }),
        2 => (1u32..=6, prop::option::of((provider(), version())))
            .prop_map(|(limit, filter)| TraceOp::Recent { limit, filter }),
        2 => (skill(), provider(), version())
            .prop_map(|(skill, provider, version)| TraceOp::Judgements { skill, provider, version }),
    ]
}

fn iso(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms).unwrap().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(runs()))]
    #[test]
    fn reads_back_what_was_written_last_write_winning_with_filters_and_orderings_that_hold(
        ops in prop::collection::vec(trace_op(), 1..=30)
    ) {
        let dir = tempfile::tempdir().unwrap();
        let db = AgentDb::open(dir.path().join("agent.db")).unwrap();
        // Insertion-ordered like a JS Map: a rewrite keeps the key's place.
        let mut written: Vec<(String, StoredTrace)> = Vec::new();
        let mut judged: BTreeMap<String, JudgementInput> = BTreeMap::new();
        let mut clock = chrono::DateTime::parse_from_rfc3339("2026-10-01T00:00:00Z").unwrap().timestamp_millis();
        for op in ops {
            match op {
                TraceOp::Trace { id, input, output, tags, name } => {
                    clock += 1000;
                    let source = tags.first().map(|s| s.to_string());
                    let t = StoredTrace {
                        id: id.into(),
                        name: name.into(),
                        source: source.clone(),
                        skill: None,
                        session_id: Some(id.into()),
                        tags: tags.iter().map(|s| s.to_string()).collect(),
                        input,
                        output,
                        metadata: Some(json!({ "signal_source": source }).as_object().unwrap().clone()),
                        observations: Vec::new(),
                        started_at: iso(clock),
                    };
                    db.write_trace(&t).unwrap();
                    match written.iter_mut().find(|(k, _)| k == id) {
                        Some(slot) => slot.1 = t.clone(),
                        None => written.push((id.into(), t.clone())),
                    }
                    let (back, _) = db.get_trace(id).unwrap().expect("getTrace after a write");
                    prop_assert_eq!(&back.id, &t.id);
                    prop_assert_eq!(&back.name, &t.name);
                    prop_assert_eq!(&back.session_id, &t.session_id);
                    prop_assert_eq!(&back.timestamp, &t.started_at);
                    prop_assert_eq!(&back.tags, &t.tags);
                    prop_assert_eq!(&back.input, &t.input.clone().unwrap_or(Value::Null));
                    prop_assert_eq!(&back.output, &t.output.clone().unwrap_or(Value::Null));
                }
                TraceOp::Judge { pick, observation_id, provider, version, skill, score } => {
                    if written.is_empty() {
                        continue;
                    }
                    let score = score.map(f64::from);
                    let j = JudgementInput {
                        trace_id: written[pick % written.len()].0.clone(),
                        observation_id: observation_id.into(),
                        node_kind: "compose".into(),
                        skill: skill.into(),
                        provider: provider.into(),
                        prompt_version: version.into(),
                        scores: Scores { query_formulation: score, process: score, ..Scores::default() },
                        detail: json!({ "note": format!("score {score:?}") }),
                    };
                    db.write_judgement(&j).unwrap();
                    judged.insert([j.trace_id.as_str(), observation_id, provider, version].join("|"), j);
                }
                TraceOp::Recent { limit, filter } => {
                    let got: Vec<String> = db.list_recent(limit, filter).unwrap().into_iter().map(|s| s.id).collect();
                    let mut eligible: Vec<&StoredTrace> = written
                        .iter()
                        .map(|(_, t)| t)
                        .filter(|t| match filter {
                            None => true,
                            Some((p, v)) => !judged.values().any(|j| j.trace_id == t.id && j.provider == p && j.prompt_version == v),
                        })
                        .collect();
                    eligible.sort_by(|a, b| b.started_at.cmp(&a.started_at));
                    let want: Vec<String> = eligible.into_iter().take(limit as usize).map(|t| t.id.clone()).collect();
                    prop_assert_eq!(got, want, "listRecent({}, {:?})", limit, filter);
                }
                TraceOp::Judgements { skill, provider, version } => {
                    let mut got: Vec<String> = db
                        .list_judgements(&JudgementFilter { skill, provider, prompt_version: version })
                        .unwrap()
                        .into_iter()
                        .map(|r| format!("{}|{}|{:?}|{}", r.trace_id, r.observation_id, r.scores.process, r.started_at))
                        .collect();
                    got.sort();
                    let mut want: Vec<String> = judged
                        .values()
                        .filter(|j| j.skill == skill && j.provider == provider && j.prompt_version == version)
                        .map(|j| {
                            let started = &written.iter().find(|(k, _)| *k == j.trace_id).unwrap().1.started_at;
                            format!("{}|{}|{:?}|{}", j.trace_id, j.observation_id, j.scores.process, started)
                        })
                        .collect();
                    want.sort();
                    prop_assert_eq!(got, want, "listJudgements({}, {}, {})", skill, provider, version);
                    let mut skills = db.list_judged_skills(provider, version).unwrap();
                    skills.sort();
                    let want_skills: Vec<String> = judged
                        .values()
                        .filter(|j| j.provider == provider && j.prompt_version == version)
                        .map(|j| j.skill.clone())
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .collect();
                    prop_assert_eq!(skills, want_skills, "listJudgedSkills");
                }
            }
        }
    }
}

// ── migrations on boot ──────────────────────────────────────────────────────

#[test]
fn migrating_the_same_file_twice_is_a_no_op() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("agent.db");
    drop(AgentDb::open(&path).unwrap());
    let again = AgentDb::open(&path).unwrap();
    assert_eq!(again.get("anything").unwrap(), None);
}

// The child half of the test below: the test binary re-run with a database
// path and a start instant boots the database and exits.
const BOOT_FILE: &str = "AGENT_DB_TEST_BOOT_FILE";
const BOOT_AT: &str = "AGENT_DB_TEST_BOOT_AT";

#[test]
fn boot_child() {
    let (Ok(file), Ok(at)) = (std::env::var(BOOT_FILE), std::env::var(BOOT_AT)) else { return };
    let at: u128 = at.parse().unwrap();
    while std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() < at {}
    drop(AgentDb::open(file).unwrap());
}

// Several processes opening one fresh database at once, as the agent,
// judge-worker and improve-worker do on the shared volume after a deploy
// with a new migration.
#[test]
fn three_processes_booting_at_once_all_come_up() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("agent.db");
    // All three wait for one shared instant, so their migrations overlap.
    let start_at = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() + 2_000;
    let exe = std::env::current_exe().unwrap();
    let children: Vec<_> = (0..3)
        .map(|_| {
            std::process::Command::new(&exe)
                .args(["--exact", "boot_child", "--test-threads=1"])
                .env(BOOT_FILE, &file)
                .env(BOOT_AT, start_at.to_string())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .unwrap()
        })
        .collect();
    let codes: Vec<_> = children.into_iter().map(|mut c| c.wait().unwrap().code()).collect();
    assert_eq!(codes, [Some(0), Some(0), Some(0)]);
    let db = AgentDb::open(&file).unwrap();
    let count: i64 = db.conn().query_row("SELECT count(*) FROM __drizzle_migrations", [], |r| r.get(0)).unwrap();
    assert_eq!(count, migrations().unwrap().len() as i64);
}

// ── Rust-only ───────────────────────────────────────────────────────────────

// An applied migration never changes: each file still hashes the way drizzle
// recorded it on the volumes that ran it (the first three were applied by the
// TS agent, whose migrator this one is), and a fresh database records exactly
// those hashes.
#[test]
fn migration_files_hash_like_drizzle_recorded_them() {
    let migrations = migrations().unwrap();
    let hashes: Vec<String> = migrations.iter().map(|m| m.hash[..16].to_owned()).collect();
    assert_eq!(
        hashes,
        [
            "8da75a8e64421e83",
            "d156e1a7e2498f5f",
            "834b8c53900bee1d",
            "000d00df8145f548",
            "183eb6e8b49f84c6",
            "c9403db5895dfcf0"
        ]
    );
    let rust = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    for migration in &migrations {
        let sql = std::fs::read(rust.join(format!("{}.sql", migration.tag))).unwrap();
        assert_eq!(migration.hash, hex::encode(Sha256::digest(sql)));
    }
    let dir = tempfile::tempdir().unwrap();
    let db = AgentDb::open(dir.path().join("agent.db")).unwrap();
    let rows: Vec<(String, i64)> = db
        .conn()
        .prepare("SELECT hash,created_at FROM __drizzle_migrations ORDER BY created_at")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(rows, migrations.into_iter().map(|m| (m.hash, m.when)).collect::<Vec<_>>());
}

fn trace(id: &str, at: &str) -> StoredTrace {
    StoredTrace {
        id: id.into(),
        name: format!("run:{id}"),
        source: Some("telegram".into()),
        skill: None,
        session_id: Some(id.into()),
        tags: vec!["telegram".into()],
        input: Some(json!({"user":"Привет 🦊"})),
        output: None,
        metadata: Some(json!({"signal_source":"telegram"}).as_object().unwrap().clone()),
        observations: Vec::new(),
        started_at: at.into(),
    }
}

#[test]
fn a_judgement_rewrite_replaces_scores_and_needs_an_existing_trace() {
    let dir = tempfile::tempdir().unwrap();
    let db = AgentDb::open(dir.path().join("agent.db")).unwrap();
    db.write_trace(&trace("old", "2026-10-01T00:00:00Z")).unwrap();
    let mut judgement = JudgementInput {
        trace_id: "old".into(),
        observation_id: "g1".into(),
        provider: "codex".into(),
        prompt_version: "v1".into(),
        node_kind: "compose".into(),
        skill: "digest".into(),
        scores: Scores { faithfulness: Some(1.0), ..Default::default() },
        detail: json!({"note":"bad"}),
    };
    db.write_judgement(&judgement).unwrap();
    judgement.scores.faithfulness = Some(5.0);
    judgement.detail = json!({"note":"fixed"});
    db.write_judgement(&judgement).unwrap();
    let rows =
        db.list_judgements(&JudgementFilter { skill: "digest", provider: "codex", prompt_version: "v1" }).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].scores.faithfulness, Some(5.0));
    assert_eq!(rows[0].detail, json!({"note":"fixed"}));
    assert!(db.get_trace("missing").unwrap().is_none());
    judgement.trace_id = "no-such-trace".into();
    assert!(db.write_judgement(&judgement).is_err());
}

#[test]
fn concurrent_boots_in_one_process_migrate_once_and_preserve_all_writes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("race.db");
    let start = std::sync::Arc::new(std::sync::Barrier::new(8));
    let tasks: Vec<_> = (0..8)
        .map(|n| {
            let path = path.clone();
            let start = start.clone();
            std::thread::spawn(move || {
                start.wait();
                let db = AgentDb::open(path).unwrap();
                db.set(&format!("key{n}"), "value").unwrap();
            })
        })
        .collect();
    for task in tasks {
        task.join().unwrap();
    }
    let db = AgentDb::open(path).unwrap();
    let count: i64 = db.conn().query_row("SELECT count(*) FROM __drizzle_migrations", [], |r| r.get(0)).unwrap();
    assert_eq!(count, migrations().unwrap().len() as i64);
    for n in 0..8 {
        assert_eq!(db.get(&format!("key{n}")).unwrap().as_deref(), Some("value"));
    }
}
