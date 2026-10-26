// The agent's sqlite state, agent.db: the memory KV (digest watermarks), the
// local trace mirror with the judge's judgements, and the activity journal's
// progress. The eval tooling (crates/evals: judge, improver) opens the same
// file. The schema, the migrations and the JSON in its columns are the ones
// the TS agent wrote, so the volumes it ran on read the same.
//
// Sections:
//   1. handle     — open, pragmas, a boot that survives other processes
//   2. migrations — drizzle's sqlite migrator, same files, same journal
//   3. memory     — the KV
//   4. traces     — the trace mirror and judgements

use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use anyhow::Context;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::trace_model::{Observation, Trace};

// ── 1. handle ───────────────────────────────────────────────────────────────

pub const DEFAULT_DB_PATH: &str = "packages/agent/data/agent.db";

// One connection for writes, shared, and one for reads (a file's: WAL
// readers never wait for a writer). Built once in the composition root and
// passed down. The engine's handlers read synchronously inside the task that
// drives every run (runs.rs §2): a read queued behind a write that waits for
// SQLite's lock (another process's transaction) would stall them all.
#[derive(Clone)]
pub struct AgentDb {
    conn: Arc<Mutex<Connection>>,
    reader: Option<Arc<Mutex<Connection>>>,
}

impl AgentDb {
    // Opens (creating) the file and applies pending migrations.
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<AgentDb> {
        let path = path.as_ref();
        if let Some(dir) = path.parent()
            && !dir.as_os_str().is_empty()
        {
            std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        }
        let conn = Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
        // Several processes (the agent, the judge and improve workers) open the same
        // file and migrate at startup. busy_timeout first: switching a fresh
        // file to WAL takes a lock too.
        conn.busy_timeout(Duration::from_millis(5000))?;
        // Turning a fresh file into WAL is not covered by the busy handler.
        retry_boot_race(|| conn.query_row("PRAGMA journal_mode = WAL", [], |_| Ok(())).map_err(anyhow::Error::from))?;
        conn.execute_batch("PRAGMA foreign_keys = ON; PRAGMA synchronous = FULL")?;
        retry_boot_race(|| migrate(&conn))?;
        // An in-memory database is private to its connection: it reads on it.
        let memory = path.as_os_str().is_empty() || path.as_os_str() == ":memory:";
        let reader = if memory {
            None
        } else {
            let reader = Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
            reader.busy_timeout(Duration::from_millis(5000))?;
            reader.execute_batch("PRAGMA query_only = ON")?;
            Some(Arc::new(Mutex::new(reader)))
        };
        Ok(AgentDb { conn: Arc::new(Mutex::new(conn)), reader })
    }

    pub fn conn(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The connection reads use: never behind a write.
    pub fn read(&self) -> MutexGuard<'_, Connection> {
        self.reader.as_ref().unwrap_or(&self.conn).lock().unwrap_or_else(|e| e.into_inner())
    }
}

// Retries a boot step that lost a race with another process opening the same
// file: "database is locked" (the WAL switch) or "already exists" (a
// migration someone else just applied — drizzle reads the last applied one
// before its BEGIN, so a loser sees it pending). Anything else, or a race
// lost MAX_BOOT_ATTEMPTS times, is returned.
const MAX_BOOT_ATTEMPTS: u32 = 8;

fn retry_boot_race(mut step: impl FnMut() -> anyhow::Result<()>) -> anyhow::Result<()> {
    let mut attempt = 1;
    loop {
        match step() {
            Ok(()) => return Ok(()),
            Err(err) => {
                let text = format!("{err:#}");
                let raced = text.contains("database is locked") || text.contains("already exists");
                if !raced || attempt >= MAX_BOOT_ATTEMPTS {
                    return Err(err);
                }
                let jitter = rand::random::<u64>() % 25;
                std::thread::sleep(Duration::from_millis(25 * u64::from(attempt) + jitter));
                attempt += 1;
            }
        }
    }
}

// ── 2. migrations ───────────────────────────────────────────────────────────

// The TS agent's drizzle migrations, unchanged (a test pins their hashes),
// baked into the binary so a schema change ships with it.
const JOURNAL: &str = include_str!("../migrations/meta/_journal.json");
const MIGRATIONS: &[(&str, &str)] = &[
    ("0000_bouncy_bloodaxe", include_str!("../migrations/0000_bouncy_bloodaxe.sql")),
    ("0001_bored_medusa", include_str!("../migrations/0001_bored_medusa.sql")),
    ("0002_unusual_thor", include_str!("../migrations/0002_unusual_thor.sql")),
    ("0003_saved_workflows", include_str!("../migrations/0003_saved_workflows.sql")),
    ("0004_durable_runs", include_str!("../migrations/0004_durable_runs.sql")),
    ("0005_agent_schedule", include_str!("../migrations/0005_agent_schedule.sql")),
    ("0006_workflow_tried", include_str!("../migrations/0006_workflow_tried.sql")),
    ("0007_system_skill_index", include_str!("../migrations/0007_system_skill_index.sql")),
    ("0008_run_ended", include_str!("../migrations/0008_run_ended.sql")),
    ("0009_workflow_events", include_str!("../migrations/0009_workflow_events.sql")),
    ("0010_confirmations", include_str!("../migrations/0010_confirmations.sql")),
    ("0011_event_listeners", include_str!("../migrations/0011_event_listeners.sql")),
    ("0012_child_runs", include_str!("../migrations/0012_child_runs.sql")),
    ("0013_guard_verdict_uses", include_str!("../migrations/0013_guard_verdict_uses.sql")),
    ("0014_trace_continuation", include_str!("../migrations/0014_trace_continuation.sql")),
    ("0015_engine_runs", include_str!("../migrations/0015_engine_runs.sql")),
    ("0016_workflow_meta", include_str!("../migrations/0016_workflow_meta.sql")),
    ("0017_engine_schedules", include_str!("../migrations/0017_engine_schedules.sql")),
];

pub struct Migration {
    pub tag: String,
    pub when: i64,
    pub hash: String,
    pub statements: Vec<String>,
}

// What drizzle's readMigrationFiles makes of the folder.
pub fn migrations() -> anyhow::Result<Vec<Migration>> {
    let journal: Value = serde_json::from_str(JOURNAL)?;
    let entries = journal["entries"].as_array().context("journal has no entries")?;
    entries
        .iter()
        .map(|entry| {
            let tag = entry["tag"].as_str().context("journal entry without a tag")?;
            let when = entry["when"].as_i64().context("journal entry without `when`")?;
            let (_, sql) = MIGRATIONS
                .iter()
                .find(|(t, _)| *t == tag)
                .with_context(|| format!("migration {tag} is in the journal but not in the binary"))?;
            Ok(Migration {
                tag: tag.to_owned(),
                when,
                hash: hex::encode(Sha256::digest(sql.as_bytes())),
                statements: sql.split("--> statement-breakpoint").map(str::to_owned).collect(),
            })
        })
        .collect()
}

// drizzle-orm/sqlite-core's migrate: the newest applied migration's
// created_at is the watermark; every migration after it runs, in one
// transaction, and is recorded with its hash.
fn migrate(conn: &Connection) -> anyhow::Result<()> {
    conn.execute_batch(
        r#"CREATE TABLE IF NOT EXISTS "__drizzle_migrations" (
				id SERIAL PRIMARY KEY,
				hash text NOT NULL,
				created_at numeric
			)"#,
    )?;
    let last: Option<f64> = conn
        .query_row(r#"SELECT created_at FROM "__drizzle_migrations" ORDER BY created_at DESC LIMIT 1"#, [], |r| {
            r.get::<_, Option<f64>>(0)
        })
        .optional()?
        .flatten();
    let pending: Vec<Migration> =
        migrations()?.into_iter().filter(|m| last.is_none_or(|last| last < m.when as f64)).collect();
    if pending.is_empty() {
        return Ok(());
    }
    conn.execute_batch("BEGIN")?;
    let applied = (|| -> anyhow::Result<()> {
        for m in &pending {
            for statement in &m.statements {
                conn.execute_batch(statement).with_context(|| format!("migration {}", m.tag))?;
            }
            conn.execute(
                r#"INSERT INTO "__drizzle_migrations" ("hash", "created_at") VALUES(?1, ?2)"#,
                params![m.hash, m.when],
            )?;
        }
        Ok(())
    })();
    match applied {
        Ok(()) => {
            conn.execute_batch("COMMIT")?;
            Ok(())
        }
        Err(err) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(err)
        }
    }
}

// ── 3. memory ───────────────────────────────────────────────────────────────

// Well-known keys injected into the session context block.
pub const NEWS_LAST_READ_AT: &str = "news_digest.last_read_at";
pub const TECH_LAST_READ_AT: &str = "tech_digest.last_read_at";

// The freeform KV for anything the agent remembers between sessions.
pub trait MemoryStore: Send + Sync {
    fn get(&self, key: &str) -> anyhow::Result<Option<String>>;
    fn set(&self, key: &str, value: &str) -> anyhow::Result<()>;
}

impl MemoryStore for AgentDb {
    fn get(&self, key: &str) -> anyhow::Result<Option<String>> {
        Ok(self.read().query_row("SELECT value FROM memory WHERE key = ?1", [key], |r| r.get(0)).optional()?)
    }

    fn set(&self, key: &str, value: &str) -> anyhow::Result<()> {
        self.conn().execute(
            "INSERT INTO memory (key, value) VALUES (?1, ?2)
             ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_at = (datetime('now'))",
            params![key, value],
        )?;
        Ok(())
    }
}

// ── 4. traces ───────────────────────────────────────────────────────────────

// What the local recorder hands over on trace.end(). `id` is the Langfuse
// trace id when Langfuse is on, so it doubles as the link for scores.
#[derive(Clone, Debug)]
pub struct StoredTrace {
    pub id: String,
    pub name: String,
    pub source: Option<String>,
    pub skill: Option<String>,
    pub session_id: Option<String>,
    pub tags: Vec<String>,
    // None and Some(Null) are both stored as SQL NULL.
    pub input: Option<Value>,
    pub output: Option<Value>,
    pub metadata: Option<Map<String, Value>>,
    pub observations: Vec<Observation>,
    pub started_at: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TraceSummary {
    pub id: String,
    pub name: String,
    pub timestamp: String,
    pub tags: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Scores {
    pub query_formulation: Option<f64>,
    pub process: Option<f64>,
    pub coverage: Option<f64>,
    pub composition: Option<f64>,
    pub faithfulness: Option<f64>,
}

#[derive(Clone, Debug)]
pub struct JudgementInput {
    pub trace_id: String,
    pub observation_id: String,
    pub node_kind: String,
    pub skill: String,
    pub provider: String,
    pub prompt_version: String,
    pub scores: Scores,
    pub detail: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct JudgementRecord {
    pub trace_id: String,
    pub observation_id: String,
    pub node_kind: String,
    pub skill: String,
    pub scores: Scores,
    pub detail: Value,
    pub started_at: String,
}

pub struct JudgementFilter<'a> {
    pub skill: &'a str,
    pub provider: &'a str,
    pub prompt_version: &'a str,
}

pub trait TraceStore: Send + Sync {
    fn write_trace(&self, t: &StoredTrace) -> anyhow::Result<()>;
    // Segment sinks may keep each segment separately; the production mirror
    // merges them atomically, including segments arriving before their root.
    fn write_segment(&self, t: &StoredTrace, _continuation: bool) -> anyhow::Result<()> {
        self.write_trace(t)
    }
    // Read back in the canonical {trace, observations} shape, the same as a
    // Langfuse fetch.
    fn get_trace(&self, id: &str) -> anyhow::Result<Option<(Trace, Vec<Observation>)>>;
}

fn json_column(value: Option<&Value>) -> Option<String> {
    match value {
        None | Some(Value::Null) => None,
        Some(v) => Some(v.to_string()),
    }
}

fn parse_column(text: Option<String>) -> anyhow::Result<Value> {
    Ok(match text {
        None => Value::Null,
        Some(t) => serde_json::from_str(&t)?,
    })
}

impl TraceStore for AgentDb {
    fn write_trace(&self, t: &StoredTrace) -> anyhow::Result<()> {
        let metadata = t.metadata.as_ref().map(|m| Value::Object(m.clone()).to_string());
        self.conn().execute(
            "INSERT INTO traces (id, name, source, skill, session_id, tags, input, output, metadata, observations, started_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
             ON CONFLICT (id) DO UPDATE SET
               name = excluded.name, source = excluded.source, skill = excluded.skill,
               session_id = excluded.session_id, tags = excluded.tags, input = excluded.input,
               output = excluded.output, metadata = excluded.metadata,
               observations = excluded.observations, started_at = excluded.started_at",
            params![
                t.id,
                t.name,
                t.source,
                t.skill,
                t.session_id,
                serde_json::to_string(&t.tags)?,
                json_column(t.input.as_ref()),
                json_column(t.output.as_ref()),
                metadata,
                serde_json::to_string(&t.observations)?,
                t.started_at,
            ],
        )?;
        Ok(())
    }

    fn write_segment(&self, t: &StoredTrace, continuation: bool) -> anyhow::Result<()> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let previous: Option<String> =
            tx.query_row("SELECT observations FROM traces WHERE id=?1", [&t.id], |r| r.get(0)).optional()?;
        let mut observations: Vec<Observation> =
            previous.map(|s| serde_json::from_str(&s)).transpose()?.unwrap_or_default();
        for observation in &t.observations {
            match observations.iter_mut().find(|o| o.id == observation.id) {
                Some(existing) => *existing = observation.clone(),
                None => observations.push(observation.clone()),
            }
        }
        // Original fields always come from the signal segment. A final root
        // continuation wins over a late signal end's temporary `resumes` output.
        tx.execute("INSERT INTO traces (id,name,source,skill,session_id,tags,input,output,metadata,observations,started_at,continued_output)
            VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12 AND ?13)
            ON CONFLICT(id) DO UPDATE SET
              name=CASE WHEN ?12 THEN name ELSE excluded.name END,
              source=CASE WHEN ?12 THEN source ELSE excluded.source END,
              skill=coalesce(excluded.skill,skill),
              session_id=CASE WHEN ?12 THEN session_id ELSE excluded.session_id END,
              tags=CASE WHEN ?12 THEN tags ELSE excluded.tags END,
              input=CASE WHEN ?12 THEN input ELSE excluded.input END,
              metadata=CASE WHEN ?12 AND ?13 THEN json_patch(coalesce(metadata,'{}'),coalesce(excluded.metadata,'{}'))
                WHEN ?12 THEN metadata
                WHEN continued_output THEN json_patch(coalesce(excluded.metadata,'{}'),json_object('error',json_extract(metadata,'$.error')))
                ELSE excluded.metadata END,
              output=CASE WHEN (?12 OR NOT continued_output) AND ?13 THEN excluded.output ELSE output END,
              continued_output=continued_output OR excluded.continued_output,
              observations=excluded.observations,
              started_at=min(started_at,excluded.started_at)",
            params![t.id,t.name,t.source,t.skill,t.session_id,serde_json::to_string(&t.tags)?,json_column(t.input.as_ref()),json_column(t.output.as_ref()),t.metadata.as_ref().map(|m|Value::Object(m.clone()).to_string()),serde_json::to_string(&observations)?,t.started_at,continuation,t.output.is_some()])?;
        tx.commit()?;
        Ok(())
    }

    fn get_trace(&self, id: &str) -> anyhow::Result<Option<(Trace, Vec<Observation>)>> {
        type Row =
            (String, String, Option<String>, String, Option<String>, Option<String>, Option<String>, String, String);
        let row: Option<Row> = self
            .conn()
            .query_row(
                "SELECT id, name, session_id, tags, input, output, metadata, observations, started_at
                 FROM traces WHERE id = ?1",
                [id],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                        r.get(6)?,
                        r.get(7)?,
                        r.get(8)?,
                    ))
                },
            )
            .optional()?;
        let Some((id, name, session_id, tags, input, output, metadata, observations, started_at)) = row else {
            return Ok(None);
        };
        let observations: Vec<Observation> = serde_json::from_str(&observations)?;
        let metadata = match parse_column(metadata)? {
            Value::Object(m) => Some(m),
            _ => None,
        };
        let trace = Trace {
            id,
            name,
            session_id,
            timestamp: started_at,
            input: parse_column(input)?,
            output: parse_column(output)?,
            metadata,
            observations: observations.clone(),
            latency: 0.0,
            total_cost: 0.0,
            tags: serde_json::from_str(&tags)?,
        };
        Ok(Some((trace, observations)))
    }
}

// The judge's half of the store. The agent never judges — crates/evals does,
// on this same file.
impl AgentDb {
    // Newest first; optionally only traces with no judgement for
    // (provider, prompt version).
    pub fn list_recent(&self, limit: u32, unjudged_for: Option<(&str, &str)>) -> anyhow::Result<Vec<TraceSummary>> {
        let conn = self.conn();
        let map = |r: &rusqlite::Row<'_>| -> rusqlite::Result<(String, String, String, String)> {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
        };
        let rows: Vec<(String, String, String, String)> = match unjudged_for {
            Some((provider, version)) => conn
                .prepare(
                    "SELECT traces.id, traces.name, traces.tags, traces.started_at FROM traces
                     LEFT JOIN judgements ON judgements.trace_id = traces.id
                       AND judgements.provider = ?1 AND judgements.prompt_version = ?2
                     WHERE judgements.trace_id IS NULL
                       AND NOT EXISTS (SELECT 1 FROM run_trace r WHERE r.trace_id=traces.id AND r.recorded=0)
                     ORDER BY traces.started_at DESC LIMIT ?3",
                )?
                .query_map(params![provider, version, limit], map)?
                .collect::<Result<_, _>>()?,
            None => conn
                .prepare("SELECT id, name, tags, started_at FROM traces ORDER BY started_at DESC LIMIT ?1")?
                .query_map(params![limit], map)?
                .collect::<Result<_, _>>()?,
        };
        rows.into_iter()
            .map(|(id, name, tags, started_at)| {
                Ok(TraceSummary { id, name, timestamp: started_at, tags: serde_json::from_str(&tags)? })
            })
            .collect()
    }

    pub fn write_judgement(&self, j: &JudgementInput) -> anyhow::Result<()> {
        let s = &j.scores;
        self.conn().execute(
            "INSERT INTO judgements (trace_id, observation_id, provider, prompt_version, node_kind, skill,
               query_formulation, process, coverage, composition, faithfulness, detail)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
             ON CONFLICT (trace_id, observation_id, provider, prompt_version) DO UPDATE SET
               node_kind = excluded.node_kind, skill = excluded.skill,
               query_formulation = excluded.query_formulation, process = excluded.process,
               coverage = excluded.coverage, composition = excluded.composition,
               faithfulness = excluded.faithfulness, detail = excluded.detail",
            params![
                j.trace_id,
                j.observation_id,
                j.provider,
                j.prompt_version,
                j.node_kind,
                j.skill,
                s.query_formulation,
                s.process,
                s.coverage,
                s.composition,
                s.faithfulness,
                j.detail.to_string(),
            ],
        )?;
        Ok(())
    }

    pub fn list_judgements(&self, f: &JudgementFilter<'_>) -> anyhow::Result<Vec<JudgementRecord>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT j.trace_id, j.observation_id, j.node_kind, j.skill, j.query_formulation, j.process,
                    j.coverage, j.composition, j.faithfulness, j.detail, t.started_at
             FROM judgements j INNER JOIN traces t ON j.trace_id = t.id
             WHERE j.skill = ?1 AND j.provider = ?2 AND j.prompt_version = ?3",
        )?;
        let rows = stmt.query_map(params![f.skill, f.provider, f.prompt_version], |r| {
            Ok((
                JudgementRecord {
                    trace_id: r.get(0)?,
                    observation_id: r.get(1)?,
                    node_kind: r.get(2)?,
                    skill: r.get(3)?,
                    scores: Scores {
                        query_formulation: r.get(4)?,
                        process: r.get(5)?,
                        coverage: r.get(6)?,
                        composition: r.get(7)?,
                        faithfulness: r.get(8)?,
                    },
                    detail: Value::Null,
                    started_at: r.get(10)?,
                },
                r.get::<_, String>(9)?,
            ))
        })?;
        rows.map(|row| {
            let (mut record, detail) = row?;
            record.detail = serde_json::from_str(&detail)?;
            Ok(record)
        })
        .collect()
    }

    pub fn list_judged_skills(&self, provider: &str, prompt_version: &str) -> anyhow::Result<Vec<String>> {
        let conn = self.conn();
        let mut stmt =
            conn.prepare("SELECT DISTINCT skill FROM judgements WHERE provider = ?1 AND prompt_version = ?2")?;
        let rows = stmt.query_map(params![provider, prompt_version], |r| r.get(0))?;
        Ok(rows.collect::<Result<_, _>>()?)
    }
}
