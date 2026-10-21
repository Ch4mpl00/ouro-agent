//! Agent-owned SQLite run state: 1. admission and leases; 2. checkpoints;
//! 3. durable events (registration, delivery and atomic consumption).
//! A lease is per execution attempt, not per process. Expired workers cannot
//! renew or commit; a crash leaves the run claimable after lease expiry.
use crate::db::AgentDb;
use owf::{
    durable::{Journal, Record, check_transition},
    run::{Status, WorkflowError},
};
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicBool, Ordering},
};

const LEASE_MS: i64 = 30_000;
pub const HEARTBEAT_MS: u64 = 10_000;
/// Interruptions in a row before a run is given up as failed. With the
/// backoff below that is about 17 minutes of retries.
pub const MAX_INTERRUPTIONS: i64 = 10;
/// How long a finished run keeps its records. The run row (its outcome)
/// stays; the records are only evidence once the run has ended, and an
/// agent loop's carry its transcript in every one.
pub const RECORDS_KEPT_MS: i64 = 7 * 24 * 3_600_000;

fn now() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

// 2 s after the first interruption, doubling, at most 10 minutes.
fn backoff(interruptions: i64) -> i64 {
    (2_000_i64 << (interruptions - 1).clamp(0, 20)).min(600_000)
}

#[derive(Clone)]
pub struct RunStore {
    db: AgentDb,
}

#[derive(Debug)]
pub struct SavedRun {
    pub id: String,
    pub name: String,
    pub state: String,
    pub definition: String,
    pub input: Value,
    pub outcome: Option<Status>,
}

impl RunStore {
    pub fn new(db: AgentDb) -> Self {
        Self { db }
    }

    pub fn confirmations(&self) -> &dyn crate::confirmation::Store { &self.db }

    pub fn get(&self, id: &str) -> anyhow::Result<Option<SavedRun>> {
        let row = self
            .db
            .conn()
            .query_row("SELECT name, definition, input, outcome, state FROM workflow_runs WHERE id = ?1", [id], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, Option<String>>(3)?,
                    r.get::<_, String>(4)?,
                ))
            })
            .optional()?;
        row.map(|(name, definition, input, outcome, state)| {
            Ok(SavedRun {
                id: id.into(),
                name,
                state,
                definition,
                input: serde_json::from_str(&input)?,
                outcome: outcome.map(|s| serde_json::from_str(&s)).transpose()?,
            })
        })
        .transpose()
    }

    /// Read evidence without claiming or renewing a run, including finished runs.
    pub fn records(&self, id: &str) -> anyhow::Result<Vec<Record>> {
        let conn = self.db.conn();
        let mut statement = conn.prepare("SELECT record FROM workflow_records WHERE run_id = ?1 ORDER BY key")?;
        let rows = statement.query_map([id], |r| r.get::<_, String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
        rows.into_iter().map(|row| Ok(serde_json::from_str(&row)?)).collect()
    }

    /// A literal prefix, not a LIKE pattern: ids can contain `_` and `%`.
    pub fn list_prefix(&self, name: &str, prefix: &str) -> anyhow::Result<Vec<String>> {
        let conn = self.db.conn();
        let mut statement = conn
            .prepare("SELECT id FROM workflow_runs WHERE name = ?1 AND substr(id, 1, length(?2)) = ?2 ORDER BY id")?;
        Ok(statement.query_map(params![name, prefix], |r| r.get(0))?.collect::<rusqlite::Result<_>>()?)
    }

    pub fn admit(&self, id: &str, name: &str, definition: &str, input: &Value) -> anyhow::Result<()> {
        self.insert(id, name, definition, input, None).map(|_| ())
    }

    /// Admits a new run already claimed, in one statement, so the recovery
    /// worker cannot take it between admission and claim. None when the run
    /// existed before.
    pub fn start(&self, id: &str, name: &str, definition: &str, input: &Value) -> anyhow::Result<Option<Lease>> {
        let owner = uuid::Uuid::new_v4().to_string();
        let inserted = self.insert(id, name, definition, input, Some(&owner))?;
        Ok(inserted.then(|| Lease { db: self.db.clone(), id: id.into(), owner, ended: AtomicBool::new(false) }))
    }

    fn insert(
        &self,
        id: &str,
        name: &str,
        definition: &str,
        input: &Value,
        owner: Option<&str>,
    ) -> anyhow::Result<bool> {
        let conn = self.db.conn();
        let state = if owner.is_some() { "running" } else { "ready" };
        let lease = owner.map(|_| now() + LEASE_MS);
        let inserted = conn.execute(
            "INSERT INTO workflow_runs (id, name, definition, input, state, owner, lease_until_ms)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) ON CONFLICT(id) DO NOTHING",
            params![id, name, definition, serde_json::to_string(input)?, state, owner, lease],
        )? == 1;
        let original: String = conn.query_row("SELECT name FROM workflow_runs WHERE id = ?1", [id], |r| r.get(0))?;
        anyhow::ensure!(original == name, "run {id} already belongs to workflow {original}");
        Ok(inserted)
    }

    /// One event belongs to one live run. Remove its registration on delivery,
    /// so a double press cannot enqueue another copy before consumption.
    pub fn deliver(&self, type_: &str, data: &Value) -> anyhow::Result<bool> {
        let mut conn = self.db.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let run: Option<String> = tx.query_row(
            "SELECT a.run_id FROM workflow_awaits a JOIN workflow_runs r ON r.id = a.run_id
             WHERE a.type = ?1 AND r.ended_ms IS NULL AND r.state IN ('ready','running','waiting')
             ORDER BY a.registered_at_ms, a.run_id LIMIT 1", [type_], |r| r.get(0)).optional()?;
        let Some(run) = run else { return Ok(false) };
        tx.execute("INSERT INTO workflow_events (run_id,type,data,delivered_at_ms) VALUES (?1,?2,?3,?4)",
            params![run,type_,serde_json::to_string(data)?,now()])?;
        tx.execute("DELETE FROM workflow_awaits WHERE run_id = ?1 AND type = ?2", params![run,type_])?;
        tx.execute("UPDATE workflow_runs SET wake_at_ms = ?2 WHERE id = ?1", params![run,now()])?;
        tx.commit()?;
        Ok(true)
    }

    pub fn due(&self) -> anyhow::Result<Vec<String>> {
        let conn = self.db.conn();
        let mut statement = conn.prepare(
            "SELECT id FROM workflow_runs WHERE
            (state IN ('ready', 'waiting') AND wake_at_ms <= ?1)
            OR (state = 'running' AND lease_until_ms <= ?1) ORDER BY wake_at_ms, id LIMIT 100",
        )?;
        Ok(statement.query_map([now()], |r| r.get(0))?.collect::<rusqlite::Result<_>>()?)
    }

    pub fn claim(&self, id: &str) -> anyhow::Result<Option<Lease>> {
        let owner = uuid::Uuid::new_v4().to_string();
        let time = now();
        let changed = self.db.conn().execute(
            "UPDATE workflow_runs SET state = 'running', owner = ?2, lease_until_ms = ?3
            WHERE id = ?1 AND ((state IN ('ready', 'waiting') AND wake_at_ms <= ?4)
                OR (state = 'running' AND lease_until_ms <= ?4))",
            params![id, owner, time + LEASE_MS, time],
        )?;
        Ok((changed == 1).then(|| Lease { db: self.db.clone(), id: id.into(), owner, ended: AtomicBool::new(false) }))
    }

    /// Explicit cancellation is terminal. Process shutdown instead releases
    /// the lease as Interrupted so that restart can resume the run.
    pub fn cancel(&self, id: &str) -> anyhow::Result<bool> {
        let mut conn = self.db.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = tx.execute(
            "UPDATE workflow_runs SET state = 'cancelled', owner = NULL, lease_until_ms = NULL,
            outcome = ?2, ended_ms = ?3 WHERE id = ?1 AND state IN ('ready', 'running', 'waiting')",
            params![id, serde_json::to_string(&Status::Cancelled)?, now()],
        )? == 1;
        tx.execute("DELETE FROM workflow_awaits WHERE run_id = ?1", [id])?;
        tx.commit()?;
        Ok(changed)
    }

    /// Deletes the records of runs that ended `kept_ms` or more ago and
    /// returns how many it deleted. The run rows stay, so a finished run
    /// still reports its outcome and is never resumed.
    pub fn sweep(&self, kept_ms: i64) -> anyhow::Result<usize> {
        let mut conn = self.db.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute("DELETE FROM confirmations WHERE run_id IN (SELECT id FROM workflow_runs WHERE ended_ms <= ?1)", [now() - kept_ms])?;
        tx.execute("DELETE FROM workflow_events WHERE run_id IN (SELECT id FROM workflow_runs WHERE ended_ms <= ?1)", [now() - kept_ms])?;
        let deleted = tx.execute(
            "DELETE FROM workflow_records WHERE run_id IN (SELECT id FROM workflow_runs WHERE ended_ms <= ?1)",
            [now() - kept_ms])?;
        tx.commit()?;
        Ok(deleted)
    }
}

pub struct Lease {
    db: AgentDb,
    id: String,
    owner: String,
    // Set once the attempt stored how it ended (finish or release).
    ended: AtomicBool,
}

// An attempt dropped before it ended (its caller went away mid-run, say a
// parent loop stopping with its sub-agent in flight) hands the run back at
// once, as a release would, instead of holding it until the lease expires.
impl Drop for Lease {
    fn drop(&mut self) {
        if !*self.ended.get_mut() {
            let _ = self.release();
        }
    }
}

impl Lease {
    fn check(&self, conn: &rusqlite::Connection) -> anyhow::Result<()> {
        let valid = conn
            .query_row(
                "SELECT 1 FROM workflow_runs WHERE id = ?1 AND owner = ?2
            AND state = 'running' AND lease_until_ms > ?3",
                params![self.id, self.owner, now()],
                |_| Ok(()),
            )
            .optional()?;
        anyhow::ensure!(valid.is_some(), "run {} lease expired or was replaced", self.id);
        Ok(())
    }

    pub fn renew(&self) -> anyhow::Result<()> {
        let time = now();
        let changed = self.db.conn().execute(
            "UPDATE workflow_runs SET lease_until_ms = ?3
            WHERE id = ?1 AND owner = ?2 AND state = 'running' AND lease_until_ms > ?4",
            params![self.id, self.owner, time + LEASE_MS, time],
        )?;
        anyhow::ensure!(changed == 1, "run {} lost its lease", self.id);
        Ok(())
    }

    /// Stores how this attempt ended and returns the run's resulting status.
    /// An interruption is retried with exponential backoff; after
    /// MAX_INTERRUPTIONS in a row (a cause that never clears: a corrupt
    /// record, state from an incompatible version) it becomes a fault.
    pub fn finish(&self, outcome: &Status) -> anyhow::Result<Status> {
        self.ended.store(true, Ordering::SeqCst);
        let mut conn = self.db.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        self.check(&tx)?;
        let mut status = outcome.clone();
        let (state, wake, interruptions) = match outcome {
            Status::Completed(_) => ("completed", 0, 0),
            Status::Faulted(_) => ("failed", 0, 0),
            Status::Cancelled => ("cancelled", 0, 0),
            Status::Waiting { wake_at_ms } => ("waiting", *wake_at_ms, 0),
            Status::Interrupted(error) => {
                let n = 1 + tx.query_row("SELECT interruptions FROM workflow_runs WHERE id = ?1", [&self.id], |r| {
                    r.get::<_, i64>(0)
                })?;
                if n >= MAX_INTERRUPTIONS {
                    status = Status::Faulted(WorkflowError::runtime(format!(
                        "gave up after {n} interruptions in a row: {error}"
                    )));
                    ("failed", 0, n)
                } else {
                    ("ready", now().saturating_add(backoff(n)), n)
                }
            }
        };
        let wake = if state == "waiting" && tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM workflow_events WHERE run_id = ?1 AND consumed_at_ms IS NULL)",
            [&self.id], |r| r.get::<_, bool>(0))? { now() } else { wake };
        let terminal = matches!(status, Status::Completed(_) | Status::Faulted(_) | Status::Cancelled);
        let encoded = terminal.then(|| serde_json::to_string(&status)).transpose()?;
        tx.execute(
            "UPDATE workflow_runs SET state = ?2, wake_at_ms = ?3, outcome = ?4,
            interruptions = ?5, owner = NULL, lease_until_ms = NULL, ended_ms = ?6 WHERE id = ?1",
            params![self.id, state, wake, encoded, interruptions, terminal.then(now)],
        )?;
        if terminal { tx.execute("DELETE FROM workflow_awaits WHERE run_id = ?1", [&self.id])?; }
        tx.commit()?;
        Ok(status)
    }

    /// Hands the run back as it is (the worker is stopping): not a failure,
    /// so it does not count towards MAX_INTERRUPTIONS.
    pub fn release(&self) -> anyhow::Result<()> {
        self.ended.store(true, Ordering::SeqCst);
        let time = now();
        let changed = self.db.conn().execute(
            "UPDATE workflow_runs SET state = 'ready', wake_at_ms = ?3, owner = NULL, lease_until_ms = NULL
            WHERE id = ?1 AND owner = ?2 AND state = 'running' AND lease_until_ms > ?3",
            params![self.id, self.owner, time],
        )?;
        anyhow::ensure!(changed == 1, "run {} lost its lease before release", self.id);
        Ok(())
    }
}

impl Journal for Lease {
    fn load(&self) -> anyhow::Result<BTreeMap<String, Record>> {
        let mut conn = self.db.conn();
        let tx = conn.transaction()?;
        self.check(&tx)?;
        let rows = {
            let mut statement = tx.prepare("SELECT key, record FROM workflow_records WHERE run_id = ?1")?;
            statement
                .query_map([&self.id], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let records = rows.into_iter().map(|(key, record)| Ok((key, serde_json::from_str(&record)?))).collect();
        tx.commit()?;
        records
    }

    fn save(&self, key: &str, record: &Record) -> anyhow::Result<()> {
        let encoded = serde_json::to_string(record)?;
        let mut conn = self.db.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        self.check(&tx)?;
        let prior: Option<String> = tx
            .query_row(
                "SELECT record FROM workflow_records WHERE run_id = ?1 AND key = ?2",
                params![self.id, key],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(prior) = prior {
            check_transition(&serde_json::from_str(&prior)?, record)?;
        }
        tx.execute(
            "INSERT INTO workflow_records (run_id, key, record) VALUES (?1, ?2, ?3)
            ON CONFLICT(run_id, key) DO UPDATE SET record = excluded.record",
            params![self.id, key, encoded],
        )?;
        tx.commit()?;
        Ok(())
    }
}

// ── 3. durable events ──────────────────────────────────────────────────────

impl owf::run::Events for Lease {
    fn register(&self, run_id: &str, types: &[String]) -> anyhow::Result<()> {
        anyhow::ensure!(run_id == self.id, "event run does not own this lease");
        let mut conn = self.db.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        self.check(&tx)?;
        for t in types {
            // Delivered or consumed tokens stay closed on replay. New loop
            // invocations can reuse a type once the preceding event was taken.
            tx.execute("INSERT INTO workflow_awaits (run_id,type,registered_at_ms)
                SELECT ?1,?2,?3 WHERE NOT EXISTS (SELECT 1 FROM workflow_events
                    WHERE run_id = ?1 AND type = ?2 AND consumed_at_ms IS NULL)
                ON CONFLICT(run_id,type) DO NOTHING", params![run_id,t,now()])?;
        }
        tx.commit()?;
        Ok(())
    }
    fn take(&self, run_id: &str, key: &str, types: &[String]) -> anyhow::Result<Option<Value>> {
        anyhow::ensure!(run_id == self.id, "event run does not own this lease");
        let mut conn = self.db.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        self.check(&tx)?;
        let saved: Option<String> = tx.query_row("SELECT record FROM workflow_records WHERE run_id = ?1 AND key = ?2",
            params![run_id,key], |r| r.get(0)).optional()?;
        if let Some(saved) = saved {
            let Record::Event { event } = serde_json::from_str(&saved)? else { anyhow::bail!("invalid event checkpoint") };
            return Ok(Some(event));
        }
        let candidate = {
            let mut stmt = tx.prepare("SELECT id,type,data,delivered_at_ms FROM workflow_events
                WHERE run_id = ?1 AND consumed_at_ms IS NULL ORDER BY id")?;
            stmt.query_map([run_id], |r| Ok((r.get::<_,i64>(0)?, r.get::<_,String>(1)?, r.get::<_,String>(2)?, r.get::<_,i64>(3)?)))?
                .collect::<rusqlite::Result<Vec<_>>>()?.into_iter().find(|(_,t,_,_)| types.contains(t))
        };
        let Some((id,t,data,time)) = candidate else { return Ok(None) };
        let event = serde_json::json!({"type":t,"data":serde_json::from_str::<Value>(&data)?,
            "time":chrono::DateTime::from_timestamp_millis(time).ok_or_else(|| anyhow::anyhow!("invalid event time"))?.to_rfc3339()});
        tx.execute("UPDATE workflow_events SET consumed_at_ms = ?2, consumed_key = ?3 WHERE id = ?1", params![id,now(),key])?;
        for t in types {
            tx.execute("DELETE FROM workflow_awaits WHERE run_id = ?1 AND type = ?2", params![run_id,t])?;
            // Two competing buttons may both arrive before the first take.
            // The loser must not keep waking a later, unrelated wait.
            tx.execute("DELETE FROM workflow_events WHERE run_id = ?1 AND type = ?2 AND consumed_at_ms IS NULL", params![run_id,t])?;
        }
        tx.execute("INSERT INTO workflow_records (run_id,key,record) VALUES (?1,?2,?3)",
            params![run_id,key,serde_json::to_string(&Record::Event { event: event.clone() })?])?;
        tx.commit()?;
        Ok(Some(event))
    }
}
