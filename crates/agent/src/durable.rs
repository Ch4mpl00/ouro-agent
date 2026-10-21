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
pub struct ChildLink<'a> {
    pub parent: &'a str,
    pub key: &'a str,
    pub reference: &'a str,
}

#[derive(Debug)]
pub struct SavedRun {
    pub id: String,
    pub name: String,
    pub parent_run: Option<String>,
    pub state: String,
    pub definition: String,
    pub input: Value,
    pub outcome: Option<Status>,
}

impl RunStore {
    pub fn new(db: AgentDb) -> Self {
        Self { db }
    }

    pub fn confirmations(&self) -> &dyn crate::confirmation::Store {
        &self.db
    }

    pub fn get(&self, id: &str) -> anyhow::Result<Option<SavedRun>> {
        let row = self
            .db
            .conn()
            .query_row(
                "SELECT name, definition, input, outcome, state, parent_run FROM workflow_runs WHERE id = ?1",
                [id],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, Option<String>>(3)?,
                        r.get::<_, String>(4)?,
                        r.get::<_, Option<String>>(5)?,
                    ))
                },
            )
            .optional()?;
        row.map(|(name, definition, input, outcome, state, parent_run)| {
            Ok(SavedRun {
                id: id.into(),
                name,
                parent_run,
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

    /// Child admission, ownership and the parent's subscriptions are atomic.
    /// Replaying the same invocation joins its immutable original definition.
    pub fn start_child(
        &self,
        id: &str,
        name: &str,
        definition: &str,
        input: &Value,
        link: ChildLink<'_>,
    ) -> anyhow::Result<Option<Lease>> {
        let mut conn = self.db.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let alive: bool =
            tx.query_row("SELECT ended_ms IS NULL FROM workflow_runs WHERE id = ?1", [link.parent], |r| r.get(0))?;
        anyhow::ensure!(alive, "parent run has ended");
        for t in [format!("done:{id}"), format!("waiting:{id}")] {
            tx.execute(
                "INSERT INTO workflow_awaits (run_id,type,registered_at_ms) VALUES (?1,?2,?3) ON CONFLICT DO NOTHING",
                params![link.parent, t, now()],
            )?;
        }
        // Keep completion subscribed across the gap between consuming waiting
        // and entering the parent's final done listen.
        tx.execute(
            "INSERT INTO workflow_listeners (run_id,key,type) VALUES (?1,?2,?3) ON CONFLICT DO NOTHING",
            params![link.parent, format!("child:{id}"), format!("done:{id}")],
        )?;
        let owner = uuid::Uuid::new_v4().to_string();
        let inserted = tx.execute("INSERT INTO workflow_runs (id,name,definition,input,state,owner,lease_until_ms,parent_run,child_key,child_reference)
            VALUES (?1,?2,?3,?4,'running',?5,?6,?7,?8,?9) ON CONFLICT DO NOTHING",
            params![id,name,definition,serde_json::to_string(input)?,owner,now()+LEASE_MS,link.parent,link.key,link.reference])? == 1;
        // Adopt workers admitted before child ownership existed. Their run id
        // and input stay unchanged, so an in-flight worker is never restarted.
        tx.execute(
            "UPDATE workflow_runs SET parent_run=?2,child_key=?3,child_reference=?4 WHERE id=?1 AND parent_run IS NULL",
            params![id, link.parent, link.key, link.reference],
        )?;
        let valid: bool = tx.query_row(
            "SELECT name=?2 AND parent_run=?3 AND child_key=?4 AND child_reference=?5 FROM workflow_runs WHERE id=?1",
            params![id, name, link.parent, link.key, link.reference],
            |r| r.get(0),
        )?;
        anyhow::ensure!(valid, "child run does not belong to this invocation");
        // Replaying after the child ended must make its outcome reachable too.
        if !inserted {
            let (state, outcome): (String, Option<String>) =
                tx.query_row("SELECT state,outcome FROM workflow_runs WHERE id=?1", [id], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })?;
            if let Some(outcome) = outcome {
                lifecycle(&tx, id, &serde_json::from_str(&outcome)?)?;
            } else if state == "waiting" {
                lifecycle(&tx, id, &Status::Waiting { wake_at_ms: 0 })?;
            }
        }
        tx.commit()?;
        Ok(inserted.then(|| Lease { db: self.db.clone(), id: id.into(), owner, ended: AtomicBool::new(false) }))
    }

    /// Outcomes are trusted only after checking the persisted parent and key.
    pub fn child_outcome(&self, parent: &str, key: &str, id: &str) -> anyhow::Result<Option<Status>> {
        let conn = self.db.conn();
        let row: Option<(Option<String>, Option<String>, Option<String>)> = conn
            .query_row("SELECT parent_run,child_key,outcome FROM workflow_runs WHERE id=?1", [id], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })
            .optional()?;
        let (p, k, outcome) = row.ok_or_else(|| anyhow::anyhow!("unknown child run"))?;
        anyhow::ensure!(
            p.as_deref() == Some(parent) && k.as_deref() == Some(key),
            "child run does not belong to this parent and key"
        );
        outcome.map(|o| Ok(serde_json::from_str(&o)?)).transpose()
    }

    pub fn pending_child_confirmation(&self, parent: &str, call: &str) -> anyhow::Result<bool> {
        Ok(self.db.conn().query_row("SELECT EXISTS(SELECT 1 FROM confirmations c JOIN workflow_runs g ON g.id=c.run_id WHERE g.parent_run=?1 AND g.name='guard' AND c.call_key=?2 AND c.outcome IS NULL)",params![parent,call],|r|r.get(0))?)
    }

    /// One reviewed call authorizes one host invocation. A malicious loop
    /// cannot spend a confirmed verdict again by lying about its round.
    pub fn use_guard_verdict(&self, child: &str, call: &str, invocation: &str) -> anyhow::Result<bool> {
        let mut conn = self.db.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT INTO guard_verdict_uses (child_run,call_key,invocation) VALUES (?1,?2,?3) ON CONFLICT DO NOTHING",
            params![child, call, invocation],
        )?;
        let same = tx.query_row(
            "SELECT invocation=?3 FROM guard_verdict_uses WHERE child_run=?1 AND call_key=?2",
            params![child, call, invocation],
            |r| r.get(0),
        )?;
        tx.commit()?;
        Ok(same)
    }

    /// One event belongs to one live run. A pending delivery excludes a second
    /// copy; taking it removes its registration, closing double presses.
    pub fn deliver(&self, type_: &str, data: &Value) -> anyhow::Result<bool> {
        let mut conn = self.db.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(run) = deliver(&tx, type_, data, None)? else { return Ok(false) };
        tx.commit()?;
        if let Some(notify) =
            self.db.notifications.lock().unwrap_or_else(|e| e.into_inner()).get(&run).and_then(std::sync::Weak::upgrade)
        {
            notify.notify_waiters();
        }
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
        tx.execute("DELETE FROM workflow_listeners WHERE run_id = ?1", [id])?;
        if changed {
            cancel_children(&tx, id)?;
            lifecycle(&tx, id, &Status::Cancelled)?;
        }
        tx.commit()?;
        notify_runs(&self.db);
        Ok(changed)
    }

    /// Deletes the records of runs that ended `kept_ms` or more ago and
    /// returns how many it deleted. The run rows stay, so a finished run
    /// still reports its outcome and is never resumed.
    pub fn sweep(&self, kept_ms: i64) -> anyhow::Result<usize> {
        let mut conn = self.db.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute("DELETE FROM guard_verdict_uses WHERE child_run IN (SELECT c.id FROM workflow_runs c JOIN workflow_runs p ON p.id=c.parent_run WHERE p.ended_ms<=?1)",[now()-kept_ms])?;
        tx.execute(
            "DELETE FROM confirmations WHERE run_id IN (SELECT id FROM workflow_runs WHERE ended_ms <= ?1)",
            [now() - kept_ms],
        )?;
        tx.execute(
            "DELETE FROM workflow_events WHERE run_id IN (SELECT id FROM workflow_runs WHERE ended_ms <= ?1)",
            [now() - kept_ms],
        )?;
        let deleted = tx.execute(
            "DELETE FROM workflow_records WHERE run_id IN (SELECT id FROM workflow_runs WHERE ended_ms <= ?1)",
            [now() - kept_ms],
        )?;
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
        let wake = if state == "waiting"
            && tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM workflow_events e JOIN workflow_awaits a ON a.run_id = e.run_id AND a.type = e.type WHERE e.run_id = ?1 AND e.consumed_at_ms IS NULL)",
                [&self.id],
                |r| r.get::<_, bool>(0),
            )? {
            now()
        } else {
            wake
        };
        let terminal = matches!(status, Status::Completed(_) | Status::Faulted(_) | Status::Cancelled);
        let encoded = terminal.then(|| serde_json::to_string(&status)).transpose()?;
        tx.execute(
            "UPDATE workflow_runs SET state = ?2, wake_at_ms = ?3, outcome = ?4,
            interruptions = ?5, owner = NULL, lease_until_ms = NULL, ended_ms = ?6 WHERE id = ?1",
            params![self.id, state, wake, encoded, interruptions, terminal.then(now)],
        )?;
        if terminal {
            tx.execute("DELETE FROM workflow_awaits WHERE run_id = ?1", [&self.id])?;
            tx.execute("DELETE FROM workflow_listeners WHERE run_id = ?1", [&self.id])?;
        }
        if terminal {
            cancel_children(&tx, &self.id)?;
        }
        lifecycle(&tx, &self.id, &status)?;
        tx.commit()?;
        notify_runs(&self.db);
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
    fn notification(&self, run_id: &str) -> futures::future::BoxFuture<'_, ()> {
        let mut notifications = self.db.notifications.lock().unwrap_or_else(|e| e.into_inner());
        notifications.retain(|_, n| n.strong_count() > 0);
        let notify = notifications.get(run_id).and_then(std::sync::Weak::upgrade).unwrap_or_else(|| {
            let notify = std::sync::Arc::new(tokio::sync::Notify::new());
            notifications.insert(run_id.into(), std::sync::Arc::downgrade(&notify));
            notify
        });
        let mut notified = Box::pin(notify.notified_owned());
        notified.as_mut().enable();
        notified
    }

    fn unregister(&self, run_id: &str, key: &str, types: &[String]) -> anyhow::Result<()> {
        anyhow::ensure!(run_id == self.id, "event run does not own this lease");
        let mut conn = self.db.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        self.check(&tx)?;
        tx.execute("DELETE FROM workflow_listeners WHERE run_id = ?1 AND key = ?2", params![run_id, key])?;
        close_awaits(&tx, run_id, types)?;
        tx.commit()?;
        Ok(())
    }

    fn register(&self, run_id: &str, types: &[String]) -> anyhow::Result<()> {
        anyhow::ensure!(run_id == self.id, "event run does not own this lease");
        let mut conn = self.db.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        self.check(&tx)?;
        for t in types {
            tx.execute(
                "INSERT INTO workflow_awaits (run_id,type,registered_at_ms) VALUES (?1,?2,?3)
                ON CONFLICT(run_id,type) DO NOTHING",
                params![run_id, t, now()],
            )?;
        }
        tx.commit()?;
        Ok(())
    }
    fn take(&self, run_id: &str, key: &str, types: &[String]) -> anyhow::Result<Option<Value>> {
        anyhow::ensure!(run_id == self.id, "event run does not own this lease");
        let mut conn = self.db.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        self.check(&tx)?;
        let saved: Option<String> = tx
            .query_row(
                "SELECT record FROM workflow_records WHERE run_id = ?1 AND key = ?2",
                params![run_id, key],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(saved) = saved {
            let Record::Event { event } = serde_json::from_str(&saved)? else {
                anyhow::bail!("invalid event checkpoint")
            };
            return Ok(Some(event));
        }
        let candidate = {
            let mut stmt = tx.prepare(
                "SELECT id,type,data,delivered_at_ms FROM workflow_events
                WHERE run_id = ?1 AND consumed_at_ms IS NULL ORDER BY id",
            )?;
            stmt.query_map([run_id], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?, r.get::<_, i64>(3)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
            .into_iter()
            .find(|(_, t, _, _)| types.contains(t))
        };
        let Some((id, t, data, time)) = candidate else {
            // Several concurrent listens may share a type. Keep each empty
            // invocation subscribed, so another listener's take cannot close it.
            for t in types {
                tx.execute(
                    "INSERT INTO workflow_listeners (run_id,key,type) VALUES (?1,?2,?3) ON CONFLICT DO NOTHING",
                    params![run_id, key, t],
                )?;
            }
            tx.commit()?;
            return Ok(None);
        };
        let event = serde_json::json!({"type":t,"data":serde_json::from_str::<Value>(&data)?,
            "time":chrono::DateTime::from_timestamp_millis(time).ok_or_else(|| anyhow::anyhow!("invalid event time"))?.to_rfc3339()});
        tx.execute(
            "UPDATE workflow_events SET consumed_at_ms = ?2, consumed_key = ?3 WHERE id = ?1",
            params![id, now(), key],
        )?;
        // The child-completion subscription survives the waiting→done gap,
        // then closes with its one consumed completion event.
        if let Some(child) = t.strip_prefix("done:") {
            tx.execute(
                "DELETE FROM workflow_listeners WHERE run_id=?1 AND key=?2",
                params![run_id, format!("child:{child}")],
            )?;
        }
        tx.execute("DELETE FROM workflow_listeners WHERE run_id = ?1 AND key = ?2", params![run_id, key])?;
        close_awaits(&tx, run_id, types)?;
        tx.execute(
            "INSERT INTO workflow_records (run_id,key,record) VALUES (?1,?2,?3)",
            params![run_id, key, serde_json::to_string(&Record::Event { event: event.clone() })?],
        )?;
        tx.commit()?;
        Ok(Some(event))
    }
}

// A run/type registration is shared by every unfinished listen of that type.
// Closing one invocation must leave its concurrent siblings reachable.
fn close_awaits(tx: &rusqlite::Transaction<'_>, run_id: &str, types: &[String]) -> anyhow::Result<()> {
    for t in types {
        tx.execute(
            "DELETE FROM workflow_awaits WHERE run_id = ?1 AND type = ?2 AND NOT EXISTS
            (SELECT 1 FROM workflow_listeners WHERE run_id = ?1 AND type = ?2)",
            params![run_id, t],
        )?;
    }
    Ok(())
}

// Lifecycle state and event insertion share the finishing transaction. A crash
// cannot strand a parent between committing the child and notifying it.
fn deliver(
    tx: &rusqlite::Transaction<'_>,
    type_: &str,
    data: &Value,
    target: Option<&str>,
) -> anyhow::Result<Option<String>> {
    let run: Option<String> = tx.query_row(
        "SELECT a.run_id FROM workflow_awaits a JOIN workflow_runs r ON r.id=a.run_id
         WHERE a.type=?1 AND (?2 IS NULL OR a.run_id=?2) AND r.ended_ms IS NULL AND r.state IN ('ready','running','waiting')
         AND NOT EXISTS (SELECT 1 FROM workflow_events e WHERE e.run_id=a.run_id AND e.type=a.type AND e.consumed_at_ms IS NULL)
         ORDER BY a.registered_at_ms,a.run_id LIMIT 1", params![type_,target], |r|r.get(0)).optional()?;
    if let Some(run) = &run {
        tx.execute(
            "INSERT INTO workflow_events (run_id,type,data,delivered_at_ms) VALUES (?1,?2,?3,?4)",
            params![run, type_, serde_json::to_string(data)?, now()],
        )?;
        tx.execute("UPDATE workflow_runs SET wake_at_ms=?2 WHERE id=?1", params![run, now()])?;
    }
    Ok(run)
}

fn lifecycle(tx: &rusqlite::Transaction<'_>, id: &str, status: &Status) -> anyhow::Result<()> {
    let parent: Option<String> =
        tx.query_row("SELECT parent_run FROM workflow_runs WHERE id=?1", [id], |r| r.get(0))?;
    if parent.is_none() {
        return Ok(());
    }
    let (kind, data) = match status {
        Status::Completed(output) => ("done", serde_json::json!({"status":"completed","output":output})),
        Status::Faulted(error) => ("done", serde_json::json!({"status":"faulted","error":error})),
        Status::Cancelled => ("done", serde_json::json!({"status":"cancelled"})),
        Status::Waiting { .. } => ("waiting", serde_json::json!({"status":"waiting"})),
        Status::Interrupted(_) => return Ok(()),
    };
    let type_ = format!("{kind}:{id}");
    // Joining an ended child again must not recreate a completion the parent
    // already consumed. Unlike ordinary events, completion happens once.
    if kind == "done"
        && tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM workflow_events WHERE run_id=?1 AND type=?2)",
            params![parent, type_],
            |r| r.get::<_, bool>(0),
        )?
    {
        return Ok(());
    }
    deliver(tx, &type_, &data, parent.as_deref())?;
    Ok(())
}

fn cancel_children(tx: &rusqlite::Transaction<'_>, parent: &str) -> anyhow::Result<()> {
    let children = {
        let mut stmt = tx.prepare("SELECT id FROM workflow_runs WHERE parent_run=?1 AND ended_ms IS NULL")?;
        stmt.query_map([parent], |r| r.get::<_, String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?
    };
    for child in children {
        tx.execute("UPDATE workflow_runs SET state='cancelled',outcome=?2,ended_ms=?3,owner=NULL,lease_until_ms=NULL WHERE id=?1", params![child,serde_json::to_string(&Status::Cancelled)?,now()])?;
        tx.execute("DELETE FROM workflow_awaits WHERE run_id=?1", [&child])?;
        tx.execute("DELETE FROM workflow_listeners WHERE run_id=?1", [&child])?;
        cancel_children(tx, &child)?;
        lifecycle(tx, &child, &Status::Cancelled)?;
    }
    Ok(())
}

fn notify_runs(db: &AgentDb) {
    let mut notifications = db.notifications.lock().unwrap_or_else(|e| e.into_inner());
    notifications.retain(|_, n| {
        if let Some(n) = n.upgrade() {
            n.notify_waiters();
            true
        } else {
            false
        }
    });
}
