//! Safety is checked against the real SQLite rows after every operation:
//! one pending delivery per run/type, one event per invocation, journal ↔
//! consumption atomicity, terminal cleanup and lease fencing. The final
//! drains are liveness checks, not just assertions that nothing was sent.
mod support;
use agent::{
    db::AgentDb,
    durable::{ChildLink, Lease, RunStore},
};
use async_trait::async_trait;
use futures::{future::BoxFuture, poll};
use owf::{
    durable::{Journal, Record},
    run::{self, Clock, Events, Functions, Host, Limits, Silent, Status, WorkflowError},
};
use proptest::{
    prelude::*,
    test_runner::{Config, TestRunner},
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use support::properties::{Witnesses, runs};
use tokio_util::sync::CancellationToken;

fn count(db: &AgentDb, sql: &str) -> i64 {
    db.conn().query_row(sql, [], |r| r.get(0)).unwrap()
}

fn audit(db: &AgentDb) {
    assert_eq!(
        count(
            db,
            "SELECT count(*) FROM (SELECT run_id,type FROM workflow_events WHERE consumed_at_ms IS NULL GROUP BY run_id,type HAVING count(*)>1)"
        ),
        0
    );
    assert_eq!(
        count(
            db,
            "SELECT count(*) FROM workflow_awaits a JOIN workflow_runs r ON r.id=a.run_id WHERE r.ended_ms IS NOT NULL"
        ),
        0
    );
    assert_eq!(
        count(
            db,
            "SELECT count(*) FROM workflow_listeners a JOIN workflow_runs r ON r.id=a.run_id WHERE r.ended_ms IS NOT NULL"
        ),
        0
    );
    assert_eq!(
        count(
            db,
            "SELECT count(*) FROM (SELECT run_id,consumed_key FROM workflow_events WHERE consumed_at_ms IS NOT NULL GROUP BY run_id,consumed_key HAVING count(*)>1)"
        ),
        0
    );
    // Sweep removes both sides of this equality together for ended runs.
    let conn = db.conn();
    let mut stmt = conn.prepare("SELECT e.type,e.data,e.consumed_at_ms,r.record FROM workflow_events e LEFT JOIN workflow_records r ON r.run_id=e.run_id AND r.key=e.consumed_key").unwrap();
    for row in stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<i64>>(2)?,
                r.get::<_, Option<String>>(3)?,
            ))
        })
        .unwrap()
    {
        let (t, data, consumed, record) = row.unwrap();
        assert_eq!(consumed.is_some(), record.is_some());
        if let Some(record) = record {
            let Record::Event { event } = serde_json::from_str(&record).unwrap() else {
                panic!("consumption without event checkpoint")
            };
            assert_eq!(event["type"], t);
            assert_eq!(event["data"], serde_json::from_str::<Value>(&data).unwrap());
        }
    }
    assert_eq!(conn.query_row("SELECT count(*) FROM workflow_records r LEFT JOIN workflow_events e ON e.run_id=r.run_id AND e.consumed_key=r.key WHERE json_extract(r.record,'$.kind')='event' AND e.id IS NULL", [], |r| r.get::<_,i64>(0)).unwrap(), 0);
}

fn expired(db: &AgentDb, id: &str) {
    db.conn().execute("UPDATE workflow_runs SET lease_until_ms=0,wake_at_ms=0 WHERE id=?1", [id]).unwrap();
}

// Fail statements *inside* the production transactions, after SQLite has
// changed rows. RAISE(ABORT) rolls the statement back; dropping the transaction
// must roll back earlier statements too. No production fault-injection seam.
fn fault(db: &AgentDb, table: &str, action: &str) {
    assert!(["workflow_records", "workflow_events", "workflow_listeners", "workflow_runs"].contains(&table));
    assert!(["INSERT", "UPDATE", "DELETE"].contains(&action));
    db.conn()
        .execute_batch(&format!(
            "CREATE TRIGGER injected AFTER {action} ON {table} BEGIN SELECT RAISE(ABORT,'injected crash'); END;"
        ))
        .unwrap();
}
fn unfault(db: &AgentDb) {
    db.conn().execute_batch("DROP TRIGGER injected").unwrap();
}

fn store_case(ops: Vec<(u8, usize, usize, usize)>, witnesses: &Witnesses) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("agent.db");
    let mut db = AgentDb::open(&path).unwrap();
    let mut store = RunStore::new(db.clone());
    let mut leases: Vec<Option<Lease>> =
        (0..3).map(|i| store.start(&format!("r{i}"), "w", "x", &json!({})).unwrap()).collect();
    // Every generated history starts with two listeners of one type. Take
    // and timeout alternate with generated histories below; no path lottery.
    let l = leases[0].as_ref().unwrap();
    l.register("r0", &["shared".into()]).unwrap();
    for key in ["first", "second"] {
        assert!(l.take("r0", key, &["shared".into()]).unwrap().is_none());
    }
    assert!(store.deliver("shared", &json!({"n":-1})).unwrap());
    let first = l.take("r0", "first", &["shared".into()]).unwrap();
    assert!(first.is_some());
    assert_eq!(l.take("r0", "first", &["shared".into()]).unwrap(), first);
    witnesses.hit("replayed consumption");
    assert!(store.deliver("shared", &json!({"n":-2})).unwrap());
    assert!(l.take("r0", "second", &["shared".into()]).unwrap().is_some());
    witnesses.hit("concurrent same-type listeners");
    audit(&db);
    for (step, (op, r, k, t)) in ops.into_iter().enumerate() {
        let id = format!("r{r}");
        let key = format!("listen{k}");
        let types = vec![format!("type{t}")];
        match op {
            0 | 1 => {
                if let Some(l) = &leases[r] {
                    l.register(&id, &types).unwrap();
                    let prior = l.load().unwrap().get(&key).cloned();
                    let before = count(&db, "SELECT count(*) FROM workflow_events WHERE consumed_at_ms IS NOT NULL");
                    let taken = l.take(&id, &key, &types).unwrap();
                    let after = count(&db, "SELECT count(*) FROM workflow_events WHERE consumed_at_ms IS NOT NULL");
                    assert_eq!(after - before, i64::from(taken.is_some() && prior.is_none()));
                    if let Some(Record::Event { event }) = prior {
                        assert_eq!(taken, Some(event));
                        witnesses.hit("replayed consumption");
                    }
                }
            }
            2 | 3 => {
                let eligible: i64 = db.conn().query_row("SELECT count(*) FROM workflow_awaits a JOIN workflow_runs r ON r.id=a.run_id WHERE a.type=?1 AND r.ended_ms IS NULL AND NOT EXISTS(SELECT 1 FROM workflow_events e WHERE e.run_id=a.run_id AND e.type=a.type AND e.consumed_at_ms IS NULL)", [&types[0]], |r| r.get(0)).unwrap();
                let before = count(&db, "SELECT count(*) FROM workflow_events");
                let delivered = store.deliver(&types[0], &json!({"step":step})).unwrap();
                assert_eq!(delivered, eligible > 0);
                assert_eq!(count(&db, "SELECT count(*) FROM workflow_events") - before, i64::from(delivered));
                witnesses.hit(if delivered { "stored delivery" } else { "rejected delivery" });
            }
            4 => {
                if let Some(l) = &leases[r] {
                    l.unregister(&id, &key, &types).unwrap();
                }
            }
            5 => {
                store.cancel(&id).unwrap();
                leases[r] = None;
            }
            6 => {
                drop(leases);
                drop(store);
                drop(db);
                db = AgentDb::open(&path).unwrap();
                store = RunStore::new(db.clone());
                leases = (0..3).map(|i| store.claim(&format!("r{i}")).unwrap()).collect();
                witnesses.hit("reopen");
            }
            7 => {
                store.sweep(0).unwrap();
            }
            8 => {
                if let Some(l) = &leases[r] {
                    l.register(&id, &types).unwrap();
                    fault(&db, "workflow_events", "INSERT");
                    let before = count(&db, "SELECT count(*) FROM workflow_events");
                    let result = store.deliver(&types[0], &json!({"crash":step}));
                    unfault(&db);
                    if result.is_err() {
                        witnesses.hit("delivery rollback");
                    }
                    assert_eq!(count(&db, "SELECT count(*) FROM workflow_events"), before);
                }
            }
            _ => unreachable!(),
        }
        audit(&db);
    }
    // Drive every surviving listener: a finite input history cannot stall.
    for (r, lease) in leases.iter().enumerate() {
        if let Some(l) = lease {
            let id = format!("r{r}");
            let listeners: Vec<(String, String)> = {
                let conn = db.conn();
                let mut s =
                    conn.prepare("SELECT key,type FROM workflow_listeners WHERE run_id=?1 ORDER BY key").unwrap();
                s.query_map([&id], |r| Ok((r.get(0)?, r.get(1)?))).unwrap().map(Result::unwrap).collect()
            };
            for (key, t) in listeners {
                // Delivery selects the oldest eligible run, which can be a
                // sibling after an unregister/register. Fill those inboxes
                // too; one pending event excludes each from the next pass.
                let mut taken = false;
                for _ in 0..leases.len() {
                    store.deliver(&t, &json!({"drain":true})).unwrap();
                    taken = l.take(&id, &key, std::slice::from_ref(&t)).unwrap().is_some();
                    audit(&db);
                    if taken {
                        break;
                    }
                }
                assert!(taken, "listener stalled after driving every eligible inbox");
            }
            l.finish(&Status::Completed(json!("drained"))).unwrap();
        }
    }
    audit(&db);
}

#[test]
fn event_histories_preserve_consumption_and_terminal_cleanup() {
    let w = Witnesses::new(&[
        "concurrent same-type listeners",
        "replayed consumption",
        "stored delivery",
        "rejected delivery",
        "reopen",
        "delivery rollback",
    ]);
    TestRunner::new(Config {
        cases: runs(),
        source_file: Some(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/durable_properties.rs")),
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::WithSource(
            "proptest-regressions",
        ))),
        ..Config::default()
    })
    .run(&prop::collection::vec((0u8..9, 0usize..3, 0usize..4, 0usize..3), 32..65), |ops| {
        store_case(ops, &w);
        Ok(())
    })
    .unwrap();
    w.finish();
}

#[test]
fn take_rolls_back_consumption_and_subscription_when_checkpoint_insert_fails() {
    for (table, action) in
        [("workflow_events", "UPDATE"), ("workflow_listeners", "DELETE"), ("workflow_records", "INSERT")]
    {
        let db = AgentDb::open(":memory:").unwrap();
        let runs = RunStore::new(db.clone());
        let lease = runs.start("r", "w", "x", &json!({})).unwrap().unwrap();
        lease.register("r", &["x".into()]).unwrap();
        assert!(lease.take("r", "listen", &["x".into()]).unwrap().is_none());
        assert!(runs.deliver("x", &json!("original")).unwrap());
        fault(&db, table, action);
        assert!(lease.take("r", "listen", &["x".into()]).is_err());
        unfault(&db);
        audit(&db);
        assert_eq!(count(&db, "SELECT count(*) FROM workflow_awaits"), 1);
        assert_eq!(count(&db, "SELECT count(*) FROM workflow_listeners"), 1);
        assert_eq!(lease.take("r", "listen", &["x".into()]).unwrap().unwrap()["data"], "original");
        audit(&db);
    }
}

struct Time(tokio::time::Instant);
impl Clock for Time {
    fn now(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::from_timestamp_millis(1_000 + self.0.elapsed().as_millis() as i64).unwrap()
    }
}

struct Inbox {
    lease: Arc<Lease>,
    saves: AtomicUsize,
    fail: usize,
    after_take: bool,
    after_save: bool,
}
impl Journal for Inbox {
    fn load(&self) -> anyhow::Result<BTreeMap<String, Record>> {
        self.lease.load()
    }
    fn save(&self, key: &str, record: &Record) -> anyhow::Result<()> {
        let n = self.saves.fetch_add(1, Ordering::SeqCst);
        anyhow::ensure!(self.after_save || n != self.fail, "crash before commit {n}");
        self.lease.save(key, record)?;
        anyhow::ensure!(!self.after_save || n != self.fail, "crash after commit {n}");
        Ok(())
    }
}
impl Events for Inbox {
    fn notification(&self, id: &str) -> BoxFuture<'_, ()> {
        self.lease.notification(id)
    }
    fn register(&self, id: &str, t: &[String]) -> anyhow::Result<()> {
        self.lease.register(id, t)
    }
    fn unregister(&self, id: &str, k: &str, t: &[String]) -> anyhow::Result<()> {
        self.lease.unregister(id, k, t)
    }
    fn take(&self, id: &str, k: &str, t: &[String]) -> anyhow::Result<Option<Value>> {
        let event = self.lease.take(id, k, t)?;
        anyhow::ensure!(!self.after_take || event.is_none(), "crash after atomic take");
        Ok(event)
    }
}
#[async_trait]
impl Functions for Inbox {
    fn effect(&self, _: &str) -> Option<run::Effect> {
        None
    }
    fn events(&self) -> Option<&dyn Events> {
        Some(self)
    }
    async fn call(&self, _: &str, _: Value, _: &run::CallInfo<'_>) -> Result<Value, WorkflowError> {
        unreachable!()
    }
}
fn host(inbox: Arc<Inbox>, time: Arc<Time>) -> Host {
    Host { functions: inbox, observer: Arc::new(Silent), clock: time, limits: Limits::default(), runtime: json!({}) }
}

async fn listen_case(mode: u8, hold: u64, timeout: u64, fail: usize, w: &Witnesses) {
    let timeout = if mode == 3 { timeout } else { timeout + hold + 1 };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("agent.db");
    let db = AgentDb::open(&path).unwrap();
    let runs = RunStore::new(db.clone());
    let lease = Arc::new(runs.start("r", "listen", "x", &json!({})).unwrap().unwrap());
    let time = Arc::new(Time(tokio::time::Instant::now()));
    let yaml = format!(
        "document: {{ dsl: '1.0.4', namespace: t, name: w, version: '1.0.0' }}\ndo:\n - attempt:\n     try:\n       - listen:\n           listen: {{ to: {{ one: {{ with: {{ type: x }} }} }} }}\n           metadata: {{ hold: PT{:.3}S }}\n           timeout: {{ after: PT{:.3}S }}\n     catch:\n       errors: {{ with: {{ status: 408 }} }}\n       do: [ {{ expired: {{ set: {{ expired: true }} }} }} ]\n",
        hold as f64 / 1000.0,
        timeout as f64 / 1000.0
    );
    let wf = owf::model::parse_yaml(&yaml).unwrap();
    let token = CancellationToken::new();
    // Exercise each checkpoint boundary for every generated ordering, plus
    // the crash between atomic take and the next interpreter checkpoint.
    let after_save = (13..25).contains(&fail);
    let inbox = Arc::new(Inbox {
        lease: lease.clone(),
        saves: AtomicUsize::new(0),
        fail: if after_save { fail - 13 } else { fail },
        after_take: fail == 12,
        after_save,
    });
    let h = host(inbox.clone(), time.clone());
    if mode == 0 {
        lease.register("r", &["x".into()]).unwrap();
        assert!(runs.deliver("x", &json!("answer")).unwrap());
    }
    let mut future = Box::pin(run::resume(&h, &wf, "r", json!({}), &token, &*inbox));
    let first = match poll!(future.as_mut()) {
        std::task::Poll::Ready(o) => o.status,
        std::task::Poll::Pending => {
            if mode == 1 {
                tokio::time::advance(Duration::from_millis(hold.min(timeout) / 2)).await;
                assert!(runs.deliver("x", &json!("answer")).unwrap());
            } else {
                tokio::time::advance(Duration::from_millis(hold.min(timeout))).await;
            }
            future.await.status
        }
    };
    audit(&db);
    let interrupted = matches!(first, Status::Interrupted(_));
    if interrupted {
        w.hit("checkpoint crash");
        if after_save {
            w.hit("after commit crash");
        }
    }
    if matches!(first, Status::Waiting { .. }) {
        lease.finish(&first).unwrap();
        w.hit("suspended");
    } else if !interrupted {
        lease.finish(&first).unwrap();
    }
    // Expire the old lease and reopen with a distinct SQLite connection.
    // Keeping the old object alive tests that a stale executor is fenced.
    expired(&db, "r");
    let reopened = AgentDb::open(&path).unwrap();
    let recovered = RunStore::new(reopened.clone());
    if recovered.get("r").unwrap().unwrap().outcome.is_none() {
        let current = Arc::new(recovered.claim("r").unwrap().unwrap());
        assert!(lease.renew().is_err());
        let next = Arc::new(Inbox {
            lease: current.clone(),
            saves: AtomicUsize::new(0),
            fail: usize::MAX,
            after_take: false,
            after_save: false,
        });
        let h = host(next.clone(), time.clone());
        // Crashing before registration needs recovery to register first.
        if mode != 3 {
            current.register("r", &["x".into()]).unwrap();
            let replacement =
                if count(&reopened, "SELECT count(*) FROM workflow_events") > 0 { "replacement" } else { "answer" };
            recovered.deliver("x", &json!(replacement)).unwrap();
        } else {
            tokio::time::advance(Duration::from_millis(timeout + 1)).await;
        }
        let mut status = run::resume(&h, &wf, "r", json!({}), &token, &*next).await.status;
        if mode == 3 && matches!(status, Status::Waiting { .. }) {
            tokio::time::advance(Duration::from_millis(timeout + 1)).await;
            status = run::resume(&h, &wf, "r", json!({}), &token, &*next).await.status;
        }
        assert!(matches!(status, Status::Completed(_)), "recovery stalled: mode={mode} fail={fail}: {status:?}");
        current.finish(&status).unwrap();
    }
    let outcome = recovered.get("r").unwrap().unwrap().outcome.unwrap();
    if mode == 3 {
        assert_eq!(outcome, Status::Completed(json!({"expired":true})));
        w.hit("catchable timeout");
    } else {
        let Status::Completed(v) = outcome else { panic!("{outcome:?}") };
        assert_eq!(v[0]["data"], "answer");
        w.hit("delivered and completed");
    }
    audit(&reopened);
    assert_eq!(
        count(
            &reopened,
            "SELECT count(*) FROM workflow_records WHERE key LIKE '%/expired#task' AND json_type(record,'$.result')='object'"
        ),
        i64::from(mode == 3),
        "timeout handler completed exactly once"
    );
    assert_eq!(
        count(&reopened, "SELECT count(*) FROM workflow_events WHERE consumed_at_ms IS NOT NULL"),
        i64::from(mode != 3)
    );
    if mode == 1 && fail == usize::MAX {
        assert!(!matches!(first, Status::Waiting { .. }));
        w.hit("inside hold");
    }
}

#[test]
fn listen_hold_delivery_timeout_and_every_checkpoint_crash() {
    let w = Witnesses::new(&[
        "checkpoint crash",
        "after commit crash",
        "suspended",
        "catchable timeout",
        "delivered and completed",
        "inside hold",
    ]);
    TestRunner::new(Config {
        cases: runs(),
        source_file: Some(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/durable_properties.rs")),
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::WithSource(
            "proptest-regressions",
        ))),
        ..Config::default()
    })
    .run(&(2u64..20, 2u64..20, 0usize..25), |(hold, timeout, fail)| {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap();
        rt.block_on(async {
            for mode in 0..4 {
                for failure in [fail, usize::MAX] {
                    listen_case(mode, hold, timeout, failure, &w).await;
                }
            }
        });
        Ok(())
    })
    .unwrap();
    w.finish();
}

fn child_link<'a>(parent: &'a str, key: &'a str) -> ChildLink<'a> {
    ChildLink { parent, key, reference: "/start" }
}
fn child_case(order: Vec<(u8, bool)>, cancel: bool, w: &Witnesses) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("agent.db");
    let db = AgentDb::open(&path).unwrap();
    let runs = RunStore::new(db.clone());
    let mut parent = runs.start("p", "parent", "original", &json!({})).unwrap().unwrap();
    let stranger = runs.start("stranger", "parent", "x", &json!({})).unwrap().unwrap();
    let mut ids = Vec::new();
    for (i, (outcome, crash)) in order.into_iter().enumerate() {
        let id = format!("child{i}");
        let key = format!("key{i}");
        let mut child =
            runs.start_child(&id, "child", "original", &json!({"i":i}), child_link("p", &key)).unwrap().unwrap();
        assert!(runs.start_child(&id, "child", "changed", &json!({}), child_link("p", &key)).unwrap().is_none());
        assert!(runs.start_child(&id, "child", "x", &json!({}), child_link("stranger", &key)).is_err());
        assert!(runs.child_outcome("stranger", &key, &id).is_err());
        assert!(runs.child_outcome("p", "foreign-key", &id).is_err());
        let saved = runs.get(&id).unwrap().unwrap();
        assert_eq!(saved.definition, "original");
        assert_eq!(saved.input, json!({"i":i}));
        audit(&db);
        if crash {
            expired(&db, &id);
            let reopened = RunStore::new(AgentDb::open(&path).unwrap());
            let next = reopened.claim(&id).unwrap().unwrap();
            assert!(child.renew().is_err());
            child = next;
            expired(&db, "p");
            let next = reopened.claim("p").unwrap().unwrap();
            assert!(parent.load().is_err());
            parent = next;
            w.hit("parent and child restart");
        }
        let expected = match outcome {
            0 => Status::Completed(json!({"answer":i})),
            1 => Status::Faulted(WorkflowError::runtime("generated failure")),
            2 => Status::Cancelled,
            3 => {
                child.register(&id, &[format!("input:{id}")]).unwrap();
                child.take(&id, "input", &[format!("input:{id}")]).unwrap();
                child.finish(&Status::Waiting { wake_at_ms: i64::MAX }).unwrap();
                let waiting = parent.take("p", &format!("waiting{i}"), &[format!("waiting:{id}")]).unwrap().unwrap();
                assert_eq!(waiting["data"], json!({"status":"waiting"}));
                assert!(runs.deliver(&format!("input:{id}"), &json!("go")).unwrap());
                child = runs.claim(&id).unwrap().unwrap();
                assert!(child.take(&id, "input", &[format!("input:{id}")]).unwrap().is_some());
                w.hit("child waiting then done");
                Status::Completed(json!("resumed child"))
            }
            4 => {
                child.finish(&Status::Interrupted(WorkflowError::runtime("temporary"))).unwrap();
                assert!(
                    parent.take("p", &format!("not-waiting{i}"), &[format!("waiting:{id}")]).unwrap().is_none(),
                    "interruption is not durable listen suspension"
                );
                expired(&db, &id);
                child = runs.claim(&id).unwrap().unwrap();
                w.hit("interruption without waiting");
                Status::Completed(json!("recovered"))
            }
            _ => unreachable!(),
        };
        // Roll back child finish after the status update but before event
        // insertion. Stored outcome and completion event must move together.
        fault(&db, "workflow_events", "INSERT");
        assert!(child.finish(&expected).is_err());
        unfault(&db);
        assert!(runs.get(&id).unwrap().unwrap().outcome.is_none());
        audit(&db);
        child.finish(&expected).unwrap();
        let done = parent.take("p", &format!("done{i}"), &[format!("done:{id}")]).unwrap().unwrap();
        assert_eq!(runs.child_outcome("p", &key, &id).unwrap(), Some(expected.clone()));
        assert_eq!(
            done["data"]["status"],
            match expected {
                Status::Completed(_) => "completed",
                Status::Faulted(_) => "faulted",
                Status::Cancelled => "cancelled",
                _ => unreachable!(),
            }
        );
        match expected {
            Status::Completed(output) => assert_eq!(done["data"]["output"], output),
            Status::Faulted(error) => assert_eq!(done["data"]["error"], serde_json::to_value(error).unwrap()),
            Status::Cancelled => assert_eq!(done["data"], json!({"status":"cancelled"})),
            _ => unreachable!(),
        }
        assert_eq!(parent.take("p", &format!("done{i}"), &[format!("done:{id}")]).unwrap(), Some(done));
        // A replay may rejoin a finished child, but cannot publish a second
        // lifecycle event after the original one was consumed.
        assert!(runs.start_child(&id, "child", "changed", &json!({}), child_link("p", &key)).unwrap().is_none());
        assert!(
            parent.take("p", &format!("duplicate{i}"), &[format!("done:{id}")]).unwrap().is_none(),
            "child published done twice after replay"
        );
        ids.push(id);
        audit(&db);
        w.hit("joined stored child outcome");
    }
    // Live descendants, including a grandchild, must end with their parent.
    let child = runs.start_child("live", "child", "x", &json!({}), child_link("p", "live")).unwrap().unwrap();
    let grandchild =
        runs.start_child("grandchild", "child", "x", &json!({}), child_link("live", "grand")).unwrap().unwrap();
    if cancel {
        assert!(runs.cancel("p").unwrap());
        w.hit("parent cancellation");
    } else {
        parent.finish(&Status::Completed(json!("all children joined"))).unwrap();
    }
    for id in ["live", "grandchild"] {
        assert_eq!(runs.get(id).unwrap().unwrap().outcome, Some(Status::Cancelled));
    }
    assert!(child.renew().is_err());
    assert!(grandchild.renew().is_err());
    assert_eq!(runs.list_prefix("child", "child").unwrap().len(), ids.len());
    audit(&db);
    stranger.finish(&Status::Completed(json!({}))).unwrap();
    if !cancel {
        w.hit("parent completes and cancels descendants");
    }
}

#[test]
fn children_join_once_replay_and_cascade_without_stranding_parents() {
    let w = Witnesses::new(&[
        "parent and child restart",
        "parent cancellation",
        "child waiting then done",
        "interruption without waiting",
        "joined stored child outcome",
        "parent completes and cancels descendants",
    ]);
    TestRunner::new(Config {
        cases: runs(),
        source_file: Some(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/durable_properties.rs")),
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::WithSource(
            "proptest-regressions",
        ))),
        ..Config::default()
    })
    .run(&(prop::collection::vec((0u8..5, any::<bool>()), 5..9), any::<bool>()), |(ops, cancel)| {
        child_case(ops, cancel, &w);
        Ok(())
    })
    .unwrap();
    w.finish();
}

#[test]
fn listen_recovers_at_every_commit_boundary() {
    let w = Witnesses::new(&[
        "checkpoint crash",
        "after commit crash",
        "suspended",
        "catchable timeout",
        "delivered and completed",
        "inside hold",
    ]);
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap();
    rt.block_on(async {
        for mode in 0..4 {
            for fail in (0..25).chain([usize::MAX]) {
                listen_case(mode, 10, 4, fail, &w).await;
            }
        }
    });
    w.finish();
}

#[test]
fn replaying_a_join_after_consuming_done_does_not_publish_done_again() {
    let db = AgentDb::open(":memory:").unwrap();
    let runs = RunStore::new(db);
    let parent = runs.start("p", "parent", "x", &json!({})).unwrap().unwrap();
    let child = runs.start_child("c", "child", "x", &json!({}), child_link("p", "key")).unwrap().unwrap();
    child.finish(&Status::Completed(json!(7))).unwrap();
    assert!(parent.take("p", "first", &["done:c".into()]).unwrap().is_some());
    assert!(runs.start_child("c", "child", "x", &json!({}), child_link("p", "key")).unwrap().is_none());
    assert!(parent.take("p", "second", &["done:c".into()]).unwrap().is_none());
}

#[test]
fn timing_out_one_same_type_listener_keeps_its_sibling_subscribed() {
    let db = AgentDb::open(":memory:").unwrap();
    let runs = RunStore::new(db.clone());
    let l = runs.start("r", "w", "x", &json!({})).unwrap().unwrap();
    l.register("r", &["x".into()]).unwrap();
    for key in ["timeout", "live"] {
        assert!(l.take("r", key, &["x".into()]).unwrap().is_none());
    }
    l.unregister("r", "timeout", &["x".into()]).unwrap();
    audit(&db);
    assert!(runs.deliver("x", &json!("survivor")).unwrap());
    assert_eq!(l.take("r", "live", &["x".into()]).unwrap().unwrap()["data"], "survivor");
    audit(&db);
}
