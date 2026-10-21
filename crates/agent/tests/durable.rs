use agent::{db::AgentDb, durable::RunStore};
use owf::{
    durable::{Journal, Record},
    run::Status,
};
use serde_json::json;

#[test]
fn leases_fence_stale_workers_and_checkpoints_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("agent.db");
    let db = AgentDb::open(&path).unwrap();
    let store = RunStore::new(db.clone());
    store.admit("r", "w", "original", &json!({"n":1})).unwrap();
    store.admit("r", "w", "changed", &json!({"n":2})).unwrap();
    assert!(store.admit("r", "other", "x", &json!({})).is_err());
    assert_eq!(store.get("r").unwrap().unwrap().definition, "original");
    let old = store.claim("r").unwrap().unwrap();
    assert!(store.claim("r").unwrap().is_none());
    old.renew().unwrap();
    let record = Record::Timer { deadline_ms: 100 };
    old.save("wait", &record).unwrap();
    assert!(old.save("wait", &Record::Timer { deadline_ms: 200 }).is_err());

    db.conn().execute("UPDATE workflow_runs SET lease_until_ms = 0 WHERE id = 'r'", []).unwrap();
    let other_process = RunStore::new(AgentDb::open(&path).unwrap());
    let current = other_process.claim("r").unwrap().unwrap();
    assert!(old.load().is_err());
    assert!(old.save("new", &record).is_err());
    assert!(old.renew().is_err());
    assert!(old.finish(&Status::Completed(json!({}))).is_err());
    assert_eq!(current.load().unwrap().get("wait"), Some(&record));
    current.finish(&Status::Waiting { wake_at_ms: 0 }).unwrap();
    drop((current, old, store, db));

    let db = AgentDb::open(&path).unwrap();
    let store = RunStore::new(db);
    let resumed = store.claim("r").unwrap().unwrap();
    assert_eq!(resumed.load().unwrap().get("wait"), Some(&record));
    resumed.finish(&Status::Completed(json!({"ok":true}))).unwrap();
    assert!(store.due().unwrap().is_empty());
    assert!(store.claim("r").unwrap().is_none());
    assert_eq!(store.get("r").unwrap().unwrap().outcome, Some(Status::Completed(json!({"ok":true}))));
}

// A cause that never clears (a corrupt record, state from an incompatible
// version) backs off and then fails the run, instead of retrying forever.
// A clean release (shutdown) and any real progress do not count.
#[test]
fn interruptions_back_off_and_eventually_fail_the_run() {
    use agent::durable::MAX_INTERRUPTIONS;
    use owf::run::WorkflowError;
    let dir = tempfile::tempdir().unwrap();
    let db = AgentDb::open(dir.path().join("agent.db")).unwrap();
    let store = RunStore::new(db.clone());
    store.admit("r", "w", "x", &json!({})).unwrap();
    let due_now = || db.conn().execute("UPDATE workflow_runs SET wake_at_ms = 0", []).unwrap();
    let state = || -> (String, i64, i64) {
        db.conn()
            .query_row("SELECT state, wake_at_ms, interruptions FROM workflow_runs", [], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })
            .unwrap()
    };
    let interrupted = Status::Interrupted(WorkflowError::runtime("corrupt"));
    let lease = store.claim("r").unwrap().unwrap();
    assert_eq!(lease.finish(&interrupted).unwrap(), interrupted);
    let (_, first_wake, n) = state();
    assert_eq!(n, 1);
    due_now();
    store.claim("r").unwrap().unwrap().release().unwrap();
    assert_eq!(state().2, 1, "a release is not an interruption");
    let lease = store.claim("r").unwrap().unwrap();
    lease.finish(&interrupted).unwrap();
    let (_, second_wake, _) = state();
    assert!(second_wake - first_wake > 1_500, "the delay grows: {first_wake} → {second_wake}");
    due_now();
    store.claim("r").unwrap().unwrap().finish(&Status::Waiting { wake_at_ms: 0 }).unwrap();
    assert_eq!(state().2, 0, "progress resets the count");
    for n in 1..MAX_INTERRUPTIONS {
        store.claim("r").unwrap().unwrap().finish(&interrupted).unwrap();
        let (run_state, _, count) = state();
        assert_eq!((run_state.as_str(), count), ("ready", n));
        due_now();
    }
    let given_up = store.claim("r").unwrap().unwrap().finish(&interrupted).unwrap();
    assert!(matches!(&given_up, Status::Faulted(e) if e.to_string().contains("corrupt")), "{given_up:?}");
    assert_eq!(state().0, "failed");
    assert_eq!(store.get("r").unwrap().unwrap().outcome, Some(given_up));
    assert!(store.due().unwrap().is_empty());
}

// A started run is claimed in the same statement that admits it: the
// recovery worker never sees it as due in between.
#[test]
fn a_started_run_is_never_due_for_another_worker() {
    let dir = tempfile::tempdir().unwrap();
    let store = RunStore::new(AgentDb::open(dir.path().join("agent.db")).unwrap());
    let lease = store.start("r", "w", "x", &json!({})).unwrap().unwrap();
    assert!(store.due().unwrap().is_empty());
    assert!(store.claim("r").unwrap().is_none());
    assert!(store.start("r", "w", "x", &json!({})).unwrap().is_none(), "the second start finds the run");
    assert!(store.start("r", "other", "x", &json!({})).is_err());
    lease.renew().unwrap();
    lease.finish(&Status::Completed(json!({}))).unwrap();
}

#[test]
fn cancellation_is_terminal_and_corrupt_records_are_not_erased() {
    let dir = tempfile::tempdir().unwrap();
    let db = AgentDb::open(dir.path().join("agent.db")).unwrap();
    let store = RunStore::new(db.clone());
    store.admit("r", "w", "x", &json!({})).unwrap();
    let lease = store.claim("r").unwrap().unwrap();
    db.conn().execute("INSERT INTO workflow_records VALUES ('r', 'bad', 'broken json')", []).unwrap();
    assert!(lease.load().is_err());
    assert!(lease.save("bad", &Record::Timer { deadline_ms: 1 }).is_err());
    assert!(store.cancel("r").unwrap());
    assert!(!store.cancel("r").unwrap());
    assert!(lease.save("later", &Record::Timer { deadline_ms: 1 }).is_err());
    assert!(store.claim("r").unwrap().is_none());
    assert_eq!(store.get("r").unwrap().unwrap().outcome, Some(Status::Cancelled));
}

#[cfg(unix)]
mod sigkill {
    use super::*;
    use async_trait::async_trait;
    use owf::{
        durable::{CallState, Delivery},
        run::{self, CallInfo, Effect, Functions, Host, Limits, Silent, SystemClock, WorkflowError},
    };
    use serde_json::Value;
    use std::{
        fs::{File, OpenOptions},
        io::Write,
        os::unix::process::ExitStatusExt,
        path::{Path, PathBuf},
        process::{Child, Command, Stdio},
        sync::Arc,
        time::{Duration, Instant},
    };
    use tokio_util::sync::CancellationToken;

    fn stop_here(dir: &Path) -> ! {
        let mut ready = File::create(dir.join("ready")).unwrap();
        ready.write_all(b"ready").unwrap();
        ready.sync_all().unwrap();
        loop {
            std::thread::park();
        }
    }

    struct Boundary<'a> {
        lease: &'a agent::durable::Lease,
        db: &'a AgentDb,
        dir: &'a Path,
        phase: &'a str,
    }
    impl Journal for Boundary<'_> {
        fn load(&self) -> anyhow::Result<std::collections::BTreeMap<String, Record>> {
            self.lease.load()
        }
        fn save(&self, key: &str, record: &Record) -> anyhow::Result<()> {
            let intent = matches!(record, Record::Call { arguments, state: CallState::Pending, .. } if arguments["label"] == "target");
            if intent && self.phase == "before_intent" {
                stop_here(self.dir);
            }
            if intent && self.phase == "uncommitted_intent" {
                // Stage a real SQLite checkpoint without committing it. The
                // process dies holding this transaction; Rust never drops it.
                let mut conn = self.db.conn();
                let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                tx.execute(
                    "INSERT INTO workflow_records (run_id, key, record) VALUES ('kill-run', ?1, ?2)",
                    rusqlite::params![key, serde_json::to_string(record)?],
                )?;
                stop_here(self.dir);
            }
            self.lease.save(key, record)?;
            let hit = match self.phase {
                "after_intent" => intent,
                "after_response" => {
                    matches!(record, Record::Call { arguments, state: CallState::Done(_), .. } if arguments["label"] == "target")
                }
                "after_task" => key.ends_with("/send#task") && matches!(record, Record::Task { result: Some(_), .. }),
                "after_finished" => matches!(record, Record::Finished { .. }),
                _ => false,
            };
            if hit {
                stop_here(self.dir);
            }
            Ok(())
        }
    }

    struct External {
        dir: PathBuf,
        phase: String,
    }
    #[async_trait]
    impl Functions for External {
        fn effect(&self, _: &str) -> Option<Effect> {
            Some(Effect::Write)
        }
        async fn call(&self, _: &str, args: Value, info: &CallInfo<'_>) -> Result<Value, WorkflowError> {
            // This represents a remote effect: it commits independently of
            // the agent's SQLite transaction and survives the worker's death.
            let mut ledger = OpenOptions::new().create(true).append(true).open(self.dir.join("effects.jsonl")).unwrap();
            writeln!(ledger, "{}", json!({"reference":info.reference,"args":args})).unwrap();
            ledger.sync_all().unwrap();
            if args["label"] == "target" && self.phase == "after_effect" {
                stop_here(&self.dir);
            }
            Ok(args)
        }
    }

    fn definition(delivery: Delivery) -> String {
        let policy = serde_json::to_value(delivery).unwrap();
        format!(
            r#"document: {{ dsl: '1.0.4', namespace: test, name: kill, version: '1.0.0' }}
do:
  - prime: {{ call: effect, with: {{ label: prime, n: 10 }}, export: {{ as: '${{ . }}' }} }}
  - pause: {{ wait: PT0.02S }}
  - send: {{ call: effect, with: {{ label: target, n: '${{ $context.n + 1 }}' }}, metadata: {{ delivery: {policy} }}, export: {{ as: '${{ . }}' }} }}
  - last: {{ call: effect, with: {{ label: last, n: '${{ $context.n + 1 }}' }}, export: {{ as: '${{ . }}' }} }}
  - result: {{ set: '${{ $context }}' }}
"#
        )
    }

    /// The current test executable doubles as an isolated worker; no hooks
    /// or kill switches are added to production code.
    #[test]
    #[ignore = "subprocess helper; invoked by recovery_survives_sigkill"]
    fn worker() {
        let Some(dir) = std::env::var_os("DURABLE_SIGKILL_DIR").map(PathBuf::from) else { return };
        let phase = std::env::var("DURABLE_SIGKILL_PHASE").unwrap_or_default();
        let db = AgentDb::open(dir.join("agent.db")).unwrap();
        let store = RunStore::new(db.clone());
        let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
        rt.block_on(async {
            loop {
                let saved = store.get("kill-run").unwrap().unwrap();
                if let Some(status) = saved.outcome {
                    std::fs::write(dir.join("result.json"), serde_json::to_vec(&status).unwrap()).unwrap();
                    return;
                }
                let Some(lease) = store.claim("kill-run").unwrap() else {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    continue;
                };
                let wf = owf::model::parse_yaml(&saved.definition).unwrap();
                let host = Host {
                    functions: Arc::new(External { dir: dir.clone(), phase: phase.clone() }),
                    observer: Arc::new(Silent),
                    clock: Arc::new(SystemClock),
                    limits: Limits::default(),
                    runtime: json!({"name":"sigkill-test"}),
                };
                let journal = Boundary { lease: &lease, db: &db, dir: &dir, phase: &phase };
                let outcome =
                    run::resume(&host, &wf, &saved.id, saved.input, &CancellationToken::new(), &journal).await;
                assert!(!matches!(outcome.status, Status::Interrupted(_)), "{outcome:?}");
                lease.finish(&outcome.status).unwrap();
                if matches!(outcome.status, Status::Waiting { .. }) && phase == "waiting" {
                    stop_here(&dir);
                }
            }
        });
    }

    struct Worker(Child);
    impl Drop for Worker {
        fn drop(&mut self) {
            if !matches!(self.0.try_wait(), Ok(Some(_))) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
    }
    fn spawn(dir: &Path, phase: &str) -> Worker {
        let log = File::create(dir.join(format!("child-{phase}.log"))).unwrap();
        Worker(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "sigkill::worker", "--ignored", "--nocapture", "--test-threads=1"])
                .env("DURABLE_SIGKILL_DIR", dir)
                .env("DURABLE_SIGKILL_PHASE", phase)
                .stdin(Stdio::null())
                .stdout(log.try_clone().unwrap())
                .stderr(log)
                .spawn()
                .unwrap(),
        )
    }
    fn logs(dir: &Path, phase: &str) -> String {
        std::fs::read_to_string(dir.join(format!("child-{phase}.log"))).unwrap_or_default()
    }
    fn wait_ready(worker: &mut Worker, dir: &Path, phase: &str) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !dir.join("ready").exists() {
            assert!(worker.0.try_wait().unwrap().is_none(), "worker exited before {phase}: {}", logs(dir, phase));
            assert!(Instant::now() < deadline, "worker never reached {phase}: {}", logs(dir, phase));
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    fn recover(dir: &Path) -> Status {
        let mut worker = spawn(dir, "resume");
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = worker.0.try_wait().unwrap() {
                assert!(status.success(), "recovery failed: {}", logs(dir, "resume"));
                return serde_json::from_slice(&std::fs::read(dir.join("result.json")).unwrap()).unwrap();
            }
            assert!(Instant::now() < deadline, "recovery hung: {}", logs(dir, "resume"));
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn recovery_survives_sigkill() {
        for policy in [Delivery::AtLeastOnce, Delivery::AtMostOnce] {
            for phase in [
                "before_intent",
                "uncommitted_intent",
                "after_intent",
                "after_effect",
                "after_response",
                "after_task",
                "after_finished",
                "waiting",
            ] {
                let dir = tempfile::tempdir().unwrap();
                {
                    let db = AgentDb::open(dir.path().join("agent.db")).unwrap();
                    RunStore::new(db).admit("kill-run", "kill", &definition(policy), &json!({})).unwrap();
                }
                let mut worker = spawn(dir.path(), phase);
                wait_ready(&mut worker, dir.path(), phase);
                worker.0.kill().unwrap();
                assert_eq!(worker.0.wait().unwrap().signal(), Some(9), "expected SIGKILL at {phase}");

                let db = AgentDb::open(dir.path().join("agent.db")).unwrap();
                assert_eq!(db.conn().query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0)).unwrap(), "ok");
                if phase == "uncommitted_intent" {
                    assert_eq!(db.conn().query_row(
                        "SELECT COUNT(*) FROM workflow_records WHERE run_id = 'kill-run' AND key = '/do/2/send#call'", [],
                        |r| r.get::<_, i64>(0)).unwrap(), 0, "uncommitted intent survived SIGKILL");
                }
                if phase != "waiting" {
                    assert!(
                        RunStore::new(db.clone()).claim("kill-run").unwrap().is_none(),
                        "kill must not release the lease"
                    );
                    // Advance lease expiry, not workflow state; avoid waiting
                    // 30 real seconds for every crash boundary in this test.
                    db.conn().execute("UPDATE workflow_runs SET lease_until_ms = 0 WHERE id = 'kill-run'", []).unwrap();
                }
                let result = recover(dir.path());
                let unknown = policy == Delivery::AtMostOnce && matches!(phase, "after_intent" | "after_effect");
                if unknown {
                    assert!(
                        matches!(result, Status::Faulted(ref error) if error.is("outcome_unknown")),
                        "{policy:?} at {phase}: {result:?}"
                    );
                } else {
                    assert_eq!(result, Status::Completed(json!({"label":"last","n":12})), "{policy:?} at {phase}");
                }
                let ledger = std::fs::read_to_string(dir.path().join("effects.jsonl")).unwrap();
                let calls: Vec<Value> = ledger.lines().map(|line| serde_json::from_str(line).unwrap()).collect();
                let count = |label: &str| calls.iter().filter(|call| call["args"]["label"] == label).count();
                assert_eq!(count("prime"), 1, "a saved effect was repeated at {phase}");
                let target = if unknown && phase == "after_intent" {
                    0
                } else if policy == Delivery::AtLeastOnce && phase == "after_effect" {
                    2
                } else {
                    1
                };
                assert_eq!(count("target"), target, "{policy:?} at {phase}: {calls:?}");
                assert_eq!(count("last"), usize::from(!unknown));
                assert_eq!(recover(dir.path()), result, "terminal state changed after another restart");
                assert_eq!(std::fs::read_to_string(dir.path().join("effects.jsonl")).unwrap(), ledger);
                assert!(RunStore::new(db).due().unwrap().is_empty());
            }
        }
    }
}

// The records of a long-finished run are swept; its row stays, so it still
// reports its outcome and is never resumed. Unfinished runs and recently
// finished ones keep theirs.
#[test]
fn a_swept_run_keeps_its_outcome_and_is_never_resumed() {
    let dir = tempfile::tempdir().unwrap();
    let db = AgentDb::open(dir.path().join("agent.db")).unwrap();
    let store = RunStore::new(db.clone());
    let record = Record::Timer { deadline_ms: 1 };
    for id in ["old", "recent", "waiting"] {
        let lease = store.start(id, "w", "x", &json!({})).unwrap().unwrap();
        lease.save("k", &record).unwrap();
        lease
            .finish(&if id == "waiting" {
                Status::Waiting { wake_at_ms: i64::MAX }
            } else {
                Status::Completed(json!(id))
            })
            .unwrap();
    }
    db.conn().execute("UPDATE workflow_runs SET ended_ms = 0 WHERE id = 'old'", []).unwrap();
    let kept = agent::durable::RECORDS_KEPT_MS;
    assert_eq!(store.sweep(kept).unwrap(), 1);
    assert_eq!(store.sweep(kept).unwrap(), 0);
    let records = |id: &str| -> i64 {
        db.conn().query_row("SELECT count(*) FROM workflow_records WHERE run_id = ?1", [id], |r| r.get(0)).unwrap()
    };
    assert_eq!((records("old"), records("recent"), records("waiting")), (0, 1, 1));
    assert_eq!(store.get("old").unwrap().unwrap().outcome, Some(Status::Completed(json!("old"))));
    assert!(store.claim("old").unwrap().is_none());
    assert!(!store.due().unwrap().contains(&"old".to_owned()));
    // Admitting it again (a redelivered signal) does not reopen it.
    assert!(store.start("old", "w", "x", &json!({})).unwrap().is_none());
    assert!(store.cancel("recent").is_ok_and(|changed| !changed));
}

#[test]
fn history_reads_do_not_claim_or_modify_runs_and_prefixes_are_literal() {
    let dir = tempfile::tempdir().unwrap();
    let db = AgentDb::open(dir.path().join("agent.db")).unwrap();
    let runs = RunStore::new(db.clone());
    let lease = runs.start("a_%/child", "w", "unused", &json!({})).unwrap().unwrap();
    let record = Record::Timer { deadline_ms: 123 };
    lease.save("timer", &record).unwrap();
    for (id, name) in [("aXX/child", "w"), ("a_%/other", "other"), ("a_%/ready", "w")] {
        runs.admit(id, name, "unused", &json!({})).unwrap();
    }
    assert_eq!(runs.list_prefix("w", "a_%/").unwrap(), ["a_%/child", "a_%/ready"]);
    assert_eq!(runs.records("a_%/child").unwrap().as_slice(), std::slice::from_ref(&record));
    assert_eq!(runs.get("a_%/child").unwrap().unwrap().state, "running");
    assert_eq!(runs.get("a_%/ready").unwrap().unwrap().state, "ready");
    assert_eq!(runs.due().unwrap(), ["aXX/child", "a_%/other", "a_%/ready"]);
    assert!(runs.records("missing").unwrap().is_empty());
    lease.finish(&Status::Completed(json!("done"))).unwrap();
    assert_eq!(runs.records("a_%/child").unwrap(), [record]);
    assert_eq!(runs.get("a_%/child").unwrap().unwrap().state, "completed");
    db.conn().execute("UPDATE workflow_records SET record = 'broken'", []).unwrap();
    assert!(runs.records("a_%/child").is_err(), "corrupt evidence is an error, not an empty history");
}

#[test]
fn events_survive_restart_consume_atomically_and_do_not_reopen_ended_runs() {
    use owf::run::Events;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.db");
    let db = AgentDb::open(&path).unwrap();
    let store = RunStore::new(db.clone());
    let lease = store.start("r", "w", "x", &json!({})).unwrap().unwrap();
    lease.register("r", &["yes".into(), "no".into()]).unwrap();
    lease.register("r", &["yes".into(), "no".into()]).unwrap();
    assert!(!store.deliver("foreign", &json!({})).unwrap());
    assert!(store.deliver("yes", &json!({"original":true})).unwrap());
    assert!(!store.deliver("yes", &json!({"original":false})).unwrap());
    // Delivery during the active execution wins over its subsequent suspension.
    lease.finish(&Status::Waiting { wake_at_ms: i64::MAX }).unwrap();
    assert_eq!(store.due().unwrap(), ["r"]);
    drop((lease, store, db));
    let db = AgentDb::open(&path).unwrap();
    let store = RunStore::new(db.clone());
    let lease = store.claim("r").unwrap().unwrap();
    let event = lease.take("r", "listen#event", &["yes".into(), "no".into()]).unwrap().unwrap();
    assert_eq!(event["data"], json!({"original":true}));
    assert_eq!(lease.take("r", "listen#event", &["yes".into()]).unwrap(), Some(event.clone()));
    assert_eq!(lease.load().unwrap()["listen#event"], Record::Event { event });
    assert!(!store.deliver("yes", &json!({})).unwrap());
    assert!(!store.deliver("no", &json!({})).unwrap());
    lease.finish(&Status::Completed(json!({}))).unwrap();
    assert!(!store.deliver("yes", &json!({})).unwrap());
    store.sweep(-1).unwrap();
    assert_eq!(db.conn().query_row("SELECT count(*) FROM workflow_events", [], |r| r.get::<_,i64>(0)).unwrap(), 0);
}
