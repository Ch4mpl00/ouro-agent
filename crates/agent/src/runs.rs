// Runs on the ouro engine (docs/designs/03-runs-on-the-engine.md): every
// workflow run of the agent (signal, loops, workers, guard rounds, recovery)
// is a run of the engine's store inside agent.db (`ouro_*` tables, its own
// connection). The agent keeps what the engine does not know.
// Sections: 1. the runtime (store, capacity, budget); 2. agent tables
// (evidence, run anchors, the MCP tool snapshot); 3. host data, the trust
// boundary; 4. live trace scopes; 5. the lifecycle observer; 6. the run tree
// and its quiescence; 7. built-ins at boot; 8. reconciliation and sweep;
// 9. waiting, the background and the reconciler; 10. starting a name the
// agent may have overridden (design 04); 11. what a handler reads of its run
// tree (design 05, decision 3).
use crate::{
    db::AgentDb,
    tracing::{Scope, Span, SpanEnd, Trace, TraceAnchor, TraceStart},
};
pub use ouro_runtime::durable::control::Runs;
use ouro_runtime::durable::{RunState, SavedRun};
pub use ouro_runtime::ows::run::Status;
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

// ── 1. the runtime ──────────────────────────────────────────────────────────

/// Drives at once: two run trees (a resumed one and the next signal's), each
/// a primary loop with its guard round and up to 16 workers with theirs
/// (1 + 1 + 16 + 16 = 34 per tree). The signal itself holds none while it
/// waits (it suspends on its primary). docs/operations.md, Capacity.
pub const DRIVES: usize = 68;
/// Handlers running at once: every drive may wait in one call while another
/// of its tree's calls runs.
pub const FUNCTION_SLOTS: usize = 2 * DRIVES;
/// The run tree's jump budget, a backstop: `max_iterations` is the binding
/// limit of every loop. One round of agent-loop takes two jumps (`route` and
/// `next`) plus one per guarded call in its guard round; the dispatcher takes
/// at most three. With the primary's 100 iterations and 16 workers of 50,
/// each with up to 16 guarded calls a round, that is
/// 100 × 18 + 16 × 50 × 18 + 3 = 16 203; this leaves a margin over it
/// (docs/operations.md, Jump budget). The recovery report runs outside the
/// signal's tree, so an exhausted budget never stops it.
pub const MAX_JUMPS: u64 = 20_000;
/// Task executions per run: a loop round enters about seven tasks per call.
pub const MAX_TASKS: usize = 50_000;

/// The runtime's configuration: capacity and budget above. The built-ins
/// opt out of the breaker themselves (`metadata.ouro.breaker: off`).
pub fn config() -> ouro_runtime::Config {
    ouro_runtime::Config {
        max_concurrency: DRIVES,
        max_function_concurrency: FUNCTION_SLOTS,
        limits: ouro_runtime::ows::run::Limits { max_jumps: MAX_JUMPS, max_tasks: MAX_TASKS, ..Default::default() },
        metadata: json!({"name":"agent-supervisor","version":env!("CARGO_PKG_VERSION")}),
        // A child starts at the worker's next tick: a signal is a chain of
        // three or four starts (signal, primary, guard, worker).
        poll_interval: std::time::Duration::from_millis(50),
        ..Default::default()
    }
}

/// The engine's store beside agent.db (`engine.db` in its directory),
/// `ouro_*` tables. Not inside agent.db, as design 01 decision 3 had it: the
/// engine's adapter takes SQLite's write lock for every operation, reads
/// included, back to back, and a write of the agent's on another connection
/// to the same file starved behind it for seconds, past its busy timeout
/// (docs/designs/LOG.md, 2026-10-26, step 2b). One directory, one volume, two
/// files to back up.
pub fn engine_store_path(agent_db: &std::path::Path) -> std::path::PathBuf {
    agent_db.with_file_name("engine.db")
}

/// Opens (and migrates) the engine's store for agent.db at `agent_db`.
pub fn open_store(agent_db: &std::path::Path) -> anyhow::Result<Arc<dyn ouro_runtime::durable::RunStore>> {
    Ok(Arc::new(ouro_runtime::durable::sqlite::open(engine_store_path(agent_db))?))
}

/// A read-only view of the engine's store for agent.db at `agent_db`
/// (`EngineDeps::dry_store`), opened after `open_store` migrated it: the
/// definitions a dry run reads (design 05, decision 1).
pub fn open_store_view(agent_db: &std::path::Path) -> anyhow::Result<Arc<dyn ouro_runtime::durable::RunStore>> {
    Ok(Arc::new(ouro_runtime::durable::sqlite::open_read_only(engine_store_path(agent_db))?))
}

/// A store in memory, for tests and tools that keep nothing.
pub fn memory_store() -> Arc<dyn ouro_runtime::durable::RunStore> {
    Arc::new(ouro_runtime::durable::memory::memory())
}

/// A store in memory and a read-only view of it (`EngineDeps::store`,
/// `dry_store`), for tests and tools that keep nothing.
pub fn memory_store_with_view() -> (Arc<dyn ouro_runtime::durable::RunStore>, Arc<dyn ouro_runtime::durable::RunStore>)
{
    use ouro_runtime::durable::{TransactionalStore, memory::MemoryBackend};
    let backend = Arc::new(MemoryBackend::default());
    let view = Arc::new(backend.read_only_view());
    (Arc::new(TransactionalStore::new(backend)), Arc::new(TransactionalStore::new(view)))
}

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

// ── 2. agent tables ─────────────────────────────────────────────────────────

/// The content address of a piece of evidence.
pub fn digest(body: &str) -> String {
    hex::encode(Sha256::digest(body.as_bytes()))
}

/// Where a run's observations go: its trace, the span they nest under, its
/// session. Written before the engine admits the run; `recorded` once its
/// end (and, for a root, its final output) reached the trace.
#[derive(Clone, Debug, PartialEq)]
pub struct RunTrace {
    pub run_id: String,
    pub trace_id: String,
    pub session_id: Option<String>,
    pub parent_span_id: Option<String>,
    pub recorded: bool,
    pub created_ms: i64,
}

impl RunTrace {
    pub fn anchor(&self) -> Option<TraceAnchor> {
        Some(TraceAnchor {
            trace_id: self.trace_id.clone(),
            parent_span_id: self.parent_span_id.clone()?,
            session_id: self.session_id.clone(),
        })
    }
}

impl AgentDb {
    /// Runs `f` (a write) on the blocking pool. A write may wait for
    /// SQLite's lock (another process, a long transaction): it must never
    /// block the task that polls the engine's runs (the worker drives them all
    /// in one task, handlers included).
    pub async fn write<T: Send + 'static>(
        &self,
        f: impl FnOnce(&AgentDb) -> anyhow::Result<T> + Send + 'static,
    ) -> anyhow::Result<T> {
        let db = self.clone();
        tokio::task::spawn_blocking(move || f(&db)).await?
    }

    /// Stores `body` (immutable, content-addressed) and links it to `run`,
    /// before the engine admits the run (design 03, R2-H1). Returns its digest.
    pub fn put_evidence(&self, run: &str, body: &str) -> anyhow::Result<String> {
        let digest = digest(body);
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT INTO evidence (digest, body, created_ms) VALUES (?1, ?2, ?3) ON CONFLICT(digest) DO NOTHING",
            params![digest, body, now_ms()],
        )?;
        tx.execute(
            "INSERT INTO run_evidence (run_id, digest) VALUES (?1, ?2) ON CONFLICT DO NOTHING",
            params![run, digest],
        )?;
        tx.commit()?;
        Ok(digest)
    }

    /// Links evidence already stored to another run.
    pub fn link_evidence(&self, run: &str, digest: &str) -> anyhow::Result<()> {
        self.conn().execute(
            "INSERT INTO run_evidence (run_id, digest) SELECT ?1, digest FROM evidence WHERE digest = ?2
             ON CONFLICT DO NOTHING",
            params![run, digest],
        )?;
        Ok(())
    }

    pub fn evidence(&self, digest: &str) -> anyhow::Result<Option<String>> {
        Ok(self.read().query_row("SELECT body FROM evidence WHERE digest = ?1", [digest], |r| r.get(0)).optional()?)
    }

    /// Insert-or-ignore: a re-dispatched starter keeps the first anchor.
    pub fn insert_run_trace(&self, row: &RunTrace) -> anyhow::Result<()> {
        self.conn().execute(
            "INSERT INTO run_trace (run_id, trace_id, session_id, parent_span_id, recorded, created_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6) ON CONFLICT(run_id) DO NOTHING",
            params![row.run_id, row.trace_id, row.session_id, row.parent_span_id, row.recorded, row.created_ms],
        )?;
        Ok(())
    }

    pub fn run_trace(&self, run: &str) -> anyhow::Result<Option<RunTrace>> {
        Ok(self
            .read()
            .query_row(
                "SELECT run_id, trace_id, session_id, parent_span_id, recorded, created_ms FROM run_trace WHERE run_id = ?1",
                [run],
                run_trace_row,
            )
            .optional()?)
    }

    pub fn open_run_traces(&self, limit: usize) -> anyhow::Result<Vec<RunTrace>> {
        let conn = self.read();
        let mut statement = conn.prepare(
            "SELECT run_id, trace_id, session_id, parent_span_id, recorded, created_ms FROM run_trace
             WHERE recorded = 0 ORDER BY created_ms LIMIT ?1",
        )?;
        Ok(statement.query_map([i64::try_from(limit)?], run_trace_row)?.collect::<rusqlite::Result<_>>()?)
    }

    pub fn trace_runs(&self, trace: &str) -> anyhow::Result<Vec<RunTrace>> {
        let conn = self.read();
        let mut statement = conn.prepare(
            "SELECT run_id, trace_id, session_id, parent_span_id, recorded, created_ms FROM run_trace WHERE trace_id = ?1",
        )?;
        Ok(statement.query_map([trace], run_trace_row)?.collect::<rusqlite::Result<_>>()?)
    }

    pub fn mark_recorded(&self, run: &str) -> anyhow::Result<()> {
        self.conn().execute("UPDATE run_trace SET recorded = 1 WHERE run_id = ?1", [run])?;
        Ok(())
    }

    /// Forgets the anchor of a run the engine refused to admit (nothing was
    /// stored under its id), so the next start of that id anchors afresh.
    pub fn forget_run_trace(&self, run: &str) -> anyhow::Result<()> {
        self.conn().execute("DELETE FROM run_trace WHERE run_id = ?1", [run])?;
        Ok(())
    }

    /// Adds MCP's tools to the snapshot (never shrunk): a tool that goes away
    /// stays registered and answers `not_executed`, so a run that calls it
    /// never faults on a missing function at resume (design 01, decision 16).
    pub fn snapshot_tools(&self, tools: &[(String, bool)]) -> anyhow::Result<()> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        for (name, read_only) in tools {
            tx.execute(
                "INSERT INTO tool_snapshot (name, read_only, seen_ms) VALUES (?1, ?2, ?3)
                 ON CONFLICT(name) DO UPDATE SET read_only = excluded.read_only, seen_ms = excluded.seen_ms",
                params![name, read_only, now_ms()],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn tool_snapshot(&self) -> anyhow::Result<Vec<(String, bool)>> {
        let conn = self.read();
        let mut statement = conn.prepare("SELECT name, read_only FROM tool_snapshot ORDER BY name")?;
        Ok(statement.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?.collect::<rusqlite::Result<_>>()?)
    }
}

fn run_trace_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<RunTrace> {
    Ok(RunTrace {
        run_id: r.get(0)?,
        trace_id: r.get(1)?,
        session_id: r.get(2)?,
        parent_span_id: r.get(3)?,
        recorded: r.get(4)?,
        created_ms: r.get(5)?,
    })
}

// ── 3. host data ────────────────────────────────────────────────────────────
// What the host vouches for, stored with the admission and read only by the
// run's functions (`FunctionContext::host`): YAML can neither read nor set it,
// and a run a workflow starts itself has none. Small by construction; large
// evidence is named by digest (§2).

pub const SIGNAL: &str = "signal";
pub const PRIMARY: &str = "primary";
pub const WORKER: &str = "worker";
pub const RECOVERY: &str = "recovery";
/// A loop the host runs itself (`AgentLoop::send`): tests, evals, tools.
pub const LOOP: &str = "loop";
pub const GUARD: &str = "guard";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SignalEvidence {
    pub content: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SignalHost {
    pub kind: String,
    pub signal_id: i64,
    pub source: String,
    pub created_ms: i64,
    pub evidence: SignalEvidence,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct LoopEvidence {
    pub skills: Option<String>,
    pub earlier: Option<String>,
    pub preselect: Option<String>,
    pub system: Option<String>,
}

/// A loop's parameters (design 03, R-M10, R2-M4): the model's preset, where
/// confirmations go, the conversation it serves, its skills and limits, its
/// session (`session`: the working memory's, `session_id`: the trace's), the
/// user's clock (`env`) and the guard's evidence.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LoopHost {
    pub kind: String,
    #[serde(rename = "loop")]
    pub workflow: String,
    pub preset: String,
    pub confirm: Option<crate::agent::ConfirmTarget>,
    pub conversation: Option<crate::agent::ConfirmTarget>,
    pub session_id: Option<String>,
    pub session: String,
    pub env: Value,
    pub skills: Vec<String>,
    pub allow_skill_writes: bool,
    pub max_iterations: usize,
    pub max_concurrent_tools: usize,
    pub tool_whitelist: Option<Vec<String>>,
    pub include_engine_skills: bool,
    pub parent_id: Option<String>,
    pub root_request_digest: Option<String>,
    pub guard_request: Option<String>,
    pub signal_id: Option<i64>,
    pub evidence: LoopEvidence,
    /// The version set aside for this run, when it runs a built-in in its
    /// place (§10): its record, and the line its instructions carry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback: Option<Fallback>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct GuardHost {
    pub kind: String,
    pub parent_loop: String,
    pub round: u64,
    pub confirm: Option<crate::agent::ConfirmTarget>,
    /// The guard context (`guard::Context`) the parent loop had for the round.
    pub evidence: String,
}

/// The kind a run's host data names, or None for a run without host data.
pub fn host_kind(host: Option<&Value>) -> Option<&str> {
    host?.get("kind")?.as_str()
}

pub fn loop_kind(kind: &str) -> bool {
    matches!(kind, PRIMARY | WORKER | RECOVERY | LOOP)
}

// ── 4. live trace scopes ────────────────────────────────────────────────────
// Where a run's handlers trace now. A run's first scope is the one its
// starter opened (the signal's trace, the span of the call that started a
// child); after the run suspends, or in another process, a continuation
// segment of its trace nests under that span (`run_trace`), as resumed runs
// always did. Handlers write their own generation and tool spans with full
// payloads; the engine's observer events only open and close these scopes.

enum Live {
    // Opened and ended by its creator (the supervisor's signal trace).
    Borrowed(Scope),
    // The span a starter opened for the child; ended when the run stops.
    Span(Arc<dyn Span>),
    // The same, inside a span of its own starter's (a route's, a saved
    // workflow's), which ends with it.
    Wrapped(Arc<dyn Span>, Arc<dyn Span>),
    // A continuation segment opened here; ended when the run stops.
    Segment(Arc<dyn Trace>),
}

#[derive(Default)]
pub struct Scopes {
    live: Mutex<HashMap<String, Live>>,
}

impl Scopes {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Live>> {
        self.live.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The creator's scope of `run`, which the creator ends itself.
    pub fn borrow(&self, run: &str, scope: Scope) {
        self.lock().insert(run.into(), Live::Borrowed(scope));
    }

    /// A span opened for `run` by its starter, ended when the run stops.
    pub fn own(&self, run: &str, span: Arc<dyn Span>) {
        self.lock().entry(run.into()).or_insert(Live::Span(span));
    }

    /// `own`, with `outer` (the span `span` nests in) ended right after it.
    pub fn own_wrapped(&self, run: &str, span: Arc<dyn Span>, outer: Arc<dyn Span>) {
        self.lock().entry(run.into()).or_insert(Live::Wrapped(span, outer));
    }

    pub fn release(&self, run: &str) {
        self.lock().remove(run);
    }

    /// `run`'s scope now: its live one, or a continuation segment of its
    /// trace opened from its anchor (`None`: no anchor; the caller traces
    /// nowhere).
    pub fn scope(&self, run: &str, db: &AgentDb, tracer: &dyn crate::tracing::Tracer, name: &str) -> Option<Scope> {
        if let Some(scope) = self.current(run) {
            return Some(scope);
        }
        let anchor = db.run_trace(run).ok().flatten()?.anchor()?;
        let trace = tracer.trace(TraceStart {
            session_id: anchor.session_id.clone(),
            continuation: Some(anchor),
            id: uuid::Uuid::new_v4().to_string(),
            name: format!("resume:{name}"),
            metadata: Some(json!({"run": run}).as_object().cloned().unwrap_or_default()),
            ..Default::default()
        });
        let mut live = self.lock();
        // Another handler of the run may have opened one meanwhile: this
        // one is dropped unwritten (nothing traced into it).
        if let Some(scope) = current(&live, run) {
            return Some(scope);
        }
        live.insert(run.into(), Live::Segment(trace.clone()));
        Some(trace)
    }

    pub fn current(&self, run: &str) -> Option<Scope> {
        current(&self.lock(), run)
    }

    /// Whether the scope of `run` is one this registry ends (not borrowed).
    pub fn owns(&self, run: &str) -> bool {
        matches!(self.lock().get(run), Some(Live::Span(_) | Live::Wrapped(..) | Live::Segment(_)))
    }

    /// Ends the scope the run's execution traced into: on a stop (suspended,
    /// interrupted) the next execution opens a continuation; on its end the
    /// span or segment gets the outcome.
    pub fn stop(&self, run: &str, end: Option<SpanEnd>) {
        let removed = {
            let mut live = self.lock();
            match live.get(run) {
                Some(Live::Borrowed(_)) | None => None,
                Some(_) => live.remove(run),
            }
        };
        match removed {
            Some(Live::Span(span)) => span.end(end.unwrap_or_default()),
            Some(Live::Wrapped(span, outer)) => {
                let end = end.unwrap_or_default();
                span.end(end.clone());
                outer.end(end);
            }
            Some(Live::Segment(trace)) => {
                if let Some(end) = end
                    && let Some(output) = end.output
                {
                    trace.update(crate::tracing::ContextUpdate {
                        output: Some(output),
                        metadata: end
                            .level
                            .filter(|l| *l == crate::tracing::Level::Error)
                            .map(|_| json!({"error": true}).as_object().cloned().unwrap_or_default()),
                        ..Default::default()
                    });
                }
                trace.end()
            }
            _ => {}
        }
    }

    /// Ends every scope this registry ends (not the borrowed ones) at a
    /// shutdown, so what the runs traced is written: a run's span ends as
    /// stopped (the run resumes at the next start, in a continuation).
    pub fn stop_all(&self) {
        let stopped: Vec<Live> = {
            let mut live = self.lock();
            let owned: Vec<String> =
                live.iter().filter(|(_, l)| !matches!(l, Live::Borrowed(_))).map(|(run, _)| run.clone()).collect();
            owned.into_iter().filter_map(|run| live.remove(&run)).collect()
        };
        for scope in stopped {
            match scope {
                Live::Span(span) => {
                    span.end(SpanEnd::error("stopped by a shutdown; the run resumes at the next start"))
                }
                Live::Wrapped(span, outer) => {
                    let stopped = SpanEnd::error("stopped by a shutdown; the run resumes at the next start");
                    span.end(stopped.clone());
                    outer.end(stopped);
                }
                Live::Segment(trace) => trace.end(),
                Live::Borrowed(_) => {}
            }
        }
    }

    pub fn open(&self) -> Vec<String> {
        let mut runs: Vec<String> = self.lock().keys().cloned().collect();
        runs.sort();
        runs
    }
}

fn current(live: &HashMap<String, Live>, run: &str) -> Option<Scope> {
    Some(match live.get(run)? {
        Live::Borrowed(scope) => scope.clone(),
        Live::Span(span) | Live::Wrapped(span, _) => span.clone(),
        Live::Segment(trace) => trace.clone(),
    })
}

/// How a run's outcome ends its span.
pub fn span_end(status: &Status) -> SpanEnd {
    match status {
        Status::Completed(output) => SpanEnd::output(output.clone()),
        Status::Faulted(error) => SpanEnd::error(error.to_string()).with_output(json!({"error": error.to_string()})),
        // The agent cancels a run only when its work is cut off (a closed
        // loop, a parent's end): an aborted observation, as before.
        Status::Cancelled => {
            SpanEnd::error(crate::errors::aborted().to_string()).with_output(json!({"cancelled": true}))
        }
        Status::Waiting { wake_at_ms } => SpanEnd::output(json!({"waiting_until_ms": wake_at_ms})),
        Status::Interrupted(error) => SpanEnd::error(error.to_string()),
    }
}

// ── 5. the lifecycle observer ───────────────────────────────────────────────
// The engine's events are at most once and only lifecycle: an execution
// stopping or a run ending closes its live scope and its live loop. A dropped
// event costs a span's end time; durable derived state comes from the store
// (§8). The observer only enqueues (`ouro_runtime::buffered`); a task drains it.

pub struct Lifecycle {
    inner: Arc<ouro_runtime::Buffered>,
    sent: AtomicU64,
}

impl Lifecycle {
    pub fn new(capacity: usize) -> (Arc<Lifecycle>, ouro_runtime::BufferedEvents) {
        let (inner, events) = ouro_runtime::buffered(capacity);
        (Arc::new(Lifecycle { inner, sent: AtomicU64::new(0) }), events)
    }

    /// Events enqueued or dropped so far.
    pub fn sent(&self) -> u64 {
        self.sent.load(Ordering::SeqCst)
    }

    pub fn dropped(&self) -> u64 {
        self.inner.dropped()
    }
}

impl ouro_runtime::Observer for Lifecycle {
    fn observe(&self, event: &ouro_runtime::Event) {
        use ouro_runtime::EventKind;
        if matches!(
            event.kind,
            EventKind::RunSuspended { .. } | EventKind::RunInterrupted { .. } | EventKind::RunEnded { .. }
        ) {
            self.sent.fetch_add(1, Ordering::SeqCst);
            ouro_runtime::Observer::observe(&*self.inner, event);
        }
    }
}

/// What the drain did, for those waiting until it caught up (`flushed`).
#[derive(Default)]
pub struct Drained {
    pub done: AtomicU64,
    pub notify: tokio::sync::Notify,
}

// ── 6. the run tree ─────────────────────────────────────────────────────────

/// The live runs of `root`'s owned tree, the root included (bounded: a
/// tree has at most a few dozen runs).
pub async fn tree(control: &ouro_runtime::control::Control, root: &SavedRun) -> anyhow::Result<Vec<SavedRun>> {
    const MAX_RUNS: usize = 1_000;
    let mut runs = vec![root.clone()];
    let mut next = 0;
    while next < runs.len() && runs.len() < MAX_RUNS {
        let parent = runs[next].admission.id.clone();
        next += 1;
        let mut cursor = None;
        loop {
            let filter = Runs { parent: Some(parent.clone()), ..Default::default() };
            let page = control.list(&filter, cursor.as_ref(), 200).await.map_err(anyhow::Error::from)?;
            runs.extend(page.items.into_iter().filter(|r| r.ended_ms.is_none()));
            match page.next {
                Some(c) if runs.len() < MAX_RUNS => cursor = Some(c),
                _ => break,
            }
        }
    }
    Ok(runs)
}

/// Design 03, decision 1 (R-H1, R2-L): a run that is due or leased keeps the
/// tree busy; a waiting run whose wake-up passed is due; an operator-held run
/// is quiescent; a waiting run with a future wake-up waits on an event or a
/// timer.
pub fn quiescent_run(run: &SavedRun, now: i64) -> bool {
    if run.ended_ms.is_some() || run.held.is_some() {
        return true;
    }
    match run.state {
        RunState::Waiting => run.wake_at_ms > now,
        RunState::Completed | RunState::Failed | RunState::Cancelled => true,
        RunState::Ready | RunState::Running => false,
    }
}

// ── 7. built-ins at boot ────────────────────────────────────────────────────
// Design 03, R-M7, on design 04's `workflow_meta`: reuse the stored version
// whose digest equals the built-in's, else save it, and record its digest as
// a built-in text; activate it only when nothing is active or the active
// version is a built-in text (an older one: the boot upgrades it). An active
// version that is not a built-in (the agent's override, an operator's) is
// never replaced, and a name an operator paused is left alone: an
// activation would lift the pause (design 04, decision 5).

pub async fn install_builtins(
    runtime: &ouro_runtime::Runtime,
    db: &AgentDb,
    builtins: &[(&str, &str)],
) -> anyhow::Result<()> {
    for (name, yaml) in builtins {
        let digest = ouro_runtime::durable::digest_of(yaml);
        let (record_name, record_digest) = (name.to_string(), digest.clone());
        db.write(move |db| crate::flows::WorkflowMeta::record(db, &record_name, &record_digest, None, true)).await?;
        let definitions = runtime.definitions(name).await?;
        if let Some(manual) = definitions.tripped.as_ref().and_then(|t| t.manual.as_ref()) {
            eprintln!("[engine] `{name}` is paused by an operator ({}): its built-in is not installed", manual.note);
            continue;
        }
        let version = match definitions.versions.values().find(|d| d.digest == digest) {
            Some(d) => d.version,
            None => runtime.save(name, yaml).await?.version,
        };
        let active = definitions.active.and_then(|v| definitions.versions.get(&v));
        let replace = match active {
            None => true,
            Some(active) => {
                active.digest != digest
                    && crate::flows::WorkflowMeta::get(db, name, &active.digest)?.is_some_and(|m| m.builtin)
            }
        };
        if replace {
            runtime.activate_as(name, version, "agent-supervisor", Some("built-in")).await?;
        }
    }
    Ok(())
}

// ── 8. reconciliation and sweep ─────────────────────────────────────────────
// The store is the truth for what a run did: the reconciler (at boot and
// every minute) finds runs that ended in a window of the engine's history,
// writes a root run's final output into its trace and marks every ended run
// `recorded`, which is what the activity journal and the judge wait for. A
// `run_trace` row whose run never started is closed after ten minutes, once
// nothing of its trace is live. Windows overlap and every step is idempotent.

pub const RECONCILE_EVERY: std::time::Duration = std::time::Duration::from_secs(60);
/// The end of a window lags the clock: a commit still in flight lands in the
/// next one.
pub const RECONCILE_LAG_MS: i64 = 30_000;
pub const RECONCILE_OVERLAP_MS: i64 = 5 * 60_000;
pub const NEVER_STARTED_MS: i64 = 10 * 60_000;
pub const RECONCILED_KEY: &str = "engine.reconciled_ms";
/// Set at the first start on the engine: cards created before it belong to
/// runs that no longer exist (design 03, R-L6).
pub const MIGRATED_KEY: &str = "engine.migrated_at";

/// Keys of the memory KV the runtime owns (`set_memory` refuses them).
pub const RESERVED_MEMORY_PREFIX: &str = "engine.";

impl AgentDb {
    /// Deletes the agent's evidence of runs ended before `cutoff_ms`, given
    /// their ids: anchors, cards, verdict uses, evidence links, and evidence
    /// no run links any more (never by age alone, R2-H1).
    pub fn sweep_runs(&self, ended: &[String]) -> anyhow::Result<usize> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let mut deleted = 0;
        for run in ended {
            deleted += tx.execute("DELETE FROM run_trace WHERE run_id = ?1 AND recorded = 1", [run])?;
            tx.execute("DELETE FROM confirmations WHERE run_id = ?1", [run])?;
            tx.execute("DELETE FROM guard_verdict_uses WHERE child_run = ?1", [run])?;
            tx.execute("DELETE FROM run_evidence WHERE run_id = ?1", [run])?;
        }
        tx.execute(
            "DELETE FROM evidence WHERE NOT EXISTS (SELECT 1 FROM run_evidence l WHERE l.digest = evidence.digest)",
            [],
        )?;
        tx.commit()?;
        Ok(deleted)
    }
}

// ── 9. waiting, the background and the reconciler ───────────────────────────

/// Waits until run `id` ended (the engine worker drives it).
pub async fn wait_end(
    engine: &crate::agent::Engine,
    id: &str,
    cancel: &tokio_util::sync::CancellationToken,
) -> anyhow::Result<Status> {
    let mut pause = std::time::Duration::from_millis(5);
    loop {
        if let Some(status) = engine.runtime.get(id).await?.and_then(|r| r.outcome) {
            return Ok(status);
        }
        tokio::select! {
            _ = cancel.cancelled() => return Err(crate::errors::aborted()),
            _ = tokio::time::sleep(pause) => {}
        }
        pause = (pause * 2).min(std::time::Duration::from_millis(50));
    }
}

// `root`'s live tree, each run read on its own after the listing, by id.
async fn tree_states(engine: &crate::agent::Engine, root: &SavedRun) -> anyhow::Result<Vec<SavedRun>> {
    let mut runs = Vec::new();
    for listed in tree(&engine.control, root).await? {
        if let Some(saved) = engine.runtime.get(&listed.admission.id).await? {
            runs.push(saved);
        }
    }
    runs.sort_by(|a, b| a.admission.id.cmp(&b.admission.id));
    Ok(runs)
}

fn same_state(a: &SavedRun, b: &SavedRun) -> bool {
    a.admission.id == b.admission.id
        && a.state == b.state
        && a.wake_at_ms == b.wake_at_ms
        && a.ended_ms == b.ended_ms
        && a.held.is_some() == b.held.is_some()
}

/// Waits until each of `roots` ended or its tree is quiescent (decision 1),
/// then until the lifecycle drain caught up with what the runs reported.
pub async fn wait_quiescent(
    engine: &crate::agent::Engine,
    roots: &[String],
    cancel: &tokio_util::sync::CancellationToken,
) -> anyhow::Result<()> {
    let mut pause = std::time::Duration::from_millis(5);
    loop {
        let mut quiet = true;
        for root in roots {
            let Some(run) = engine.runtime.get(root).await? else { continue };
            if run.ended_ms.is_some() {
                continue;
            }
            // Two reads of the tree that agree (a double collect): a run
            // that started a child and suspended between the listing and
            // its own read would otherwise look quiescent with its child
            // unlisted. A child's end wakes its parent in the same commit,
            // and a wake-up the engine set before `now` counts as due.
            let first = tree_states(engine, &run).await?;
            let fresh = tree_states(engine, &run).await?;
            let now = engine.clock.now_ms();
            let same = first.len() == fresh.len() && first.iter().zip(&fresh).all(|(a, b)| same_state(a, b));
            if !same || !fresh.iter().all(|r| quiescent_run(r, now)) {
                quiet = false;
                break;
            }
        }
        if quiet {
            break;
        }
        tokio::select! {
            _ = cancel.cancelled() => return Err(crate::errors::aborted()),
            _ = tokio::time::sleep(pause) => {}
        }
        pause = (pause * 2).min(std::time::Duration::from_millis(50));
    }
    flushed(engine, std::time::Duration::from_secs(2)).await;
    Ok(())
}

/// Waits (at most `limit`) until the drain handled every lifecycle event
/// reported so far.
pub async fn flushed(engine: &crate::agent::Engine, limit: std::time::Duration) {
    if !engine.started() {
        return;
    }
    let target = engine.lifecycle().sent();
    let deadline = tokio::time::Instant::now() + limit;
    while engine.drained.done.load(Ordering::SeqCst) + engine.lifecycle().dropped() < target {
        let notified = engine.drained.notify.notified();
        if tokio::time::timeout_at(deadline, notified).await.is_err() {
            break;
        }
    }
}

/// The engine's background work (`Engine::start`): the worker in a restart
/// loop with backoff (design 01, decision 12), the lifecycle drain and the
/// reconciler, until `stop`.
pub async fn background(
    engine: std::sync::Weak<crate::agent::Engine>,
    events: Option<ouro_runtime::BufferedEvents>,
    stop: tokio_util::sync::CancellationToken,
) {
    let worker = {
        let (engine, stop) = (engine.clone(), stop.clone());
        async move {
            let mut backoff = std::time::Duration::from_secs(1);
            loop {
                let Some(runtime) = engine.upgrade().map(|e| e.runtime.clone()) else { return };
                match runtime.worker(stop.clone()).await {
                    Ok(()) => return,
                    Err(error) => eprintln!("[engine] worker stopped, restarting in {backoff:?}: {error:#}"),
                }
                drop(runtime);
                tokio::select! {
                    _ = stop.cancelled() => return,
                    _ = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(std::time::Duration::from_secs(60));
            }
        }
    };
    let drain = {
        let (engine, stop) = (engine.clone(), stop.clone());
        async move {
            let Some(mut events) = events else { return };
            loop {
                let event = tokio::select! {
                    _ = stop.cancelled() => return,
                    event = events.recv() => event,
                };
                let Some(event) = event else { return };
                let Some(engine) = engine.upgrade() else { return };
                handle(&engine, event).await;
                engine.drained.done.fetch_add(1, Ordering::SeqCst);
                engine.drained.notify.notify_waiters();
            }
        }
    };
    let reconciler = {
        let (engine, stop) = (engine.clone(), stop.clone());
        async move {
            let mut tick = tokio::time::interval(RECONCILE_EVERY);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = stop.cancelled() => return,
                    _ = tick.tick() => {}
                }
                let Some(engine) = engine.upgrade() else { return };
                if let Err(error) = reconcile_once(&engine, now_ms()).await {
                    eprintln!("[engine] reconcile: {error:#}");
                }
            }
        }
    };
    // Each its own task: the drain and the reconciler write agent.db
    // synchronously, and such a write may wait for the engine's lock, which
    // only the worker's progress releases (§2): in the worker's task it
    // would stall the very commit it waits for.
    let (drain, reconciler) = (tokio::spawn(drain), tokio::spawn(reconciler));
    worker.await;
    let _ = tokio::join!(drain, reconciler);
}

// One lifecycle event: a stop ends the execution's scope; an end ends the
// run's live loop too, and records a child's end (a root's final output is
// its creator's, else written here).
async fn handle(engine: &std::sync::Arc<crate::agent::Engine>, event: ouro_runtime::Event) {
    use ouro_runtime::EventKind;
    let run = event.run_id.clone();
    match event.kind {
        EventKind::RunSuspended { .. } | EventKind::RunInterrupted { .. } => engine.scopes.stop(&run, None),
        EventKind::RunEnded { .. } => {
            let saved = engine.runtime.get(&run).await.ok().flatten();
            if let Some(saved) = &saved
                && let Some(status) = &saved.outcome
            {
                finish(engine, &run, trace_root(saved), status).await;
            }
            engine.end_run_loop(&run).await;
        }
        _ => {}
    }
}

/// Whether the reconciler writes a run's outcome into its trace: a root run
/// whose creator may be gone when it ends (a signal that suspended), except
/// a recovery report, a root of its own (outside the signal's tree, R2-H2)
/// that traces as a span of the signal's trace, whose outcome is the
/// signal's, and a loop the host runs itself (`AgentLoop::send`), whose
/// caller writes its outcome.
pub fn trace_root(run: &SavedRun) -> bool {
    run.child.is_none() && !matches!(host_kind(run.admission.host()), Some(RECOVERY | LOOP))
}

// A run that ended: its live scope ends with the outcome; a child is
// recorded at once; a root's final output goes into its trace (unless its
// creator still holds the trace and writes it) before it is recorded.
async fn finish(engine: &std::sync::Arc<crate::agent::Engine>, run: &str, root: bool, status: &Status) {
    let borrowed = engine.scopes.current(run).is_some() && !engine.scopes.owns(run);
    engine.scopes.stop(run, Some(span_end(status)));
    let Ok(Some(row)) = engine.deps.db.run_trace(run) else { return };
    if row.recorded || (root && borrowed) {
        return;
    }
    if root && let Some(anchor) = row.anchor() {
        let trace = engine.deps.tracer.trace(TraceStart {
            session_id: anchor.session_id.clone(),
            continuation: Some(anchor),
            id: uuid::Uuid::new_v4().to_string(),
            name: "run_end".into(),
            metadata: Some(json!({"run": run}).as_object().cloned().unwrap_or_default()),
            ..Default::default()
        });
        trace.update(final_output(status));
        trace.end();
    }
    if let Err(error) = engine.deps.db.mark_recorded(run) {
        eprintln!("[engine] recording {run}: {error:#}");
    }
}

/// What a root run's outcome writes into its trace.
pub fn final_output(status: &Status) -> crate::tracing::ContextUpdate {
    match status {
        Status::Completed(output) => crate::tracing::ContextUpdate::output(match output {
            Value::String(text) => json!(text),
            other => other.clone(),
        }),
        Status::Faulted(error) => crate::tracing::ContextUpdate {
            output: Some(json!({"error": error.detail.clone().unwrap_or_else(|| error.to_string())})),
            metadata: Some(json!({"error": true}).as_object().cloned().unwrap_or_default()),
            ..Default::default()
        },
        Status::Cancelled => crate::tracing::ContextUpdate::output(json!({"cancelled": true})),
        _ => Default::default(),
    }
}

/// One pass of the reconciler (§8) at `now`: runs that ended in the window
/// since the last pass (with overlap) are recorded, root outputs written;
/// stale live loops end; orphan anchors close; evidence past retention goes.
pub async fn reconcile_once(engine: &std::sync::Arc<crate::agent::Engine>, now: i64) -> anyhow::Result<usize> {
    let memory = &engine.deps.memory;
    let retention = ouro_runtime::durable::RECORDS_KEPT_MS;
    let since = memory
        .get(RECONCILED_KEY)?
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(now - retention)
        .saturating_sub(RECONCILE_OVERLAP_MS);
    let before = now - RECONCILE_LAG_MS;
    let mut recorded = 0;
    if since < before {
        for run in history(engine, since, before).await? {
            let Some(status) = &run.outcome else { continue };
            let id = &run.admission.id;
            if engine.deps.db.run_trace(id)?.is_some_and(|row| !row.recorded) {
                finish(engine, id, trace_root(&run), status).await;
                recorded += 1;
            }
            engine.end_run_loop(id).await;
        }
        memory.set(RECONCILED_KEY, &before.to_string())?;
    }
    for id in engine.loop_ids().await {
        if engine.runtime.get(&id).await?.is_some_and(|r| r.ended_ms.is_some()) {
            engine.end_run_loop(&id).await;
        }
    }
    for row in engine.deps.db.open_run_traces(500)? {
        if row.created_ms > now - NEVER_STARTED_MS || engine.runtime.get(&row.run_id).await?.is_some() {
            continue;
        }
        let mut live = false;
        for other in engine.deps.db.trace_runs(&row.trace_id)? {
            if engine.runtime.get(&other.run_id).await?.is_some_and(|r| r.ended_ms.is_none()) {
                live = true;
                break;
            }
        }
        if !live {
            engine.deps.db.mark_recorded(&row.run_id)?;
            recorded += 1;
        }
    }
    let swept_since = memory.get(SWEPT_KEY)?.and_then(|s| s.parse::<i64>().ok()).unwrap_or(0);
    let swept_before = now - retention;
    if swept_since < swept_before {
        let ended: Vec<String> =
            history(engine, swept_since, swept_before).await?.into_iter().map(|r| r.admission.id).collect();
        engine.deps.db.sweep_runs(&ended)?;
        memory.set(SWEPT_KEY, &swept_before.to_string())?;
    }
    Ok(recorded)
}

pub const SWEPT_KEY: &str = "engine.swept_ms";

// Every run that ended in `[since, before)`, paged.
async fn history(engine: &crate::agent::Engine, since: i64, before: i64) -> anyhow::Result<Vec<SavedRun>> {
    let mut runs = Vec::new();
    let mut cursor = None;
    let filter = Runs { since_ms: Some(since.max(0)), before_ms: Some(before), ..Default::default() };
    loop {
        let page = engine.control.history(&filter, cursor.as_ref(), 500).await.map_err(anyhow::Error::from)?;
        runs.extend(page.items);
        match page.next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    Ok(runs)
}

/// The first start on the engine (design 03, C and D): the built-ins are
/// installed, and cards created before the migration marker are edited to
/// "expired" once (their runs are gone).
pub async fn boot(engine: &std::sync::Arc<crate::agent::Engine>) -> anyhow::Result<()> {
    install_builtins(&engine.runtime, &engine.deps.db, &crate::flows::builtins()).await?;
    if engine.deps.memory.get(MIGRATED_KEY)?.is_none() {
        let now = now_ms();
        let card = crate::confirmation::Card::load(&*engine.deps.memory);
        let expired = crate::confirmation::expire_cut_over(
            &engine.deps.db,
            &*engine.deps.mcp,
            now,
            &card,
            &tokio_util::sync::CancellationToken::new(),
        )
        .await?;
        if expired > 0 {
            eprintln!("[engine] cut-over: {expired} pending confirmation cards expired");
        }
        engine.deps.memory.set(MIGRATED_KEY, &now.to_string())?;
    }
    Ok(())
}

// ── 10. starting a name the agent may have overridden ───────────────────────
// Design 04 (R-M1, R2-H1): every start of `signal`, a loop, a route or a
// saved workflow goes through one decision. A start that the engine refuses
// before admitting anything (the breaker tripped, an operator paused it, a
// function it calls is gone, it no longer validates) never fails the run
// it serves: the same id starts pinned at the built-in in its place, or the
// route or saved path steps aside for the default path. A fault during a
// run is not a refusal. A starter dispatched again joins what its first
// dispatch committed, under the name that ran.

/// Where a named start happens: the signal's own run; a loop (the primary,
/// a worker, a recovery report, a loop the host runs itself), which only a
/// loop may serve; a route the dispatcher starts; a scheduled task's saved
/// workflow.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Site {
    Signal,
    Loop,
    Route,
    Saved,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FallbackReason {
    Tripped,
    Invalid,
    Missing,
    NotALoop,
    Paused,
}

impl FallbackReason {
    fn text(self, faults: Option<u32>) -> String {
        match self {
            FallbackReason::Tripped => {
                format!("its breaker tripped after {} faults in a row", faults.unwrap_or(0))
            }
            FallbackReason::Invalid => "it no longer fits the tools".into(),
            FallbackReason::Missing => "it has no active version".into(),
            FallbackReason::NotALoop => "it is not a loop".into(),
            FallbackReason::Paused => "an operator paused it".into(),
        }
    }
}

/// A version set aside for one start (design 04, R-M4): which, why, what
/// ran instead (`None`: a route or a saved workflow, whose path steps aside),
/// and the line the run that runs tells its model. Informs; the agent decides.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Fallback {
    pub name: String,
    pub version: Option<u64>,
    pub digest: Option<String>,
    pub reason: FallbackReason,
    pub error: Option<String>,
    pub ran: Option<(String, u64)>,
    pub line: String,
}

// An error carried into host data stays short.
const FALLBACK_ERROR_CHARS: usize = 400;

/// Why a version is set aside for one start, before what runs instead is
/// known: its version and content, the reason, the engine's words, and the
/// breaker's count.
struct Aside {
    version: Option<u64>,
    digest: Option<String>,
    reason: FallbackReason,
    error: Option<String>,
    faults: Option<u32>,
}

impl Fallback {
    fn new(name: &str, aside: Aside, site: Site, ran: Option<(String, u64)>) -> Fallback {
        let Aside { version, digest, reason, error, faults } = aside;
        let error = error.map(|e| crate::activity::clip(&e, FALLBACK_ERROR_CHARS));
        let version_text = version.map(|v| format!(" v{v}")).unwrap_or_default();
        let why = match &error {
            Some(error) => format!("{}: {error}", reason.text(faults)),
            None => reason.text(faults),
        };
        let instead = match (&ran, site) {
            (Some((ran, _)), _) => format!("the built-in `{ran}` runs this time"),
            (None, Site::Route) => "the default path handles this signal".into(),
            (None, _) => "the agent loop takes this task".into(),
        };
        let line = format!(
            "Workflow `{name}`{version_text} was set aside ({why}); {instead}. Tell the user if it matters; fix or re-activate it if you own it."
        );
        Fallback { name: name.into(), version, digest, reason, error, ran, line }
    }
}

/// The fallback record where it shows (design 04, R-M4): a `fallback`
/// event on `scope` and, for the span of the run that ran in its place
/// (`metadata`), the record in its metadata.
pub fn trace_fallback(scope: crate::tracing::Scope, fallback: &Fallback, metadata: bool) {
    let record = serde_json::to_value(fallback).unwrap_or_default();
    scope.event(crate::tracing::EventStart {
        name: "fallback".into(),
        level: Some(crate::tracing::Level::Warning),
        metadata: json!({"fallback": record}).as_object().cloned(),
        ..Default::default()
    });
    if metadata {
        scope.update(crate::tracing::ContextUpdate::metadata(
            json!({"fallback": record}).as_object().cloned().unwrap_or_default(),
        ));
    }
}

/// What a named start ran: its definition and version, whether it was
/// pinned, and the version set aside for it, if any.
#[derive(Clone, Debug, PartialEq)]
pub struct Ran {
    pub name: String,
    pub version: u64,
    pub pinned: bool,
    pub fallback: Option<Fallback>,
}

/// What a start runs: the definition chosen and the version set aside for
/// it, if any. The starter's builder makes the run's input and host data
/// from it, so a fallback rebuilds both (the line in its instructions, the
/// definition it names in its host data).
#[derive(Clone, Debug)]
pub struct Choice {
    pub name: String,
    pub version: u64,
    pub yaml: String,
    pub fallback: Option<Fallback>,
}

/// The outcome of a named start at a route or a saved site, which may step
/// aside instead of starting.
pub enum Named<T> {
    Started(T, Ran),
    SetAside(Fallback),
}

/// What a stored run ran, from its admission: a joined start reports it.
pub fn ran_of(run: &SavedRun) -> Ran {
    let fallback = run
        .admission
        .host()
        .and_then(|h| h.get("fallback"))
        .or_else(|| run.admission.input.get("fallback"))
        .and_then(|f| serde_json::from_value(f.clone()).ok());
    Ran {
        name: run.admission.name.clone(),
        version: run.admission.version,
        pinned: run.admission.runtime[ouro_runtime::durable::PINNED] == true,
        fallback,
    }
}

enum Decision {
    Start { version: u64, yaml: String },
    Aside(Aside),
}

// Step 1: the active version just read, unless there is none, it is set
// aside (its breaker, an operator), or it is no loop at a loop site (R-H2).
async fn decide(engine: &crate::agent::Engine, name: &str, site: Site) -> anyhow::Result<Decision> {
    let definitions = engine.runtime.definitions(name).await?;
    let Some(d) = definitions.active.and_then(|v| definitions.versions.get(&v)) else {
        return Ok(Decision::Aside(Aside {
            version: None,
            digest: None,
            reason: FallbackReason::Missing,
            error: None,
            faults: None,
        }));
    };
    if let Some(trip) = &definitions.tripped {
        let refusal = trip.refusal(name);
        let reason = if refusal.paused { FallbackReason::Paused } else { FallbackReason::Tripped };
        let error = match &trip.manual {
            Some(manual) => format!("{refusal}: {}", manual.note),
            None => refusal.to_string(),
        };
        return Ok(Decision::Aside(Aside {
            version: Some(d.version),
            digest: Some(d.digest.clone()),
            reason,
            error: Some(error),
            faults: Some(trip.faults),
        }));
    }
    if site == Site::Loop && !crate::flows::is_loop_definition(name, &d.yaml) {
        return Ok(Decision::Aside(Aside {
            version: Some(d.version),
            digest: Some(d.digest.clone()),
            reason: FallbackReason::NotALoop,
            error: None,
            faults: None,
        }));
    }
    Ok(Decision::Start { version: d.version, yaml: d.yaml.clone() })
}

// The refusal table's rows a fallback answers: a breaker's trip or an
// operator's pause, and a definition this host no longer admits. Anything
// else (nesting depth, host data, a mismatch, storage, an interruption) is
// no property of the version and stays the start's error.
fn refusal(error: &anyhow::Error) -> Option<(FallbackReason, String, Option<u32>)> {
    if let Some(tripped) = error.downcast_ref::<ouro_runtime::durable::DefinitionTripped>() {
        let reason = if tripped.paused { FallbackReason::Paused } else { FallbackReason::Tripped };
        return Some((reason, tripped.to_string(), Some(tripped.faults)));
    }
    if error.is::<ouro_runtime::MissingFunction>() || error.is::<ouro_runtime::InvalidDefinition>() {
        return Some((FallbackReason::Invalid, format!("{error:#}"), None));
    }
    None
}

fn child_refusal(error: &ouro_runtime::ChildError) -> Option<(FallbackReason, String, Option<u32>)> {
    use ouro_runtime::{ChildError, ChildRefusal};
    match error {
        ChildError::Refused(ChildRefusal::Tripped(tripped)) => {
            let reason = if tripped.paused { FallbackReason::Paused } else { FallbackReason::Tripped };
            Some((reason, tripped.to_string(), Some(tripped.faults)))
        }
        ChildError::Refused(ChildRefusal::Inadmissible(error)) => {
            Some((FallbackReason::Invalid, format!("{error:#}"), None))
        }
        _ => None,
    }
}

// What runs in place of a set-aside version of `name`: the signal's
// built-in for the dispatcher, the built-in `agent-loop` at a loop site
// (the only built-in loop); none for a route or a saved workflow.
async fn fallback_target(engine: &crate::agent::Engine, site: Site) -> anyhow::Result<Option<(String, u64, String)>> {
    let name = match site {
        Site::Signal => crate::supervisor::SIGNAL_NAME,
        Site::Loop => crate::agent::AGENT_LOOP_NAME,
        Site::Route | Site::Saved => return Ok(None),
    };
    let version = crate::flows::builtin_version(&engine.runtime, name)
        .await?
        .ok_or_else(|| anyhow::anyhow!("the built-in `{name}` is not installed"))?;
    let yaml = crate::flows::built_in(name).unwrap_or_default().to_owned();
    Ok(Some((name.to_owned(), version, yaml)))
}

// The fallback for `name` set aside at `site`, or the start's own error: a
// pause of the very name the fallback would run stops the start with the
// pause's note (decision 5).
async fn set_aside(
    engine: &crate::agent::Engine,
    name: &str,
    site: Site,
    aside: Aside,
) -> anyhow::Result<(Fallback, Option<(String, u64, String)>)> {
    let target = fallback_target(engine, site).await?;
    if aside.reason == FallbackReason::Paused && target.as_ref().is_some_and(|(t, _, _)| t == name) {
        let note = aside.error.unwrap_or_else(|| format!("{name} is paused by an operator"));
        eprintln!("[engine] `{name}` is paused by an operator and has no other version to run: {note}");
        anyhow::bail!("{note}");
    }
    let ran = target.as_ref().map(|(t, v, _)| (t.clone(), *v));
    Ok((Fallback::new(name, aside, site, ran), target))
}

// A start's refusal in the table, as the version it set aside.
fn refused(version: u64, (reason, text, faults): (FallbackReason, String, Option<u32>)) -> Aside {
    Aside { version: Some(version), digest: None, reason, error: Some(text), faults }
}

/// What a start of `name` at `site` runs, by the rule every start follows:
/// its active version, or what runs in place of one set aside (step 1). A
/// dry run's root and its recorded recovery start choose this way (design
/// 05, decision 5), without the refusal a start may still meet.
pub async fn choose(engine: &crate::agent::Engine, name: &str, site: Site) -> anyhow::Result<Choice> {
    let aside = match decide(engine, name, site).await? {
        Decision::Start { version, yaml } => return Ok(Choice { name: name.into(), version, yaml, fallback: None }),
        Decision::Aside(aside) => aside,
    };
    let (fallback, target) = set_aside(engine, name, site, aside).await?;
    let Some((target, version, yaml)) = target else {
        anyhow::bail!("`{name}` was set aside and has nothing to run in its place: {}", fallback.line)
    };
    Ok(Choice { name: target, version, yaml, fallback: Some(fallback) })
}

/// Starts root run `id` of `name` at `site` (the signal; a recovery report or
/// a loop the host runs itself), its input and host data from `build`. An
/// existing run is returned as it was admitted (step 0). A refusal in the
/// table starts the same id pinned at the built-in in its place; the
/// fallback's own refusal is the original error, logged (R-L9).
pub async fn start_named_root(
    engine: &crate::agent::Engine,
    id: &str,
    name: &str,
    site: Site,
    build: impl Fn(Choice) -> futures::future::BoxFuture<'static, anyhow::Result<(Value, Value)>>,
) -> anyhow::Result<(SavedRun, Ran)> {
    if let Some(saved) = engine.runtime.get(id).await? {
        let ran = ran_of(&saved);
        return Ok((saved, ran));
    }
    let aside = match decide(engine, name, site).await? {
        Decision::Start { version, yaml } => {
            let (input, host) = build(Choice { name: name.into(), version, yaml, fallback: None }).await?;
            let start = ouro_runtime::Start { version: Some(version), host };
            match engine.runtime.start_with(id, name, input, start).await {
                Ok(saved) => {
                    let ran = Ran { name: name.into(), version, pinned: true, fallback: None };
                    return Ok((saved, ran));
                }
                Err(e) => match refusal(&e) {
                    Some(why) => refused(version, why),
                    None => return Err(e),
                },
            }
        }
        Decision::Aside(aside) => aside,
    };
    let (fallback, target) = set_aside(engine, name, site, aside).await?;
    let Some((target, target_version, yaml)) = target else {
        anyhow::bail!("`{name}` was set aside and has nothing to run in its place: {}", fallback.line)
    };
    let choice = Choice { name: target.clone(), version: target_version, yaml, fallback: Some(fallback.clone()) };
    let (input, host) = build(choice).await?;
    let start = ouro_runtime::Start { version: Some(target_version), host };
    match engine.runtime.start_with(id, &target, input, start).await {
        Ok(saved) => {
            let ran = Ran { name: target, version: target_version, pinned: true, fallback: Some(fallback) };
            Ok((saved, ran))
        }
        Err(e) => {
            eprintln!("[engine] the built-in `{target}` in place of `{name}` was refused too: {e:#}");
            anyhow::bail!("{}", fallback.error.clone().unwrap_or(fallback.line))
        }
    }
}

/// What starts a host child of the calling run: its function's context
/// (`FunctionContext`); a seam, so a test can stand in for a re-dispatched
/// call. It reads the child it may have started already through the call's
/// run access (`RunReads::run`, design 05, R2-L).
#[async_trait::async_trait]
pub trait ChildStarts: RunReads {
    fn child_id(&self, key: &str) -> String;
    async fn start_child(
        &self,
        child: ouro_runtime::ChildStart,
    ) -> Result<ouro_runtime::ChildRef, ouro_runtime::ChildError>;
}

#[async_trait::async_trait]
impl ChildStarts for ouro_runtime::FunctionContext {
    fn child_id(&self, key: &str) -> String {
        ouro_runtime::FunctionContext::child_id(self, key)
    }
    async fn start_child(
        &self,
        child: ouro_runtime::ChildStart,
    ) -> Result<ouro_runtime::ChildRef, ouro_runtime::ChildError> {
        ouro_runtime::FunctionContext::start_child(self, child).await
    }
}

/// Starts the host child `key` of the calling run as a run of `name` at
/// `site` (the primary, a worker, a route, a saved workflow). A child the
/// call started before is joined under the name it runs (step 0), whatever
/// `name` says now. At a route or saved site a set-aside version starts
/// nothing (`Named::SetAside`): the caller steps aside.
pub async fn start_named_child(
    engine: &crate::agent::Engine,
    ctx: &dyn ChildStarts,
    key: &str,
    name: &str,
    site: Site,
    build: impl Fn(Choice) -> futures::future::BoxFuture<'static, anyhow::Result<(Value, Value)>>,
) -> anyhow::Result<Named<ouro_runtime::ChildRef>> {
    // A scheduled task's definition is never a child the host starts (a
    // forged `{workflow: "task-…"}` payload reaching `launch`, design 06).
    anyhow::ensure!(
        !name.starts_with(crate::schedule::TASK_PREFIX),
        "`{name}` is a scheduled task's definition and is not started as a workflow"
    );
    let start = async |name: &str, version: Option<u64>, input: Value, host: Value| {
        ctx.start_child(ouro_runtime::ChildStart { key: key.into(), name: name.into(), version, input, host }).await
    };
    if let Some(saved) = ctx.run(&ctx.child_id(key)).await? {
        let child = start(&saved.admission.name, None, Value::Null, Value::Null).await?;
        return Ok(Named::Started(child, ran_of(&saved)));
    }
    let aside = match decide(engine, name, site).await? {
        Decision::Start { version, yaml } => {
            let (input, host) = build(Choice { name: name.into(), version, yaml, fallback: None }).await?;
            match start(name, Some(version), input, host).await {
                Ok(child) => {
                    let ran = Ran { name: name.into(), version, pinned: true, fallback: None };
                    return Ok(Named::Started(child, ran));
                }
                Err(e) => match child_refusal(&e) {
                    Some(why) => refused(version, why),
                    None => return Err(e.into()),
                },
            }
        }
        Decision::Aside(aside) => aside,
    };
    let (fallback, target) = set_aside(engine, name, site, aside).await?;
    // A dry run reports the version it set aside (design 05, decision 8).
    if let Some(sandbox) = &engine.sandbox {
        sandbox.set_aside(&fallback);
    }
    let Some((target, target_version, yaml)) = target else { return Ok(Named::SetAside(fallback)) };
    let choice = Choice { name: target.clone(), version: target_version, yaml, fallback: Some(fallback.clone()) };
    let (input, host) = build(choice).await?;
    match start(&target, Some(target_version), input, host).await {
        Ok(child) => {
            if let Some(sandbox) = &engine.sandbox {
                sandbox.fallback(&child.id, &fallback);
            }
            let ran = Ran { name: target, version: target_version, pinned: true, fallback: Some(fallback) };
            Ok(Named::Started(child, ran))
        }
        Err(e) => {
            eprintln!("[engine] the built-in `{target}` in place of `{name}` was refused too: {e:#}");
            anyhow::bail!("{}", fallback.error.clone().unwrap_or(fallback.line))
        }
    }
}

// ── 11. what a handler reads of its run tree ────────────────────────────────
// Design 05, decision 3 (ouro design 07): a handler reads its own run, its
// ancestors, its descendants and their records through the call's run
// access, which is the store driving the call: the host's in a live run, a
// dry run's private one in a dry run. The engine's own handle
// (`Engine::runtime`, `control`) reads the host's store, so a handler that
// used it would read the wrong runs in a dry run (the sandbox's guard store
// refuses it, counting). Host-side code (the supervisor, a loop the host
// drives, tests) reads through the engine (`impl RunReads for Engine`).

/// The runs a caller may read, by id: a handler's lineage, or for the host
/// its whole store.
#[async_trait::async_trait]
pub trait RunReads: Send + Sync {
    /// A run (`None`: no such run, or one outside the caller's lineage).
    async fn run(&self, id: &str) -> anyhow::Result<Option<SavedRun>>;
    /// A run's records (empty for one the caller cannot read).
    async fn records(&self, id: &str) -> anyhow::Result<crate::agent::Records>;
    /// The children of `parent` by their parent link, every page.
    async fn children(&self, parent: &str) -> anyhow::Result<Vec<SavedRun>>;
}

// A run-access failure as the agent's handlers report it: a call that lost
// its run is stopped (`interrupted`, it resumes), anything else an error.
pub fn access_error(error: ouro_runtime::RunAccessError) -> anyhow::Error {
    match error {
        ouro_runtime::RunAccessError::Interrupted(cause) => {
            crate::errors::aborted().context(format!("the call lost its run: {cause:#}"))
        }
        other => anyhow::Error::new(other),
    }
}

const CHILDREN_PAGE: usize = 200;

#[async_trait::async_trait]
impl RunReads for ouro_runtime::FunctionContext {
    async fn run(&self, id: &str) -> anyhow::Result<Option<SavedRun>> {
        ouro_runtime::FunctionContext::run(self, id).await.map_err(access_error)
    }
    async fn records(&self, id: &str) -> anyhow::Result<crate::agent::Records> {
        Ok(ouro_runtime::FunctionContext::records(self, id).await.map_err(access_error)?.unwrap_or_default())
    }
    async fn children(&self, parent: &str) -> anyhow::Result<Vec<SavedRun>> {
        let mut children = Vec::new();
        let mut cursor = None;
        loop {
            let page = ouro_runtime::FunctionContext::children(self, parent, cursor, CHILDREN_PAGE)
                .await
                .map_err(access_error)?;
            children.extend(page.items);
            match page.next {
                Some(next) => cursor = Some(next),
                None => return Ok(children),
            }
        }
    }
}

#[async_trait::async_trait]
impl RunReads for crate::agent::Engine {
    async fn run(&self, id: &str) -> anyhow::Result<Option<SavedRun>> {
        self.runtime.get(id).await
    }
    async fn records(&self, id: &str) -> anyhow::Result<crate::agent::Records> {
        self.runtime.records(id).await
    }
    async fn children(&self, parent: &str) -> anyhow::Result<Vec<SavedRun>> {
        let mut children = Vec::new();
        let mut cursor = None;
        loop {
            let filter = Runs { parent: Some(parent.into()), ..Default::default() };
            let page = self.control.list(&filter, cursor.as_ref(), CHILDREN_PAGE).await.map_err(anyhow::Error::from)?;
            children.extend(page.items);
            match page.next {
                Some(next) => cursor = Some(next),
                None => return Ok(children),
            }
        }
    }
}
