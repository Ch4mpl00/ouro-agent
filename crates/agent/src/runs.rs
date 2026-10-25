// Runs on the ouro engine (docs/designs/03-runs-on-the-engine.md): every
// workflow run of the agent (signal, loops, workers, guard rounds, recovery)
// is a run of the engine's store inside agent.db (`ouro_*` tables, its own
// connection). The agent keeps what the engine does not know.
// Sections: 1. the runtime (store, capacity, budget); 2. agent tables
// (evidence, run anchors, the MCP tool snapshot); 3. host data, the trust
// boundary; 4. live trace scopes; 5. the lifecycle observer; 6. the run tree
// and its quiescence; 7. built-ins at boot; 8. reconciliation and sweep.
use crate::{
    db::AgentDb,
    tracing::{Scope, Span, SpanEnd, Trace, TraceAnchor, TraceStart},
};
use ouro_runtime::durable::{RunState, SavedRun, control::Runs};
use ouro_runtime::ows::run::Status;
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
        ..Default::default()
    }
}

/// The engine's store in agent.db: its own connection, `ouro_*` tables.
pub fn open_store(path: &std::path::Path) -> anyhow::Result<Arc<dyn ouro_runtime::durable::RunStore>> {
    Ok(Arc::new(ouro_runtime::durable::sqlite::open(path)?))
}

/// A store in memory, for tests and tools that keep nothing.
pub fn memory_store() -> Arc<dyn ouro_runtime::durable::RunStore> {
    Arc::new(ouro_runtime::durable::memory::memory())
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
    /// Stores `body` (immutable, content-addressed) and links it to `run`,
    /// before the engine admits the run (design 03, R2-H1). Returns its digest.
    pub fn put_evidence(&self, run: &str, body: &str) -> anyhow::Result<String> {
        let digest = digest(body);
        let mut conn = self.conn();
        let tx = conn.transaction()?;
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
        Ok(self.conn().query_row("SELECT body FROM evidence WHERE digest = ?1", [digest], |r| r.get(0)).optional()?)
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
            .conn()
            .query_row(
                "SELECT run_id, trace_id, session_id, parent_span_id, recorded, created_ms FROM run_trace WHERE run_id = ?1",
                [run],
                run_trace_row,
            )
            .optional()?)
    }

    pub fn open_run_traces(&self, limit: usize) -> anyhow::Result<Vec<RunTrace>> {
        let conn = self.conn();
        let mut statement = conn.prepare(
            "SELECT run_id, trace_id, session_id, parent_span_id, recorded, created_ms FROM run_trace
             WHERE recorded = 0 ORDER BY created_ms LIMIT ?1",
        )?;
        Ok(statement.query_map([i64::try_from(limit)?], run_trace_row)?.collect::<rusqlite::Result<_>>()?)
    }

    pub fn trace_runs(&self, trace: &str) -> anyhow::Result<Vec<RunTrace>> {
        let conn = self.conn();
        let mut statement = conn.prepare(
            "SELECT run_id, trace_id, session_id, parent_span_id, recorded, created_ms FROM run_trace WHERE trace_id = ?1",
        )?;
        Ok(statement.query_map([trace], run_trace_row)?.collect::<rusqlite::Result<_>>()?)
    }

    pub fn mark_recorded(&self, run: &str) -> anyhow::Result<()> {
        self.conn().execute("UPDATE run_trace SET recorded = 1 WHERE run_id = ?1", [run])?;
        Ok(())
    }

    /// Adds MCP's tools to the snapshot (never shrunk): a tool that goes away
    /// stays registered and answers `not_executed`, so a run that calls it
    /// never faults on a missing function at resume (design 01, decision 16).
    pub fn snapshot_tools(&self, tools: &[(String, bool)]) -> anyhow::Result<()> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
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
        let conn = self.conn();
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
        // Another handler of the run may have opened one meanwhile.
        if let Some(scope) = current(&live, run) {
            trace.end();
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
        matches!(self.lock().get(run), Some(Live::Span(_) | Live::Segment(_)))
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

    pub fn open(&self) -> Vec<String> {
        let mut runs: Vec<String> = self.lock().keys().cloned().collect();
        runs.sort();
        runs
    }
}

fn current(live: &HashMap<String, Live>, run: &str) -> Option<Scope> {
    Some(match live.get(run)? {
        Live::Borrowed(scope) => scope.clone(),
        Live::Span(span) => span.clone(),
        Live::Segment(trace) => trace.clone(),
    })
}

/// How a run's outcome ends its span.
pub fn span_end(status: &Status) -> SpanEnd {
    match status {
        Status::Completed(output) => SpanEnd::output(output.clone()),
        Status::Faulted(error) => SpanEnd::error(error.to_string()).with_output(json!({"error": error.to_string()})),
        Status::Cancelled => SpanEnd::output(json!({"cancelled": true})),
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
// Design 03, R-M7: reuse the stored version whose digest equals the
// built-in's, else save it; activate only when nothing is active or the
// active version is one of the built-in digests seen before (kept in the
// memory KV until the versions step's `workflow_meta`). An active version
// that is not a built-in (an operator's) is never replaced.

pub fn builtin_digests_key(name: &str) -> String {
    format!("engine.builtin_digests.{name}")
}

pub async fn install_builtins(
    runtime: &ouro_runtime::Runtime,
    memory: &dyn crate::db::MemoryStore,
    builtins: &[(&str, &str)],
) -> anyhow::Result<()> {
    for (name, yaml) in builtins {
        let digest = ouro_runtime::durable::digest_of(yaml);
        let known_key = builtin_digests_key(name);
        let mut known: Vec<String> =
            memory.get(&known_key)?.and_then(|raw| serde_json::from_str(&raw).ok()).unwrap_or_default();
        let definitions = runtime.definitions(name).await?;
        let version = match definitions.versions.values().find(|d| d.digest == digest) {
            Some(d) => d.version,
            None => runtime.save(name, yaml).await?.version,
        };
        if !known.contains(&digest) {
            known.push(digest.clone());
            memory.set(&known_key, &serde_json::to_string(&known)?)?;
        }
        let active_digest = definitions.active.and_then(|v| definitions.versions.get(&v)).map(|d| d.digest.clone());
        let replace = match &active_digest {
            None => true,
            Some(active) => active != &digest && known.contains(active),
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
        let tx = conn.transaction()?;
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
        pause = (pause * 2).min(std::time::Duration::from_millis(100));
    }
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
            let now = now_ms();
            if !tree(&engine.control, &run).await?.iter().all(|r| quiescent_run(r, now)) {
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
        pause = (pause * 2).min(std::time::Duration::from_millis(100));
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
    tokio::join!(worker, drain, reconciler);
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
            let status = engine.runtime.get(&run).await.ok().flatten().and_then(|r| r.outcome);
            if let Some(status) = &status {
                finish(engine, &run, event.parent_id.is_none(), status).await;
            }
            engine.end_run_loop(&run).await;
        }
        _ => {}
    }
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
                finish(engine, id, run.child.is_none(), status).await;
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
    install_builtins(&engine.runtime, &*engine.deps.memory, &crate::flows::builtins()).await?;
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
