// Scheduled tasks on the engine's triggers (docs/designs/06-schedules.md).
// A task is the definition `task-<id>`, written by the host, never by the
// model: its trigger (a cron on the user's wall clock, or a one-shot `at`)
// starts a run whose one call, `fire_scheduled`, records a fire in agent.db
// and ends. The supervisor's drain runs each fire as a `scheduler` (or the
// task's own source) signal, one at a time with every other signal, so no
// handler ever waits on the supervisor (design 06, decision 2).
//
// Sections:
//   1. the fire table — `scheduled_fires`, the task id sequence, the seam
//   2. task definitions — their YAML, the cron rule, reading a task back
//   3. the drain and the zone watch
//   4. tools — schedule_task / list_scheduled_tasks / cancel_scheduled_task
//   5. fire_scheduled — what a trigger's run records
//
// Nothing is seeded. Design 06 decision 14 seeded a system `dreaming` task
// on the first start; the maintainer removed it (LOG.md, 2026-10-26). The
// `dreaming` skill stays, and the agent schedules it when the user asks. The
// key the seed wrote (`schedule.engine_seeded`) is no longer read.
use crate::{
    agent::Engine,
    db::AgentDb,
    session::iso,
    supervisor::{PendingSignal, Supervisor},
};
use async_trait::async_trait;
use chrono::{DateTime, FixedOffset, TimeZone, Utc};
use chrono_tz::Tz;
use croner::Cron;
use ouro_runtime::durable::SavedRun;
use ouro_runtime::durable::control::{DefinitionSummary, ScheduleKind};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

// ── 1. the fire table ──────────────────────────────────────────────────────

/// What a task is, as its definition carries it (`document.metadata.task`):
/// the host's own block, never an expression, so a prompt that looks like
/// `${ … }` arrives verbatim (decision 4).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Task {
    pub id: i64,
    pub cron: String,
    pub recurring: bool,
    pub prompt: String,
    // None → the signal's source is `scheduler`.
    pub source: Option<String>,
    // A saved workflow each fire runs instead of the primary agent loop.
    pub workflow: Option<String>,
    // The zone the task was saved with (a recurring task's trigger zone).
    pub timezone: String,
    // When it was scheduled, ISO 8601 UTC.
    pub created: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub cancelled: bool,
}

/// A recorded fire: the task's parts at its slot, the previous slot it
/// fired for, and whether its signal run exists yet.
#[derive(Debug, Clone, PartialEq)]
pub struct Fire {
    pub id: i64,
    pub task_id: i64,
    pub run_id: String,
    pub slot: DateTime<Utc>,
    pub previous: Option<DateTime<Utc>>,
    pub cron: String,
    pub recurring: bool,
    pub prompt: String,
    pub source: Option<String>,
    pub workflow: Option<String>,
    pub pending: bool,
}

/// The agent's side of the schedule: fires and task ids (agent.db, or
/// memory for tools that keep nothing).
#[async_trait]
pub trait FireStore: Send + Sync {
    /// The fire of trigger run `run_id`: inserted once, `pending`, with the
    /// slot of the task's latest earlier fire as `previous` (decision 6);
    /// the row already there for a run recorded before (decision 3).
    async fn record(&self, run_id: &str, task: &Task, slot_ms: i64) -> anyhow::Result<i64>;
    /// The oldest fire whose signal has not run yet.
    async fn next_pending(&self) -> anyhow::Result<Option<Fire>>;
    async fn done(&self, id: i64) -> anyhow::Result<()>;
    /// The task's latest fire (by slot).
    async fn latest(&self, task_id: i64) -> anyhow::Result<Option<Fire>>;
    /// A new task id, never one used before.
    async fn next_task_id(&self) -> anyhow::Result<i64>;
}

/// Fire ids continue above any signal id MCP handed out (migration 0005), so
/// a fire's trace and run (`scheduler:<id>`, `signal:<id>`) never land on an
/// older signal's; the in-memory store starts there too.
pub const FIRST_FIRE_ID: i64 = 1_000_000_000;

fn from_ms(ms: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(ms).single().unwrap_or_default()
}

const FIRE_COLUMNS: &str = "id, task_id, run_id, slot, previous, cron, recurring, prompt, source, workflow, state";

fn fire_row(r: &rusqlite::Row) -> rusqlite::Result<Fire> {
    let state: String = r.get(10)?;
    Ok(Fire {
        id: r.get(0)?,
        task_id: r.get(1)?,
        run_id: r.get::<_, Option<String>>(2)?.unwrap_or_default(),
        slot: from_ms(r.get(3)?),
        previous: r.get::<_, Option<i64>>(4)?.map(from_ms),
        cron: r.get::<_, Option<String>>(5)?.unwrap_or_default(),
        recurring: r.get::<_, Option<bool>>(6)?.unwrap_or_default(),
        prompt: r.get::<_, Option<String>>(7)?.unwrap_or_default(),
        source: r.get(8)?,
        workflow: r.get(9)?,
        pending: state == "pending",
    })
}

// Writes go through `AgentDb::write`: `fire_scheduled` runs inside the
// engine's drive, and a write may wait for SQLite's lock (runs.rs §2).
#[async_trait]
impl FireStore for AgentDb {
    async fn record(&self, run_id: &str, task: &Task, slot_ms: i64) -> anyhow::Result<i64> {
        let (run_id, task) = (run_id.to_owned(), task.clone());
        self.write(move |db| {
            let mut conn = db.conn();
            let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let existing: Option<i64> =
                tx.query_row("SELECT id FROM scheduled_fires WHERE run_id = ?1", [&run_id], |r| r.get(0)).optional()?;
            if let Some(id) = existing {
                return Ok(id);
            }
            let previous: Option<i64> = tx.query_row(
                "SELECT max(slot) FROM scheduled_fires WHERE task_id = ?1 AND slot < ?2",
                params![task.id, slot_ms],
                |r| r.get(0),
            )?;
            let id = tx.query_row(
                "INSERT INTO scheduled_fires
                   (task_id, slot, run_id, previous, cron, recurring, prompt, source, workflow, state)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'pending') RETURNING id",
                params![
                    task.id,
                    slot_ms,
                    run_id,
                    previous,
                    task.cron,
                    task.recurring,
                    task.prompt,
                    task.source,
                    task.workflow
                ],
                |r| r.get(0),
            )?;
            tx.commit()?;
            Ok(id)
        })
        .await
    }

    async fn next_pending(&self) -> anyhow::Result<Option<Fire>> {
        Ok(self
            .read()
            .query_row(
                &format!("SELECT {FIRE_COLUMNS} FROM scheduled_fires WHERE state = 'pending' ORDER BY id LIMIT 1"),
                [],
                fire_row,
            )
            .optional()?)
    }

    async fn done(&self, id: i64) -> anyhow::Result<()> {
        self.write(move |db| {
            db.conn().execute("UPDATE scheduled_fires SET state = 'done' WHERE id = ?1", [id])?;
            Ok(())
        })
        .await
    }

    async fn latest(&self, task_id: i64) -> anyhow::Result<Option<Fire>> {
        Ok(self
            .read()
            .query_row(
                &format!(
                    "SELECT {FIRE_COLUMNS} FROM scheduled_fires WHERE task_id = ?1 ORDER BY slot DESC, id DESC LIMIT 1"
                ),
                [task_id],
                fire_row,
            )
            .optional()?)
    }

    async fn next_task_id(&self) -> anyhow::Result<i64> {
        self.write(|db| {
            Ok(db.conn().query_row("INSERT INTO task_ids DEFAULT VALUES RETURNING id", [], |r| r.get(0))?)
        })
        .await
    }
}

/// The same contract without a database.
#[derive(Default)]
pub struct MemoryFires(Mutex<MemoryState>);

#[derive(Default)]
struct MemoryState {
    fires: Vec<Fire>,
    tasks: i64,
}

#[async_trait]
impl FireStore for MemoryFires {
    async fn record(&self, run_id: &str, task: &Task, slot_ms: i64) -> anyhow::Result<i64> {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(fire) = state.fires.iter().find(|f| f.run_id == run_id) {
            return Ok(fire.id);
        }
        let slot = from_ms(slot_ms);
        let previous = state.fires.iter().filter(|f| f.task_id == task.id && f.slot < slot).map(|f| f.slot).max();
        let id = FIRST_FIRE_ID + state.fires.len() as i64 + 1;
        state.fires.push(Fire {
            id,
            task_id: task.id,
            run_id: run_id.into(),
            slot,
            previous,
            cron: task.cron.clone(),
            recurring: task.recurring,
            prompt: task.prompt.clone(),
            source: task.source.clone(),
            workflow: task.workflow.clone(),
            pending: true,
        });
        Ok(id)
    }
    async fn next_pending(&self) -> anyhow::Result<Option<Fire>> {
        Ok(self.0.lock().unwrap_or_else(|e| e.into_inner()).fires.iter().find(|f| f.pending).cloned())
    }
    async fn done(&self, id: i64) -> anyhow::Result<()> {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(fire) = state.fires.iter_mut().find(|f| f.id == id) {
            fire.pending = false;
        }
        Ok(())
    }
    async fn latest(&self, task_id: i64) -> anyhow::Result<Option<Fire>> {
        let state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.fires.iter().filter(|f| f.task_id == task_id).max_by_key(|f| (f.slot, f.id)).cloned())
    }
    async fn next_task_id(&self) -> anyhow::Result<i64> {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        state.tasks += 1;
        Ok(state.tasks)
    }
}

/// The schedule's seam (`EngineDeps::schedule`, R2-L3): the fire table, the
/// one mutex every `task-` write holds (a cancel and a re-save never
/// interleave, decision 9) and the drain's wake-up. The runtime is the
/// engine's: the tools, the drain and the watch take `&Engine`.
pub struct Schedules {
    pub fires: Arc<dyn FireStore>,
    writes: tokio::sync::Mutex<()>,
    wake: tokio::sync::Notify,
}

impl Schedules {
    pub fn new(fires: Arc<dyn FireStore>) -> Self {
        Self { fires, writes: Default::default(), wake: Default::default() }
    }
    pub fn in_memory() -> Self {
        Self::new(Arc::new(MemoryFires::default()))
    }
    /// Wakes the drain: `notify_one` keeps the permit when nobody waits, so
    /// a fire recorded mid-round is never missed (R2-L6).
    pub fn wake(&self) {
        self.wake.notify_one();
    }
    async fn woken(&self) {
        self.wake.notified().await
    }
}

// ── 2. task definitions ────────────────────────────────────────────────────

pub const TASK_PREFIX: &str = "task-";

pub fn task_name(id: i64) -> String {
    format!("{TASK_PREFIX}{id}")
}

/// `cron_expr: "now"`: a one-shot due at once.
pub const NOW: &str = "now";

const DEFAULT_SOURCE: &str = "scheduler";

/// How a task's definition fires.
#[derive(Debug, Clone, PartialEq)]
pub enum Trigger {
    /// Its cron on `zone`'s wall clock.
    Cron(Tz),
    /// Once, at this instant.
    At(DateTime<FixedOffset>),
    /// Never again: a version without a schedule removes the triggers.
    Cancelled,
}

/// The definition of `task`, built as a value and serialized (JSON text is
/// YAML), never formatted: a cron like `0 9 * * 1,3` or a prompt with quotes
/// and newlines round-trips (R2-L4). `breaker: off`: a passing agent.db
/// fault never stops a task for good (decision 7).
pub fn definition(task: &Task, trigger: &Trigger) -> String {
    let mut ouro = Map::new();
    let mut root = Map::new();
    let fire = json!([{"fire": {"call": "fire_scheduled", "with": {"slot": "${ $run.schedule.slot.iso8601 }"}}}]);
    let tasks = match trigger {
        Trigger::Cron(zone) => {
            ouro.insert("timezone".into(), json!(zone.name()));
            ouro.insert("breaker".into(), json!("off"));
            root.insert("schedule".into(), json!({"cron": task.cron}));
            fire
        }
        Trigger::At(at) => {
            ouro.insert("at".into(), json!(at.to_rfc3339()));
            ouro.insert("breaker".into(), json!("off"));
            fire
        }
        Trigger::Cancelled => json!([{"cancelled": {"set": {"cancelled": true}}}]),
    };
    let mut metadata = Map::new();
    if !ouro.is_empty() {
        metadata.insert("ouro".into(), Value::Object(ouro));
    }
    metadata.insert("task".into(), json!(task));
    let mut document = Map::new();
    document.insert(
        "document".into(),
        json!({"dsl": "1.0.4", "namespace": "agent", "name": task_name(task.id), "version": "1.0.0",
               "metadata": metadata}),
    );
    document.extend(root);
    document.insert("do".into(), tasks);
    serde_json::to_string_pretty(&Value::Object(document)).unwrap_or_default()
}

fn metadata_of(yaml: &str) -> Option<Value> {
    let wf = ouro_runtime::ows::model::parse_yaml(yaml).ok()?;
    Some(crate::flows::workflow_metadata(&wf).clone())
}

/// The task block of a task's definition.
pub fn task_of(yaml: &str) -> Option<Task> {
    serde_json::from_value(metadata_of(yaml)?.get("task")?.clone()).ok()
}

// The zone a definition's cron runs on.
fn zone_of(yaml: &str) -> Option<String> {
    metadata_of(yaml)?["ouro"]["timezone"].as_str().map(str::to_owned)
}

// croner also reads 6-field expressions, as ones with a seconds field, so a
// "with seconds" cron would fire on a schedule nobody wrote. Only the
// documented 5 fields.
fn field_count_error(cron_expr: &str) -> Option<String> {
    let fields = cron_expr.split_whitespace().count();
    (fields != 5).then(|| format!("expected 5 fields (minute hour day-of-month month day-of-week), got {fields}"))
}

/// The engine's rule (ouro `schedule.rs`, `next_cron`): croner on `zone`'s
/// wall clock, the same crate and version, so a one-shot and the same
/// recurring task agree (decision 11).
pub fn next_slot(cron: &Cron, zone: Tz, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
    cron.find_next_occurrence(&after.with_timezone(&zone), false).ok().map(|t| t.with_timezone(&Utc))
}

/// A cron as a task takes it: 5 fields, parsed, and matching a date that
/// exists after `now`.
pub fn parse_cron(cron_expr: &str, zone: Tz, now: DateTime<Utc>) -> Result<(Cron, DateTime<Utc>), String> {
    if let Some(error) = field_count_error(cron_expr) {
        return Err(format!("Invalid cron expression: {error}"));
    }
    let cron: Cron = cron_expr.parse().map_err(|e| format!("Invalid cron expression: {e}"))?;
    // croner accepts a day that never comes (Nov 31); such a task would
    // never fire.
    let next = next_slot(&cron, zone, now)
        .ok_or_else(|| "Invalid cron expression: it never matches a date that exists".to_owned())?;
    Ok((cron, next))
}

/// A non-recurring cron's instant: its next slot after `now` on `zone`'s
/// wall clock, with the zone's offset then.
pub fn one_shot_at(cron_expr: &str, zone: Tz, now: DateTime<Utc>) -> Result<DateTime<FixedOffset>, String> {
    let (_, next) = parse_cron(cron_expr, zone, now)?;
    Ok(next.with_timezone(&zone).fixed_offset())
}

// Saves `yaml` as a version of `name` (the version with the same content
// when there is one) and activates it.
async fn put(engine: &Engine, name: &str, yaml: &str, note: &str) -> anyhow::Result<u64> {
    let digest = ouro_runtime::durable::digest_of(yaml);
    let definitions = engine.runtime.definitions(name).await?;
    let version = match definitions.versions.values().find(|d| d.digest == digest) {
        Some(d) => d.version,
        None => engine.runtime.save(name, yaml).await?.version,
    };
    engine.runtime.activate_as(name, version, "agent", Some(note)).await?;
    Ok(version)
}

// A one-shot whose `at` slot was taken: its trigger has no due time left
// (one definition for the list, cancel and the cap, R2-L7).
fn consumed(summary: &DefinitionSummary) -> bool {
    summary.schedules.iter().any(|s| s.kind == ScheduleKind::At && s.next_due_ms.is_none() && s.queued.is_none())
}

// A task the engine still holds triggers for: its active version, its task
// block and the control summary.
struct Held {
    task: Task,
    yaml: String,
    summary: DefinitionSummary,
}

// Every `task-` definition with a schedule (a cancelled one has none), by id.
async fn held(engine: &Engine) -> anyhow::Result<Vec<Held>> {
    let mut summaries = Vec::new();
    let mut cursor = None;
    loop {
        let page = engine.control.definitions(cursor.as_ref(), 1_000).await.map_err(anyhow::Error::from)?;
        summaries.extend(page.items.into_iter().filter(|d| d.name.starts_with(TASK_PREFIX) && !d.schedules.is_empty()));
        match page.next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    let mut out = Vec::new();
    for summary in summaries {
        let definitions = engine.runtime.definitions(&summary.name).await?;
        let Some(d) = definitions.active.and_then(|v| definitions.versions.get(&v)) else { continue };
        let Some(task) = task_of(&d.yaml) else { continue };
        out.push(Held { task, yaml: d.yaml.clone(), summary });
    }
    out.sort_by_key(|h| h.task.id);
    Ok(out)
}

fn now(engine: &Engine) -> DateTime<Utc> {
    from_ms(engine.clock.now_ms())
}

// ── 3. the drain and the zone watch ────────────────────────────────────────

/// A fire as the signal it runs as (decisions 2 and 4): the header source
/// skills parse, then the prompt verbatim; `Now` is the drain's clock.
pub fn signal_of(fire: &Fire, env_context: Option<&str>, now: DateTime<Utc>) -> PendingSignal {
    let never = "never (this is the first run)".to_owned();
    let content = [
        format!("Scheduled task #{} fired.", fire.task_id),
        format!("Cron: {}", fire.cron),
        format!("Slot: {}", iso(fire.slot)),
        format!("Now: {}", iso(now)),
        format!("Previous fire: {}", fire.previous.map(iso).unwrap_or(never)),
        format!("Recurring: {}", if fire.recurring { "yes" } else { "no (one-shot)" }),
        String::new(),
        fire.prompt.clone(),
    ]
    .join("\n");
    PendingSignal {
        id: fire.id,
        source: fire.source.clone().unwrap_or_else(|| DEFAULT_SOURCE.into()),
        content,
        env_context: env_context.map(str::to_owned),
        created_at: now.format("%Y-%m-%d %H:%M:%S").to_string(),
        payload: fire.workflow.as_ref().map(|workflow| json!({ "workflow": workflow })),
    }
}

/// How a drain round ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoundEnd {
    /// No fire is pending.
    Empty,
    /// The supervisor is stopping: nothing of the fire ran.
    Stopping,
    /// Fire `id`'s signal run was not admitted; it stays pending.
    Pending(i64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Drained {
    /// Fires whose signal ran in this round.
    pub ran: usize,
    pub end: RoundEnd,
}

/// One round of the drain: each pending fire, oldest first, by decision 3's
/// one rule. A fire whose signal run `signal:<id>` exists is done (the
/// engine resumes that run: `run_active` never enters twice for one trace);
/// otherwise the supervisor starts it, and it is done once its run exists (a
/// faulted signal too: its catch reported it). No run after the start
/// (stopping, an admission error) leaves it pending and ends the round.
pub async fn drain(supervisor: &Arc<Supervisor>, env_context: Option<&str>) -> anyhow::Result<Drained> {
    let engine = &supervisor.engine;
    let fires = &engine.deps.schedule.fires;
    let mut ran = 0;
    loop {
        let Some(fire) = fires.next_pending().await? else { return Ok(Drained { ran, end: RoundEnd::Empty }) };
        let run = crate::functions::signal_run(fire.id);
        if engine.runtime.get(&run).await?.is_some() {
            fires.done(fire.id).await?;
            continue;
        }
        let signal = signal_of(&fire, env_context, now(engine));
        let Some(result) = supervisor.start(&signal).await else {
            return Ok(Drained { ran, end: RoundEnd::Stopping });
        };
        if let Err(error) = &result {
            eprintln!("[schedule] fire {} of task {}: {error:#}", fire.id, fire.task_id);
        }
        if engine.runtime.get(&run).await?.is_none() {
            return Ok(Drained { ran, end: RoundEnd::Pending(fire.id) });
        }
        fires.done(fire.id).await?;
        ran += 1;
    }
}

const DRAIN_ROUND: Duration = Duration::from_secs(30);
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(300);

/// The drain (spawned by main.rs unless `--once`): a round at once, then on
/// every recorded fire or every 30 s; a fire left pending is retried after a
/// backoff of 1 s doubling to five minutes (in memory).
pub async fn run_drain(supervisor: Arc<Supervisor>, env_context: Option<String>, cancel: CancellationToken) {
    let schedules = supervisor.engine.deps.schedule.clone();
    let mut backoff = BACKOFF_MIN;
    loop {
        let failed = match drain(&supervisor, env_context.as_deref()).await {
            Ok(Drained { end: RoundEnd::Stopping, .. }) => return,
            Ok(Drained { end: RoundEnd::Pending(id), .. }) => {
                eprintln!("[schedule] fire {id} was not admitted; retried in {backoff:?}");
                true
            }
            Ok(_) => false,
            Err(error) => {
                eprintln!("[schedule] drain failed, retried in {backoff:?}: {error:#}");
                true
            }
        };
        if failed {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(backoff) => {}
            }
            backoff = (backoff * 2).min(BACKOFF_MAX);
            continue;
        }
        backoff = BACKOFF_MIN;
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = schedules.woken() => {}
            _ = tokio::time::sleep(DRAIN_ROUND) => {}
        }
    }
}

/// One round of the zone watch (decision 9): every live recurring task
/// (active, not cancelled, not set aside) whose stored zone is not `zone`
/// gets a version on `zone`, activated. Idempotent by construction: an
/// unchanged zone saves nothing. Returns how many it moved.
pub async fn zone_round(engine: &Engine, zone: Tz) -> anyhow::Result<usize> {
    let _writing = engine.deps.schedule.writes.lock().await;
    let mut moved = 0;
    for held in held(engine).await? {
        let Held { task, yaml, summary } = held;
        if !task.recurring || task.cancelled || summary.tripped.is_some() {
            continue;
        }
        let Some(old) = zone_of(&yaml).filter(|z| z != zone.name()) else { continue };
        let task = Task { timezone: zone.name().into(), ..task };
        let yaml = definition(&task, &Trigger::Cron(zone));
        put(engine, &summary.name, &yaml, &format!("time zone {old} → {}", zone.name())).await?;
        moved += 1;
    }
    Ok(moved)
}

/// The user's zone from MCP's settings; `None` when unreadable.
pub async fn user_zone(engine: &Engine, cancel: &CancellationToken) -> Option<Tz> {
    let raw = engine.deps.mcp.call_tool("get_timezone", Map::new(), cancel).await.ok()?;
    serde_json::from_str::<Value>(&raw).ok()?["timezone"].as_str()?.parse().ok()
}

const ZONE_WATCH: Duration = Duration::from_secs(60);

/// The zone watch (spawned by main.rs unless `--once`): every 60 s; an
/// unreadable zone skips the round.
pub async fn run_zone_watch(engine: Arc<Engine>, cancel: CancellationToken) {
    let mut interval = tokio::time::interval(ZONE_WATCH);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = interval.tick() => {}
        }
        let Some(zone) = user_zone(&engine, &cancel).await else { continue };
        if let Err(error) = zone_round(&engine, zone).await {
            eprintln!("[schedule] zone watch: {error:#}");
        }
    }
}

// ── 4. tools ───────────────────────────────────────────────────────────────

// Synthetic tools of the agent loop.
pub const TOOLS: &[&str] = &["schedule_task", "list_scheduled_tasks", "cancel_scheduled_task"];

/// The most live tasks (not cancelled, not consumed) the agent keeps
/// (decision 15, raised to 100 by the maintainer): the list and the zone
/// watch stay small. Reversible.
pub const MAX_LIVE_TASKS: usize = 100;

fn failure(error: impl Into<String>) -> anyhow::Result<String> {
    Ok(json!({"ok": false, "error": error.into()}).to_string())
}

// The ISO time of a task's next due slot, from the engine's summary.
fn next_due(summary: &DefinitionSummary) -> Option<String> {
    summary.schedules.iter().filter_map(|s| s.next_due_ms).min().map(|ms| iso(from_ms(ms)))
}

// The error of the latest faulted run of `name` (`None` in a dry run: its
// store reads no runs).
async fn last_error(engine: &Engine, name: &str) -> Option<String> {
    if engine.sandbox.is_some() {
        return None;
    }
    let filter = ouro_runtime::durable::control::Runs {
        definition: Some(name.into()),
        state: Some(ouro_runtime::durable::RunState::Failed),
        ..Default::default()
    };
    let page = engine.control.list(&filter, None, 1_000).await.ok()?;
    let run = page.items.into_iter().max_by_key(|r| r.ended_ms)?;
    match run.outcome? {
        crate::runs::Status::Faulted(error) => Some(error.detail.clone().unwrap_or_else(|| error.to_string())),
        _ => None,
    }
}

/// The schedule's synthetic tools; `tz` is the session's zone (the user's).
pub async fn tool(engine: &Engine, tz: &str, name: &str, args: &Map<String, Value>) -> anyhow::Result<String> {
    let zone: Tz = tz.parse().unwrap_or(Tz::UTC);
    let now = now(engine);
    let schedules = &engine.deps.schedule;
    match name {
        "schedule_task" => {
            let text = |key: &str| args.get(key).and_then(Value::as_str).unwrap_or_default().to_owned();
            let (cron, prompt) = (text("cron_expr"), text("prompt"));
            let recurring =
                args.get("recurring").and_then(Value::as_bool).ok_or_else(|| anyhow::anyhow!("recurring: Required"))?;
            anyhow::ensure!(!cron.is_empty() && !prompt.is_empty(), "cron_expr and prompt must be non-empty");
            let workflow = args.get("workflow").and_then(Value::as_str).filter(|w| !w.is_empty()).map(str::to_owned);
            if let Some(workflow) = &workflow {
                if crate::flows::reserved(workflow) {
                    return failure(format!("`{workflow}` is a reserved name, not a saved workflow a task can run"));
                }
                if engine.runtime.definitions(workflow).await?.active.is_none() {
                    return failure(format!(
                        "workflow `{workflow}` has no active version; save, try and activate it first"
                    ));
                }
            }
            // `now` runs once, whatever `recurring` says.
            let (recurring, trigger) = if cron == NOW {
                (false, Trigger::At(now.fixed_offset()))
            } else {
                let next = match parse_cron(&cron, zone, now) {
                    Ok((_, next)) => next,
                    Err(error) => return failure(error),
                };
                let trigger =
                    if recurring { Trigger::Cron(zone) } else { Trigger::At(next.with_timezone(&zone).fixed_offset()) };
                (recurring, trigger)
            };
            let _writing = schedules.writes.lock().await;
            let live = held(engine).await?.iter().filter(|h| !consumed(&h.summary)).count();
            if live >= MAX_LIVE_TASKS {
                return failure(format!(
                    "{live} tasks are scheduled, the most the agent keeps; cancel one first (list_scheduled_tasks)"
                ));
            }
            let id = schedules.fires.next_task_id().await?;
            let task = Task {
                id,
                cron: cron.clone(),
                recurring,
                prompt: prompt.clone(),
                source: None,
                workflow: workflow.clone(),
                timezone: zone.name().into(),
                created: iso(now),
                cancelled: false,
            };
            let name = task_name(id);
            if let Err(error) = put(engine, &name, &definition(&task, &trigger), "scheduled").await {
                return failure(format!("{error:#}"));
            }
            let summary = engine.control.definition(&name).await.map_err(anyhow::Error::from)?;
            Ok(json!({
                "ok": true,
                "task": {"id": id, "cron_expr": cron, "recurring": recurring, "prompt": prompt,
                         "workflow": workflow, "timezone": zone.name()},
                "next_fire": summary.as_ref().and_then(next_due),
            })
            .to_string())
        }
        "list_scheduled_tasks" => {
            let mut tasks = Vec::new();
            for Held { task, summary, .. } in held(engine).await? {
                let latest = schedules.fires.latest(task.id).await?;
                let consumed = consumed(&summary);
                // A one-shot that fired is listed until its signal ran.
                if consumed && !latest.as_ref().is_some_and(|f| f.pending) {
                    continue;
                }
                let mut listed = json!({
                    "id": task.id, "cron_expr": task.cron, "recurring": task.recurring, "prompt": task.prompt,
                    "source": task.source, "workflow": task.workflow, "timezone": task.timezone,
                    "created_at": task.created, "next_fire": next_due(&summary),
                    "last_fire": latest.as_ref().map(|f| iso(f.slot)),
                });
                if consumed {
                    listed["consumed"] = json!(true);
                }
                if summary.consecutive_faults > 0 {
                    listed["faults"] = json!(summary.consecutive_faults);
                    listed["last_error"] = json!(last_error(engine, &summary.name).await);
                }
                if let Some(trip) = &summary.tripped {
                    listed["paused"] = json!(trip.manual.as_ref().map(|m| m.note.clone()).unwrap_or_default());
                }
                tasks.push(listed);
            }
            Ok(json!({"timezone": zone.name(), "count": tasks.len(), "tasks": tasks}).to_string())
        }
        "cancel_scheduled_task" => {
            let id = args.get("id").and_then(Value::as_i64).filter(|id| *id >= 1);
            let id = id.ok_or_else(|| anyhow::anyhow!("id must be a positive integer"))?;
            let refused = |error: &str| Ok(json!({"ok": false, "id": id, "error": error}).to_string());
            let _writing = schedules.writes.lock().await;
            let name = task_name(id);
            let definitions = engine.runtime.definitions(&name).await?;
            let Some(d) = definitions.active.and_then(|v| definitions.versions.get(&v)) else {
                return refused("no such task");
            };
            let Some(task) = task_of(&d.yaml) else { return refused("no such task") };
            if task.cancelled {
                return refused("the task is already cancelled");
            }
            let summary = engine.control.definition(&name).await.map_err(anyhow::Error::from)?;
            if summary.as_ref().is_some_and(consumed) {
                return refused("the one-shot task already fired");
            }
            let task = Task { cancelled: true, ..task };
            put(engine, &name, &definition(&task, &Trigger::Cancelled), "cancelled").await?;
            Ok(json!({"ok": true, "id": id}).to_string())
        }
        _ => anyhow::bail!("unknown schedule tool {name}"),
    }
}

// ── 5. fire_scheduled ──────────────────────────────────────────────────────

/// Why `fire_scheduled` recorded nothing.
#[derive(Debug)]
pub enum FireFailure {
    /// agent.db failed: the call is interrupted and resent (decision 7).
    Storage(anyhow::Error),
    /// The run's definition is not a task's: a definite failure.
    Invalid(String),
}

/// What a trigger's run of a task records (decisions 2–5): one fire per run
/// id, read from the run's admitted definition, never from an expression.
/// Only a trigger's root run of a `task-` definition records; anything else
/// (a child a workflow started) completes refused, and a run of a task
/// cancelled since its admission records nothing.
pub async fn fire(engine: &Engine, run: &SavedRun, slot: &Value) -> Result<Value, FireFailure> {
    let name = &run.admission.name;
    let trigger_root = run.child.is_none()
        && name.starts_with(TASK_PREFIX)
        && run.admission.id.starts_with(&format!("schedule:{TASK_PREFIX}"));
    if !trigger_root {
        return Ok(json!({"fire": null, "refused": "not a trigger's run"}));
    }
    let definitions = engine.runtime.definitions(name).await.map_err(FireFailure::Storage)?;
    let active = definitions.active.and_then(|v| definitions.versions.get(&v)).and_then(|d| task_of(&d.yaml));
    if active.is_none_or(|t| t.cancelled) {
        return Ok(json!({"fire": null, "cancelled": true}));
    }
    let task = task_of(&run.admission.definition)
        .ok_or_else(|| FireFailure::Invalid(format!("{name} carries no task block")))?;
    let slot = slot
        .as_str()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .ok_or_else(|| FireFailure::Invalid(format!("slot must be an RFC 3339 time, got {slot}")))?;
    let schedules = &engine.deps.schedule;
    let id = schedules
        .fires
        .record(&run.admission.id, &task, slot.timestamp_millis())
        .await
        .map_err(FireFailure::Storage)?;
    schedules.wake();
    Ok(json!({"fire": id}))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test 14 (DST): a non-recurring `30 2 * * *` in Berlin created the day
    // before the 2027 spring-forward fires at the first instant after the
    // gap, 03:00 CEST, by the engine's rule (the tick's old rule gave 03:30).
    #[test]
    fn a_one_shot_in_a_skipped_hour_takes_the_engines_rule() {
        let now = DateTime::parse_from_rfc3339("2027-03-27T12:00:00Z").unwrap().with_timezone(&Utc);
        let at = one_shot_at("30 2 * * *", chrono_tz::Europe::Berlin, now).unwrap();
        assert_eq!(at.to_rfc3339(), "2027-03-28T03:00:00+02:00");
    }
}
