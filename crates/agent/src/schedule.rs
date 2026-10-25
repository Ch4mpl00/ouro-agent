// Scheduled tasks: the agent's own clock. Cron rows in agent.db, fired by a
// 30 s tick inside the supervisor process; a fire runs as a `scheduler` (or
// the task's own source) signal, and a task naming a saved workflow runs it.
//
// Sections:
//   1. storage — task rows and fires, on agent.db and in memory
//   2. cron    — parse + next-slot math on the user's wall clock
//   3. tick    — due tasks → fires, claimed once per slot; the loop
//   4. tools   — schedule_task / list_scheduled_tasks / cancel_scheduled_task
//   5. boot    — first start: import MCP's tasks, or seed the system ones
//
// A slot is claimed (compare-and-swap on `last_run_at`) before its fire runs:
// at most once. A fire that never started (the supervisor was stopping) gives
// its slot back; one that started is never repeated, even if it failed or the
// process died under it — a run may already have sent something.
use crate::{
    db::AgentDb,
    mcp::Mcp,
    session::iso,
    supervisor::{PendingSignal, Supervisor},
};
use async_trait::async_trait;
use chrono::{DateTime, Offset, TimeZone, Utc};
use chrono_tz::Tz;
use croner::Cron;
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

// ── 1. storage ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TaskRow {
    pub id: i64,
    pub cron_expr: String,
    // 0 | 1 — schedule_task has always returned the raw row.
    pub recurring: i64,
    pub prompt: String,
    // None → signal source 'scheduler' (user-created).
    pub source: Option<String>,
    // A saved workflow the fire runs instead of the primary agent loop.
    pub workflow: Option<String>,
    // Unix seconds of the slot last fired for.
    pub last_run_at: Option<i64>,
    // "YYYY-MM-DD HH:MM:SS", UTC.
    pub created_at: String,
    #[serde(skip)]
    pub created: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewTask {
    pub cron_expr: String,
    pub recurring: bool,
    pub prompt: String,
    pub source: Option<String>,
    pub workflow: Option<String>,
    pub last_run_at: Option<i64>,
    pub created: DateTime<Utc>,
}

#[async_trait]
pub trait ScheduleStore: Send + Sync {
    async fn insert(&self, task: &NewTask) -> anyhow::Result<TaskRow>;
    // Tasks that may still fire: every recurring task, plus one-shots that
    // have not fired yet.
    async fn list_active(&self) -> anyhow::Result<Vec<TaskRow>>;
    // Moves last_run_at from `expected` to `next` only while it still is
    // `expected` and the task still exists. Stamping a one-shot retires it.
    async fn swap_last_run(&self, id: i64, expected: Option<i64>, next: Option<i64>) -> anyhow::Result<bool>;
    async fn delete(&self, id: i64) -> anyhow::Result<bool>;
    // Logs a claimed slot; the id names the fire's signal and trace.
    async fn record_fire(&self, task_id: i64, slot: i64) -> anyhow::Result<i64>;
    // Stores the first boot's tasks, once: false (and nothing stored) when
    // the schedule was initialized before.
    async fn initialize(&self, tasks: &[NewTask]) -> anyhow::Result<bool>;
}

fn sql_time(t: DateTime<Utc>) -> String {
    t.format("%Y-%m-%d %H:%M:%S").to_string()
}

fn from_ms(ms: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(ms).single().unwrap_or_default()
}

const COLUMNS: &str = "id, cron_expr, recurring, prompt, source, workflow, last_run_at, created_ms";
const INITIALIZED: &str = "schedule.initialized";

fn task_row(r: &rusqlite::Row) -> rusqlite::Result<TaskRow> {
    let created = from_ms(r.get(7)?);
    Ok(TaskRow {
        id: r.get(0)?,
        cron_expr: r.get(1)?,
        recurring: r.get(2)?,
        prompt: r.get(3)?,
        source: r.get(4)?,
        workflow: r.get(5)?,
        last_run_at: r.get(6)?,
        created_at: sql_time(created),
        created,
    })
}

fn insert_task(conn: &rusqlite::Connection, t: &NewTask) -> rusqlite::Result<TaskRow> {
    conn.query_row(
        &format!(
            "INSERT INTO scheduled_tasks (cron_expr, recurring, prompt, source, workflow, last_run_at, created_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) RETURNING {COLUMNS}"
        ),
        params![
            t.cron_expr,
            i64::from(t.recurring),
            t.prompt,
            t.source,
            t.workflow,
            t.last_run_at,
            t.created.timestamp_millis()
        ],
        task_row,
    )
}

#[async_trait]
impl ScheduleStore for AgentDb {
    async fn insert(&self, task: &NewTask) -> anyhow::Result<TaskRow> {
        Ok(insert_task(&self.conn(), task)?)
    }

    async fn list_active(&self) -> anyhow::Result<Vec<TaskRow>> {
        let conn = self.conn();
        let mut statement = conn.prepare(&format!(
            "SELECT {COLUMNS} FROM scheduled_tasks WHERE recurring = 1 OR last_run_at IS NULL ORDER BY id"
        ))?;
        Ok(statement.query_map([], task_row)?.collect::<rusqlite::Result<_>>()?)
    }

    async fn swap_last_run(&self, id: i64, expected: Option<i64>, next: Option<i64>) -> anyhow::Result<bool> {
        let changed = self.conn().execute(
            "UPDATE scheduled_tasks SET last_run_at = ?1 WHERE id = ?2 AND last_run_at IS ?3",
            params![next, id, expected],
        )?;
        Ok(changed > 0)
    }

    async fn delete(&self, id: i64) -> anyhow::Result<bool> {
        Ok(self.conn().execute("DELETE FROM scheduled_tasks WHERE id = ?1", params![id])? > 0)
    }

    async fn record_fire(&self, task_id: i64, slot: i64) -> anyhow::Result<i64> {
        Ok(self.conn().query_row(
            "INSERT INTO scheduled_fires (task_id, slot) VALUES (?1, ?2) RETURNING id",
            params![task_id, slot],
            |r| r.get(0),
        )?)
    }

    async fn initialize(&self, tasks: &[NewTask]) -> anyhow::Result<bool> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let done = tx.query_row("SELECT 1 FROM memory WHERE key = ?1", params![INITIALIZED], |_| Ok(())).optional()?;
        if done.is_some() {
            return Ok(false);
        }
        for task in tasks {
            insert_task(&tx, task)?;
        }
        tx.execute("INSERT INTO memory (key, value) VALUES (?1, '1')", params![INITIALIZED])?;
        tx.commit()?;
        Ok(true)
    }
}

// The same contract without a database, for tests and tools that need none.
#[derive(Default)]
pub struct MemorySchedule(Mutex<MemoryState>);

#[derive(Default)]
struct MemoryState {
    tasks: Vec<TaskRow>,
    next_id: i64,
    fires: i64,
    initialized: bool,
}

impl MemoryState {
    fn insert(&mut self, t: &NewTask) -> TaskRow {
        self.next_id += 1;
        let row = TaskRow {
            id: self.next_id,
            cron_expr: t.cron_expr.clone(),
            recurring: i64::from(t.recurring),
            prompt: t.prompt.clone(),
            source: t.source.clone(),
            workflow: t.workflow.clone(),
            last_run_at: t.last_run_at,
            created_at: sql_time(t.created),
            created: t.created,
        };
        self.tasks.push(row.clone());
        row
    }
}

#[async_trait]
impl ScheduleStore for MemorySchedule {
    async fn insert(&self, task: &NewTask) -> anyhow::Result<TaskRow> {
        Ok(self.0.lock().unwrap_or_else(|e| e.into_inner()).insert(task))
    }

    async fn list_active(&self) -> anyhow::Result<Vec<TaskRow>> {
        let state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.tasks.iter().filter(|t| t.recurring == 1 || t.last_run_at.is_none()).cloned().collect())
    }

    async fn swap_last_run(&self, id: i64, expected: Option<i64>, next: Option<i64>) -> anyhow::Result<bool> {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        match state.tasks.iter_mut().find(|t| t.id == id && t.last_run_at == expected) {
            Some(task) => {
                task.last_run_at = next;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    async fn delete(&self, id: i64) -> anyhow::Result<bool> {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let before = state.tasks.len();
        state.tasks.retain(|t| t.id != id);
        Ok(state.tasks.len() < before)
    }

    async fn record_fire(&self, _: i64, _: i64) -> anyhow::Result<i64> {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        state.fires += 1;
        Ok(FIRST_FIRE_ID + state.fires)
    }

    async fn initialize(&self, tasks: &[NewTask]) -> anyhow::Result<bool> {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if state.initialized {
            return Ok(false);
        }
        for task in tasks {
            state.insert(task);
        }
        state.initialized = true;
        Ok(true)
    }
}

// Fire ids continue above any signal id MCP handed out, so a fire's trace
// (`scheduler:<id>`) never lands on an older signal's (see the migration).
pub const FIRST_FIRE_ID: i64 = 1_000_000_000;

// ── 2. cron ────────────────────────────────────────────────────────────────

// croner's defaults match cron-parser's: 5 fields (an optional leading
// seconds field), and day-of-month OR day-of-week when both are restricted.
pub fn parse_cron(expr: &str) -> Result<Cron, croner::errors::CronError> {
    expr.parse()
}

// First slot strictly after `after`, evaluated on the wall clock of `tz`.
//
// The match is searched on the wall clock as if it never changed, then the
// wall time is placed on the timeline: a time the clock skipped (going
// forward) fires shifted by the size of the jump — 03:30 on a 03:00→04:00
// night fires at 04:30, as the TS server's cron-parser does (croner alone
// fires at 04:00) — and a time the clock shows twice (going back) fires the
// first time only.
fn next_slot(cron: &Cron, tz: Tz, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let mut cursor = after.with_timezone(&tz).naive_local();
    // A wall match whose instant is not after `after` (the first pass of a
    // repeated hour already behind us) moves the search on; a handful of
    // steps always reaches the next real one.
    for _ in 0..8 {
        let wall = cron.find_next_occurrence(&Utc.from_utc_datetime(&cursor), false).ok()?.naive_utc();
        let instant = match tz.from_local_datetime(&wall) {
            chrono::LocalResult::Single(t) => t.with_timezone(&Utc),
            chrono::LocalResult::Ambiguous(first, _) => first.with_timezone(&Utc),
            chrono::LocalResult::None => {
                // Skipped: read the wall time with the offset in force before
                // the jump.
                let before = tz.from_local_datetime(&(wall - chrono::Duration::hours(24))).earliest()?;
                Utc.from_utc_datetime(&(wall - before.offset().fix()))
            }
        };
        if instant > after {
            return Some(instant);
        }
        cursor = wall;
    }
    None
}

fn preview_next_fires(cron: &Cron, tz: Tz, count: usize, now: DateTime<Utc>) -> Vec<String> {
    let mut out = Vec::with_capacity(count);
    let mut cursor = now;
    while out.len() < count {
        let Some(next) = next_slot(cron, tz, cursor) else { break };
        out.push(iso(next));
        cursor = next;
    }
    out
}

// `cron_expr: "now"`: a one-shot due at once, fired by the first tick that
// finds it (after the signal running at the time, like every fire).
pub const NOW: &str = "now";

// The slot a task's next fire is computed from: the slot it last fired for,
// or — never fired — its creation time.
fn anchor(task: &TaskRow) -> DateTime<Utc> {
    match task.last_run_at {
        Some(secs) => Utc.timestamp_opt(secs, 0).single().unwrap_or_default(),
        None => task.created,
    }
}

// ── 3. tick ────────────────────────────────────────────────────────────────

const TICK_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_SIGNAL_SOURCE: &str = "scheduler";

// One claimed slot, ready to run.
#[derive(Debug, Clone)]
pub struct Fire {
    pub id: i64,
    pub task_id: i64,
    pub source: String,
    pub content: String,
    pub workflow: Option<String>,
    pub slot: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fired {
    Started,
    // Nothing ran (the runner was stopping): the slot goes back.
    NotStarted,
}

// What runs a fire: the supervisor, or a test's recorder.
#[async_trait]
pub trait FireSink: Send + Sync {
    async fn fire(&self, fire: Fire) -> Fired;
}

// Fires every task whose next slot (after its anchor) has passed. Restart-safe
// and never double-fires a slot: the anchor is the *slot* fired for, not the
// wall clock, so a late tick still advances to the following slot.
pub async fn tick(store: &dyn ScheduleStore, sink: &dyn FireSink, tz: Tz, now: DateTime<Utc>) -> anyhow::Result<usize> {
    let mut fired = 0;
    for task in store.list_active().await? {
        let slot = if task.cron_expr == NOW {
            // Its slot is its creation: a claimed one retires it.
            task.created
        } else {
            let cron = match parse_cron(&task.cron_expr) {
                Ok(cron) => cron,
                Err(err) => {
                    eprintln!("[schedule] task {} has an invalid cron {:?}, skipping: {err}", task.id, task.cron_expr);
                    continue;
                }
            };
            let Some(slot) = next_slot(&cron, tz, anchor(&task)) else { continue };
            slot
        };
        if slot > now {
            continue;
        }
        // Snapshot before stamping, so skills can scope `since=<previous
        // fire>` from the signal header.
        let previous = task.last_run_at.and_then(|s| Utc.timestamp_opt(s, 0).single()).map(iso);
        // Claimed with a compare-and-set: an overlapping tick that got there
        // first, or a cancel since list_active, means this tick doesn't fire it.
        let slot_unix = slot.timestamp();
        if !store.swap_last_run(task.id, task.last_run_at, Some(slot_unix)).await? {
            continue;
        }
        let id = match store.record_fire(task.id, slot_unix).await {
            Ok(id) => id,
            Err(err) => {
                store.swap_last_run(task.id, Some(slot_unix), task.last_run_at).await?;
                return Err(err);
            }
        };
        let fire = Fire {
            id,
            task_id: task.id,
            source: task.source.clone().unwrap_or_else(|| DEFAULT_SIGNAL_SOURCE.into()),
            content: render_content(&task, previous.as_deref(), slot, now),
            workflow: task.workflow.clone(),
            slot,
        };
        if sink.fire(fire).await == Fired::NotStarted {
            // Give the slot back so a later tick fires it, rather than
            // leaving it marked as done with nothing run.
            store.swap_last_run(task.id, Some(slot_unix), task.last_run_at).await?;
            return Ok(fired);
        }
        fired += 1;
    }
    Ok(fired)
}

// A header with the cron metadata (source skills parse the lines they need),
// then the task's prompt verbatim.
fn render_content(task: &TaskRow, previous: Option<&str>, slot: DateTime<Utc>, now: DateTime<Utc>) -> String {
    [
        format!("Scheduled task #{} fired.", task.id),
        format!("Cron: {}", task.cron_expr),
        format!("Slot: {}", iso(slot)),
        format!("Now: {}", iso(now)),
        format!("Previous fire: {}", previous.unwrap_or("never (this is the first run)")),
        format!("Recurring: {}", if task.recurring == 1 { "yes" } else { "no (one-shot)" }),
        String::new(),
        task.prompt.clone(),
    ]
    .join("\n")
}

// The user's zone, from MCP's settings: read on every tick, so a change takes
// effect on the next one. UTC when unset or unreadable.
async fn timezone(mcp: &dyn Mcp, cancel: &CancellationToken) -> Tz {
    let raw = mcp.call_tool("get_timezone", Map::new(), cancel).await.unwrap_or_default();
    serde_json::from_str::<Value>(&raw)
        .ok()
        .and_then(|v| v["timezone"].as_str().and_then(|z| z.parse().ok()))
        .unwrap_or(Tz::UTC)
}

// Fires run as signals on the supervisor, one at a time with every other
// signal, carrying the environment note a queued signal carries
// (`session::telegram_env_note`).
pub struct SupervisorSink {
    pub supervisor: Arc<Supervisor>,
    pub env_context: Option<String>,
}

#[async_trait]
impl FireSink for SupervisorSink {
    async fn fire(&self, fire: Fire) -> Fired {
        let signal = PendingSignal {
            id: fire.id,
            source: fire.source,
            content: fire.content,
            env_context: self.env_context.clone(),
            created_at: sql_time(Utc::now()),
            payload: fire.workflow.map(|workflow| json!({ "workflow": workflow })),
        };
        match self.supervisor.start(&signal).await {
            None => Fired::NotStarted,
            Some(result) => {
                if let Err(err) = result {
                    eprintln!("[schedule] fire {} of task {}: {err:#}", signal.id, fire.task_id);
                }
                Fired::Started
            }
        }
    }
}

pub async fn run_ticks(
    store: Arc<dyn ScheduleStore>,
    sink: Arc<dyn FireSink>,
    mcp: Arc<dyn Mcp>,
    cancel: CancellationToken,
) {
    let mut interval = tokio::time::interval(TICK_INTERVAL);
    // A tick that overran (a long fire) should not be followed by a burst.
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = interval.tick() => {}
        }
        let tz = timezone(&*mcp, &cancel).await;
        if let Err(err) = tick(&*store, &*sink, tz, Utc::now()).await {
            eprintln!("[schedule] tick failed: {err:#}");
        }
    }
}

// ── 4. tools ───────────────────────────────────────────────────────────────

// Synthetic tools of the agent loop.
pub const TOOLS: &[&str] = &["schedule_task", "list_scheduled_tasks", "cancel_scheduled_task"];

#[derive(Serialize)]
struct Failure {
    ok: bool,
    error: String,
}

fn failure(error: String) -> anyhow::Result<String> {
    Ok(serde_json::to_string(&Failure { ok: false, error })?)
}

#[derive(Serialize)]
struct Scheduled {
    ok: bool,
    task: TaskRow,
    timezone: String,
    upcoming_fires: Vec<String>,
}

#[derive(Serialize)]
struct ListedTask {
    id: i64,
    cron_expr: String,
    recurring: bool,
    prompt: String,
    source: Option<String>,
    workflow: Option<String>,
    last_run_at: Option<i64>,
    created_at: String,
    last_run_at_iso: Option<String>,
    upcoming_fires: Vec<String>,
}

#[derive(Serialize)]
struct Listed {
    timezone: String,
    count: usize,
    tasks: Vec<ListedTask>,
}

// croner also reads 6-field expressions — as ones with a seconds field — so a
// "with seconds" cron would fire on a schedule nobody wrote. Only the
// documented 5 fields.
fn field_count_error(cron_expr: &str) -> Option<String> {
    let fields = cron_expr.split_whitespace().count();
    (fields != 5).then(|| format!("expected 5 fields (minute hour day-of-month month day-of-week), got {fields}"))
}

fn upcoming_count(recurring: bool) -> usize {
    if recurring { 3 } else { 1 }
}

// `tz` is the session's zone (the user's, from MCP).
pub async fn tool(
    store: &dyn ScheduleStore,
    tz: &str,
    name: &str,
    args: &Map<String, Value>,
    now: DateTime<Utc>,
) -> anyhow::Result<String> {
    let tz: Tz = tz.parse().unwrap_or(Tz::UTC);
    match name {
        "schedule_task" => {
            let text = |key: &str| args.get(key).and_then(Value::as_str).unwrap_or_default().to_owned();
            let (cron_expr, prompt) = (text("cron_expr"), text("prompt"));
            let recurring =
                args.get("recurring").and_then(Value::as_bool).ok_or_else(|| anyhow::anyhow!("recurring: Required"))?;
            anyhow::ensure!(!cron_expr.is_empty() && !prompt.is_empty(), "cron_expr and prompt must be non-empty");
            let workflow = args.get("workflow").and_then(Value::as_str).filter(|w| !w.is_empty()).map(str::to_owned);
            // `now` runs once, whatever `recurring` says.
            let (recurring, upcoming_fires) = if cron_expr == NOW {
                (false, vec![iso(now)])
            } else {
                if let Some(error) = field_count_error(&cron_expr) {
                    return failure(format!("Invalid cron expression: {error}"));
                }
                let cron = match parse_cron(&cron_expr) {
                    Ok(cron) => cron,
                    Err(err) => return failure(format!("Invalid cron expression: {err}")),
                };
                // croner accepts a day that never comes (Nov 31) and would store a
                // task that never fires; the TS server's parser refuses it.
                if next_slot(&cron, tz, now).is_none() {
                    return failure("Invalid cron expression: it never matches a date that exists".into());
                }
                (recurring, preview_next_fires(&cron, tz, upcoming_count(recurring), now))
            };
            // Saved workflows run again with the versions and schedules steps
            // of the engine migration (design 03, R2-M5).
            if workflow.is_some() {
                return Ok(crate::flows::unavailable("schedule_task(workflow: …)", "schedules"));
            }
            let task = store
                .insert(&NewTask {
                    cron_expr,
                    recurring,
                    prompt,
                    source: None,
                    workflow,
                    last_run_at: None,
                    created: now,
                })
                .await?;
            Ok(serde_json::to_string(&Scheduled { ok: true, task, timezone: tz.name().to_owned(), upcoming_fires })?)
        }
        "list_scheduled_tasks" => {
            let tasks: Vec<ListedTask> = store
                .list_active()
                .await?
                .into_iter()
                .map(|t| {
                    let recurring = t.recurring == 1;
                    // An invalid cron surfaces as an empty `upcoming_fires`.
                    let upcoming_fires = if t.cron_expr == NOW {
                        vec![iso(now)]
                    } else {
                        parse_cron(&t.cron_expr)
                            .map(|cron| preview_next_fires(&cron, tz, upcoming_count(recurring), now))
                            .unwrap_or_default()
                    };
                    ListedTask {
                        last_run_at_iso: t.last_run_at.and_then(|s| Utc.timestamp_opt(s, 0).single()).map(iso),
                        id: t.id,
                        cron_expr: t.cron_expr,
                        recurring,
                        prompt: t.prompt,
                        source: t.source,
                        workflow: t.workflow,
                        last_run_at: t.last_run_at,
                        created_at: t.created_at,
                        upcoming_fires,
                    }
                })
                .collect();
            Ok(serde_json::to_string(&Listed { timezone: tz.name().to_owned(), count: tasks.len(), tasks })?)
        }
        "cancel_scheduled_task" => {
            let id = args.get("id").and_then(Value::as_i64).filter(|id| *id >= 1);
            let id = id.ok_or_else(|| anyhow::anyhow!("id must be a positive integer"))?;
            Ok(json!({"ok": store.delete(id).await?, "id": id}).to_string())
        }
        _ => anyhow::bail!("unknown schedule tool {name}"),
    }
}

// ── 5. boot ────────────────────────────────────────────────────────────────

struct SystemTask {
    cron: &'static str,
    // None → fires as a generic `scheduler` signal; the scheduler skill maps
    // the prompt body to the right composer sub-agent.
    source: Option<&'static str>,
    prompt: &'static str,
}

const SYSTEM_TASKS: &[SystemTask] = &[SystemTask {
    cron: "0 4 * * *",
    source: Some("dreaming"),
    prompt: "Daily dreaming tick. Review the signals processed since the \
             previous dreaming fire (see 'Previous fire' header above) and \
             consider whether any skill files deserve an edit based on \
             patterns, recurring user feedback, or failure modes you \
             observed. Use list_signals(since=<previous fire>) to scope \
             the review. Edit skills via write_skill when warranted.",
}];

// MCP's `list_scheduled_tasks` reply, as far as the import reads it.
#[derive(Deserialize)]
struct McpListed {
    tasks: Vec<McpTask>,
}

#[derive(Deserialize)]
struct McpTask {
    cron_expr: String,
    recurring: bool,
    prompt: String,
    source: Option<String>,
    last_run_at: Option<i64>,
    created_at: String,
}

// The schedule MCP kept until the agent owned it.
async fn mcp_tasks(mcp: &dyn Mcp, cancel: &CancellationToken) -> anyhow::Result<Vec<NewTask>> {
    if !mcp.tools().iter().any(|t| t.name == "list_scheduled_tasks") {
        return Ok(Vec::new());
    }
    let raw = mcp.call_tool("list_scheduled_tasks", Map::new(), cancel).await?;
    let listed: McpListed = serde_json::from_str(&raw)
        .map_err(|e| anyhow::anyhow!("list_scheduled_tasks: unexpected reply ({e}): {raw}"))?;
    listed
        .tasks
        .into_iter()
        .map(|t| {
            let created = chrono::NaiveDateTime::parse_from_str(&t.created_at, "%Y-%m-%d %H:%M:%S")
                .map_err(|e| anyhow::anyhow!("created_at {:?}: {e}", t.created_at))?
                .and_utc();
            Ok(NewTask {
                cron_expr: t.cron_expr,
                recurring: t.recurring,
                prompt: t.prompt,
                source: t.source,
                workflow: None,
                last_run_at: t.last_run_at,
                created,
            })
        })
        .collect()
}

// On the first start, the tasks MCP held move here with their last fire, so
// nothing owed is lost and nothing fired is fired again; with none to move
// (a fresh install), the system tasks are seeded: only the generic dreaming
// tick. User-specific tasks (the digests and their prompts) are not shipped;
// they live in agent.db once the user asks the agent to schedule them. Never
// again after that: a task the user cancelled stays cancelled. A failed import changes nothing
// and is retried on the next start.
pub async fn initialize(store: &dyn ScheduleStore, mcp: &dyn Mcp, cancel: &CancellationToken) -> anyhow::Result<usize> {
    let mut tasks = mcp_tasks(mcp, cancel).await?;
    if tasks.is_empty() {
        tasks = SYSTEM_TASKS
            .iter()
            .map(|t| NewTask {
                cron_expr: t.cron.into(),
                recurring: true,
                prompt: t.prompt.into(),
                source: t.source.map(str::to_owned),
                workflow: None,
                last_run_at: None,
                created: Utc::now(),
            })
            .collect();
    }
    Ok(if store.initialize(&tasks).await? { tasks.len() } else { 0 })
}
