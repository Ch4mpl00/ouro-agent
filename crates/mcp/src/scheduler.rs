// Cron-driven tasks (`scheduled_tasks` table) — the single mechanism for any
// time-triggered signal. User reminders and the system digests/dreaming are
// all rows here; the latter are seeded by `db.rs`.
//
// Sections:
//   1. storage — task rows: insert / list active / mark fired / delete
//   2. cron    — parse + next-slot math in the user's timezone
//   3. poller  — 30s tick that turns due tasks into signals
//   4. tools   — `scheduler` toolset: schedule/list/cancel + get/set timezone

use std::time::Duration;

use chrono::{DateTime, Offset, TimeZone, Utc};
use chrono_tz::Tz;
use croner::Cron;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::{schemars, tool, tool_router};
use serde::{Deserialize, Serialize};
use tokio_postgres::Row;
use tokio_util::sync::CancellationToken;

use crate::db::{Db, sql_time};
use crate::server::{McpTools, ToolResult, invalid_params, json_result};
use crate::settings::{Settings, local_time, parse_timezone};
use crate::signals::Signals;
use crate::time::iso;

// ── 1. storage ───────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct Scheduler {
    db: Db,
    settings: Settings,
}

#[derive(Debug, Clone, Serialize)]
pub struct TaskRow {
    pub id: i64,
    pub cron_expr: String,
    // 0 | 1 — schedule_task has always returned the raw sqlite row.
    pub recurring: i64,
    pub prompt: String,
    // None → signal source 'scheduler' (user-created).
    pub source: Option<String>,
    // Unix seconds of the slot last fired for.
    pub last_run_at: Option<i64>,
    // "YYYY-MM-DD HH:MM:SS", UTC.
    pub created_at: String,
    #[serde(skip)]
    pub created: DateTime<Utc>,
}

const TASK_COLUMNS: &str = "id, cron_expr, recurring, prompt, source, last_run_at, created_at";

fn task_row(r: &Row) -> TaskRow {
    let created: DateTime<Utc> = r.get(6);
    TaskRow {
        id: r.get(0),
        cron_expr: r.get(1),
        recurring: i64::from(r.get::<_, bool>(2)),
        prompt: r.get(3),
        source: r.get(4),
        last_run_at: r.get(5),
        created_at: sql_time(created),
        created,
    }
}

impl Scheduler {
    pub fn new(db: Db, settings: Settings) -> Self {
        Self { db, settings }
    }

    pub async fn insert(
        &self,
        cron_expr: &str,
        recurring: bool,
        prompt: &str,
        source: Option<&str>,
    ) -> anyhow::Result<TaskRow> {
        let row = self
            .db
            .client()
            .await?
            .query_one(
                &format!(
                    "INSERT INTO scheduled_tasks (cron_expr, recurring, prompt, source) VALUES ($1, $2, $3, $4)
                     RETURNING {TASK_COLUMNS}"
                ),
                &[&cron_expr, &recurring, &prompt, &source],
            )
            .await?;
        Ok(task_row(&row))
    }

    // Tasks that may still fire: every recurring task, plus one-shots that
    // have not fired yet.
    pub async fn list_active(&self) -> anyhow::Result<Vec<TaskRow>> {
        let rows = self
            .db
            .client()
            .await?
            .query(
                &format!(
                    "SELECT {TASK_COLUMNS} FROM scheduled_tasks WHERE recurring OR last_run_at IS NULL ORDER BY id ASC"
                ),
                &[],
            )
            .await?;
        Ok(rows.iter().map(task_row).collect())
    }

    #[cfg(test)]
    async fn get(&self, id: i64) -> anyhow::Result<Option<TaskRow>> {
        let row = self
            .db
            .client()
            .await?
            .query_opt(&format!("SELECT {TASK_COLUMNS} FROM scheduled_tasks WHERE id = $1"), &[&id])
            .await?;
        Ok(row.as_ref().map(task_row))
    }

    // Compare-and-set of the slot last fired for: moves last_run_at from
    // `expected` to `next` only while it still is `expected` and the task
    // still exists. The poller claims a slot with it (previous → slot), so
    // overlapping ticks or a cancel in between can't fire it as well, and
    // gives a claim back (slot → previous) when the signal couldn't be
    // recorded. Stamping a slot on a one-shot also retires it.
    pub async fn swap_last_run(&self, id: i64, expected: Option<i64>, next: Option<i64>) -> anyhow::Result<bool> {
        let changed = self
            .db
            .client()
            .await?
            .execute(
                "UPDATE scheduled_tasks SET last_run_at = $1 WHERE id = $2 AND last_run_at IS NOT DISTINCT FROM $3",
                &[&next, &id, &expected],
            )
            .await?;
        Ok(changed > 0)
    }

    pub async fn delete(&self, id: i64) -> anyhow::Result<bool> {
        Ok(self.db.client().await?.execute("DELETE FROM scheduled_tasks WHERE id = $1", &[&id]).await? > 0)
    }
}

// ── 2. cron ──────────────────────────────────────────────────────────────────

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

// The slot a task's next fire is computed from: the slot it last fired for,
// or — never fired — its creation time.
fn anchor(task: &TaskRow) -> DateTime<Utc> {
    match task.last_run_at {
        Some(secs) => Utc.timestamp_opt(secs, 0).single().unwrap_or_default(),
        None => task.created,
    }
}

// ── 3. poller ────────────────────────────────────────────────────────────────

const TICK_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_SIGNAL_SOURCE: &str = "scheduler";

// What a tick reads and writes, behind traits so a test can hold a tick
// between listing and firing, or fail the signal write.
#[async_trait::async_trait]
pub trait TaskStore: Send + Sync {
    async fn timezone(&self) -> chrono_tz::Tz;
    async fn list_active(&self) -> anyhow::Result<Vec<TaskRow>>;
    async fn swap_last_run(&self, id: i64, expected: Option<i64>, next: Option<i64>) -> anyhow::Result<bool>;
}

#[async_trait::async_trait]
impl TaskStore for Scheduler {
    async fn timezone(&self) -> chrono_tz::Tz {
        self.settings.timezone().await
    }

    async fn list_active(&self) -> anyhow::Result<Vec<TaskRow>> {
        Scheduler::list_active(self).await
    }

    async fn swap_last_run(&self, id: i64, expected: Option<i64>, next: Option<i64>) -> anyhow::Result<bool> {
        Scheduler::swap_last_run(self, id, expected, next).await
    }
}

#[async_trait::async_trait]
pub trait SignalSink: Send + Sync {
    async fn record(&self, source: &str, content: &str) -> anyhow::Result<()>;
}

#[async_trait::async_trait]
impl SignalSink for Signals {
    async fn record(&self, source: &str, content: &str) -> anyhow::Result<()> {
        Signals::record(self, source, content).await.map(drop)
    }
}

// Fires every task whose next slot (after its anchor) has passed. Restart-safe
// and never double-fires a slot: the anchor is the *slot* fired for, not the
// wall clock, so a late tick still advances to the following slot.
pub async fn tick(tasks: &dyn TaskStore, signals: &dyn SignalSink, now: DateTime<Utc>) -> anyhow::Result<usize> {
    let tz = tasks.timezone().await;
    let mut fired = 0;
    for task in tasks.list_active().await? {
        let cron = match parse_cron(&task.cron_expr) {
            Ok(cron) => cron,
            Err(err) => {
                tracing::warn!(task = task.id, cron = %task.cron_expr, %err, "invalid cron, skipping");
                continue;
            }
        };
        let Some(slot) = next_slot(&cron, tz, anchor(&task)) else { continue };
        if slot > now {
            continue;
        }
        // Snapshot before stamping, so skills (dreaming) can scope
        // `since=<previous fire>` from the signal header.
        let previous = task.last_run_at.and_then(|s| Utc.timestamp_opt(s, 0).single()).map(iso);
        // Claimed with a compare-and-set: an overlapping tick that got there
        // first, or a cancel since list_active, means this tick doesn't fire it.
        let slot_unix = slot.timestamp();
        if !tasks.swap_last_run(task.id, task.last_run_at, Some(slot_unix)).await? {
            continue;
        }
        let source = task.source.as_deref().unwrap_or(DEFAULT_SIGNAL_SOURCE);
        if let Err(err) = signals.record(source, &render_content(&task, previous.as_deref(), slot, now)).await {
            // Give the slot back so the next tick fires it, rather than
            // leaving it marked as done with no signal.
            tasks.swap_last_run(task.id, Some(slot_unix), task.last_run_at).await?;
            return Err(err);
        }
        tracing::info!(
            task = task.id,
            source,
            slot = %iso(slot),
            kind = if task.recurring == 1 { "recurring" } else { "one-shot" },
            "fired scheduled task"
        );
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

pub async fn run_poller(scheduler: Scheduler, signals: Signals, cancel: CancellationToken) {
    let tz = scheduler.settings.timezone().await;
    tracing::info!(every = ?TICK_INTERVAL, %tz, "scheduler poller started");
    let mut interval = tokio::time::interval(TICK_INTERVAL);
    // A tick that overran (a stalled disk) should not be followed by a burst.
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = interval.tick() => {}
        }
        if let Err(err) = tick(&scheduler, &signals, Utc::now()).await {
            tracing::error!(%err, "scheduler tick failed");
        }
    }
}

// ── 4. tools ─────────────────────────────────────────────────────────────────

#[derive(Deserialize, schemars::JsonSchema)]
struct ScheduleTaskParams {
    /// 5-field cron expression (e.g. '30 9 * * *' = 09:30 daily).
    cron_expr: String,
    /// true = keep firing on every cron match; false = fire once then deactivate.
    recurring: bool,
    /// Free-text instruction delivered to the agent when the task fires. The agent interprets it under the `scheduler` skill (e.g. 'remind me to take pills').
    prompt: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct CancelTaskParams {
    /// Task id from list_scheduled_tasks.
    #[schemars(range(min = 1))]
    id: i64,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct SetTimezoneParams {
    /// IANA timezone name, e.g. 'Europe/Kiev'.
    tz: String,
}

#[derive(Serialize)]
struct Failure {
    ok: bool,
    error: String,
}

fn failure(error: String) -> ToolResult {
    json_result(&Failure { ok: false, error })
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

#[derive(Serialize)]
struct Cancelled {
    ok: bool,
    id: i64,
}

#[derive(Serialize)]
struct TimezoneInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    ok: Option<bool>,
    timezone: String,
    local_now: String,
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

#[tool_router(router = scheduler_tools, vis = "pub(crate)")]
impl McpTools {
    #[tool(
        name = "schedule_task",
        title = "Schedule an agent task",
        description = "Register a cron-driven task. When the cron matches, MCP enqueues a \
            `scheduler` signal with the given prompt and the agent acts on it \
            (send a Telegram message, run a check, etc). Cron is standard 5-field \
            (minute hour day-of-month month day-of-week) evaluated in the \
            user's configured timezone. For one-shot reminders set `recurring: \
            false` and use a specific cron like '30 14 12 5 *' (14:30 on May \
            12); the task auto-deactivates after the first fire. For repeating \
            tasks use a generic cron like '0 9 * * *' (every day 9:00). The \
            agent is responsible for converting natural-language times into \
            cron syntax before calling this tool."
    )]
    async fn schedule_task(
        &self,
        Parameters(ScheduleTaskParams { cron_expr, recurring, prompt }): Parameters<ScheduleTaskParams>,
    ) -> ToolResult {
        if cron_expr.is_empty() || prompt.is_empty() {
            return Err(invalid_params("cron_expr and prompt must be non-empty"));
        }
        if let Some(error) = field_count_error(&cron_expr) {
            return failure(format!("Invalid cron expression: {error}"));
        }
        let cron = match parse_cron(&cron_expr) {
            Ok(cron) => cron,
            Err(err) => return failure(format!("Invalid cron expression: {err}")),
        };
        let tz = self.deps.settings.timezone().await;
        // croner accepts a day that never comes (Nov 31) and would store a
        // task that never fires; the TS server's parser refuses it.
        if next_slot(&cron, tz, Utc::now()).is_none() {
            return failure("Invalid cron expression: it never matches a date that exists".into());
        }
        let upcoming_fires = preview_next_fires(&cron, tz, upcoming_count(recurring), Utc::now());
        let task = crate::try_tool!(self.deps.scheduler.insert(&cron_expr, recurring, &prompt, None).await);
        json_result(&Scheduled { ok: true, task, timezone: tz.name().to_owned(), upcoming_fires })
    }

    #[tool(
        name = "list_scheduled_tasks",
        annotations(read_only_hint = true),
        title = "List scheduled tasks",
        description = "Show every task that may still fire — recurring tasks (always) and \
            one-shots that haven't been triggered yet. Each row includes the \
            cron expression, prompt, last fire time, and the next 1-3 upcoming \
            fire timestamps in the user's timezone for sanity-checking."
    )]
    async fn list_scheduled_tasks(&self) -> ToolResult {
        let tz = self.deps.settings.timezone().await;
        let now = Utc::now();
        let tasks: Vec<ListedTask> = crate::try_tool!(self.deps.scheduler.list_active().await)
            .into_iter()
            .map(|t| {
                let recurring = t.recurring == 1;
                // An invalid cron surfaces as an empty `upcoming_fires`.
                let upcoming_fires = parse_cron(&t.cron_expr)
                    .map(|cron| preview_next_fires(&cron, tz, upcoming_count(recurring), now))
                    .unwrap_or_default();
                ListedTask {
                    last_run_at_iso: t.last_run_at.and_then(|s| Utc.timestamp_opt(s, 0).single()).map(iso),
                    id: t.id,
                    cron_expr: t.cron_expr,
                    recurring,
                    prompt: t.prompt,
                    source: t.source,
                    last_run_at: t.last_run_at,
                    created_at: t.created_at,
                    upcoming_fires,
                }
            })
            .collect();
        json_result(&Listed { timezone: tz.name().to_owned(), count: tasks.len(), tasks })
    }

    #[tool(
        name = "cancel_scheduled_task",
        title = "Cancel a scheduled task",
        description = "Permanently remove a task by id. Use this when the user says \
            'forget about that reminder' or 'stop the daily X'. Returns \
            { ok: true, removed: <id> } on success, { ok: false } if no such task."
    )]
    async fn cancel_scheduled_task(
        &self,
        Parameters(CancelTaskParams { id }): Parameters<CancelTaskParams>,
    ) -> ToolResult {
        if id < 1 {
            return Err(invalid_params("id must be a positive integer"));
        }
        let ok = crate::try_tool!(self.deps.scheduler.delete(id).await);
        json_result(&Cancelled { ok, id })
    }

    #[tool(
        name = "get_timezone",
        annotations(read_only_hint = true),
        title = "Get configured timezone",
        description = "Return the IANA timezone driving cron evaluation and digest \
            schedule decisions. Defaults to UTC when unset."
    )]
    async fn get_timezone(&self) -> ToolResult {
        let now = self.deps.settings.local_time(Utc::now()).await;
        json_result(&TimezoneInfo { ok: None, timezone: now.tz.name().to_owned(), local_now: now.display() })
    }

    #[tool(
        name = "set_timezone",
        title = "Set the configured timezone",
        description = "Update the IANA timezone (e.g. 'Europe/Kiev', 'America/New_York', \
            'UTC'). Takes effect immediately — the next scheduler tick, daily \
            digest check, and any new schedule_task call all use the new value. \
            Existing tasks keep their cron string as-is, so their next-fire \
            wall-clock time shifts. Invalid IANA names are rejected."
    )]
    async fn set_timezone(&self, Parameters(SetTimezoneParams { tz }): Parameters<SetTimezoneParams>) -> ToolResult {
        if tz.is_empty() {
            return Err(invalid_params("tz must be non-empty"));
        }
        let zone = match parse_timezone(&tz) {
            Ok(zone) => zone,
            Err(err) => return failure(format!("Invalid timezone '{tz}': {err}")),
        };
        crate::try_tool!(self.deps.settings.set_timezone(zone).await);
        let now = local_time(zone, Utc::now());
        json_result(&TimezoneInfo { ok: Some(true), timezone: tz, local_now: now.display() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup() -> Option<(Scheduler, Signals, Db)> {
        let db = Db::test().await?;
        // Drop the seeded system tasks so each test sees only its own rows.
        db.client().await.unwrap().execute("DELETE FROM scheduled_tasks", &[]).await.unwrap();
        Some((Scheduler::new(db.clone(), Settings::new(db.clone())), Signals::new(db.clone()), db))
    }

    fn utc(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn evaluates_cron_on_the_users_wall_clock() {
        let cron = parse_cron("0 9 * * *").unwrap();
        // 09:00 in Kyiv (UTC+3, summer time) is 06:00Z.
        let next = next_slot(&cron, chrono_tz::Europe::Kiev, utc("2026-07-01T00:00:00Z")).unwrap();
        assert_eq!(iso(next), "2026-07-01T06:00:00.000Z");
    }

    #[test]
    fn dom_and_dow_are_ored_like_cron_parser() {
        // The 1st of the month OR any Monday. 2026-10-02 is a Friday, so the
        // next match is Monday the 5th, not November 1st.
        let cron = parse_cron("0 0 1 * 1").unwrap();
        let next = next_slot(&cron, Tz::UTC, utc("2026-10-02T00:00:00Z")).unwrap();
        assert_eq!(iso(next), "2026-10-05T00:00:00.000Z");
    }

    #[tokio::test]
    async fn fires_a_due_slot_once_and_retires_one_shots() {
        let Some((scheduler, signals, db)) = setup().await else { return };
        let task = scheduler.insert("30 14 2 10 *", false, "take pills", None).await.unwrap();
        assert_eq!(task.recurring, 0);
        db.client()
            .await
            .unwrap()
            .execute("UPDATE scheduled_tasks SET created_at = '2026-10-01 00:00:00+00' WHERE id = $1", &[&task.id])
            .await
            .unwrap();

        assert_eq!(tick(&scheduler, &signals, utc("2026-10-02T14:29:00Z")).await.unwrap(), 0);
        assert_eq!(tick(&scheduler, &signals, utc("2026-10-02T14:31:00Z")).await.unwrap(), 1);
        assert_eq!(tick(&scheduler, &signals, utc("2026-10-02T14:32:00Z")).await.unwrap(), 0);
        assert!(scheduler.list_active().await.unwrap().is_empty());

        let signal = signals.pop_next().await.unwrap().unwrap();
        assert_eq!(signal.source, "scheduler");
        assert_eq!(
            signal.content,
            format!(
                "Scheduled task #{} fired.\nCron: 30 14 2 10 *\nSlot: 2026-10-02T14:30:00.000Z\n\
                 Now: 2026-10-02T14:31:00.000Z\nPrevious fire: never (this is the first run)\n\
                 Recurring: no (one-shot)\n\ntake pills",
                task.id
            )
        );
    }

    #[tokio::test]
    async fn a_late_tick_advances_by_slot_not_by_wall_clock() {
        let Some((scheduler, signals, _db)) = setup().await else { return };
        let task = scheduler.insert("0 * * * *", true, "hourly", Some("dreaming")).await.unwrap();
        scheduler.swap_last_run(task.id, None, Some(utc("2026-10-02T10:00:00Z").timestamp())).await.unwrap();

        // Three hours late: owes the 11:00 slot now, 12:00 on the next tick.
        assert_eq!(tick(&scheduler, &signals, utc("2026-10-02T13:05:00Z")).await.unwrap(), 1);
        let fired = scheduler.get(task.id).await.unwrap().unwrap();
        assert_eq!(fired.last_run_at, Some(utc("2026-10-02T11:00:00Z").timestamp()));

        let signal = signals.pop_next().await.unwrap().unwrap();
        assert_eq!(signal.source, "dreaming");
        assert!(signal.content.contains("Previous fire: 2026-10-02T10:00:00.000Z"));
    }
}
