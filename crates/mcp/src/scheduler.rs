// The scheduled tasks MCP used to fire, kept read-only for one release: the
// agent now owns the schedule (`crates/agent/src/schedule.rs`) and imports
// these rows once through `list_scheduled_tasks` on its first boot. The
// table and this module go once every deployment has been imported.
//
// Sections:
//   1. storage — task rows: insert (tests) / list active
//   2. cron    — parse + next-slot math in the user's timezone
//   3. tools   — `scheduler` toolset: list + get/set timezone

use chrono::{DateTime, Offset, TimeZone, Utc};
use chrono_tz::Tz;
use croner::Cron;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::{schemars, tool, tool_router};
use serde::{Deserialize, Serialize};
use tokio_postgres::Row;

use crate::db::{Db, sql_time};
use crate::server::{McpTools, ToolResult, invalid_params, json_result};
use crate::settings::{local_time, parse_timezone};
use crate::time::iso;

// ── 1. storage ───────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct Scheduler {
    db: Db,
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
    pub fn new(db: Db) -> Self {
        Self { db }
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

// ── 3. tools ─────────────────────────────────────────────────────────────────

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
struct TimezoneInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    ok: Option<bool>,
    timezone: String,
    local_now: String,
}

fn upcoming_count(recurring: bool) -> usize {
    if recurring { 3 } else { 1 }
}

#[tool_router(router = scheduler_tools, vis = "pub(crate)")]
impl McpTools {
    #[tool(
        name = "list_scheduled_tasks",
        annotations(read_only_hint = true, open_world_hint = false),
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
        name = "get_timezone",
        annotations(read_only_hint = true, open_world_hint = false),
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
        annotations(open_world_hint = false),
        title = "Set the configured timezone",
        description = "Update the IANA timezone (e.g. 'Europe/Kiev', 'America/New_York', \
            'UTC'). Takes effect immediately — the agent's next scheduler tick, \
            daily digest check, and any new schedule_task call all use the new value. \
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
}
