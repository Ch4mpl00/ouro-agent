// Persistent state for the closed-loop improver (Phase 3, part 3): one row per
// (skill, axis) in agent.db's `improver_state`, holding the last cycle's
// outcome and — when the last attempt shipped — the live-monitor bookkeeping
// (pre-ship baseline + shipped lesson + status). The improver owns this
// state; the trace mirror does not.

use agent::db::AgentDb;
use rusqlite::{OptionalExtension, params};

#[derive(Clone, Debug, PartialEq)]
pub struct ImproverState {
    pub skill: String,
    pub axis: String,
    pub last_attempt_at: String,
    pub last_outcome: String,
    pub shipped_at: Option<String>,
    pub shipped_lesson: Option<String>,
    pub baseline_mean: Option<f64>,
    pub baseline_n: Option<f64>,
    pub monitor_status: Option<String>,
}

// The mutable fields a cycle writes back. Identity (skill, axis) is separate.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ImproverStateUpdate {
    pub last_outcome: String,
    pub shipped_at: Option<String>,
    pub shipped_lesson: Option<String>,
    pub baseline_mean: Option<f64>,
    pub baseline_n: Option<f64>,
    pub monitor_status: Option<String>,
}

impl ImproverStateUpdate {
    // Nothing shipped, nothing monitored.
    pub fn settled(outcome: &str, monitor_status: Option<&str>) -> ImproverStateUpdate {
        ImproverStateUpdate {
            last_outcome: outcome.into(),
            monitor_status: monitor_status.map(str::to_owned),
            ..ImproverStateUpdate::default()
        }
    }
}

pub fn get(db: &AgentDb, skill: &str, axis: &str) -> anyhow::Result<Option<ImproverState>> {
    Ok(db
        .conn()
        .query_row(
            "SELECT skill, axis, last_attempt_at, last_outcome, shipped_at, shipped_lesson, baseline_mean,
                    baseline_n, monitor_status
             FROM improver_state WHERE skill = ?1 AND axis = ?2",
            params![skill, axis],
            |r| {
                Ok(ImproverState {
                    skill: r.get(0)?,
                    axis: r.get(1)?,
                    last_attempt_at: r.get(2)?,
                    last_outcome: r.get(3)?,
                    shipped_at: r.get(4)?,
                    shipped_lesson: r.get(5)?,
                    baseline_mean: r.get(6)?,
                    baseline_n: r.get(7)?,
                    monitor_status: r.get(8)?,
                })
            },
        )
        .optional()?)
}

pub fn upsert(
    db: &AgentDb,
    skill: &str,
    axis: &str,
    last_attempt_at: &str,
    u: &ImproverStateUpdate,
) -> anyhow::Result<()> {
    db.conn().execute(
        "INSERT INTO improver_state (skill, axis, last_attempt_at, last_outcome, shipped_at, shipped_lesson,
           baseline_mean, baseline_n, monitor_status)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
         ON CONFLICT (skill, axis) DO UPDATE SET
           last_attempt_at = excluded.last_attempt_at, last_outcome = excluded.last_outcome,
           shipped_at = excluded.shipped_at, shipped_lesson = excluded.shipped_lesson,
           baseline_mean = excluded.baseline_mean, baseline_n = excluded.baseline_n,
           monitor_status = excluded.monitor_status",
        params![
            skill,
            axis,
            last_attempt_at,
            u.last_outcome,
            u.shipped_at,
            u.shipped_lesson,
            u.baseline_mean,
            u.baseline_n,
            u.monitor_status
        ],
    )?;
    Ok(())
}
