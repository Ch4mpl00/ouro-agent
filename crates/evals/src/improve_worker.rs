// The closed-loop improver's cron driver (Phase 3, part 3). On each tick it walks
// every (skill, axis) present in the judged corpus and, per pair:
//   1. MONITOR — if the last ship is still "pending", compare the axis's prod
//      scores on traces that ran AFTER the ship against the pre-ship baseline.
//      Confident drop → AUTO-REVERT (remove the lesson); held → mark "kept";
//      too few post-ship traces → keep watching and DON'T author (one change at
//      a time, so each ship's effect stays attributable).
//   2. IMPROVE — otherwise run one improve cycle and record the outcome.
// Mirrors the judge worker: a long-running poll loop, one service in compose.
// The gate is the in-process guard; THIS adds prod as the second, ground-truth
// guard.

use std::time::Duration;

use agent::db::{JudgementFilter, JudgementRecord};
use agent::local_recorder::now_iso;
use agent::skills::SkillStore;
use serde_json::{Value, json};

use crate::backend::JudgeProvider;
use crate::env::{bool_env, num_env};
use crate::improve_cycle::{CycleOutcome, CycleResult, ImproveCycleOpts, ImproveDeps, run_improve_cycle};
use crate::improver_store::{self, ImproverStateUpdate};
use crate::monitor::{Baseline, RevertDecision, decide_revert, remove_lesson};
use crate::noise::{AxisScores, NOISE_AXES, NoiseAxis};
use crate::schema::JUDGE_PROMPT_VERSION;

#[derive(Clone, Debug)]
pub struct ImproveWorkerOpts {
    pub provider: JudgeProvider,
    // false → shadow mode (propose + log, never ship). Flip on after prod-validation.
    pub apply: bool,
    pub poll_interval_ms: u64,
    pub once: bool,
    pub recent_days: f64,
    // Min post-ship traces before the monitor will judge a trend.
    pub min_monitor_n: usize,
    pub k: f64,
    pub budget: usize,
    pub cluster: usize,
    pub holdout: usize,
    pub samples: usize,
    pub abs_max: f64,
    pub bar: f64,
    pub holdout_min: f64,
    pub max_attempts: usize,
    pub guard_faithfulness: bool,
    // Cost ceiling: at most this many improve cycles (the codex-heavy step) per
    // tick. Monitoring/auto-revert is free and runs for ALL pairs every tick;
    // only AUTHORING is capped. Pairs are picked least-recently-attempted first
    // (round-robin), so an unattended tick can't blow the shared codex quota.
    pub max_cycles_per_tick: usize,
}

impl ImproveWorkerOpts {
    pub fn from_env() -> ImproveWorkerOpts {
        ImproveWorkerOpts {
            provider: JudgeProvider::from_env(),
            apply: bool_env("IMPROVE_APPLY", false),
            // Daily; the cap below bounds per-tick cost.
            poll_interval_ms: num_env("IMPROVE_POLL_INTERVAL_MS", 86_400_000.0) as u64,
            once: bool_env("IMPROVE_ONCE", false),
            recent_days: num_env("IMPROVE_RECENT_DAYS", 14.0),
            min_monitor_n: num_env("IMPROVE_MIN_MONITOR_N", 5.0) as usize,
            k: num_env("IMPROVE_K", 2.0),
            budget: num_env("IMPROVE_BUDGET", 8.0) as usize,
            cluster: num_env("IMPROVE_CLUSTER", 3.0) as usize,
            holdout: num_env("IMPROVE_HOLDOUT", 3.0) as usize,
            samples: num_env("IMPROVE_SAMPLES", 2.0) as usize,
            abs_max: num_env("IMPROVE_ABS_MAX", 0.6),
            bar: num_env("IMPROVE_BAR", 0.75),
            holdout_min: num_env("IMPROVE_HOLDOUT_MIN", 0.85),
            // 1 = no informed retry (retry doubles codex).
            max_attempts: num_env("IMPROVE_MAX_ATTEMPTS", 1.0) as usize,
            guard_faithfulness: bool_env("IMPROVE_GUARD_FAITHFULNESS", false),
            max_cycles_per_tick: num_env("IMPROVE_MAX_CYCLES_PER_TICK", 1.0) as usize,
        }
    }
}

// Outcome of the monitor step: Watching / Reverted / Kept means we already
// acted on a pending ship; Author means there is nothing pending so the caller
// should run an improve cycle. Watching tells the caller to SKIP authoring.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MonitorAction {
    Author,
    Watching,
    Reverted,
    Kept,
}

pub struct WorkerDeps<'a> {
    pub cycle: ImproveDeps<'a>,
    // Durable, structured audit sink (one record per cycle + monitor event),
    // distinct from `log` (ephemeral stdout lost on container recreate): a week
    // of runs — INCLUDING rejected proposals (with the lesson + gate reasons) —
    // stays reviewable later.
    pub audit: &'a (dyn Fn(Value) + Send + Sync),
}

async fn monitor_pending(
    deps: &WorkerDeps<'_>,
    opts: &ImproveWorkerOpts,
    skill: &str,
    axis: NoiseAxis,
    records: &[JudgementRecord],
    now: &str,
) -> anyhow::Result<MonitorAction> {
    let d = &deps.cycle;
    let state = improver_store::get(d.db, skill, axis.as_str())?;
    let Some(state) = state.filter(|s| s.monitor_status.as_deref() == Some("pending")) else {
        return Ok(MonitorAction::Author);
    };
    let (Some(shipped_at), Some(baseline_mean)) = (state.shipped_at.clone(), state.baseline_mean) else {
        return Ok(MonitorAction::Author);
    };

    let post: Vec<f64> = records
        .iter()
        .filter(|r| r.started_at > shipped_at)
        .filter_map(|r| AxisScores::from_db(&r.scores).get(axis))
        .collect();
    let verdict = decide_revert(
        Baseline { mean: baseline_mean, n: state.baseline_n.unwrap_or(1.0) },
        &post,
        d.sigma.get(&axis).copied(),
        opts.k,
        opts.min_monitor_n,
    );
    let post3 = verdict.post_mean.map_or("—".into(), |m| format!("{m:.3}"));
    (d.log)(&format!(
        "[{skill}/{}] monitor: baseline {baseline_mean:.3} vs post {post3} (n={}) → {}",
        axis.as_str(),
        verdict.post_n,
        verdict.decision.as_str()
    ));

    let event = |action: &str| {
        json!({
            "at": now, "kind": "monitor", "action": action, "skill": skill, "axis": axis.as_str(),
            "baselineMean": baseline_mean, "postMean": verdict.post_mean, "postN": verdict.post_n,
            "lesson": state.shipped_lesson,
        })
    };
    match verdict.decision {
        RevertDecision::Insufficient => Ok(MonitorAction::Watching),
        RevertDecision::Revert => {
            let patch = d.skills.read_patch(skill).await?.unwrap_or_default();
            let rebuilt = remove_lesson(&patch, state.shipped_lesson.as_deref().unwrap_or(""));
            if rebuilt.trim().is_empty() {
                d.skills.delete_patch(skill).await?;
            } else {
                d.skills.save_patch(skill, &rebuilt).await?;
            }
            improver_store::upsert(d.db, skill, axis.as_str(), now, &ImproverStateUpdate::settled("reverted", None))?;
            (d.log)(&format!(
                "[{skill}/{}] AUTO-REVERTED — live trend fell below baseline; removed the lesson.",
                axis.as_str()
            ));
            (deps.audit)(event("reverted"));
            Ok(MonitorAction::Reverted)
        }
        // Kept: the ship held in prod — settle it (stop monitoring) and let the
        // caller author the next improvement this round.
        RevertDecision::Keep => {
            improver_store::upsert(
                d.db,
                skill,
                axis.as_str(),
                now,
                &ImproverStateUpdate::settled("kept", Some("kept")),
            )?;
            (d.log)(&format!("[{skill}/{}] ship held in prod — kept.", axis.as_str()));
            (deps.audit)(event("kept"));
            Ok(MonitorAction::Kept)
        }
    }
}

// Records a cycle's outcome; "error" is the worker-level outcome of a cycle
// that failed — last_outcome is free text, so this just records the attempt.
fn record_cycle(
    deps: &WorkerDeps<'_>,
    skill: &str,
    axis: NoiseAxis,
    result: Option<&CycleResult>,
    now: &str,
) -> anyhow::Result<()> {
    let update = match result {
        Some(r) if r.outcome == CycleOutcome::Shipped => ImproverStateUpdate {
            last_outcome: "shipped".into(),
            shipped_at: Some(now.to_owned()),
            shipped_lesson: r.lesson.clone(),
            baseline_mean: r.baseline.map(|b| b.0),
            baseline_n: r.baseline.map(|b| b.1 as f64),
            // Can't monitor without a baseline.
            monitor_status: r.baseline.map(|_| "pending".into()),
        },
        Some(r) => ImproverStateUpdate::settled(r.outcome.as_str(), None),
        None => ImproverStateUpdate::settled("error", None),
    };
    improver_store::upsert(deps.cycle.db, skill, axis.as_str(), now, &update)
}

struct Pair {
    skill: String,
    axis: NoiseAxis,
    records: Vec<JudgementRecord>,
    // "" = never attempted → sorts first (round-robin).
    last_attempt_at: String,
}

async fn tick(deps: &WorkerDeps<'_>, opts: &ImproveWorkerOpts) -> anyhow::Result<()> {
    let d = &deps.cycle;
    let provider = opts.provider.as_str();
    let skills = d.db.list_judged_skills(provider, JUDGE_PROMPT_VERSION)?;
    let listed = if skills.is_empty() { "(none)".into() } else { skills.join(", ") };
    (d.log)(&format!("[improve-worker] {} skill(s) with a corpus: {listed}", skills.len()));

    // Every (skill, axis) pair the corpus actually scores, each skill's
    // judgements loaded once.
    let mut pairs = Vec::new();
    for skill in &skills {
        let records =
            d.db.list_judgements(&JudgementFilter { skill, provider, prompt_version: JUDGE_PROMPT_VERSION })?;
        for axis in NOISE_AXES {
            if !records.iter().any(|r| AxisScores::from_db(&r.scores).get(axis).is_some()) {
                continue;
            }
            let last_attempt_at =
                improver_store::get(d.db, skill, axis.as_str())?.map(|s| s.last_attempt_at).unwrap_or_default();
            pairs.push(Pair { skill: skill.clone(), axis, records: records.clone(), last_attempt_at });
        }
    }

    // Monitor every pair first — free (no codex): auto-revert a ship whose live
    // trend fell, settle a held one. Watching/Reverted pairs skip authoring.
    let mut eligible = Vec::new();
    for p in pairs {
        let action = monitor_pending(deps, opts, &p.skill, p.axis, &p.records, &now_iso()).await?;
        if matches!(action, MonitorAction::Author | MonitorAction::Kept) {
            eligible.push(p);
        }
    }

    // Author for at most max_cycles_per_tick pairs — the codex-heavy step —
    // picking the least-recently-attempted first so coverage rotates and no
    // single tick can spike the shared quota.
    eligible.sort_by(|a, b| a.last_attempt_at.cmp(&b.last_attempt_at));
    let run_count = eligible.len().min(opts.max_cycles_per_tick);
    (d.log)(&format!(
        "[improve-worker] {} pair(s) eligible to author; running {run_count} this tick (cap {})",
        eligible.len(),
        opts.max_cycles_per_tick
    ));

    for p in eligible.into_iter().take(run_count) {
        let now = now_iso();
        let cycle = ImproveCycleOpts {
            skill: p.skill.clone(),
            axis: p.axis,
            provider: opts.provider,
            cluster: opts.cluster,
            holdout: opts.holdout,
            samples: opts.samples,
            abs_max: opts.abs_max,
            bar: opts.bar,
            holdout_min: opts.holdout_min,
            recent_days: opts.recent_days,
            k: opts.k,
            apply: opts.apply,
            max_attempts: opts.max_attempts,
            budget: opts.budget,
            guard_faithfulness: opts.guard_faithfulness,
            now_ms: chrono::Utc::now().timestamp_millis(),
        };
        let axis = p.axis.as_str();
        match run_improve_cycle(d, &cycle).await {
            Ok(result) => {
                let mode = result.mode.as_ref().map(|m| format!(" (mode: {m})")).unwrap_or_default();
                (d.log)(&format!("[{}/{axis}] cycle → {}{mode}", p.skill, result.outcome.as_str()));
                record_cycle(deps, &p.skill, p.axis, Some(&result), &now)?;
                (deps.audit)(json!({
                    "at": now, "kind": "cycle", "skill": p.skill, "axis": axis,
                    "outcome": result.outcome.as_str(), "mode": result.mode,
                    "lesson": result.lesson, "reasons": result.reasons,
                }));
            }
            // A cycle that fails (a flaky generator/judge call, a bad trace)
            // must NOT abort the tick or starve the round-robin: record the
            // attempt (so the pair moves to the back of the queue) and audit
            // the failure so a week of logs shows it too.
            Err(err) => {
                let msg = format!("{err:#}");
                (d.log)(&format!("[{}/{axis}] cycle FAILED: {msg}", p.skill));
                record_cycle(deps, &p.skill, p.axis, None, &now)?;
                (deps.audit)(json!({
                    "at": now, "kind": "cycle", "skill": p.skill, "axis": axis, "outcome": "error", "error": msg,
                }));
            }
        }
    }
    Ok(())
}

pub async fn run_improve_worker(deps: &WorkerDeps<'_>, opts: &ImproveWorkerOpts) {
    let log = deps.cycle.log;
    log(&format!(
        "[improve-worker] provider={} apply={} interval={}ms cap={}/tick maxAttempts={} guardFaith={} recent={}d minMonitorN={} budget={} once={}",
        opts.provider.as_str(),
        opts.apply,
        opts.poll_interval_ms,
        opts.max_cycles_per_tick,
        opts.max_attempts,
        opts.guard_faithfulness,
        opts.recent_days,
        opts.min_monitor_n,
        opts.budget,
        opts.once
    ));
    if !opts.apply {
        log("[improve-worker] SHADOW MODE (IMPROVE_APPLY not set) — proposes + gates but never ships.");
    }
    loop {
        if let Err(err) = tick(deps, opts).await {
            log(&format!("[improve-worker] tick failed: {err:#}"));
        }
        if opts.once {
            break;
        }
        tokio::time::sleep(Duration::from_millis(opts.poll_interval_ms)).await;
    }
}
