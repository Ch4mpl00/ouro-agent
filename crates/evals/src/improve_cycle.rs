// ONE improvement cycle for a single (skill, axis): select recent failures →
// induce a failure-mode taxonomy → author an append-only lesson for the dominant
// mode (with ≤1 informed retry on a failed gate) → cheap-gate it (stored before,
// cluster S× + holdout S=1) → decide → ship. Pure orchestration over injected
// deps so BOTH the CLI (improve) and the cron worker (improve-worker) drive the
// exact same logic. The live-trend monitor and auto-revert live in the worker
// (they span runs); this is one cycle.

use std::collections::{HashMap, HashSet};

use agent::db::{AgentDb, JudgementFilter, JudgementRecord};
use agent::skills::FsSkillStore;
use agent::trace_model::Observation;
use serde_json::Value;

use crate::backend::{JudgeBackend, JudgeProvider};
use crate::gate::{Generator, NodeGateResult, Sigma, run_node_gate};
use crate::gate_runtime::build_gate_target;
use crate::improver::{
    AuthorArgs, PatchExample, RationaleItem, SelectOpts, author_patch, decide_ship, dominant_mode, induce_taxonomy,
    node_id, select_candidates,
};
use crate::materials::{NodeMaterial, assemble_node_materials};
use crate::monitor::{budget_exceeded, mean};
use crate::noise::{AxisScores, NoiseAxis};
use crate::schema::JUDGE_PROMPT_VERSION;
use crate::source::TraceSource;

// The judge's rationale for one axis out of the stored detail JSON.
pub fn judge_rationale(detail: &Value, axis: NoiseAxis) -> String {
    if axis == NoiseAxis::Faithfulness {
        return detail["faithfulness"]["note"].as_str().unwrap_or_default().to_owned();
    }
    let Some(axes) = detail["scorecard"]["axes"].as_array() else { return String::new() };
    let Some(a) = axes.iter().find(|a| a["axis"] == axis.as_str()) else { return String::new() };
    let rationale = a["rationale"].as_str().unwrap_or_default();
    let evidence = a["evidence"].as_str().filter(|e| !e.is_empty());
    let text = match evidence {
        Some(e) => format!("{rationale} (evidence: {e})"),
        None => rationale.to_owned(),
    };
    text.trim().to_owned()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CycleOutcome {
    // Nothing judged for this (skill, axis) yet.
    NoCorpus,
    // The floor reached the ceiling — no confident recent failures.
    NoCandidates,
    // Taxonomy/material yielded nothing patchable.
    NoMode,
    // The author found no generalizable lesson.
    NoFix,
    // The candidate(s) failed the gate.
    Rejected,
    // Accepted, but the per-skill patch budget is full — not shipped.
    Budget,
    // Passed the gate, but apply=false (propose-only).
    Accepted,
    // Applied to skills/<skill>.patch.md.
    Shipped,
}

impl CycleOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            CycleOutcome::NoCorpus => "no-corpus",
            CycleOutcome::NoCandidates => "no-candidates",
            CycleOutcome::NoMode => "no-mode",
            CycleOutcome::NoFix => "no-fix",
            CycleOutcome::Rejected => "rejected",
            CycleOutcome::Budget => "budget",
            CycleOutcome::Accepted => "accepted",
            CycleOutcome::Shipped => "shipped",
        }
    }
}

#[derive(Clone, Debug)]
pub struct CycleResult {
    pub outcome: CycleOutcome,
    pub mode: Option<String>,
    pub lesson: Option<String>,
    pub reasons: Option<Vec<String>>,
    // Pre-ship recent axis mean + count — the baseline the worker's live monitor
    // compares post-ship prod traces against. Set on Shipped.
    pub baseline: Option<(f64, usize)>,
    pub patch_path: Option<String>,
}

impl CycleResult {
    fn of(outcome: CycleOutcome) -> CycleResult {
        CycleResult { outcome, mode: None, lesson: None, reasons: None, baseline: None, patch_path: None }
    }

    fn with_mode(outcome: CycleOutcome, mode: &str) -> CycleResult {
        CycleResult { mode: Some(mode.to_owned()), ..CycleResult::of(outcome) }
    }
}

#[derive(Clone, Debug)]
pub struct ImproveCycleOpts {
    pub skill: String,
    pub axis: NoiseAxis,
    pub provider: JudgeProvider,
    pub cluster: usize,
    pub holdout: usize,
    // Cluster samples (S); holdout is always S=1.
    pub samples: usize,
    pub abs_max: f64,
    pub bar: f64,
    pub holdout_min: f64,
    pub recent_days: f64,
    pub k: f64,
    pub apply: bool,
    // 1 → no retry; 2 → one informed retry after a failed gate.
    pub max_attempts: usize,
    // Per-skill patch lesson budget (block new ships when full).
    pub budget: usize,
    // Re-judge the faithfulness axis in the gate too (collateral-regression guard).
    // Default OFF: faithfulness is a separate, input-heavy judge call — skipping it
    // when it isn't the target halves the gate's judge cost (target-axis-only).
    pub guard_faithfulness: bool,
    // Now in ms since the epoch, injected for the recent window + testability.
    pub now_ms: i64,
}

pub struct ImproveDeps<'a> {
    pub db: &'a AgentDb,
    pub source: &'a dyn TraceSource,
    pub skills: &'a FsSkillStore,
    pub backend: &'a dyn JudgeBackend,
    pub generator: &'a dyn Generator,
    pub sigma: &'a Sigma,
    pub log: &'a (dyn Fn(&str) + Send + Sync),
}

pub fn iso_millis(ms: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms)
        .unwrap_or_default()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

// Compact feedback for the informed retry: why the previous lesson failed the
// gate, per cluster node (before→after on the target axis + verdict).
fn gate_feedback(reasons: &[String], cluster: &[NodeGateResult], axis: NoiseAxis) -> String {
    let f = |x: Option<f64>| x.map_or("—".into(), |x| format!("{x:.2}"));
    let mut lines = vec![format!("Gate reasons: {}", reasons.join("; "))];
    for n in cluster {
        lines.push(match n.grades.iter().find(|g| g.axis == axis) {
            None => format!("- {}: {} not scored", n.node.label, axis.as_str()),
            Some(g) => format!(
                "- {}: {} {}→{} [{}]",
                n.node.label,
                axis.as_str(),
                f(g.before_mean),
                f(g.after_mean),
                g.verdict.as_str()
            ),
        });
    }
    lines.join("\n")
}

struct Resolved {
    nodes: Vec<NodeMaterial>,
    by_id: HashMap<String, Observation>,
}

pub async fn run_improve_cycle(deps: &ImproveDeps<'_>, opts: &ImproveCycleOpts) -> anyhow::Result<CycleResult> {
    let log = deps.log;
    let (skill, axis) = (opts.skill.as_str(), opts.axis);
    let axis_sigma = deps.sigma.get(&axis).copied();

    let records = deps.db.list_judgements(&JudgementFilter {
        skill,
        provider: opts.provider.as_str(),
        prompt_version: JUDGE_PROMPT_VERSION,
    })?;
    if records.is_empty() {
        return Ok(CycleResult::of(CycleOutcome::NoCorpus));
    }

    let recent_since =
        (opts.recent_days > 0.0).then(|| iso_millis(opts.now_ms - (opts.recent_days * 86_400_000.0) as i64));
    let in_window = |r: &JudgementRecord| recent_since.as_ref().is_none_or(|since| r.started_at >= *since);

    let selection = select_candidates(
        &records,
        axis,
        &SelectOpts {
            holdout_size: opts.holdout,
            abs_max: opts.abs_max,
            bar: opts.bar,
            k: opts.k,
            sigma: axis_sigma,
            holdout_min: opts.holdout_min,
            recent_since: recent_since.clone(),
        },
    );
    let (candidates, holdout) = (selection.candidates, selection.holdout);
    log(&format!("corpus={} · recent candidates={} · holdout={}", records.len(), candidates.len(), holdout.len()));
    if candidates.is_empty() {
        return Ok(CycleResult::of(CycleOutcome::NoCandidates));
    }

    // Pre-ship baseline = mean of the axis over ALL recent nodes (not just the
    // failures) — the skill's current live level the monitor will compare against.
    let recent_scores: Vec<f64> =
        records.iter().filter(|r| in_window(r)).filter_map(|r| AxisScores::from_db(&r.scores).get(axis)).collect();
    let baseline = mean(&recent_scores).map(|m| (m, recent_scores.len()));

    // Taxonomy → dominant mode → cluster.
    let rationale_by_id: HashMap<String, String> =
        candidates.iter().map(|r| (node_id(r), judge_rationale(&r.detail, axis))).collect();
    log(&format!("Inducing failure-mode taxonomy over {} candidate(s)…", candidates.len()));
    let items: Vec<RationaleItem> = candidates
        .iter()
        .map(|r| RationaleItem {
            id: node_id(r),
            rationale: rationale_by_id.get(&node_id(r)).cloned().unwrap_or_default(),
        })
        .collect();
    let taxonomy = induce_taxonomy(deps.backend, skill, axis, &items).await?;
    for m in &taxonomy.modes {
        let known = m.node_ids.iter().filter(|id| rationale_by_id.contains_key(*id)).count();
        log(&format!("  · {} ({known}): {}", m.name, m.description));
    }
    let valid: HashSet<String> = rationale_by_id.keys().cloned().collect();
    let Some(mode) = dominant_mode(&taxonomy, &valid) else { return Ok(CycleResult::of(CycleOutcome::NoMode)) };
    let mode_ids: HashSet<&String> = mode.node_ids.iter().collect();
    let cluster: Vec<JudgementRecord> =
        candidates.iter().filter(|r| mode_ids.contains(&node_id(r))).take(opts.cluster).cloned().collect();
    log(&format!("Dominant mode: \"{}\" — {} · cluster={}", mode.name, mode.description, cluster.len()));

    // Resolve cluster + holdout nodes to replayable material (cache per trace).
    let mut traces: HashMap<String, Resolved> = HashMap::new();
    let mut resolve = async |rec: &JudgementRecord| -> anyhow::Result<Option<(NodeMaterial, Option<Observation>)>> {
        if !traces.contains_key(&rec.trace_id) {
            let (_, observations) = deps.source.get_trace(&rec.trace_id).await?;
            let (_, nodes) = assemble_node_materials(deps.source, deps.skills, &rec.trace_id).await?;
            let by_id = observations.into_iter().map(|o| (o.id.clone(), o)).collect();
            traces.insert(rec.trace_id.clone(), Resolved { nodes, by_id });
        }
        let entry = &traces[&rec.trace_id];
        Ok(entry
            .nodes
            .iter()
            .find(|n| n.observation_id == rec.observation_id)
            .map(|n| (n.clone(), entry.by_id.get(&rec.observation_id).cloned())))
    };

    let mut examples = Vec::new();
    let mut contract: Option<String> = None;
    for rec in &cluster {
        let Some((node, _)) = resolve(rec).await? else { continue };
        contract = node.contract.clone();
        let rationale = judge_rationale(&rec.detail, axis);
        examples.push(PatchExample {
            input_excerpt: node.input_text,
            output: node.output_text,
            judge_rationale: if rationale.is_empty() { "(no rationale recorded)".into() } else { rationale },
        });
    }
    if examples.is_empty() {
        return Ok(CycleResult::of(CycleOutcome::NoMode));
    }

    let existing_patch = deps.skills_read_patch(skill).await?;

    // Short-circuit before spending judge calls on author+gate: if we'd ship but the
    // per-skill budget is full, there's nothing to do until lessons are pruned.
    // (In propose-only mode we still author, to surface the candidate.)
    if opts.apply && budget_exceeded(&existing_patch, opts.budget) {
        log(&format!(
            "patch budget ({}) reached for {skill} — not authoring; prune lessons or raise the budget.",
            opts.budget
        ));
        return Ok(CycleResult::with_mode(CycleOutcome::Budget, &mode.description));
    }

    // Target-axis-only judging: only re-judge faithfulness when it IS the target
    // or the caller explicitly guards it — otherwise skip its costly extra pass.
    let skip_faithfulness = axis != NoiseAxis::Faithfulness && !opts.guard_faithfulness;

    // Author → gate, with ≤(max_attempts−1) informed retries on a failed gate.
    let mut prior_feedback: Option<String> = None;
    let mut last_reasons: Vec<String> = Vec::new();
    let mut last_lesson: Option<String> = None;
    for attempt in 1..=opts.max_attempts.max(1) {
        let authored = author_patch(
            deps.backend,
            &AuthorArgs {
                skill,
                axis,
                contract: contract.as_deref(),
                examples: &examples,
                failure_mode: Some(&mode.description),
                existing_patch: Some(&existing_patch),
                prior_feedback: prior_feedback.as_deref(),
            },
        )
        .await?;
        let lesson = authored.lesson.trim().to_owned();
        log(&format!("[attempt {attempt}] candidate: {}", if lesson.is_empty() { "(empty)" } else { &lesson }));
        if lesson.is_empty() {
            return Ok(CycleResult::with_mode(CycleOutcome::NoFix, &mode.description));
        }
        last_lesson = Some(lesson.clone());

        let mut gated = Vec::new();
        for (recs, samples) in [(&cluster, opts.samples), (&holdout, 1)] {
            let mut out = Vec::new();
            for rec in recs.iter() {
                let Some((node, obs)) = resolve(rec).await? else { continue };
                let Some(target) = build_gate_target(&node, obs.as_ref()) else { continue };
                out.push(
                    run_node_gate(
                        deps.backend,
                        deps.generator,
                        &target,
                        &authored.lesson,
                        samples,
                        deps.sigma,
                        opts.k,
                        &AxisScores::from_db(&rec.scores),
                        skip_faithfulness,
                    )
                    .await?,
                );
            }
            gated.push(out);
        }
        let holdout_results = gated.pop().unwrap_or_default();
        let cluster_results = gated.pop().unwrap_or_default();
        let decision = decide_ship(axis, &cluster_results, &holdout_results);
        for r in &decision.reasons {
            log(&format!("  • {r}"));
        }
        last_reasons = decision.reasons.clone();

        if !decision.accept {
            prior_feedback = Some(gate_feedback(&decision.reasons, &cluster_results, axis));
            continue;
        }

        // Accepted. (Budget was already checked before authoring when apply=true.)
        if !opts.apply {
            return Ok(CycleResult {
                lesson: Some(authored.lesson.clone()),
                reasons: Some(decision.reasons),
                ..CycleResult::with_mode(CycleOutcome::Accepted, &mode.description)
            });
        }
        let merged = if existing_patch.trim().is_empty() {
            format!("{lesson}\n")
        } else {
            format!("{}\n\n{lesson}\n", existing_patch.trim_end())
        };
        let saved = deps.skills.save_patch(skill, &merged).await?;
        log(&format!("SHIPPED → {} ({} bytes)", saved.path, saved.size_bytes));
        return Ok(CycleResult {
            lesson: Some(lesson),
            reasons: Some(decision.reasons),
            baseline,
            patch_path: Some(saved.path),
            ..CycleResult::with_mode(CycleOutcome::Shipped, &mode.description)
        });
    }

    Ok(CycleResult {
        lesson: last_lesson,
        reasons: Some(last_reasons),
        ..CycleResult::with_mode(CycleOutcome::Rejected, &mode.description)
    })
}

impl ImproveDeps<'_> {
    async fn skills_read_patch(&self, skill: &str) -> anyhow::Result<String> {
        use agent::skills::SkillStore;
        Ok(self.skills.read_patch(skill).await?.unwrap_or_default())
    }
}
