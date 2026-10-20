// The improver's brain (Phase 3, part 2): turn a corpus of judged nodes into a
// candidate append-only patch, then decide whether the gate's measured Δ earns
// shipping it. Pure logic lives here (cluster selection, ship decision, prompt
// building); the IO (reading judgements, replaying through the gate, writing the
// .patch.md) lives in improve_cycle.rs.

use std::collections::HashSet;

use agent::db::JudgementRecord;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::backend::{JudgeBackend, JudgeCompletion};
use crate::gate::{AxisGrade, GateVerdict, NodeGateResult};
use crate::noise::{AxisScores, NoiseAxis};

// ─── candidate selection (absolute + σ, never a percentile) ──────────

fn axis_score(r: &JudgementRecord, axis: NoiseAxis) -> Option<f64> {
    AxisScores::from_db(&r.scores).get(axis)
}

pub fn node_id(r: &JudgementRecord) -> String {
    format!("{}:{}", r.trace_id, r.observation_id)
}

pub struct CandidateSelection {
    // All RECENT-window failures on the axis — the raw material the taxonomy
    // groups into failure modes (the patch target is ONE mode, not all of these).
    pub candidates: Vec<JudgementRecord>,
    // All-time high scorers — the "don't break what works" guardrail. Height, not
    // recency, is what makes a gold standard, so this ignores the window.
    pub holdout: Vec<JudgementRecord>,
}

pub struct SelectOpts {
    pub holdout_size: usize,
    // A node is a candidate failure iff score < abs_max (confidently low in
    // absolute terms — so the loop CONVERGES and shuts off on a uniformly-good
    // skill) AND score < bar − k·σ (confidently below a meaningful band edge, not
    // just judge noise on a tight distribution). bar defaults to the "ok" anchor.
    pub abs_max: f64,
    pub bar: f64,
    pub k: f64,
    // The axis's judge-noise σ. None → the σ term is skipped (absolute-only).
    pub sigma: Option<f64>,
    // Gold-standard threshold for the holdout (all-time score ≥ this).
    pub holdout_min: f64,
    // ISO cut: candidates must have started at/after this (recent window). None →
    // no window (consider the whole corpus — used in tests / tiny corpora).
    pub recent_since: Option<String>,
}

// Split the corpus into RECENT low-score candidates (the taxonomy's input) and
// an all-time high-score holdout (regression guard), by one axis. Nodes where
// the axis is null (the rubric didn't emit it / n/a) carry no signal and are
// ignored. Absolute + σ, never a percentile: a percentile never converges
// (there's always a bottom X%), ignores σ, and is noisy at our small N.
pub fn select_candidates(records: &[JudgementRecord], axis: NoiseAxis, opts: &SelectOpts) -> CandidateSelection {
    let scored: Vec<(&JudgementRecord, f64)> =
        records.iter().filter_map(|r| axis_score(r, axis).map(|s| (r, s))).collect();
    let sigma_floor = opts.sigma.map_or(f64::INFINITY, |sigma| opts.bar - opts.k * sigma);
    let mut candidates: Vec<(&JudgementRecord, f64)> = scored
        .iter()
        .filter(|(_, s)| *s < opts.abs_max && *s < sigma_floor)
        .filter(|(r, _)| opts.recent_since.as_ref().is_none_or(|since| r.started_at >= *since))
        .copied()
        .collect();
    candidates.sort_by(|a, b| a.1.total_cmp(&b.1));
    let candidate_ids: HashSet<String> = candidates.iter().map(|(r, _)| node_id(r)).collect();
    let mut holdout: Vec<(&JudgementRecord, f64)> = scored
        .iter()
        .filter(|(r, s)| *s >= opts.holdout_min && !candidate_ids.contains(&node_id(r)))
        .copied()
        .collect();
    holdout.sort_by(|a, b| b.1.total_cmp(&a.1));
    holdout.truncate(opts.holdout_size);
    CandidateSelection {
        candidates: candidates.into_iter().map(|(r, _)| r.clone()).collect(),
        holdout: holdout.into_iter().map(|(r, _)| r.clone()).collect(),
    }
}

// ─── failure-mode taxonomy (open-coding, improve-time) ───────────────

// A patch fixes a recurring failure MODE, not a number. The lowest-N nodes may
// fail for unrelated reasons (one patch can't fix them); nodes sharing a judge
// complaint are patchable. So between selection and authoring we induce a
// taxonomy over the candidates' rationales and patch the most frequent mode
// (Pareto). At our N (~40–60 short rationales) one LLM open-coding call beats
// embeddings: human-readable, gives frequencies for free, no threshold tuning.
// (Embedding-then-cluster on distilled phrases is the SCALE-UP path.)
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FailureMode {
    pub name: String,
    // Instance-free atomic failure phrase — also the form a general patch takes.
    pub description: String,
    pub node_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct Taxonomy {
    pub modes: Vec<FailureMode>,
}

pub fn taxonomy_response_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "modes": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": { "type": "string" },
                        "description": { "type": "string" },
                        "nodeIds": { "type": "array", "items": { "type": "string" } },
                    },
                    "required": ["name", "description", "nodeIds"],
                    "additionalProperties": false,
                },
            },
        },
        "required": ["modes"],
        "additionalProperties": false,
    })
}

pub const TAXONOMY_SYSTEM: &str = r#"You are doing OPEN-CODING of an AI agent's failures on ONE quality axis. You are given short judge rationales, each for one low-scoring node (identified by an id). Induce a small taxonomy of recurring FAILURE MODES.

Rules:
- 3–7 named modes, fewer if the rationales genuinely share fewer patterns. Do NOT invent modes to pad the list.
- Each mode's description is ONE atomic, instance-FREE failure phrase — strip item ids, names, dates, quotes (those are evidence, not the pattern). It should read like the lesson a patch would teach. Example: "omits a concrete infra/data-architecture item the contract asks to include".
- Assign every node id to EXACTLY ONE mode (its dominant complaint). A rationale mixing two complaints goes to the more severe / more frequent one.
- Group by the UNDERLYING defect, not by the judge's wording — two differently-phrased rationales describing the same defect are the same mode.

Return JSON: { "modes": [ { "name": "<short kebab/Title>", "description": "<atomic instance-free phrase>", "nodeIds": ["<id>", ...] }, ... ] }."#;

// JS String.prototype.slice on UTF-16 units, kept to char boundaries.
pub fn truncate(s: &str, n: usize) -> String {
    let units: usize = s.encode_utf16().count();
    if units <= n {
        return s.to_owned();
    }
    let mut taken = 0;
    let mut end = 0;
    for (i, c) in s.char_indices() {
        if taken + c.len_utf16() > n {
            break;
        }
        taken += c.len_utf16();
        end = i + c.len_utf8();
    }
    format!("{}… (+{} chars)", &s[..end], units - n)
}

pub struct RationaleItem {
    pub id: String,
    pub rationale: String,
}

pub fn build_taxonomy_user_prompt(skill: &str, axis: NoiseAxis, items: &[RationaleItem]) -> String {
    let blocks = items
        .iter()
        .map(|it| {
            let rationale = if it.rationale.is_empty() { "(no rationale recorded)" } else { &it.rationale };
            format!("- id={}: {}", it.id, truncate(rationale, 600))
        })
        .collect::<Vec<_>>()
        .join("\n");
    [
        format!("SKILL: {skill}"),
        format!("AXIS: {}", axis.as_str()),
        format!("{} low-scoring node rationale(s):", items.len()),
        blocks,
        "\nInduce the failure-mode taxonomy per your instructions. Assign every id exactly once. Return JSON.".into(),
    ]
    .join("\n\n")
}

pub async fn induce_taxonomy(
    backend: &dyn JudgeBackend,
    skill: &str,
    axis: NoiseAxis,
    items: &[RationaleItem],
) -> anyhow::Result<Taxonomy> {
    let user = build_taxonomy_user_prompt(skill, axis, items);
    let schema = taxonomy_response_schema();
    let json = backend
        .complete(&JudgeCompletion { name: "taxonomy", system: TAXONOMY_SYSTEM, user: &user, schema: &schema })
        .await?;
    Ok(serde_json::from_value(json)?)
}

// Pick the Pareto-dominant mode (most members), keeping only ids that are real
// candidates (the LLM can hallucinate an id). Ties broken by first-seen order.
pub fn dominant_mode(taxonomy: &Taxonomy, valid_ids: &HashSet<String>) -> Option<FailureMode> {
    let cleaned: Vec<FailureMode> = taxonomy
        .modes
        .iter()
        .map(|m| FailureMode {
            node_ids: m.node_ids.iter().filter(|id| valid_ids.contains(*id)).cloned().collect(),
            ..m.clone()
        })
        .filter(|m| !m.node_ids.is_empty())
        .collect();
    cleaned.into_iter().reduce(|best, m| if m.node_ids.len() > best.node_ids.len() { m } else { best })
}

// ─── patch author ────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct AuthoredPatch {
    pub lesson: String,
    pub rationale: String,
}

pub fn patch_response_schema() -> Value {
    json!({
        "type": "object",
        "properties": { "lesson": { "type": "string" }, "rationale": { "type": "string" } },
        "required": ["lesson", "rationale"],
        "additionalProperties": false,
    })
}

// Kept deliberate and constrained: the author writes ONE appended lesson, never
// a rewrite, never a contradiction of the body. Overfitting to a single example
// is the failure mode to avoid — the lesson must generalize.
pub const PATCH_AUTHOR_SYSTEM: &str = r#"You improve an AI agent's skill by writing ONE short APPEND-ONLY patch: extra instruction text that will be appended AFTER the skill's existing body (which you must NOT rewrite or contradict). You are given the skill's contract, the axis that is scoring low, and concrete failing examples (each with the input the node received, the output it produced, and the judge's rationale for the low score).

Write a lesson that fixes the PATTERN behind these failures on that axis:
- Concrete but GENERAL — a rule the agent can apply to new inputs, not a fix hard-coded to one example. Do not name specific items/dates from the examples.
- Minimal — one or a few markdown bullet points. Token-cheap. No preamble.
- Compatible — it must not contradict or duplicate a rule already in the contract; it sharpens or adds, never overrides.
- On-axis — target the specific failure the judge described; do not drift to other concerns.
- NON-REDUNDANT — you are shown the patch lessons already appended to this skill. Do NOT restate, rephrase, or partially overlap an existing lesson. If the current failure is already covered by an existing lesson, return an empty lesson (the fix is in place; the lows are something else).
- If the examples reveal NO generalizable, fixable pattern (the lows look like judge noise or one-off input problems), or the pattern is already patched, say so in rationale and return an empty lesson.

Return JSON: { "lesson": "<markdown bullets, or empty>", "rationale": "<one or two sentences: the pattern you found and why this lesson fixes it>" }."#;

#[derive(Clone, Debug)]
pub struct PatchExample {
    pub input_excerpt: String,
    pub output: String,
    pub judge_rationale: String,
}

pub struct AuthorArgs<'a> {
    pub skill: &'a str,
    pub axis: NoiseAxis,
    pub contract: Option<&'a str>,
    pub examples: &'a [PatchExample],
    pub failure_mode: Option<&'a str>,
    pub existing_patch: Option<&'a str>,
    pub prior_feedback: Option<&'a str>,
}

pub fn build_author_user_prompt(a: &AuthorArgs<'_>) -> String {
    let axis = a.axis.as_str();
    let blocks = a
        .examples
        .iter()
        .enumerate()
        .map(|(i, e)| {
            [
                format!("### Failing example {}", i + 1),
                format!("INPUT (excerpt):\n{}", truncate(&e.input_excerpt, 2000)),
                format!("OUTPUT:\n{}", truncate(&e.output, 2000)),
                format!("JUDGE ({axis}): {}", e.judge_rationale),
            ]
            .join("\n\n")
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    let existing = a.existing_patch.map(str::trim).filter(|p| !p.is_empty()).unwrap_or("(none yet)");
    [
        format!("SKILL: {}", a.skill),
        format!("LOW-SCORING AXIS: {axis}"),
        a.failure_mode
            .filter(|m| !m.is_empty())
            .map(|m| format!("TARGET FAILURE MODE (fix THIS recurring pattern): {m}"))
            .unwrap_or_default(),
        format!("SKILL_CONTRACT:\n{}", a.contract.unwrap_or("(none — prompt-only node owned by the planner)")),
        format!("EXISTING PATCH (already appended — do NOT repeat or overlap these lessons):\n{existing}"),
        a.prior_feedback
            .filter(|f| !f.is_empty())
            .map(|f| {
                format!(
                    "PREVIOUS ATTEMPT FAILED THE GATE — do NOT repeat that angle; try a different, sharper fix:\n{f}"
                )
            })
            .unwrap_or_default(),
        format!("\n{} failing example(s) of this mode:\n\n{blocks}", a.examples.len()),
        "\nWrite the append-only patch per your instructions. Return JSON.".into(),
    ]
    .into_iter()
    .filter(|s| !s.is_empty())
    .collect::<Vec<_>>()
    .join("\n\n")
}

pub async fn author_patch(backend: &dyn JudgeBackend, args: &AuthorArgs<'_>) -> anyhow::Result<AuthoredPatch> {
    let user = build_author_user_prompt(args);
    let schema = patch_response_schema();
    let json = backend
        .complete(&JudgeCompletion { name: "patch", system: PATCH_AUTHOR_SYSTEM, user: &user, schema: &schema })
        .await?;
    Ok(serde_json::from_value(json)?)
}

// ─── ship decision ───────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
pub struct ShipDecision {
    pub accept: bool,
    pub reasons: Vec<String>,
    pub cluster_improve: usize,
    pub cluster_regress: usize,
}

fn target_grades(results: &[NodeGateResult], axis: NoiseAxis) -> Vec<&AxisGrade> {
    results.iter().flat_map(|n| n.grades.iter().filter(move |g| g.axis == axis)).collect()
}

// Accept a patch iff it NET-improves the target axis on the cluster AND causes
// no regression anywhere (cluster collateral OR holdout) — conservative, since
// shipping is automatic. A single regress kills it: prod is the ground truth,
// and a live-trend monitor (part 3) is the second line, not this gate.
pub fn decide_ship(axis: NoiseAxis, cluster: &[NodeGateResult], holdout: &[NodeGateResult]) -> ShipDecision {
    let mut reasons = Vec::new();
    let target = target_grades(cluster, axis);
    let cluster_improve = target.iter().filter(|g| g.verdict == GateVerdict::Improve).count();
    let cluster_regress = target.iter().filter(|g| g.verdict == GateVerdict::Regress).count();
    let net_improve = cluster_improve >= 1 && cluster_improve > cluster_regress;
    if !net_improve {
        reasons.push(format!(
            "target axis \"{}\" did not net-improve on the cluster (improve={cluster_improve}, regress={cluster_regress})",
            axis.as_str()
        ));
    }
    let any_regress =
        cluster.iter().chain(holdout).flat_map(|n| n.grades.iter()).any(|g| g.verdict == GateVerdict::Regress);
    if any_regress {
        reasons.push("a regression appeared on the cluster or holdout (any axis)".into());
    }
    let accept = net_improve && !any_regress;
    if accept {
        reasons
            .push(format!("accepted: {} improved on {cluster_improve} cluster node(s), no regressions", axis.as_str()));
    }
    ShipDecision { accept, reasons, cluster_improve, cluster_regress }
}
