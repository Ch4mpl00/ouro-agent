// The improver gate: replay a node under a CANDIDATE patch over its FROZEN
// recorded input, re-judge the result, and decide whether the patch moved the
// target axis BEYOND the judge's own noise (σ_judge, measured by judge-noise).
//
// before = the STORED judgement score (free — we already judged every node once
//          when building the corpus). A single point, so the accept threshold is
//          widened to k·σ·√(1+1/S) to cover its own judge-noise.
// after  = `samples` FRESH generations from the patched prompt, each judged once
//          (generation + judge variance under the patch).
// The judge yardstick stays fixed: it scores against the ORIGINAL contract, not
// the patched one — a patch nudges the GENERATOR, it must not move the goalposts.
//
// REGENERATION is the irreducible cost: re-judging the OLD output under a patched
// contract is meaningless (the yardstick is the ORIGINAL contract, fixed), so the
// only way to see a patch's effect is to re-run the generator. Everything else —
// re-judging "before" S× — is cut: the corpus already gives us the before.

use std::collections::HashMap;

use agent::skills::append_patch;
use async_trait::async_trait;
use serde_json::Value;

use crate::backend::JudgeBackend;
use crate::node_judge::judge_node;
use crate::noise::{AxisScores, NOISE_AXES, NoiseAxis, extract_axis_scores, round};
use crate::schema::{JudgeNodeInput, NodeKind};

// ─── patch injection ─────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
pub struct ChatMessage {
    pub role: String,
    pub content: Value,
}

pub fn as_text(content: &Value) -> String {
    match content {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

// Apply a patch to a recorded chat-message array by appending it to the FIRST
// system message (the skill body for a compose node; the body+tools+skills
// prompt for a planner node). Returns a NEW vec; fails when there is no system
// message to attach to (a malformed recording the gate can't replay). The
// patch goes on with `append_patch`, the SAME function the runtime uses, so
// the gate and prod can't drift.
pub fn patch_messages(messages: &[ChatMessage], patch: &str) -> anyhow::Result<Vec<ChatMessage>> {
    let idx = messages.iter().position(|m| m.role == "system");
    let idx = idx.ok_or_else(|| anyhow::anyhow!("recorded input has no system message to patch"))?;
    let mut out = messages.to_vec();
    out[idx].content = Value::String(append_patch(&as_text(&messages[idx].content), patch));
    Ok(out)
}

// The chat messages a generation was recorded with.
pub fn recorded_messages(input: &Value) -> Vec<ChatMessage> {
    let Value::Array(items) = input else { return Vec::new() };
    items
        .iter()
        .filter_map(|m| {
            let o = m.as_object()?;
            let role = o.get("role")?;
            Some(ChatMessage {
                role: role.as_str().map(str::to_owned).unwrap_or_else(|| role.to_string()),
                content: o.get("content").cloned().unwrap_or(Value::Null),
            })
        })
        .collect()
}

// ─── grading ─────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GateVerdict {
    Improve,
    Regress,
    Noise,
    NoBaseline,
    NotApplicable,
}

impl GateVerdict {
    pub fn as_str(self) -> &'static str {
        match self {
            GateVerdict::Improve => "improve",
            GateVerdict::Regress => "regress",
            GateVerdict::Noise => "noise",
            GateVerdict::NoBaseline => "no-baseline",
            GateVerdict::NotApplicable => "n/a",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AxisGrade {
    pub axis: NoiseAxis,
    pub before_n: usize,
    pub before_mean: Option<f64>,
    pub after_n: usize,
    pub after_mean: Option<f64>,
    pub delta: Option<f64>,
    pub sigma: Option<f64>,
    pub threshold: Option<f64>,
    pub verdict: GateVerdict,
}

fn mean(xs: &[f64]) -> Option<f64> {
    if xs.is_empty() { None } else { Some(xs.iter().sum::<f64>() / xs.len() as f64) }
}

// Pure verdict for one axis. before is the node's single STORED score; after is
// the S fresh patched judgements. A patch's Δ counts only when it clears
// k·σ·√(1+1/S) — the σ envelope around a (single before) vs (S-sample after)
// comparison. Without a σ baseline we can't tell signal from noise, so we abstain.
pub fn grade_axis(axis: NoiseAxis, before: Option<f64>, after: &[f64], sigma: Option<f64>, k: f64) -> AxisGrade {
    let after_mean = mean(after);
    let mut grade = AxisGrade {
        axis,
        before_n: usize::from(before.is_some()),
        before_mean: before.map(|b| round(b, 4)),
        after_n: after.len(),
        after_mean: after_mean.map(|a| round(a, 4)),
        delta: None,
        sigma,
        threshold: None,
        verdict: GateVerdict::NotApplicable,
    };
    let (Some(before), Some(after_mean)) = (before, after_mean) else { return grade };
    let delta = round(after_mean - before, 4);
    grade.delta = Some(delta);
    let Some(sigma) = sigma else {
        grade.verdict = GateVerdict::NoBaseline;
        return grade;
    };
    let threshold = round(k * sigma * (1.0 + 1.0 / after.len() as f64).sqrt(), 4);
    grade.threshold = Some(threshold);
    grade.verdict = if delta > threshold {
        GateVerdict::Improve
    } else if delta < -threshold {
        GateVerdict::Regress
    } else {
        GateVerdict::Noise
    };
    grade
}

// ─── one node through the gate ───────────────────────────────────────

#[derive(Clone, Debug)]
pub struct GateNodeTarget {
    pub observation_id: String,
    pub kind: NodeKind,
    pub skill: String,
    pub label: String,
    pub contract: Option<String>,
    // R — what the node received; the judge scores F against this. Unchanged by
    // the patch (the patch only touches the generator's system message).
    pub input_text: String,
    // F — the recorded output. Kept for display/debug; the "before" score now
    // comes from the stored corpus judgement, not from re-judging this.
    pub original_output: String,
    // Replay material: the recorded chat messages + the model that produced them.
    pub model: String,
    pub recorded_input: Vec<ChatMessage>,
    pub json_mode: bool,
}

// Re-run the generator under the patched prompt. json_mode mirrors production
// (planner emits JSON, composer emits prose).
#[async_trait]
pub trait Generator: Send + Sync {
    async fn run(&self, messages: &[ChatMessage], model: &str, json_mode: bool) -> anyhow::Result<String>;
}

#[derive(Clone, Debug)]
pub struct GatedNode {
    pub observation_id: String,
    pub kind: NodeKind,
    pub skill: String,
    pub label: String,
}

#[derive(Clone, Debug)]
pub struct NodeGateResult {
    pub node: GatedNode,
    pub patched_outputs: Vec<String>,
    pub grades: Vec<AxisGrade>,
}

pub type Sigma = HashMap<NoiseAxis, f64>;

async fn judge_outputs(
    backend: &dyn JudgeBackend,
    target: &GateNodeTarget,
    outputs: &[String],
    skip_faithfulness: bool,
) -> anyhow::Result<HashMap<NoiseAxis, Vec<f64>>> {
    let mut acc: HashMap<NoiseAxis, Vec<f64>> = NOISE_AXES.iter().map(|a| (*a, Vec::new())).collect();
    for output_text in outputs {
        let node = JudgeNodeInput {
            kind: target.kind,
            skill: target.skill.clone(),
            contract: target.contract.clone(),
            input_text: target.input_text.clone(),
            output_text: output_text.clone(),
        };
        let scores = extract_axis_scores(&judge_node(backend, &node, skip_faithfulness).await?);
        for axis in NOISE_AXES {
            if let Some(v) = scores.get(axis) {
                acc.entry(axis).or_default().push(v);
            }
        }
    }
    Ok(acc)
}

// Run the gate for ONE node. Sequential by design — the judge is the bottleneck and
// shares the user's ChatGPT quota. `stored` are the node's existing corpus
// scores (the free "before"); only REGENERATION + after-judging cost judge calls.
#[allow(clippy::too_many_arguments)]
pub async fn run_node_gate(
    backend: &dyn JudgeBackend,
    generator: &dyn Generator,
    target: &GateNodeTarget,
    patch: &str,
    samples: usize,
    sigma: &Sigma,
    k: f64,
    stored: &AxisScores,
    skip_faithfulness: bool,
) -> anyhow::Result<NodeGateResult> {
    // after: generate `samples` fresh outputs from the patched prompt, judge each.
    let patched = patch_messages(&target.recorded_input, patch)?;
    let mut patched_outputs = Vec::new();
    for _ in 0..samples {
        patched_outputs.push(generator.run(&patched, &target.model, target.json_mode).await?);
    }
    let after = judge_outputs(backend, target, &patched_outputs, skip_faithfulness).await?;
    let grades = NOISE_AXES
        .iter()
        .map(|axis| {
            let samples = after.get(axis).map(Vec::as_slice).unwrap_or(&[]);
            grade_axis(*axis, stored.get(*axis), samples, sigma.get(axis).copied(), k)
        })
        .filter(|g| g.before_n > 0 || g.after_n > 0)
        .collect();
    Ok(NodeGateResult {
        node: GatedNode {
            observation_id: target.observation_id.clone(),
            kind: target.kind,
            skill: target.skill.clone(),
            label: target.label.clone(),
        },
        patched_outputs,
        grades,
    })
}
