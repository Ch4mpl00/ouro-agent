// One generative node lifted out of the observation tree, ready to judge.
// Classification is by an explicit metadata contract, NEVER by observation
// name (names are display-only and can be renamed freely).

use std::collections::HashMap;

use agent::skills::FsSkillStore;
use agent::trace_model::{JUDGE_NODE_META, Observation, ObservationType, Trace};
use serde_json::Value;

use crate::schema::{JudgeNodeInput, NodeKind};
use crate::source::TraceSource;

fn stringify(x: &Value) -> String {
    match x {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        other => serde_json::to_string_pretty(other).unwrap_or_else(|_| other.to_string()),
    }
}

fn js_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => "null".into(),
        other => other.to_string(),
    }
}

// Render a node's recorded input/output for the judge. A generation's input is
// a chat-messages array (system + user) — flatten it to role-labelled blocks
// so the judge reads the contract/system and the candidates the way the model
// saw them. Everything else (a span's input, a plain output string) stringifies.
pub fn render_io(value: &Value) -> String {
    if let Value::Array(items) = value
        && !items.is_empty()
        && items.iter().all(|m| m.as_object().is_some_and(|o| o.contains_key("role")))
    {
        return items
            .iter()
            .map(|m| format!("[{}]\n{}", js_string(&m["role"]), stringify(&m["content"])))
            .collect::<Vec<_>>()
            .join("\n\n");
    }
    stringify(value)
}

// The `skill` is the OWNER (a named skill, or "planner" for the planner node
// and for prompt-only composes the planner authored); the improver aggregates
// by it. `contract` is that owner's rubric text. `input_text`/`output_text`
// are the node's own recorded IO — the judge scores this node in isolation,
// never the whole run.
#[derive(Clone, Debug, PartialEq)]
pub struct NodeMaterial {
    pub observation_id: String,
    pub kind: NodeKind,
    pub skill: String,
    // Display label for the CLI scorecard — the observation name
    // (e.g. "attempt-1", "llm_compose:digest", "step[2]:llm_agent").
    pub label: String,
    pub contract: Option<String>,
    pub input_text: String,
    pub output_text: String,
}

impl NodeMaterial {
    pub fn judge_input(&self) -> JudgeNodeInput {
        JudgeNodeInput {
            kind: self.kind,
            skill: self.skill.clone(),
            contract: self.contract.clone(),
            input_text: self.input_text.clone(),
            output_text: self.output_text.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Classified {
    // The trace root.
    Root,
    // Not a judgeable node.
    Skip,
    Node { kind: NodeKind, skill: String },
}

// True when any ancestor of `obs` is a SPAWNED sub-agent AGENT span — i.e.
// `obs` is a generation/span INSIDE an llm_agent step (its `iter-*` calls),
// which we judge only black-box at the agent-step boundary, never node by node.
// The trace ROOT is itself an AGENT span, but it is NOT a spawned sub-agent —
// every node lives under it. So a root AGENT (no parent) does NOT count; only
// a NESTED AGENT (an actual llm_agent step) black-boxes its descendants.
fn has_agent_ancestor(obs: &Observation, by_id: &HashMap<&str, &Observation>) -> bool {
    let mut parent = obs.parent_observation_id.as_deref().and_then(|p| by_id.get(p));
    let mut hops = 0;
    while let Some(p) = parent {
        if p.kind == ObservationType::Agent && p.parent_observation_id.is_some() {
            return true;
        }
        hops += 1;
        if hops > by_id.len() {
            return false;
        }
        parent = p.parent_observation_id.as_deref().and_then(|id| by_id.get(id));
    }
    false
}

// A node's owner: the named skill if present, else the planner (prompt-only
// composes and any node whose skill is absent are the planner's responsibility).
fn owner_skill(skill: Option<&Value>) -> String {
    match skill {
        Some(Value::String(s)) if !s.is_empty() => s.clone(),
        _ => "planner".into(),
    }
}

// Pure classification of one observation:
//   - generation tagged judge_node="planner"  → planner node (owner planner)
//   - generation tagged judge_node="compose"  → compose node; owner = the
//                                                tagged-along metadata.skill, or
//                                                planner when prompt-only (null)
//   - AGENT-type span (an llm_agent step)      → agent node, judged black-box
//                                                (input→output); its inner
//                                                generations are skipped
// Tool spans, embeddings, events, the trace root, untagged generations, and
// pure container spans (planner/runner/step/parallel) are skipped.
pub fn classify(o: &Observation, trace_name: &str, by_id: &HashMap<&str, &Observation>) -> Classified {
    if o.parent_observation_id.is_none() && o.name == trace_name {
        return Classified::Root;
    }
    // An llm_agent step IS an AGENT span — judged black-box. A nested agent
    // (an agent spawned inside another) is already covered by the outer box.
    if o.kind == ObservationType::Agent {
        if has_agent_ancestor(o, by_id) {
            return Classified::Skip;
        }
        return Classified::Node { kind: NodeKind::Agent, skill: owner_skill(o.meta("skill")) };
    }
    if o.kind != ObservationType::Generation || has_agent_ancestor(o, by_id) {
        return Classified::Skip;
    }
    match o.meta(JUDGE_NODE_META).and_then(Value::as_str) {
        Some("planner") => Classified::Node { kind: NodeKind::Planner, skill: "planner".into() },
        // A compose node carries its own owner skill (null → prompt-only → planner).
        Some("compose") => Classified::Node { kind: NodeKind::Compose, skill: owner_skill(o.meta("skill")) },
        _ => Classified::Skip,
    }
}

pub fn millis(iso: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(iso).map(|t| t.timestamp_millis()).unwrap_or(0)
}

// A durable worker's original AGENT span ends at suspension. Its terminal
// runner (possibly below a continuation) carries the eventual black-box
// output. Keep judging that same agent node, with its completed result.
pub fn node_output<'a>(
    o: &'a Observation,
    observations: &'a [Observation],
    by_id: &HashMap<&str, &Observation>,
) -> &'a Value {
    if o.kind != ObservationType::Agent {
        return &o.output;
    }
    observations
        .iter()
        .filter(|candidate| {
            if !candidate
                .meta("durable_terminal")
                .is_some_and(|v| v == &Value::Bool(true) || v.as_str() == Some("true"))
                || candidate.meta("durable_workflow").and_then(Value::as_str) != Some(agent::agent::AGENT_LOOP_NAME)
            {
                return false;
            }
            let mut parent = candidate.parent_observation_id.as_deref().and_then(|id| by_id.get(id));
            for _ in 0..by_id.len() {
                let Some(p) = parent else {
                    return false;
                };
                if p.kind == ObservationType::Agent {
                    return p.id == o.id;
                }
                parent = p.parent_observation_id.as_deref().and_then(|id| by_id.get(id));
            }
            false
        })
        .max_by_key(|candidate| &candidate.end_time)
        .map(|candidate| &candidate.output)
        .unwrap_or(&o.output)
}

// Walk the observation tree and lift out every JUDGEABLE generative node, in
// execution order.
pub async fn assemble_node_materials(
    source: &dyn TraceSource,
    skills: &FsSkillStore,
    trace_id: &str,
) -> anyhow::Result<(Trace, Vec<NodeMaterial>)> {
    let (trace, observations) = source.get_trace(trace_id).await?;
    let by_id: HashMap<&str, &Observation> = observations.iter().map(|o| (o.id.as_str(), o)).collect();

    // Memoized contract reads — a run can have several compose nodes on the
    // same skill (map stage), and the same skill across nodes.
    let mut contracts: HashMap<String, Option<String>> = HashMap::new();
    contracts.insert("planner".into(), skills.read_skill_raw("planner").await?);

    let mut sorted: Vec<&Observation> = observations.iter().collect();
    sorted.sort_by_key(|o| millis(&o.start_time));

    let mut nodes = Vec::new();
    for o in sorted {
        let Classified::Node { kind, skill } = classify(o, &trace.name, &by_id) else { continue };
        if !contracts.contains_key(&skill) {
            let contract = skills.read_skill_raw(&skill).await?;
            contracts.insert(skill.clone(), contract);
        }
        nodes.push(NodeMaterial {
            observation_id: o.id.clone(),
            kind,
            contract: contracts.get(&skill).cloned().flatten(),
            skill,
            label: o.name.clone(),
            input_text: render_io(&o.input),
            output_text: render_io(node_output(o, &observations, &by_id)),
        });
    }
    Ok((trace, nodes))
}
