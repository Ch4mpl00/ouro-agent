// Canonical READ shape of a run's trace — what the judge and the improver
// (crates/evals) consume. It mirrors the Langfuse public API response, so the
// local mirror (traces.observations in agent.db) and a Langfuse fetch are
// interchangeable. Field names and order are the TS agent's, so traces it
// mirrored read the same.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ObservationType {
    Generation,
    Span,
    Event,
    Agent,
    Tool,
    Chain,
    Retriever,
    Evaluator,
    Guardrail,
    Embedding,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    pub input: u64,
    pub output: u64,
    pub total: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Observation {
    pub id: String,
    pub name: String,
    #[serde(rename = "type")]
    pub kind: ObservationType,
    pub parent_observation_id: Option<String>,
    pub start_time: String,
    pub end_time: String,
    pub level: String,
    pub status_message: Option<String>,
    pub input: Value,
    pub output: Value,
    pub metadata: Option<Map<String, Value>>,
    pub model: Option<String>,
    pub model_parameters: Option<Map<String, Value>>,
    pub usage: Option<Usage>,
    pub usage_details: Option<Map<String, Value>>,
    pub calculated_total_cost: Option<f64>,
    pub latency: f64,
}

impl Observation {
    pub fn meta(&self, key: &str) -> Option<&Value> {
        self.metadata.as_ref().and_then(|m| m.get(key))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Trace {
    pub id: String,
    pub name: String,
    pub session_id: Option<String>,
    pub timestamp: String,
    pub input: Value,
    pub output: Value,
    pub metadata: Option<Map<String, Value>>,
    pub observations: Vec<Observation>,
    pub latency: f64,
    pub total_cost: f64,
    pub tags: Vec<String>,
}

// A judgeable generation tags itself with its node role under this key, so
// the per-node judge classifies by an explicit contract, not by names.
pub const JUDGE_NODE_META: &str = "judge_node";

// A trace handle updated its output here (including a root continuation).
pub const TRACE_OUTPUT_META: &str = "trace_output";

// Activity-journal contract: "run" on the trace of a production signal (the
// only traces the journal reads), "primary_loop" on the span whose input is
// exactly what the primary loop was given.
pub const ACTIVITY_META: &str = "activity";
pub const ACTIVITY_RUN: &str = "run";
pub const ACTIVITY_PRIMARY_LOOP: &str = "primary_loop";

// Every tool-call span names the tool under this key; consumers never read
// the tool from a span name.
pub const TOOL_META: &str = "tool";

// Which skill composed the output: the first non-empty metadata.skill of an
// observation, else the first of trace.metadata.skills.
pub fn resolve_skill(observations: &[Observation], trace_metadata: Option<&Map<String, Value>>) -> Option<String> {
    for o in observations {
        if let Some(Value::String(skill)) = o.meta("skill")
            && !skill.is_empty()
        {
            return Some(skill.clone());
        }
    }
    match trace_metadata.and_then(|m| m.get("skills")) {
        Some(Value::Array(skills)) => skills.first().and_then(Value::as_str).map(str::to_owned),
        _ => None,
    }
}
