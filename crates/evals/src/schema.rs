// The per-node judge's contract: the rubrics (verbatim prompts, versioned),
// the strict response schemas the judge is forced into, and the verdict types
// they parse back to. The judge-trace skill keeps a copy of the three rubrics
// — change them together, and bump JUDGE_PROMPT_VERSION, which re-judges the
// corpus.

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Value, json};

pub const JUDGE_MODEL: &str = "gpt-6.1-sol";
// Per-node rubrics (n2). Supersedes the whole-run v3 prompt: each generative
// node is scored against its OWNER contract, so the signal pins to one skill.
// n2: planner `process` axis hardened to penalize unresolved referential
// ambiguity (pronouns to unseen context → must gather+replan) and to always
// judge the planner substantively.
// n3: calibration — an unresolved referent DEGRADES (weak/ok), it is not a
// fail when the core task still executes; reserve fail for not-accomplished.
// Bump re-judges the corpus at the new version.
pub const JUDGE_PROMPT_VERSION: &str = "n4";

// A judgeable node's owner type. Selects the rubric (planner axes vs composer
// axes) and whether faithfulness applies. `compose` and `agent` share a rubric
// — both produce a final text from a known input.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeKind {
    Planner,
    Compose,
    Agent,
}

impl NodeKind {
    pub fn as_str(self) -> &'static str {
        match self {
            NodeKind::Planner => "planner",
            NodeKind::Compose => "compose",
            NodeKind::Agent => "agent",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Axis {
    Coverage,
    QueryFormulation,
    Composition,
    Process,
}

impl Axis {
    pub fn as_str(self) -> &'static str {
        match self {
            Axis::Coverage => "coverage",
            Axis::QueryFormulation => "query_formulation",
            Axis::Composition => "composition",
            Axis::Process => "process",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Label {
    #[serde(rename = "fail")]
    Fail,
    #[serde(rename = "weak")]
    Weak,
    #[serde(rename = "ok")]
    Ok,
    #[serde(rename = "strong")]
    Strong,
    #[serde(rename = "n/a")]
    NotApplicable,
}

impl Label {
    pub fn as_str(self) -> &'static str {
        match self {
            Label::Fail => "fail",
            Label::Weak => "weak",
            Label::Ok => "ok",
            Label::Strong => "strong",
            Label::NotApplicable => "n/a",
        }
    }
}

// A nullable field that must still be present, as zod's `.nullable()`.
fn present<'de, D: Deserializer<'de>, T: Deserialize<'de>>(d: D) -> Result<Option<T>, D::Error> {
    Option::<T>::deserialize(d)
}

// All axes any rubric can emit. This is the lenient PARSE side (accepts
// whatever a rubric returned); the per-rubric response schema below is the
// strict FORCE side that pins each rubric to exactly its own axes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AxisResult {
    pub axis: Axis,
    pub applicable: bool,
    #[serde(deserialize_with = "present")]
    pub score: Option<f64>,
    pub label: Label,
    pub rationale: String,
    pub evidence: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Scorecard {
    pub axes: Vec<AxisResult>,
    pub overall_note: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Verdict {
    Supported,
    Partial,
    Unsupported,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FaithClaim {
    pub claim: String,
    pub verdict: Verdict,
    pub evidence: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Faithfulness {
    pub applicable: bool,
    pub claims: Vec<FaithClaim>,
    #[serde(deserialize_with = "present")]
    pub score: Option<f64>,
    pub note: String,
}

// Strict JSON-schema generator — one shape, the axis enum varies per rubric so
// the model can only return the axes that node's owner is responsible for.
fn response_schema_for_axes(axes: &[&str]) -> Value {
    json!({
        "type": "object",
        "properties": {
            "axes": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "axis": { "type": "string", "enum": axes },
                        "applicable": { "type": "boolean" },
                        "score": { "type": ["number", "null"] },
                        "label": { "type": "string", "enum": ["fail", "weak", "ok", "strong", "n/a"] },
                        "rationale": { "type": "string" },
                        "evidence": { "type": "string" },
                    },
                    "required": ["axis", "applicable", "score", "label", "rationale", "evidence"],
                    "additionalProperties": false,
                },
            },
            "overall_note": { "type": "string" },
        },
        "required": ["axes", "overall_note"],
        "additionalProperties": false,
    })
}

pub fn planner_response_schema() -> Value {
    response_schema_for_axes(&["query_formulation", "process"])
}

pub fn composer_response_schema() -> Value {
    response_schema_for_axes(&["coverage", "composition"])
}

pub fn faith_response_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "applicable": { "type": "boolean" },
            "claims": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "claim": { "type": "string" },
                        "verdict": { "type": "string", "enum": ["supported", "partial", "unsupported"] },
                        "evidence": { "type": "string" },
                    },
                    "required": ["claim", "verdict", "evidence"],
                    "additionalProperties": false,
                },
            },
            "score": { "type": ["number", "null"] },
            "note": { "type": "string" },
        },
        "required": ["applicable", "claims", "score", "note"],
        "additionalProperties": false,
    })
}

// ─── planner node rubric ─────────────────────────────────────────────
// Judges ONE planner generation: the orchestration decision frozen as a
// Workflow JSON plan, against the planner contract over the signal. No
// retrieval replay — the plan IS the output; execution is deterministic code.
pub const PLANNER_NODE_PROMPT: &str = r#"You are a rigorous evaluation judge for the PLANNER (orchestrator) of an AI agent. The planner reads one signal plus environment context and emits a workflow: a JSON plan of steps — tool calls (search_news, get_telegram_chat_history, send_telegram_message, set_memory, …), llm_compose / llm_agent steps that delegate to a skill, parallel groups, replan, and a terminal. You are scoring the PLAN ITSELF (the orchestration decision), NOT its execution: execution is deterministic code that walks the steps, so a sound plan is a sound run. You did not author the plan and have no stake in it.

Inputs:
- PLANNER_CONTRACT — how the planner should phrase / reformulate / route retrieval and how it should structure the workflow.
- SIGNAL_AND_ENV — the frozen input the planner saw (the signal content + env: timezone, now, watermarks, envContext).
- PLAN — the planner's output: the Workflow JSON (the steps to run).

Score EXACTLY these two axes from 0 to 1 (fail < 0.3, weak < 0.5, ok < 0.75, strong >= 0.75), each with a one-sentence rationale and concrete evidence (a step kind/bind or a query string):
- query_formulation -> the PLANNER_CONTRACT's retrieval rules AND the target topics implied by the signal. Look at the search/RAG steps' arguments (the queries Q, the source routing, sinceISO/limit filters): do they cover the intent's target topics with good retrieval terms, the right sources, and a correct time window? Reward precise, well-routed queries; penalize vague, missing, or mis-routed ones. This axis covers ONLY semantic search/RAG (search_news / list_news / find_notes); when the plan legitimately needs none, set it n/a (applicable=false) — and then process carries the FULL evaluation. (Gathering decisions that are NOT semantic search — e.g. fetching chat history — belong to process, not here.)
- process -> the PLANNER_CONTRACT. Walk the plan step by step: is every step the right tool/skill with sane arguments, in a sensible order; does each binding get consumed downstream (not bound and dropped); are watermarks/memory updated when the contract requires it; is the result delivered the way the contract requires (e.g. send to the right chat); is the terminal/replan structure correct? CRITICAL — does the plan RESOLVE referential ambiguity BEFORE acting: when the signal carries a pronoun or back-reference to unseen prior context (e.g. «ему»/«его»/«там»/«продолжай»/«то же самое»/"the other one"), the plan MUST gather that context (e.g. get_telegram_chat_history) and replan per the contract's ambiguous-context shape — a plan that bakes an unresolved referent into its deliverable (a reminder, reply, or task prompt) has a real defect that DEGRADES the deliverable: dock it (typically to weak/ok, not strong). Calibrate by what the plan ACCOMPLISHES: reserve fail (<0.3) for plans that don't accomplish the task at all; when the core task still executes correctly (e.g. the reminder still fires at the right time, just with an unresolved referent) it is "done but imperfect" → weak/ok, not fail. Score what the deliverable accomplishes for the signal, not just whether the steps are well-formed. Redundant, missing, contradictory, or dangling steps lower the score.

Rules:
- Judge the plan against the contract, not against your own idea of a nicer plan. A different-but-valid plan is not a defect.
- If the signal legitimately calls for a tiny plan (e.g. a one-shot reply), a short correct plan scores high — reward correctness, not elaborateness. But "tidy mechanics" is NOT correctness if the plan ignored ambiguity or fails the signal's actual intent.
- The planner is ALWAYS substantively judged. n/a on query_formulation (no retrieval) is fine, but never sign the planner off with a lenient process just because the steps look clean — always weigh whether the plan satisfies the signal.
- Ground every claim in the PLAN / SIGNAL_AND_ENV. Never invent steps or queries that aren't there."#;

// ─── composer / agent node rubric ────────────────────────────────────
// Judges ONE llm_compose generation (or an llm_agent step black-box): the
// final text the node produced, against the node's skill contract and the
// input it actually received. The composer does not orchestrate — it receives
// candidates as input and writes text; never penalize it for tool calls.
pub const COMPOSER_NODE_PROMPT: &str = r#"You are a rigorous evaluation judge for ONE composer node of an AI agent — a single skill that receives gathered candidates (and any chat history) as INPUT and writes a final text (F). The composer does NOT call tools and does NOT fetch anything; it legitimately receives its material as input. You are scoring THIS node in isolation: its input is everything it had to work with, its output is the text it produced. You did not author it and have no stake in it.

Inputs:
- COMPOSER_CONTRACT — the skill: how candidates should be filtered and the output composed (format, thresholds, length). For a prompt-only node the owner is the planner and the binding instruction is the inline prompt shown in NODE_INPUT — judge against that instruction.
- NODE_INPUT — exactly what the node received: the retrieved candidates / posts / chat history (this is R). The system message is the contract above; the user message carries R.
- NODE_OUTPUT — the text the node produced (this is F).

Score EXACTLY these two axes from 0 to 1 (fail < 0.3, weak < 0.5, ok < 0.75, strong >= 0.75), each with a one-sentence rationale and concrete evidence (an item id or a quoted phrase):
- coverage -> the COMPOSER_CONTRACT. Of what the node received in NODE_INPUT (R), did F include the salient contract-fitting items and drop the noise? Missing a clearly contract-fitting item lowers it; padding with off-contract noise lowers it.
- composition -> the COMPOSER_CONTRACT. Does F respect the contract's EXPLICIT structural rules — the format/shape it prescribes and any stated length or threshold limit? Judge OBJECTIVELY against rules the contract actually states: dock ONLY for a clear, citable CONTRADICTION with such a rule (wrong format, a blown length cap, a violated "stop if < N matches"). Do NOT judge tone, voice, style, polish, or elegance; do NOT penalize anything the contract is silent about — silence is not a violation; factual fabrication is the faithfulness axis's job, not this one. Absent a concrete contract violation, score composition strong.

Rules:
- Obey the contract. If it says "< 3 matches -> short message and stop", a short / empty output is CORRECT when R really held < 3 contract-fitting, non-duplicate items — judge whether the count was right, not whether it produced a long digest.
- NEVER penalize the composer for orchestration. That a result is later sent via a Telegram tool, or that history arrived via a fetch tool, is the planner's job and is not even visible in this node's input. A contract line like "do not call any Telegram tool" describes the composer's role (it composes, it doesn't fetch); it is satisfied as long as F itself doesn't try to call tools.
- If an axis does not apply (an empty output has nothing to compose, nothing in R to cover), set applicable=false, score=null, label="n/a".
- Reward neither length nor fluency. A correct short output beats a verbose wrong one.
- Ground every claim in NODE_INPUT / NODE_OUTPUT. Never invent items that aren't in R."#;

// Faithfulness sub-judge — claim decomposition for a compose/agent node. R is
// the NODE's own input (not the whole run): a claim is grounded iff it appears
// in the snippets this node received.
pub const FAITH_SYSTEM_PROMPT: &str = r#"You are a faithfulness checker for ONE composer node of an AI agent. You verify that every factual claim in the node's final text (F = NODE_OUTPUT) is grounded in the material the node received (R = NODE_INPUT: the retrieved snippets and any chat history). Quoting specifics — numbers, counts, dates, version numbers — not present in R is the single most damaging error class.

Method:
1. Extract ATOMIC factual claims from F. Focus on verifiable specifics: numbers/counts, dates, named entities, concrete events, and comparisons ("up from 63 the day before").
2. For each claim, look for support in R. Verdict:
   - supported — the claim and its specifics appear in some item in R.
   - partial — the gist is backed but a specific (number/date/name) is missing, altered, or aggregated beyond what any single item states.
   - unsupported — no item in R backs it; likely fabricated or editorialized.
3. Cite the supporting (or contradicting) item id as evidence, or "none".

Rules:
- Judge only F's factual content. The text's own header/date line, category labels, and emojis are not claims.
- A hard number synthesized by aggregating several items is at best PARTIAL unless an item states that number.
- If F is empty / a "тихий день" style message with no factual claims, set applicable=false, score=null, claims=[].
- score = (count(supported) + 0.5 * count(partial)) / total_claims, rounded to 2 decimals.
- Ground every verdict in R; never invent item content."#;

// Common rendering of one node's input/output blocks, shared by all three
// user prompts so the judge sees the same evidence framing each time.
fn node_blocks(input_label: &str, input_text: &str, output_label: &str, output_text: &str) -> String {
    let or_empty = |s: &str| if s.is_empty() { "(empty)".to_owned() } else { s.to_owned() };
    format!(
        "<{input_label}>\n{}\n</{input_label}>\n\n<{output_label}>\n{}\n</{output_label}>",
        or_empty(input_text),
        or_empty(output_text)
    )
}

pub fn build_planner_user_prompt(contract: Option<&str>, node_input: &str, node_output: &str) -> String {
    format!(
        "<planner_contract skill=\"planner\">\n{}\n</planner_contract>\n\n{}\n\nScore this plan. Return JSON matching the schema, with exactly these two axes: query_formulation, process.",
        contract.unwrap_or("(planner contract unavailable)"),
        node_blocks("signal_and_env", node_input, "plan", node_output)
    )
}

pub fn build_composer_user_prompt(skill: &str, contract: Option<&str>, node_input: &str, node_output: &str) -> String {
    format!(
        "<composer_contract skill=\"{skill}\">\n{}\n</composer_contract>\n\n{}\n\nScore this node's output. Return JSON matching the schema, with exactly these two axes: coverage, composition.",
        contract.unwrap_or("(no contract found — judge against general digest/answer expectations)"),
        node_blocks("node_input", node_input, "node_output", node_output)
    )
}

pub fn build_faith_user_prompt(contract: Option<&str>, node_input: &str, node_output: &str) -> String {
    format!(
        "<composer_contract>\n{}\n</composer_contract>\n\n{}\n\nExtract F's atomic factual claims (F = node_output) and verify each against R (R = node_input). Return JSON per the schema.",
        contract.unwrap_or("(no contract)"),
        node_blocks("node_input", node_input, "node_output", node_output)
    )
}

// Rubric selector — maps a node kind to its system prompt, the strict response
// schema, and whether the faithfulness sub-judge runs. `compose` and `agent`
// share the composer rubric (both: input -> final text).
pub struct NodeRubric {
    pub kind: NodeKind,
    pub system: &'static str,
    pub response_schema: Value,
    pub faithfulness: bool,
}

impl NodeRubric {
    pub fn build_user_prompt(&self, skill: &str, contract: Option<&str>, input: &str, output: &str) -> String {
        match self.kind {
            NodeKind::Planner => build_planner_user_prompt(contract, input, output),
            _ => build_composer_user_prompt(skill, contract, input, output),
        }
    }
}

pub fn rubric_for(kind: NodeKind) -> NodeRubric {
    match kind {
        NodeKind::Planner => NodeRubric {
            kind,
            system: PLANNER_NODE_PROMPT,
            response_schema: planner_response_schema(),
            faithfulness: false,
        },
        _ => NodeRubric {
            kind,
            system: COMPOSER_NODE_PROMPT,
            response_schema: composer_response_schema(),
            faithfulness: true,
        },
    }
}

// The minimal node shape a judge needs — a structural subset of NodeMaterial,
// so the judge functions don't depend on materials.rs (and its trace IO).
#[derive(Clone, Debug)]
pub struct JudgeNodeInput {
    pub kind: NodeKind,
    pub skill: String,
    pub contract: Option<String>,
    pub input_text: String,
    pub output_text: String,
}

// One node's verdict: the axis scorecard + (for compose/agent) the
// faithfulness pass. `faithfulness` is None for planner nodes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NodeJudgement {
    pub scorecard: Scorecard,
    pub faithfulness: Option<Faithfulness>,
}
