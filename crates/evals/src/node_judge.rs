// Provider-agnostic per-node judging. Selects the rubric by node kind, builds
// the prompts, and validates the backend's JSON — the same for every provider.
// The only provider-specific part (running the completion) is the injected
// JudgeBackend.

use crate::backend::{JudgeBackend, JudgeCompletion};
use crate::schema::{
    FAITH_SYSTEM_PROMPT, Faithfulness, JudgeNodeInput, NodeJudgement, Scorecard, build_faith_user_prompt,
    faith_response_schema, rubric_for,
};

async fn scorecard_for(backend: &dyn JudgeBackend, node: &JudgeNodeInput) -> anyhow::Result<Scorecard> {
    let rubric = rubric_for(node.kind);
    let user = rubric.build_user_prompt(&node.skill, node.contract.as_deref(), &node.input_text, &node.output_text);
    let json = backend
        .complete(&JudgeCompletion {
            name: "scorecard",
            system: rubric.system,
            user: &user,
            schema: &rubric.response_schema,
        })
        .await?;
    Ok(serde_json::from_value(json)?)
}

async fn faithfulness_for(backend: &dyn JudgeBackend, node: &JudgeNodeInput) -> anyhow::Result<Faithfulness> {
    let user = build_faith_user_prompt(node.contract.as_deref(), &node.input_text, &node.output_text);
    let schema = faith_response_schema();
    let json = backend
        .complete(&JudgeCompletion { name: "faithfulness", system: FAITH_SYSTEM_PROMPT, user: &user, schema: &schema })
        .await?;
    Ok(serde_json::from_value(json)?)
}

// Judge ONE node: its axis scorecard, plus the faithfulness pass for
// compose/agent nodes (planner nodes have no faithfulness axis). The two
// completions run concurrently.
//
// `skip_faithfulness` is the gate's cost knob (target-axis-only judging): the
// faithfulness pass is a SEPARATE backend call over the node's (often large)
// input, so when the improver gates a non-faithfulness axis it can drop it and
// halve the judge cost per sample. The judge-worker never skips — the stored
// corpus keeps all axes.
pub async fn judge_node(
    backend: &dyn JudgeBackend,
    node: &JudgeNodeInput,
    skip_faithfulness: bool,
) -> anyhow::Result<NodeJudgement> {
    let wants_faith = rubric_for(node.kind).faithfulness && !skip_faithfulness;
    let (scorecard, faithfulness) = futures::join!(scorecard_for(backend, node), async {
        if wants_faith { faithfulness_for(backend, node).await.map(Some) } else { Ok(None) }
    });
    Ok(NodeJudgement { scorecard: scorecard?, faithfulness: faithfulness? })
}
