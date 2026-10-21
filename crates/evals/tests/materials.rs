// A port of packages/agent/src/judging/materials.test.ts: node identity is
// by metadata, not name, and the node list + rendered IO.
use std::collections::HashMap;
use std::path::Path;

use agent::db::TraceSummary;
use agent::skills::FsSkillStore;
use agent::trace_model::{JUDGE_NODE_META, Observation, ObservationType, Trace};
use async_trait::async_trait;
use evals::materials::{Classified, assemble_node_materials, classify};
use evals::schema::NodeKind;
use evals::source::TraceSource;
use serde_json::{Map, Value, json};

// Only the fields classify/render read matter.
fn obs(id: &str, name: &str, kind: ObservationType, parent: Option<&str>, start: &str) -> Observation {
    Observation {
        id: id.into(),
        name: name.into(),
        kind,
        parent_observation_id: parent.map(str::to_owned),
        start_time: start.into(),
        end_time: "2026-06-14T00:00:01.000Z".into(),
        level: "DEFAULT".into(),
        status_message: None,
        input: Value::Null,
        output: Value::Null,
        metadata: None,
        model: None,
        model_parameters: None,
        usage: None,
        usage_details: None,
        calculated_total_cost: None,
        latency: 0.0,
    }
}

fn meta(v: Value) -> Option<Map<String, Value>> {
    v.as_object().cloned()
}

// A realistic news-digest workflow tree: planner attempt → runner → a skill
// compose, a prompt-only compose, an llm_agent step (with an inner iteration),
// plus tool/embedding spans that must be ignored.
fn news_digest_tree(planner_gen_name: &str) -> (Trace, Vec<Observation>) {
    use ObservationType::*;
    // The trace root is an AGENT span — the supervisor / agent loop start every
    // trace with kind agent. The judge must NOT treat this root AGENT as a
    // spawned sub-agent black box, or it skips every node under it.
    let root = obs("root", "news-digest", Agent, None, "2026-06-14T00:00:00.000Z");
    let mut planner = obs("planner", "planner", Chain, Some("root"), "2026-06-14T00:00:00.100Z");
    planner.metadata = meta(json!({ "signal_source": "news-digest" }));
    let mut gen_planner = obs("gen-planner", planner_gen_name, Generation, Some("planner"), "2026-06-14T00:00:00.200Z");
    gen_planner.metadata = meta(json!({ JUDGE_NODE_META: "planner" }));
    gen_planner.input = json!([
        { "role": "system", "content": "planner contract body + tools" },
        { "role": "user", "content": "Source: news-digest\nContent: digest please" },
    ]);
    gen_planner.output = json!("{\"version\":1,\"steps\":[]}");
    let runner = obs("runner", "runner", Chain, Some("root"), "2026-06-14T00:00:01.000Z");
    let step_tool = obs("step-tool", "step[0]:tool", Tool, Some("runner"), "2026-06-14T00:00:01.100Z");
    let emb = obs("emb", "embed", Embedding, Some("step-tool"), "2026-06-14T00:00:01.150Z");
    let mut step_compose =
        obs("step-compose", "step[1]:llm_compose", Chain, Some("runner"), "2026-06-14T00:00:02.000Z");
    step_compose.metadata = meta(json!({ "preset": "smart", "skill": "news-digest", "bind": "digest" }));
    let mut gen_compose =
        obs("gen-compose", "llm_compose:digest", Generation, Some("step-compose"), "2026-06-14T00:00:02.200Z");
    gen_compose.metadata = meta(json!({ JUDGE_NODE_META: "compose", "skill": "news-digest" }));
    gen_compose.input = json!([
        { "role": "system", "content": "news-digest contract" },
        { "role": "user", "content": "<posts>\nitem-1\n</posts>" },
    ]);
    gen_compose.output = json!("📰 digest text");
    let mut step_compose2 =
        obs("step-compose2", "step[2]:llm_compose", Chain, Some("runner"), "2026-06-14T00:00:03.000Z");
    step_compose2.metadata = meta(json!({ "preset": "fast", "skill": null, "bind": "summary" }));
    let mut prompt_only = obs(
        "gen-compose-prompt-only",
        "llm_compose:summary",
        Generation,
        Some("step-compose2"),
        "2026-06-14T00:00:03.200Z",
    );
    prompt_only.metadata = meta(json!({ JUDGE_NODE_META: "compose", "skill": null }));
    prompt_only.input = json!([{ "role": "user", "content": "Summarize: ${digest}" }]);
    prompt_only.output = json!("short summary");
    let mut step_agent = obs("step-agent", "step[3]:llm_agent", Agent, Some("runner"), "2026-06-14T00:00:04.000Z");
    step_agent.metadata = meta(json!({ "skill": "researcher", "bind": "answer" }));
    step_agent.input = json!({ "skill": "researcher", "prompt": "research X" });
    step_agent.output = json!("agent answer");
    let mut agent_iter = obs("agent-iter", "iter-0", Generation, Some("step-agent"), "2026-06-14T00:00:04.200Z");
    agent_iter.metadata = meta(json!({ JUDGE_NODE_META: "compose" }));
    agent_iter.output = json!("inner");
    let observations = vec![
        root,
        planner,
        gen_planner,
        runner,
        step_tool,
        emb,
        step_compose,
        gen_compose,
        step_compose2,
        prompt_only,
        step_agent,
        agent_iter,
    ];
    let trace = Trace {
        id: "trace-1".into(),
        name: "news-digest".into(),
        session_id: Some("scheduler:1".into()),
        timestamp: "2026-06-14T00:00:00.000Z".into(),
        input: Value::Null,
        output: Value::Null,
        metadata: None,
        observations: observations.clone(),
        latency: 0.0,
        total_cost: 0.0,
        tags: vec!["news-digest".into()],
    };
    (trace, observations)
}

struct Fixture((Trace, Vec<Observation>));

#[async_trait]
impl TraceSource for Fixture {
    async fn get_trace(&self, _: &str) -> anyhow::Result<(Trace, Vec<Observation>)> {
        Ok(self.0.clone())
    }

    async fn recent_traces(&self, _: u32, _: Option<(&str, &str)>) -> anyhow::Result<Vec<TraceSummary>> {
        Ok(Vec::new())
    }
}

fn node(kind: NodeKind, skill: &str) -> Classified {
    Classified::Node { kind, skill: skill.into() }
}

fn classified(id: &str) -> Classified {
    let (trace, observations) = news_digest_tree("attempt-1");
    let by_id: HashMap<&str, &Observation> = observations.iter().map(|o| (o.id.as_str(), o)).collect();
    classify(by_id[id], &trace.name, &by_id)
}

#[test]
fn skips_the_trace_root() {
    assert_eq!(classified("root"), Classified::Root);
}

#[test]
fn classifies_the_planner_generation_as_a_planner_node_owned_by_planner() {
    assert_eq!(classified("gen-planner"), node(NodeKind::Planner, "planner"));
}

#[test]
fn classifies_a_skill_compose_as_a_compose_node_owned_by_that_skill() {
    assert_eq!(classified("gen-compose"), node(NodeKind::Compose, "news-digest"));
}

#[test]
fn attributes_a_prompt_only_compose_to_the_planner() {
    assert_eq!(classified("gen-compose-prompt-only"), node(NodeKind::Compose, "planner"));
}

#[test]
fn classifies_an_llm_agent_step_as_an_agent_node_black_box() {
    assert_eq!(classified("step-agent"), node(NodeKind::Agent, "researcher"));
}

#[test]
fn does_not_treat_the_trace_root_agent_as_a_sub_agent() {
    // Regression: the root is an AGENT span; a buggy ancestor walk that counts
    // it would skip every node, yielding 0 judgeable nodes on every real trace.
    let (_, observations) = news_digest_tree("attempt-1");
    assert_eq!(observations[0].kind, ObservationType::Agent);
    assert_eq!(classified("gen-planner"), node(NodeKind::Planner, "planner"));
    assert_eq!(classified("gen-compose"), node(NodeKind::Compose, "news-digest"));
}

#[test]
fn skips_generations_inside_an_agent_span_even_if_tagged() {
    assert_eq!(classified("agent-iter"), Classified::Skip);
}

#[test]
fn skips_tool_spans_embeddings_and_container_spans() {
    for id in ["step-tool", "emb", "planner", "runner", "step-compose"] {
        assert_eq!(classified(id), Classified::Skip, "{id}");
    }
}

#[test]
fn is_independent_of_observation_names() {
    let (trace, observations) = news_digest_tree("generation-1");
    let by_id: HashMap<&str, &Observation> = observations.iter().map(|o| (o.id.as_str(), o)).collect();
    assert_eq!(by_id["gen-planner"].name, "generation-1");
    assert_eq!(classify(by_id["gen-planner"], &trace.name, &by_id), node(NodeKind::Planner, "planner"));
}

#[test]
fn skips_an_untagged_generation() {
    let (trace, mut observations) = news_digest_tree("attempt-1");
    observations.push(obs(
        "x",
        "llm_compose:ghost",
        ObservationType::Generation,
        Some("runner"),
        "2026-06-14T00:00:05.000Z",
    ));
    let by_id: HashMap<&str, &Observation> = observations.iter().map(|o| (o.id.as_str(), o)).collect();
    assert_eq!(classify(by_id["x"], &trace.name, &by_id), Classified::Skip);
}

fn skills() -> FsSkillStore {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    FsSkillStore { live_dir: "/nonexistent-evals-test-skills".into(), defaults_dir: repo.join("skills.default") }
}

#[tokio::test]
async fn returns_one_node_per_judgeable_observation_in_execution_order() {
    let (_, nodes) =
        assemble_node_materials(&Fixture(news_digest_tree("attempt-1")), &skills(), "trace-1").await.unwrap();
    let got: Vec<(NodeKind, &str, &str)> = nodes.iter().map(|n| (n.kind, n.skill.as_str(), n.label.as_str())).collect();
    assert_eq!(
        got,
        [
            (NodeKind::Planner, "planner", "attempt-1"),
            (NodeKind::Compose, "news-digest", "llm_compose:digest"),
            (NodeKind::Compose, "planner", "llm_compose:summary"),
            (NodeKind::Agent, "researcher", "step[3]:llm_agent"),
        ]
    );
}

#[tokio::test]
async fn renders_a_generations_chat_messages_input_as_role_labelled_blocks() {
    let (_, nodes) =
        assemble_node_materials(&Fixture(news_digest_tree("attempt-1")), &skills(), "trace-1").await.unwrap();
    let planner = nodes.iter().find(|n| n.kind == NodeKind::Planner).unwrap();
    assert!(planner.input_text.contains("[system]"));
    assert!(planner.input_text.contains("[user]"));
    assert!(planner.input_text.contains("digest please"));
    assert!(planner.output_text.contains("\"version\":1"));
}

#[tokio::test]
async fn renders_an_agent_spans_object_input_as_json_and_carries_the_result_as_output() {
    let (_, nodes) =
        assemble_node_materials(&Fixture(news_digest_tree("attempt-1")), &skills(), "trace-1").await.unwrap();
    let agent = nodes.iter().find(|n| n.kind == NodeKind::Agent).unwrap();
    assert!(agent.input_text.contains("research X"));
    assert_eq!(agent.output_text, "agent answer");
}

// ── Rust-only: gaps a mutation check found in the ported cases ──────────────

#[tokio::test]
async fn orders_nodes_by_start_time_not_by_where_the_store_put_them() {
    let (trace, mut observations) = news_digest_tree("attempt-1");
    observations.reverse();
    let (_, nodes) = assemble_node_materials(&Fixture((trace, observations)), &skills(), "trace-1").await.unwrap();
    let labels: Vec<&str> = nodes.iter().map(|n| n.label.as_str()).collect();
    assert_eq!(labels, ["attempt-1", "llm_compose:digest", "llm_compose:summary", "step[3]:llm_agent"]);
}

#[test]
fn attributes_a_compose_with_an_empty_skill_to_the_planner() {
    let (trace, mut observations) = news_digest_tree("attempt-1");
    let i = observations.iter().position(|o| o.id == "gen-compose").unwrap();
    observations[i].metadata = meta(json!({ JUDGE_NODE_META: "compose", "skill": "" }));
    let by_id: HashMap<&str, &Observation> = observations.iter().map(|o| (o.id.as_str(), o)).collect();
    assert_eq!(classify(by_id["gen-compose"], &trace.name, &by_id), node(NodeKind::Compose, "planner"));
}

#[test]
fn continued_trace_keeps_root_identity_final_output_and_judgeable_nodes() {
    use ObservationType::*;
    use evals::langfuse_api::trace_from_observations;
    let root = obs("root", "signal:telegram", Agent, None, "2026-06-14T00:00:00.000Z");
    let primary = obs("primary", "agent_loop", Chain, Some("root"), "2026-06-14T00:00:00.100Z");
    let mut resumed = obs("resume", "resume:agent-loop", Span, Some("primary"), "2026-06-14T00:10:00.000Z");
    resumed.end_time = "2026-06-14T00:10:01.000Z".into();
    resumed.output = json!("final answer");
    resumed.metadata = meta(json!({"trace_output":true}));
    let mut gen_after = obs("after", "attempt-1", Generation, Some("resume"), "2026-06-14T00:10:00.100Z");
    gen_after.metadata = meta(json!({JUDGE_NODE_META:"compose"}));
    let worker = obs("worker", "invoke_sub_agent", Agent, Some("primary"), "2026-06-14T00:00:00.200Z");
    let child_resume = obs("worker-resume", "resume:agent-loop", Span, Some("worker"), "2026-06-14T00:09:00.000Z");
    let child_gen =
        obs("worker-generation", "attempt-1", Generation, Some("worker-resume"), "2026-06-14T00:09:00.100Z");
    // Endpoint order is arbitrary; nested continuation output belongs to the root trace.
    let (trace, observations) =
        trace_from_observations("trace", vec![gen_after, resumed, child_resume, child_gen, root, primary, worker]);
    assert_eq!(trace.name, "signal:telegram");
    assert_eq!(trace.output, "final answer");
    let by_id = observations.iter().map(|o| (o.id.as_str(), o)).collect();
    assert!(matches!(
        classify(&observations[0], &trace.name, &by_id),
        Classified::Node { kind: NodeKind::Compose, .. }
    ));
    assert_eq!(classify(&observations[1], &trace.name, &by_id), Classified::Skip);
    assert_eq!(classify(&observations[3], &trace.name, &by_id), Classified::Skip);
}

#[test]
fn a_suspended_workers_agent_material_uses_its_final_continued_result() {
    use ObservationType::*;
    use evals::materials::node_output;
    let worker = obs("worker", "invoke_sub_agent", Agent, Some("root"), "2026-06-14T00:00:00.000Z");
    let resume = obs("resume", "resume:agent-loop", Span, Some("worker"), "2026-06-14T00:10:00.000Z");
    let mut runner = obs("runner", "runner", Chain, Some("resume"), "2026-06-14T00:10:00.100Z");
    runner.metadata = meta(json!({"durable_terminal":"true","durable_workflow":"agent-loop"}));
    runner.output = json!("worker final answer");
    let observations = vec![worker, resume, runner];
    let by_id = observations.iter().map(|o| (o.id.as_str(), o)).collect();
    assert_eq!(node_output(&observations[0], &observations, &by_id), &json!("worker final answer"));
}
