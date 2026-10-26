use std::sync::{Arc, Mutex};

use agent::models::{ModelPreset, ReasoningEffort};
use agent::providers::{ChatProvider, CompletionParams, CompletionResult, ProviderKind, assistant};
use agent::trace_model::{Observation, ObservationType, Trace, Usage};
use agent::tracing::{TokenUsage, null_scope};
use async_trait::async_trait;
use evals::optimizer::{compact_trace, prompt, proposal_yaml, propose};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

// A definition in the interpreter dialect the workbench runs (owf, until
// the engine migration removes it): the reference loop as the owf era wrote
// it; the reference loop itself and the built-ins are engine definitions.
const DEFINITION: &str = include_str!("../../agent/tests/fixtures/telegram-loop.flat.yaml");

fn observation(id: &str, kind: ObservationType, parent: Option<&str>) -> Observation {
    Observation {
        id: id.into(),
        name: id.into(),
        kind,
        parent_observation_id: parent.map(str::to_owned),
        start_time: "2026-10-22T00:00:00Z".into(),
        end_time: "2026-10-22T00:00:01Z".into(),
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
        latency: 1.0,
    }
}

#[test]
fn evidence_keeps_reasoning_calls_failures_and_ancestry_without_duplicate_transcripts() {
    let reasoning = "Рассуждение ".repeat(200);
    let mut generation = observation("turn", ObservationType::Generation, Some("worker"));
    generation.model = Some("model".into());
    generation.input = json!([{"role":"system", "content":"repeated system prompt"}]);
    generation.output = json!({"reasoning_content": reasoning, "content": "fetch these", "tool_calls": [
        {"id": "c1", "function": {"name": "fetch_article", "arguments": "{\"url\":\"https://example.org\"}"}}
    ]});
    generation.usage = Some(Usage { input: 100, output: 10, total: 110 });
    generation.usage_details = Some(json!({"cached": 60}).as_object().unwrap().clone());
    let mut worker = observation("worker", ObservationType::Agent, Some("root"));
    worker.input = json!({"prompt": "fetch the articles"});
    worker.metadata = Some(json!({"tool": "invoke_sub_agent"}).as_object().unwrap().clone());
    let mut tool = observation("fetch", ObservationType::Tool, Some("worker"));
    tool.metadata = Some(json!({"tool": "fetch_article"}).as_object().unwrap().clone());
    tool.input = json!({"url":"https://example.org"});
    tool.output = json!(serde_json::to_string(&json!({"articles":[{"body":"ё".repeat(1500)}],"cached":true})).unwrap());
    tool.level = "ERROR".into();
    tool.status_message = Some("403 forbidden".into());
    let trace = Trace {
        id: "trace".into(),
        name: "task".into(),
        session_id: None,
        timestamp: "2026-10-22T00:00:00Z".into(),
        input: json!("fetch"),
        output: json!("partial success"),
        metadata: None,
        observations: vec![],
        latency: 1.0,
        total_cost: 99.0,
        tags: vec![],
    };
    let compact = compact_trace(&trace, &[generation.clone(), worker, tool]);
    let nodes = compact["nodes"].as_array().unwrap();
    let turn = nodes.iter().find(|o| o["id"] == "turn").unwrap();
    assert_eq!(turn["output"], generation.output);
    assert_eq!(turn["parent"], "worker");
    let fetched = nodes.iter().find(|o| o["id"] == "fetch").unwrap();
    assert_eq!(fetched["input"], json!({"url":"https://example.org"}));
    assert_eq!(fetched["status"], "403 forbidden");
    assert_eq!(fetched["output"]["truncated"], true);
    assert_eq!(fetched["output"]["shape"]["fields"]["articles"]["length"], 1);
    assert_eq!(fetched["output"]["shape"]["fields"]["cached"]["value"], true);
    assert_eq!(fetched["output"]["ref"], "trace:trace/observation:fetch/output");
    assert_eq!(compact["models"]["model"], json!({"calls":1,"input":100,"output":10,"cached":60,"calls_with_usage":1}));
    let text = compact.to_string();
    assert!(!text.contains("repeated system prompt"));
    assert!(!text.contains("99.0"));
}

#[test]
fn proposal_requires_one_complete_definition_and_checks_the_schema() {
    assert_eq!(proposal_yaml(&format!("Analysis\n```yaml\n{DEFINITION}```\nEvidence")).unwrap(), DEFINITION);
    for bad in [
        "No YAML".to_owned(),
        format!("```yaml\n{DEFINITION}"),
        format!("```yaml\n{DEFINITION}```\n```yml\n{DEFINITION}```"),
        "```yaml\ndocument: {name: missing-required-fields}\ndo: []\n```".into(),
        "```yaml\nnot: [valid\n```".into(),
    ] {
        assert!(proposal_yaml(&bad).is_err(), "must reject incomplete or ambiguous proposal: {bad}");
    }
}

#[derive(Default)]
struct Model {
    calls: Mutex<usize>,
    finish_reason: Option<String>,
}

#[async_trait]
impl ChatProvider for Model {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Deepseek
    }
    async fn complete(&self, params: &mut CompletionParams<'_>) -> anyhow::Result<CompletionResult> {
        *self.calls.lock().unwrap() += 1;
        assert_eq!(params.reasoning_effort, ReasoningEffort::Max);
        assert!(params.tools.is_empty(), "proposal cannot cause external actions");
        assert_eq!(params.messages.len(), 2);
        assert!(params.messages[1]["content"].as_str().unwrap().contains("reference-evidence"));
        Ok(CompletionResult {
            message: assistant(&format!("Proposal\n```yaml\n{DEFINITION}```")),
            finish_reason: self.finish_reason.clone(),
            usage: Some(TokenUsage { input: 100, output: 10, total: 110, cached: None }),
        })
    }
}

#[tokio::test]
async fn one_generation_no_tools_and_usage_returned() {
    let input = prompt("author guide", "optimizer rules", DEFINITION, &[json!({"id":"reference-evidence"})]).unwrap();
    let model = Arc::new(Model::default());
    let provider: Arc<dyn ChatProvider> = model.clone();
    let preset = ModelPreset { model: "deepseek".into(), reasoning_effort: ReasoningEffort::Max };
    let result = propose(&provider, &preset, &input, &null_scope(), CancellationToken::new()).await.unwrap();
    proposal_yaml(&result.report).unwrap();
    assert_eq!(result.usage.unwrap().total, 110);
    assert_eq!(*model.calls.lock().unwrap(), 1);
    assert!(input.system.contains("author guide"));
    assert!(input.system.contains("optimizer rules"));
    assert!(prompt("author", "optimizer", DEFINITION, &[]).is_err());
}

#[tokio::test]
async fn output_limit_and_cancellation_do_not_become_valid_proposals() {
    let input = prompt("author", "optimizer", DEFINITION, &[json!("reference-evidence")]).unwrap();
    let provider: Arc<dyn ChatProvider> =
        Arc::new(Model { finish_reason: Some("length".into()), ..Default::default() });
    let preset = ModelPreset { model: "deepseek".into(), reasoning_effort: ReasoningEffort::Max };
    let error = propose(&provider, &preset, &input, &null_scope(), CancellationToken::new()).await.err().unwrap();
    assert!(error.to_string().contains("output limit"));
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(propose(&provider, &preset, &input, &null_scope(), cancel).await.is_err());
}
