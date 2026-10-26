// PARKED (not compiled), for the sandbox step (design 01, step 4): a dry
// run of a saved draft records its writes and changes nothing. The versions
// step (design 04) restored the rest of this suite in tests/flows.rs (the
// catalog over the engine, the authoring tools, handover by effect, only an
// active version runs, a definition that no longer validates, a waiting
// saved child); tests/workflow_steps.rs runs a workflow's own steps.
// Dropped (the old run store; the engine's conformance covers them):
// durable_wait_reopens_on_the_pinned_definition_*, fast_child_starts_*,
// a_run_admitted_without_an_anchor_*.
mod support;
use agent::{
    agent::{Engine, EngineDeps, Providers, create_engine},
    db::AgentDb,
    flows::{FlowResult, MemoryWorkflows, Status, WorkflowSignal, WorkflowStore, check, create_flows},
    mcp::Mcp,
    models::{ModelPreset, Presets, ReasoningEffort},
    providers::{ToolDef, assistant},
    trace_model::Observation,
    tracing::{Scope, TraceStart, Tracer},
};
use async_trait::async_trait;
use serde_json::{Map, Value, json};
use std::sync::Arc;
use support::{AnySkill, NoCodex, PersistLog, Scripted, TELEGRAM_LOOP, ToolMcp, TraceLog, recording, task_context};
use tokio_util::sync::CancellationToken;

// ── the world ───────────────────────────────────────────────────────────────

// MCP with the read-only marks a real server sends.
struct Marked {
    inner: Arc<ToolMcp>,
}

const READ_ONLY: &[&str] = &["search_news"];

#[async_trait]
impl Mcp for Marked {
    fn tools(&self) -> Vec<ToolDef> {
        self.inner.tools()
    }
    fn read_only(&self, name: &str) -> bool {
        READ_ONLY.contains(&name)
    }

    fn open_world(&self, _name: &str) -> bool {
        false
    }
    async fn call_tool(
        &self,
        name: &str,
        args: Map<String, Value>,
        cancel: &CancellationToken,
    ) -> anyhow::Result<String> {
        self.inner.call_tool(name, args, cancel).await
    }
}

struct World {
    engine: Arc<Engine>,
    mcp: Arc<ToolMcp>,
    tracer: Arc<dyn Tracer>,
    written: Arc<TraceLog>,
    persist: Arc<PersistLog>,
}

fn world(replies: Vec<&str>) -> World {
    world_with(replies, None)
}

fn world_with(replies: Vec<&str>, decisions: Option<Arc<agent::decisions::DecisionClient>>) -> World {
    let llm: Arc<Scripted> = Arc::new(Scripted::new(replies.into_iter().map(assistant).collect()));
    let schema = |properties: &[&str], required: &[&str]| {
        let properties: Map<String, Value> = properties.iter().map(|p| (p.to_string(), json!({}))).collect();
        json!({"type": "object", "properties": properties, "required": required})
    };
    let defs = vec![
        ToolDef::new("search_news", "search", schema(&["query", "k"], &[])),
        ToolDef::new("send_telegram_message", "send", schema(&["chatId", "text"], &["text"])),
        ToolDef::new("get_next_signal", "pop", schema(&[], &[])),
    ];
    let mcp = ToolMcp::returning(defs, "");
    mcp.always(Arc::new(|name, args, _| {
        let out = match name.as_str() {
            "search_news" => json!({"items": [{"title": "Rust 2.0", "q": args.get("query")}]}).to_string(),
            "send_telegram_message" => json!({"ok": true, "text": args.get("text")}).to_string(),
            other => format!("[tool error] {other} is not here"),
        };
        Box::pin(async move { Ok(out) })
    }));
    let p = |model: &str| ModelPreset { model: model.into(), reasoning_effort: ReasoningEffort::Disabled };
    let (tracer, written) = recording();
    let persist = Arc::new(PersistLog::default());
    let engine = create_engine(EngineDeps {
        providers: Providers { openai: llm.clone(), gemini: llm.clone(), deepseek: llm, openrouter: None },
        mcp: Arc::new(Marked { inner: mcp.clone() }),
        presets: Presets {
            base: p("gpt-5.4-mini"),
            smart: p("gpt-5.4"),
            smartest: p("gpt-5.4"),
            compiler: p("gpt-5.4"),
            compose: p("gpt-5.4"),
        },
        skills: Vec::new(),
        skill_store: Arc::new(AnySkill),
        memory: persist.clone(),
        tracer: tracer.clone(),
        codex: Arc::new(NoCodex),
        decisions,
        env: Default::default(),
        db: agent::db::AgentDb::open(":memory:").unwrap(),
        store: agent::runs::memory_store(),
        schedule: std::sync::Arc::new(agent::schedule::MemorySchedule::default()),
    });
    World { engine, mcp, tracer, written, persist }
}

impl World {
    fn activate(&self, name: &str, definition: &str) {
        let saved = self.engine.deps.workflows.save(name, definition, None).unwrap();
        self.engine.deps.workflows.activate(name, saved.version).unwrap();
    }

    async fn run(&self, name: &str) -> (FlowResult, Vec<Observation>) {
        let trace = self.tracer.trace(TraceStart { id: "scheduler:7".into(), name: "t".into(), ..Default::default() });
        let scope: Scope = trace.clone();
        let signal = WorkflowSignal {
            id: 7,
            source: "scheduler".into(),
            content: "Daily digest tick.".into(),
            env_context: None,
        };
        let flows = create_flows(self.engine.clone());
        let result = flows.run(name, &signal, &task_context("scheduler:7"), &scope, &CancellationToken::new()).await;
        trace.end();
        (result, self.written.all().pop().map(|t| t.observations).unwrap_or_default())
    }

    fn calls(&self) -> Vec<(String, Map<String, Value>)> {
        self.mcp.calls.lock().unwrap().clone()
    }
}

fn named<'a>(observations: &'a [Observation], name: &str) -> &'a Observation {
    observations.iter().find(|o| o.name == name).unwrap_or_else(|| {
        panic!("no observation {name} in {:?}", observations.iter().map(|o| &o.name).collect::<Vec<_>>())
    })
}

const DIGEST: &str = r#"
document: { dsl: '1.0.4', namespace: agent, name: digest, version: '1.0.0' }
do:
  - search:
      call: search_news
      with: { query: '${ "rust since " + (.env.newsLastReadAt // "ever") }' }
  - compose:
      call: llm_compose
      with: { skill: news-digest, prompt: Write the digest., input: { items: '${ .items }' } }
  - send:
      call: send_telegram_message
      with: { text: '${ . }' }
  - mark:
      call: set_memory
      with: { key: news_digest.last_read_at, value: '${ $workflow.input.env.now }' }
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dry_run_records_the_writes_and_changes_nothing() {
    use agent::flows::try_draft;
    let w = world(vec!["Today: Rust 2.0"]);
    let draft = DIGEST.replace("do:\n", "do:\n  - pause: { wait: PT1H }\n");
    w.engine.deps.workflows.save("digest", &draft, None).unwrap();
    let env = task_context("author").env.clone();
    let cancel = CancellationToken::new();
    let trace = w.tracer.trace(TraceStart { id: "author".into(), name: "t".into(), ..Default::default() });
    let scope: Scope = trace.clone();
    let args = |v: Value| v.as_object().unwrap().clone();
    let reply = try_draft(&w.engine, &args(json!({"name": "digest"})), &env, &scope, &cancel).await.unwrap();
    trace.end();
    let reply: Value = serde_json::from_str(&reply).unwrap();
    assert_eq!((reply["status"].as_str(), reply["version"].as_i64()), (Some("completed"), Some(1)), "{reply}");

    assert_eq!(w.calls().iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(), ["search_news"], "only the read ran");
    assert!(w.persist.0.lock().unwrap().is_empty(), "no watermark moved");
    let writes: Vec<(&str, &str)> = reply["writes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|w| (w["function"].as_str().unwrap(), w["task"].as_str().unwrap()))
        .collect();
    assert_eq!(writes, [("send_telegram_message", "/do/3/send"), ("set_memory", "/do/4/mark")]);
    assert_eq!(reply["writes"][0]["arguments"]["text"], "Today: Rust 2.0");
    let compose = reply["steps"].as_array().unwrap().iter().find(|s| s["task"] == "/do/2/compose").unwrap();
    assert_eq!(compose["output"], "Today: Rust 2.0");
    assert_eq!(reply["path"], json!(["pause", "search", "compose", "send", "mark"]));
    assert!(w.engine.deps.workflows.get("digest", Some(1)).unwrap().unwrap().tried, "a completed dry run marks it");
    assert!(reply["next"].as_str().unwrap().contains("fix"), "{reply}");
    let observations = w.written.all().pop().unwrap().observations;
    let run = named(&observations, "dry-run");
    assert_eq!(run.metadata.as_ref().unwrap()["workflow"], "digest");
    assert_eq!(named(&observations, "call:search").parent_observation_id.as_deref(), Some(run.id.as_str()));

    assert!(try_draft(&w.engine, &args(json!({"name": "nope"})), &env, &scope, &cancel).await.is_err());
    w.engine.deps.workflows.save("digest", &DIGEST.replace("call: search_news", "call: search_web"), None).unwrap();
    let invalid = try_draft(&w.engine, &args(json!({"name": "digest"})), &env, &scope, &cancel).await;
    assert!(invalid.unwrap_err().to_string().contains("search_web"));
}
