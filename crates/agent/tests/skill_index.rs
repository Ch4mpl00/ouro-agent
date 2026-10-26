// The skill-index workflow (migration 0007's text) in a dry run on the
// engine (design 05, test 4): it generates descriptions with real model
// steps and reads the real memory document, and its writes to memory are
// recorded, never made. Restored from tests/pending/skill_index.rs; the rest
// of that suite runs the workflow on its scheduled fires, which return with
// the schedules step (design 01, step 5), and stays parked there.
mod support;
use agent::{
    agent::{EngineDeps, Providers, create_engine},
    db::AgentDb,
    mcp::Mcp,
    models::{ModelPreset, Presets, ReasoningEffort},
    providers::{ToolDef, assistant},
    skills::FsSkillStore,
    tracing::{Scope, TraceStart},
};
use async_trait::async_trait;
use serde_json::{Map, Value, json};
use std::sync::{Arc, Mutex};
use support::{NoCodex, Scripted, ToolMcp, recording, task_context};
use tokio_util::sync::CancellationToken;

struct MarkedMemory(Arc<ToolMcp>);
#[async_trait]
impl Mcp for MarkedMemory {
    fn tools(&self) -> Vec<ToolDef> {
        self.0.tools()
    }
    fn read_only(&self, name: &str) -> bool {
        name == "read_doc"
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
        self.0.call_tool(name, args, cancel).await
    }
}

#[derive(Default)]
struct Memory {
    doc: Option<Value>,
    writes: usize,
}

// The installed definition as migration 0007 wrote it into the `workflows`
// table main kept versions in (a later migration dropped the table; the
// engine's catalog never imported it).
fn skill_index() -> String {
    let sql = include_str!("../migrations/0007_system_skill_index.sql");
    let start = sql.find("VALUES ('skill-index', 1, '").unwrap() + "VALUES ('skill-index', 1, '".len();
    let end = sql[start..].find("', 'active', ").unwrap() + start;
    sql[start..end].replace("''", "'")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dry_run_generates_without_writing_memory() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("skills.default")).unwrap();
    for name in ["alpha", "beta"] {
        std::fs::write(
            dir.path().join(format!("skills.default/{name}.md")),
            format!("---\ntools: []\n---\n{name} instructions"),
        )
        .unwrap();
    }
    let skills = Arc::new(FsSkillStore::new(dir.path()));
    let memory = Arc::new(Mutex::new(Memory::default()));
    let names = [
        ("read_doc", vec!["project", "doc"]),
        ("create_project", vec!["slug", "title"]),
        ("write_doc", vec!["project", "doc", "body", "expected_version", "searchable", "summary", "rationale"]),
    ];
    let defs = names
        .iter()
        .map(|(name, args)| {
            let props: Map<String, Value> = args.iter().map(|a| (a.to_string(), json!({}))).collect();
            ToolDef::new(
                name,
                "sandbox memory",
                json!({"type":"object","properties":props,"additionalProperties":false}),
            )
        })
        .collect();
    let mcp = ToolMcp::returning(defs, "");
    let state = memory.clone();
    mcp.always(Arc::new(move |name, args, _| {
        let mut m = state.lock().unwrap();
        let out = match name.as_str() {
            "read_doc" => match &m.doc {
                Some(doc) => json!({"ok":true,"doc":doc}),
                None => json!({"ok":false,"error":"project_not_found"}),
            },
            "create_project" => json!({"ok":true}),
            "write_doc" => {
                m.writes += 1;
                m.doc = Some(json!({"body":args["body"],"version":1,"searchable":args["searchable"]}));
                json!({"ok":true})
            }
            _ => panic!("unexpected tool {name}"),
        };
        Box::pin(async move { Ok(out.to_string()) })
    }));
    let llm = Arc::new(Scripted::new(["Alpha", "Beta"].into_iter().map(assistant).collect()));
    let p = || ModelPreset { model: "gpt-test".into(), reasoning_effort: ReasoningEffort::Disabled };
    let (tracer, written) = recording();
    let db = AgentDb::open(dir.path().join("agent.db")).unwrap();
    let (store, dry_store) = agent::runs::memory_store_with_view();
    let engine = create_engine(EngineDeps {
        providers: Providers { openai: llm.clone(), gemini: llm.clone(), deepseek: llm.clone(), openrouter: None },
        mcp: Arc::new(MarkedMemory(mcp)),
        presets: Presets { base: p(), smart: p(), smartest: p(), compiler: p(), compose: p() },
        skills: vec![],
        skill_store: skills,
        memory: Arc::new(db.clone()),
        tracer,
        codex: Arc::new(NoCodex),
        decisions: None,
        schedule: Arc::new(agent::schedule::MemorySchedule::default()),
        env: Default::default(),
        db,
        store,
        dry_store,
    });
    engine.ready().await.unwrap();
    let definition = skill_index();
    let args = json!({"name":"skill-index","definition":definition});
    agent::flows::tool(&engine, "save_workflow", args.as_object().unwrap()).await.unwrap();

    let trace =
        engine.deps.tracer.trace(TraceStart { id: "dry-index".into(), name: "dry".into(), ..Default::default() });
    let scope: Scope = trace.clone();
    let report: Value = serde_json::from_str(
        &agent::sandbox::try_draft(
            &engine,
            json!({"name":"skill-index"}).as_object().unwrap(),
            &task_context("dry-index"),
            &scope,
            &CancellationToken::new(),
        )
        .await
        .unwrap(),
    )
    .unwrap();
    trace.end();
    assert_eq!(report["status"], "completed", "{report:#}");
    assert_eq!(report["output"]["generated"], 2, "{report:#}");
    assert_eq!(memory.lock().unwrap().writes, 0);
    assert!(memory.lock().unwrap().doc.is_none());
    let writes: Vec<&str> =
        report["writes"].as_array().unwrap().iter().map(|w| w["function"].as_str().unwrap()).collect();
    assert_eq!(writes, ["create_project", "write_doc"], "{report:#}");
    assert_eq!(llm.seen.lock().unwrap().len(), 2, "both descriptions generated for real");
    assert!(written.all().iter().any(|t| t.id == "dry-index"));
    engine.shutdown().await;
}
