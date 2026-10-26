// PARKED (not compiled), for the sandbox step (design 01, step 4): the
// skill-index workflow is installed and run through `try_draft`.
//! The installed definition runs in a sandbox with a real skill filesystem,
//! SQLite migrations/durable journal, scripted model and versioned memory.
mod support;
use agent::mcp::Mcp;
use agent::{
    agent::{Engine, EngineDeps, Providers, create_engine},
    db::AgentDb,
    flows::WorkflowSignal,
    flows::{FlowResult, WorkflowStore, create_durable_flows, try_draft},
    models::{ModelPreset, Presets, ReasoningEffort},
    providers::{ToolDef, assistant},
    skills::{FsSkillStore, SkillStore, catalog_snapshot, hash_value},
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
    project: bool,
    doc: Option<Value>,
    writes: usize,
    conflict: bool,
    lost_reply: bool,
    before_publish: Option<Box<dyn FnOnce() + Send>>,
}
struct Sandbox {
    dir: tempfile::TempDir,
    db: AgentDb,
    engine: Arc<Engine>,
    llm: Arc<Scripted>,
    memory: Arc<Mutex<Memory>>,
    skills: Arc<FsSkillStore>,
}
impl Sandbox {
    fn new(replies: &[&str]) -> Self {
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
        let db = AgentDb::open(dir.path().join("agent.db")).unwrap();
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
                    None => json!({"ok":false,"error":if m.project {"doc_not_found"} else {"project_not_found"}}),
                },
                "create_project" => {
                    m.project = true;
                    if let Some(f) = m.before_publish.take() {
                        f();
                    }
                    json!({"ok":true})
                }
                "write_doc" => {
                    let version = m.doc.as_ref().map_or(0, |d| d["version"].as_i64().unwrap());
                    if m.conflict {
                        m.doc = Some(json!({"body":"other writer","version":version+1,"searchable":false}));
                        json!({"ok":false,"error":"version_conflict"})
                    } else if args["expected_version"] != version {
                        json!({"ok":false,"error":"version_conflict"})
                    } else {
                        m.writes += 1;
                        m.doc = Some(json!({"body":args["body"],"version":version+1,"searchable":args["searchable"]}));
                        if m.lost_reply {
                            m.lost_reply = false;
                            return Box::pin(async { anyhow::bail!("lost response after commit") });
                        }
                        json!({"ok":true})
                    }
                }
                _ => panic!("unexpected tool {name}"),
            };
            Box::pin(async move { Ok(out.to_string()) })
        }));
        let llm = Arc::new(Scripted::new(replies.iter().map(|s| assistant(s)).collect()));
        let p = || ModelPreset { model: "gpt-test".into(), reasoning_effort: ReasoningEffort::Disabled };
        let (tracer, _) = recording();
        let engine = create_engine(EngineDeps {
            providers: Providers { openai: llm.clone(), gemini: llm.clone(), deepseek: llm.clone(), openrouter: None },
            mcp: Arc::new(MarkedMemory(mcp)),
            presets: Presets { base: p(), smart: p(), smartest: p(), compiler: p(), compose: p() },
            skills: vec![],
            skill_store: skills.clone(),
            memory: Arc::new(db.clone()),
            tracer,
            codex: Arc::new(NoCodex),
            decisions: None,
            workflows: Arc::new(db.clone()),
            schedule: Arc::new(db.clone()),
        });
        Self { dir, db, engine, llm, memory, skills }
    }
    async fn run(&self, id: i64) -> FlowResult {
        let trace = self.engine.deps.tracer.trace(TraceStart {
            id: format!("scheduler:{id}"),
            name: "sandbox-index".into(),
            ..Default::default()
        });
        let scope: Scope = trace.clone();
        let result = create_durable_flows(self.engine.clone(), self.db.clone())
            .run(
                "skill-index",
                &WorkflowSignal { id, source: "scheduler".into(), content: "index".into(), env_context: None },
                &task_context(&format!("scheduler:{id}")),
                &scope,
                &CancellationToken::new(),
            )
            .await;
        trace.end();
        result
    }
    async fn completed(&self, id: i64) -> Value {
        match self.run(id).await {
            FlowResult::Completed(v) => v,
            other => panic!("{other:?}"),
        }
    }
    fn index(&self) -> Value {
        serde_json::from_str(self.memory.lock().unwrap().doc.as_ref().unwrap()["body"].as_str().unwrap()).unwrap()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn installed_workflow_indexes_once_then_only_changed_skills_and_removes_deleted() {
    let s = Sandbox::new(&["Alpha routing", "Beta routing", "Alpha revised"]);
    let first = s.completed(1).await;
    assert_eq!(first["generated"], 2);
    assert_eq!(first["indexed"], 2);
    assert_eq!(first["errors"], json!([]));
    assert_eq!(s.index()["skills"]["alpha"]["description"], "Alpha routing");
    assert_eq!(s.index()["skills"]["beta"]["description"], "Beta routing");
    assert_eq!(s.memory.lock().unwrap().doc.as_ref().unwrap()["searchable"], false);
    let second = s.completed(2).await;
    assert_eq!(second["generated"], 0);
    assert_eq!(second["written"], false);
    assert_eq!(s.memory.lock().unwrap().writes, 1);
    assert_eq!(s.llm.seen.lock().unwrap().len(), 2);
    s.skills.save_patch("alpha", "New boundary").await.unwrap();
    let third = s.completed(3).await;
    assert_eq!(third["generated"], 1);
    assert_eq!(s.index()["skills"]["alpha"]["description"], "Alpha revised");
    std::fs::remove_file(s.dir.path().join("skills.default/beta.md")).unwrap();
    let fourth = s.completed(4).await;
    assert_eq!(fourth["generated"], 0);
    assert_eq!(fourth["indexed"], 1);
    assert!(s.index()["skills"].get("beta").is_none());
    // Each LLM call has only one skill and no catalog / memory write arguments.
    for seen in s.llm.seen.lock().unwrap().iter() {
        assert!(seen.tools.is_empty());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prompt_changes_invalidate_descriptions_and_edits_survive_database_reopen() {
    let s = Sandbox::new(&["Alpha", "Beta", "New alpha", "New beta"]);
    s.completed(1).await;
    let original = s.db.active("skill-index").unwrap().unwrap();
    let changed = original
        .definition
        .replace("Preserve the intended scope;", "Keep distinctions explicit. Preserve the intended scope;");
    let draft = s.db.save("skill-index", &changed, None).unwrap();
    s.db.activate("skill-index", draft.version).unwrap();
    assert_eq!(s.completed(2).await["generated"], 2);
    let reopened = AgentDb::open(s.dir.path().join("agent.db")).unwrap();
    assert_eq!(reopened.active("skill-index").unwrap().unwrap().definition, changed);
    let count: i64 = reopened
        .conn()
        .query_row("SELECT count(*) FROM scheduled_tasks WHERE workflow='skill-index'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_failure_does_not_discard_other_results_and_next_run_retries_it() {
    let s = Sandbox::new(&["", "Beta", "Alpha recovered"]);
    let first = s.completed(1).await;
    assert_eq!(first["errors"].as_array().unwrap().len(), 1);
    assert_eq!(first["indexed"], 1);
    let second = s.completed(2).await;
    assert_eq!(second["generated"], 1);
    assert_eq!(second["indexed"], 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_document_edit_is_not_overwritten() {
    let s = Sandbox::new(&["Alpha", "Beta"]);
    s.memory.lock().unwrap().conflict = true;
    assert!(matches!(s.run(1).await, FlowResult::Faulted { .. }));
    assert_eq!(s.memory.lock().unwrap().doc.as_ref().unwrap()["body"], "other writer");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dry_run_generates_without_writing_memory() {
    let s = Sandbox::new(&["Alpha", "Beta"]);
    let trace =
        s.engine.deps.tracer.trace(TraceStart { id: "dry-index".into(), name: "dry".into(), ..Default::default() });
    let scope: Scope = trace.clone();
    let report: Value = serde_json::from_str(
        &try_draft(
            &s.engine,
            json!({"name":"skill-index"}).as_object().unwrap(),
            &task_context("dry-index").env,
            &scope,
            &CancellationToken::new(),
        )
        .await
        .unwrap(),
    )
    .unwrap();
    assert_eq!(report["status"], "completed", "{report}");
    assert_eq!(s.memory.lock().unwrap().writes, 0);
    assert!(s.memory.lock().unwrap().doc.is_none());
    assert!(report["writes"].as_array().unwrap().iter().any(|w| w["function"] == "write_doc"));
    trace.end();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_tracks_overlay_patch_tools_and_routable_without_timestamp_noise() {
    let s = Sandbox::new(&[]);
    let before = catalog_snapshot(&*s.skills).await.unwrap();
    s.skills.save_skill("alpha", "---\nroutable: false\ntools: []\n---\nchanged").await.unwrap();
    let live = catalog_snapshot(&*s.skills).await.unwrap();
    assert_eq!(live["skills"][0]["routable"], false);
    assert_ne!(before["skills"][0]["content_hash"], live["skills"][0]["content_hash"]);
    std::fs::remove_file(s.dir.path().join("skills/alpha.md")).unwrap();
    assert_eq!(catalog_snapshot(&*s.skills).await.unwrap(), before);
    assert_eq!(hash_value(&json!({"a":1,"nested":{"x":2,"y":3}})), hash_value(&json!({"nested":{"y":3,"x":2},"a":1})));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_write_response_and_replaying_a_completed_run_do_not_duplicate_writes_or_generations() {
    let s = Sandbox::new(&["Alpha", "Beta"]);
    let wf = s.db.active("skill-index").unwrap().unwrap();
    let v = s.db.save("skill-index", &wf.definition.replace("delay: PT2S", "delay: PT0S"), None).unwrap();
    s.db.activate("skill-index", v.version).unwrap();
    s.memory.lock().unwrap().lost_reply = true;
    assert_eq!(s.completed(1).await["indexed"], 2);
    assert_eq!(s.completed(1).await["indexed"], 2);
    assert_eq!(s.memory.lock().unwrap().writes, 1);
    assert_eq!(s.llm.seen.lock().unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn changed_source_is_not_published_from_an_old_generation() {
    let s = Sandbox::new(&["Old alpha", "Beta"]);
    let path = s.dir.path().join("skills.default/alpha.md");
    s.memory.lock().unwrap().before_publish = Some(Box::new(move || {
        std::fs::write(path, "---\ntools: []\n---\nEdited during generation").unwrap();
    }));
    assert_eq!(s.completed(1).await["indexed"], 1);
    assert!(s.index()["skills"].get("alpha").is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn excluded_skills_are_removed_without_model_calls_and_invalid_metadata_is_rejected() {
    let s = Sandbox::new(&["Alpha", "Beta"]);
    s.completed(1).await;
    s.skills.save_skill("alpha", "---\nroutable: false # internal\ntools: []\n---\nalpha").await.unwrap();
    let result = s.completed(2).await;
    assert_eq!(result["generated"], 0);
    assert_eq!(result["indexed"], 1);
    assert!(s.skills.save_skill("alpha", "---\nroutable: maybe\ntools: []\n---\nalpha").await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_corrupt_document_is_never_overwritten() {
    let s = Sandbox::new(&[]);
    s.memory.lock().unwrap().doc = Some(json!({"body":"{broken json","version":1,"searchable":false}));
    assert!(matches!(s.run(1).await, FlowResult::Faulted { .. }));
    assert_eq!(s.memory.lock().unwrap().writes, 0);
    assert!(s.llm.seen.lock().unwrap().is_empty());
}
