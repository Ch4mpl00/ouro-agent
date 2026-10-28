// The skill-index workflow (migration 0007's text) on the engine: in a dry
// run (design 05, test 4) it generates descriptions with real model steps
// and reads the real memory document, and its writes to memory are
// recorded, never made; on its scheduled fires (design 06, test 9) it runs
// as a task's saved workflow. Restored from tests/pending/skill_index.rs.
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
use support::{Scripted, ToolMcp, recording, task_context};
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
        presets: Presets { instant: p(), base: p(), smart: p(), smartest: p(), compiler: p(), compose: p() },
        skills: vec![],
        skill_store: skills,
        memory: Arc::new(db.clone()),
        tracer,
        decisions: None,
        schedule: Arc::new(agent::schedule::Schedules::in_memory()),
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

// ── the installed workflow on its scheduled fires ───────────────────────────
// Restored from tests/pending/skill_index.rs (design 06, test 9): the
// workflow is activated as the agent activates one, and each run is a fire
// of a task naming it, recorded as its trigger's run records one and run by
// the schedule's drain through `run_saved_workflow`.

#[derive(Default)]
struct Store {
    project: bool,
    doc: Option<Value>,
    writes: usize,
    conflict: bool,
    lost_reply: bool,
    before_publish: Option<Box<dyn FnOnce() + Send>>,
}

struct Fired {
    dir: tempfile::TempDir,
    engine: Arc<agent::agent::Engine>,
    supervisor: Arc<agent::supervisor::Supervisor>,
    llm: Arc<Scripted>,
    memory: Arc<Mutex<Store>>,
    skills: Arc<FsSkillStore>,
}

impl Fired {
    async fn new(replies: &[&str]) -> Self {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("skills.default")).unwrap();
        for name in ["alpha", "beta"] {
            std::fs::write(
                dir.path().join(format!("skills.default/{name}.md")),
                format!("---\ntools: []\n---\n{name} instructions"),
            )
            .unwrap();
        }
        let memory = Arc::new(Mutex::new(Store::default()));
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
                // The signal's session and first-turn context.
                "get_timezone" => json!({"timezone":"UTC"}),
                "journal_tail" => json!({"entries":[]}),
                _ => panic!("unexpected tool {name}"),
            };
            Box::pin(async move { Ok(out.to_string()) })
        }));
        let llm = Arc::new(Scripted::new(replies.iter().map(|s| assistant(s)).collect()));
        let skills = Arc::new(FsSkillStore::new(dir.path()));
        let engine = Self::engine(dir.path(), llm.clone(), Arc::new(MarkedMemory(mcp)), skills.clone());
        engine.ready().await.unwrap();
        support::activate_definition(&engine, "skill-index", &skill_index()).await;
        let supervisor = agent::supervisor::create_supervisor(engine.clone());
        Self { dir, engine, supervisor, llm, memory, skills }
    }

    fn engine(
        root: &std::path::Path,
        llm: Arc<Scripted>,
        mcp: agent::mcp::SharedMcp,
        skills: Arc<FsSkillStore>,
    ) -> Arc<agent::agent::Engine> {
        let p = || ModelPreset { model: "gpt-test".into(), reasoning_effort: ReasoningEffort::Disabled };
        let db = Arc::new(AgentDb::open(root.join("agent.db")).unwrap());
        let (tracer, _) = recording();
        create_engine(EngineDeps {
            providers: Providers { openai: llm.clone(), gemini: llm.clone(), deepseek: llm, openrouter: None },
            mcp,
            presets: Presets { instant: p(), base: p(), smart: p(), smartest: p(), compiler: p(), compose: p() },
            skills: vec![],
            skill_store: skills,
            memory: db.clone(),
            tracer,
            decisions: None,
            schedule: Arc::new(agent::schedule::Schedules::new(db.clone())),
            env: Default::default(),
            db: (*db).clone(),
            store: agent::runs::open_store(&root.join("agent.db")).unwrap(),
            dry_store: agent::runs::open_store_view(&root.join("agent.db")).unwrap(),
        })
    }

    // Fire `n` of a task naming skill-index; the signal it ran as.
    async fn fire(&self, n: i64) -> agent::supervisor::PendingSignal {
        let task = agent::schedule::Task {
            id: 1,
            cron: "0 3 * * *".into(),
            recurring: true,
            prompt: "index".into(),
            source: None,
            workflow: Some("skill-index".into()),
            timezone: "UTC".into(),
            created: "2026-10-26T12:00:00.000Z".into(),
            cancelled: false,
        };
        let fires = &self.engine.deps.schedule.fires;
        fires.record(&format!("schedule:task-1:cron:{n}"), &task, n).await.unwrap();
        let fire = fires.next_pending().await.unwrap().unwrap();
        assert_eq!(agent::schedule::drain(&self.supervisor, None).await.unwrap().ran, 1);
        agent::schedule::signal_of(&fire, None, chrono::Utc::now())
    }

    // The saved workflow's outcome in the signal's run.
    async fn outcome(&self, signal: &agent::supervisor::PendingSignal) -> agent::runs::Status {
        let filter = agent::runs::Runs { parent: Some(format!("signal:{}", signal.id)), ..Default::default() };
        let children = self.engine.control.list(&filter, None, 50).await.unwrap().items;
        let run = children.into_iter().find(|r| r.admission.name == "skill-index").expect("the saved run");
        self.engine.runtime.get(&run.admission.id).await.unwrap().unwrap().outcome.expect("ended")
    }

    async fn completed(&self, n: i64) -> Value {
        let signal = self.fire(n).await;
        match self.outcome(&signal).await {
            agent::runs::Status::Completed(v) => v,
            other => panic!("{other:?}"),
        }
    }

    fn index(&self) -> Value {
        serde_json::from_str(self.memory.lock().unwrap().doc.as_ref().unwrap()["body"].as_str().unwrap()).unwrap()
    }

    // Activates `definition` as a new version of skill-index.
    async fn change(&self, definition: &str) {
        support::activate_definition(&self.engine, "skill-index", definition).await;
    }

    async fn active(&self) -> String {
        let definitions = self.engine.runtime.definitions("skill-index").await.unwrap();
        definitions.versions[&definitions.active.unwrap()].yaml.clone()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn installed_workflow_indexes_once_then_only_changed_skills_and_removes_deleted() {
    let s = Fired::new(&["Alpha routing", "Beta routing", "Alpha revised"]).await;
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
    s.supervisor.shutdown().await;
}

// `main` counted the task rows naming the workflow after a reopen; tasks are
// engine definitions now, so the task is read back with list_scheduled_tasks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prompt_changes_invalidate_descriptions_and_edits_survive_database_reopen() {
    let s = Fired::new(&["Alpha", "Beta", "New alpha", "New beta"]).await;
    s.completed(1).await;
    let original = s.active().await;
    let changed =
        original.replace("Preserve the intended scope;", "Keep distinctions explicit. Preserve the intended scope;");
    assert_ne!(changed, original);
    s.change(&changed).await;
    assert_eq!(s.completed(2).await["generated"], 2);
    let args = json!({"cron_expr":"0 3 * * *","recurring":true,"prompt":"index","workflow":"skill-index"});
    let reply = agent::schedule::tool(&s.engine, "UTC", "schedule_task", args.as_object().unwrap()).await.unwrap();
    assert!(reply.contains("\"ok\":true"), "{reply}");
    s.supervisor.shutdown().await;
    let reopened = Fired::engine(
        s.dir.path(),
        s.llm.clone(),
        Arc::new(MarkedMemory(ToolMcp::returning(vec![], ""))),
        s.skills.clone(),
    );
    let definitions = reopened.runtime.definitions("skill-index").await.unwrap();
    assert_eq!(definitions.versions[&definitions.active.unwrap()].yaml, changed);
    let listed = agent::schedule::tool(&reopened, "UTC", "list_scheduled_tasks", &Map::new()).await.unwrap();
    let listed: Value = serde_json::from_str(&listed).unwrap();
    let naming: Vec<&Value> =
        listed["tasks"].as_array().unwrap().iter().filter(|t| t["workflow"] == "skill-index").collect();
    assert_eq!(naming.len(), 1, "{listed}");
    reopened.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generation_failure_does_not_discard_other_results_and_next_run_retries_it() {
    let s = Fired::new(&["", "Beta", "Alpha recovered"]).await;
    let first = s.completed(1).await;
    assert_eq!(first["errors"].as_array().unwrap().len(), 1);
    assert_eq!(first["indexed"], 1);
    let second = s.completed(2).await;
    assert_eq!(second["generated"], 1);
    assert_eq!(second["indexed"], 2);
    s.supervisor.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_document_edit_is_not_overwritten() {
    let s = Fired::new(&["Alpha", "Beta"]).await;
    s.memory.lock().unwrap().conflict = true;
    let signal = s.fire(1).await;
    assert!(matches!(s.outcome(&signal).await, agent::runs::Status::Faulted(_)));
    assert_eq!(s.memory.lock().unwrap().doc.as_ref().unwrap()["body"], "other writer");
    s.supervisor.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_tracks_overlay_patch_tools_and_routable_without_timestamp_noise() {
    use agent::skills::{SkillStore, catalog_snapshot, hash_value};
    let s = Fired::new(&[]).await;
    let before = catalog_snapshot(&*s.skills).await.unwrap();
    s.skills.save_skill("alpha", "---\nroutable: false\ntools: []\n---\nchanged").await.unwrap();
    let live = catalog_snapshot(&*s.skills).await.unwrap();
    assert_eq!(live["skills"][0]["routable"], false);
    assert_ne!(before["skills"][0]["content_hash"], live["skills"][0]["content_hash"]);
    std::fs::remove_file(s.dir.path().join("skills/alpha.md")).unwrap();
    assert_eq!(catalog_snapshot(&*s.skills).await.unwrap(), before);
    assert_eq!(hash_value(&json!({"a":1,"nested":{"x":2,"y":3}})), hash_value(&json!({"nested":{"y":3,"x":2},"a":1})));
    s.supervisor.shutdown().await;
}

// A lost reply is an at-most-once write's unknown outcome on the engine: the
// run faults instead of sending it again (`main` retried and verified the
// commit, design 03 R-M8); a fire started again joins its signal's run, and
// the next fire finds the document written and writes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_write_response_and_replaying_a_completed_run_do_not_duplicate_writes_or_generations() {
    let s = Fired::new(&["Alpha", "Beta"]).await;
    s.change(&s.active().await.replace("delay: PT2S", "delay: PT0S")).await;
    s.memory.lock().unwrap().lost_reply = true;
    let signal = s.fire(1).await;
    let first = s.outcome(&signal).await;
    assert!(
        matches!(&first, agent::runs::Status::Faulted(e) if e.type_ == "urn:ouro:errors:call.unknown"),
        "{first:?}"
    );
    // Started again (a crash before the fire was marked done): the same run.
    s.supervisor.start(&signal).await.unwrap().unwrap_or_default();
    assert_eq!(s.outcome(&signal).await, first);
    let next = s.completed(2).await;
    assert_eq!(
        (next["generated"].as_u64(), next["indexed"].as_u64(), next["written"].as_bool()),
        (Some(0), Some(2), Some(false))
    );
    assert_eq!(s.memory.lock().unwrap().writes, 1);
    assert_eq!(s.llm.seen.lock().unwrap().len(), 2);
    s.supervisor.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn changed_source_is_not_published_from_an_old_generation() {
    let s = Fired::new(&["Old alpha", "Beta"]).await;
    let path = s.dir.path().join("skills.default/alpha.md");
    s.memory.lock().unwrap().before_publish = Some(Box::new(move || {
        std::fs::write(path, "---\ntools: []\n---\nEdited during generation").unwrap();
    }));
    assert_eq!(s.completed(1).await["indexed"], 1);
    assert!(s.index()["skills"].get("alpha").is_none());
    s.supervisor.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn excluded_skills_are_removed_without_model_calls_and_invalid_metadata_is_rejected() {
    use agent::skills::SkillStore;
    let s = Fired::new(&["Alpha", "Beta"]).await;
    s.completed(1).await;
    s.skills.save_skill("alpha", "---\nroutable: false # internal\ntools: []\n---\nalpha").await.unwrap();
    let result = s.completed(2).await;
    assert_eq!(result["generated"], 0);
    assert_eq!(result["indexed"], 1);
    assert!(s.skills.save_skill("alpha", "---\nroutable: maybe\ntools: []\n---\nalpha").await.is_err());
    s.supervisor.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_corrupt_document_is_never_overwritten() {
    let s = Fired::new(&[]).await;
    s.memory.lock().unwrap().doc = Some(json!({"body":"{broken json","version":1,"searchable":false}));
    let signal = s.fire(1).await;
    assert!(matches!(s.outcome(&signal).await, agent::runs::Status::Faulted(_)));
    assert_eq!(s.memory.lock().unwrap().writes, 0);
    assert!(s.llm.seen.lock().unwrap().is_empty());
    s.supervisor.shutdown().await;
}
