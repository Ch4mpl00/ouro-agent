//! Full sandbox: real MCP over HTTP, throwaway Postgres + SQLite, the agent's
//! engine. The skill-index workflow is dry-run (`try_draft`, design 05), then
//! activated and run on two scheduled fires of a task naming it, through the
//! schedule's drain (design 06, test 9). The optional live test also uses a
//! real model; no production data/pollers.
#[path = "../../agent/tests/support/mod.rs"]
mod agent_support;
mod support;
use agent::{
    agent::{EngineDeps, Providers, create_engine},
    db::AgentDb,
    mcp::{ConnectOptions, Mcp, McpClient, open_http},
    models::default_presets,
    providers::{ChatProvider, HttpApi, OPENAI_BASE_URL, ProviderKind, assistant, provider},
    sandbox::try_draft,
    skills::FsSkillStore,
    tracing::{Scope, TraceStart},
};
use agent_support::{Scripted, activate_definition, recording, task_context};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

// The installed definition as migration 0007 wrote it (crates/agent/tests/skill_index.rs).
fn skill_index() -> String {
    let sql = include_str!("../../agent/migrations/0007_system_skill_index.sql");
    let start = sql.find("VALUES ('skill-index', 1, '").unwrap() + "VALUES ('skill-index', 1, '".len();
    let end = sql[start..].find("', 'active', ").unwrap() + start;
    sql[start..end].replace("''", "'")
}

async fn sandbox(live: bool) {
    let database = support::test_database().await;
    // memory for the index, scheduler for the signal session's time zone.
    let server = support::start_test_server(&database, Some("memory,scheduler")).await.unwrap();
    let mcp = Arc::new(McpClient::connect(open_http(server.url.clone()), ConnectOptions::default()).await.unwrap());
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("skills.default")).unwrap();
    let shipped = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../skills.default");
    for name in ["nashdom-bill", "news-query"] {
        std::fs::copy(shipped.join(format!("{name}.md")), root.path().join(format!("skills.default/{name}.md")))
            .unwrap();
    }
    let db = Arc::new(AgentDb::open(root.path().join("agent.db")).unwrap());
    let llm: Arc<dyn ChatProvider> = if live {
        let env_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.env.agent");
        let values: std::collections::HashMap<String, String> =
            dotenvy::from_path_iter(env_path).unwrap().map(Result::unwrap).collect();
        let key = std::env::var("OPENAI_API_KEY")
            .ok()
            .or_else(|| values.get("OPENAI_API_KEY").cloned())
            .expect("OPENAI_API_KEY");
        let url = values.get("OPENAI_BASE_URL").map_or(OPENAI_BASE_URL, String::as_str);
        Arc::new(provider(ProviderKind::Openai, Arc::new(HttpApi::new(url, &key))))
    } else {
        Arc::new(Scripted::new(
            ["Daily digest", "Topical news", "Daily digest", "Topical news"].into_iter().map(assistant).collect(),
        ))
    };
    let (tracer, _) = recording();
    let engine = create_engine(EngineDeps {
        providers: Providers { openai: llm.clone(), deepseek: llm.clone(), gemini: llm.clone(), openrouter: Some(llm) },
        mcp: mcp.clone(),
        presets: default_presets(),
        skills: vec![],
        skill_store: Arc::new(FsSkillStore::new(root.path())),
        memory: db.clone(),
        schedule: Arc::new(agent::schedule::Schedules::new(db.clone())),
        tracer: tracer.clone(),
        decisions: None,
        env: Default::default(),
        db: (*db).clone(),
        store: agent::runs::open_store(&root.path().join("agent.db")).unwrap(),
        dry_store: agent::runs::open_store_view(&root.path().join("agent.db")).unwrap(),
    });
    engine.ready().await.unwrap();
    let args = json!({"name":"skill-index","definition":skill_index()});
    agent::flows::tool(&engine, "save_workflow", args.as_object().unwrap()).await.unwrap();
    let trace = tracer.trace(TraceStart {
        id: "sandbox-skill-index".into(),
        name: "sandbox-skill-index".into(),
        ..Default::default()
    });
    let scope: Scope = trace.clone();
    let cancel = CancellationToken::new();
    let report: Value = serde_json::from_str(
        &try_draft(&engine, json!({"name":"skill-index"}).as_object().unwrap(), &task_context("dry"), &scope, &cancel)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(report["status"], "completed", "{report}");
    let client = server.pool.get().await.unwrap();
    let count: i64 = client.query_one("SELECT count(*) FROM memory_project_docs", &[]).await.unwrap().get(0);
    assert_eq!(count, 0, "dry run cannot publish a document");
    activate_definition(&engine, "skill-index", &skill_index()).await;
    let supervisor = agent::supervisor::create_supervisor(engine.clone());
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
    for n in [1, 2] {
        let fires = &engine.deps.schedule.fires;
        let fire = fires.record(&format!("schedule:task-1:cron:{n}"), &task, n).await.unwrap();
        assert_eq!(agent::schedule::drain(&supervisor, None).await.unwrap().ran, 1);
        let filter = agent::runs::Runs { parent: Some(format!("signal:{fire}")), ..Default::default() };
        let children = engine.control.list(&filter, None, 50).await.unwrap().items;
        let run = children.into_iter().find(|r| r.admission.name == "skill-index").expect("the saved run");
        let ended = engine.runtime.get(&run.admission.id).await.unwrap().unwrap().outcome;
        let Some(agent::runs::Status::Completed(output)) = ended else { panic!("{ended:?}") };
        assert_eq!(output["errors"], json!([]), "{output}");
        assert_eq!(output["indexed"], 2, "{output}");
        assert_eq!(output["generated"], if n == 1 { 2 } else { 0 });
        println!("sandbox live={live}, run={n}: {output}");
    }
    let row = client
        .query_one("SELECT body_md, version, searchable FROM memory_project_docs WHERE name='skill-index.md'", &[])
        .await
        .unwrap();
    let index: Value = serde_json::from_str(&row.get::<_, String>(0)).unwrap();
    assert_eq!(row.get::<_, i32>(1), 1, "unchanged run does not create another version");
    assert!(!row.get::<_, bool>(2));
    assert!(index["skills"]["nashdom-bill"]["description"].is_string());
    let count: i64 = client.query_one("SELECT count(*) FROM memory_index", &[]).await.unwrap().get(0);
    assert_eq!(count, 0, "catalog does not generate embeddings");
    trace.end();
    supervisor.shutdown().await;
    mcp.close().await;
    drop(client);
    drop(server);
    database.drop_all().await;
}

#[tokio::test]
async fn workflow_runs_against_real_memory_in_isolated_databases() {
    sandbox(false).await;
}

#[tokio::test]
#[ignore = "requires local OpenAI credentials; all storage is isolated"]
async fn live_model_sandbox() {
    sandbox(true).await;
}
