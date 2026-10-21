//! Full sandbox: real MCP over HTTP, throwaway Postgres + SQLite, real OWF.
//! The optional live test also uses a real model; no production data/pollers.
#[path = "../../agent/tests/support/mod.rs"]
mod agent_support;
mod support;
use agent::{
    agent::{EngineDeps, Providers, create_engine},
    db::AgentDb,
    flows::WorkflowSignal,
    flows::{FlowResult, FlowRun, create_durable_flows, try_draft},
    mcp::{ConnectOptions, Mcp, McpClient, open_http},
    models::default_presets,
    providers::{ChatProvider, HttpApi, OPENAI_BASE_URL, ProviderKind, assistant, provider},
    skills::FsSkillStore,
    tracing::{Scope, TraceStart},
};
use agent_support::{NoCodex, Scripted, recording, task_context};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

async fn sandbox(live: bool) {
    let database = support::test_database().await;
    let server = support::start_test_server(&database, Some("memory")).await.unwrap();
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
        workflows: db.clone(),
        schedule: db.clone(),
        tracer: tracer.clone(),
        codex: Arc::new(NoCodex),
        decisions: None,
    });
    let trace = tracer.trace(TraceStart {
        id: "sandbox-skill-index".into(),
        name: "sandbox-skill-index".into(),
        ..Default::default()
    });
    let scope: Scope = trace.clone();
    let cancel = CancellationToken::new();
    let report: Value = serde_json::from_str(
        &try_draft(
            &engine,
            json!({"name":"skill-index"}).as_object().unwrap(),
            &task_context("dry").env,
            &scope,
            &cancel,
        )
        .await
        .unwrap(),
    )
    .unwrap();
    assert_eq!(report["status"], "completed", "{report}");
    let client = server.pool.get().await.unwrap();
    let count: i64 = client.query_one("SELECT count(*) FROM memory_project_docs", &[]).await.unwrap().get(0);
    assert_eq!(count, 0, "dry run cannot publish a document");
    let flows = create_durable_flows(engine, (*db).clone());
    for id in [1, 2] {
        let result = flows
            .run(
                "skill-index",
                &WorkflowSignal { id, source: "scheduler".into(), content: "index".into(), env_context: None },
                &task_context(&format!("scheduler:{id}")),
                &scope,
                &cancel,
            )
            .await;
        let FlowResult::Completed(output) = result else { panic!("{result:?}") };
        assert_eq!(output["errors"], json!([]), "{output}");
        assert_eq!(output["indexed"], 2, "{output}");
        assert_eq!(output["generated"], if id == 1 { 2 } else { 0 });
        println!("sandbox live={live}, run={id}: {output}");
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
