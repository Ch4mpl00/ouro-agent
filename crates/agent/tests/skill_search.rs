mod support;
use agent::{
    decisions::{DecisionClient, decision_client},
    mcp::Mcp,
    providers::{JsonApi, ToolDef},
    skills::{FindSkillsParams, FsSkillStore, catalog_snapshot, find_skills, read_skill_index},
    tracing::{NullTracer, null_scope},
};
use async_trait::async_trait;
use serde_json::{Map, Value, json};
use std::sync::{Arc, Mutex};
use support::{Harness, answer, call, last_result, task_context, turn};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct Api {
    requests: Mutex<Vec<Value>>,
}
#[async_trait]
impl JsonApi for Api {
    async fn post(&self, path: &str, body: Value, _: &CancellationToken) -> anyhow::Result<Value> {
        assert_eq!(path, agent::decisions::OPENROUTER_DECISIONS_URL);
        let skills = body["state"]["skills"].as_array().unwrap();
        let query = body["state"]["current_request"].as_str().unwrap();
        let answers: Map<String, Value> = skills
            .iter()
            .enumerate()
            .filter_map(|(n, s)| {
                if query == "missing" {
                    return None;
                }
                let probability = if query == "invalid" {
                    2.0
                } else if query == "batch" {
                    s["name"].as_str().unwrap().parse::<f64>().unwrap() / 100.0
                } else {
                    0.8
                };
                Some((format!("skill_{n}"), json!({"type":"noul","noul":probability})))
            })
            .collect();
        self.requests.lock().unwrap().push(body);
        Ok(json!({"model":"jev-test","answers":answers}))
    }
}

fn params(query: &str) -> FindSkillsParams {
    FindSkillsParams {
        query: query.into(),
        context: Some("Signal source: telegram".into()),
        threshold: None,
        limit: None,
    }
}
fn store() -> (tempfile::TempDir, FsSkillStore) {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("skills.default")).unwrap();
    std::fs::create_dir(root.path().join("skills")).unwrap();
    let store = FsSkillStore::new(root.path());
    (root, store)
}
fn client() -> (Arc<Api>, Arc<DecisionClient>) {
    let api = Arc::new(Api::default());
    let client = decision_client("jev-test".into(), api.clone());
    (api, client)
}

#[tokio::test]
#[ignore = "requires local OpenRouter credentials; uses isolated skill files"]
async fn live_agent_discovery_scores_new_telegram_and_domain_skills_without_mcp_search() {
    let (_root, store) = store();
    std::fs::write(store.live_dir.join("telegram.md"),
        "---\ntools: *\n---\n# Telegram communication\n\nApply to every Telegram message and reply regardless of topic. Define the preferred reply style; combine with domain skills.\n").unwrap();
    let shipped = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../skills.default/news-query.md");
    std::fs::copy(shipped, store.defaults_dir.join("news-query.md")).unwrap();
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.env.agent");
    let values: std::collections::HashMap<String, String> =
        dotenvy::from_path_iter(path).unwrap().map(Result::unwrap).collect();
    let key = std::env::var("OPEN_ROUTER_API_KEY")
        .ok()
        .or_else(|| values.get("OPEN_ROUTER_API_KEY").cloned())
        .expect("OPEN_ROUTER_API_KEY");
    let client =
        decision_client(agent::models::DEFAULT_DECISION_MODEL.into(), Arc::new(agent::providers::openrouter_api(&key)));
    let result = find_skills(
        &store,
        &json!({}),
        &client,
        &params("Find recent OpenAI news and reply briefly"),
        &null_scope(),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    let names: Vec<_> = result["skills"].as_array().unwrap().iter().map(|s| s["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"telegram") && names.contains(&"news-query"), "{result}");
    assert_eq!(result["scored_count"], 2);
    println!("Agent-local live discovery: {result}");
}

#[tokio::test]
async fn scores_all_current_skills_then_filters_sorts_and_limits_with_hash_checked_descriptions() {
    let (_root, store) = store();
    for name in ["a", "b"] {
        std::fs::write(
            store.defaults_dir.join(format!("{name}.md")),
            format!("---\ntools: []\n---\n# {name}\n\nCurrent `{name}`.\n\nPRIVATE FULL INSTRUCTIONS"),
        )
        .unwrap();
    }
    std::fs::write(store.defaults_dir.join("internal.md"), "---\nroutable: false\ntools: []\n---\nInternal").unwrap();
    let snapshot = catalog_snapshot(&store).await.unwrap();
    let hash = &snapshot["skills"][0]["content_hash"];
    let index = json!({"skills":{"a":{"content_hash":hash,"description":"Generated a"},"b":{"content_hash":"old","description":"STALE"}}});
    let (api, client) = client();
    let mut p = params("task");
    p.threshold = Some(0.8);
    p.limit = Some(1);
    let found = find_skills(&store, &index, &client, &p, &null_scope(), &CancellationToken::new()).await.unwrap();
    assert_eq!(found["scored_count"], 2);
    assert_eq!(found["total_matches"], 2);
    assert_eq!(found["skills"].as_array().unwrap().len(), 1);
    assert_eq!(found["skills"][0]["name"], "a");
    assert_eq!(found["skills"][0]["description_source"], "index");
    assert_eq!(found["unindexed"], json!(["b"]));
    let first = api.requests.lock().unwrap()[0].clone();
    assert_eq!(first["state"]["skills"][0]["description"], "Generated a");
    assert_eq!(first["state"]["skills"][1]["description"], "Current b.");
    assert_eq!(first["state"]["context"], "Signal source: telegram");
    assert!(!first.to_string().contains("PRIVATE FULL INSTRUCTIONS"));
    assert_eq!(first["questions"].as_object().unwrap().len(), 2);
    std::fs::write(store.live_dir.join("a.patch.md"), "Changed scope").unwrap();
    p.threshold = Some(0.9);
    let found = find_skills(&store, &index, &client, &p, &null_scope(), &CancellationToken::new()).await.unwrap();
    assert_eq!(found["skills"], json!([]));
    assert_eq!(found["unindexed"], json!(["a", "b"]));
    assert_eq!(api.requests.lock().unwrap()[1]["state"]["skills"][0]["description"], "Current a.");
}

#[tokio::test]
async fn batches_all_candidates_and_rejects_missing_or_invalid_decision_answers() {
    let (_root, store) = store();
    for n in 0..51 {
        std::fs::write(store.defaults_dir.join(format!("{n}.md")), "---\ntools: []\n---\nPurpose").unwrap();
    }
    let (api, client) = client();
    let mut p = params("batch");
    p.threshold = Some(0.0);
    p.limit = Some(100);
    let found = find_skills(&store, &json!({}), &client, &p, &null_scope(), &CancellationToken::new()).await.unwrap();
    assert_eq!(found["scored_count"], 51);
    assert_eq!(found["skills"][0]["name"], "50");
    assert_eq!(found["skills"][0]["score"], 0.5);
    let requests = api.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 3);
    assert!(requests.iter().all(|r| r["questions"].as_object().unwrap().len() <= 25));
    for query in ["invalid", "missing"] {
        assert!(
            find_skills(&store, &json!({}), &client, &params(query), &null_scope(), &CancellationToken::new())
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn empty_catalog_skips_jev_and_invalid_arguments_or_cancellation_fail_before_model_calls() {
    let (_root, store) = store();
    let (api, client) = client();
    let p = params("task");
    assert_eq!(
        find_skills(&store, &json!({}), &client, &p, &null_scope(), &CancellationToken::new()).await.unwrap()["scored_count"],
        0
    );
    for p in [
        params(""),
        FindSkillsParams { threshold: Some(-0.1), ..params("task") },
        FindSkillsParams { limit: Some(0), ..params("task") },
        FindSkillsParams { context: Some("x".repeat(8001)), ..params("task") },
    ] {
        assert!(find_skills(&store, &json!({}), &client, &p, &null_scope(), &CancellationToken::new()).await.is_err());
    }
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(find_skills(&store, &json!({}), &client, &params("task"), &null_scope(), &cancel).await.is_err());
    assert!(api.requests.lock().unwrap().is_empty());
}

struct MemoryReply(Value);
#[async_trait]
impl Mcp for MemoryReply {
    fn tools(&self) -> Vec<ToolDef> {
        Vec::new()
    }
    async fn call_tool(&self, name: &str, args: Map<String, Value>, _: &CancellationToken) -> anyhow::Result<String> {
        assert_eq!(name, "read_doc");
        assert_eq!(args, json!({"project":"agent-system","doc":"skill-index.md"}).as_object().unwrap().clone());
        Ok(self.0.to_string())
    }
}
#[tokio::test]
async fn memory_boundary_accepts_only_explicit_missing_or_valid_index_and_surfaces_failures() {
    for error in ["project_not_found", "doc_not_found"] {
        let index =
            read_skill_index(&MemoryReply(json!({"ok":false,"error":error})), &CancellationToken::new()).await.unwrap();
        assert_eq!(index["skills"], json!({}));
    }
    for reply in [
        json!({"ok":false,"error":"database_unavailable"}),
        json!({"ok":true,"doc":{"body":"broken"}}),
        json!({"ok":true,"doc":{"body":"{}"}}),
    ] {
        assert!(read_skill_index(&MemoryReply(reply), &CancellationToken::new()).await.is_err());
    }
    let index = json!({"schema_version":1,"skills":{"a":{"description":"Generated"}}});
    assert_eq!(
        read_skill_index(&MemoryReply(json!({"ok":true,"doc":{"body":index.to_string()}})), &CancellationToken::new())
            .await
            .unwrap(),
        index
    );
}

#[tokio::test]
async fn missing_decision_client_is_an_explicit_agent_tool_error_without_an_mcp_search_call() {
    let mut h = Harness::new(
        task_context("unavailable"),
        vec![
            turn(|p| {
                assert!(p.tools.iter().any(|t| t.name == "find_skills"));
                call("find_skills", json!({"query":"task"}))
            }),
            turn(|p| {
                assert!(last_result(&p.messages).contains("agent decision client"));
                answer("fallback")
            }),
        ],
        "unused",
        Arc::new(NullTracer),
    );
    Arc::get_mut(&mut h.engine).unwrap().deps.decisions = None;
    assert_eq!(h.start().await.send("task".into()).await.unwrap(), "fallback");
    assert!(h.mcp.calls.lock().unwrap().is_empty());
    h.engine.shutdown().await;
}
