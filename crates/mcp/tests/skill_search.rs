//! Skill discovery over the real MCP/memory boundary, with an isolated DB
//! and an injected deterministic scorer (no external model requests).
mod support;
use async_trait::async_trait;
use mcp_tools::skills::{SkillCandidate, SkillScorer};
use serde_json::{Value, json};
use std::sync::Arc;
use support::{Outcome, call, connect};

struct Scorer;
#[async_trait]
impl SkillScorer for Scorer {
    async fn score(&self, query: &str, _: &str, skills: &[SkillCandidate]) -> anyhow::Result<Vec<f64>> {
        assert_eq!(query, "topical news");
        Ok(skills.iter().map(|s| if s.name == "news-query" { 0.95 } else { 0.1 }).collect())
    }
}
fn ok(out: Outcome) -> Value {
    match out {
        Outcome::Ok(v) => v,
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn finds_then_reads_a_skill_and_uses_the_published_index_when_current() {
    let db = support::test_database().await;
    let server = support::start_test_server_with(
        &db,
        Some("skill-search,skills,memory"),
        support::Overrides { skill_scorer: Some(Arc::new(Scorer)), ..Default::default() },
    )
    .await
    .unwrap();
    let client = connect(&server.url).await;
    let found = ok(call(&client, "find_skills", json!({"query":"topical news"})).await);
    assert_eq!(found["skills"].as_array().unwrap().len(), 1);
    assert_eq!(found["skills"][0]["name"], "news-query");
    assert_eq!(found["skills"][0]["description_source"], "file");
    assert_eq!(found["index_complete"], false);
    let item = &found["skills"][0];
    let read =
        ok(call(&client, "read_skill", json!({"fileName":item["fileName"],"expectedHash":item["content_hash"]})).await);
    assert_eq!(read["found"], true);
    assert!(read["effectiveInstructions"].as_str().unwrap().contains("news"));
    assert!(matches!(
        call(&client, "read_skill", json!({"fileName":item["fileName"],"expectedHash":"wrong"})).await,
        Outcome::ToolError(_)
    ));
    ok(call(&client, "create_project", json!({"slug":"agent-system","title":"System"})).await);
    let index = json!({"schema_version":1,"skills":{"news-query":{"content_hash":item["content_hash"],"generator_hash":"g","description":"Generated topical news description"}}});
    ok(call(&client,"write_doc",json!({"project":"agent-system","doc":"skill-index.md","body":index.to_string(),"searchable":false,"summary":"Skill index"})).await);
    let found = ok(call(&client, "find_skills", json!({"query":"topical news"})).await);
    assert_eq!(found["skills"][0]["description_source"], "index");
    assert_eq!(found["skills"][0]["description"], "Generated topical news description");
    client.cancel().await.unwrap();
    drop(server);
    db.drop_all().await;
}

#[tokio::test]
async fn missing_scorer_is_an_explicit_tool_error_and_discovery_is_on_the_default_surface() {
    let db = support::test_database().await;
    let server = support::start_test_server(&db, Some("skill-search")).await.unwrap();
    let client = connect(&server.url).await;
    let Outcome::ToolError(error) = call(&client, "find_skills", json!({"query":"topical news"})).await else {
        panic!("must report unavailable scorer")
    };
    assert!(error.contains("OPEN_ROUTER_API_KEY"));
    client.cancel().await.unwrap();
    drop(server);
    db.drop_all().await;
}
