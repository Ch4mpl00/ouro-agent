//! Agent writes and MCP discovery share the live directory, without an index refresh.
#[path = "../../agent/tests/support/mod.rs"]
mod agent_support;
use agent::{mcp::Mcp, providers::ToolDef, skills::FsSkillStore, tracing::NullTracer};
use agent_support::{Harness, answer, call, calls, last_json, task_context, turn};
use async_trait::async_trait;
use mcp_tools::skills::{SkillCandidate, SkillCatalog, SkillScorer, find_skills};
use serde_json::{Map, Value, json};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

struct Scorer;
#[async_trait]
impl SkillScorer for Scorer {
    async fn score(&self, query: &str, context: &str, skills: &[SkillCandidate]) -> anyhow::Result<Vec<f64>> {
        assert_eq!(query, "What's new?");
        assert!(context.contains("telegram"));
        let telegram = skills.iter().find(|s| s.name == "telegram").unwrap();
        assert!(telegram.description.contains("every Telegram message"));
        Ok(skills.iter().map(|s| if s.name == "telegram" { 0.99 } else { 0.9 }).collect())
    }
}

struct Discovery(SkillCatalog);
#[async_trait]
impl Mcp for Discovery {
    fn tools(&self) -> Vec<ToolDef> {
        vec![ToolDef::new("find_skills", "Discover skills", json!({"type":"object"}))]
    }
    async fn call_tool(&self, name: &str, args: Map<String, Value>, _: &CancellationToken) -> anyhow::Result<String> {
        assert_eq!(name, "find_skills");
        Ok(find_skills(
            &self.0,
            &json!({}),
            &Scorer,
            args["query"].as_str().unwrap(),
            args["context"].as_str().unwrap(),
            0.5,
            10,
        )
        .await?
        .to_string())
    }
}

#[tokio::test]
async fn primary_saves_live_rules_then_a_new_session_discovers_them_alongside_domain_skills() {
    let root = tempfile::tempdir().unwrap();
    let defaults = root.path().join("skills.default");
    let live = root.path().join("skills");
    std::fs::create_dir(&defaults).unwrap();
    let shipped = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../skills.default");
    std::fs::copy(shipped.join("orchestrator.md"), defaults.join("orchestrator.md")).unwrap();
    std::fs::write(defaults.join("news-query.md"), "---\ntools: []\n---\n# News\n\nFind topical news.").unwrap();
    let content = "---\nroutable: true\ntools: *\n---\n# Telegram\n\nApply to every Telegram message, regardless of topic.\n\nReply briefly in Russian, without tables.\n";
    let mut h = Harness::new(
        task_context("save"),
        vec![
            turn(move |p| {
                assert!(p.tools.iter().any(|t| t.name == "write_skill"));
                assert!(!p.messages[0]["content"].as_str().unwrap().contains("Reply briefly in Russian"));
                call("write_skill", json!({"name":"telegram","content":content}))
            }),
            turn(|p| {
                let result: Value = serde_json::from_str(last_json(&p.messages)["content"].as_str().unwrap()).unwrap();
                assert_eq!(result["ok"], true);
                answer("Saved")
            }),
            turn(|p| {
                assert!(!p.messages[0]["content"].as_str().unwrap().contains("Reply briefly in Russian"));
                call("find_skills", json!({"query":"What's new?","context":"Signal source: telegram"}))
            }),
            turn(|p| {
                let result: Value = serde_json::from_str(last_json(&p.messages)["content"].as_str().unwrap()).unwrap();
                let found = result["skills"].as_array().unwrap();
                assert_eq!(
                    found.iter().map(|s| s["name"].as_str().unwrap()).collect::<Vec<_>>(),
                    ["telegram", "news-query"]
                );
                assert_eq!(found[0]["description_source"], "file");
                calls(&[
                    ("read_skill", json!({"name":"telegram","expected_hash":found[0]["content_hash"]})),
                    ("read_skill", json!({"name":"news-query","expected_hash":found[1]["content_hash"]})),
                ])
            }),
            turn(|p| {
                assert!(agent::generation::messages_json(&p.messages).to_string().contains("Reply briefly in Russian"));
                call("read_skill", json!({"name":"telegram"}))
            }),
            turn(|p| {
                let result: Value = serde_json::from_str(last_json(&p.messages)["content"].as_str().unwrap()).unwrap();
                assert!(result["effectiveInstructions"].as_str().unwrap().contains("IMPROVER LESSON"));
                assert!(!result["editableBody"].as_str().unwrap().contains("IMPROVER LESSON"));
                assert_eq!(result["routable"], true);
                answer("Done")
            }),
        ],
        "unused",
        Arc::new(NullTracer),
    );
    let engine = Arc::get_mut(&mut h.engine).unwrap();
    engine.deps.skills = vec!["orchestrator".into()];
    engine.deps.skill_store = Arc::new(FsSkillStore::new(root.path()));
    engine.deps.mcp = Arc::new(Discovery(SkillCatalog::new(live.clone(), defaults.clone())));
    let mut opts = h.options();
    opts.skills.clear();
    assert_eq!(
        h.engine.start_loop(opts).await.unwrap().send(format!("Save these Telegram rules:\n{content}")).await.unwrap(),
        "Saved"
    );
    h.engine.end_loop("parent").await;
    assert_eq!(std::fs::read_to_string(live.join("telegram.md")).unwrap(), content);
    assert!(!defaults.join("telegram.md").exists());
    std::fs::write(live.join("telegram.patch.md"), "IMPROVER LESSON").unwrap();
    let mut opts = h.options();
    opts.id = "next-message".into();
    opts.session = task_context("next-message");
    opts.skills.clear();
    assert_eq!(h.engine.start_loop(opts).await.unwrap().send("What's new?".into()).await.unwrap(), "Done");
    h.engine.shutdown().await;
}
