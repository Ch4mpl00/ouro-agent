//! Agent-local discovery sees new live rules without an index refresh.
#[path = "support/mod.rs"]
mod agent_support;
use agent::{decisions::decision_client, providers::JsonApi};
use agent::{mcp::Mcp, providers::ToolDef, skills::FsSkillStore, tracing::NullTracer};
use agent_support::{Harness, answer, call, calls, last_json, task_context, turn};
use async_trait::async_trait;
use serde_json::{Map, Value, json};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

struct Scorer;
#[async_trait]
impl JsonApi for Scorer {
    async fn post(&self, _: &str, body: Value, _: &CancellationToken) -> anyhow::Result<Value> {
        if body["state"]["calls"].is_array() {
            return agent_support::GuardApi::default().post("guard", body, &CancellationToken::new()).await;
        }
        assert_eq!(body["state"]["current_request"], "What's new?");
        assert!(body["state"]["context"].as_str().unwrap().contains("telegram"));
        let skills = body["state"]["skills"].as_array().unwrap();
        let telegram = skills.iter().find(|s| s["name"] == "telegram").unwrap();
        assert!(telegram["description"].as_str().unwrap().contains("every Telegram message"));
        let answers: Map<String, Value> = skills
            .iter()
            .enumerate()
            .map(|(n, s)| (format!("skill_{n}"), json!({"type":"noul","noul":if s["name"]=="telegram"{0.99}else{0.9}})))
            .collect();
        Ok(json!({"model":"jev-test","answers":answers}))
    }
}

struct MemoryOnly;
#[async_trait]
impl Mcp for MemoryOnly {
    fn tools(&self) -> Vec<ToolDef> {
        Vec::new()
    }
    async fn call_tool(&self, name: &str, args: Map<String, Value>, _: &CancellationToken) -> anyhow::Result<String> {
        assert_eq!(name, "read_doc", "discovery must not be delegated to MCP");
        assert_eq!(args["project"], "agent-system");
        assert_eq!(args["doc"], "skill-index.md");
        Ok(json!({"ok":false,"error":"doc_not_found"}).to_string())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn primary_saves_live_rules_then_a_new_session_discovers_them_alongside_domain_skills() {
    let root = tempfile::tempdir().unwrap();
    let defaults = root.path().join("skills.default");
    let live = root.path().join("skills");
    std::fs::create_dir(&defaults).unwrap();
    let shipped = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../skills.default");
    std::fs::copy(shipped.join("orchestrator.md"), defaults.join("orchestrator.md")).unwrap();
    std::fs::write(defaults.join("news-query.md"), "---\ntools: []\n---\n# News\n\nFind topical news.").unwrap();
    let content = "---\nroutable: true\ntools: *\n---\n# Telegram\n\nApply to every Telegram message, regardless of topic.\n\nReply briefly in Russian, without tables.\n";
    let h = Harness::configured(
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
        |deps| {
        deps.skills = vec!["orchestrator".into()];
        deps.skill_store = Arc::new(FsSkillStore::new(root.path()));
        deps.mcp = Arc::new(MemoryOnly);
        deps.decisions = Some(decision_client("jev-test".into(), Arc::new(Scorer)));
        },
    );
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
