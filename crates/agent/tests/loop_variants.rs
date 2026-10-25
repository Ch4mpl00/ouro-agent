// Loop variants: until the engine migration's versions step a worker runs
// the built-in agent-loop whatever variant it names (design 03, R2-M5); the
// variant tests are parked in tests/pending/loop_variants.rs until then.
// What stays: the loop's generic outcomes and the author's preset.
mod support;
use agent::{agent::LoopOptions, providers::assistant};
use serde_json::json;
use std::sync::Arc;
use support::*;

const WORKER: &[(&str, &str)] = &[("worker", "---\ntools: *\n---\nworker")];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settle_exposes_ordered_generic_outcomes_before_model_truncation() {
    let f = fixture(Arc::new(Scripted::new(vec![])), &["t_ok", "t_err", "t_read"], WORKER);
    let mut opts = LoopOptions::new("outcomes", context());
    opts.skills = vec!["worker".into()];
    let live = f.engine.start_loop(opts).await.unwrap();
    live.admit_task("inspect tool facts").await;
    let full = json!({"answer": "evidence".repeat(1500)});
    let huge = "🦀".repeat(40_000);
    let calls = vec![
        json!({"id":"json","name":"t_read","arguments":json!({"output":full.to_string()}).to_string()}),
        json!({"id":"text","name":"t_read","arguments":json!({"output":huge}).to_string()}),
        json!({"id":"error","name":"t_err","arguments":"{}"}),
        json!({"id":"invalid","name":"t_ok","arguments":"not JSON"}),
        json!({"id":"unknown","name":"t_ok","arguments":"{}"}),
    ];
    // t_read uses the same injected MCP as any other tool, with no channel contract.
    let mut results = Vec::new();
    for call in &calls[..4] {
        let result = agent::agent::loop_tool(&f.engine, &live, &json!({"call":call}), "outcomes", "invocation", true)
            .await
            .unwrap();
        results.push(result);
    }
    assert!(results[0]["presented"].as_str().unwrap().contains("\"truncated\":true"));
    // A round admitted before outcomes existed only has the full stored reply.
    results[1].as_object_mut().unwrap().remove("result");
    results.push(agent::agent::unknown_tool_outcome("t_ok"));
    let settled = agent::agent::loop_settle(&live, &json!({"calls":calls,"results":results})).await.unwrap();
    assert!(settled.get("replied").is_none() && settled.get("reply").is_none());
    let outcomes = settled["outcomes"].as_array().unwrap();
    assert_eq!(outcomes.len(), calls.len());
    assert_eq!(outcomes[0], json!({"name":"t_read","arguments":{"output":full.to_string()},"ok":true,"result":full}));
    assert_eq!(outcomes[1]["ok"], true);
    let clipped = outcomes[1]["result"].as_str().unwrap();
    assert!(clipped.starts_with('🦀') && clipped.contains("[clipped, 80000 chars]"));
    assert!(clipped.encode_utf16().count() < 66_000);
    for (outcome, name) in outcomes[2..].iter().zip(["t_err", "t_ok", "t_ok"]) {
        assert_eq!(outcome["name"], name);
        assert_eq!(outcome["ok"], false);
        assert!(outcome["result"].as_str().unwrap().starts_with("[tool error]"));
    }
    assert_eq!(outcomes[3]["arguments"], "not JSON");
    assert!(outcomes[4]["result"].as_str().unwrap().contains("outcome unknown"));
    f.engine.end_loop("outcomes").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workflow_authors_use_maximum_effort_for_every_requested_worker_preset() {
    {
        for preset in ["base", "smart", "smartest"] {
            let model = Arc::new(Scripted::new(vec![
                calls(&[("invoke_sub_agent", json!({"skills":["workflow-author"],"preset":preset,"prompt":"draft"}))]),
                assistant("draft"),
                assistant("done"),
            ]));
            let f = fixture(
                model.clone(),
                &[],
                &[("worker", "---\ntools: *\n---\nworker"), ("workflow-author", "---\ntools: []\n---\nauthor")],
            );
            let mut opts = LoopOptions::new("author", context());
            opts.skills = vec!["worker".into()];
            assert_eq!(f.engine.start_loop(opts).await.unwrap().send("draft".into()).await.unwrap(), "done");
            let seen = model.seen.lock().unwrap();
            assert_eq!(seen[1].model, agent::models::default_presets().smartest.model, "{preset}");
            assert_eq!(seen[1].reasoning_effort, agent::models::ReasoningEffort::Max, "{preset}");
        }
    }
}
