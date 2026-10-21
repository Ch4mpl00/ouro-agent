// The dry-run sandbox (sandbox.rs): a Telegram-shaped signal through draft
// versions of the example route and loop (tests/fixtures), with real
// (scripted) model turns, real reads, the real guard workflow and mocked
// writes; nothing it does may outlive the try.
mod support;
use agent::{
    db::TraceStore,
    mcp::Mcp,
    providers::{ToolDef, assistant},
    sandbox::{MOCK_REPLY, try_signal},
    tracing::{Scope, TraceStart},
};
use async_trait::async_trait;
use serde_json::{Map, Value, json};
use std::sync::{Arc, Mutex};
use support::*;
use tokio_util::sync::CancellationToken;

const ROUTE: &str = include_str!("fixtures/signal-telegram.yaml");
const LOOP: &str = include_str!("fixtures/agent-loop-telegram.yaml");
const SKILLS: &[(&str, &str)] = &[
    ("telegram", "---\ntools: *\n---\nTELEGRAM"),
    ("worker", "---\ntools: *\n---\nWORKER"),
    ("recovery", "---\ntools: []\n---\nRECOVERY"),
    ("workflow-author", "---\ntools: []\n---\nAUTHOR"),
];
const MESSAGE: &str = "Telegram message in chat 42 (forum topic thread_id=7).\nText: \"what is new in Rust?\"";

// MCP as the sandbox meets it: reads the server marks read-only, writes it
// does not, and a queue whose latest signals give the signal types.
#[derive(Default)]
struct World {
    calls: Mutex<Vec<(String, Map<String, Value>)>>,
}

const READS: &[&str] =
    &["list_signals", "get_telegram_chat_history", "journal_tail", "read_doc", "get_timezone", "search_news"];
const WRITES: &[&str] = &["send_telegram_message", "edit_telegram_message", "remember", "journal_append"];

impl World {
    fn names(&self) -> Vec<String> {
        self.calls.lock().unwrap().iter().map(|(n, _)| n.clone()).collect()
    }
}

#[async_trait]
impl Mcp for World {
    fn tools(&self) -> Vec<ToolDef> {
        READS.iter().chain(WRITES).map(|n| ToolDef::new(n, n, json!({"type":"object"}))).collect()
    }
    fn read_only(&self, name: &str) -> bool {
        READS.contains(&name)
    }
    fn open_world(&self, _: &str) -> bool {
        false
    }
    async fn call_tool(&self, name: &str, args: Map<String, Value>, _: &CancellationToken) -> anyhow::Result<String> {
        self.calls.lock().unwrap().push((name.into(), args));
        Ok(match name {
            "list_signals" => json!({"count":2,"signals":[
                {"id":5,"source":"telegram","content":"Telegram message in chat 42.\nText: \"older\"","created_at":"2026-10-20 09:00:00","consumed_at":null},
                {"id":6,"source":"peer","content":"Message from claude-code, thread 3:\n\nhi","created_at":"2026-10-20 09:30:00","consumed_at":null},
                {"id":7,"source":"telegram","content":"Telegram message in chat 42 (forum topic thread_id=7).\nText: \"latest\"","created_at":"2026-10-20 10:00:00","consumed_at":null}]}),
            "get_telegram_chat_history" => json!({"messages":[]}),
            "journal_tail" => json!({"entries":[]}),
            "read_doc" => json!({"ok":false,"error":"doc_not_found"}),
            "get_timezone" => json!({"timezone":"UTC"}),
            "search_news" => json!({"items":[{"title":"Rust 2.0 is out"}]}),
            _ => json!({"delivered":true,"messageId":700}),
        }
        .to_string())
    }
}

struct Sandbox {
    f: Fixture,
    world: Arc<World>,
    model: Arc<Scripted>,
}

// An engine whose guard answers `label`, with the example route and loop
// active (versions that would answer differently) and their fixtures as
// drafts on top.
fn sandbox(script: Vec<agent::providers::Message>, label: &str) -> Sandbox {
    let model = Arc::new(Scripted::new(script));
    let mut f = fixture(model.clone(), &[], SKILLS);
    let world = Arc::new(World::default());
    let api = Arc::new(GuardApi::default());
    *api.label.lock().unwrap() = Some(label.into());
    let deps = &mut Arc::get_mut(&mut f.engine).unwrap().deps;
    deps.mcp = world.clone();
    deps.decisions = Some(agent::decisions::decision_client("guard-test".into(), api));
    activate_definition(
        &f.engine,
        "agent-loop-telegram",
        &LOOP.replace("set: '${ { output: .reply } }'", "set: '${ { output: (\"active: \" + .reply) } }'"),
    );
    activate_definition(&f.engine, "signal-telegram", &ROUTE.replace("loop: agent-loop-telegram", "loop: agent-loop"));
    for (name, definition) in [("agent-loop-telegram", LOOP), ("signal-telegram", ROUTE)] {
        f.engine.deps.workflows.save(name, definition, Some("draft")).unwrap();
    }
    Sandbox { f, world, model }
}

impl Sandbox {
    async fn try_signal(&self, args: Value) -> anyhow::Result<Value> {
        let trace = self.f.engine.deps.tracer.trace(TraceStart {
            id: "author".into(),
            name: "author".into(),
            ..Default::default()
        });
        let scope: Scope = trace.clone();
        let reply =
            try_signal(&self.f.engine, args.as_object().unwrap(), &context(), &scope, &CancellationToken::new()).await;
        trace.end();
        Ok(serde_json::from_str(&reply?)?)
    }

    fn rows(&self, table: &str) -> i64 {
        self.f.db.conn().query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0)).unwrap()
    }

    fn assert_nothing_kept(&self) {
        for table in ["workflow_runs", "workflow_records", "workflow_events", "confirmations", "guard_verdict_uses"] {
            assert_eq!(self.rows(table), 0, "{table}");
        }
        assert_eq!(self.rows("memory"), 0, "no breaker, watermark or card wording");
        let names = self.world.names();
        assert!(!names.iter().any(|n| WRITES.contains(&n.as_str())), "a write reached MCP: {names:?}");
        for name in ["agent-loop-telegram", "signal-telegram"] {
            assert_eq!(self.f.engine.deps.workflows.active(name).unwrap().unwrap().version, 1, "{name} stays active");
        }
    }
}

fn telegram(confirm: Option<&str>) -> Value {
    let mut args = json!({"name":"signal","drafts":["signal-telegram","agent-loop-telegram"],
        "signal":{"source":"telegram","content":MESSAGE}});
    if let Some(confirm) = confirm {
        args["confirm"] = json!(confirm);
    }
    args
}

fn send(text: &str) -> agent::providers::Message {
    calls(&[("send_telegram_message", json!({"chatId":"42","messageThreadId":7,"text":text}))])
}

fn run<'a>(report: &'a Value, id: &str) -> &'a Value {
    fn find<'a>(runs: &'a Value, id: &str) -> Option<&'a Value> {
        runs.as_array()?.iter().find_map(|r| if r["run"] == id { Some(r) } else { find(&r["children"], id) })
    }
    find(&report["runs"], id).unwrap_or_else(|| panic!("no run {id} in {report:#}"))
}

#[tokio::test]
async fn a_telegram_signal_runs_the_draft_chain_with_real_reads_and_mocked_writes() {
    let mut first = calls(&[("search_news", json!({"query":"rust"}))]);
    first.insert("reasoning_content".into(), json!("Look the news up first."));
    let s = sandbox(vec![first, send("Rust 2.0 is out")], "allow");
    let untried = json!({"name":"agent-loop-telegram","version":2});
    let refused = agent::flows::tool(&s.f.engine, "activate_workflow", untried.as_object().unwrap()).unwrap_err();
    assert!(refused.to_string().contains("has not completed a dry run"), "{refused}");

    let report = s.try_signal(telegram(None)).await.unwrap();
    assert_eq!(report["status"], "completed", "{report:#}");
    assert_eq!(report["output"], "Rust 2.0 is out", "the draft loop's reply, not the active one's");
    assert_eq!(report["ended"]["by"], "workflow rule");
    assert_eq!(report["ended"]["at"], "reply");
    assert_eq!(
        report["writes"],
        json!([{"function":"send_telegram_message","arguments":{"chatId":"42","messageThreadId":7,"text":"Rust 2.0 is out"}}])
    );
    assert_eq!(
        report["drafts"],
        json!([{"name":"agent-loop-telegram","version":2,"ran":true},{"name":"signal-telegram","version":2,"ran":true}])
    );

    // The path of each workflow, with every switch's case.
    let dispatcher = run(&report, "telegram:0/signal");
    assert_eq!(dispatcher["version"], "built-in");
    let path: Vec<&str> = dispatcher["path"].as_array().unwrap().iter().map(|p| p.as_str().unwrap()).collect();
    for step in ["offer_button (skipped)", "after_button → ordinary", "route", "after_route → routed"] {
        assert!(path.contains(&step), "{step}: {path:?}");
    }
    assert_eq!(run(&report, "telegram:0/signal-telegram")["version"], "draft v2");
    let primary = run(&report, "telegram:0/loop");
    assert_eq!(
        (primary["workflow"].as_str(), primary["version"].as_str()),
        (Some("agent-loop-telegram"), Some("draft v2"))
    );
    let last: Vec<&str> =
        primary["path"].as_array().unwrap().iter().rev().take(2).map(|p| p.as_str().unwrap()).collect();
    assert_eq!(last, ["reply", "next[1] → replied"], "the second pass of `next` took `replied`");

    // Each turn: reasoning, calls with verdicts and results; reads real, writes mocked.
    let turns = primary["turns"].as_array().unwrap();
    assert_eq!(turns.len(), 2);
    assert_eq!(turns[0]["reasoning"], "Look the news up first.");
    let read = &turns[0]["calls"][0];
    assert_eq!((read["name"].as_str(), read["ok"].as_bool()), (Some("search_news"), Some(true)));
    assert_eq!(read["result"]["items"][0]["title"], "Rust 2.0 is out");
    assert!(read.get("mock").is_none() && read.get("verdict").is_none(), "a closed read is not reviewed: {read}");
    let write = &turns[1]["calls"][0];
    assert_eq!(write["arguments"]["text"], "Rust 2.0 is out");
    assert_eq!(write["verdict"]["label"], "allow");
    assert_eq!((write["mock"].as_bool(), write["ok"].as_bool()), (Some(true), Some(true)));
    assert_eq!(write["result"], serde_json::from_str::<Value>(MOCK_REPLY).unwrap());
    let guard = run(&report, "telegram:0/loop/guard/2");
    assert_eq!(guard["workflow"], "guard");
    assert!(guard["path"].as_array().unwrap().contains(&json!("route → ordinary")), "{guard:#}");
    assert!(s.world.names().iter().any(|n| n == "search_news"));
    assert!(s.world.names().iter().any(|n| n == "get_telegram_chat_history"));
    assert_eq!(s.model.seen.lock().unwrap().len(), 2, "two real model turns");

    // Nothing kept; the drafts that ran are tried, which activation needs.
    s.assert_nothing_kept();
    for name in ["agent-loop-telegram", "signal-telegram"] {
        assert!(s.f.engine.deps.workflows.get(name, Some(2)).unwrap().unwrap().tried, "{name}");
    }
    agent::flows::tool(&s.f.engine, "activate_workflow", untried.as_object().unwrap()).unwrap();

    // The builder's own trace holds the dry run, its write marked mock.
    let (_, observations) = s.f.db.get_trace("author").unwrap().unwrap();
    let dry = observations.iter().find(|o| o.name == "dry-run").unwrap();
    assert_eq!(dry.meta("dry_run"), Some(&json!(true)));
    assert!(observations.iter().any(|o| o.name == "signal:telegram"));
    let sent = observations.iter().find(|o| o.name == "send_telegram_message").unwrap();
    assert_eq!(sent.meta("mock"), Some(&json!(true)));
    assert_eq!(s.f.db.list_recent(20, None).unwrap().len(), 1, "no trace of a signal of its own");
}

// A reply to the current chat needs no confirmation (guard.rs); a write of
// memory does, and the guard asks the user about it.
#[tokio::test]
async fn confirmations_are_answered_on_the_spot_through_the_real_guard() {
    for (confirm, label) in [(None, "allow"), (Some("decline"), "declined"), (Some("expire"), "expired")] {
        let s = sandbox(vec![calls(&[("remember", json!({"text":"likes Rust"}))]), send("done")], "needs_confirmation");
        let start = std::time::Instant::now();
        let report = s.try_signal(telegram(confirm)).await.unwrap();
        assert!(start.elapsed() < std::time::Duration::from_secs(4), "{label}: the guard did not wait");
        assert_eq!(report["status"], "completed", "{label}: {report:#}");
        assert_eq!(report["output"], "done");
        let remembered = &run(&report, "telegram:0/loop")["turns"][0]["calls"][0];
        assert_eq!(remembered["verdict"]["label"], label, "{report:#}");
        let word = confirm.unwrap_or("accept");
        assert_eq!(remembered["confirmation"], format!("confirmation requested: remember → auto-{word}"), "{report:#}");
        let guard: Vec<&str> = run(&report, "telegram:0/loop/guard/1")["path"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p.as_str().unwrap())
            .collect();
        assert!(
            guard.contains(&"route → ask") && guard.contains(&"confirm") && guard.contains(&"resolve"),
            "{guard:?}"
        );
        let written: Vec<&str> =
            report["writes"].as_array().unwrap().iter().map(|w| w["function"].as_str().unwrap()).collect();
        if label == "allow" {
            assert_eq!(remembered["mock"], true);
            assert_eq!(written, ["remember", "send_telegram_message"]);
            assert!(guard.contains(&"outcome"), "{guard:?}");
        } else {
            assert_eq!(remembered["ok"], false);
            assert!(remembered["result"].as_str().unwrap().starts_with("[guard] blocked:"));
            assert_eq!(written, ["send_telegram_message"], "a refused write is not even mocked");
        }
        if label == "expired" {
            assert!(guard.iter().any(|p| p.starts_with("listen faulted: Timeout")), "{guard:?}");
            assert!(guard.contains(&"expired") && !guard.contains(&"outcome"), "{guard:?}");
        }
        s.assert_nothing_kept();
    }
}

#[tokio::test]
async fn a_worker_inside_runs_dry_and_is_nested_in_the_trace() {
    let s = sandbox(
        vec![
            calls(&[("invoke_sub_agent", json!({"prompt":"note the request"}))]),
            calls(&[("remember", json!({"text":"Rust question"}))]),
            assistant("noted"),
            send("noted it"),
        ],
        "allow",
    );
    let report = s.try_signal(telegram(None)).await.unwrap();
    assert_eq!(report["status"], "completed", "{report:#}");
    assert_eq!(report["output"], "noted it");
    let worker = run(&report, "telegram:0__sub1.0/loop");
    assert_eq!((worker["workflow"].as_str(), worker["version"].as_str()), (Some("agent-loop"), Some("built-in")));
    let remembered = &worker["turns"][0]["calls"][0];
    assert_eq!((remembered["name"].as_str(), remembered["mock"].as_bool()), (Some("remember"), Some(true)));
    assert_eq!(worker["ended"]["by"], "answer");
    assert_eq!(worker["ended"]["output"], "noted");
    let functions: Vec<&str> =
        report["writes"].as_array().unwrap().iter().map(|w| w["function"].as_str().unwrap()).collect();
    assert_eq!(functions, ["remember", "send_telegram_message"]);
    s.assert_nothing_kept();
}

#[tokio::test]
async fn a_signal_type_is_one_this_agent_receives() {
    let s = sandbox(vec![calls(&[("try_workflow", json!({"name":"signal","signal":{}}))]), assistant("seen")], "allow");
    for signal in [json!({"source":"carrier-pigeon","content":"hi"}), json!({})] {
        let error = s.try_signal(json!({"name":"signal","signal":signal})).await.unwrap_err().to_string();
        assert!(error.contains("is not a signal type") || error.contains("is missing"), "{error}");
        // The latest real signal of each source, and the agent's own scheduler.
        assert!(
            error.contains("thread_id=7") && error.contains("\\\"latest\\\"") && !error.contains("older"),
            "{error}"
        );
        assert!(error.contains("\"source\":\"peer\"") && error.contains("\"source\":\"scheduler\""), "{error}");
    }
    assert!(s.model.seen.lock().unwrap().is_empty(), "nothing ran");
    let error =
        s.try_signal(json!({"name":"nope","signal":{"source":"telegram","content":MESSAGE}})).await.unwrap_err();
    assert!(error.to_string().contains("no saved workflow `nope`"));
    // The old form refuses what runs on signals, and the author sees the types.
    let old = agent::flows::try_draft(
        &s.f.engine,
        json!({"name":"agent-loop-telegram"}).as_object().unwrap(),
        &context().env,
        &agent::tracing::null_scope(),
        &CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(old.to_string().contains("try it with a signal"), "{old}");
    let mut opts = agent::agent::LoopOptions::new("author", context());
    opts.skills = vec!["workflow-author".into()];
    let author = s.f.engine.start_loop(opts).await.unwrap();
    assert_eq!(author.send("try it".into()).await.unwrap(), "seen");
    let seen = s.model.seen.lock().unwrap();
    let reply = seen[1].messages.last().unwrap()["content"].as_str().unwrap();
    assert!(reply.contains("try_workflow error") && reply.contains("scheduler"), "{reply}");
}

#[tokio::test]
async fn a_jq_error_in_a_draft_names_the_expression_and_the_value_it_met() {
    let s = sandbox(vec![assistant("reported")], "allow");
    let broken = ROUTE.replace("set: '${ [\"telegram\"] }'", "set: '${ $context.signal.content | .text }'");
    assert_ne!(broken, ROUTE);
    s.f.engine.deps.workflows.save("signal-telegram", &broken, None).unwrap();
    let report = s.try_signal(telegram(None)).await.unwrap();
    assert_eq!(report["status"], "faulted", "{report:#}");
    let route = run(&report, "telegram:0/signal-telegram");
    assert_eq!(route["version"], "draft v3");
    let fault =
        route["path"].as_array().unwrap().iter().find_map(|p| p.as_str().filter(|p| p.contains("faulted"))).unwrap();
    assert!(
        fault.starts_with("own_skills faulted") && fault.contains(".text") && fault.contains("failed on"),
        "{fault}"
    );
    assert!(!s.f.engine.deps.workflows.get("signal-telegram", Some(3)).unwrap().unwrap().tried);
    s.assert_nothing_kept();
    assert!(s.f.engine.deps.memory.get("signal_telegram.breaker").unwrap().is_none(), "its fault counts nowhere");
}
