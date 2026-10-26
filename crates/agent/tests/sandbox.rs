// The dry-run sandbox on the engine (docs/designs/05-sandbox.md): a signal
// through the whole chain with the drafts it names in place of the active
// versions (an overlay, never an activation), real (scripted) model turns,
// real reads and the real guard, every write recorded and answered with
// MOCK_REPLY, confirmations answered on the spot, and nothing kept. Restored
// from tests/pending/sandbox.rs (main's suite, adapted to the engine: run ids
// are the engine's, so runs are found by workflow and link key), then the
// design's new tests 5–19.
mod support;
use agent::{
    agent::{Engine, LoopOptions},
    db::TraceStore,
    flows::WorkflowMeta,
    mcp::Mcp,
    providers::{ChatProvider, Message, ToolDef, assistant},
    sandbox::{MOCK_REPLY, TryOptions, try_draft, try_signal, try_signal_with},
    supervisor::{PendingSignal, create_supervisor},
    tracing::{Scope, TraceStart},
};
use async_trait::async_trait;
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use support::*;
use tokio_util::sync::CancellationToken;

const LOOP: &str = TELEGRAM_LOOP;
const SKILLS: &[(&str, &str)] = &[
    ("telegram", "---\ntools: *\n---\nTELEGRAM"),
    ("worker", "---\ntools: *\n---\nWORKER"),
    ("peer", "---\ntools: *\n---\nPEER"),
    ("scheduler", "---\ntools: *\n---\nSCHEDULER"),
    ("recovery", "---\ntools: [send_telegram_message]\n---\nRECOVERY"),
    ("workflow-author", "---\ntools: []\n---\nAUTHOR"),
];
const MESSAGE: &str = "Telegram message in chat 42 (forum topic thread_id=7).\nText: \"what is new in Rust?\"";

// ── the world ───────────────────────────────────────────────────────────────

// MCP as the sandbox meets it: reads the server marks read-only, writes it
// does not, and a queue whose latest signals give the signal types.
// `mark_seen` is read-only until `flipped` (test 16: a handler's own write).
#[derive(Default)]
struct World {
    calls: Mutex<Vec<(String, Map<String, Value>)>>,
    flipped: AtomicBool,
}

const READS: &[&str] = &[
    "list_signals",
    "get_telegram_chat_history",
    "get_peer_thread",
    "journal_tail",
    "read_doc",
    "get_timezone",
    "search_news",
];
// The Telegram tools the built-in route takes away from its loop (design 08)
// are all offered, so that route runs here as on the full MCP.
const WRITES: &[&str] = &[
    "send_telegram_message",
    "edit_telegram_message",
    "remember",
    "journal_append",
    "telegram_send_status",
    "start_typing",
    "send_telegram_chat_action",
];
const FLIPPING: &str = "mark_seen";

impl World {
    fn names(&self) -> Vec<String> {
        self.calls.lock().unwrap().iter().map(|(n, _)| n.clone()).collect()
    }
    fn writes(&self) -> Vec<String> {
        self.names().into_iter().filter(|n| WRITES.contains(&n.as_str()) || n == FLIPPING).collect()
    }
}

#[async_trait]
impl Mcp for World {
    fn tools(&self) -> Vec<ToolDef> {
        READS.iter().chain(WRITES).chain([&FLIPPING]).map(|n| ToolDef::new(n, n, json!({"type":"object"}))).collect()
    }
    fn read_only(&self, name: &str) -> bool {
        READS.contains(&name) || (name == FLIPPING && !self.flipped.load(Ordering::SeqCst))
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
            "get_peer_thread" => json!({"messages":[]}),
            "journal_tail" => json!({"entries":[]}),
            "read_doc" => json!({"ok":false,"error":"doc_not_found"}),
            "get_timezone" => json!({"timezone":"UTC"}),
            "search_news" => json!({"items":[{"title":"Rust 2.0 is out"}]}),
            "mark_seen" => json!({"seen":true}),
            _ => json!({"delivered":true,"messageId":700}),
        }
        .to_string())
    }
}

struct Sandbox {
    f: Fixture,
    world: Arc<World>,
}

// An engine whose guard answers `label`. `active`: the example loop is
// active (a version that answers differently) and its fixture a draft (v2)
// on top; otherwise neither.
async fn sandbox_on(provider: Arc<dyn ChatProvider>, label: &str, active: bool) -> Sandbox {
    let world = Arc::new(World::default());
    let guard = Arc::new(GuardApi::default());
    *guard.label.lock().unwrap() = Some(label.into());
    let f = fixture_on(provider, &[], SKILLS, |deps| {
        deps.mcp = world.clone();
        deps.decisions = Some(agent::decisions::decision_client("guard-test".into(), guard.clone()));
    });
    f.engine.ready().await.unwrap();
    if active {
        activate_definition(&f.engine, "telegram-loop", &active_loop()).await;
        assert_eq!(save(&f.engine, "telegram-loop", LOOP).await, 2);
    }
    Sandbox { f, world }
}

async fn sandbox(script: Vec<Message>, label: &str) -> (Sandbox, Arc<Scripted>) {
    let model = Arc::new(Scripted::new(script));
    (sandbox_on(model.clone(), label, true).await, model)
}

// The example loop as the active version: it answers with a prefix.
fn active_loop() -> String {
    let active = LOOP.replace("set: '${ { output: .reply } }'", "set: '${ { output: (\"active: \" + .reply) } }'");
    assert_ne!(active, LOOP);
    active
}

fn renamed(definition: &str, name: &str) -> String {
    let renamed = definition.replace("name: telegram-loop,", &format!("name: {name},"));
    assert_ne!(renamed, definition);
    renamed
}

async fn tool(engine: &Engine, name: &str, args: Value) -> anyhow::Result<Value> {
    let reply = agent::flows::tool(engine, name, args.as_object().unwrap()).await?;
    Ok(serde_json::from_str(&reply).unwrap())
}

async fn save(engine: &Engine, name: &str, definition: &str) -> u64 {
    let saved = tool(engine, "save_workflow", json!({"name": name, "definition": definition})).await;
    saved.unwrap_or_else(|e| panic!("saving {name}: {e:#}"))["version"].as_u64().unwrap()
}

fn tried(engine: &Engine, name: &str, definition: &str) -> bool {
    let digest = ouro_runtime::durable::digest_of(definition);
    WorkflowMeta::get(&engine.deps.db, name, &digest).unwrap().is_some_and(|m| m.tried_ms.is_some())
}

async fn active_version(engine: &Engine, name: &str) -> Option<u64> {
    engine.runtime.definitions(name).await.unwrap().active
}

impl Sandbox {
    async fn try_signal(&self, args: Value) -> anyhow::Result<Value> {
        self.try_with(args, TryOptions::default()).await
    }

    async fn try_with(&self, args: Value, options: TryOptions) -> anyhow::Result<Value> {
        let trace = self.f.engine.deps.tracer.trace(TraceStart {
            id: "author".into(),
            name: "author".into(),
            ..Default::default()
        });
        let scope: Scope = trace.clone();
        let reply = try_signal_with(
            &self.f.engine,
            args.as_object().unwrap(),
            &context(),
            &scope,
            &CancellationToken::new(),
            options,
        )
        .await;
        trace.end();
        Ok(serde_json::from_str(&reply?)?)
    }

    async fn try_draft(&self, args: Value) -> anyhow::Result<Value> {
        let trace = self.f.engine.deps.tracer.trace(TraceStart {
            id: "author".into(),
            name: "author".into(),
            ..Default::default()
        });
        let scope: Scope = trace.clone();
        let reply =
            try_draft(&self.f.engine, args.as_object().unwrap(), &context(), &scope, &CancellationToken::new()).await;
        trace.end();
        Ok(serde_json::from_str(&reply?)?)
    }

    fn rows(&self, table: &str) -> i64 {
        self.f.db.conn().query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0)).unwrap()
    }

    // Nothing of a try outlives it: no run in the real store, no card,
    // verdict, anchor or evidence in agent.db, no memory key of the agent's,
    // no write at MCP.
    async fn assert_nothing_kept(&self) {
        assert!(self.f.engine.runtime.get("signal:0").await.unwrap().is_none(), "no run in the real store");
        for table in ["confirmations", "guard_verdict_uses", "run_trace", "run_evidence", "evidence"] {
            assert_eq!(self.rows(table), 0, "{table}");
        }
        let kept: i64 = self
            .f
            .db
            .conn()
            .query_row("SELECT count(*) FROM memory WHERE key NOT LIKE 'engine.%'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(kept, 0, "no breaker, watermark or card wording");
        assert!(self.world.writes().is_empty(), "a write reached MCP: {:?}", self.world.names());
    }
}

fn telegram(confirm: Option<&str>) -> Value {
    let mut args = json!({"name":"telegram-loop","signal":{"source":"telegram","content":MESSAGE}});
    if let Some(confirm) = confirm {
        args["confirm"] = json!(confirm);
    }
    args
}

fn send(text: &str) -> Message {
    calls(&[("send_telegram_message", json!({"chatId":"42","messageThreadId":7,"text":text}))])
}

// Every run of the report (and of its recovery), depth first in tree order.
fn all_runs(report: &Value) -> Vec<&Value> {
    fn walk<'a>(runs: &'a Value, out: &mut Vec<&'a Value>) {
        for run in runs.as_array().into_iter().flatten() {
            out.push(run);
            walk(&run["children"], out);
        }
    }
    let mut out = Vec::new();
    walk(&report["runs"], &mut out);
    walk(&report["recovery"]["runs"], &mut out);
    out
}

// The runs of `workflow` (started under the link key `key`, when given).
fn runs_of<'a>(report: &'a Value, workflow: &str, key: Option<&str>) -> Vec<&'a Value> {
    all_runs(report).into_iter().filter(|r| r["workflow"] == workflow && key.is_none_or(|k| r["key"] == k)).collect()
}

fn run<'a>(report: &'a Value, workflow: &str, key: Option<&str>) -> &'a Value {
    runs_of(report, workflow, key).into_iter().next().unwrap_or_else(|| panic!("no run of {workflow} in {report:#}"))
}

fn path(run: &Value) -> Vec<&str> {
    run["path"].as_array().unwrap().iter().map(|p| p.as_str().unwrap()).collect()
}

fn functions(report: &Value) -> Vec<(&str, &str)> {
    report["writes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|w| (w["function"].as_str().unwrap(), w["via"].as_str().unwrap()))
        .collect()
}

// Test 19 (R2-M2): a try that read no run through the engine's own handle.
fn no_refusals(report: &Value) {
    assert!(report.get("sandbox_refusals").is_none(), "a handler read the real store: {report:#}");
}

// ── 1. the restored suite ───────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_telegram_signal_runs_the_draft_chain_with_real_reads_and_mocked_writes() {
    let mut first = calls(&[("search_news", json!({"query":"rust"}))]);
    first.insert("reasoning_content".into(), json!("Look the news up first."));
    let (s, model) = sandbox(vec![first, send("Rust 2.0 is out")], "allow").await;
    let untried = json!({"name":"telegram-loop","version":2});
    let refused = tool(&s.f.engine, "activate_workflow", untried.clone()).await.unwrap_err();
    assert!(refused.to_string().contains("has not completed a dry run"), "{refused}");

    let report = s.try_signal(telegram(None)).await.unwrap();
    assert_eq!(report["status"], "completed", "{report:#}");
    no_refusals(&report);
    assert_eq!(report["output"], "Rust 2.0 is out", "the draft loop's reply, not the active one's");
    assert_eq!(report["ended"]["by"], "workflow rule");
    assert_eq!(report["ended"]["at"], "clear");
    // The loop's own status steps are direct writes, mocked like the model's.
    let status = |text: &str| json!({"chatId":"42","messageThreadId":7,"id":"status:0","text":text});
    let writes = report["writes"].as_array().unwrap();
    assert_eq!(
        functions(&report),
        [("telegram_send_status", "yaml"), ("send_telegram_message", "tool"), ("telegram_send_status", "yaml")]
    );
    assert_eq!(writes[0]["arguments"], status("👀"));
    assert_eq!(writes[1]["arguments"], json!({"chatId":"42","messageThreadId":7,"text":"Rust 2.0 is out"}));
    assert_eq!(writes[2]["arguments"], status(""));
    assert_eq!(report["drafts"], json!([{"name":"telegram-loop","version":2,"ran":true,"outcome":"completed"}]));

    // The path of each workflow, with every switch's case.
    let dispatcher = &report["runs"][0];
    assert_eq!((dispatcher["workflow"].as_str(), dispatcher["version"].as_str()), (Some("signal"), Some("built-in")));
    let steps = path(dispatcher);
    for step in ["offer_button (skipped)", "after_button → ordinary", "route", "after_route → started"] {
        assert!(steps.contains(&step), "{step}: {steps:?}");
    }
    let primary = run(&report, "telegram-loop", Some("route"));
    assert_eq!(primary["version"], "draft v2");
    let steps = path(primary);
    for step in ["input", "activity", "pending_confirmations", "preselect", "setup", "status"] {
        assert!(steps.contains(&step), "the loop gathers its own context: {step}: {steps:?}");
    }
    let last: Vec<&str> = steps.iter().rev().take(3).copied().collect();
    assert_eq!(last, ["clear", "reply", "next[1] → replied"], "the second pass of `next` took `replied`");

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
    let guards = runs_of(&report, "guard", Some("guard"));
    assert_eq!(guards.len(), 1, "only the write's round is reviewed: {report:#}");
    assert!(path(guards[0]).contains(&"route → ordinary"), "{:#}", guards[0]);
    assert!(s.world.names().iter().any(|n| n == "search_news"));
    assert!(s.world.names().iter().any(|n| n == "get_telegram_chat_history"));
    assert_eq!(model.seen.lock().unwrap().len(), 2, "two real model turns");

    // Nothing kept; the drafts that ran are tried, which activation needs.
    s.assert_nothing_kept().await;
    assert_eq!(active_version(&s.f.engine, "telegram-loop").await, Some(1), "stays active");
    assert!(tried(&s.f.engine, "telegram-loop", LOOP));
    tool(&s.f.engine, "activate_workflow", untried).await.unwrap();

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
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn confirmations_are_answered_on_the_spot_through_the_real_guard() {
    // What a try must not do is wait out a hold. The engine's dry run cuts
    // them (`Limits::max_hold` is zero there, and a join's child is not
    // driven in real time), and the dry run exposes no record of a wait that
    // ended by its timeout, so the structural side is the engine's own test
    // (ouro's `a_dry_run_cuts_holds_short_...`). Here a wall-clock bound
    // stays only as a backstop, far above any load: it catches a round's
    // PT60S join and a worker's PT15M (each alone exceeds it), not the
    // guard's PT5S answer wait (design 05, deviation 3).
    for (confirm, label) in [(None, "allow"), (Some("decline"), "declined"), (Some("expire"), "expired")] {
        let (s, _) =
            sandbox(vec![calls(&[("remember", json!({"text":"likes Rust"}))]), send("done")], "needs_confirmation")
                .await;
        let start = Instant::now();
        let report = s.try_signal(telegram(confirm)).await.unwrap();
        let took = start.elapsed();
        assert!(took < Duration::from_secs(60), "{label}: the try waited out a hold ({took:?})");
        assert_eq!(report["status"], "completed", "{label}: {report:#}");
        no_refusals(&report);
        assert_eq!(report["output"], "done");
        let primary = run(&report, "telegram-loop", Some("route"));
        let remembered = &primary["turns"][0]["calls"][0];
        assert_eq!(remembered["verdict"]["label"], label, "{report:#}");
        let word = confirm.unwrap_or("accept");
        assert_eq!(remembered["confirmation"], format!("confirmation requested: remember → auto-{word}"), "{report:#}");
        let guard = path(runs_of(&report, "guard", Some("guard"))[0]);
        assert!(
            guard.contains(&"route → ask") && guard.contains(&"confirm") && guard.contains(&"resolve"),
            "{guard:?}"
        );
        let written: Vec<&str> = functions(&report).into_iter().map(|(f, _)| f).collect();
        if label == "allow" {
            assert_eq!(remembered["mock"], true);
            assert_eq!(written, ["telegram_send_status", "remember", "send_telegram_message", "telegram_send_status"]);
            assert!(guard.contains(&"outcome"), "{guard:?}");
        } else {
            assert_eq!(remembered["ok"], false);
            assert!(remembered["result"].as_str().unwrap().starts_with("[guard] blocked:"), "{remembered}");
            assert_eq!(
                written,
                ["telegram_send_status", "send_telegram_message", "telegram_send_status"],
                "a refused write is not even mocked"
            );
        }
        if label == "expired" {
            assert!(guard.iter().any(|p| p.starts_with("listen faulted")), "{guard:?}");
            assert!(guard.contains(&"expired") && !guard.contains(&"outcome"), "{guard:?}");
        }
        s.assert_nothing_kept().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_inside_runs_dry_and_is_nested_in_the_trace() {
    let (s, _) = sandbox(
        vec![
            calls(&[("invoke_sub_agent", json!({"prompt":"note the request"}))]),
            calls(&[("remember", json!({"text":"Rust question"}))]),
            assistant("noted"),
            send("noted it"),
        ],
        "allow",
    )
    .await;
    let report = s.try_signal(telegram(None)).await.unwrap();
    assert_eq!(report["status"], "completed", "{report:#}");
    no_refusals(&report);
    assert_eq!(report["output"], "noted it");
    let worker = run(&report, "agent-loop", Some("worker:1.0"));
    assert_eq!(worker["version"], "built-in");
    let remembered = &worker["turns"][0]["calls"][0];
    assert_eq!((remembered["name"].as_str(), remembered["mock"].as_bool()), (Some("remember"), Some(true)));
    assert_eq!(worker["ended"]["by"], "answer");
    assert_eq!(worker["ended"]["output"], "noted");
    let written: Vec<&str> = functions(&report).into_iter().map(|(f, _)| f).collect();
    assert_eq!(written, ["telegram_send_status", "remember", "send_telegram_message", "telegram_send_status"]);
    // The worker's turns trace inside the builder's dry run.
    let (_, observations) = s.f.db.get_trace("author").unwrap().unwrap();
    let delegated = observations.iter().find(|o| o.name == "invoke_sub_agent").unwrap();
    let dry = observations.iter().find(|o| o.name == "dry-run").unwrap();
    let mut parent = delegated.parent_observation_id.clone();
    let mut under_dry_run = false;
    while let Some(id) = parent {
        under_dry_run |= id == dry.id;
        parent = observations.iter().find(|o| o.id == id).and_then(|o| o.parent_observation_id.clone());
    }
    assert!(under_dry_run, "the worker nests in the dry run");
    s.assert_nothing_kept().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_empty_signal_discovers_types_without_a_draft_or_a_run() {
    let (s, model) = sandbox(vec![], "allow").await;
    let report = s.try_signal(json!({"name":"not-saved-yet","signal":{},"drafts":["also-not-saved"]})).await.unwrap();
    assert_eq!(report["status"], "signal_types");
    let types = report["signal_types"].as_array().unwrap();
    let telegram = types.iter().find(|t| t["source"] == "telegram").unwrap();
    assert!(telegram["latest"]["content"].as_str().unwrap().contains("thread_id=7"));
    assert!(telegram["latest"]["content"].as_str().unwrap().contains("latest"));
    assert!(types.iter().any(|t| t["source"] == "peer"));
    assert!(types.iter().any(|t| t["source"] == "scheduler"));
    assert!(report.get("runs").is_none());
    assert!(model.seen.lock().unwrap().is_empty());
    assert_eq!(s.world.names(), ["list_signals"]);
    assert!(!tried(&s.f.engine, "telegram-loop", LOOP));
    s.assert_nothing_kept().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_signal_type_is_one_this_agent_receives() {
    let (s, model) =
        sandbox(vec![calls(&[("try_workflow", json!({"name":"signal","signal":{}}))]), assistant("seen")], "allow")
            .await;
    for signal in [json!({"source":"carrier-pigeon","content":"hi"}), json!({"content":"missing source"})] {
        let error = s.try_signal(json!({"name":"signal","signal":signal})).await.unwrap_err().to_string();
        assert!(error.contains("is not a signal type") || error.contains("is missing"), "{error}");
        // The latest real signal of each source, and the agent's own scheduler.
        assert!(
            error.contains("thread_id=7") && error.contains("\\\"latest\\\"") && !error.contains("older"),
            "{error}"
        );
        assert!(error.contains("\"source\":\"peer\"") && error.contains("\"source\":\"scheduler\""), "{error}");
    }
    assert!(model.seen.lock().unwrap().is_empty(), "nothing ran");
    let error =
        s.try_signal(json!({"name":"nope","signal":{"source":"telegram","content":MESSAGE}})).await.unwrap_err();
    assert!(error.to_string().contains("no saved workflow `nope`"), "{error}");
    // Without a signal, what runs on signals is refused, and the author sees the types.
    let old = s.try_draft(json!({"name":"telegram-loop"})).await.unwrap_err();
    assert!(old.to_string().contains("try it with a signal"), "{old}");
    let mut opts = LoopOptions::new("author", context());
    opts.skills = vec!["workflow-author".into()];
    let author = s.f.engine.start_loop(opts).await.unwrap();
    assert_eq!(author.send("try it".into()).await.unwrap(), "seen");
    let seen = model.seen.lock().unwrap();
    let reply = seen[1].messages.last().unwrap()["content"].as_str().unwrap();
    assert!(
        !reply.contains("try_workflow error") && reply.contains("signal_types") && reply.contains("scheduler"),
        "{reply}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_jq_error_in_a_draft_names_the_expression_and_the_value_it_met() {
    let (s, _) = sandbox(vec![assistant("reported")], "allow").await;
    let broken =
        LOOP.replace("request: '${ $context.signal.content }'", "request: '${ $context.signal.content | .text }'");
    assert_ne!(broken, LOOP);
    assert_eq!(save(&s.f.engine, "telegram-loop", &broken).await, 3);
    let report = s.try_signal(telegram(None)).await.unwrap();
    assert_eq!(report["status"], "faulted", "{report:#}");
    no_refusals(&report);
    let primary = run(&report, "telegram-loop", Some("route"));
    assert_eq!(primary["version"], "draft v3");
    let fault = path(primary).into_iter().find(|p| p.contains("faulted")).unwrap().to_owned();
    assert!(fault.starts_with("preselect faulted") && fault.contains(".text"), "{fault}");
    assert!(!tried(&s.f.engine, "telegram-loop", &broken));
    s.assert_nothing_kept().await;
    let definitions = s.f.engine.runtime.definitions("telegram-loop").await.unwrap();
    assert_eq!(definitions.consecutive_faults, 0, "its fault counts nowhere");
}

// The dispatcher itself can be a draft: a peer signal takes its default path,
// on the draft, while the live `signal` stays the built-in one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_signal_override_draft_runs_in_place_of_the_built_in_dispatcher() {
    let (s, model) = sandbox(vec![assistant("terse answer")], "allow").await;
    let builtin = without_breaker(agent::supervisor::SIGNAL);
    let terse = builtin.replace(r#""Call find_skills only when"#, r#""Answer tersely.", "Call find_skills only when"#);
    assert_ne!(terse, builtin);
    let version = save(&s.f.engine, "signal", &terse).await;
    let peer = "Message from claude-code, thread 3:\n\nwhat time is it?";
    let report = s.try_signal(json!({"name":"signal","signal":{"source":"peer","content":peer}})).await.unwrap();
    assert_eq!(report["status"], "completed", "{report:#}");
    no_refusals(&report);
    assert_eq!(report["output"], "terse answer");
    let root = &report["runs"][0];
    assert_eq!(root["workflow"], "signal");
    assert_eq!(root["version"], format!("draft v{version}"));
    assert_eq!(report["ended"]["by"], "answer");
    let system = model.seen.lock().unwrap()[0].messages[0]["content"].as_str().unwrap().to_owned();
    assert!(system.contains("Answer tersely."), "{system}");
    assert!(tried(&s.f.engine, "signal", &terse));
    let builtin_version = agent::flows::builtin_version(&s.f.engine.runtime, "signal").await.unwrap();
    assert_eq!(active_version(&s.f.engine, "signal").await, builtin_version, "the built-in still handles signals");
    s.assert_nothing_kept().await;
}

// ── 5. D1: nothing persists ─────────────────────────────────────────────────

// Every row of every table of a SQLite file, as text, but what a try may
// change: `workflow_meta.tried_ms`, the trace mirror (the builder's own
// trace), the runtime's own memory keys (the reconciler's watermark moves on
// its own), the engine worker's heartbeat rows and the tool snapshot's
// last-seen times.
fn rows(path: &std::path::Path) -> BTreeMap<String, Vec<String>> {
    let conn = rusqlite::Connection::open(path).unwrap();
    let tables: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    let mut all = BTreeMap::new();
    for table in tables.into_iter().filter(|t| t != "traces") {
        let mut statement = conn.prepare(&format!("SELECT * FROM \"{table}\"")).unwrap();
        let names: Vec<String> = statement.column_names().into_iter().map(str::to_owned).collect();
        let mut rows: Vec<String> = statement
            .query_map([], |r| {
                let mut row = Vec::new();
                for (i, name) in names.iter().enumerate() {
                    let skipped = (table == "workflow_meta" && name == "tried_ms")
                        || (table == "tool_snapshot" && name == "seen_ms");
                    if !skipped {
                        let value: rusqlite::types::Value = r.get(i)?;
                        row.push(format!("{name}={value:?}"));
                    }
                }
                Ok(row.join(","))
            })
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        if table == "memory" {
            rows.retain(|r| !r.contains("Text(\"engine."));
        }
        // The live engine worker's heartbeat beats on its own.
        if table == "ouro_rows" {
            rows.retain(|r| !r.starts_with("kind=Text(\"worker"));
        }
        rows.sort();
        all.insert(table, rows);
    }
    all
}

// The rows that differ, per table: none.
fn assert_same(before: &BTreeMap<String, Vec<String>>, after: &BTreeMap<String, Vec<String>>, what: &str) {
    let mut changed = Vec::new();
    for table in before.keys().chain(after.keys()).collect::<std::collections::BTreeSet<_>>() {
        let (b, a) = (before.get(table).cloned().unwrap_or_default(), after.get(table).cloned().unwrap_or_default());
        let gone: Vec<&String> = b.iter().filter(|r| !a.contains(r)).collect();
        let new: Vec<&String> = a.iter().filter(|r| !b.contains(r)).collect();
        if !gone.is_empty() || !new.is_empty() {
            changed.push(format!("{table}: gone {gone:?}, new {new:?}"));
        }
    }
    assert!(changed.is_empty(), "{what}: {}", changed.join("\n"));
}

fn trace_ids(db: &agent::db::AgentDb) -> Vec<String> {
    let mut ids: Vec<String> = db.list_recent(100, None).unwrap().into_iter().map(|t| t.id).collect();
    ids.sort();
    ids
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_try_that_writes_confirms_and_starts_a_worker_changes_nothing_but_tried() {
    let (s, _) = sandbox(
        vec![
            calls(&[("remember", json!({"text":"likes Rust"})), ("invoke_sub_agent", json!({"prompt":"look it up"}))]),
            calls(&[("search_news", json!({"query":"rust"}))]),
            assistant("found it"),
            send("Rust 2.0 is out"),
        ],
        "needs_confirmation",
    )
    .await;
    let root = s.f.root.path();
    let (agent_db, engine_db) = (root.join("agent.db"), root.join("engine.db"));
    let (agent_before, engine_before, traces_before) = (rows(&agent_db), rows(&engine_db), trace_ids(&s.f.db));
    let report = s.try_signal(telegram(None)).await.unwrap();
    assert_eq!(report["status"], "completed", "{report:#}");
    no_refusals(&report);
    let written: Vec<&str> = functions(&report).into_iter().map(|(f, _)| f).collect();
    assert!(written.contains(&"remember") && written.contains(&"send_telegram_message"), "{written:?}");
    assert!(!runs_of(&report, "agent-loop", Some("worker:1.1")).is_empty(), "a worker ran: {report:#}");
    assert!(runs_of(&report, "guard", Some("guard")).iter().any(|g| path(g).contains(&"route → ask")), "a card");
    assert_same(&agent_before, &rows(&agent_db), "agent.db as it was, but tried");
    assert_same(&engine_before, &rows(&engine_db), "the engine's store as it was");
    let mut traces = traces_before.clone();
    traces.push("author".into());
    traces.sort();
    assert_eq!(trace_ids(&s.f.db), traces, "no signal trace of its own");
    assert!(s.world.writes().is_empty(), "no write reached MCP: {:?}", s.world.names());
    assert!(tried(&s.f.engine, "telegram-loop", LOOP));
}

// ── 6. D2: tried only by a completed try, for drafts whose runs all completed

// A turn that waits until its loop is cancelled (a try's timeout stops it).
fn stuck() -> Turn {
    turn_async(|p| async move {
        p.cancel.cancelled().await;
        Err(anyhow::anyhow!("stopped"))
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_a_completed_try_marks_tried_and_only_drafts_whose_runs_all_completed() {
    // Faulted: the draft's runs fault.
    let (s, _) = sandbox(vec![assistant("reported")], "allow").await;
    let faulting_loop = faulting(LOOP);
    save(&s.f.engine, "telegram-loop", &faulting_loop).await;
    let report = s.try_signal(telegram(None)).await.unwrap();
    assert_eq!(report["status"], "faulted", "{report:#}");
    assert_eq!(report["drafts"][0]["ran"], true);
    assert!(!tried(&s.f.engine, "telegram-loop", &faulting_loop));

    // Faulted after the draft completed: the routed loop's runs all
    // completed, but a draft of `signal` faults once it settled; the try
    // faulted, so nothing is tried, not even the loop.
    let (s, _) = sandbox(vec![send("Rust 2.0 is out"), assistant("reported")], "allow").await;
    let late = without_breaker(agent::supervisor::SIGNAL).replace(
        "        - answer:\n            set: '${ { output: .output } }'",
        "        - answer:\n            raise: { error: { type: 'https://example.com/late', status: 500, detail: late } }",
    );
    assert_ne!(late, without_breaker(agent::supervisor::SIGNAL));
    save(&s.f.engine, "signal", &late).await;
    let mut args = telegram(None);
    args["drafts"] = json!(["signal"]);
    let report = s.try_signal(args).await.unwrap();
    assert_eq!(report["status"], "faulted", "{report:#}");
    let draft = |name: &str| report["drafts"].as_array().unwrap().iter().find(|d| d["name"] == name).unwrap().clone();
    assert_eq!(draft("telegram-loop")["outcome"], "completed", "{report:#}");
    assert!(!tried(&s.f.engine, "telegram-loop", LOOP), "a faulted try marks nothing");
    assert!(!tried(&s.f.engine, "signal", &late));

    // Timed out: stopped by the try's own budget.
    let s = sandbox_on(Turns::new(vec![stuck()]), "allow", true).await;
    let options = TryOptions { timeout: Duration::from_millis(500), ..TryOptions::default() };
    let report = s.try_with(telegram(None), options).await.unwrap();
    assert_eq!(report["status"], "timed_out", "{report:#}");
    assert!(!tried(&s.f.engine, "telegram-loop", LOOP));
    s.assert_nothing_kept().await;

    // Completed: the route draft is tried; a worker's loop draft that
    // faulted and a draft no route reached are not.
    let research = faulting(&variant("research-loop"));
    let unused = variant("unused-loop");
    let (s, _) = sandbox(
        vec![
            calls(&[("invoke_sub_agent", json!({"prompt":"look it up","loop":"research-loop"}))]),
            send("could not look it up"),
        ],
        "allow",
    )
    .await;
    save(&s.f.engine, "research-loop", &research).await;
    save(&s.f.engine, "unused-loop", &unused).await;
    let mut args = telegram(None);
    args["drafts"] = json!(["research-loop", "unused-loop"]);
    let report = s.try_signal(args).await.unwrap();
    assert_eq!(report["status"], "completed", "{report:#}");
    no_refusals(&report);
    let draft = |name: &str| report["drafts"].as_array().unwrap().iter().find(|d| d["name"] == name).unwrap().clone();
    assert_eq!(draft("telegram-loop")["ran"], true);
    assert_eq!(draft("telegram-loop")["outcome"], "completed");
    assert_eq!(draft("research-loop")["ran"], true);
    assert!(draft("research-loop")["outcome"].as_str().unwrap().starts_with("faulted"), "{report:#}");
    assert_eq!(draft("unused-loop")["ran"], false);
    assert!(tried(&s.f.engine, "telegram-loop", LOOP));
    assert!(!tried(&s.f.engine, "research-loop", &research));
    assert!(!tried(&s.f.engine, "unused-loop", &unused));
    assert!(report["next"].as_str().unwrap().contains("unused-loop"), "{report:#}");
}

// A scheduled task's saved workflow that faults before any write hands its
// task over: the signal completes, the draft is not tried.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_handed_over_saved_draft_is_not_tried() {
    let (s, _) = sandbox(vec![assistant("did it myself")], "allow").await;
    let nightly = "document: { dsl: '1.0.4', namespace: agent, name: nightly, version: '1.0.0' }\ndo:\n  - boom: { raise: { error: { type: 'https://example.com/boom', status: 500, detail: boom } } }\n";
    save(&s.f.engine, "nightly", nightly).await;
    let args = json!({"name":"nightly","signal":{"source":"scheduler","content":"do the task","payload":{"workflow":"nightly"}}});
    let report = s.try_signal(args).await.unwrap();
    assert_eq!(report["status"], "completed", "{report:#}");
    no_refusals(&report);
    assert_eq!(report["output"], "did it myself");
    let draft = &report["drafts"][0];
    assert_eq!((draft["name"].as_str(), draft["ran"].as_bool()), (Some("nightly"), Some(true)));
    assert!(draft["outcome"].as_str().unwrap().starts_with("faulted"), "{report:#}");
    assert!(!tried(&s.f.engine, "nightly", nightly));
    s.assert_nothing_kept().await;
}

// ── 7. D3: a try never saves or activates ───────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_drafts_own_save_and_activation_are_mocked_writes() {
    let fresh = renamed(LOOP, "fresh-loop");
    let (s, _) = sandbox(
        vec![
            calls(&[
                ("activate_workflow", json!({"name":"telegram-loop","version":2})),
                ("invoke_sub_agent", json!({"prompt":"save it","skills":["workflow-author"]})),
            ]),
            calls(&[("save_workflow", json!({"name":"fresh-loop","definition":fresh}))]),
            assistant("saved"),
            send("done"),
        ],
        "allow",
    )
    .await;
    let report = s.try_signal(telegram(None)).await.unwrap();
    assert_eq!(report["status"], "completed", "{report:#}");
    no_refusals(&report);
    let written = functions(&report);
    assert!(
        written.contains(&("activate_workflow", "tool")) && written.contains(&("save_workflow", "tool")),
        "{written:?}"
    );
    assert_eq!(active_version(&s.f.engine, "telegram-loop").await, Some(1), "no activation");
    assert!(s.f.engine.runtime.definitions("fresh-loop").await.unwrap().versions.is_empty(), "no version saved");
    let digest = ouro_runtime::durable::digest_of(&fresh);
    assert!(WorkflowMeta::get(&s.f.engine.deps.db, "fresh-loop", &digest).unwrap().is_none());
    s.assert_nothing_kept().await;
}

// ── 8. D5: drafts only in the try ───────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_live_signal_during_a_try_runs_the_active_version() {
    let release = Arc::new(tokio::sync::Notify::new());
    let held = release.clone();
    let model = Turns::new(vec![
        turn_async(move |_| async move {
            held.notified().await;
            Ok(send("from the draft"))
        }),
        turn(|_| calls(&[("send_telegram_message", json!({"chatId":"42","text":"live reply"}))])),
    ]);
    let s = Arc::new(sandbox_on(model.clone(), "allow", true).await);
    let trying = {
        let s = s.clone();
        tokio::spawn(async move { s.try_signal(telegram(None)).await })
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    while model.requests.lock().unwrap().is_empty() {
        assert!(Instant::now() < deadline, "the try's first turn never came");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let supervisor = create_supervisor(s.f.engine.clone());
    let live = PendingSignal {
        id: 9,
        source: "telegram".into(),
        content: "Telegram message in chat 42.\nText: \"hi\"".into(),
        env_context: None,
        created_at: "2026-10-26 12:00:00".into(),
        payload: None,
    };
    assert_eq!(supervisor.run_signal(&live).await.unwrap(), "active: live reply", "the active version ran");
    assert_eq!(active_version(&s.f.engine, "telegram-loop").await, Some(1));
    release.notify_one();
    let report = trying.await.unwrap().unwrap();
    assert_eq!(report["status"], "completed", "{report:#}");
    assert_eq!(report["output"], "from the draft");
    assert_eq!(run(&report, "telegram-loop", Some("route"))["version"], "draft v2");
    supervisor.shutdown().await;
}

// ── 9. activation end to end ────────────────────────────────────────────────

const DIGEST: &str = r#"document: { dsl: '1.0.4', namespace: agent, name: digest, version: '1.0.0' }
do:
  - search:
      call: search_news
      with: { query: '${ "rust since " + (.env.newsLastReadAt // "ever") }' }
  - compose:
      call: llm_compose
      with: { skill: worker, prompt: Write the digest., input: { items: '${ .items }' } }
  - send:
      call: send_telegram_message
      with: { chatId: '42', text: '${ . }' }
  - mark:
      call: set_memory
      with: { key: news_digest.last_read_at, value: '${ $workflow.input.env.now | tostring }' }
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_saved_version_activates_only_after_a_try() {
    let (s, _) = sandbox(vec![assistant("Today: Rust 2.0")], "allow").await;
    let version = save(&s.f.engine, "digest", DIGEST).await;
    let activate = json!({"name":"digest","version":version});
    let refused = tool(&s.f.engine, "activate_workflow", activate.clone()).await.unwrap_err();
    assert!(refused.to_string().contains("has not completed a dry run"), "{refused}");
    let report = s.try_draft(json!({"name":"digest"})).await.unwrap();
    assert_eq!(report["status"], "completed", "{report:#}");
    tool(&s.f.engine, "activate_workflow", activate).await.unwrap();
    assert_eq!(active_version(&s.f.engine, "digest").await, Some(version));
}

// ── 10. R-H2: joins never hold a try ────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_guarded_rounds_and_a_worker_take_seconds_not_holds() {
    let (s, _) = sandbox(
        vec![
            calls(&[("remember", json!({"text":"a"})), ("invoke_sub_agent", json!({"prompt":"note it"}))]),
            assistant("noted"),
            calls(&[("remember", json!({"text":"b"}))]),
            send("done"),
        ],
        "allow",
    )
    .await;
    let start = Instant::now();
    let report = s.try_signal(telegram(None)).await.unwrap();
    assert_eq!(report["status"], "completed", "{report:#}");
    // One held join (a round's PT60S, a worker's PT15M) would exceed it
    // (design 05, deviation 3).
    assert!(start.elapsed() < Duration::from_secs(30), "{:?}", start.elapsed());
    assert!(runs_of(&report, "guard", Some("guard")).len() >= 2, "two guarded rounds: {report:#}");
    assert!(!runs_of(&report, "agent-loop", Some("worker:1.1")).is_empty(), "{report:#}");
}

// ── 11. R-H4: drafts are an overlay of what is active ───────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_route_draft_of_a_name_never_activated_runs_and_a_narrowed_route_does_not_match() {
    // Never activated: its own `when` routes the signal to it.
    let s = sandbox_on(Arc::new(Scripted::new(vec![send("from the new loop")])), "allow", false).await;
    let fresh = renamed(LOOP, "tg-new");
    save(&s.f.engine, "tg-new", &fresh).await;
    let mut args = telegram(None);
    args["name"] = json!("tg-new");
    let report = s.try_signal(args).await.unwrap();
    assert_eq!(report["status"], "completed", "{report:#}");
    no_refusals(&report);
    assert_eq!(run(&report, "tg-new", Some("route"))["version"], "draft v1");
    assert!(tried(&s.f.engine, "tg-new", &fresh));
    assert_eq!(active_version(&s.f.engine, "tg-new").await, None, "nothing activated");

    // Narrowed: only presses route to the draft now, so a message takes the
    // next route, the built-in `telegram` (design 08), although the active
    // version's `when` matched it.
    let (s, _) = sandbox(vec![assistant("default path"), assistant(r#"{"reply":"default path"}"#)], "allow").await;
    let narrowed = LOOP.replace("when: { source: [telegram, telegram_button] }", "when: { source: telegram_button }");
    assert_ne!(narrowed, LOOP);
    save(&s.f.engine, "telegram-loop", &narrowed).await;
    let report = s.try_signal(telegram(None)).await.unwrap();
    assert_eq!(report["status"], "completed", "{report:#}");
    assert_eq!(report["drafts"][0]["ran"], false, "{report:#}");
    assert!(runs_of(&report, "telegram-loop", None).is_empty(), "neither the draft nor the active one: {report:#}");
    assert_eq!(run(&report, "telegram", Some("route"))["version"], "built-in");
    assert_eq!(run(&report, "agent-loop", Some("loop"))["version"], "built-in");
    assert!(!tried(&s.f.engine, "telegram-loop", &narrowed));
}

// ── 12. R-H5: a fix of a tripped name becomes tried; a paused name is set aside

async fn trip(engine: &Arc<Engine>, name: &str) {
    for n in 0..3 {
        let mut opts = LoopOptions::new(format!("trip-{name}-{n}"), context());
        opts.skills = vec!["worker".into()];
        opts.workflow = name.into();
        assert!(engine.start_loop(opts).await.unwrap().send("go".into()).await.is_err());
    }
    let definitions = engine.runtime.definitions(name).await.unwrap();
    assert!(definitions.tripped.as_ref().is_some_and(|t| t.breaker), "three faults in a row trip it");
}

async fn pause(engine: &Engine, name: &str, note: &str) {
    let request =
        ouro_runtime::control::Request { id: format!("pause-{name}"), actor: "operator".into(), reason: "test".into() };
    engine.control.pause(&request, name, note).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_draft_of_a_tripped_name_is_tried_and_one_of_a_paused_name_is_set_aside() {
    let s = sandbox_on(Arc::new(Scripted::new(vec![send("fixed")])), "allow", false).await;
    let broken = faulting(&renamed(LOOP, "tg-loop"));
    activate_definition(&s.f.engine, "tg-loop", &broken).await;
    trip(&s.f.engine, "tg-loop").await;
    let fixed = renamed(LOOP, "tg-loop");
    let version = save(&s.f.engine, "tg-loop", &fixed).await;
    let mut args = telegram(None);
    args["name"] = json!("tg-loop");
    let report = s.try_signal(args.clone()).await.unwrap();
    assert_eq!(report["status"], "completed", "{report:#}");
    no_refusals(&report);
    assert_eq!(run(&report, "tg-loop", Some("route"))["version"], format!("draft v{version}"));
    assert!(tried(&s.f.engine, "tg-loop", &fixed));
    tool(&s.f.engine, "activate_workflow", json!({"name":"tg-loop","version":version})).await.unwrap();
    assert!(s.f.engine.runtime.definitions("tg-loop").await.unwrap().tripped.is_none());

    // Paused: the operator's pause stands for the draft too (the built-in
    // Telegram route takes the signal, design 08).
    let model = Scripted::new(vec![assistant("default path"), assistant(r#"{"reply":"default path"}"#)]);
    let s = sandbox_on(Arc::new(model), "allow", false).await;
    activate_definition(&s.f.engine, "tg-loop", &renamed(&active_loop(), "tg-loop")).await;
    let version = save(&s.f.engine, "tg-loop", &fixed).await;
    pause(&s.f.engine, "tg-loop", "maintenance window").await;
    let report = s.try_signal(args).await.unwrap();
    assert_eq!(report["status"], "completed", "{report:#}");
    let draft = &report["drafts"][0];
    assert_eq!((draft["ran"].as_bool(), draft["version"].as_u64()), (Some(false), Some(version)));
    let outcome = draft["outcome"].as_str().unwrap();
    assert!(outcome.contains("set aside") && outcome.contains("an operator paused it"), "{outcome}");
    assert!(!tried(&s.f.engine, "tg-loop", &fixed));
}

// ── 13. R-H6/D4: each answer through the real guard, no card edited ─────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_answer_gives_its_verdict_and_no_card_is_edited() {
    for (confirm, label, ran) in
        [("accept", "allow", true), ("decline", "declined", false), ("expire", "expired", false)]
    {
        let (s, _) =
            sandbox(vec![calls(&[("remember", json!({"text":"likes Rust"}))]), send("done")], "needs_confirmation")
                .await;
        let report = s.try_signal(telegram(Some(confirm))).await.unwrap();
        assert_eq!(report["status"], "completed", "{confirm}: {report:#}");
        assert_eq!(report["confirm"], confirm);
        let remembered = &run(&report, "telegram-loop", Some("route"))["turns"][0]["calls"][0];
        assert_eq!(remembered["verdict"]["label"], label, "{report:#}");
        let written = functions(&report);
        assert_eq!(written.contains(&("remember", "tool")), ran, "{confirm}: {written:?}");
        assert!(!written.iter().any(|(f, _)| *f == "edit_telegram_message"), "no card to edit: {written:?}");
        assert!(!s.world.names().iter().any(|n| n == "edit_telegram_message" || n == "send_telegram_message"));
    }
}

// ── 14. R-M2/R2-H2: recovery as a second dry run ────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_faulted_chain_reports_its_recovery_and_a_completed_one_has_none() {
    let (s, _) = sandbox(vec![send("Sorry, that failed."), assistant("reported")], "allow").await;
    save(&s.f.engine, "telegram-loop", &faulting(LOOP)).await;
    let report = s.try_signal(telegram(None)).await.unwrap();
    assert_eq!(report["status"], "faulted", "{report:#}");
    no_refusals(&report);
    let recovery = &report["recovery"];
    let reporter = &recovery["runs"][0];
    assert_eq!(reporter["workflow"], "agent-loop", "{report:#}");
    assert_eq!(reporter["run"], "recovery:0", "{report:#}");
    assert_eq!(reporter["ended"]["output"], "reported");
    assert_eq!(recovery["writes"][0]["function"], "send_telegram_message", "{report:#}");
    assert_eq!(recovery["writes"][0]["via"], "tool");
    s.assert_nothing_kept().await;

    let (s, _) = sandbox(vec![send("fine")], "allow").await;
    let report = s.try_signal(telegram(None)).await.unwrap();
    assert_eq!(report["status"], "completed", "{report:#}");
    assert!(report.get("recovery").is_none(), "{report:#}");
}

// ── 15. R-M5: the root goes through the live start rule ─────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tripped_signal_override_not_listed_runs_the_built_in_with_its_line() {
    let (s, model) = sandbox(vec![send("still answered")], "allow").await;
    let supervisor = create_supervisor(s.f.engine.clone());
    let version =
        activate_definition(&s.f.engine, "signal", &faulting(&without_breaker(agent::supervisor::SIGNAL))).await;
    for id in 41..=43 {
        let signal = PendingSignal {
            id,
            source: "peer".into(),
            content: "hello".into(),
            env_context: None,
            created_at: "2026-10-26 12:00:00".into(),
            payload: None,
        };
        assert!(supervisor.run_signal(&signal).await.is_err());
    }
    let report = s.try_signal(telegram(None)).await.unwrap();
    assert_eq!(report["status"], "completed", "{report:#}");
    no_refusals(&report);
    let root = &report["runs"][0];
    assert_eq!((root["workflow"].as_str(), root["version"].as_str()), (Some("signal"), Some("built-in")));
    let line = format!("Workflow `signal` v{version} was set aside (its breaker tripped after 3 faults in a row");
    assert!(root["fallback"].as_str().unwrap().contains(&line), "{root:#}");
    let system = model.seen.lock().unwrap()[0].messages[0]["content"].as_str().unwrap().to_owned();
    assert!(system.contains(&line), "the routed loop hears it: {system}");
    supervisor.shutdown().await;
}

// ── 16. R-M6: one write log, in order, across YAML, model and handlers ──────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_are_one_log_in_the_order_they_happened() {
    let world: Arc<Mutex<Option<Arc<World>>>> = Arc::default();
    let flip = world.clone();
    let model = Turns::new(vec![turn(move |_| {
        // The tool's annotation changes mid-run: the registry still says
        // read-only, so YAML dispatches it and the handler's MCP catches it.
        flip.lock().unwrap().as_ref().unwrap().flipped.store(true, Ordering::SeqCst);
        send("Rust 2.0 is out")
    })]);
    let s = sandbox_on(model, "allow", true).await;
    *world.lock().unwrap() = Some(s.world.clone());
    assert!(LOOP.contains("      then: clear\n"), "the reply jumps to clear");
    let marking = LOOP.replace("      then: clear\n", "      then: seen\n").replace(
        "  - clear:\n",
        "  - seen:\n      call: mark_seen\n      with: { id: '${ $context.signal.id }' }\n      output: { as: '${ $task.input }' }\n  - clear:\n",
    );
    save(&s.f.engine, "telegram-loop", &marking).await;
    let report = s.try_signal(telegram(None)).await.unwrap();
    assert_eq!(report["status"], "completed", "{report:#}");
    no_refusals(&report);
    assert_eq!(
        functions(&report),
        [
            ("telegram_send_status", "yaml"),
            ("send_telegram_message", "tool"),
            ("mark_seen", "handler"),
            ("telegram_send_status", "yaml"),
        ]
    );
    let writes = report["writes"].as_array().unwrap();
    let primary = run(&report, "telegram-loop", Some("route"));
    for write in [&writes[0], &writes[1], &writes[3]] {
        assert_eq!(write["run"], primary["run"], "{write}");
        assert!(write["task"].as_str().is_some_and(|t| !t.is_empty()), "{write}");
    }
    assert!(s.world.writes().is_empty(), "{:?}", s.world.names());
}

// ── 17. R-L5: no try inside a try ───────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn try_workflow_inside_a_try_is_not_available() {
    let (s, _) = sandbox(
        vec![
            calls(&[("invoke_sub_agent", json!({"prompt":"try it","skills":["workflow-author"]}))]),
            calls(&[(
                "try_workflow",
                json!({"name":"telegram-loop","signal":{"source":"telegram","content":MESSAGE}}),
            )]),
            assistant("could not"),
            send("done"),
        ],
        "allow",
    )
    .await;
    let report = s.try_signal(telegram(None)).await.unwrap();
    assert_eq!(report["status"], "completed", "{report:#}");
    let worker = run(&report, "agent-loop", Some("worker:1.0"));
    let inner = &worker["turns"][0]["calls"][0];
    assert_eq!(inner["name"], "try_workflow");
    assert!(inner["result"].to_string().contains("not available inside a dry run"), "{inner}");
    assert_eq!(s.world.names().iter().filter(|n| *n == "list_signals").count(), 1, "only the outer try's check");
}

// ── 18. R2-M3: the engine's own starts see the drafts ───────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_drafts_start_child_starts_the_other_draft() {
    let (s, _) = sandbox(vec![], "allow").await;
    let child = |answer: &str| {
        format!(
            "document: {{ dsl: '1.0.4', namespace: agent, name: helper, version: '1.0.0' }}\ndo:\n  - answer: {{ set: {{ from: {answer} }} }}\n"
        )
    };
    activate_definition(&s.f.engine, "helper", &child("active")).await;
    let draft = child("draft");
    save(&s.f.engine, "helper", &draft).await;
    let parent = r#"document: { dsl: '1.0.4', namespace: agent, name: caller, version: '1.0.0' }
do:
  - start:
      call: start_child
      with: { name: helper, input: {} }
      export: { as: '${ $context + { child: . } }' }
  - wait:
      listen: { to: { one: { with: { type: '${ $context.child.done }' } } } }
  - result:
      call: child_outcome
      with: { id: '${ $context.child.id }', key: '${ $context.child.key }' }
"#;
    save(&s.f.engine, "caller", parent).await;
    let report = s.try_draft(json!({"name":"caller","drafts":["helper"]})).await.unwrap();
    assert_eq!(report["status"], "completed", "{report:#}");
    // `child_outcome` answers the child's status.
    assert_eq!(report["output"]["Completed"]["from"], "draft", "{report:#}");
    assert!(tried(&s.f.engine, "helper", &draft) && tried(&s.f.engine, "caller", parent));
    let report = s.try_draft(json!({"name":"caller"})).await.unwrap();
    assert_eq!(report["output"]["Completed"]["from"], "active", "without the draft listed: {report:#}");
}

// ── 19. R2-M2: a handler left on the engine's own handle is counted ─────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stray_read_of_the_engine_handle_is_refused_and_counted() {
    let (s, _) = sandbox(vec![send("fine")], "allow").await;
    let options = TryOptions { stray_read: true, ..TryOptions::default() };
    let report = s.try_with(telegram(None), options).await.unwrap();
    assert!(report["sandbox_refusals"].as_u64().is_some_and(|n| n > 0), "{report:#}");
    s.assert_nothing_kept().await;
}

// The plain entry point, what the synthetic tool calls: the default options.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn try_signal_runs_with_the_default_options() {
    let (s, _) = sandbox(vec![send("fine")], "allow").await;
    let reply = try_signal(
        &s.f.engine,
        telegram(None).as_object().unwrap(),
        &context(),
        &agent::tracing::null_scope(),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    let report: Value = serde_json::from_str(&reply).unwrap();
    assert_eq!(report["status"], "completed", "{report:#}");
}
