// Rust-only: routing, source isolation, recovery, shutdown and the poll loop
// as scenarios, beyond the one-to-one ports in agent_loop.rs,
// telegram_context.rs and supervisor_e2e.rs.
mod support;
use agent::{
    db::TraceStore,
    mcp::Mcp,
    providers::{ToolDef, assistant},
    supervisor::*,
    tracing::null_scope,
};
use async_trait::async_trait;
use serde_json::{Map, Value, json};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use support::*;
use tokio_util::sync::CancellationToken;
#[derive(Default)]
struct ContextMcp {
    calls: Mutex<Vec<(String, Map<String, Value>)>>,
    history: Mutex<Vec<Value>>,
    thread: Mutex<Vec<Value>>,
    notes: Mutex<Vec<String>>,
    queue: Mutex<VecDeque<Value>>,
    closed: AtomicUsize,
    // wait_for_signal: answers at once with nothing pending, unless `waits`
    // (blocks until `arrived` fires) or `wait_broken` (a reply of the wrong
    // shape).
    waits: std::sync::atomic::AtomicBool,
    wait_broken: std::sync::atomic::AtomicBool,
    arrived: tokio::sync::Notify,
    // Offers the status tool the reference Telegram loop calls directly. Off,
    // the built-in Telegram route (design 08) is not installed, and Telegram
    // signals take the default path these tests are about.
    status: std::sync::atomic::AtomicBool,
}
#[async_trait]
impl Mcp for ContextMcp {
    fn tools(&self) -> Vec<ToolDef> {
        [
            "get_timezone",
            "get_telegram_chat_history",
            "get_peer_thread",
            "journal_tail",
            "t1",
            "send_telegram_message",
            "edit_telegram_message",
            "telegram_send_status",
        ]
        .iter()
        .filter(|n| **n != "telegram_send_status" || self.status.load(Ordering::SeqCst))
        .map(|n| ToolDef::new(n, n, json!({"type":"object"})))
        .collect()
    }
    async fn call_tool(
        &self,
        name: &str,
        args: Map<String, Value>,
        cancel: &CancellationToken,
    ) -> anyhow::Result<String> {
        self.calls.lock().unwrap().push((name.into(), args));
        if name == "wait_for_signal" {
            if self.wait_broken.load(Ordering::SeqCst) {
                return Ok(json!({"ok":true}).to_string());
            }
            if !self.waits.load(Ordering::SeqCst) {
                // A round trip, however short: the loop yields here.
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                return Ok(json!({"pending":0}).to_string());
            }
            // Cancellable, as the real client is.
            return tokio::select! {
                _ = self.arrived.notified() => Ok(json!({"pending":1}).to_string()),
                _ = cancel.cancelled() => Err(agent::errors::aborted()),
            };
        }
        Ok(match name {
            "send_telegram_message" => json!({"delivered":true,"messageId":700}),
            "edit_telegram_message" => json!({"edited":true}),
            "get_timezone" => json!({"timezone":"Europe/Chisinau"}),
            "get_telegram_chat_history" => json!({"messages":self.history.lock().unwrap().clone()}),
            "get_peer_thread" => json!({"threadId":3,"messages":self.thread.lock().unwrap().clone()}),
            "read_doc" => json!({"ok":false,"error":"doc_not_found"}),
            "journal_tail" => {
                json!({"entries":self.notes.lock().unwrap().iter().map(|s|json!({"body":s})).collect::<Vec<_>>()})
            }
            "get_next_signal" => {
                self.queue.lock().unwrap().pop_front().unwrap_or_else(|| json!({"signal":null,"pendingAfter":0}))
            }
            _ => json!({"ok":true}),
        }
        .to_string())
    }
    async fn close(&self) {
        self.closed.fetch_add(1, Ordering::SeqCst);
    }
}
fn signal(source: &str, id: i64) -> PendingSignal {
    PendingSignal {
        id,
        source: source.into(),
        content: if source == "telegram" {
            "Telegram message in chat -10042 (forum topic thread_id=7).\nText: \"go ahead\"".into()
        } else if source == "peer" {
            PEER_MESSAGE.into()
        } else {
            "do the task".into()
        },
        env_context: Some("Default Telegram chat id: 999.".into()),
        created_at: "2026-09-06 12:00:00".into(),
        payload: None,
    }
}
const PEER_MESSAGE: &str = "Message from peer agent \"claude-code\" (via chatgpt), thread 3:\n\nAnd times 3?";
fn peer_message(id: i64, direction: &str, text: &str, at: &str) -> Value {
    json!({"id":id,"direction":direction,"actor":"chatgpt","from":null,"text":text,"created_at":at})
}
fn history(id: i64, text: &str, role: &str, chat: i64, thread: Option<i64>, at: &str) -> Value {
    json!({"id":id,"chat_id":chat,"thread_id":thread,"role":role,"text":text,"created_at":at})
}
const SKILLS: &[(&str, &str)] = &[
    ("telegram", "---\ntools: *\n---\nTELEGRAM"),
    ("scheduler", "---\ntools: *\n---\nSCHEDULER"),
    ("peer", "---\ntools: *\n---\nPEER"),
    ("orchestrator", "---\ntools: []\n---\nORCHESTRATOR"),
    ("routing", "---\ntools: []\n---\nROUTING"),
    ("recovery", "---\ntools: []\n---\nRECOVERY"),
    ("dreaming", "---\ntools: []\n---\nDREAMING"),
];
fn setup(model: Arc<dyn agent::providers::ChatProvider>) -> (Fixture, Arc<ContextMcp>, Arc<Supervisor>) {
    setup_with(model, None)
}
fn setup_with(
    model: Arc<dyn agent::providers::ChatProvider>,
    decisions: Option<Arc<agent::decisions::DecisionClient>>,
) -> (Fixture, Arc<ContextMcp>, Arc<Supervisor>) {
    let mcp = Arc::new(ContextMcp::default());
    let f = fixture_on(model, &[], SKILLS, |deps| {
        deps.mcp = mcp.clone();
        deps.skills = vec!["orchestrator".into(), "routing".into()];
        deps.decisions = decisions;
        deps.env = agent::session::EnvConfig { user_email: Some("user@test".into()), telegram_chat_id: None };
    });
    let supervisor = create_supervisor(f.engine.clone());
    (f, mcp, supervisor)
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signals_without_saved_workflows_run_the_primary_loop_with_the_source_context() {
    for source in ["scheduler", "telegram", "dreaming", "new-domain"] {
        let model = Arc::new(Scripted::new(vec![assistant("done"), assistant("done")]));
        let (f, mcp, supervisor) = setup(model.clone());
        assert_eq!(supervisor.run_signal(&signal(source, 42)).await.unwrap(), "done");
        let seen = model.seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 1, "one primary turn, with no compiler generation");
        let system = seen[0].messages[0]["content"].as_str().unwrap();
        assert!(system.contains("Europe/Chisinau"));
        assert!(system.contains("Default Telegram chat id: 999."));
        assert!(system.contains("ORCHESTRATOR"));
        assert_eq!(system.contains("SCHEDULER"), source == "scheduler");
        let presets = agent::models::default_presets();
        let preset = if source == "scheduler" { presets.base } else { presets.smart };
        assert_eq!(
            (seen[0].model.as_str(), seen[0].reasoning_effort),
            (preset.model.as_str(), preset.reasoning_effort)
        );
        assert!(!system.contains("TELEGRAM"), "communication rules are discovered, never autoloaded");
        assert!(system.contains("Call find_skills only when the task needs something specific"));
        assert!(!system.contains("Skills selected for this request"), "no decision client, nothing preselected");
        assert_eq!(system.contains("Delivery: use send_telegram_message"), source == "telegram");
        assert!(seen[0].tools.iter().any(|t| t.name == "write_skill"));
        let (trace, obs) = f.db.get_trace(&format!("{source}:42")).unwrap().unwrap();
        assert_eq!(trace.metadata.as_ref().unwrap()["activity"], "run");
        assert_eq!(trace.tags, [source, "agent-loop"]);
        assert!(obs.iter().any(|o| o.meta("activity") == Some(&json!("primary_loop"))));
        let calls = mcp.calls.lock().unwrap().clone();
        assert_eq!(calls.iter().any(|(n, _)| n == "get_telegram_chat_history"), source == "telegram");
        supervisor.shutdown().await;
    }
}
// A scheduled fire runs on the supervisor like a queued signal, through the
// schedule's drain: with the environment note one carries, and its saved
// workflow when the task names one. A supervisor that is stopping starts
// nothing: the fire stays pending (design 06, decision 3).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_scheduled_fire_runs_as_a_signal_with_mcps_note_and_its_workflow() {
    use agent::schedule::{RoundEnd, Task, drain};
    let model = Arc::new(Scripted::new(vec![assistant("done"), assistant("done")]));
    let (f, _, supervisor) = setup(model.clone());
    let env_context = agent::session::telegram_env_note(Some("4242"), &[]);
    let fires = f.engine.deps.schedule.fires.clone();
    let record = |n: i64, source: Option<&str>, workflow: Option<&str>| {
        let task = Task {
            id: n,
            cron: "now".into(),
            recurring: false,
            prompt: "Daily dreaming tick.".into(),
            source: source.map(str::to_owned),
            workflow: workflow.map(str::to_owned),
            timezone: "UTC".into(),
            created: "2026-09-06T12:00:00.000Z".into(),
            cancelled: false,
        };
        let fires = fires.clone();
        async move { fires.record(&format!("schedule:task-{n}:at:{n}"), &task, n).await.unwrap() }
    };

    let first = record(1, Some("dreaming"), None).await;
    assert_eq!(drain(&supervisor, env_context.as_deref()).await.unwrap().ran, 1);
    let system = model.seen.lock().unwrap()[0].messages[0]["content"].as_str().unwrap().to_owned();
    assert!(system.contains("Default Telegram chat id: 4242."), "the note reaches the prompt: {system}");
    assert!(f.db.get_trace(&format!("dreaming:{first}")).unwrap().is_some());

    // A task naming a workflow with no active version goes to the agent
    // loop, told so.
    let second = record(2, None, Some("w")).await;
    assert_eq!(drain(&supervisor, env_context.as_deref()).await.unwrap().ran, 1);
    let (_, obs) = f.db.get_trace(&format!("scheduler:{second}")).unwrap().unwrap();
    assert!(obs.iter().any(|o| o.name == "fallback"), "the task is handed over");
    let system = model.seen.lock().unwrap()[1].messages[0]["content"].as_str().unwrap().to_owned();
    assert!(system.contains("saved workflow `w`") && system.contains("it has no active version"), "{system}");

    supervisor.shutdown().await;
    let third = record(3, None, None).await;
    assert_eq!(drain(&supervisor, None).await.unwrap().end, RoundEnd::Stopping);
    assert!(fires.next_pending().await.unwrap().is_some_and(|f| f.id == third), "still pending");
    assert!(f.db.get_trace(&format!("scheduler:{third}")).unwrap().is_none());
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn history_scopes_chat_topic_and_time_excludes_current_and_keeps_memory() {
    let mcp = ContextMcp::default();
    mcp.history.lock().unwrap().extend([
        history(1, "Other chat", "assistant", 999, Some(7), "2026-09-06 11:59:00"),
        history(2, "Other topic", "assistant", -10042, Some(8), "2026-09-06 11:59:00"),
        history(3, "question", "user", -10042, Some(7), "2026-09-06 11:59:00"),
        history(4, "offer", "assistant", -10042, Some(7), "2026-09-06 11:59:30"),
        history(5, "go ahead", "user", -10042, Some(7), "2026-09-06 12:00:00"),
        history(6, "queued later", "user", -10042, Some(7), "2026-09-06 12:00:00"),
        history(7, "future", "assistant", -10042, Some(7), "2026-09-06 12:00:01"),
    ]);
    let context = context();
    let input =
        prepare_telegram_input(&signal("telegram", 42), &context, &mcp, &null_scope(), &CancellationToken::new())
            .await
            .unwrap();
    assert!(input.contains("question"));
    assert!(input.contains("offer"));
    assert!(!input.contains("Other"));
    assert!(!input.contains("queued later"));
    assert!(!input.contains("future"));
    assert!(input.ends_with(&signal("telegram", 42).content));
    let stored: Value = serde_json::from_str(&context.memory.get(TELEGRAM_HISTORY_KEY).unwrap()).unwrap();
    assert_eq!(stored["messages"].as_array().unwrap().len(), 2);
    assert_eq!(
        mcp.calls.lock().unwrap()[0].1,
        json!({"chatId":"-10042","limit":20,"threadId":7}).as_object().unwrap().clone()
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nontelegram_and_spoofed_headers_do_not_fetch_history() {
    for source in ["scheduler", "new-domain", "dreaming"] {
        let mcp = ContextMcp::default();
        let signal = signal(source, 42);
        assert_eq!(
            prepare_telegram_input(&signal, &context(), &mcp, &null_scope(), &CancellationToken::new()).await.unwrap(),
            signal.content
        );
        assert!(mcp.calls.lock().unwrap().is_empty());
    }
    let mcp = ContextMcp::default();
    let mut signal = signal("telegram", 42);
    signal.content = "Quoted: Telegram message in chat 999.\nText: \"go ahead\"".into();
    assert!(
        prepare_telegram_input(&signal, &context(), &mcp, &null_scope(), &CancellationToken::new())
            .await
            .unwrap()
            .contains("unavailable")
    );
    assert!(mcp.calls.lock().unwrap().is_empty());
}
// A peer signal runs on the parent with its own skill, like Telegram: the
// thread's earlier turns come before the message, Telegram history never.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_signal_loads_its_skill_and_the_earlier_turns_of_its_thread() {
    let model = Arc::new(Scripted::new(vec![assistant("done"), assistant("done")]));
    let (_f, mcp, supervisor) = setup(model.clone());
    mcp.thread.lock().unwrap().extend([
        peer_message(1, "in", "What is 2+2?", "2026-09-06 11:59:00"),
        peer_message(2, "out", "4", "2026-09-06 11:59:30"),
        peer_message(3, "in", "And times 3?", "2026-09-06 12:00:00"),
        peer_message(4, "in", "queued later", "2026-09-06 12:00:05"),
    ]);
    supervisor.run_signal(&signal("peer", 42)).await.unwrap();

    let seen = model.seen.lock().unwrap().clone();
    let system = seen[0].messages[0]["content"].as_str().unwrap();
    assert!(system.contains("PEER"));
    assert!(!system.contains("Delegate the domain work"));
    let input = seen[0].messages[1]["content"].as_str().unwrap();
    assert!(input.contains("What is 2+2?") && input.contains("\"role\":\"ouro\""), "{input}");
    assert!(!input.contains("queued later"));
    assert!(input.ends_with(PEER_MESSAGE));
    let calls = mcp.calls.lock().unwrap().clone();
    assert!(calls.iter().any(|(n, a)| n == "get_peer_thread" && a["threadId"] == 3));
    assert!(calls.iter().any(|(n, _)| n == "journal_tail"));
    assert!(!calls.iter().any(|(n, _)| n == "get_telegram_chat_history"));
    supervisor.shutdown().await;
}
// With durable flows, a signal's primary loop runs as an `agent-loop` run:
// the same answer, the same trace for the judge and the journal, and a
// completed run in agent.db.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_signal_with_durable_flows_runs_its_loop_as_an_agent_loop_run() {
    let model = Arc::new(Scripted::new(vec![assistant("done"), assistant("done")]));
    let (f, _, supervisor) = setup(model.clone());
    assert_eq!(supervisor.run_signal(&signal("peer", 42)).await.unwrap(), "done");
    // The primary loop is a host child of the signal's run.
    let children = f
        .engine
        .control
        .list(&agent::runs::Runs { parent: Some("signal:42".into()), ..Default::default() }, None, 10)
        .await
        .unwrap()
        .items;
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].admission.name, "agent-loop");
    let primary = f.engine.runtime.get(&children[0].admission.id).await.unwrap().unwrap();
    assert_eq!(primary.admission.host().unwrap()["kind"], "primary");
    assert_eq!(children[0].outcome, Some(agent::runs::Status::Completed(json!("done"))));
    let (_, obs) = f.db.get_trace("peer:42").unwrap().unwrap();
    let primary =
        obs.iter().find(|o| o.meta("activity") == Some(&json!("primary_loop"))).expect("the primary loop span");
    let turn = obs.iter().find(|o| o.name == "iter-0").expect("the first generation");
    assert_eq!(turn.parent_observation_id.as_deref(), Some(primary.id.as_str()));
    supervisor.shutdown().await;
}
// Jev for preselection: the telegram skill fits, nothing else does; `broken`
// answers without scores.
struct Jev {
    broken: bool,
    requests: Mutex<Vec<Value>>,
}
#[async_trait]
impl agent::providers::JsonApi for Jev {
    async fn post(&self, _: &str, body: Value, _: &CancellationToken) -> anyhow::Result<Value> {
        self.requests.lock().unwrap().push(body.clone());
        let answers: Map<String, Value> = if self.broken {
            Map::new()
        } else {
            body["state"]["skills"]
                .as_array()
                .unwrap()
                .iter()
                .enumerate()
                .map(|(n, s)| {
                    let p = if s["name"] == "dreaming" { 0.97 } else { 0.05 };
                    (format!("skill_{n}"), json!({"type":"noul","noul":p}))
                })
                .collect()
        };
        Ok(json!({"model":"jev-test","answers":answers}))
    }
}
fn jev(broken: bool) -> (Arc<Jev>, Arc<agent::decisions::DecisionClient>) {
    let api = Arc::new(Jev { broken, requests: Mutex::new(Vec::new()) });
    (api.clone(), agent::decisions::decision_client("jev-test".into(), api))
}
// The skills a message needs are chosen before the first turn and inlined,
// so the model starts with the channel's rules instead of searching for them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_signal_starts_with_the_skills_jev_selects_for_it() {
    let model = Arc::new(Scripted::new(vec![assistant("done"), assistant("done")]));
    let (api, client) = jev(false);
    let (f, _mcp, supervisor) = setup_with(model.clone(), Some(client));
    supervisor.run_signal(&signal("telegram", 42)).await.unwrap();
    let seen = model.seen.lock().unwrap().clone();
    let system = seen[0].messages[0]["content"].as_str().unwrap();
    let selected = system.split("## Skills selected for this request").nth(1).expect("a preselected block");
    assert!(selected.contains("### Skill `dreaming`") && selected.contains("DREAMING"), "{system}");
    assert!(!selected.contains("### Skill `orchestrator`"), "loaded skills are never inlined twice");
    assert!(!selected.contains("SCHEDULER"));
    let request = api.requests.lock().unwrap()[0].clone();
    assert!(request["state"]["current_request"].as_str().unwrap().contains("go ahead"));
    assert!(request["state"]["context"].as_str().unwrap().starts_with("Signal source: telegram"));
    let (trace, obs) = f.db.get_trace("telegram:42").unwrap().unwrap();
    // The engine's skills, then what preselection inlined (the default
    // path; a routed loop's preselection:
    // a_telegram_signal_runs_the_loop_that_declares_its_route).
    assert_eq!(trace.metadata.as_ref().unwrap()["skills"], json!(["orchestrator", "routing", "dreaming"]));
    let search = obs.iter().find(|o| o.name == "find_skills").expect("an automatic find_skills span");
    assert_eq!(search.meta("automatic"), Some(&json!(true)));
    supervisor.shutdown().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_preselection_leaves_the_search_to_the_model() {
    let model = Arc::new(Scripted::new(vec![assistant("done"), assistant("done")]));
    let (_api, client) = jev(true);
    let (f, _mcp, supervisor) = setup_with(model.clone(), Some(client));
    assert_eq!(supervisor.run_signal(&signal("telegram", 42)).await.unwrap(), "done");
    let seen = model.seen.lock().unwrap().clone();
    let system = seen[0].messages[0]["content"].as_str().unwrap();
    assert!(!system.contains("Skills selected for this request"));
    assert!(seen[0].tools.iter().any(|t| t.name == "find_skills"));
    let (_, obs) = f.db.get_trace("telegram:42").unwrap().unwrap();
    assert!(obs.iter().any(|o| o.name == "find_skills" && o.level == "ERROR"));
    supervisor.shutdown().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_peer_thread_is_the_message_alone() {
    let mcp = ContextMcp::default();
    mcp.thread.lock().unwrap().push(peer_message(1, "in", "And times 3?", "2026-09-06 12:00:00"));
    let signal = signal("peer", 42);
    let input = prepare_peer_input(&signal, &mcp, &null_scope(), &CancellationToken::new()).await.unwrap();
    assert_eq!(input, signal.content);
}
#[test]
fn oversized_history_prefers_recent_whole_messages_and_utf8_safe_tail() {
    let messages = vec![
        history(1, &"OLD".repeat(20000), "assistant", 42, None, "a"),
        history(2, "recent", "assistant", 42, None, "b"),
    ];
    let (inline, truncated) = inline_history(&messages);
    assert!(truncated);
    assert_eq!(inline.len(), 1);
    assert_eq!(inline[0]["text"], "recent");
    let long = "📰 Headline \"quoted\" — naïve café\n".repeat(2000) + "Collect for the week?";
    let (inline, truncated) = inline_history(&[history(1, &long, "assistant", 42, None, "a")]);
    assert!(truncated);
    assert!(json!(inline).to_string().len() <= 8000);
    assert!(inline[0]["text"].as_str().unwrap().ends_with("Collect for the week?"));
    assert_eq!(inline[0]["truncated"], true);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recent_activity_is_bounded_oldest_first_and_stays_out_of_user_history() {
    let model = Arc::new(Scripted::new(vec![assistant("done"), assistant("done")]));
    let (f, mcp, supervisor) = setup(model.clone());
    mcp.notes.lock().unwrap().extend(["newest".into(), "older".into(), "huge".repeat(4000)]);
    supervisor.run_signal(&signal("telegram", 42)).await.unwrap();
    let seen = model.seen.lock().unwrap().clone();
    let system = seen[0].messages[0]["content"].as_str().unwrap();
    assert!(system.contains("ouro/activity"));
    assert!(system.contains("older\n\nnewest"));
    assert!(!system.contains("hugehuge"));
    assert!(!seen[0].messages[1]["content"].as_str().unwrap().contains("newest"));
    let (_, obs) = f.db.get_trace("telegram:42").unwrap().unwrap();
    assert!(obs.iter().find(|o| o.name == "journal_tail").unwrap().meta("automatic") == Some(&json!(true)));
    supervisor.shutdown().await;
}
struct PendingProvider {
    started: tokio::sync::Notify,
    cancel: Mutex<Option<CancellationToken>>,
}
#[async_trait]
impl agent::providers::ChatProvider for PendingProvider {
    fn kind(&self) -> agent::providers::ProviderKind {
        agent::providers::ProviderKind::Openai
    }
    async fn complete(
        &self,
        p: &mut agent::providers::CompletionParams<'_>,
    ) -> anyhow::Result<agent::providers::CompletionResult> {
        *self.cancel.lock().unwrap() = Some(p.cancel.clone());
        self.started.notify_one();
        std::future::pending().await
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_cancels_generation_without_recovery_and_closes_trace_before_clients() {
    let model = Arc::new(PendingProvider { started: tokio::sync::Notify::new(), cancel: Mutex::new(None) });
    let (f, mcp, supervisor) = setup(model.clone());
    let s = supervisor.clone();
    let running = tokio::spawn(async move { s.run_signal(&signal("dreaming", 42)).await });
    model.started.notified().await;
    supervisor.shutdown().await;
    assert!(running.await.unwrap().is_err());
    assert!(model.cancel.lock().unwrap().as_ref().unwrap().is_cancelled());
    assert_eq!(mcp.closed.load(Ordering::SeqCst), 1);
    let (_, obs) = f.db.get_trace("dreaming:42").unwrap().unwrap();
    assert!(!obs.iter().any(|o| o.name == "recovery"));
    assert!(obs.iter().any(|o| o.name == "iter-0" && o.level == "ERROR"));
    assert!(supervisor.run_signal(&signal("telegram", 43)).await.is_err());
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_polling_handles_signals_serially_and_survives_bad_polls() {
    let model = Arc::new(Scripted::new(vec![assistant("first"), assistant("second")]));
    let (f, mcp, supervisor) = setup(model.clone());
    mcp.queue.lock().unwrap().extend([
        json!({"wrong":"shape"}),
        json!({"signal":signal("dreaming",1),"pendingAfter":1}),
        json!({"signal":signal("new-domain",2),"pendingAfter":0}),
    ]);
    let stop = CancellationToken::new();
    let s = stop.clone();
    let c = mcp.clone();
    let sup = supervisor.clone();
    let task = tokio::spawn(async move { run_poll_loop(c, sup, s, std::time::Duration::from_millis(1)).await });
    // Each signal is a few engine runs on the worker's ticks.
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let n = mcp.calls.lock().unwrap().iter().filter(|(n, _)| n == "get_next_signal").count();
            if n >= 4 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    stop.cancel();
    task.await.unwrap().unwrap();

    assert_eq!(model.seen.lock().unwrap().len(), 2);
    assert!(f.db.get_trace("dreaming:1").unwrap().is_some());
    assert!(f.db.get_trace("new-domain:2").unwrap().is_some());
    supervisor.shutdown().await;
}

#[test]
fn queue_contract_requires_nullable_fields_and_integer_counts() {
    let empty = json!({"signal":null,"pendingAfter":0});
    assert!(serde_json::from_value::<NextSignal>(empty).is_ok());
    let valid = json!({"signal":signal("telegram",42),"pendingAfter":0});
    assert!(serde_json::from_value::<NextSignal>(valid.clone()).is_ok());
    let mut missing_env = valid.clone();
    missing_env["signal"].as_object_mut().unwrap().remove("envContext");
    let mut fractional_id = valid;
    fractional_id["signal"]["id"] = json!(1.5);
    for bad in [
        json!({"pendingAfter":0}),
        json!({"signal":null}),
        json!({"signal":null,"pendingAfter":0.5}),
        missing_env,
        fractional_id,
    ] {
        assert!(serde_json::from_value::<NextSignal>(bad.clone()).is_err(), "{bad}");
    }
}

// Rust-only: with an empty queue the loop waits on wait_for_signal and takes
// the signal the moment the wait ends — no poll interval in between (it is
// an hour here, so a sleep would show).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_empty_queue_is_waited_on_and_a_new_signal_taken_at_once() {
    let model = Arc::new(Scripted::new(vec![assistant("handled")]));
    let (f, mcp, supervisor) = setup(model.clone());
    mcp.waits.store(true, Ordering::SeqCst);
    let stop = CancellationToken::new();
    let (c, sup, s) = (mcp.clone(), supervisor.clone(), stop.clone());
    let task = tokio::spawn(async move { run_poll_loop(c, sup, s, std::time::Duration::from_secs(3600)).await });
    let waiting = || mcp.calls.lock().unwrap().iter().filter(|(n, _)| n == "wait_for_signal").count();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while waiting() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let asked = mcp.calls.lock().unwrap().iter().find(|(n, _)| n == "wait_for_signal").unwrap().1.clone();
    assert_eq!(asked["timeoutMs"], json!(25_000), "one wait round is 25 s");
    let polls = || mcp.calls.lock().unwrap().iter().filter(|(n, _)| n == "get_next_signal").count();
    let before = polls();
    mcp.queue.lock().unwrap().push_back(json!({"signal":signal("dreaming",7),"pendingAfter":0}));
    mcp.arrived.notify_waiters();
    // What this pins is the call, not the engine's speed: the loop asks for
    // the signal as the wait ends. The poll interval is an hour, so any
    // timeout far below it still tells a sleep from a prompt call.
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        while polls() == before {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("get_next_signal is called right after the wait, not an interval later");
    // The run itself takes the engine's starts (seconds under load): no tight bound.
    tokio::time::timeout(std::time::Duration::from_secs(300), async {
        while f.db.get_trace("dreaming:7").unwrap().is_none() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the signal is handled");
    stop.cancel();
    mcp.arrived.notify_waiters();
    tokio::time::timeout(std::time::Duration::from_secs(5), task).await.unwrap().unwrap().unwrap();
    assert_eq!(model.seen.lock().unwrap().len(), 1);
    supervisor.shutdown().await;
}

// Rust-only: a wait that fails (MCP down, a reply of the wrong shape) is an
// error like any other — the loop backs off instead of spinning on
// get_next_signal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_wait_backs_off_instead_of_spinning() {
    let model = Arc::new(Scripted::new(vec![]));
    let (_f, mcp, supervisor) = setup(model);
    mcp.wait_broken.store(true, Ordering::SeqCst);
    let stop = CancellationToken::new();
    let (c, sup, s) = (mcp.clone(), supervisor.clone(), stop.clone());
    let task = tokio::spawn(async move { run_poll_loop(c, sup, s, std::time::Duration::from_secs(3600)).await });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let polls = mcp.calls.lock().unwrap().iter().filter(|(n, _)| n == "get_next_signal").count();
    assert_eq!(polls, 1, "one round, then the backoff");
    stop.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(2), task).await.unwrap().unwrap().unwrap();
    supervisor.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_failure_keeps_the_primary_error_and_transcript() {
    for invalid_preset in [false, true] {
        let model = Arc::new(Scripted::new(vec![calls(&[("t1", json!({"marker":"primary transcript marker"}))])]));
        let (f, _, supervisor) = setup(model.clone());
        if invalid_preset {
            // An operator's (or an older rule's) signal version, past the gate.
            store_definition(&f.engine, "signal", &SIGNAL.replace("preset: base", "preset: unknown")).await;
        }
        // After one tool round, both the primary and the recovery provider
        // fail. An invalid recovery preset also exercises setup failure.
        let error = supervisor.run_signal(&signal("peer", 42)).await.unwrap_err().to_string();
        assert!(error.contains("script exhausted"), "{error}");
        let seen = model.seen.lock().unwrap().clone();
        assert_eq!(seen.len(), if invalid_preset { 2 } else { 3 });
        if !invalid_preset {
            let prompt = seen.last().unwrap().messages[1]["content"].as_str().unwrap();
            assert!(prompt.contains("primary transcript marker"), "{prompt}");
            assert!(prompt.contains("script exhausted"));
        }
        let (trace, obs) = f.db.get_trace("peer:42").unwrap().unwrap();
        assert_eq!(trace.output["error"], error);
        let recovery = obs.iter().find(|o| o.name == "recovery").unwrap();
        assert_eq!(recovery.level, "ERROR");
        if invalid_preset {
            assert!(recovery.status_message.as_deref().unwrap().contains("unknown preset unknown"));
        }
        supervisor.shutdown().await;
    }
}

// ── confirmations through the supervisor (design 03, decision 1) ────────────
// The signal's tree waits for a press while the supervisor takes the next
// signal; the press arrives as a signal of its own and the tree goes on in
// the engine's worker. Telegram runs the default path in step 2 (no loop
// variant is active, design 03 decision 9).

// The engine's time: the system clock plus an offset a test moves forward.
#[derive(Default)]
struct Clock(std::sync::atomic::AtomicI64);

impl ouro_runtime::durable::Clock for Clock {
    fn now_ms(&self) -> i64 {
        chrono::Utc::now().timestamp_millis() + self.0.load(Ordering::SeqCst)
    }
}

fn confirming(mcp: &Arc<ContextMcp>, label: &str) -> impl FnOnce(&mut agent::agent::EngineDeps) {
    let (mcp, api) = (mcp.clone(), Arc::new(GuardApi::default()));
    *api.label.lock().unwrap() = Some(label.into());
    move |deps| {
        deps.mcp = mcp;
        deps.skills = vec!["orchestrator".into(), "routing".into(), "telegram".into()];
        deps.decisions = Some(agent::decisions::decision_client("guard-test".into(), api));
        deps.env = agent::session::EnvConfig { user_email: None, telegram_chat_id: Some("999".into()) };
    }
}

fn confirmation_fixture(model: Arc<Scripted>, mcp: &Arc<ContextMcp>, clock: Arc<Clock>) -> Fixture {
    fixture_clocked(model, &[], SKILLS, clock, confirming(mcp, "needs_confirmation"))
}

async fn until<T>(what: &str, mut probe: impl AsyncFnMut() -> Option<T>) -> T {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if let Some(found) = probe().await {
            return found;
        }
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

async fn ended(engine: &agent::agent::Engine, run: &str) -> agent::runs::Status {
    until(run, async || engine.runtime.get(run).await.unwrap().and_then(|r| r.outcome)).await
}

// The signal's primary loop run (its host child), with its host data (the
// control API's listing redacts it).
async fn primary_of(engine: &agent::agent::Engine, signal: i64) -> ouro_runtime::durable::SavedRun {
    children_of(engine, &format!("signal:{signal}")).await.into_iter().next().expect("the primary loop")
}

// The children of `run` that ended, with their host data.
async fn ended_children_of(engine: &agent::agent::Engine, run: &str) -> Vec<ouro_runtime::durable::SavedRun> {
    let filter = agent::runs::Runs {
        parent: Some(run.into()),
        since_ms: Some(0),
        before_ms: Some(i64::MAX / 4),
        ..Default::default()
    };
    let mut children = Vec::new();
    for child in engine.control.history(&filter, None, 10).await.unwrap().items {
        children.push(engine.runtime.get(&child.admission.id).await.unwrap().unwrap());
    }
    children
}

async fn children_of(engine: &agent::agent::Engine, run: &str) -> Vec<ouro_runtime::durable::SavedRun> {
    let filter = agent::runs::Runs { parent: Some(run.into()), ..Default::default() };
    let mut children = Vec::new();
    for child in engine.control.list(&filter, None, 10).await.unwrap().items {
        children.push(engine.runtime.get(&child.admission.id).await.unwrap().unwrap());
    }
    children
}

fn card_of(mcp: &ContextMcp) -> Map<String, Value> {
    mcp.calls
        .lock()
        .unwrap()
        .iter()
        .find(|(n, a)| n == "send_telegram_message" && a.contains_key("buttons"))
        .expect("a confirmation card")
        .1
        .clone()
}

fn cards_sent(mcp: &ContextMcp) -> usize {
    mcp.calls.lock().unwrap().iter().filter(|(n, a)| n == "send_telegram_message" && a.contains_key("buttons")).count()
}

fn held(mcp: &ContextMcp) -> Vec<Map<String, Value>> {
    mcp.calls.lock().unwrap().iter().filter(|(n, _)| n == "t1").map(|(_, a)| a.clone()).collect()
}

fn press_of(id: i64, card: &Map<String, Value>, index: usize) -> PendingSignal {
    let mut press = signal("telegram_button", id);
    press.content = json!({"chatId":-10042,"messageThreadId":7,"messageId":700,
        "data":card["buttons"][0][index]["data"],"text":card["buttons"][0][index]["text"]})
    .to_string();
    press
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn confirmation_holds_exact_call_frees_supervisor_and_resumes_after_press_or_expiry() {
    for mode in ["confirmed", "declined", "expired", "restart"] {
        let args = json!({"recipient":"original@example.test","body":"exact original arguments"});
        let model = Arc::new(Scripted::new(vec![
            calls(&[("t1", args.clone())]),
            assistant("other signal answered"),
            calls(&[(
                "send_telegram_message",
                json!({"chatId":"-10042","messageThreadId":7,"text":"confirmation handled"}),
            )]),
            assistant("confirmation handled"),
        ]));
        let mcp = Arc::new(ContextMcp::default());
        let clock = Arc::new(Clock::default());
        let f = confirmation_fixture(model.clone(), &mcp, clock.clone());
        let mut supervisor = create_supervisor(f.engine.clone());
        f.db.activity_cutoff(chrono::Utc::now() - chrono::TimeDelta::minutes(1)).unwrap();
        let result = supervisor.run_signal(&signal("telegram", 42)).await.unwrap();
        assert!(result.contains("resumes"), "{mode}: {result}");
        assert!(!f.db.due_activity(20, chrono::Utc::now()).unwrap().iter().any(|t| t.trace_id == "telegram:42"));
        let primary = primary_of(&f.engine, 42).await;
        assert_eq!(primary.state, ouro_runtime::durable::RunState::Waiting, "{mode}: {:?}", primary.outcome);
        assert!(held(&mcp).is_empty());
        let card = card_of(&mcp);
        assert_eq!(card["chatId"], "-10042");
        assert_eq!(card["messageThreadId"], 7);
        assert!(card["text"].as_str().unwrap().contains("original@example.test"));
        assert!(card["text"].as_str().unwrap().contains("go ahead"));
        for b in card["buttons"][0].as_array().unwrap() {
            assert!((19..=64).contains(&b["data"].as_str().unwrap().len()));
        }
        assert_eq!(supervisor.run_signal(&signal("peer", 43)).await.unwrap(), "other signal answered");

        // A new engine on the same files: no live loop, verdict or working
        // memory of the first process.
        let engine = if mode == "restart" {
            supervisor.shutdown().await;
            let next = reopen(&f, model.clone(), clock.clone(), confirming(&mcp, "allow"));
            supervisor = create_supervisor(next.clone());
            next.ready().await.unwrap();
            next
        } else {
            f.engine.clone()
        };
        if mode == "expired" {
            f.db.conn().execute("UPDATE confirmations SET created_at_ms = created_at_ms - 86400001", []).unwrap();
            clock.0.fetch_add(86_400_000 + 60_000, Ordering::SeqCst);
        } else {
            let index = usize::from(mode == "declined");
            assert_eq!(supervisor.run_signal(&press_of(44, &card, index)).await.unwrap(), "Button delivered");
            assert_eq!(model.seen.lock().unwrap().len(), 2, "a delivery takes no model turn");
        }
        assert_eq!(
            ended(&engine, "signal:42").await,
            agent::runs::Status::Completed(json!("confirmation handled")),
            "{mode}"
        );
        let held = held(&mcp);
        assert_eq!(held.len(), usize::from(mode == "confirmed" || mode == "restart"), "{mode}: {held:?}");
        if let Some(call) = held.first() {
            assert_eq!(json!(call), args);
        }
        let (trace, observations) = until("the trace's final output", async || {
            let (trace, observations) = f.db.get_trace("telegram:42").unwrap()?;
            (trace.output == json!("confirmation handled")).then_some((trace, observations))
        })
        .await;
        assert_eq!(trace.name, "signal:telegram");
        assert_eq!(trace.session_id.as_deref(), Some("telegram:42"));
        assert!(trace.input.to_string().contains("Telegram message"), "{}", trace.input);
        assert!(observations.iter().any(|o| o.name == "guard"));
        assert!(
            observations.iter().any(|o| o.name == "send_telegram_message" && o.input["text"] == "confirmation handled")
        );
        until("the run's end recorded", async || {
            f.db.due_activity(20, chrono::Utc::now()).unwrap().iter().any(|t| t.trace_id == "telegram:42").then_some(())
        })
        .await;
        assert_eq!(cards_sent(&mcp), 1, "{mode}: the card is not sent twice");
        let edit = until("the card's edit", async || {
            mcp.calls.lock().unwrap().iter().find(|(n, _)| n == "edit_telegram_message").map(|(_, a)| a.clone())
        })
        .await;
        let defaults = agent::confirmation::Card::default();
        let label = defaults.outcome(match mode {
            "restart" | "confirmed" => "confirmed",
            "declined" => "declined",
            _ => "expired",
        });
        assert!(edit["text"].as_str().unwrap().ends_with(label), "{mode}: {edit:?}");
        assert!(edit.get("buttons").is_none());
        if mode == "declined" || mode == "expired" {
            let transcript = serde_json::to_string(&model.seen.lock().unwrap().last().unwrap().messages).unwrap();
            assert!(transcript.contains(if mode == "declined" {
                "declined by the user"
            } else {
                "not confirmed within 24 h"
            }));
        }
        supervisor.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_next_session_knows_pending_cards_in_its_conversation_only() {
    let model = Arc::new(Scripted::new(vec![
        calls(&[("t1", json!({"operation":"pending action"}))]),
        assistant("same topic"),
        assistant("other topic"),
    ]));
    let mcp = Arc::new(ContextMcp::default());
    let f = confirmation_fixture(model.clone(), &mcp, Arc::new(Clock::default()));
    let supervisor = create_supervisor(f.engine.clone());
    supervisor.run_signal(&signal("telegram", 42)).await.unwrap();
    assert_eq!(supervisor.run_signal(&signal("telegram", 43)).await.unwrap(), "same topic");
    let mut other = signal("telegram", 44);
    other.content = other.content.replace("thread_id=7", "thread_id=8");
    assert_eq!(supervisor.run_signal(&other).await.unwrap(), "other topic");
    let seen = model.seen.lock().unwrap().clone();
    let same = seen[1].messages[0]["content"].as_str().unwrap();
    assert!(
        same.contains("Pending confirmations")
            && same.contains("pending action")
            && same.contains("-10042")
            && same.contains("since"),
        "{same}"
    );
    assert!(!seen[2].messages[0]["content"].as_str().unwrap().contains("Pending confirmations"));
    assert_eq!(cards_sent(&mcp), 1);
    assert!(held(&mcp).is_empty());
    supervisor.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_confirmation_sleeps_its_parent_and_survives_restart() {
    for restart in [false, true] {
        let args = json!({"recipient":"worker@example.test","body":"held exactly once"});
        let model = Arc::new(Scripted::new(vec![
            calls(&[("invoke_sub_agent", json!({"prompt":"send the proposed message","skills":["telegram"]}))]),
            calls(&[("t1", args.clone())]),
            assistant("next signal answered"),
            assistant("worker answer"),
            assistant("parent answer"),
        ]));
        let mcp = Arc::new(ContextMcp::default());
        let clock = Arc::new(Clock::default());
        let f = confirmation_fixture(model.clone(), &mcp, clock.clone());
        let mut supervisor = create_supervisor(f.engine.clone());
        let result = supervisor.run_signal(&signal("telegram", 42)).await.unwrap();
        assert!(result.contains("resumes"), "{result}");
        let primary = primary_of(&f.engine, 42).await;
        let worker = children_of(&f.engine, &primary.admission.id)
            .await
            .into_iter()
            .find(|r| r.admission.name != "guard")
            .expect("the worker");
        let guard = children_of(&f.engine, &worker.admission.id)
            .await
            .into_iter()
            .find(|r| r.admission.name == "guard")
            .expect("the worker's guard");
        for run in [&primary, &worker, &guard] {
            let state = f.engine.runtime.get(&run.admission.id).await.unwrap().unwrap().state;
            assert_eq!(state, ouro_runtime::durable::RunState::Waiting, "{}", run.admission.id);
        }
        // The worker confirms where its parent does (invariant 5).
        assert_eq!(worker.admission.host().unwrap()["confirm"], json!({"chatId":"-10042","messageThreadId":7}));
        assert_eq!(supervisor.run_signal(&signal("peer", 43)).await.unwrap(), "next signal answered");
        assert!(held(&mcp).is_empty());
        let card = card_of(&mcp);
        let engine = if restart {
            supervisor.shutdown().await;
            let next = reopen(&f, model.clone(), clock.clone(), confirming(&mcp, "needs_confirmation"));
            supervisor = create_supervisor(next.clone());
            next.ready().await.unwrap();
            next
        } else {
            f.engine.clone()
        };
        assert_eq!(supervisor.run_signal(&press_of(44, &card, 0)).await.unwrap(), "Button delivered");
        assert_eq!(ended(&engine, "signal:42").await, agent::runs::Status::Completed(json!("parent answer")));
        assert_eq!(ended(&engine, &worker.admission.id).await, agent::runs::Status::Completed(json!("worker answer")));
        let held = held(&mcp);
        assert_eq!(held.len(), 1);
        assert_eq!(json!(held[0]), args);
        assert_eq!(cards_sent(&mcp), 1);
        let (trace, observations) = until("the trace's final output", async || {
            let (trace, observations) = f.db.get_trace("telegram:42").unwrap()?;
            (trace.output == json!("parent answer")).then_some((trace, observations))
        })
        .await;
        assert_eq!(trace.output, "parent answer");
        assert!(observations.iter().any(|o| o.name == "invoke_sub_agent"));
        supervisor.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn confirmation_during_the_hold_finishes_without_suspending() {
    let model = Arc::new(Scripted::new(vec![calls(&[("t1", json!({"body":"held"}))]), assistant("done")]));
    let mcp = Arc::new(ContextMcp::default());
    let f = confirmation_fixture(model, &mcp, Arc::new(Clock::default()));
    let supervisor = create_supervisor(f.engine.clone());
    let incoming = signal("telegram", 42);
    let running = {
        let supervisor = supervisor.clone();
        tokio::spawn(async move { supervisor.run_signal(&incoming).await })
    };
    // The card's press arrives during the guard's hold, straight to it.
    let (token, guard): (String, String) = until("the card", async || {
        f.db.conn()
            .query_row("SELECT yes_type, run_id FROM confirmations WHERE message_id IS NOT NULL", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .ok()
    })
    .await;
    let event = json!({"source":"telegram","id":"telegram_button:during-hold","type":token,
        "data":{"chatId":-10042,"messageThreadId":7,"messageId":700,"data":token}});
    assert_eq!(f.engine.runtime.deliver(event, Some(&guard)).await.unwrap().as_deref(), Some(guard.as_str()));
    assert_eq!(running.await.unwrap().unwrap(), "done", "the signal ended without suspending");
    assert_eq!(held(&mcp).len(), 1);
    let (trace, observations) = f.db.get_trace("telegram:42").unwrap().unwrap();
    assert_eq!(trace.output, "done");
    assert!(!observations.iter().any(|o| o.name.starts_with("resume:")), "no execution was resumed");
    supervisor.shutdown().await;
}

// A press is delivered to the guard round whose card holds its token, with
// no model turn; a press of a card already answered goes to the agent with
// the Telegram context and the reason; a peer's confirmations go to the
// default chat.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn button_routing_delivers_without_a_model_and_falls_back_with_telegram_context() {
    let model = Arc::new(Scripted::new(vec![
        calls(&[("t1", json!({"body":"held"}))]),
        assistant("done"),
        assistant("stale button understood"),
        assistant("peer done"),
    ]));
    let mcp = Arc::new(ContextMcp::default());
    let f = confirmation_fixture(model.clone(), &mcp, Arc::new(Clock::default()));
    let supervisor = create_supervisor(f.engine.clone());
    assert!(supervisor.run_signal(&signal("telegram", 9)).await.unwrap().contains("resumes"));
    let card = card_of(&mcp);
    assert_eq!(supervisor.run_signal(&press_of(10, &card, 0)).await.unwrap(), "Button delivered");
    assert_eq!(model.seen.lock().unwrap().len(), 1, "a delivery takes no model turn");
    assert_eq!(ended(&f.engine, "signal:9").await, agent::runs::Status::Completed(json!("done")));
    assert_eq!(supervisor.run_signal(&press_of(11, &card, 0)).await.unwrap(), "stale button understood");
    let seen = model.seen.lock().unwrap().clone();
    let stale = seen.last().unwrap();
    let content = stale.messages.last().unwrap()["content"].as_str().unwrap();
    assert!(content.contains("pressed the button") && content.contains("700"), "{content}");
    let system = stale.messages[0]["content"].as_str().unwrap();
    assert!(system.contains("Delivery:") && system.contains("already answered"), "{system}");
    let primary = primary_of(&f.engine, 11).await;
    assert_eq!(primary.admission.host().unwrap()["confirm"], json!({"chatId":"-10042","messageThreadId":7}));
    assert!(mcp.calls.lock().unwrap().iter().any(|(n, a)| n == "get_telegram_chat_history" && a["threadId"] == 7));
    assert_eq!(supervisor.run_signal(&signal("peer", 12)).await.unwrap(), "peer done");
    let primary = primary_of(&f.engine, 12).await;
    assert_eq!(primary.admission.host().unwrap()["confirm"], json!({"chatId":"999","messageThreadId":null}));
    supervisor.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn telegram_without_an_active_route_keeps_the_default_first_turn_and_loop() {
    for source in ["telegram", "telegram_button"] {
        let model = Arc::new(Scripted::new(vec![assistant("default answer")]));
        let mcp = Arc::new(ContextMcp::default());
        let f = fixture_on(model.clone(), &[], SKILLS, |deps| {
            deps.mcp = mcp.clone();
            deps.skills = vec!["orchestrator".into(), "routing".into()];
            deps.env = agent::session::EnvConfig { user_email: None, telegram_chat_id: Some("999".into()) };
        });
        mcp.notes.lock().unwrap().push("Earlier activity from the journal".into());
        mcp.history.lock().unwrap().push(history(
            100,
            "Earlier message",
            "user",
            -10042,
            Some(7),
            "2026-09-06 11:59:00",
        ));
        let supervisor = create_supervisor(f.engine.clone());
        let mut incoming = signal(source, 50);
        if source == "telegram_button" {
            incoming.content =
                json!({"chatId":-10042,"messageThreadId":7,"messageId":123,"text":"Stale button","data":"unclaimed"})
                    .to_string();
        }
        assert_eq!(supervisor.run_signal(&incoming).await.unwrap(), "default answer");
        let primary = primary_of(&f.engine, 50).await;
        assert_eq!(primary.admission.name, "agent-loop");
        let target = json!({"chatId":"-10042","messageThreadId":7});
        assert_eq!(primary.admission.host().unwrap()["confirm"], target);
        assert_eq!(primary.admission.host().unwrap()["conversation"], target);
        let seen = model.seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 1, "the default loop adds no silent-end reminder");
        let system = seen[0].messages[0]["content"].as_str().unwrap();
        assert!(system.contains("Earlier activity from the journal"), "{system}");
        assert!(system.contains("Delivery: use send_telegram_message for the current signal's chatId and optional messageThreadId. A final assistant message alone is not delivered to Telegram."));
        assert!(!system.contains("A message delivered to this conversation ends the session"));
        assert!(!system.contains("TELEGRAM"), "the default path does not autoload transport rules");
        let user = seen[0].messages.last().unwrap()["content"].as_str().unwrap();
        assert!(user.contains("telegram.history"), "{user}");
        assert!(
            mcp.calls
                .lock()
                .unwrap()
                .iter()
                .any(|(n, a)| n == "get_telegram_chat_history" && a["chatId"] == "-10042" && a["threadId"] == 7)
        );
        supervisor.shutdown().await;
    }
}

// A primary loop cancelled while its turn is in flight (an operator's
// cancel) ends its signal without a failure report (design 03, decision 6).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancelled_primary_ends_its_signal_without_a_report() {
    let in_flight = Arc::new(tokio::sync::Notify::new());
    let notify = in_flight.clone();
    let model = Turns::new(vec![turn_async(move |p| async move {
        notify.notify_one();
        p.cancel.cancelled().await;
        Err(agent::errors::aborted())
    })]);
    let (f, _, supervisor) = setup(model.clone());
    let running = {
        let supervisor = supervisor.clone();
        tokio::spawn(async move { supervisor.run_signal(&signal("scheduler", 42)).await })
    };
    in_flight.notified().await;
    let primary = primary_of(&f.engine, 42).await;
    assert!(f.engine.runtime.cancel(&primary.admission.id).await.unwrap());
    running.await.unwrap().unwrap();
    assert_eq!(model.requests.lock().unwrap().len(), 1, "no report was asked of the model");
    assert!(f.engine.runtime.get("recovery:42").await.unwrap().is_none(), "no report run");
    let (_, obs) = f.db.get_trace("scheduler:42").unwrap().unwrap();
    assert!(!obs.iter().any(|o| o.name == "recovery"));
    let loops = f.engine.loop_ids().await;
    assert!(loops.is_empty(), "{loops:?} {:?}", primary.admission.id);
    supervisor.shutdown().await;
}

// ── saved workflows, signal overrides and routes (design 04) ────────────────
// Restored from tests/pending on the engine: a scheduled task's workflow
// and a route are the signal's primary run; the agent's `signal` replaces
// the dispatcher until its breaker sets it aside.

// A scheduled task naming a saved workflow runs it without a model. A
// run that faulted before any write hands the task to the agent loop; one
// that faulted after a write only recovers, as a failed execution does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_saved_workflow_runs_and_faults_hand_over_by_effect() {
    const HEAD: &str = "document: { dsl: '1.0.4', namespace: agent, name: w, version: '1.0.0' }\n";
    let cases = [
        ("completes", "do: [ { a: { set: { done: true } } } ]"),
        ("faults first", "do: [ { a: { raise: { error: { type: 'https://x/e', status: 500 } } } } ]"),
        (
            "faults after a write",
            "do: [ { a: { call: t1 } }, { b: { raise: { error: { type: 'https://x/e', status: 500 } } } } ]",
        ),
        ("is missing", ""),
    ];
    for (case, body) in cases {
        let model = Arc::new(Scripted::new(vec![assistant("done"), assistant("done")]));
        let (f, mcp, supervisor) = setup(model.clone());
        if !body.is_empty() {
            activate_definition(&f.engine, "w", &format!("{HEAD}{body}")).await;
        }
        let scheduled = PendingSignal { payload: Some(json!({"workflow":"w"})), ..signal("scheduler", 42) };
        let result = supervisor.run_signal(&scheduled).await;

        let seen = model.seen.lock().unwrap().clone();
        let system = seen.first().map(|s| s.messages[0]["content"].as_str().unwrap().to_owned());
        match case {
            "completes" => {
                let output: Value = serde_json::from_str(&result.unwrap()).unwrap();
                assert_eq!(output, json!({"ok":true,"workflow":"w","output":{"done":true}}));
                assert!(seen.is_empty(), "no model call");
            }
            "faults first" | "is missing" => {
                assert_eq!(result.unwrap(), "done");
                let system = system.unwrap();
                assert!(system.contains("SCHEDULER"), "{case}: the agent loop takes the task");
                let note =
                    if case == "is missing" { "has no active version" } else { "failed before changing anything" };
                assert!(system.contains(note), "{case}: {system}");
            }
            _ => {
                assert!(result.unwrap_err().to_string().contains("may have changed something"));
                let system = system.unwrap();
                assert!(system.contains("RECOVERY"), "{case}: only a report");
                assert!(!system.contains("SCHEDULER") && !system.contains("ROUTING"));
                assert!(
                    seen[0].messages[1]["content"].as_str().unwrap().contains("Do not repeat the original actions")
                );
                assert!(!seen[0].tools.iter().any(|t| t.name == "invoke_sub_agent"));
                assert!(mcp.calls.lock().unwrap().iter().any(|(n, _)| n == "t1"));
            }
        }
        let (trace, obs) = f.db.get_trace("scheduler:42").unwrap().unwrap();
        assert_eq!(trace.tags, ["scheduler", "saved-workflow"]);
        if matches!(case, "faults first" | "is missing") {
            assert_eq!(trace.metadata.as_ref().unwrap()["fallback"], "agent-loop");
            let primary = obs.iter().find(|o| o.name == "agent_loop").unwrap();
            assert_eq!(primary.meta("preset"), Some(&json!("base")));
            assert!(obs.iter().any(|o| o.name == "fallback" && o.level == "WARNING"));
        }
        assert_eq!(obs.iter().any(|o| o.name == "recovery"), case == "faults after a write");
        let ran = obs.iter().any(|o| o.name == "runner" && o.meta("workflow") == Some(&json!("w")));
        assert_eq!(ran, !body.is_empty(), "{case}");
        supervisor.shutdown().await;
    }
    // A payload on another source is not a workflow.
    let model = Arc::new(Scripted::new(vec![assistant("done"), assistant("done")]));
    let (_f, _, supervisor) = setup(model);
    let telegram = PendingSignal { payload: Some(json!({"workflow":"w"})), ..signal("telegram", 43) };
    assert_eq!(telegram.saved_workflow(), None);
    supervisor.run_signal(&telegram).await.unwrap();

    supervisor.shutdown().await;
}

// A saved workflow left waiting (here, for an event) is still the engine's
// to finish: the signal ends without a recovery report and without the
// agent loop, and resumes when its event arrives.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_interrupted_saved_workflow_is_left_to_durable_recovery() {
    let model = Arc::new(Scripted::new(vec![assistant("done"), assistant("done")]));
    let (f, _, supervisor) = setup(model.clone());
    let head = "document: { dsl: '1.0.4', namespace: agent, name: w, version: '1.0.0' }\n";
    let waits = format!(
        "{head}do: [ {{ a: {{ listen: {{ to: {{ one: {{ with: {{ type: go-on }} }} }} }} }} }}, {{ b: {{ set: {{ done: true }} }} }} ]"
    );
    activate_definition(&f.engine, "w", &waits).await;
    let scheduled = PendingSignal { payload: Some(json!({"workflow":"w"})), ..signal("scheduler", 42) };
    let output: Value = serde_json::from_str(&supervisor.run_signal(&scheduled).await.unwrap()).unwrap();
    assert_eq!(output["resumes"], true);
    assert!(model.seen.lock().unwrap().is_empty(), "neither the agent loop nor recovery ran");
    let (_, obs) = f.db.get_trace("scheduler:42").unwrap().unwrap();
    assert!(!obs.iter().any(|o| o.name == "recovery"));
    let event = json!({"source":"test","id":"go-on-1","type":"go-on","data":{}});
    f.engine.runtime.deliver(event, None).await.unwrap();
    let ended = ended(&f.engine, "signal:42").await;
    assert_eq!(ended, agent::runs::Status::Completed(json!({"ok":true,"workflow":"w","output":{"done":true}})));
    supervisor.shutdown().await;
}

// The agent's `signal` (after a dry run) handles every signal started
// after; three faults of it in a row set it aside and the built-in runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_activated_signal_workflow_handles_signals_until_its_breaker_opens() {
    let model = Arc::new(Scripted::new((0..4).map(|_| assistant("done")).collect()));
    let (f, _mcp, supervisor) = setup(model.clone());
    f.engine.ready().await.unwrap();
    let tool = async |name: &str, args: Value| {
        let reply = agent::flows::tool(&f.engine, name, args.as_object().unwrap()).await.unwrap();
        serde_json::from_str::<Value>(&reply).unwrap()
    };
    let read = tool("read_workflow", json!({"name": "signal"})).await;
    assert_eq!((read["builtin"].as_bool(), read["definition"].as_str()), (Some(true), Some(agent::supervisor::SIGNAL)));
    let last_system = || model.seen.lock().unwrap().last().unwrap().messages[0]["content"].as_str().unwrap().to_owned();

    let terse = without_breaker(agent::supervisor::SIGNAL)
        .replace(r#""Call find_skills only when"#, r#""Answer tersely.", "Call find_skills only when"#);
    assert_ne!(terse, without_breaker(agent::supervisor::SIGNAL));
    activate_definition(&f.engine, "signal", &terse).await;
    supervisor.run_signal(&signal("peer", 1)).await.unwrap();
    assert!(last_system().contains("Answer tersely."), "{}", last_system());

    let broken = "document: { dsl: '1.0.4', namespace: agent, name: signal, version: '9.0.0' }\n\
        do: [ { boom: { raise: { error: { type: 'https://example.com/boom', status: 500 } } } } ]\n";
    activate_definition(&f.engine, "signal", broken).await;
    for n in 2..5 {
        assert!(supervisor.run_signal(&signal("peer", n)).await.is_err(), "fault {}", n - 1);
    }
    supervisor.run_signal(&signal("peer", 5)).await.unwrap();
    assert!(!last_system().contains("Answer tersely."), "the built-in signal workflow is back");
    assert!(last_system().contains("Workflow `signal`"), "told why: {}", last_system());
    assert_eq!(model.seen.lock().unwrap().len(), 2);
    supervisor.shutdown().await;
}

// Recovery policy belongs to the activated signal definition, even though
// the host keeps the same report span and loop lifecycle.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_activated_signal_override_changes_the_recovery_prompt() {
    let model = Arc::new(Scripted::new(vec![assistant("reported")]));
    let (f, _, supervisor) = setup(model.clone());
    let definition =
        without_breaker(SIGNAL).replace("Report the failure only.", "Explain this failure in one sentence.");
    activate_definition(&f.engine, "signal", &definition).await;
    let broken = r#"
document: { dsl: '1.0.4', namespace: agent, name: broken, version: '1.0.0' }
do:
  - write: { call: t1 }
  - fail: { raise: { error: { type: 'https://example.com/failure', status: 500, detail: 'original failure' } } }
"#;
    activate_definition(&f.engine, "broken", broken).await;
    let pending = PendingSignal { payload: Some(json!({"workflow":"broken"})), ..signal("scheduler", 42) };
    let error = supervisor.run_signal(&pending).await.unwrap_err().to_string();
    assert!(error.contains("original failure"), "{error}");
    let seen = model.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 1);
    let prompt = seen[0].messages[1]["content"].as_str().unwrap();
    assert!(prompt.contains("Explain this failure in one sentence."), "{prompt}");
    assert!(!prompt.contains("Report the failure only."));
    assert!(prompt.contains("original failure"));
    assert!(prompt.contains("Recent transcript (may be truncated):\n[]"));
    let (trace, obs) = f.db.get_trace("scheduler:42").unwrap().unwrap();
    let recovery = obs.iter().find(|o| o.name == "recovery").unwrap();
    let root = obs.iter().find(|o| o.parent_observation_id.is_none()).unwrap();
    assert_eq!(recovery.parent_observation_id.as_deref(), Some(root.id.as_str()));
    assert_eq!(trace.output["error"], error);
    let loop_span = obs.iter().find(|o| o.meta("agent_id") == Some(&json!("recovery:42"))).unwrap();
    assert_eq!(loop_span.meta("max_iterations"), Some(&json!(5)));
    supervisor.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_signal_override_without_a_catch_gets_no_automatic_recovery() {
    let model = Arc::new(Scripted::new(vec![]));
    let (f, _, supervisor) = setup(model.clone());
    let definition = without_breaker(SIGNAL).split("      catch:").next().unwrap().replace("      try:", "      do:");
    activate_definition(&f.engine, "signal", &definition).await;
    assert!(supervisor.run_signal(&signal("peer", 42)).await.is_err());
    assert_eq!(model.seen.lock().unwrap().len(), 1);
    let (_, obs) = f.db.get_trace("peer:42").unwrap().unwrap();
    assert!(!obs.iter().any(|o| o.name == "recovery"));
    supervisor.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_catch_before_signal_session_cannot_start_recovery() {
    let model = Arc::new(Scripted::new(vec![]));
    let (f, _, supervisor) = setup(model.clone());
    let definition = SIGNAL.replace("call: signal_session", "call: primary_loop");
    // The gate refuses it (primary_loop needs its input); an older rule's
    // version, stored past the gate, still gets no report.
    let args = json!({"name":"signal","definition":without_breaker(&definition)});
    let refused = agent::flows::tool(&f.engine, "save_workflow", args.as_object().unwrap()).await.unwrap_err();
    assert!(refused.to_string().contains("needs the argument `input`"), "{refused}");
    store_definition(&f.engine, "signal", &definition).await;
    let error = supervisor.run_signal(&signal("peer", 42)).await.unwrap_err().to_string();
    assert!(error.contains("input: Required"), "{error}");
    assert!(model.seen.lock().unwrap().is_empty());
    let (_, obs) = f.db.get_trace("peer:42").unwrap().unwrap();
    assert!(!obs.iter().any(|o| o.name == "recovery"));
    supervisor.shutdown().await;
}

// A route is a workflow the dispatcher starts as the signal's primary run
// (design 04: a workflow calling the dispatcher's functions is a `signal`
// override and declares no route).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn activated_signal_routes_share_the_session_and_trace_and_take_precedence_over_default() {
    let model = Arc::new(Scripted::new(vec![assistant("route answer")]));
    let (f, _, supervisor) = setup(model.clone());
    f.engine.ready().await.unwrap();
    let superseded = r#"document: {dsl: '1.0.4', namespace: agent, name: research-signal, version: '1.0.0', metadata: {route: {when: {source: telegram, topic: research}}}}
do:
  - primary:
      call: primary_loop
      with: {input: '${ $context.signal.content }', loop: agent-loop, skills: [telegram], preset: smart}
output: {as: '${ .output }'}
"#;
    let args = json!({"name":"research-signal","definition":superseded});
    let refused = agent::flows::tool(&f.engine, "save_workflow", args.as_object().unwrap()).await.unwrap_err();
    assert!(refused.to_string().contains("primary_loop"), "{refused}");
    let definition = variant("research-signal").replace(
        "metadata: { loop: { description: 'General ReAct loop with guarded tools and workers.' } }",
        "metadata: { route: { when: { source: telegram, topic: research } } }",
    );
    activate_definition(&f.engine, "research-signal", &definition).await;
    let mut incoming = signal("telegram", 45);
    incoming.payload = Some(json!({"topic":"research"}));
    assert_eq!(supervisor.run_signal(&incoming).await.unwrap(), "route answer");
    let (trace, observations) = f.db.get_trace("telegram:45").unwrap().unwrap();
    assert_eq!(trace.session_id.as_deref(), Some("telegram:45"));
    assert_eq!(trace.output, "route answer");
    assert!(observations.iter().any(|o| o.name == "route:research-signal"));
    assert!(!observations.iter().any(|o| o.name.starts_with("route:telegram")));
    assert_eq!(model.seen.lock().unwrap().len(), 1);
    supervisor.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_signal_route_has_its_own_breaker_and_reports_each_failure_once() {
    let model = Arc::new(Scripted::new(vec![
        assistant("report 1"),
        assistant("report 2"),
        assistant("report 3"),
        assistant("reply restored"),
    ]));
    let (f, _, supervisor) = setup(model.clone());
    let definition = r#"document: {dsl: '1.0.4', namespace: agent, name: signal-telegram, version: '2.0.0', metadata: {route: {when: {source: telegram}}}}
do:
  - failure: {raise: {error: {type: 'https://example.com/fault', status: 500}}}
"#;
    activate_definition(&f.engine, "signal-telegram", definition).await;
    for id in 46..49 {
        assert!(supervisor.run_signal(&signal("telegram", id)).await.is_err());
    }
    assert_eq!(model.seen.lock().unwrap().len(), 3, "one recovery turn per fault");
    assert_eq!(supervisor.run_signal(&signal("telegram", 49)).await.unwrap(), "reply restored");
    assert_eq!(model.seen.lock().unwrap().len(), 4);
    let (trace, _) = f.db.get_trace("telegram:49").unwrap().unwrap();
    assert_eq!(trace.output, "reply restored");
    supervisor.shutdown().await;
}

// A loop that declares the Telegram route handles the signal alone: no
// signal route, its own context, its own setup, and the guard sees what it
// was told and what the user wrote before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_telegram_signal_runs_the_loop_that_declares_its_route() {
    let model = Arc::new(Scripted::new(vec![
        calls(&[("t1", json!({"note":"go ahead"}))]),
        calls(&[("send_telegram_message", json!({"chatId":"-10042","messageThreadId":7,"text":"all done"}))]),
    ]));
    let api = Arc::new(GuardApi::default());
    let mcp = Arc::new(ContextMcp::default());
    mcp.status.store(true, Ordering::SeqCst);
    let f = fixture_on(model.clone(), &[], SKILLS, |deps| {
        deps.mcp = mcp.clone();
        deps.skills = vec!["orchestrator".into(), "routing".into()];
        deps.decisions = Some(agent::decisions::decision_client("guard-test".into(), api.clone()));
        deps.env = agent::session::EnvConfig { user_email: None, telegram_chat_id: Some("999".into()) };
    });
    mcp.notes.lock().unwrap().push("Earlier activity from the journal".into());
    mcp.history.lock().unwrap().push(history(100, "Earlier message", "user", -10042, Some(7), "2026-09-06 11:59:00"));
    activate_telegram_loop(&f.engine).await;
    let routes = agent::flows::signal_routes(&f.engine, "telegram", &Value::Null).await.unwrap();
    assert_eq!(routes[0].0, "telegram-loop");
    let definitions = f.engine.runtime.definitions("telegram-loop").await.unwrap();
    assert!(agent::flows::is_loop_definition("telegram-loop", &definitions.versions[&routes[0].1].yaml));
    let supervisor = create_supervisor(f.engine.clone());
    assert_eq!(supervisor.run_signal(&signal("telegram", 42)).await.unwrap(), "all done");
    let run = ended_children_of(&f.engine, "signal:42").await.into_iter().find(|c| c.admission.name == "telegram-loop");
    let run = run.expect("the route's run");
    assert_eq!(run.admission.input["signal"]["source"], "telegram");
    let host = run.admission.host().unwrap();
    assert_eq!(host["preset"], "smart");
    assert_eq!(host["confirm"], json!({"chatId":"-10042","messageThreadId":7}));
    let seen = model.seen.lock().unwrap().clone();
    let system = seen[0].messages[0]["content"].as_str().unwrap();
    for part in [
        "Signal source: telegram",
        "Default Telegram chat id: 999.",
        "Earlier activity from the journal",
        "TELEGRAM",
        "ORCHESTRATOR",
        "Delivery: use send_telegram_message",
        "A message delivered to this conversation ends the session",
    ] {
        assert!(system.contains(part), "{part}: {system}");
    }
    assert!(system.ends_with(agent::tool_results::WORKING_MEMORY_INSTRUCTIONS), "the setup goes before the rules");
    let user = seen[0].messages[1]["content"].as_str().unwrap();
    assert!(user.contains("telegram.history") && user.contains("Earlier message") && user.contains("go ahead"));
    let review = api.requests.lock().unwrap().iter().find(|r| r.to_string().contains("\\\"note\\\"")).unwrap().clone();
    assert!(review["state"]["skills"].as_str().unwrap().contains("TELEGRAM"), "{review}");
    assert!(review["state"]["earlier_user_messages"].as_str().unwrap().contains("Earlier message"), "{review}");
    assert_eq!(review["state"]["task"], signal("telegram", 42).content);
    let statuses: Vec<_> = mcp
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(n, _)| n == "telegram_send_status")
        .map(|(_, a)| a["text"].clone())
        .collect();
    assert_eq!(statuses, [json!("👀"), json!("")], "opened before the first turn, cleared at the end");
    let (trace, observations) = f.db.get_trace("telegram:42").unwrap().unwrap();
    assert_eq!(trace.output, "all done");
    assert!(observations.iter().any(|o| o.name == "route:telegram-loop"));
    assert!(observations.iter().any(|o| o.name == "find_skills" && o.meta("automatic") == Some(&json!(true))));
    // The routed loop's skills are on the signal's trace, as on `main`, and
    // so the activity journal's packet lists them.
    let skills = json!(["orchestrator", "routing", "telegram"]);
    assert_eq!(trace.metadata.as_ref().unwrap()["skills"], skills);
    let packet = agent::activity::project_trace(&trace, &observations, None, &Default::default());
    assert_eq!(packet["skills"], skills);
    supervisor.shutdown().await;
}

// What loop_setup added survives a restart: a loop rebuilt from its journal
// in the next process still runs the skills it loaded and gives the guard
// the same evidence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rebuilt_routed_loop_keeps_what_its_setup_added() {
    let model = Arc::new(Scripted::new(vec![
        calls(&[("t1", json!({"n":1}))]),
        calls(&[("t1", json!({"n":2}))]),
        calls(&[("send_telegram_message", json!({"chatId":"-10042","messageThreadId":7,"text":"finished"}))]),
    ]));
    let mcp = Arc::new(ContextMcp::default());
    mcp.status.store(true, Ordering::SeqCst);
    mcp.history.lock().unwrap().push(history(100, "Earlier message", "user", -10042, Some(7), "2026-09-06 11:59:00"));
    let routed = |label: &str| {
        let api = Arc::new(GuardApi::default());
        *api.label.lock().unwrap() = Some(label.into());
        let (mcp, decisions) = (mcp.clone(), agent::decisions::decision_client("guard-test".into(), api.clone()));
        let configure = move |deps: &mut agent::agent::EngineDeps| {
            deps.mcp = mcp;
            deps.skills = vec!["orchestrator".into(), "routing".into()];
            deps.decisions = Some(decisions);
            deps.env = agent::session::EnvConfig { user_email: None, telegram_chat_id: Some("999".into()) };
        };
        (api, configure)
    };
    let (_, configure) = routed("needs_confirmation");
    let f = fixture_on(model.clone(), &[], SKILLS, configure);
    activate_telegram_loop(&f.engine).await;
    let supervisor = create_supervisor(f.engine.clone());
    assert!(supervisor.run_signal(&signal("telegram", 42)).await.unwrap().contains("resumes"));
    let card = card_of(&mcp);
    supervisor.shutdown().await;

    // The next process, with a guard that allows: the press resumes the loop.
    let (api, configure) = routed("allow");
    let restarted = reopen(&f, model.clone(), Arc::new(ouro_runtime::durable::SystemClock), configure);
    let supervisor = create_supervisor(restarted.clone());
    let mut press = signal("telegram_button", 44);
    press.content = json!({"chatId":-10042,"messageThreadId":7,"messageId":700,"data":card["buttons"][0][0]["data"],"text":"Confirm"}).to_string();
    assert_eq!(supervisor.run_signal(&press).await.unwrap(), "Button delivered");
    assert_eq!(ended(&restarted, "signal:42").await, agent::runs::Status::Completed(json!("finished")));
    let review = api.requests.lock().unwrap().iter().find(|r| r.to_string().contains("\\\"n\\\":2")).unwrap().clone();
    assert!(review["state"]["skills"].as_str().unwrap().contains("TELEGRAM"), "{review}");
    assert!(review["state"]["earlier_user_messages"].as_str().unwrap().contains("Earlier message"), "{review}");
    assert_eq!(mcp.calls.lock().unwrap().iter().filter(|(n, _)| n == "t1").count(), 2);
    supervisor.shutdown().await;
}
