// Rust-only: routing, source isolation, recovery, shutdown and the poll loop
// as scenarios, beyond the one-to-one ports in agent_loop.rs,
// telegram_context.rs and supervisor_e2e.rs.
mod support;
use agent::{
    db::TraceStore,
    flows::WorkflowSignal,
    mcp::Mcp,
    providers::{ToolDef, assistant},
    session::{EnvDeps, Session},
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
}
#[async_trait]
impl Mcp for ContextMcp {
    fn tools(&self) -> Vec<ToolDef> {
        ["get_timezone", "get_telegram_chat_history", "get_peer_thread", "journal_tail", "t1"]
            .iter()
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
    let mut f = fixture(model, &[], SKILLS);
    let mcp = Arc::new(ContextMcp::default());
    let deps = &mut Arc::get_mut(&mut f.engine).unwrap().deps;
    deps.mcp = mcp.clone();
    deps.skills = vec!["orchestrator".into(), "routing".into()];
    deps.decisions = decisions;
    let supervisor = create_supervisor(
        f.engine.clone(),
        EnvDeps {
            mcp: mcp.clone(),
            memory: f.db.clone(),
            user_email: Some("user@test".into()),
            telegram_chat_id: None,
        },
        agent::flows::create_flows(f.engine.clone()),
    );
    (f, mcp, supervisor)
}
#[tokio::test]
async fn signals_without_saved_workflows_run_the_primary_loop_with_the_source_context() {
    for source in ["scheduler", "telegram", "dreaming", "new-domain"] {
        let model = Arc::new(Scripted::new(vec![assistant("done")]));
        let (f, mcp, supervisor) = setup(model.clone());
        assert_eq!(supervisor.run_signal(&signal(source, 42)).await.unwrap(), "done");
        let seen = model.seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 1, "one primary turn, with no compiler generation");
        let system = seen[0].messages[0]["content"].as_str().unwrap();
        assert!(system.contains("Europe/Chisinau"));
        assert!(system.contains("Default Telegram chat id: 999."));
        assert!(system.contains("ORCHESTRATOR"));
        assert_eq!(system.contains("SCHEDULER"), source == "scheduler");
        assert_eq!(seen[0].model, if source == "scheduler" { "gpt-5.4-mini" } else { "gemini-3.7-flash" });
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
// A scheduled task naming a saved workflow runs it without a model. A
// run that faulted before any write hands the task to the agent loop; one
// that faulted after a write only recovers, as a failed execution does.
#[tokio::test]
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
        let model = Arc::new(Scripted::new(vec![assistant("done")]));
        let (f, mcp, supervisor) = setup(model.clone());
        if !body.is_empty() {
            let saved = f.engine.deps.workflows.save("w", &format!("{HEAD}{body}"), None).unwrap();
            f.engine.deps.workflows.activate("w", saved.version).unwrap();
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
    let model = Arc::new(Scripted::new(vec![assistant("done")]));
    let (_f, _, supervisor) = setup(model);
    let telegram = PendingSignal { payload: Some(json!({"workflow":"w"})), ..signal("telegram", 43) };
    assert_eq!(telegram.saved_workflow(), None);
    supervisor.run_signal(&telegram).await.unwrap();

    supervisor.shutdown().await;
}
// An interrupted saved workflow is still the durable worker's to finish:
// the signal ends without a recovery report and without the agent loop.
#[tokio::test]
async fn an_interrupted_saved_workflow_is_left_to_durable_recovery() {
    struct Interrupting;
    #[async_trait]
    impl agent::flows::FlowRun for Interrupting {
        async fn run(
            &self,
            _: &str,
            _: &WorkflowSignal,
            _: &Session,
            _: &agent::tracing::Scope,
            _: &CancellationToken,
        ) -> agent::flows::FlowResult {
            agent::flows::FlowResult::Interrupted { error: owf::run::WorkflowError::runtime("disk full") }
        }
    }
    let model = Arc::new(Scripted::new(vec![assistant("done")]));
    let (f, mcp, _) = setup(model.clone());
    let supervisor = create_supervisor(
        f.engine.clone(),
        EnvDeps {
            mcp: mcp.clone(),
            memory: f.db.clone(),
            user_email: Some("user@test".into()),
            telegram_chat_id: None,
        },
        Arc::new(Interrupting),
    );
    let scheduled = PendingSignal { payload: Some(json!({"workflow":"w"})), ..signal("scheduler", 42) };
    let output: Value = serde_json::from_str(&supervisor.run_signal(&scheduled).await.unwrap()).unwrap();
    assert_eq!(output["resumes"], true);
    assert!(model.seen.lock().unwrap().is_empty(), "neither the agent loop nor recovery ran");
    let (_, obs) = f.db.get_trace("scheduler:42").unwrap().unwrap();
    assert!(!obs.iter().any(|o| o.name == "recovery"));
    supervisor.shutdown().await;
}
// A scheduled fire runs on the supervisor like a queued signal: with the
// environment note one carries, and its saved workflow when the task names
// one. A supervisor that is stopping starts nothing: the slot goes back.
#[tokio::test]
async fn a_scheduled_fire_runs_as_a_signal_with_mcps_note_and_its_workflow() {
    use agent::schedule::{FIRST_FIRE_ID, Fire, FireSink, Fired, SupervisorSink};
    let model = Arc::new(Scripted::new(vec![assistant("done")]));
    let (f, _, supervisor) = setup(model.clone());
    let head = "document: { dsl: '1.0.4', namespace: agent, name: w, version: '1.0.0' }\n";
    let saved = f.engine.deps.workflows.save("w", &format!("{head}do: [ {{ a: {{ set: {{ done: true }} }} }} ]"), None);
    f.engine.deps.workflows.activate("w", saved.unwrap().version).unwrap();
    let env_context = agent::session::telegram_env_note(Some("4242"), &[]);
    let sink = SupervisorSink { supervisor: supervisor.clone(), env_context };
    let fire = |n: i64, source: &str, workflow: Option<&str>| Fire {
        id: FIRST_FIRE_ID + n,
        task_id: n,
        source: source.into(),
        content: format!("Scheduled task #{n} fired.\n\nDaily dreaming tick."),
        workflow: workflow.map(str::to_owned),
        slot: chrono::Utc::now(),
    };

    assert_eq!(sink.fire(fire(1, "dreaming", None)).await, Fired::Started);
    let system = model.seen.lock().unwrap()[0].messages[0]["content"].as_str().unwrap().to_owned();
    assert!(system.contains("Default Telegram chat id: 4242."), "the note reaches the prompt: {system}");
    assert!(f.db.get_trace(&format!("dreaming:{}", FIRST_FIRE_ID + 1)).unwrap().is_some());

    assert_eq!(sink.fire(fire(2, "scheduler", Some("w"))).await, Fired::Started);
    let (_, obs) = f.db.get_trace(&format!("scheduler:{}", FIRST_FIRE_ID + 2)).unwrap().unwrap();
    assert!(obs.iter().any(|o| o.name == "runner"), "the saved workflow ran");

    assert_eq!(model.seen.lock().unwrap().len(), 1, "no model call for the workflow");

    supervisor.shutdown().await;
    assert_eq!(sink.fire(fire(3, "scheduler", None)).await, Fired::NotStarted);
    assert!(f.db.get_trace(&format!("scheduler:{}", FIRST_FIRE_ID + 3)).unwrap().is_none());
}
#[tokio::test]
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
#[tokio::test]
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
#[tokio::test]
async fn a_peer_signal_loads_its_skill_and_the_earlier_turns_of_its_thread() {
    let model = Arc::new(Scripted::new(vec![assistant("done")]));
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
#[tokio::test]
async fn a_signal_with_durable_flows_runs_its_loop_as_an_agent_loop_run() {
    let model = Arc::new(Scripted::new(vec![assistant("done")]));
    let mut f = fixture(model.clone(), &[], SKILLS);
    let mcp = Arc::new(ContextMcp::default());
    let deps = &mut Arc::get_mut(&mut f.engine).unwrap().deps;
    deps.mcp = mcp.clone();
    deps.skills = vec!["orchestrator".into(), "routing".into()];
    let engine = f.engine.clone();
    let supervisor = create_supervisor_with_cancel(
        engine.clone(),
        EnvDeps { mcp: mcp.clone(), memory: f.db.clone(), user_email: None, telegram_chat_id: None },
        agent::flows::create_durable_flows(engine.clone(), (*f.db).clone()),
        CancellationToken::new(),
    );
    assert_eq!(supervisor.run_signal(&signal("peer", 42)).await.unwrap(), "done");
    let run = agent::durable::RunStore::new((*f.db).clone()).get("peer:42/loop").unwrap().unwrap();
    assert_eq!(run.outcome, Some(owf::run::Status::Completed(json!("done"))));
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
                    let p = if s["name"] == "telegram" { 0.97 } else { 0.05 };
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
#[tokio::test]
async fn a_signal_starts_with_the_skills_jev_selects_for_it() {
    let model = Arc::new(Scripted::new(vec![assistant("done")]));
    let (api, client) = jev(false);
    let (f, _mcp, supervisor) = setup_with(model.clone(), Some(client));
    supervisor.run_signal(&signal("telegram", 42)).await.unwrap();
    let seen = model.seen.lock().unwrap().clone();
    let system = seen[0].messages[0]["content"].as_str().unwrap();
    let selected = system.split("## Skills selected for this request").nth(1).expect("a preselected block");
    assert!(selected.contains("### Skill `telegram`") && selected.contains("TELEGRAM"), "{system}");
    assert!(!selected.contains("### Skill `orchestrator`"), "loaded skills are never inlined twice");
    assert!(!selected.contains("SCHEDULER"));
    let request = api.requests.lock().unwrap()[0].clone();
    assert!(request["state"]["current_request"].as_str().unwrap().contains("go ahead"));
    assert!(request["state"]["context"].as_str().unwrap().starts_with("Signal source: telegram"));
    let (trace, obs) = f.db.get_trace("telegram:42").unwrap().unwrap();
    assert_eq!(trace.metadata.as_ref().unwrap()["skills"], json!(["orchestrator", "routing", "telegram"]));
    let search = obs.iter().find(|o| o.name == "find_skills").expect("an automatic find_skills span");
    assert_eq!(search.meta("automatic"), Some(&json!(true)));
    supervisor.shutdown().await;
}
#[tokio::test]
async fn a_failed_preselection_leaves_the_search_to_the_model() {
    let model = Arc::new(Scripted::new(vec![assistant("done")]));
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
#[tokio::test]
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
#[tokio::test]
async fn recent_activity_is_bounded_oldest_first_and_stays_out_of_user_history() {
    let model = Arc::new(Scripted::new(vec![assistant("done")]));
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
#[tokio::test]
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
#[tokio::test]
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
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let n = mcp.calls.lock().unwrap().iter().filter(|(n, _)| n == "get_next_signal").count();
            if n >= 4 {
                break;
            }
            tokio::task::yield_now().await;
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
#[tokio::test]
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
    mcp.queue.lock().unwrap().push_back(json!({"signal":signal("dreaming",7),"pendingAfter":0}));
    mcp.arrived.notify_waiters();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while f.db.get_trace("dreaming:7").unwrap().is_none() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the signal is handled right after the wait, not an interval later");
    stop.cancel();
    mcp.arrived.notify_waiters();
    tokio::time::timeout(std::time::Duration::from_secs(2), task).await.unwrap().unwrap().unwrap();
    assert_eq!(model.seen.lock().unwrap().len(), 1);
    supervisor.shutdown().await;
}

// Rust-only: a wait that fails (MCP down, a reply of the wrong shape) is an
// error like any other — the loop backs off instead of spinning on
// get_next_signal.
#[tokio::test]
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

// The agent may replace how signals are handled: the version it activates
// under `signal` handles every signal after it, with no dry run, and three
// faults of it in a row put the built-in one back.
#[tokio::test]
async fn an_activated_signal_workflow_handles_signals_until_its_breaker_opens() {
    let model = Arc::new(Scripted::new((0..4).map(|_| assistant("done")).collect()));
    let (f, _mcp, supervisor) = setup(model.clone());
    let tool = |name: &str, args: Value| agent::flows::tool(&f.engine, name, args.as_object().unwrap()).unwrap();
    let activate = |definition: &str| {
        let saved = f.engine.deps.workflows.save("signal", definition, None).unwrap();
        tool("activate_workflow", json!({"name": "signal", "version": saved.version}))
    };
    let read: Value = serde_json::from_str(&tool("read_workflow", json!({"name": "signal"}))).unwrap();
    assert_eq!(
        (read["status"].as_str(), read["definition"].as_str()),
        (Some("built-in"), Some(agent::supervisor::SIGNAL))
    );
    let last_system = || model.seen.lock().unwrap().last().unwrap().messages[0]["content"].as_str().unwrap().to_owned();

    let terse = agent::supervisor::SIGNAL
        .replace(r#""Call find_skills only when"#, r#""Answer tersely.", "Call find_skills only when"#);
    assert_ne!(terse, agent::supervisor::SIGNAL);
    activate(&terse);
    supervisor.run_signal(&signal("telegram", 1)).await.unwrap();
    assert!(last_system().contains("Answer tersely."), "{}", last_system());

    let broken = "document: { dsl: '1.0.4', namespace: agent, name: signal, version: '9.0.0' }\n\
        do: [ { boom: { raise: { error: { type: 'https://example.com/boom', status: 500 } } } } ]\n";
    activate(broken);
    for n in 2..5 {
        assert!(supervisor.run_signal(&signal("telegram", n)).await.is_err(), "fault {}", n - 1);
    }
    supervisor.run_signal(&signal("telegram", 5)).await.unwrap();
    assert!(!last_system().contains("Answer tersely."), "the built-in signal workflow is back");
    assert_eq!(model.seen.lock().unwrap().len(), 2);
    supervisor.shutdown().await;
}

// Recovery policy belongs to the activated signal definition, even though
// the host keeps the same report span and loop lifecycle.
#[tokio::test]
async fn an_activated_signal_override_changes_the_recovery_prompt() {
    let model = Arc::new(Scripted::new(vec![assistant("reported")]));
    let (f, _, supervisor) = setup(model.clone());
    let definition = SIGNAL.replace("Report the failure only.", "Explain this failure in one sentence.");
    let saved = f.engine.deps.workflows.save("signal", &definition, None).unwrap();
    agent::flows::tool(
        &f.engine,
        "activate_workflow",
        json!({"name": "signal", "version": saved.version}).as_object().unwrap(),
    )
    .unwrap();
    let saved = f
        .engine
        .deps
        .workflows
        .save(
            "broken",
            r#"
document: { dsl: '1.0.4', namespace: agent, name: broken, version: '1.0.0' }
do:
  - write: { call: t1 }
  - fail: { raise: { error: { type: 'https://example.com/failure', status: 500, detail: 'original failure' } } }
"#,
            None,
        )
        .unwrap();
    f.engine.deps.workflows.activate("broken", saved.version).unwrap();
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
    let loop_span = obs.iter().find(|o| o.meta("agent_id") == Some(&json!("scheduler:42__recovery"))).unwrap();
    assert_eq!(loop_span.meta("max_iterations"), Some(&json!(5)));
    supervisor.shutdown().await;
}

#[tokio::test]
async fn recovery_failure_keeps_the_primary_error_and_transcript() {
    for invalid_preset in [false, true] {
        let model = Arc::new(Scripted::new(vec![calls(&[("t1", json!({"marker":"primary transcript marker"}))])]));
        let (f, _, supervisor) = setup(model.clone());
        if invalid_preset {
            let definition = SIGNAL.replace("preset: base", "preset: unknown");
            let saved = f.engine.deps.workflows.save("signal", &definition, None).unwrap();
            f.engine.deps.workflows.activate("signal", saved.version).unwrap();
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

#[tokio::test]
async fn a_signal_override_without_a_catch_gets_no_automatic_recovery() {
    let model = Arc::new(Scripted::new(vec![]));
    let (f, _, supervisor) = setup(model.clone());
    let definition = SIGNAL.split("      catch:").next().unwrap().replace("      try:", "      do:");
    let saved = f.engine.deps.workflows.save("signal", &definition, None).unwrap();
    f.engine.deps.workflows.activate("signal", saved.version).unwrap();
    assert!(supervisor.run_signal(&signal("peer", 42)).await.is_err());
    assert_eq!(model.seen.lock().unwrap().len(), 1);
    let (_, obs) = f.db.get_trace("peer:42").unwrap().unwrap();
    assert!(!obs.iter().any(|o| o.name == "recovery"));
    supervisor.shutdown().await;
}

#[tokio::test]
async fn a_catch_before_signal_session_cannot_start_recovery() {
    let model = Arc::new(Scripted::new(vec![]));
    let (f, _, supervisor) = setup(model.clone());
    let definition = SIGNAL.replace("call: signal_session", "call: primary_loop");
    let saved = f.engine.deps.workflows.save("signal", &definition, None).unwrap();
    f.engine.deps.workflows.activate("signal", saved.version).unwrap();
    let error = supervisor.run_signal(&signal("peer", 42)).await.unwrap_err().to_string();
    assert!(error.contains("input: Required"), "{error}");
    assert!(model.seen.lock().unwrap().is_empty());
    let (_, obs) = f.db.get_trace("peer:42").unwrap().unwrap();
    assert!(!obs.iter().any(|o| o.name == "recovery"));
    supervisor.shutdown().await;
}

#[tokio::test]
async fn an_abort_without_supervisor_shutdown_gets_no_recovery() {
    struct Aborting;
    #[async_trait]
    impl agent::flows::FlowRun for Aborting {
        async fn run(
            &self,
            _: &str,
            _: &WorkflowSignal,
            _: &Session,
            _: &agent::tracing::Scope,
            _: &CancellationToken,
        ) -> agent::flows::FlowResult {
            agent::flows::FlowResult::Cancelled
        }
    }
    let model = Arc::new(Scripted::new(vec![]));
    let (f, mcp, _) = setup(model.clone());
    let supervisor = create_supervisor(
        f.engine.clone(),
        EnvDeps { mcp, memory: f.db.clone(), user_email: Some("user@test".into()), telegram_chat_id: None },
        Arc::new(Aborting),
    );
    let pending = PendingSignal { payload: Some(json!({"workflow":"w"})), ..signal("scheduler", 42) };
    let error = supervisor.run_signal(&pending).await.unwrap_err();
    assert!(agent::errors::is_aborted(&error), "{error}");
    assert!(model.seen.lock().unwrap().is_empty());
    let (_, obs) = f.db.get_trace("scheduler:42").unwrap().unwrap();
    assert!(!obs.iter().any(|o| o.name == "recovery"));
    assert!(f.engine.loop_ids().await.is_empty());
    supervisor.shutdown().await;
}
