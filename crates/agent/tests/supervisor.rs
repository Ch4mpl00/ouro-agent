// Rust-only: routing, source isolation, recovery, shutdown and the poll loop
// as scenarios, beyond the one-to-one ports in agent_loop.rs,
// telegram_context.rs and supervisor_e2e.rs.
mod support;
use agent::{
    db::TraceStore,
    mcp::Mcp,
    providers::{ToolDef, assistant},
    session::{EnvDeps, Session},
    supervisor::*,
    tracing::null_scope,
    workflow::{RunResult, Step, VariableStore, WorkflowRun, WorkflowSignal},
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
        ["get_timezone", "get_telegram_chat_history", "journal_tail", "t1"]
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
            "Telegram message in chat -10042 (forum topic thread_id=7).\nText: \"давай\"".into()
        } else {
            "do the task".into()
        },
        env_context: Some("Default Telegram chat id: 999.".into()),
        created_at: "2026-09-06 12:00:00".into(),
        payload: None,
    }
}
fn history(id: i64, text: &str, role: &str, chat: i64, thread: Option<i64>, at: &str) -> Value {
    json!({"id":id,"chat_id":chat,"thread_id":thread,"role":role,"text":text,"created_at":at})
}
const SKILLS: &[(&str, &str)] = &[
    ("telegram", "---\ntools: *\n---\nTELEGRAM"),
    ("scheduler", "---\ntools: *\n---\nSCHEDULER"),
    ("orchestrator", "---\ntools: []\n---\nORCHESTRATOR"),
    ("routing", "---\ntools: []\n---\nROUTING"),
    ("recovery", "---\ntools: []\n---\nRECOVERY"),
    ("dreaming", "---\ntools: []\n---\nDREAMING"),
];
#[derive(Clone, Copy)]
enum Stage {
    Success,
    Compile,
    Execute,
    Replan,
}
struct FakeWorkflow {
    stage: Stage,
    called: AtomicUsize,
}
#[async_trait]
impl WorkflowRun for FakeWorkflow {
    async fn run_signal(
        &self,
        _: &WorkflowSignal,
        _: Session,
        _: agent::tracing::Scope,
        _: &CancellationToken,
    ) -> anyhow::Result<RunResult> {
        self.called.fetch_add(1, Ordering::SeqCst);
        Ok(match self.stage {
            Stage::Success => RunResult::Ok { attempts: 1, step_count: 2, store: VariableStore::new(Map::new()) },
            Stage::Compile => {
                RunResult::CompileFailed { reason: "invalid_json", errors: vec!["broken".into()], attempts: 3 }
            }
            Stage::Execute => RunResult::ExecuteFailed {
                reason: "tool_error",
                error: anyhow::anyhow!("partial delivery"),
                step_index: 1,
                step: Step::Terminal,
            },
            Stage::Replan => RunResult::ReplanExhausted { passes: 3, attempts: 1 },
        })
    }
}
fn setup(
    model: Arc<dyn agent::providers::ChatProvider>,
    stage: Stage,
) -> (Fixture, Arc<ContextMcp>, Arc<FakeWorkflow>, Arc<Supervisor>) {
    let mut f = fixture(model, &[], SKILLS);
    let mcp = Arc::new(ContextMcp::default());
    let deps = &mut Arc::get_mut(&mut f.engine).unwrap().deps;
    deps.mcp = mcp.clone();
    deps.skills = vec!["orchestrator".into(), "routing".into()];
    let workflow = Arc::new(FakeWorkflow { stage, called: AtomicUsize::new(0) });
    let supervisor = create_supervisor(
        f.engine.clone(),
        EnvDeps { mcp: mcp.clone(), memory: f.db.clone(), user_email: Some("user@test".into()) },
        workflow.clone(),
        agent::flows::create_flows(f.engine.clone()),
    );
    (f, mcp, workflow, supervisor)
}
#[tokio::test]
async fn routing_uses_workflow_only_for_scheduler_and_domain_signals_are_delegation_hints() {
    for source in ["scheduler", "telegram", "dreaming", "new-domain"] {
        let model = Arc::new(Scripted::new(if source == "scheduler" { vec![] } else { vec![assistant("done")] }));
        let (f, mcp, workflow, supervisor) = setup(model.clone(), Stage::Success);
        let output = supervisor.run_signal(&signal(source, 42)).await.unwrap();
        assert_eq!(workflow.called.load(Ordering::SeqCst), usize::from(source == "scheduler"));
        if source == "scheduler" {
            assert_eq!(serde_json::from_str::<Value>(&output).unwrap(), json!({"ok":true,"attempts":1,"stepCount":2}));
        } else {
            let seen = model.seen.lock().unwrap().clone();
            let system = seen[0].messages[0]["content"].as_str().unwrap();
            assert!(system.contains("Europe/Chisinau"));
            assert!(system.contains("Default Telegram chat id: 999."));
            assert!(system.contains("ORCHESTRATOR"));
            assert_eq!(seen[0].model, "gemini-3.7-flash");
            assert_eq!(system.contains("TELEGRAM"), source == "telegram");
            if source != "telegram" {
                assert!(system.contains(&format!("Delegate the domain work to skill {}", json!(source))));
            }
        }
        let (trace, obs) = f.db.get_trace(&format!("{source}:42")).unwrap().unwrap();
        assert_eq!(trace.metadata.as_ref().unwrap()["activity"], "run");
        if source != "scheduler" {
            assert!(obs.iter().any(|o| o.meta("activity") == Some(&json!("primary_loop"))));
        }
        let calls = mcp.calls.lock().unwrap().clone();
        assert_eq!(calls.iter().any(|(n, _)| n == "get_telegram_chat_history"), source == "telegram");
        supervisor.shutdown().await;
    }
}
// A scheduled task naming a saved workflow runs it instead of the planner. A
// run that faulted before any write hands the task to the agent loop; one
// that faulted after a write only recovers, as a failed execution does.
#[tokio::test]
async fn a_saved_workflow_runs_without_the_planner_and_faults_hand_over_by_effect() {
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
        let (f, mcp, workflow, supervisor) = setup(model.clone(), Stage::Success);
        if !body.is_empty() {
            let saved = f.engine.deps.workflows.save("w", &format!("{HEAD}{body}"), None).unwrap();
            f.engine.deps.workflows.activate("w", saved.version).unwrap();
        }
        let scheduled = PendingSignal { payload: Some(json!({"workflow":"w"})), ..signal("scheduler", 42) };
        let result = supervisor.run_signal(&scheduled).await;
        assert_eq!(workflow.called.load(Ordering::SeqCst), 0, "{case}: the planner never runs");
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
                assert!(system.unwrap().contains("RECOVERY"), "{case}: only a report");
                assert!(mcp.calls.lock().unwrap().iter().any(|(n, _)| n == "t1"));
            }
        }
        let (_, obs) = f.db.get_trace("scheduler:42").unwrap().unwrap();
        assert_eq!(obs.iter().any(|o| o.name == "runner"), !body.is_empty(), "{case}");
        supervisor.shutdown().await;
    }
    // A payload on another source is not a workflow.
    let model = Arc::new(Scripted::new(vec![assistant("done")]));
    let (_f, _, workflow, supervisor) = setup(model, Stage::Success);
    let telegram = PendingSignal { payload: Some(json!({"workflow":"w"})), ..signal("telegram", 43) };
    assert_eq!(telegram.saved_workflow(), None);
    supervisor.run_signal(&telegram).await.unwrap();
    assert_eq!(workflow.called.load(Ordering::SeqCst), 0);
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
    let (f, mcp, workflow, _) = setup(model.clone(), Stage::Success);
    let supervisor = create_supervisor(
        f.engine.clone(),
        EnvDeps { mcp: mcp.clone(), memory: f.db.clone(), user_email: Some("user@test".into()) },
        workflow.clone(),
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
    let (f, _, workflow, supervisor) = setup(model.clone(), Stage::Success);
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
    assert_eq!(workflow.called.load(Ordering::SeqCst), 0, "no planner");
    assert_eq!(model.seen.lock().unwrap().len(), 1, "no model call for the workflow");

    supervisor.shutdown().await;
    assert_eq!(sink.fire(fire(3, "scheduler", None)).await, Fired::NotStarted);
    assert!(f.db.get_trace(&format!("scheduler:{}", FIRST_FIRE_ID + 3)).unwrap().is_none());
}
#[tokio::test]
async fn compile_and_replan_failures_fall_back_but_execute_failure_only_recovers() {
    for stage in [Stage::Compile, Stage::Replan, Stage::Execute] {
        let model = Arc::new(Scripted::new(vec![assistant("done")]));
        let (f, _, _, supervisor) = setup(model.clone(), stage);
        let result = supervisor.run_signal(&signal("scheduler", 42)).await;
        assert_eq!(result.is_err(), matches!(stage, Stage::Execute));
        let seen = model.seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 1);
        let system = seen[0].messages[0]["content"].as_str().unwrap();
        if matches!(stage, Stage::Execute) {
            assert!(system.contains("RECOVERY"));
            assert!(!system.contains("SCHEDULER"));
            assert!(!system.contains("ROUTING"));
            assert!(seen[0].messages[1]["content"].as_str().unwrap().contains("Do not repeat the original actions"));
            assert!(!seen[0].tools.iter().any(|t| t.name == "invoke_sub_agent"));
        } else {
            assert!(system.contains("SCHEDULER"));
        }
        let (trace, obs) = f.db.get_trace("scheduler:42").unwrap().unwrap();
        assert_eq!(trace.session_id.as_deref(), Some("scheduler:42"));
        assert_eq!(obs.iter().filter(|o| o.name == "recovery").count(), usize::from(matches!(stage, Stage::Execute)));
        supervisor.shutdown().await;
    }
}
#[tokio::test]
async fn history_scopes_chat_topic_and_time_excludes_current_and_keeps_memory() {
    let mcp = ContextMcp::default();
    mcp.history.lock().unwrap().extend([
        history(1, "Other chat", "assistant", 999, Some(7), "2026-09-06 11:59:00"),
        history(2, "Other topic", "assistant", -10042, Some(8), "2026-09-06 11:59:00"),
        history(3, "question", "user", -10042, Some(7), "2026-09-06 11:59:00"),
        history(4, "offer", "assistant", -10042, Some(7), "2026-09-06 11:59:30"),
        history(5, "давай", "user", -10042, Some(7), "2026-09-06 12:00:00"),
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
    signal.content = "Quoted: Telegram message in chat 999.\nText: \"давай\"".into();
    assert!(
        prepare_telegram_input(&signal, &context(), &mcp, &null_scope(), &CancellationToken::new())
            .await
            .unwrap()
            .contains("unavailable")
    );
    assert!(mcp.calls.lock().unwrap().is_empty());
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
    let long = "📰 Новость \"цитата\"\n".repeat(2000) + "Собрать за неделю?";
    let (inline, truncated) = inline_history(&[history(1, &long, "assistant", 42, None, "a")]);
    assert!(truncated);
    assert!(json!(inline).to_string().len() <= 8000);
    assert!(inline[0]["text"].as_str().unwrap().ends_with("Собрать за неделю?"));
    assert_eq!(inline[0]["truncated"], true);
}
#[tokio::test]
async fn recent_activity_is_bounded_oldest_first_and_stays_out_of_user_history() {
    let model = Arc::new(Scripted::new(vec![assistant("done")]));
    let (f, mcp, _, supervisor) = setup(model.clone(), Stage::Success);
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
    let (f, mcp, _, supervisor) = setup(model.clone(), Stage::Success);
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
    let (f, mcp, workflow, supervisor) = setup(model.clone(), Stage::Success);
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
    assert_eq!(workflow.called.load(Ordering::SeqCst), 0);
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
    let (f, mcp, _, supervisor) = setup(model.clone(), Stage::Success);
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
    let (_f, mcp, _, supervisor) = setup(model, Stage::Success);
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
