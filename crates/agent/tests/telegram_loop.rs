// Design 08: the built-in `telegram` wrapper around a loop. A Telegram
// signal routes to the wrapper, which opens the 👀 bubble, starts the loop
// as its owned child (`start_loop`, the delivery tools excluded, a status
// observer on every step), joins it, composes the reply from what the loop
// did and sends it through the guard, then clears the bubble. The loop does
// not know it runs inside a Telegram flow.
mod support;
use agent::{
    db::TraceStore,
    mcp::Mcp,
    providers::{ChatProvider, CompletionParams, CompletionResult, Message, ProviderKind, ToolDef, assistant},
    supervisor::*,
};
use async_trait::async_trait;
use serde_json::{Map, Value, json};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use support::*;
use tokio_util::sync::CancellationToken;

const EXCLUDED: [&str; 4] =
    ["send_telegram_message", "telegram_send_status", "start_typing", "send_telegram_chat_action"];
const STATUS_PROMPT: &str = "progress bubble";
const COMPOSE_PROMPT: &str = "Telegram message that answers";
const REPORT_PROMPT: &str = "Report the failure only";

// ── the fakes ───────────────────────────────────────────────────────────────

type Events = Arc<Mutex<Vec<String>>>;

// MCP as the wrapper and its loop see it: every call recorded, in one event
// log with the model's requests.
#[derive(Default)]
struct Tg {
    events: Events,
    calls: Mutex<Vec<(String, Map<String, Value>)>>,
    history: Mutex<Vec<Value>>,
    status_fails: AtomicBool,
    // The composed reply's send hangs and fires this: the process dies there.
    crash_on_reply: Mutex<Option<CancellationToken>>,
    // A tool call in the loop dies here: a restart mid-loop.
    crash_on_t1: Mutex<Option<CancellationToken>>,
    // Read at every `t1`: the observer runs started so far (their `seq`).
    probe: Mutex<Option<Arc<agent::agent::Engine>>>,
    observed: Mutex<Vec<(i64, Vec<i64>)>>,
}

#[async_trait]
impl Mcp for Tg {
    fn tools(&self) -> Vec<ToolDef> {
        [
            "get_timezone",
            "get_telegram_chat_history",
            "journal_tail",
            "t1",
            "send_telegram_message",
            "edit_telegram_message",
            "telegram_send_status",
            "start_typing",
            "send_telegram_chat_action",
            "list_signals",
        ]
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
        self.events.lock().unwrap().push(format!("mcp:{name}"));
        self.calls.lock().unwrap().push((name.into(), args.clone()));
        if name == "telegram_send_status" && self.status_fails.load(Ordering::SeqCst) {
            anyhow::bail!("Telegram sendMessage failed (429): Too Many Requests");
        }
        let crash = if name == "send_telegram_message" && !args.contains_key("buttons") {
            self.crash_on_reply.lock().unwrap().take()
        } else {
            None
        };
        if let Some(stop) = crash {
            stop.cancel();
            cancel.cancelled().await;
            return Err(agent::errors::aborted());
        }
        if name == "t1" {
            let crash = self.crash_on_t1.lock().unwrap().take();
            if let Some(stop) = crash {
                stop.cancel();
                cancel.cancelled().await;
                return Err(agent::errors::aborted());
            }
            let engine = self.probe.lock().unwrap().clone();
            if let Some(engine) = engine {
                let seqs =
                    observers(&engine, 42).await.iter().map(|r| r.admission.input["seq"].as_i64().unwrap()).collect();
                self.observed.lock().unwrap().push((args["n"].as_i64().unwrap_or(0), seqs));
            }
        }
        Ok(match name {
            "send_telegram_message" => json!({"delivered":true,"messageId":700}),
            "get_timezone" => json!({"timezone":"Europe/Chisinau"}),
            "get_telegram_chat_history" => json!({"messages":self.history.lock().unwrap().clone()}),
            "journal_tail" => json!({"entries":[]}),
            "list_signals" => json!({"count":1,"signals":[
                {"id":5,"source":"telegram","content":MESSAGE,"created_at":"2026-10-20 09:00:00","consumed_at":null}]}),
            "telegram_send_status" => json!({"action":"updated","id":args["id"]}),
            "t1" => json!({"t1":"ok","n":args.get("n")}),
            _ => json!({"ok":true}),
        }
        .to_string())
    }
    async fn close(&self) {}
}

// The model, by request: the status observer's line, the reply composer,
// the recovery report, else the loop's next scripted turn.
#[derive(Default)]
struct Model {
    events: Events,
    turns: Mutex<VecDeque<Message>>,
    loop_seen: Mutex<Vec<(Vec<Message>, Vec<ToolDef>)>>,
    composes: Mutex<Vec<String>>,
    statuses: Mutex<Vec<String>>,
    reports: Mutex<Vec<String>>,
    reply: Mutex<Value>,
    hang_status: AtomicBool,
}

impl Model {
    fn new(turns: Vec<Message>, reply: Value, events: Events) -> Arc<Model> {
        Arc::new(Model { events, turns: Mutex::new(turns.into()), reply: Mutex::new(reply), ..Default::default() })
    }
    fn loop_system(&self, request: usize) -> String {
        self.loop_seen.lock().unwrap()[request].0[0]["content"].as_str().unwrap().to_owned()
    }
    fn loop_tools(&self, request: usize) -> Vec<String> {
        self.loop_seen.lock().unwrap()[request].1.iter().map(|t| t.name.clone()).collect()
    }
}

#[async_trait]
impl ChatProvider for Model {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Openai
    }
    async fn complete(&self, p: &mut CompletionParams<'_>) -> anyhow::Result<CompletionResult> {
        let user = p
            .messages
            .iter()
            .rev()
            .find(|m| m["role"] == "user")
            .and_then(|m| m["content"].as_str())
            .unwrap_or_default()
            .to_owned();
        let done = |message: Message| Ok(CompletionResult { message, finish_reason: Some("stop".into()), usage: None });
        if user.contains(STATUS_PROMPT) {
            self.events.lock().unwrap().push("model:status".into());
            self.statuses.lock().unwrap().push(user);
            if self.hang_status.load(Ordering::SeqCst) {
                p.cancel.cancelled().await;
                return Err(agent::errors::aborted());
            }
            return done(assistant(&json!({"status":"🔎 смотрю"}).to_string()));
        }
        if user.contains(COMPOSE_PROMPT) {
            self.events.lock().unwrap().push("model:compose".into());
            self.composes.lock().unwrap().push(user);
            return done(assistant(&self.reply.lock().unwrap().to_string()));
        }
        if user.contains(REPORT_PROMPT) {
            self.events.lock().unwrap().push("model:report".into());
            self.reports.lock().unwrap().push(user);
            return done(assistant("Не получилось"));
        }
        self.events.lock().unwrap().push("model:turn".into());
        self.loop_seen.lock().unwrap().push((p.messages.clone(), p.tools.clone()));
        let next = self.turns.lock().unwrap().pop_front().ok_or_else(|| anyhow::anyhow!("script exhausted"))?;
        done(next)
    }
}

// ── the fixture ─────────────────────────────────────────────────────────────

const SKILLS: &[(&str, &str)] = &[
    ("telegram", "---\ntools: [t1, edit_telegram_message]\n---\nTELEGRAM"),
    ("orchestrator", "---\ntools: *\n---\nORCHESTRATOR"),
    ("routing", "---\ntools: []\n---\nROUTING"),
    ("recovery", "---\ntools: []\n---\nRECOVERY"),
    ("composer", "---\ntools: []\n---\nCOMPOSER"),
    ("narrow", "---\ntools: [t1]\n---\nNARROW"),
    ("wide", "---\ntools: *\n---\nWIDE"),
    ("worker", "---\ntools: *\n---\nWORKER"),
    ("scheduler", "---\ntools: *\n---\nSCHEDULER"),
];

const MESSAGE: &str = "Telegram message in chat -10042 (forum topic thread_id=7).\nText: \"go ahead\"";

fn signal(id: i64) -> PendingSignal {
    PendingSignal {
        id,
        source: "telegram".into(),
        content: MESSAGE.into(),
        env_context: Some("Default Telegram chat id: 999.".into()),
        created_at: "2026-09-06 12:00:00".into(),
        payload: None,
    }
}

fn configure(
    mcp: &Arc<Tg>,
    api: &Arc<GuardApi>,
    engine_skills: &[&str],
) -> impl FnOnce(&mut agent::agent::EngineDeps) + use<> {
    let (mcp, api) = (mcp.clone(), api.clone());
    let skills: Vec<String> = engine_skills.iter().map(|s| (*s).to_owned()).collect();
    move |deps| {
        deps.mcp = mcp;
        deps.skills = skills;
        deps.decisions = Some(agent::decisions::decision_client("guard-test".into(), api));
        deps.env = agent::session::EnvConfig { user_email: None, telegram_chat_id: Some("999".into()) };
    }
}

struct Setup {
    f: Fixture,
    mcp: Arc<Tg>,
    api: Arc<GuardApi>,
    model: Arc<Model>,
    supervisor: Arc<Supervisor>,
}

fn setup(turns: Vec<Message>, reply: Value) -> Setup {
    setup_with(turns, reply, "allow", &["orchestrator", "routing"])
}

fn setup_with(turns: Vec<Message>, reply: Value, label: &str, engine_skills: &[&str]) -> Setup {
    let events: Events = Default::default();
    let model = Model::new(turns, reply, events.clone());
    let mcp = Arc::new(Tg { events, ..Default::default() });
    let api = Arc::new(GuardApi::default());
    *api.label.lock().unwrap() = Some(label.into());
    let f = fixture_on(model.clone(), &[], SKILLS, configure(&mcp, &api, engine_skills));
    let supervisor = create_supervisor(f.engine.clone());
    Setup { f, mcp, api, model, supervisor }
}

fn reply(text: &str) -> Value {
    json!({"reply": text})
}

// The composed reply's sends (a confirmation card carries buttons).
fn replies(mcp: &Tg) -> Vec<Map<String, Value>> {
    mcp.calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(n, a)| n == "send_telegram_message" && !a.contains_key("buttons"))
        .map(|(_, a)| a.clone())
        .collect()
}

fn statuses(mcp: &Tg) -> Vec<Map<String, Value>> {
    mcp.calls.lock().unwrap().iter().filter(|(n, _)| n == "telegram_send_status").map(|(_, a)| a.clone()).collect()
}

fn position(mcp: &Tg, found: impl Fn(&str, &Map<String, Value>) -> bool) -> usize {
    mcp.calls.lock().unwrap().iter().position(|(n, a)| found(n, a)).expect("the call")
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

// Every child of `run`, live or ended, with its host data (the control
// API's listings redact it).
async fn children(engine: &agent::agent::Engine, run: &str) -> Vec<ouro_runtime::durable::SavedRun> {
    let live = agent::runs::Runs { parent: Some(run.into()), ..Default::default() };
    let ended = agent::runs::Runs {
        parent: Some(run.into()),
        since_ms: Some(0),
        before_ms: Some(i64::MAX / 4),
        ..Default::default()
    };
    let mut ids: Vec<String> = Vec::new();
    for child in engine.control.list(&live, None, 100).await.unwrap().items {
        ids.push(child.admission.id);
    }
    for child in engine.control.history(&ended, None, 100).await.unwrap().items {
        if !ids.contains(&child.admission.id) {
            ids.push(child.admission.id);
        }
    }
    let mut found = Vec::new();
    for id in ids {
        found.push(engine.runtime.get(&id).await.unwrap().unwrap());
    }
    found
}

async fn named(engine: &agent::agent::Engine, run: &str, name: &str) -> Vec<ouro_runtime::durable::SavedRun> {
    children(engine, run).await.into_iter().filter(|c| c.admission.name == name).collect()
}

// The wrapper's run and its loop child.
async fn wrapper_and_loop(
    engine: &agent::agent::Engine,
    signal: i64,
) -> (ouro_runtime::durable::SavedRun, ouro_runtime::durable::SavedRun) {
    let wrapper = named(engine, &format!("signal:{signal}"), "telegram").await.pop().expect("the wrapper");
    let inner = children(engine, &wrapper.admission.id)
        .await
        .into_iter()
        .find(|c| agent::runs::host_kind(c.admission.host()) == Some(agent::runs::PRIMARY))
        .expect("the wrapper's loop");
    (wrapper, inner)
}

// The status observers of signal `signal`'s loop.
async fn observers(engine: &agent::agent::Engine, signal: i64) -> Vec<ouro_runtime::durable::SavedRun> {
    let Some(wrapper) = named(engine, &format!("signal:{signal}"), "telegram").await.pop() else { return Vec::new() };
    let mut found = Vec::new();
    for inner in children(engine, &wrapper.admission.id).await {
        found.extend(named(engine, &inner.admission.id, "telegram-status").await);
    }
    found
}

// ── 1, 2, 5: the route, the bubble, the composed reply ─────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_telegram_signal_runs_the_wrapper_its_loop_and_sends_the_composed_reply_once() {
    let s = setup(
        vec![calls(&[("t1", json!({"n":1}))]), assistant("Did t1; tell the user it worked")],
        reply("Готово: t1"),
    );
    s.mcp.history.lock().unwrap().push(
        json!({"id":100,"chat_id":-10042,"thread_id":7,"role":"user","text":"Earlier message","created_at":"2026-09-06 11:59:00"}),
    );
    assert_eq!(s.supervisor.run_signal(&signal(42)).await.unwrap(), "Готово: t1");

    // 1: the route runs the wrapper, and the loop as its child.
    let routes = agent::flows::signal_routes(&s.f.engine, "telegram", &Value::Null).await.unwrap();
    assert_eq!(routes[0].0, "telegram");
    let (wrapper, inner) = wrapper_and_loop(&s.f.engine, 42).await;
    assert_eq!(inner.admission.name, "agent-loop");
    assert_eq!(wrapper.outcome, Some(agent::runs::Status::Completed(json!("Готово: t1"))));
    let host = inner.admission.host().unwrap();
    let conversation = json!({"chatId":"-10042","messageThreadId":7});
    assert_eq!(host["conversation"], conversation, "inherited from the wrapper's host data");
    assert_eq!(host["confirm"], conversation);
    assert_eq!(host["signal_id"], 42);
    assert_eq!(host["preset"], "smart");
    let mut excluded: Vec<&str> = EXCLUDED.to_vec();
    excluded.sort();
    assert_eq!(host["exclude_tools"], json!(excluded));

    // 3: the loop is offered everything but the four.
    let tools = s.model.loop_tools(0);
    for name in EXCLUDED {
        assert!(!tools.iter().any(|t| t == name), "{name} offered: {tools:?}");
    }
    assert!(tools.iter().any(|t| t == "t1") && tools.iter().any(|t| t == "edit_telegram_message"));

    // The loop's instructions: the wrapper's context, then its own lines.
    let system = s.model.loop_system(0);
    for part in [
        "Signal source: telegram",
        "Default Telegram chat id: 999.",
        "Europe/Chisinau",
        "TELEGRAM",
        "ORCHESTRATOR",
        "written and sent for you after you finish",
    ] {
        assert!(system.contains(part), "{part}: {system}");
    }
    let first = s.model.loop_seen.lock().unwrap()[0].0[1]["content"].as_str().unwrap().to_owned();
    assert!(first.contains("Earlier message") && first.contains("go ahead"), "{first}");

    // 5: compose reads the history, the work (no instructions) and the result.
    let composes = s.model.composes.lock().unwrap().clone();
    assert_eq!(composes.len(), 1);
    let compose = &composes[0];
    for part in
        ["<history>", "Earlier message", "go ahead", "<work>", "t1", "<result>", "Did t1; tell the user it worked"]
    {
        assert!(compose.contains(part), "{part}: {compose}");
    }
    assert!(!compose.contains("ORCHESTRATOR") && !compose.contains("TELEGRAM\n"), "no system text: {compose}");

    // Sent once, to the conversation's chat and thread, then the bubble goes.
    let sent = replies(&s.mcp);
    assert_eq!(sent.len(), 1, "{sent:?}");
    assert_eq!(sent[0]["chatId"], "-10042");
    assert_eq!(sent[0]["messageThreadId"], 7);
    assert_eq!(sent[0]["text"], "Готово: t1");
    let bubbles = statuses(&s.mcp);
    assert_eq!(bubbles[0]["text"], "👀");
    assert_eq!(bubbles[0]["seq"], 0);
    assert_eq!(bubbles[0]["id"], "status:42");
    let last = bubbles.last().unwrap();
    assert_eq!((last["text"].clone(), last["seq"].clone()), (json!(""), json!(1_000_000)));
    let send_at = position(&s.mcp, |n, a| n == "send_telegram_message" && !a.contains_key("buttons"));
    let clear_at = position(&s.mcp, |n, a| n == "telegram_send_status" && a["text"] == "");
    assert!(send_at < clear_at, "the bubble is cleared after the reply");

    // The trace and the journal show what the user got, and the loop's skills.
    let (trace, observations) = s.f.db.get_trace("telegram:42").unwrap().unwrap();
    assert_eq!(trace.output, "Готово: t1");
    let skills = trace.metadata.as_ref().unwrap()["skills"].clone();
    assert!(skills.as_array().unwrap().contains(&json!("telegram")), "{skills}");
    let packet = agent::activity::project_trace(&trace, &observations, None, &Default::default());
    assert_eq!(packet["final"], "Готово: t1", "{packet:#}");
    assert!(packet["skills"].as_array().unwrap().contains(&json!("telegram")));
    let actions = packet["actions"].to_string();
    assert!(!actions.contains("telegram_send_status"), "observers stay out of the journal: {actions}");
    s.supervisor.shutdown().await;
}

// 2: the bubble opens before any model request and before the history read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_bubble_is_the_first_telegram_call_before_any_model_request() {
    let s = setup(vec![assistant("nothing to do")], reply("ok"));
    s.supervisor.run_signal(&signal(42)).await.unwrap();
    let events = s.mcp.events.lock().unwrap().clone();
    let bubble = events.iter().position(|e| e == "mcp:telegram_send_status").expect("the bubble");
    let model = events.iter().position(|e| e.starts_with("model:")).expect("a model request");
    let history = events.iter().position(|e| e == "mcp:get_telegram_chat_history").expect("the history read");
    assert!(bubble < model && bubble < history, "{events:?}");
    assert_eq!(statuses(&s.mcp)[0]["text"], "👀");
    s.supervisor.shutdown().await;
}

// ── 3: exclude_tools ────────────────────────────────────────────────────────

// A model call of an excluded tool is refused and never runs; a worker the
// loop starts inherits the exclusion, so only the wrapper sends the reply.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_excluded_tool_never_runs_and_workers_inherit_the_exclusion() {
    let s = setup(
        vec![
            calls(&[("send_telegram_message", json!({"chatId":"-10042","messageThreadId":7,"text":"by the loop"}))]),
            calls(&[("invoke_sub_agent", json!({"prompt":"send the answer to the chat"}))]),
            assistant("worker done"),
            assistant("all done"),
        ],
        reply("the one reply"),
    );
    assert_eq!(s.supervisor.run_signal(&signal(42)).await.unwrap(), "the one reply");
    let sent = replies(&s.mcp);
    assert_eq!(sent.len(), 1, "{sent:?}");
    assert_eq!(sent[0]["text"], "the one reply");
    let seen = s.model.loop_seen.lock().unwrap().clone();
    let refused = serde_json::to_string(&seen[1].0).unwrap();
    assert!(refused.contains("send_telegram_message is unavailable in this loop"), "{refused}");
    // The worker's request (the third loop request) offers no delivery tool.
    let worker_tools: Vec<_> = seen[2].1.iter().map(|t| t.name.as_str()).collect();
    assert!(seen[2].0[0]["content"].as_str().unwrap().contains("focused worker"));
    for name in EXCLUDED {
        assert!(!worker_tools.contains(&name), "{name}: {worker_tools:?}");
    }
    s.supervisor.shutdown().await;
}

fn probe_route(name: &str, source: &str, with: &str) -> String {
    format!(
        r#"document: {{dsl: '1.0.4', namespace: agent, name: {name}, version: '1.0.0', metadata: {{route: {{when: {{source: {source}}}}}}}}}
do:
  - loop:
      call: start_loop
      with: {with}
      export: {{ as: '${{ $context + {{ loop: . }} }}' }}
  - wait:
      listen: {{ to: {{ one: {{ with: {{ type: '${{ $context.loop.done }}' }} }} }} }}
  - outcome:
      call: loop_outcome
      with: {{ ref: '${{ $context.loop }}' }}
      export: {{ as: '${{ $context + {{ outcome: . }} }}' }}
  - transcript:
      call: loop_transcript
      with: {{ ref: '${{ $context.loop }}' }}
      export: {{ as: '${{ $context + {{ work: . }} }}' }}
  - answer:
      set: '${{ {{ output: ($context.outcome.status + "|" + ($context.outcome.output // "") + "|" + $context.work) }} }}'
output: {{ as: '${{ .output }}' }}
"#
    )
}

fn probe_signal(source: &str, id: i64) -> PendingSignal {
    PendingSignal {
        id,
        source: source.into(),
        content: "probe the loop".into(),
        env_context: None,
        created_at: "2026-09-06 12:00:00".into(),
        payload: None,
    }
}

// The maintainer's requirement: a wrapper starts any named loop, an agent's
// own (a copy of agent-loop) included, and reads its outcome and transcript
// back. The exclusion narrows, never widens: a skill that allows only t1
// keeps edit_telegram_message out without naming it, and a `tools: *` skill
// loaded by the loop's setup brings everything back but the excluded name.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wrapper_starts_a_custom_loop_and_reads_its_outcome_and_transcript() {
    let s = setup_with(
        vec![calls(&[("t1", json!({"n":7}))]), assistant("custom done"), assistant("setup done")],
        reply("unused"),
        "allow",
        &[],
    );
    activate_definition(&s.f.engine, "research-loop", &variant("research-loop")).await;
    let setup_loop = variant("setup-loop").replacen(
        "\ndo:\n",
        "\ndo:\n  - setup:\n      call: loop_setup\n      with: { skills: [wide] }\n      export: { as: '${ $context + { messages: .messages, memory: .memory } }' }\n",
        1,
    );
    activate_definition(&s.f.engine, "setup-loop", &setup_loop).await;
    let narrow = r#"{ input: '${ $context.signal.content }', loop: research-loop, skills: [narrow], preset: base, exclude_tools: [send_telegram_message], system: [PROBE LINE] }"#;
    activate_definition(&s.f.engine, "probe-a", &probe_route("probe-a", "probe-a", narrow)).await;
    let wide = r#"{ input: '${ $context.signal.content }', loop: setup-loop, skills: [narrow], exclude_tools: [edit_telegram_message] }"#;
    activate_definition(&s.f.engine, "probe-b", &probe_route("probe-b", "probe-b", wide)).await;

    let output = s.supervisor.run_signal(&probe_signal("probe-a", 50)).await.unwrap();
    let (status, rest) = output.split_once('|').unwrap();
    let (answer, work) = rest.split_once('|').unwrap();
    assert_eq!((status, answer), ("completed", "custom done"));
    assert!(work.contains("t1") && work.contains("probe the loop"), "{work}");
    assert!(!work.contains("NARROW") && !work.contains("PROBE LINE"), "no instructions in the transcript: {work}");
    let route = named(&s.f.engine, "signal:50", "probe-a").await.pop().unwrap();
    let inner = children(&s.f.engine, &route.admission.id).await;
    assert_eq!(inner.iter().map(|c| c.admission.name.as_str()).collect::<Vec<_>>(), ["research-loop"]);
    assert!(s.model.loop_system(0).contains("PROBE LINE"));
    let tools = s.model.loop_tools(0);
    assert!(tools.contains(&"t1".to_owned()));
    assert!(!tools.contains(&"edit_telegram_message".to_owned()), "not added by the exclusion of another: {tools:?}");

    let output = s.supervisor.run_signal(&probe_signal("probe-b", 51)).await.unwrap();
    assert!(output.starts_with("completed|setup done|"), "{output}");
    let tools = s.model.loop_tools(2);
    assert!(tools.contains(&"send_telegram_message".to_owned()), "the tools: * skill widened: {tools:?}");
    assert!(!tools.contains(&"edit_telegram_message".to_owned()), "the exclusion survived the setup: {tools:?}");
    s.supervisor.shutdown().await;
}

// A name that is no tool fails the start: a typo must not leave the tool in.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unknown_excluded_name_fails_the_start() {
    let s = setup_with(vec![], reply("unused"), "allow", &[]);
    let typo = r#"{ input: x, exclude_tools: [send_telegram_mesage] }"#;
    activate_definition(&s.f.engine, "probe-c", &probe_route("probe-c", "probe-c", typo)).await;
    let error = s.supervisor.run_signal(&probe_signal("probe-c", 52)).await.unwrap_err();
    assert!(format!("{error:#}").contains("send_telegram_mesage"), "{error:#}");
    assert!(s.model.loop_seen.lock().unwrap().is_empty(), "no loop ran");
    s.supervisor.shutdown().await;
}

// The exclusion is host data: a loop rebuilt after a restart keeps it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_exclusion_survives_a_restart_mid_loop() {
    let s = setup(vec![calls(&[("t1", json!({"n":1}))])], reply("after the restart"));
    let stop = CancellationToken::new();
    *s.mcp.crash_on_t1.lock().unwrap() = Some(stop.clone());
    let supervisor = s.supervisor.clone();
    let first = tokio::spawn(async move { supervisor.run_signal(&signal(42)).await });
    stop.cancelled().await;
    s.supervisor.shutdown().await;
    let _ = first.await;

    s.model.turns.lock().unwrap().push_back(assistant("resumed"));
    let restarted = reopen(
        &s.f,
        s.model.clone(),
        Arc::new(ouro_runtime::durable::SystemClock),
        configure(&s.mcp, &s.api, &["orchestrator", "routing"]),
    );
    restarted.ready().await.unwrap();
    assert_eq!(ended(&restarted, "signal:42").await, agent::runs::Status::Completed(json!("after the restart")));
    let tools = s.model.loop_tools(1);
    assert!(tools.contains(&"t1".to_owned()));
    for name in EXCLUDED {
        assert!(!tools.contains(&name.to_owned()), "{name} came back after the restart: {tools:?}");
    }
    assert_eq!(replies(&s.mcp).len(), 1);
    restarted.shutdown().await;
}

// ── 4: on_step ──────────────────────────────────────────────────────────────

// Each acting turn starts its observer before the turn's calls run; the
// observer's line goes to the bubble with the turn's number as its `seq`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_acting_turn_starts_an_observer_before_its_calls_run() {
    let s = setup(
        vec![calls(&[("t1", json!({"n":1}))]), calls(&[("t1", json!({"n":2}))]), assistant("two steps")],
        reply("done"),
    );
    *s.mcp.probe.lock().unwrap() = Some(s.f.engine.clone());
    s.supervisor.run_signal(&signal(42)).await.unwrap();
    let observed = s.mcp.observed.lock().unwrap().clone();
    assert_eq!(observed.len(), 2);
    assert!(observed[0].1.contains(&1), "{observed:?}");
    assert!(observed[1].1.contains(&1) && observed[1].1.contains(&2), "{observed:?}");
    let runs = observers(&s.f.engine, 42).await;
    let mut seqs: Vec<i64> = runs.iter().map(|r| r.admission.input["seq"].as_i64().unwrap()).collect();
    seqs.sort();
    assert_eq!(seqs, [1, 2]);
    for run in &runs {
        assert_eq!(run.admission.input["id"], "status:42");
        assert_eq!(run.admission.input["step"]["calls"][0]["name"], "t1");
        assert!(run.admission.host().is_none(), "an observer runs no agent turns");
    }
    // Their lines reached MCP with their seq, or were cancelled with the loop.
    for line in statuses(&s.mcp).iter().filter(|a| a["text"] == "🔎 смотрю") {
        assert!(line["seq"] == 1 || line["seq"] == 2, "{line:?}");
    }
    s.supervisor.shutdown().await;
}

// A status model that never answers delays nothing: the reply goes out and
// the hanging observers are cancelled when the loop ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hanging_status_model_delays_nothing() {
    let s = setup(vec![calls(&[("t1", json!({"n":1}))]), assistant("done fast")], reply("fast reply"));
    s.model.hang_status.store(true, Ordering::SeqCst);
    let started = std::time::Instant::now();
    assert_eq!(s.supervisor.run_signal(&signal(42)).await.unwrap(), "fast reply");
    assert!(started.elapsed() < std::time::Duration::from_secs(15), "{:?}", started.elapsed());
    let runs = observers(&s.f.engine, 42).await;
    assert_eq!(runs.len(), 1);
    let ended = ended(&s.f.engine, &runs[0].admission.id).await;
    assert_eq!(ended, agent::runs::Status::Cancelled, "cancelled with the loop");
    s.supervisor.shutdown().await;
}

// Without on_step (the default path) nothing observes; an observer that
// does not exist or that an operator paused is ignored, and the loop ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_an_observer_or_with_a_broken_one_the_loop_completes() {
    let s = setup(vec![calls(&[("t1", json!({"n":1}))]), assistant("scheduled done")], reply("unused"));
    let mut scheduled = signal(60);
    scheduled.source = "scheduler".into();
    scheduled.content = "do the task".into();
    assert_eq!(s.supervisor.run_signal(&scheduled).await.unwrap(), "scheduled done");
    let primary = children(&s.f.engine, "signal:60").await.pop().unwrap();
    assert!(children(&s.f.engine, &primary.admission.id).await.iter().all(|c| c.admission.name != "telegram-status"));

    let missing = agent::flows::built_in("telegram")
        .unwrap()
        .replace("workflow: \"telegram-status\"", "workflow: \"no-such-observer\"")
        .replace("name: telegram,", "name: telegram-missing,")
        .replace("priority: -1", "priority: 1");
    assert!(missing.contains("no-such-observer"));
    activate_definition(&s.f.engine, "telegram-missing", &without_breaker(&missing)).await;
    s.model.turns.lock().unwrap().extend([calls(&[("t1", json!({"n":2}))]), assistant("loop done")]);
    *s.model.reply.lock().unwrap() = reply("despite the observer");
    assert_eq!(s.supervisor.run_signal(&signal(61)).await.unwrap(), "despite the observer");

    let request =
        ouro_runtime::control::Request { id: "pause-status".into(), actor: "operator".into(), reason: "test".into() };
    s.f.engine.control.pause(&request, "telegram-status", "maintenance").await.unwrap();
    s.model.turns.lock().unwrap().extend([calls(&[("t1", json!({"n":3}))]), assistant("loop done")]);
    *s.model.reply.lock().unwrap() = reply("despite the pause");
    assert_eq!(s.supervisor.run_signal(&signal(62)).await.unwrap(), "despite the pause");
    s.supervisor.shutdown().await;
}

// ── 6, 7, 8: no reply, a failed loop, a crash after the send ──────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_null_reply_sends_nothing_and_clears_the_bubble() {
    let s = setup(vec![assistant("nothing to say")], json!({"reply": null}));
    assert_eq!(s.supervisor.run_signal(&signal(42)).await.unwrap(), "nothing to say");
    assert!(replies(&s.mcp).is_empty());
    assert_eq!(statuses(&s.mcp).last().unwrap()["text"], "");
    s.supervisor.shutdown().await;
}

// The loop faults after a round: the bubble goes, and the recovery report
// sees the loop's work (the wrapper took no turn).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_loop_clears_the_bubble_and_the_report_sees_its_work() {
    let s = setup(vec![calls(&[("t1", json!({"n":"loop transcript marker"}))])], reply("unused"));
    let error = s.supervisor.run_signal(&signal(42)).await.unwrap_err();
    assert!(format!("{error:#}").contains("script exhausted"), "{error:#}");
    assert!(replies(&s.mcp).is_empty());
    assert_eq!(statuses(&s.mcp).last().unwrap()["text"], "");
    let reports = s.model.reports.lock().unwrap().clone();
    assert_eq!(reports.len(), 1);
    assert!(reports[0].contains("loop transcript marker"), "{}", reports[0]);
    s.supervisor.shutdown().await;
}

// A status tool that fails never fails the signal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_bubble_never_fails_the_signal() {
    let s = setup(vec![calls(&[("t1", json!({"n":1}))]), assistant("done")], reply("sent anyway"));
    s.mcp.status_fails.store(true, Ordering::SeqCst);
    assert_eq!(s.supervisor.run_signal(&signal(42)).await.unwrap(), "sent anyway");
    assert_eq!(replies(&s.mcp).len(), 1);
    s.supervisor.shutdown().await;
}

// The process dies in the reply's send: the next one never sends it again
// (its outcome is unknown, so it counts as sent) and clears the bubble.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_in_the_send_never_sends_twice_and_the_bubble_is_cleared_on_resume() {
    let s = setup(vec![assistant("work done")], reply("the reply"));
    let stop = CancellationToken::new();
    *s.mcp.crash_on_reply.lock().unwrap() = Some(stop.clone());
    let supervisor = s.supervisor.clone();
    let first = tokio::spawn(async move { supervisor.run_signal(&signal(42)).await });
    stop.cancelled().await;
    s.supervisor.shutdown().await;
    let _ = first.await;
    let cleared_before = statuses(&s.mcp).iter().filter(|a| a["text"] == "").count();
    assert_eq!(cleared_before, 0);

    let restarted = reopen(
        &s.f,
        s.model.clone(),
        Arc::new(ouro_runtime::durable::SystemClock),
        configure(&s.mcp, &s.api, &["orchestrator", "routing"]),
    );
    restarted.ready().await.unwrap();
    assert_eq!(ended(&restarted, "signal:42").await, agent::runs::Status::Completed(json!("the reply")));
    assert_eq!(replies(&s.mcp).len(), 1, "never sent again");
    assert_eq!(statuses(&s.mcp).last().unwrap()["text"], "", "cleared on resume");
    restarted.shutdown().await;
}

// ── 9: a confirmation inside the loop ───────────────────────────────────────

// A guarded write waits for the user's press: the wrapper sleeps with its
// loop (the bubble goes while it sleeps); after the press the loop goes on
// and the reply is composed and sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_confirmation_in_the_loop_lets_the_wrapper_sleep_and_resume() {
    let s = setup_with(
        vec![calls(&[("t1", json!({"n":1}))]), assistant("confirmed and done")],
        reply("after the press"),
        "needs_confirmation",
        &["orchestrator", "routing"],
    );
    let output = s.supervisor.run_signal(&signal(42)).await.unwrap();
    assert!(output.contains("resumes"), "{output}");
    let card = s
        .mcp
        .calls
        .lock()
        .unwrap()
        .iter()
        .find(|(n, a)| n == "send_telegram_message" && a.contains_key("buttons"))
        .expect("a confirmation card")
        .1
        .clone();
    assert!(replies(&s.mcp).is_empty());
    assert_eq!(statuses(&s.mcp).last().unwrap()["text"], "", "no frozen bubble above the card");

    let mut press = signal(44);
    press.source = "telegram_button".into();
    press.content = json!({"chatId":-10042,"messageThreadId":7,"messageId":700,"data":card["buttons"][0][0]["data"],"text":"Confirm"}).to_string();
    assert_eq!(s.supervisor.run_signal(&press).await.unwrap(), "Button delivered");
    assert_eq!(ended(&s.f.engine, "signal:42").await, agent::runs::Status::Completed(json!("after the press")));
    let sent = replies(&s.mcp);
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0]["text"], "after the press");
    assert_eq!(s.mcp.calls.lock().unwrap().iter().filter(|(n, _)| n == "t1").count(), 1);
    s.supervisor.shutdown().await;
}

// ── review: the guard, button notes, an agent's copy ───────────────────────

// The reply is model text from the loop's tool results: it passes the guard
// the model's own reply passes. A secret never leaves; a link is reviewed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_composed_reply_goes_through_the_guard() {
    let secret = "the key is sk-abcdefghijklmnopqrstuvwxyz012345";
    let s = setup(vec![assistant("found a key")], reply(secret));
    let output = s.supervisor.run_signal(&signal(42)).await.unwrap();
    assert!(output.contains("blocked"), "{output}");
    assert!(replies(&s.mcp).is_empty(), "a secret is never sent");
    assert_eq!(statuses(&s.mcp).last().unwrap()["text"], "");
    s.supervisor.shutdown().await;

    let link = "read https://example.com/post";
    let s = setup(vec![assistant("found a post")], reply(link));
    assert_eq!(s.supervisor.run_signal(&signal(42)).await.unwrap(), link);
    assert_eq!(replies(&s.mcp).len(), 1);
    let reviewed = s.api.requests.lock().unwrap().iter().any(|r| r.to_string().contains("example.com/post"));
    assert!(reviewed, "a link goes to the guard's review");
    s.supervisor.shutdown().await;
}

// A press that finds no waiting card reaches the wrapper; its loop is told
// the press changed nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_undelivered_press_tells_the_loop_it_changed_nothing() {
    let s = setup(vec![assistant("noted")], json!({"reply": null}));
    let mut press = signal(45);
    press.source = "telegram_button".into();
    press.content =
        json!({"chatId":-10042,"messageThreadId":7,"messageId":700,"data":"no-such-card","text":"Confirm"}).to_string();
    s.supervisor.run_signal(&press).await.unwrap();
    let system = s.model.loop_system(0);
    assert!(system.contains("No confirmation card matches the pressed button"), "{system}");
    s.supervisor.shutdown().await;
}

// An agent's copy of the wrapper passes the gate and a dry run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_agent_copy_of_the_wrapper_passes_the_gate_and_a_dry_run() {
    let s = setup(vec![calls(&[("t1", json!({"n":1}))]), assistant("dry done")], reply("dry reply"));
    s.f.engine.ready().await.unwrap();
    let copy = without_breaker(agent::flows::built_in("telegram").unwrap());
    assert!(!copy.contains("breaker"));
    agent::flows::gate(&s.f.engine, "telegram", &copy).unwrap_or_else(|e| panic!("{e:#}"));
    let args = json!({"name":"telegram","definition":copy});
    agent::flows::tool(&s.f.engine, "save_workflow", args.as_object().unwrap()).await.unwrap();
    let trace = s.f.engine.deps.tracer.trace(agent::tracing::TraceStart {
        id: "author".into(),
        name: "author".into(),
        ..Default::default()
    });
    let scope: agent::tracing::Scope = trace.clone();
    let args = json!({"name":"telegram","signal":{"source":"telegram","content":MESSAGE}});
    let report = agent::sandbox::try_signal(
        &s.f.engine,
        args.as_object().unwrap(),
        &context(),
        &scope,
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    let report: Value = serde_json::from_str(&report).unwrap();
    assert_eq!(report["status"], "completed", "{report:#}");
    assert_eq!(report["output"], "dry reply", "{report:#}");
    assert!(replies(&s.mcp).is_empty(), "a dry run's writes are mocked");
    s.supervisor.shutdown().await;
}
