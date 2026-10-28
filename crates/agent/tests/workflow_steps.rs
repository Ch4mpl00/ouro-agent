// A workflow's own steps on the engine (functions.rs §2 and §8): MCP tools
// called straight from a definition, with their outcomes typed by how far
// they got and never sent twice when at most once; `llm_compose` and
// `choose`; `set_memory`. Definitions are saved and started with the runtime
// itself here; the agent's authoring tools and a saved workflow's launch are
// tested in tests/flows.rs. What the parked saved-workflow suite tested of the
// steps, on engine runs.
mod support;
use agent::{
    agent::{Engine, EngineDeps, Providers, create_engine},
    db::AgentDb,
    models::{ModelPreset, Presets, ReasoningEffort},
    providers::{ToolDef, assistant},
    runs::{RunTrace, Status},
    trace_model::Observation,
    tracing::{TraceStart, Tracer},
};
use async_trait::async_trait;
use serde_json::{Map, Value, json};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use support::{AnySkill, PersistLog, Scripted, ToolMcp, TraceLog, recording};
use tokio_util::sync::CancellationToken;

// What outlives an engine: its stores, the world, the model and the trace.
#[derive(Clone)]
struct World {
    store: Arc<dyn ouro_runtime::durable::RunStore>,
    dry_store: Arc<dyn ouro_runtime::durable::RunStore>,
    db: AgentDb,
    mcp: Arc<ToolMcp>,
    llm: Arc<Scripted>,
    decisions: Option<Arc<agent::decisions::DecisionClient>>,
    tracer: Arc<dyn Tracer>,
    written: Arc<TraceLog>,
    persist: Arc<PersistLog>,
}

fn world(replies: Vec<&str>) -> World {
    world_with(replies, None)
}

fn world_with(replies: Vec<&str>, decisions: Option<Arc<agent::decisions::DecisionClient>>) -> World {
    let schema = |properties: &[&str], required: &[&str]| {
        let properties: Map<String, Value> = properties.iter().map(|p| (p.to_string(), json!({}))).collect();
        json!({"type": "object", "properties": properties, "required": required})
    };
    let defs = vec![
        ToolDef::new("search_news", "search", schema(&["query", "k"], &[])),
        ToolDef::new("send_telegram_message", "send", schema(&["chatId", "text"], &["text"])),
        ToolDef::new("get_next_signal", "pop", schema(&[], &[])),
    ];
    let mcp = ToolMcp::returning(defs, "");
    mcp.always(Arc::new(|name, args, _| {
        let out = match name.as_str() {
            "search_news" => json!({"items": [{"title": "Rust 2.0", "q": args.get("query")}]}).to_string(),
            "send_telegram_message" => json!({"ok": true, "text": args.get("text")}).to_string(),
            other => format!("[tool error] {other} is not here"),
        };
        Box::pin(async move { Ok(out) })
    }));
    let (tracer, written) = recording();
    let (store, dry_store) = agent::runs::memory_store_with_view();
    World {
        store,
        dry_store,
        db: AgentDb::open(":memory:").unwrap(),
        mcp,
        llm: Arc::new(Scripted::new(replies.into_iter().map(assistant).collect())),
        decisions,
        tracer,
        written,
        persist: Arc::new(PersistLog::default()),
    }
}

// MCP marks search_news read-only: it may run again; a send at most once.
struct Marked(Arc<ToolMcp>);

#[async_trait]
impl agent::mcp::Mcp for Marked {
    fn tools(&self) -> Vec<ToolDef> {
        self.0.tools()
    }
    fn read_only(&self, name: &str) -> bool {
        name == "search_news"
    }
    fn open_world(&self, _: &str) -> bool {
        false
    }
    async fn call_tool(
        &self,
        name: &str,
        args: Map<String, Value>,
        cancel: &CancellationToken,
    ) -> anyhow::Result<String> {
        self.0.call_tool(name, args, cancel).await
    }
}

impl World {
    fn engine(&self) -> Arc<Engine> {
        let p = |model: &str| ModelPreset { model: model.into(), reasoning_effort: ReasoningEffort::Disabled };
        create_engine(EngineDeps {
            providers: Providers {
                openai: self.llm.clone(),
                gemini: self.llm.clone(),
                deepseek: self.llm.clone(),
                openrouter: None,
            },
            mcp: Arc::new(Marked(self.mcp.clone())),
            presets: Presets {
                instant: p("gpt-5.4-mini"),
                base: p("gpt-5.4-mini"),
                smart: p("gpt-5.4"),
                smartest: p("gpt-5.4"),
                compiler: p("gpt-5.4"),
                compose: p("gpt-5.4"),
            },
            skills: Vec::new(),
            skill_store: Arc::new(AnySkill),
            memory: self.persist.clone(),
            tracer: self.tracer.clone(),
            decisions: self.decisions.clone(),
            env: Default::default(),
            db: self.db.clone(),
            store: self.store.clone(),
            dry_store: self.dry_store.clone(),
            schedule: Arc::new(agent::schedule::Schedules::in_memory()),
        })
    }

    // Saves and activates `definition` under `name`, then starts run `id` of
    // it with `input`, traced under a trace of its own.
    async fn start(&self, engine: &Arc<Engine>, name: &str, definition: &str, id: &str, input: Value) {
        engine.ready().await.unwrap();
        let version = engine.runtime.save(name, definition).await.unwrap().version;
        engine.runtime.activate(name, version).await.unwrap();
        let trace = self.tracer.trace(TraceStart { id: format!("trace:{id}"), name: "t".into(), ..Default::default() });
        let anchor = trace.anchor().unwrap();
        trace.end();
        self.db
            .insert_run_trace(&RunTrace {
                run_id: id.into(),
                trace_id: anchor.trace_id,
                session_id: None,
                parent_span_id: Some(anchor.parent_span_id),
                recorded: false,
                created_ms: agent::runs::now_ms(),
            })
            .unwrap();
        engine.runtime.start(id, name, input).await.unwrap();
    }

    async fn run(&self, name: &str, definition: &str) -> (Status, Vec<Observation>) {
        let engine = self.engine();
        self.start(&engine, name, definition, "run-7", input()).await;
        let status = ended(&engine, "run-7").await;
        engine.shutdown().await;
        (status, self.observations())
    }

    // Every observation of every trace segment written so far.
    fn observations(&self) -> Vec<Observation> {
        self.written.all().into_iter().flat_map(|t| t.observations).collect()
    }

    fn calls(&self) -> Vec<(String, Map<String, Value>)> {
        self.mcp.calls.lock().unwrap().clone()
    }

    fn sends(&self) -> usize {
        self.calls().iter().filter(|(name, _)| name == "send_telegram_message").count()
    }
}

fn input() -> Value {
    json!({"env": {"now": "2026-09-06T12:00:00.000Z", "telegramChatId": "4242"}})
}

async fn ended(engine: &Engine, run: &str) -> Status {
    tokio::time::timeout(Duration::from_secs(30), agent::runs::wait_end(engine, run, &CancellationToken::new()))
        .await
        .expect("the run ends")
        .unwrap()
}

fn named<'a>(observations: &'a [Observation], name: &str) -> &'a Observation {
    observations.iter().find(|o| o.name == name).unwrap_or_else(|| {
        panic!("no observation {name} in {:?}", observations.iter().map(|o| &o.name).collect::<Vec<_>>())
    })
}

fn faulted(status: &Status) -> &ouro_runtime::ows::run::WorkflowError {
    match status {
        Status::Faulted(error) => error,
        other => panic!("not faulted: {other:?}"),
    }
}

const DIGEST: &str = r#"
document: { dsl: '1.0.4', namespace: agent, name: digest, version: '1.0.0' }
do:
  - search:
      call: search_news
      with: { query: '${ "rust since " + (.env.newsLastReadAt // "ever") }' }
  - compose:
      call: llm_compose
      with: { skill: news-digest, prompt: Write the digest., input: { items: '${ .items }' } }
  - send:
      call: send_telegram_message
      with: { text: '${ . }' }
  - mark:
      call: set_memory
      with: { key: news_digest.last_read_at, value: '${ $workflow.input.env.now }' }
"#;

// ── tools and model steps ───────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_workflow_runs_tools_and_model_steps_and_traces_each_call() {
    let w = world(vec!["Today: Rust 2.0"]);
    let (status, observations) = w.run("digest", DIGEST).await;
    assert_eq!(status, Status::Completed(json!({"ok": true, "key": "news_digest.last_read_at"})));
    let calls = w.calls();
    assert_eq!(calls.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(), ["search_news", "send_telegram_message"]);
    assert_eq!(calls[0].1["query"], "rust since ever");
    assert_eq!(calls[1].1["text"], "Today: Rust 2.0");
    assert_eq!(*w.persist.0.lock().unwrap(), [("news_digest.last_read_at".into(), "2026-09-06T12:00:00.000Z".into())]);
    let call = named(&observations, "search_news");
    assert_eq!(call.meta("tool"), Some(&json!("search_news")));
    assert_eq!(call.meta("reference"), Some(&json!("/do/0/search")));
    assert_eq!(call.meta("workflow_run"), Some(&json!("run-7")));
    assert_eq!(call.input, json!({"query": "rust since ever"}));
    // The compose generation sits under its task's span and is tagged for
    // the judge.
    let compose = named(&observations, "call:compose");
    let generation = named(&observations, "llm_compose:compose");
    assert_eq!(generation.parent_observation_id.as_deref(), Some(compose.id.as_str()));
    assert_eq!(generation.metadata.as_ref().unwrap()["judge_node"], "compose");
    assert_eq!(generation.metadata.as_ref().unwrap()["skill"], "news-digest");
    let prompt = generation.input.to_string();
    assert!(prompt.contains("Write the digest.") && prompt.contains("<items>") && prompt.contains("Rust 2.0"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_workflow_sends_to_the_chat_its_input_names() {
    let w = world(vec![]);
    let ping = r#"document: { dsl: '1.0.4', namespace: t, name: ping, version: '0.1.0' }
do: [ { send: { call: send_telegram_message, with: { chatId: '${ $workflow.input.env.telegramChatId }', text: ping } } } ]
"#;
    let (status, _) = w.run("ping", ping).await;
    assert!(matches!(status, Status::Completed(_)), "{status:?}");
    assert_eq!(
        w.calls(),
        [("send_telegram_message".to_string(), json!({"chatId":"4242","text":"ping"}).as_object().unwrap().clone())]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fault_before_any_write_sends_nothing_and_one_after_keeps_what_ran() {
    let w = world(Vec::new());
    let early =
        DIGEST.replace("call: search_news", "call: search_news\n      output: { as: '${ error(\"no news\") }' }");
    let (status, observations) = w.run("digest", &early).await;
    let error = faulted(&status);
    assert_eq!(error.instance.as_deref(), Some("/do/0/search"));
    assert!(error.is("expression"), "{error:?}");
    assert_eq!(w.sends(), 0);
    assert_eq!(named(&observations, "search_news").level, "DEFAULT");

    let w = world(vec!["text"]);
    let late =
        DIGEST.replace("  - mark:", "  - stop: { raise: { error: { type: 'https://x/e', status: 500 } } }\n  - mark:");
    let (status, _) = w.run("digest", &late).await;
    assert_eq!(faulted(&status).instance.as_deref(), Some("/do/3/stop"));
    assert_eq!(w.sends(), 1);
    assert!(w.persist.0.lock().unwrap().is_empty(), "nothing after the fault ran");
}

// A tool MCP no longer offers is refused before it is sent (the registry
// keeps it, a superset): the run faults with nothing sent (design 03, R-M8).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tool_that_is_not_offered_is_refused_before_sending() {
    let w = world(Vec::new());
    w.db.snapshot_tools(&[("search_web".into(), true)]).unwrap();
    let (status, _) = w.run("digest", &DIGEST.replace("call: search_news", "call: search_web")).await;
    assert_eq!(faulted(&status).type_, ouro_runtime::CALL_NOT_EXECUTED);
    assert!(w.calls().is_empty());
}

// A tool's own error reply is a definite failure; a transport error of a
// read leaves it unanswered, retried by the engine as a read may be, and of
// a send leaves its outcome unknown. Each call has its error span.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn direct_calls_record_reported_and_transport_errors() {
    for (tool, transport) in [("search_news", false), ("send_telegram_message", false), ("send_telegram_message", true)]
    {
        let w = world(vec![]);
        w.mcp.always(Arc::new(move |_, _, _| {
            Box::pin(async move {
                if transport {
                    anyhow::bail!("connection reset")
                }
                Ok("[tool error] refused".into())
            })
        }));
        let failing = format!(
            "document: {{ dsl: '1.0.4', namespace: t, name: failing, version: '1.0.0' }}\ndo: [{{call_it: {{call: {tool}, with: {{text: failing}}}}}}]\n"
        );
        let (status, observations) = w.run("failing", &failing).await;
        let error = faulted(&status);
        if transport {
            assert_eq!(error.type_, ouro_runtime::CALL_UNKNOWN, "{error:?}");
        } else {
            assert!(error.detail.as_deref().unwrap_or_default().contains("[tool error] refused"), "{error:?}");
        }
        assert_eq!(w.calls().len(), 1, "{tool}: a failed call is not sent again");
        let call = named(&observations, tool);
        assert_eq!(call.meta("workflow_run"), Some(&json!("run-7")));
        assert_eq!(call.meta("reference"), Some(&json!("/do/0/call_it")));
        assert_eq!(call.level, "ERROR");
        assert!(!call.end_time.is_empty());
        if transport {
            assert_eq!(call.output["error"], "connection reset");
        } else {
            assert_eq!(call.output, "[tool error] refused");
        }
    }
}

// ── at most once across a restart (invariant 4) ─────────────────────────────

// The message went out, the read after it failed, the process stopped
// before the retry: the next process retries from the store and reuses the
// delivered send instead of messaging the chat again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retry_after_restart_does_not_resend_a_delivered_message() {
    let w = world(vec![]);
    let searches = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(tokio::sync::Notify::new());
    let (counter, notice) = (searches.clone(), failed.clone());
    w.mcp.always(Arc::new(move |name, args, _| {
        let out = match name.as_str() {
            "search_news" if counter.fetch_add(1, Ordering::SeqCst) == 0 => {
                notice.notify_one();
                "[tool error] index offline".to_string()
            }
            "search_news" => json!({"items": []}).to_string(),
            _ => json!({"ok": true, "text": args.get("text")}).to_string(),
        };
        Box::pin(async move { Ok(out) })
    }));
    let retried = r#"document: { dsl: '1.0.4', namespace: t, name: retried, version: '0.1.0' }
do:
  - deliver:
      try:
        - send: { call: send_telegram_message, with: { text: digest } }
        - check: { call: search_news, with: { query: sent } }
      catch:
        retry: { delay: PT1S, limit: { attempt: { count: 3 } } }
"#;
    let first = w.engine();
    w.start(&first, "retried", retried, "retry-1", input()).await;
    failed.notified().await;
    first.shutdown().await;
    let second = w.engine();
    second.ready().await.unwrap();
    assert!(matches!(ended(&second, "retry-1").await, Status::Completed(_)));
    assert_eq!(w.sends(), 1, "the retry reused the delivered message");
    assert_eq!(searches.load(Ordering::SeqCst), 2, "the failed read did run again");
    second.shutdown().await;
}

// A send cut in flight by a shutdown is never sent again: the next process
// finds its outcome unknown, and the span of the cut call ends as an error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_leaves_an_in_flight_send_unknown_never_resent() {
    let w = world(vec![]);
    let sent = Arc::new(tokio::sync::Notify::new());
    let notice = sent.clone();
    w.mcp.always(Arc::new(move |_, _, _| {
        notice.notify_one();
        Box::pin(std::future::pending())
    }));
    let once = r#"document: { dsl: '1.0.4', namespace: t, name: once, version: '0.1.0' }
do: [ { send: { call: send_telegram_message, with: { text: once } } } ]
"#;
    let first = w.engine();
    w.start(&first, "once", once, "once-1", input()).await;
    sent.notified().await;
    first.shutdown().await;
    assert!(first.runtime.get("once-1").await.unwrap().unwrap().outcome.is_none(), "a shutdown is no end");
    let second = w.engine();
    second.ready().await.unwrap();
    let status = ended(&second, "once-1").await;
    assert_eq!(faulted(&status).type_, ouro_runtime::CALL_UNKNOWN, "{status:?}");
    assert_eq!(w.sends(), 1);
    second.shutdown().await;
    let calls: Vec<_> =
        w.observations().into_iter().filter(|o| o.meta("tool") == Some(&json!("send_telegram_message"))).collect();
    assert_eq!(calls.len(), 1, "resuming an unknown write fabricates no second call span");
    assert_eq!(calls[0].input, json!({"text":"once"}));
    assert_eq!(calls[0].level, "ERROR");
    assert!(calls[0].status_message.as_deref().unwrap().contains("interrupted"));
}

// ── choose ──────────────────────────────────────────────────────────────────

struct Jev {
    choice: &'static str,
    confidence: f64,
    fail: bool,
    requests: Mutex<Vec<Value>>,
}

#[async_trait]
impl agent::providers::JsonApi for Jev {
    async fn post(&self, _: &str, body: Value, _: &CancellationToken) -> anyhow::Result<Value> {
        self.requests.lock().unwrap().push(body.clone());
        anyhow::ensure!(!self.fail, "Jev unavailable");
        let probabilities: Map<String, Value> = body["questions"]["choice"]["criteria"]
            .as_object()
            .unwrap()
            .keys()
            .map(|l| (l.clone(), json!(if l == self.choice { self.confidence } else { 1.0 - self.confidence })))
            .collect();
        Ok(json!({"model":"jev-test","answers":{"choice":{"type":"choice","choice":self.choice,
            "confidence":self.confidence,"probabilities":probabilities}}}))
    }
}

fn jev_world(choice: &'static str, confidence: f64, fail: bool) -> (World, Arc<Jev>) {
    let api = Arc::new(Jev { choice, confidence, fail, requests: Default::default() });
    (world_with(vec![], Some(agent::decisions::decision_client("jev-test".into(), api.clone()))), api)
}

fn routed(with: &str) -> String {
    format!(
        r#"
document: {{ dsl: '1.0.4', namespace: agent, name: routed, version: '1.0.0' }}
do:
  - search:
      call: search_news
      with: {{ query: rust }}
  - route:
      call: choose
      with: {with}
  - jump:
      switch:
        - quiet: {{ when: '${{ .choice == "quiet" }}', then: quiet_text }}
        - digest: {{ when: '${{ .choice == "digest" }}', then: digest_text }}
  - digest_text:
      set: '${{ "digest" }}'
      then: end
  - quiet_text:
      set: '${{ "quiet" }}'
"#
    )
}

const CHOOSE: &str = "{ question: Is there news worth a digest?, options: { digest: Something new and significant, quiet: Nothing worth sending }, input: { items: '${ .items }' } }";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn choose_jumps_where_jev_decides() {
    for (choice, expected) in [("digest", "digest"), ("quiet", "quiet")] {
        let (w, api) = jev_world(choice, 0.9, false);
        let (status, observations) = w.run("routed", &routed(CHOOSE)).await;
        assert_eq!(status, Status::Completed(json!(expected)));
        let request = api.requests.lock().unwrap()[0].clone();
        assert_eq!(request["state"]["input"]["items"][0]["title"], "Rust 2.0", "Jev judges the input");
        assert!(request["questions"]["choice"]["instructions"].as_str().unwrap().starts_with("Is there news"));
        let route = named(&observations, "call:route");
        assert_eq!(route.metadata.as_ref().unwrap()["choice"], choice);
        assert_eq!(route.metadata.as_ref().unwrap()["fallback"], false);
        assert!(
            observations
                .iter()
                .any(|o| o.name == "decide:choose" && o.parent_observation_id.as_deref() == Some(route.id.as_str()))
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unsure_choice_takes_the_default() {
    let (w, _) = jev_world("quiet", 0.55, false);
    let with = CHOOSE.replace(" }, input:", " }, min_confidence: 0.7, default: digest, input:");
    let (status, observations) = w.run("routed", &routed(&with)).await;
    assert_eq!(status, Status::Completed(json!("digest")));
    assert_eq!(named(&observations, "call:route").metadata.as_ref().unwrap()["fallback"], true);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bad_choose_or_a_failed_jev_faults_before_any_write() {
    let cases = [
        ("valid", CHOOSE.to_owned(), true),
        ("label", CHOOSE.replace("quiet: Nothing", "Quiet: Nothing"), false),
        ("one option", "{ question: q, options: { only: x } }".to_owned(), false),
        ("default alone", CHOOSE.replace(" }, input:", " }, default: digest, input:"), false),
        ("unknown default", CHOOSE.replace(" }, input:", " }, min_confidence: 0.7, default: busy, input:"), false),
        ("confidence range", CHOOSE.replace(" }, input:", " }, min_confidence: 2, default: digest, input:"), false),
    ];
    for (case, with, jev_down) in cases {
        let (w, _) = jev_world("digest", 0.9, jev_down);
        let (status, _) = w.run("routed", &routed(&with)).await;
        let error = faulted(&status);
        assert!(error.is(if jev_down { "runtime" } else { "validation" }), "{case}: {error:?}");
        assert_eq!(w.sends(), 0, "{case}");
    }
    let w = world(vec![]);
    let (status, _) = w.run("routed", &routed(CHOOSE)).await;
    assert!(faulted(&status).to_string().contains("no decision model"), "{status:?}");
}

// ── llm_compose ─────────────────────────────────────────────────────────────

const QUIET_OR_DIGEST: &str = r#"
document: { dsl: '1.0.4', namespace: agent, name: digest, version: '1.0.0' }
do:
  - compose:
      call: llm_compose
      with: { prompt: 'Answer {"quiet": true} or {"quiet": false, "text": the digest}.', json: true }
  - route:
      switch:
        - quiet: { when: '${ .quiet == true }', then: end }
        - digest: { then: send }
  - send:
      call: send_telegram_message
      with: { text: '${ .text }' }
"#;

// A compose step asked for JSON holds the provider to one object, and the
// switch reads a field of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_json_compose_answers_an_object_a_switch_branches_on() {
    for (reply, sent) in [(r#"{"quiet": true}"#, None), (r#"{"quiet": false, "text": "Rust 2.0"}"#, Some("Rust 2.0"))] {
        let w = world(vec![reply]);
        let (status, observations) = w.run("digest", QUIET_OR_DIGEST).await;
        assert!(matches!(status, Status::Completed(_)), "{reply}: {status:?}");
        let texts: Vec<Value> = w.calls().into_iter().map(|(_, args)| args["text"].clone()).collect();
        assert_eq!(texts, sent.map(|t| json!(t)).into_iter().collect::<Vec<_>>(), "{reply}");
        let generation = named(&observations, "llm_compose:compose");
        assert_eq!(generation.model_parameters.as_ref().unwrap()["response_format"], "json_object");
        assert!(generation.input.to_string().contains("Reply with one JSON object."), "{}", generation.input);
    }
}

// A reply that is not one object faults before the send.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_json_compose_that_answers_text_faults_before_any_write() {
    for reply in ["Quiet day.", "[1, 2]", r#"{"quiet": tr"#] {
        let w = world(vec![reply]);
        let (status, _) = w.run("digest", QUIET_OR_DIGEST).await;
        assert!(faulted(&status).to_string().contains("not a JSON object"), "{reply}: {status:?}");
        assert!(w.calls().is_empty(), "{reply}");
    }
    let w = world(vec![]);
    let (status, _) = w.run("digest", &QUIET_OR_DIGEST.replace("json: true", "json: 'yes'")).await;
    assert!(faulted(&status).to_string().contains("`json` must be true or false"), "{status:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_compose_renders_its_prompt_before_the_input_xml() {
    let w = world(vec!["written"]);
    let draft = r#"
document: { dsl: '1.0.4', namespace: agent, name: draft, version: '1.0.0' }
do:
  - draft:
      call: llm_compose
      with: { prompt: 'Write the digest.', input: { posts: [ { id: 1 } ] } }
"#;
    let (status, observations) = w.run("draft", draft).await;
    assert!(matches!(status, Status::Completed(_)));
    let generation = named(&observations, "llm_compose:draft");
    let messages = generation.input.as_array().unwrap();
    assert_eq!(
        messages.last().unwrap()["content"],
        "Write the digest.\n\n<posts>\n[\n  {\n    \"id\": 1\n  }\n]\n</posts>"
    );
    assert!(generation.model_parameters.as_ref().unwrap().get("response_format").is_none());
}
