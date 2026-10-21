// Saved workflows (`flows.rs`): the store contract on both implementations,
// validation against what the agent can call, and runs on a real engine —
// MCP tools go to a recording fake, `llm_compose` to a scripted provider —
// with the trace they leave.
mod support;
use agent::{
    agent::{Engine, EngineDeps, Providers, create_engine},
    db::AgentDb,
    flows::{FlowResult, FlowRun, MemoryWorkflows, Status, WorkflowSignal, WorkflowStore, check, create_flows},
    mcp::Mcp,
    models::{ModelPreset, Presets, ReasoningEffort},
    providers::{ToolDef, assistant},
    trace_model::Observation,
    tracing::{Scope, TraceStart, Tracer},
};
use async_trait::async_trait;
use serde_json::{Map, Value, json};
use std::sync::Arc;
use support::{AnySkill, NoCodex, PersistLog, Scripted, ToolMcp, TraceLog, recording, task_context};
use tokio_util::sync::CancellationToken;

// ── the store ───────────────────────────────────────────────────────────────

fn store_contract(store: &dyn WorkflowStore) {
    assert_eq!(store.get("digest", None).unwrap(), None);
    let v1 = store.save("digest", "one", Some("first")).unwrap();
    let v2 = store.save("digest", "two", None).unwrap();
    store.save("other", "x", None).unwrap();
    assert_eq!((v1.version, v1.status, v2.version), (1, Status::Draft, 2));
    assert_eq!(store.active("digest").unwrap(), None, "a saved version is a draft");

    assert!(!v1.tried, "a saved version is untried");
    store.mark_tried("digest", 1).unwrap();
    assert!(store.get("digest", Some(1)).unwrap().unwrap().tried);
    assert!(!store.get("digest", Some(2)).unwrap().unwrap().tried, "per version");
    assert!(store.mark_tried("digest", 9).is_err());
    store.activate("digest", 1).unwrap();
    assert_eq!(store.active("digest").unwrap().map(|w| w.definition), Some("one".into()));
    store.activate("digest", 2).unwrap();
    assert_eq!(store.active("digest").unwrap().map(|w| w.version), Some(2));
    assert_eq!(store.get("digest", Some(1)).unwrap().map(|w| w.status), Some(Status::Retired));
    assert_eq!(store.get("digest", None).unwrap().map(|w| (w.version, w.note)), Some((2, None)));
    assert_eq!(store.get("digest", Some(1)).unwrap().and_then(|w| w.note), Some("first".into()));

    assert!(store.activate("digest", 9).is_err());
    assert_eq!(store.active("digest").unwrap().map(|w| w.version), Some(2), "a failed activation changes nothing");
    let listed: Vec<(String, i64)> =
        store.list().unwrap().into_iter().filter(|w| w.name != "skill-index").map(|w| (w.name, w.version)).collect();
    assert_eq!(listed, [("digest".to_string(), 2), ("other".to_string(), 1)]);
}

#[test]
fn the_store_keeps_versions_and_one_active_each() {
    store_contract(&MemoryWorkflows::default());
    let dir = tempfile::tempdir().unwrap();
    store_contract(&AgentDb::open(dir.path().join("agent.db")).unwrap());
}

// The language card of workflow-author.md stays true to the interpreter:
// its reference definition parses and validates. Its function names stand
// for real ones, so any name is callable here.
#[test]
fn the_authoring_skills_reference_definition_is_valid() {
    struct Any;
    impl owf::model::Catalog for Any {
        fn has(&self, _: &str) -> bool {
            true
        }
    }
    let skill =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../skills.default/workflow-author.md"))
            .unwrap();
    let examples: Vec<&str> = skill.split("```yaml\n").skip(1).filter_map(|block| block.split("```").next()).collect();
    assert!(!examples.is_empty(), "workflow-author shows a reference");
    for example in examples {
        let wf = owf::model::parse_yaml(example).unwrap_or_else(|e| panic!("{e}\n{example}"));
        owf::model::validate(&wf, &Any).unwrap_or_else(|e| panic!("{e}\n{example}"));
    }
}

// ── the world ───────────────────────────────────────────────────────────────

// MCP with the read-only marks a real server sends.
struct Marked {
    inner: Arc<ToolMcp>,
}

const READ_ONLY: &[&str] = &["search_news"];

#[async_trait]
impl Mcp for Marked {
    fn tools(&self) -> Vec<ToolDef> {
        self.inner.tools()
    }
    fn read_only(&self, name: &str) -> bool {
        READ_ONLY.contains(&name)
    }

    fn open_world(&self, _name: &str) -> bool {
        false
    }
    async fn call_tool(
        &self,
        name: &str,
        args: Map<String, Value>,
        cancel: &CancellationToken,
    ) -> anyhow::Result<String> {
        self.inner.call_tool(name, args, cancel).await
    }
}

struct World {
    engine: Arc<Engine>,
    mcp: Arc<ToolMcp>,
    tracer: Arc<dyn Tracer>,
    written: Arc<TraceLog>,
    persist: Arc<PersistLog>,
}

fn world(replies: Vec<&str>) -> World {
    world_with(replies, None)
}

fn world_with(replies: Vec<&str>, decisions: Option<Arc<agent::decisions::DecisionClient>>) -> World {
    let llm: Arc<Scripted> = Arc::new(Scripted::new(replies.into_iter().map(assistant).collect()));
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
    let p = |model: &str| ModelPreset { model: model.into(), reasoning_effort: ReasoningEffort::Disabled };
    let (tracer, written) = recording();
    let persist = Arc::new(PersistLog::default());
    let engine = create_engine(EngineDeps {
        providers: Providers { openai: llm.clone(), gemini: llm.clone(), deepseek: llm, openrouter: None },
        mcp: Arc::new(Marked { inner: mcp.clone() }),
        presets: Presets {
            base: p("gpt-5.4-mini"),
            smart: p("gpt-5.4"),
            smartest: p("gpt-5.4"),
            compiler: p("gpt-5.4"),
            compose: p("gpt-5.4"),
        },
        skills: Vec::new(),
        skill_store: Arc::new(AnySkill),
        memory: persist.clone(),
        tracer: tracer.clone(),
        codex: Arc::new(NoCodex),
        decisions,
        workflows: Arc::new(MemoryWorkflows::default()),
        schedule: std::sync::Arc::new(agent::schedule::MemorySchedule::default()),
    });
    World { engine, mcp, tracer, written, persist }
}

impl World {
    fn activate(&self, name: &str, definition: &str) {
        let saved = self.engine.deps.workflows.save(name, definition, None).unwrap();
        self.engine.deps.workflows.activate(name, saved.version).unwrap();
    }

    async fn run(&self, name: &str) -> (FlowResult, Vec<Observation>) {
        let trace = self.tracer.trace(TraceStart { id: "scheduler:7".into(), name: "t".into(), ..Default::default() });
        let scope: Scope = trace.clone();
        let signal = WorkflowSignal {
            id: 7,
            source: "scheduler".into(),
            content: "Daily digest tick.".into(),
            env_context: None,
        };
        let flows = create_flows(self.engine.clone());
        let result = flows.run(name, &signal, &task_context("scheduler:7"), &scope, &CancellationToken::new()).await;
        trace.end();
        (result, self.written.all().pop().map(|t| t.observations).unwrap_or_default())
    }

    fn calls(&self) -> Vec<(String, Map<String, Value>)> {
        self.mcp.calls.lock().unwrap().clone()
    }
}

fn named<'a>(observations: &'a [Observation], name: &str) -> &'a Observation {
    observations.iter().find(|o| o.name == name).unwrap_or_else(|| {
        panic!("no observation {name} in {:?}", observations.iter().map(|o| &o.name).collect::<Vec<_>>())
    })
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

// ── validation ──────────────────────────────────────────────────────────────

#[test]
fn definitions_are_checked_against_what_the_agent_can_call() {
    let w = world(Vec::new());
    assert!(check(&*w.engine.deps.mcp, DIGEST).is_ok());
    let unknown = check(&*w.engine.deps.mcp, &DIGEST.replace("call: search_news", "call: search_web")).unwrap_err();
    assert!(unknown.contains("/do/0/search/call") && unknown.contains("search_web"), "{unknown}");
    let withheld =
        check(&*w.engine.deps.mcp, &DIGEST.replace("call: search_news", "call: get_next_signal")).unwrap_err();
    assert!(withheld.contains("get_next_signal"), "{withheld}");
    assert!(check(&*w.engine.deps.mcp, "not: [ a workflow").is_err());
    // Argument names are checked against the tool's schema and the model steps'.
    let misspelt = check(&*w.engine.deps.mcp, &DIGEST.replace("with: { query:", "with: { qeury:")).unwrap_err();
    assert!(
        misspelt.contains("/do/0/search/with: `search_news` takes no argument `qeury` (it takes: query, k)"),
        "{misspelt}"
    );
    let missing =
        check(&*w.engine.deps.mcp, &DIGEST.replace("with: { text: '${ . }' }", "with: { chatId: 1 }")).unwrap_err();
    assert!(missing.contains("/do/2/send/with: `send_telegram_message` needs the argument `text`"), "{missing}");
    let model = check(&*w.engine.deps.mcp, &DIGEST.replace("skill: news-digest", "skills: news-digest")).unwrap_err();
    assert!(model.contains("`llm_compose` takes no argument `skills`"), "{model}");
}

// A task a jump leads to that also runs after a task that does not jump is
// flagged; the same branch ended with `then`, or a shared tail reached from
// the task before it, is not.
#[test]
fn a_jump_target_that_a_task_falls_into_is_a_warning() {
    let definition = |compose_then: &str| {
        format!(
            r#"
document: {{ dsl: '1.0.4', namespace: agent, name: digest, version: '1.0.0' }}
do:
  - route:
      switch:
        - quiet: {{ when: '${{ . == [] }}', then: quiet_text }}
        - busy: {{ then: compose }}
  - compose:
      set: '${{ "digest" }}'{compose_then}
  - quiet_text:
      set: '${{ "nothing new" }}'
      then: send
  - send:
      fork:
        branches:
          - inner:
              do:
                - a: {{ set: '${{ 1 }}', if: '${{ false }}', then: c }}
                - b: {{ set: '${{ 2 }}' }}
                - c: {{ set: '${{ 3 }}' }}
"#
        )
    };
    let parse = |text: &str| owf::model::parse_yaml(text).unwrap_or_else(|e| panic!("{e}\n{text}"));
    let warned = agent::flows::warnings(&parse(&definition("")));
    assert_eq!(warned.len(), 2, "{warned:?}");
    assert!(
        warned[0].starts_with("/do/2/quiet_text: a jump from `route` leads here") && warned[0].contains("`compose`")
    );
    assert!(warned[1].starts_with("/do/3/send/fork/branches/0/inner/do/2/c: a jump from `a`"), "{}", warned[1]);
    let fixed = agent::flows::warnings(&parse(&definition("\n      then: send")));
    assert_eq!(fixed.len(), 1, "only the nested one is left: {fixed:?}");
}

// ── runs ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn a_saved_workflow_runs_tools_and_model_steps_and_traces_each_task() {
    let w = world(vec!["Today: Rust 2.0"]);
    w.activate("digest", DIGEST);
    let (result, observations) = w.run("digest").await;
    let FlowResult::Completed(output) = result else { panic!("{result:?}") };
    assert_eq!(output, json!({"ok": true, "key": "news_digest.last_read_at"}));

    let calls = w.calls();
    assert_eq!(calls.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(), ["search_news", "send_telegram_message"]);
    assert_eq!(calls[0].1["query"], "rust since ever");
    assert_eq!(calls[1].1["text"], "Today: Rust 2.0");
    assert_eq!(*w.persist.0.lock().unwrap(), [("news_digest.last_read_at".into(), "2026-09-06T12:00:00.000Z".into())]);

    let runner = named(&observations, "runner");
    assert_eq!(runner.metadata.as_ref().unwrap()["workflow"], "digest");
    assert_eq!(runner.input["signal"]["content"], "Daily digest tick.");
    let search = named(&observations, "call:search");
    assert_eq!(search.parent_observation_id.as_deref(), Some(runner.id.as_str()));
    assert_eq!(search.input, json!({"tool": "search_news", "args": {"query": "rust since ever"}}));
    assert_eq!(search.metadata.as_ref().unwrap()["reference"], "/do/0/search");
    // The compose generation sits under its task and is tagged for the judge.
    let compose = named(&observations, "call:compose");
    let generation = named(&observations, "llm_compose:compose");
    assert_eq!(generation.parent_observation_id.as_deref(), Some(compose.id.as_str()));
    assert_eq!(generation.metadata.as_ref().unwrap()["judge_node"], "compose");
    assert_eq!(generation.metadata.as_ref().unwrap()["skill"], "news-digest");
    let prompt = generation.input.to_string();
    assert!(
        prompt.contains("Write the digest.") && prompt.contains("<items>") && prompt.contains("Rust 2.0"),
        "{prompt}"
    );
}

#[tokio::test]
async fn durable_wait_reopens_on_the_pinned_definition_without_repeating_a_send() {
    use agent::{durable::RunStore, flows::create_durable_flows};
    let w = world(vec![]);
    let definition = r#"document: { dsl: '1.0.4', namespace: t, name: durable, version: '0.1.0' }
do:
  - before: { call: send_telegram_message, with: { text: before }, metadata: { delivery: at_most_once } }
  - pause: { wait: PT0.01S }
  - after: { call: send_telegram_message, with: { text: after }, metadata: { delivery: at_most_once } }
"#;
    w.activate("durable", definition);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("agent.db");
    let signal = WorkflowSignal { id: 44, source: "scheduler".into(), content: "go".into(), env_context: None };
    let trace = w.tracer.trace(TraceStart { id: "durable-test".into(), name: "test".into(), ..Default::default() });
    let scope: Scope = trace.clone();
    let cancel = CancellationToken::new();
    let session = task_context("scheduler:44");
    {
        let flows = create_durable_flows(w.engine.clone(), AgentDb::open(&path).unwrap());
        assert!(matches!(flows.run("durable", &signal, &session, &scope, &cancel).await, FlowResult::Waiting { .. }));
    }
    assert_eq!(w.calls().len(), 1);
    w.activate("durable", &definition.replace("text: after", "text: changed"));
    tokio::time::sleep(std::time::Duration::from_millis(15)).await;
    let db = AgentDb::open(&path).unwrap();
    let runs = RunStore::new(db.clone());
    let flows = create_durable_flows(w.engine.clone(), db);
    assert_eq!(flows.recover_once(&cancel).await.unwrap(), 1);
    assert!(matches!(runs.get("scheduler:44").unwrap().unwrap().outcome, Some(owf::run::Status::Completed(_))));
    let calls = w.calls();
    assert_eq!(calls.iter().map(|(_, args)| args["text"].as_str().unwrap()).collect::<Vec<_>>(), ["before", "after"]);
    assert!(matches!(flows.run("durable", &signal, &session, &scope, &cancel).await, FlowResult::Completed(_)));
    assert_eq!(w.calls().len(), 2);
    assert_eq!(flows.recover_once(&cancel).await.unwrap(), 0);
    trace.end();
}

// A retry that outlives the process: the message went out, the read after
// it failed, the agent restarted before the retry. The retry reuses the
// send from agent.db instead of messaging the chat again.
#[tokio::test]
async fn a_retry_after_restart_does_not_resend_a_delivered_message() {
    use agent::{durable::RunStore, flows::create_durable_flows};
    use std::sync::atomic::{AtomicUsize, Ordering};
    let w = world(vec![]);
    let searches = Arc::new(AtomicUsize::new(0));
    let counter = searches.clone();
    w.mcp.always(Arc::new(move |name, args, _| {
        let out = match name.as_str() {
            "search_news" if counter.fetch_add(1, Ordering::SeqCst) == 0 => "[tool error] index offline".to_string(),
            "search_news" => json!({"items": []}).to_string(),
            _ => json!({"ok": true, "text": args.get("text")}).to_string(),
        };
        Box::pin(async move { Ok(out) })
    }));
    w.activate(
        "retried",
        r#"document: { dsl: '1.0.4', namespace: t, name: retried, version: '0.1.0' }
do:
  - deliver:
      try:
        - send: { call: send_telegram_message, with: { text: digest } }
        - check: { call: search_news, with: { query: sent } }
      catch:
        retry: { delay: PT0.01S, limit: { attempt: { count: 3 } } }
"#,
    );
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("agent.db");
    let signal = WorkflowSignal { id: 46, source: "scheduler".into(), content: "go".into(), env_context: None };
    let trace = w.tracer.trace(TraceStart { id: "retry-test".into(), name: "test".into(), ..Default::default() });
    let scope: Scope = trace.clone();
    let cancel = CancellationToken::new();
    {
        let flows = create_durable_flows(w.engine.clone(), AgentDb::open(&path).unwrap());
        let session = task_context("scheduler:46");
        assert!(matches!(flows.run("retried", &signal, &session, &scope, &cancel).await, FlowResult::Waiting { .. }));
    }
    tokio::time::sleep(std::time::Duration::from_millis(15)).await;
    let db = AgentDb::open(&path).unwrap();
    assert_eq!(create_durable_flows(w.engine.clone(), db.clone()).recover_once(&cancel).await.unwrap(), 1);
    assert!(matches!(
        RunStore::new(db).get("scheduler:46").unwrap().unwrap().outcome,
        Some(owf::run::Status::Completed(_))
    ));
    let sends = w.calls().iter().filter(|(name, _)| name == "send_telegram_message").count();
    assert_eq!(sends, 1, "the retry reused the delivered message");
    assert_eq!(searches.load(Ordering::SeqCst), 2, "the failed read did run again");
    trace.end();
}

#[tokio::test]
async fn shutdown_leaves_an_in_flight_run_recoverable_without_resending_at_most_once() {
    use agent::{durable::RunStore, flows::create_durable_flows};
    let w = world(vec![]);
    w.activate(
        "durable",
        r#"document: { dsl: '1.0.4', namespace: t, name: durable, version: '0.1.0' }
do: [ { send: { call: send_telegram_message, with: { text: once }, metadata: { delivery: at_most_once } } } ]
"#,
    );
    w.mcp.always(Arc::new(|_, _, _| Box::pin(std::future::pending())));
    let dir = tempfile::tempdir().unwrap();
    let db = AgentDb::open(dir.path().join("agent.db")).unwrap();
    let flows = create_durable_flows(w.engine.clone(), db.clone());
    let trace = w.tracer.trace(TraceStart { id: "shutdown-test".into(), name: "test".into(), ..Default::default() });
    let scope: Scope = trace.clone();
    let cancel = CancellationToken::new();
    let signal = WorkflowSignal { id: 45, source: "scheduler".into(), content: "go".into(), env_context: None };
    let session = task_context("scheduler:45");
    let future = flows.run("durable", &signal, &session, &scope, &cancel);
    tokio::pin!(future);
    tokio::select! {
        result = &mut future => panic!("call should be pending: {result:?}"),
        _ = async { while w.calls().is_empty() { tokio::task::yield_now().await; } } => {},
    }
    cancel.cancel();
    assert!(matches!(future.await, FlowResult::Interrupted { .. }));
    let runs = RunStore::new(db.clone());
    assert!(runs.get("scheduler:45").unwrap().unwrap().outcome.is_none());
    db.conn().execute("UPDATE workflow_runs SET wake_at_ms = 0", []).unwrap();
    flows.recover_once(&CancellationToken::new()).await.unwrap();
    assert!(matches!(runs.get("scheduler:45").unwrap().unwrap().outcome,
        Some(owf::run::Status::Faulted(ref error)) if error.is("outcome_unknown")));
    assert_eq!(w.calls().len(), 1);
    trace.end();
}

// A workflow sends to the user's chat from its input, not from an id
// written into the definition.
// The author's catalog: every callable function one line each, one in
// full on request; the probe runs reads only, cut to their shape.
#[tokio::test]
async fn the_author_sees_the_functions_and_probes_only_reads() {
    use agent::flows::{functions, probe};
    let w = world(vec![]);
    let none = Map::new();
    let catalog = functions(&w.engine, &none).unwrap();
    let line = |name: &str| catalog.lines().find(|l| l.starts_with(&format!("{name}("))).map(str::to_owned);
    assert_eq!(line("search_news").as_deref(), Some("search_news(query?: any, k?: any) read — search"));
    assert_eq!(
        line("send_telegram_message").as_deref(),
        Some("send_telegram_message(chatId?: any, text: any) write — send")
    );
    assert!(line("filter_relevant").unwrap().starts_with("filter_relevant(items, prompt, threshold?) read"));
    assert_eq!(line("get_next_signal"), None, "the signal queue is the supervisor's");
    let one: Value = serde_json::from_str(
        &functions(&w.engine, json!({"name": "send_telegram_message"}).as_object().unwrap()).unwrap(),
    )
    .unwrap();
    assert_eq!((one["effect"].as_str(), one["parameters"]["required"].clone()), (Some("write"), json!(["text"])));

    let cancel = CancellationToken::new();
    let args = |v: Value| v.as_object().unwrap().clone();
    let reply = probe(&w.engine, &args(json!({"name": "search_news", "arguments": {"query": "x"}})), &cancel).await;
    assert!(reply.unwrap().contains("Rust 2.0"));
    for refused in ["send_telegram_message", "llm_compose", "get_next_signal", "nope"] {
        assert!(probe(&w.engine, &args(json!({"name": refused})), &cancel).await.is_err(), "{refused}");
    }
    assert_eq!(w.calls().iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(), ["search_news"], "nothing else ran");

    let big = "x".repeat(20_000);
    w.mcp.always(Arc::new(move |_, _, _| {
        let big = big.clone();
        Box::pin(async move { Ok(big) })
    }));
    let cut = probe(&w.engine, &args(json!({"name": "search_news"})), &cancel).await.unwrap();
    assert!(cut.len() < 7_000 && cut.contains("20000 characters in all"), "{}", cut.len());
}

#[tokio::test]
async fn the_author_can_inspect_signal_and_loop_hosts_but_probes_cannot_execute_them() {
    use agent::flows::{functions, probe};
    let w = world(vec![]);
    let catalog = functions(&w.engine, &Map::new()).unwrap();
    // Follow the definitions an author actually copies, rather than a second
    // list of expected names that could omit the next host function too.
    for definition in [agent::supervisor::SIGNAL, agent::agent::AGENT_LOOP] {
        let wf = owf::model::parse_yaml(definition).unwrap();
        for site in owf::model::call_sites(&wf) {
            let name = &site.function;
            assert!(catalog.lines().any(|line| line.starts_with(&format!("{name}("))), "missing {name}");
            let detail: Value =
                serde_json::from_str(&functions(&w.engine, json!({"name":name}).as_object().unwrap()).unwrap())
                    .unwrap();
            let properties = detail["parameters"]["properties"].as_object().unwrap();
            for argument in site.with.as_ref().and_then(Value::as_object).into_iter().flat_map(|a| a.keys()) {
                assert!(properties.contains_key(argument), "{name}: undocumented argument {argument}");
            }
        }
    }
    for function in agent::supervisor::SIGNAL_FUNCTIONS {
        let detail: Value =
            serde_json::from_str(&functions(&w.engine, json!({"name":function.name}).as_object().unwrap()).unwrap())
                .unwrap();
        assert_eq!(detail["context"], "signal");
        assert!(!detail["description"].as_str().unwrap().is_empty());
        assert_eq!(detail["parameters"]["required"], json!(function.needs));
        let error = probe(&w.engine, json!({"name":function.name}).as_object().unwrap(), &CancellationToken::new())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("try_workflow"), "{error}");
    }
    let route: Value =
        serde_json::from_str(&functions(&w.engine, json!({"name":"route_signal"}).as_object().unwrap()).unwrap())
            .unwrap();
    assert_eq!(route["needs"], json!(["context"]));
    assert!(route["description"].as_str().unwrap().contains("Dispatcher only"));
    let primary: Value =
        serde_json::from_str(&functions(&w.engine, json!({"name":"primary_loop"}).as_object().unwrap()).unwrap())
            .unwrap();
    assert!(primary["parameters"]["properties"].get("loop").is_some());
    for name in ["agent_turn", "tool", "agent_settle"] {
        assert!(
            probe(&w.engine, json!({"name":name}).as_object().unwrap(), &CancellationToken::new())
                .await
                .unwrap_err()
                .to_string()
                .contains("try_workflow")
        );
    }
    assert!(w.calls().is_empty(), "inspecting host contracts must not execute anything");
}

// A dry run runs a draft's reads and model steps on the session's input and
// only records its writes: nothing is sent, no watermark moves, and a wait
// does not sleep.
#[tokio::test]
async fn a_dry_run_records_the_writes_and_changes_nothing() {
    use agent::flows::try_draft;
    let w = world(vec!["Today: Rust 2.0"]);
    let draft = DIGEST.replace("do:\n", "do:\n  - pause: { wait: PT1H }\n");
    w.engine.deps.workflows.save("digest", &draft, None).unwrap();
    let env = task_context("author").env.clone();
    let cancel = CancellationToken::new();
    let trace = w.tracer.trace(TraceStart { id: "author".into(), name: "t".into(), ..Default::default() });
    let scope: Scope = trace.clone();
    let args = |v: Value| v.as_object().unwrap().clone();
    let reply = try_draft(&w.engine, &args(json!({"name": "digest"})), &env, &scope, &cancel).await.unwrap();
    trace.end();
    let reply: Value = serde_json::from_str(&reply).unwrap();
    assert_eq!((reply["status"].as_str(), reply["version"].as_i64()), (Some("completed"), Some(1)), "{reply}");

    assert_eq!(w.calls().iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(), ["search_news"], "only the read ran");
    assert!(w.persist.0.lock().unwrap().is_empty(), "no watermark moved");
    let writes: Vec<(&str, &str)> = reply["writes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|w| (w["function"].as_str().unwrap(), w["task"].as_str().unwrap()))
        .collect();
    assert_eq!(writes, [("send_telegram_message", "/do/3/send"), ("set_memory", "/do/4/mark")]);
    assert_eq!(reply["writes"][0]["arguments"]["text"], "Today: Rust 2.0");
    let compose = reply["steps"].as_array().unwrap().iter().find(|s| s["task"] == "/do/2/compose").unwrap();
    assert_eq!(compose["output"], "Today: Rust 2.0");
    assert_eq!(reply["path"], json!(["pause", "search", "compose", "send", "mark"]));
    assert!(w.engine.deps.workflows.get("digest", Some(1)).unwrap().unwrap().tried, "a completed dry run marks it");
    assert!(reply["next"].as_str().unwrap().contains("fix"), "{reply}");
    let observations = w.written.all().pop().unwrap().observations;
    let run = named(&observations, "dry-run");
    assert_eq!(run.metadata.as_ref().unwrap()["workflow"], "digest");
    assert_eq!(named(&observations, "call:search").parent_observation_id.as_deref(), Some(run.id.as_str()));

    assert!(try_draft(&w.engine, &args(json!({"name": "nope"})), &env, &scope, &cancel).await.is_err());
    w.engine.deps.workflows.save("digest", &DIGEST.replace("call: search_news", "call: search_web"), None).unwrap();
    let invalid = try_draft(&w.engine, &args(json!({"name": "digest"})), &env, &scope, &cancel).await;
    assert!(invalid.unwrap_err().to_string().contains("search_web"));
}

#[tokio::test]
async fn a_workflow_sends_to_the_chat_its_input_names_on_the_users_clock() {
    use agent::session::{EnvData, session};
    let w = world(vec![]);
    w.activate(
        "ping",
        r#"document: { dsl: '1.0.4', namespace: t, name: ping, version: '0.1.0' }
do: [ { send: { call: send_telegram_message, with: { chatId: '${ $workflow.input.env.telegramChatId }', text: '${ $workflow.input.env.localNow }' } } } ]
"#,
    );
    let env = EnvData {
        telegram_chat_id: Some("4242".into()),
        timezone: "Europe/Kiev".into(),
        ..task_context("scheduler:8").env.clone()
    };
    let signal = WorkflowSignal { id: 8, source: "scheduler".into(), content: "go".into(), env_context: None };
    let trace = w.tracer.trace(TraceStart { id: "scheduler:8".into(), name: "t".into(), ..Default::default() });
    let scope: Scope = trace.clone();
    let result = create_flows(w.engine.clone())
        .run("ping", &signal, &session("scheduler:8", env), &scope, &CancellationToken::new())
        .await;
    trace.end();
    assert!(matches!(result, FlowResult::Completed(_)), "{result:?}");
    assert_eq!(
        w.calls(),
        [(
            "send_telegram_message".to_string(),
            json!({"chatId":"4242","text":"2026-09-06T15:00:00+03:00"}).as_object().unwrap().clone()
        )]
    );
}

#[tokio::test]
async fn a_fault_before_any_write_reports_that_nothing_changed() {
    let w = world(Vec::new());
    w.activate(
        "digest",
        &DIGEST.replace("call: search_news", "call: search_news\n      output: { as: '${ error(\"no news\") }' }"),
    );
    let (result, observations) = w.run("digest").await;
    let FlowResult::Faulted { error, effects_started } = result else { panic!("{result:?}") };
    assert!(!effects_started);
    assert_eq!(error.instance.as_deref(), Some("/do/0/search"));
    assert!(error.is("expression"), "{error:?}");
    assert_eq!(named(&observations, "call:search").level, "ERROR");
    assert_eq!(named(&observations, "runner").level, "ERROR");
}

#[tokio::test]
async fn a_fault_after_a_write_reports_that_something_may_have_changed() {
    let w = world(vec!["text"]);
    w.activate(
        "digest",
        &DIGEST.replace("  - mark:", "  - stop: { raise: { error: { type: 'https://x/e', status: 500 } } }\n  - mark:"),
    );
    let (result, _) = w.run("digest").await;
    let FlowResult::Faulted { error, effects_started } = result else { panic!("{result:?}") };
    assert!(effects_started);
    assert_eq!(error.instance.as_deref(), Some("/do/3/stop"));
}

#[tokio::test]
async fn only_an_active_version_runs() {
    let w = world(Vec::new());
    assert!(matches!(w.run("digest").await.0, FlowResult::Missing));
    w.engine.deps.workflows.save("digest", DIGEST, None).unwrap();
    assert!(matches!(w.run("digest").await.0, FlowResult::Missing), "a draft does not run");
    assert!(w.calls().is_empty());
}

#[tokio::test]
async fn a_definition_that_no_longer_validates_changes_nothing() {
    let w = world(Vec::new());
    w.activate("digest", &DIGEST.replace("call: search_news", "call: search_web"));
    let (result, _) = w.run("digest").await;
    let FlowResult::Faulted { error, effects_started: false } = result else { panic!("{result:?}") };
    assert!(error.is("validation") && error.detail.unwrap_or_default().contains("search_web"));
    assert!(w.calls().is_empty());
}

// ── authoring tools ─────────────────────────────────────────────────────────

fn tool(w: &World, name: &str, args: Value) -> anyhow::Result<Value> {
    let args = args.as_object().cloned().unwrap();
    agent::flows::tool(&w.engine, name, &args).map(|reply| serde_json::from_str(&reply).unwrap())
}

#[test]
fn the_authoring_tools_save_drafts_and_activate_only_valid_versions() {
    let w = world(Vec::new());
    // Every tool is offered to the model, so the loop can reach it.
    let offered: Vec<&str> = agent::agent::synthetic_definitions().iter().map(|t| t.name.as_str()).collect();
    assert!(agent::flows::TOOLS.iter().all(|t| offered.contains(t)), "{offered:?}");

    let broken = tool(
        &w,
        "save_workflow",
        json!({"name":"digest","definition":DIGEST.replace("call: search_news", "call: nope")}),
    );
    assert!(broken.unwrap_err().to_string().contains("/do/0/search/call"));
    let renamed = tool(&w, "save_workflow", json!({"name":"other","definition":DIGEST}));
    assert!(renamed.unwrap_err().to_string().contains("document.name is `digest`"));

    let saved = tool(&w, "save_workflow", json!({"name":"digest","definition":DIGEST,"note":"first"})).unwrap();
    assert_eq!((saved["version"].clone(), saved["status"].clone()), (json!(1), json!("draft")));
    let effects: Vec<(String, String)> = saved["calls"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| (c["function"].as_str().unwrap().into(), c["effect"].as_str().unwrap().into()))
        .collect();
    assert_eq!(
        effects,
        [
            ("search_news".to_string(), "read".to_string()),
            ("llm_compose".into(), "read".into()),
            ("send_telegram_message".into(), "write".into()),
            ("set_memory".into(), "write".into()),
        ]
    );
    let listed = tool(&w, "list_workflows", json!({})).unwrap();
    assert_eq!(listed["workflows"][0]["active_version"], Value::Null);

    assert!(tool(&w, "activate_workflow", json!({"name":"digest","version":2})).is_err());
    let untried = tool(&w, "activate_workflow", json!({"name":"digest","version":1})).unwrap_err();
    assert!(untried.to_string().contains("has not completed a dry run"), "{untried}");
    w.engine.deps.workflows.mark_tried("digest", 1).unwrap();
    tool(&w, "activate_workflow", json!({"name":"digest","version":1})).unwrap();
    let listed = tool(&w, "list_workflows", json!({})).unwrap();
    assert_eq!((listed["count"].clone(), listed["workflows"][0]["active_version"].clone()), (json!(1), json!(1)));
    let read = tool(&w, "read_workflow", json!({"name":"digest"})).unwrap();
    assert_eq!((read["definition"].as_str(), read["status"].as_str()), (Some(DIGEST), Some("active")));
    assert_eq!(tool(&w, "read_workflow", json!({"name":"nope"})).unwrap()["found"], false);
}

// ── choose: a jump where Jev decides ────────────────────────────────────────

// Jev answering every choice with `choice` at `confidence`, or failing.
struct Jev {
    choice: &'static str,
    confidence: f64,
    fail: bool,
    requests: std::sync::Mutex<Vec<Value>>,
}
#[async_trait]
impl agent::providers::JsonApi for Jev {
    async fn post(&self, _: &str, body: Value, _: &CancellationToken) -> anyhow::Result<Value> {
        self.requests.lock().unwrap().push(body.clone());
        anyhow::ensure!(!self.fail, "Jev is down");
        let labels = body["questions"]["choice"]["criteria"].as_object().unwrap();
        let probabilities: Map<String, Value> = labels
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

#[tokio::test]
async fn choose_jumps_where_jev_decides() {
    for (choice, expected) in [("digest", "digest"), ("quiet", "quiet")] {
        let (w, api) = jev_world(choice, 0.9, false);
        w.activate("routed", &routed(CHOOSE));
        let (result, observations) = w.run("routed").await;
        let FlowResult::Completed(output) = result else { panic!("{result:?}") };
        assert_eq!(output, json!(expected));
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

#[tokio::test]
async fn an_unsure_choice_takes_the_default() {
    let (w, _) = jev_world("quiet", 0.55, false);
    w.activate("routed", &routed(&CHOOSE.replace(" }, input:", " }, min_confidence: 0.7, default: digest, input:")));
    let (result, observations) = w.run("routed").await;
    assert!(matches!(result, FlowResult::Completed(ref v) if v == "digest"), "{result:?}");
    assert_eq!(named(&observations, "call:route").metadata.as_ref().unwrap()["fallback"], true);
}

#[tokio::test]
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
        w.activate("routed", &routed(&with));
        let (result, _) = w.run("routed").await;
        let FlowResult::Faulted { error, effects_started } = result else { panic!("{case}: {result:?}") };
        assert!(!effects_started, "{case}");
        assert!(error.is(if jev_down { "runtime" } else { "validation" }), "{case}: {error:?}");
    }
    let w = world(vec![]);
    w.activate("routed", &routed(CHOOSE));
    let (result, _) = w.run("routed").await;
    assert!(matches!(result, FlowResult::Faulted { ref error, .. } if error.to_string().contains("no decision model")));
}

#[test]
fn a_switch_that_misses_or_invents_a_choose_option_is_a_warning() {
    let parse = |text: &str| owf::model::parse_yaml(text).unwrap_or_else(|e| panic!("{e}\n{text}"));
    assert_eq!(agent::flows::warnings(&parse(&routed(CHOOSE))), Vec::<String>::new());
    let three = CHOOSE.replace("quiet: Nothing worth sending", "quiet: Nothing worth sending, later: Wait a day");
    let warned = agent::flows::warnings(&parse(&routed(&three)));
    assert_eq!(warned.len(), 1, "{warned:?}");
    assert!(warned[0].starts_with("/do/1/route: choose option `later` has no case"), "{}", warned[0]);
    let invented = routed(CHOOSE).replace(r#".choice == "quiet""#, r#".choice == "silent""#);
    let warned = agent::flows::warnings(&parse(&invented));
    assert_eq!(warned.len(), 2, "{warned:?}");
    assert!(warned.iter().any(|w| w.contains("`quiet` has no case")));
    assert!(warned.iter().any(|w| w.contains("compares with `silent`, which is not one of its options")));
    let defaulted = routed(&three).replace(
        "        - digest: { when: '${ .choice == \"digest\" }', then: digest_text }",
        "        - digest: { then: digest_text }",
    );
    assert_eq!(agent::flows::warnings(&parse(&defaulted)), Vec::<String>::new(), "a default case covers the rest");
}

#[test]
fn choose_is_in_the_catalog_and_refuses_at_most_once() {
    let w = world(vec![]);
    let catalog = agent::flows::functions(&w.engine, &Map::new()).unwrap();
    assert!(
        catalog.lines().any(|l| l.starts_with("choose(question, options, input?, min_confidence?, default?) read")),
        "{catalog}"
    );
    let pinned = routed(CHOOSE)
        .replace("      call: choose\n", "      call: choose\n      metadata: { delivery: at_most_once }\n");
    let error = check(&*w.engine.deps.mcp, &pinned).unwrap_err();
    assert!(error.contains("model adapters do not yet support at_most_once"), "{error}");
}

// ── llm_compose json ────────────────────────────────────────────────────────

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
// switch reads a field of it: no sentinel phrase to misspell.
#[tokio::test]
async fn a_json_compose_answers_an_object_a_switch_branches_on() {
    for (reply, sent) in [(r#"{"quiet": true}"#, None), (r#"{"quiet": false, "text": "Rust 2.0"}"#, Some("Rust 2.0"))] {
        let w = world(vec![reply]);
        w.activate("digest", QUIET_OR_DIGEST);
        let (result, observations) = w.run("digest").await;
        assert!(matches!(result, FlowResult::Completed(_)), "{reply}: {result:?}");
        let texts: Vec<Value> = w.calls().into_iter().map(|(_, args)| args["text"].clone()).collect();
        assert_eq!(texts, sent.map(|t| json!(t)).into_iter().collect::<Vec<_>>(), "{reply}");
        let generation = named(&observations, "llm_compose:compose");
        assert_eq!(generation.model_parameters.as_ref().unwrap()["response_format"], "json_object");
        assert!(generation.input.to_string().contains("Reply with one JSON object."), "{}", generation.input);
    }
}

// A reply that is not one object faults before the send: the text a
// sentinel branch would have mistaken for a digest never goes out.
#[tokio::test]
async fn a_json_compose_that_answers_text_faults_before_any_write() {
    for reply in ["Quiet day.", "[1, 2]", r#"{"quiet": tr"#] {
        let w = world(vec![reply]);
        w.activate("digest", QUIET_OR_DIGEST);
        let (result, _) = w.run("digest").await;
        let FlowResult::Faulted { error, effects_started } = result else { panic!("{reply}: {result:?}") };
        assert!(!effects_started && error.to_string().contains("not a JSON object"), "{reply}: {error}");
        assert!(w.calls().is_empty(), "{reply}");
    }
    let w = world(vec![]);
    let bad = QUIET_OR_DIGEST.replace("json: true", "json: 'yes'");
    w.activate("digest", &bad);
    let (result, _) = w.run("digest").await;
    assert!(
        matches!(result, FlowResult::Faulted { ref error, .. } if error.to_string().contains("`json` must be true or false")),
        "{result:?}"
    );
}

// ── composition and input helpers (shared with the saved-workflow host) ─────

#[test]
fn input_xml_keeps_strings_and_pretty_prints_structured_values() {
    use agent::flows::render_input_xml;
    assert_eq!(render_input_xml(&Map::new()), "");
    assert_eq!(render_input_xml(json!({"a":"x","b":"y"}).as_object().unwrap()), "<a>\nx\n</a>\n\n<b>\ny\n</b>");
    assert_eq!(
        render_input_xml(json!({"posts":[{"id":1}],"missing":null}).as_object().unwrap()),
        "<posts>\n[\n  {\n    \"id\": 1\n  }\n]\n</posts>\n\n<missing>\nnull\n</missing>"
    );
}

#[test]
fn tool_result_parsing_keeps_non_json_and_only_parses_objects_and_arrays() {
    use agent::flows::parse_tool_json;
    for (raw, parsed) in [
        (r#"{"ok":true}"#, json!({"ok":true})),
        ("[1,2]", json!([1, 2])),
        ("{broken", json!("{broken")),
        ("pong", json!("pong")),
        ("42", json!("42")),
        ("null", json!("null")),
        (" [1]", json!(" [1]")),
    ] {
        assert_eq!(parse_tool_json(raw), parsed, "{raw}");
    }
}

#[tokio::test]
async fn compose_layers_base_skill_and_patch_and_keeps_prompt_only_calls_tool_free() {
    for (base, skill) in [(true, true), (true, false), (false, true), (false, false)] {
        let model = Arc::new(Scripted::new(vec![assistant(r#"{"text":"done"}"#)]));
        let mut skills = Vec::new();
        if base {
            skills.push(("composer", "---\ntools: []\n---\nBASE COMPOSE RULES"));
        }
        if skill {
            skills.push(("writer", "---\ntools: []\n---\nWRITER RULES"));
        }
        let f = support::fixture(model.clone(), &["search_news"], &skills);
        std::fs::create_dir(f.root.path().join("skills")).unwrap();
        std::fs::write(f.root.path().join("skills/writer.patch.md"), "PATCH LESSON").unwrap();
        let user = "Compose.\n\n<posts>\n[\n  {\n    \"id\": 1\n  }\n]\n</posts>";
        let result = agent::flows::compose(
            &f.engine,
            "smartest",
            skill.then_some("writer"),
            user,
            false,
            "draft",
            &agent::tracing::null_scope(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(result, json!({"text":"done"}));
        let seen = model.seen.lock().unwrap().clone();
        assert!(seen[0].tools.is_empty());
        assert_eq!(seen[0].model, f.engine.deps.presets.smartest.model);
        assert_eq!(seen[0].messages.last().unwrap()["content"], user);
        let system = seen[0].messages.iter().find(|m| m["role"] == "system");
        if base || skill {
            let system = system.unwrap()["content"].as_str().unwrap();
            assert_eq!(system.contains("BASE COMPOSE RULES"), base);
            assert_eq!(system.contains("WRITER RULES"), skill);
            assert_eq!(system.contains("PATCH LESSON"), skill);
            if base {
                assert!(system.starts_with("BASE COMPOSE RULES"));
            }
            if skill {
                assert!(system.ends_with("<!-- improver-patch -->\nPATCH LESSON\n"));
            }
        } else {
            assert!(system.is_none());
        }
        f.engine.shutdown().await;
    }
}

#[tokio::test]
async fn a_compose_with_a_missing_skill_fails_before_generation() {
    let model = Arc::new(Scripted::new(vec![]));
    let f = support::fixture(model.clone(), &[], &[]);
    let error = agent::flows::compose(
        &f.engine,
        "base",
        Some("missing"),
        "write",
        false,
        "draft",
        &agent::tracing::null_scope(),
        &CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.to_string(), "skill not found: missing");
    assert!(model.seen.lock().unwrap().is_empty());
    f.engine.shutdown().await;
}

#[tokio::test]
async fn a_saved_compose_renders_prompt_before_input_xml() {
    let w = world(vec!["written"]);
    w.activate(
        "draft",
        r#"
document: { dsl: '1.0.4', namespace: agent, name: draft, version: '1.0.0' }
do:
  - draft:
      call: llm_compose
      with: { prompt: 'Write the digest.', input: { posts: [ { id: 1 } ] } }
"#,
    );
    let (result, observations) = w.run("draft").await;
    assert!(matches!(result, FlowResult::Completed(_)));
    let generation = named(&observations, "llm_compose:draft");
    let messages = generation.input.as_array().unwrap();
    assert_eq!(
        messages.last().unwrap()["content"],
        "Write the digest.\n\n<posts>\n[\n  {\n    \"id\": 1\n  }\n]\n</posts>"
    );
    assert!(generation.model_parameters.as_ref().unwrap().get("response_format").is_none());
}

#[tokio::test]
async fn fast_child_starts_immediately_and_parent_never_saves_waiting() {
    use agent::{durable::RunStore, flows::create_durable_flows};
    let w = world(vec![]);
    w.activate(
        "child",
        "document: { dsl: '1.0.4', namespace: t, name: child, version: '1.0.0' }\ndo: [ { answer: { set: '${ 42 }' } } ]",
    );
    w.activate(
        "parent",
        r#"document: { dsl: '1.0.4', namespace: t, name: parent, version: '1.0.0' }
do:
 - start:
     call: start_child
     with: { workflow: child, key: one }
     export: { as: '${ . }' }
 - wait:
     listen: { to: { one: { with: { type: '${ "done:" + $context.run }' } } } }
     metadata: { hold: PT1S }
 - result:
     call: child_outcome
     with: { run: '${ $context.run }', key: one }
"#,
    );
    let dir = tempfile::tempdir().unwrap();
    let db = AgentDb::open(dir.path().join("agent.db")).unwrap();
    db.conn().execute_batch("CREATE TABLE waiting_saved (id TEXT); CREATE TRIGGER record_waiting AFTER UPDATE OF state ON workflow_runs WHEN NEW.state='waiting' BEGIN INSERT INTO waiting_saved VALUES (NEW.id); END;").unwrap();
    let flows = create_durable_flows(w.engine.clone(), db.clone());
    let trace = w.tracer.trace(TraceStart { id: "children".into(), name: "test".into(), ..Default::default() });
    let scope: Scope = trace.clone();
    let signal = WorkflowSignal { id: 1, source: "test".into(), content: "".into(), env_context: None };
    let started = std::time::Instant::now();
    let result = flows.run("parent", &signal, &task_context("test:1"), &scope, &CancellationToken::new()).await;
    assert!(matches!(result, FlowResult::Completed(ref v) if v == &json!({"Completed":42})), "{result:?}");
    assert!(started.elapsed() < std::time::Duration::from_secs(1));
    assert_eq!(db.conn().query_row("SELECT count(*) FROM waiting_saved", [], |r| r.get::<_, i64>(0)).unwrap(), 0);
    assert_eq!(RunStore::new(db).get("test:1").unwrap().unwrap().state, "completed");
    trace.end();
}

#[test]
fn guard_is_reserved_readable_and_its_private_functions_are_not_in_the_catalog() {
    let w = world(vec![]);
    for name in ["save_workflow", "activate_workflow"] {
        let args = json!({"name":"guard","version":1,"definition":agent::flows::built_in("guard")});
        assert!(
            agent::flows::tool(&w.engine, name, args.as_object().unwrap())
                .unwrap_err()
                .to_string()
                .contains("reserved")
        );
    }
    let args = json!({"name":"guard"});
    let read = agent::flows::tool(&w.engine, "read_workflow", args.as_object().unwrap()).unwrap();
    assert!(read.contains("call: review"));
    // Even a directly stored override never replaces the reserved definition.
    let version = w.engine.deps.workflows.save("guard", "forged", None).unwrap();
    w.engine.deps.workflows.activate("guard", version.version).unwrap();
    assert_eq!(agent::flows::definition(&w.engine, "guard").0, agent::flows::built_in("guard").unwrap());
    for function in ["review", "confirm_request", "resolve_review"] {
        let def = format!(
            "document: {{ dsl: '1.0.4', namespace: t, name: other, version: '1.0.0' }}\ndo: [ {{ step: {{ call: {function} }} }} ]"
        );
        assert!(check(&*w.engine.deps.mcp, &def).is_err(), "{function}");
        assert!(agent::flows::functions(&w.engine, json!({"name":function}).as_object().unwrap()).is_err());
    }
}

#[tokio::test]
async fn a_waiting_saved_child_recovers_in_its_parents_session() {
    use agent::{db::TraceStore, durable::RunStore, flows::create_durable_flows};
    let mut w = world(vec![]);
    w.activate(
        "child",
        r#"document: { dsl: '1.0.4', namespace: t, name: child, version: '1.0.0' }
do:
 - event: { listen: { to: { one: { with: { type: child-answer } } } } }
 - result: { set: '${ 42 }' }
"#,
    );
    w.activate("parent",r#"document: { dsl: '1.0.4', namespace: t, name: parent, version: '1.0.0' }
do:
 - start:
     call: start_child
     with: { workflow: child, key: one }
     export: { as: '${ . }' }
 - first:
     listen: { to: { any: [{ with: { type: '${ "done:" + $context.run }' } },{ with: { type: '${ "waiting:" + $context.run }' } }] } }
     metadata: { hold: PT1S }
 - final:
     if: '${ .[0].type == ("waiting:" + $context.run) }'
     listen: { to: { one: { with: { type: '${ "done:" + $context.run }' } } } }
 - result:
     call: child_outcome
     with: { run: '${ $context.run }', key: one }
"#);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("agent.db");
    let db = AgentDb::open(&path).unwrap();
    w.tracer = Arc::new(agent::local_recorder::local_recorder_tracer(Arc::new(db.clone())));
    Arc::get_mut(&mut w.engine).unwrap().deps.tracer = w.tracer.clone();
    let flows = create_durable_flows(w.engine.clone(), db.clone());
    let trace = w.tracer.trace(TraceStart {
        id: "parents-session".into(),
        session_id: Some("shared-session".into()),
        name: "test".into(),
        ..Default::default()
    });
    let scope: Scope = trace.clone();
    let signal = WorkflowSignal { id: 2, source: "test".into(), content: "".into(), env_context: None };
    let cancel = CancellationToken::new();
    assert!(matches!(
        flows.run("parent", &signal, &task_context("shared-session"), &scope, &cancel).await,
        FlowResult::Waiting { .. }
    ));
    trace.end();
    drop(flows);
    let db = AgentDb::open(&path).unwrap();
    let runs = RunStore::new(db.clone());
    assert!(runs.deliver("child-answer", &json!({})).unwrap());
    let flows = create_durable_flows(w.engine.clone(), db.clone());
    assert_eq!(flows.recover_once(&cancel).await.unwrap(), 1);
    assert_eq!(flows.recover_once(&cancel).await.unwrap(), 1);
    assert_eq!(
        runs.get("test:2").unwrap().unwrap().outcome,
        Some(owf::run::Status::Completed(json!({"Completed":42})))
    );
    let (trace, observations) = db.get_trace("parents-session").unwrap().unwrap();
    assert_eq!(trace.session_id.as_deref(), Some("shared-session"));
    assert_eq!(trace.output, json!({"Completed":42}));
    assert_eq!(db.list_recent(20, None).unwrap().len(), 1);
    let child = observations.iter().find(|o| o.name == "resume:child").unwrap();
    let anchor_span = observations.iter().find(|o| Some(&o.id) == child.parent_observation_id.as_ref()).unwrap();
    assert_eq!(anchor_span.name, "call:start");
    assert!(observations.iter().any(|o| o.name == "resume:parent"));
}

#[tokio::test]
async fn a_run_admitted_without_an_anchor_retains_its_separate_resume_trace() {
    use agent::{db::TraceStore, durable::RunStore, flows::create_durable_flows};
    let mut w = world(vec![]);
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(AgentDb::open(dir.path().join("agent.db")).unwrap());
    Arc::get_mut(&mut w.engine).unwrap().deps.tracer =
        Arc::new(agent::local_recorder::local_recorder_tracer(db.clone()));
    let definition = "document: {dsl: '1.0.4', namespace: t, name: legacy, version: '1.0.0'}\ndo:\n  - done: {set: '${ \"finished\" }'}\n";
    let runs = RunStore::new((*db).clone());
    runs.admit("legacy", "legacy", definition, &json!({"__session_id":"original"})).unwrap();
    assert!(runs.get("legacy").unwrap().unwrap().trace_anchor.is_none());
    let flows = create_durable_flows(w.engine, (*db).clone());
    assert_eq!(flows.recover_once(&CancellationToken::new()).await.unwrap(), 1);
    let rows = db.list_recent(20, None).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "resume:legacy");
    let (trace, _) = db.get_trace(&rows[0].id).unwrap().unwrap();
    assert_eq!(trace.session_id.as_deref(), Some("original"));
    assert_eq!(trace.output, "finished");
}
