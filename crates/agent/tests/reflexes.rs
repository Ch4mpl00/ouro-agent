// Design 10: reflexes. A reflex is an ordinary saved workflow with a
// declaration (`metadata.reflex`); the loop sees the active, tried ones in a
// catalog in its instructions and runs one with `run_reflex(name, input)`.
// The call is pinned to one resolution (version, declaration, the
// transitive summary of what it does): a closed-world read runs in the
// round with no review, an open-world read or a write goes through the
// guard with the reflex's YAML and input as evidence, and the start is the
// effect (`start_reflex`, a host child of the loop keyed by the call). A
// reflex composes reflexes with `start_child { host: inherit }`; their
// traces nest under the call's span, across restarts.
mod support;
use agent::{
    agent::{AgentLoop, Engine, EngineDeps, LoopOptions, Providers, create_engine_on},
    db::{AgentDb, TraceStore},
    mcp::Mcp,
    models::{ModelPreset, Presets, ReasoningEffort},
    providers::{ChatProvider, ToolDef, assistant},
    runs::Status,
    trace_model::Observation,
};
use async_trait::async_trait;
use ouro_runtime::durable::{AtomicBackend, Rows, Snapshot, Transaction, TransactionalStore, memory::MemoryBackend};
use serde_json::{Map, Value, json};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use support::{AnySkill, GuardApi, NoCodex, Scripted, calls, task_context};
use tokio_util::sync::CancellationToken;

// ── the world ───────────────────────────────────────────────────────────────

// `news_list` reads the agent's own store (closed world); `fetch_url` reads
// the internet (open world); `send` changes the agent's own store (a
// closed-world write: only its effect asks for a review).
// `stop_on(name)` makes the next call of `name` stop the process: it is
// recorded, then never answers.
#[derive(Default)]
struct World {
    calls: Mutex<Vec<(String, Map<String, Value>)>>,
    stops: Mutex<Vec<(String, CancellationToken)>>,
}

impl World {
    fn stop_on(&self, name: &str) -> CancellationToken {
        let stop = CancellationToken::new();
        self.stops.lock().unwrap().push((name.into(), stop.clone()));
        stop
    }
    fn count(&self, name: &str) -> usize {
        self.calls.lock().unwrap().iter().filter(|(n, _)| n == name).count()
    }
    fn args(&self, name: &str) -> Vec<Value> {
        self.calls.lock().unwrap().iter().filter(|(n, _)| n == name).map(|(_, a)| Value::Object(a.clone())).collect()
    }
}

#[async_trait]
impl Mcp for World {
    fn tools(&self) -> Vec<ToolDef> {
        ["news_list", "fetch_url", "send"].iter().map(|n| ToolDef::new(n, n, json!({"type":"object"}))).collect()
    }
    fn read_only(&self, name: &str) -> bool {
        matches!(name, "news_list" | "fetch_url")
    }
    fn open_world(&self, name: &str) -> bool {
        name == "fetch_url"
    }
    async fn call_tool(&self, name: &str, args: Map<String, Value>, _: &CancellationToken) -> anyhow::Result<String> {
        self.calls.lock().unwrap().push((name.into(), args.clone()));
        let stop = {
            let mut stops = self.stops.lock().unwrap();
            stops.iter().position(|(n, _)| n == name).map(|at| stops.remove(at).1)
        };
        if let Some(stop) = stop {
            stop.cancel();
            std::future::pending::<()>().await;
        }
        Ok(match name {
            "news_list" => json!({"items": [{"title": format!("news since {}", args["since"].as_str().unwrap_or("?")),
                "query": args.get("query")}]})
            .to_string(),
            "fetch_url" => format!("page of {}", args["url"].as_str().unwrap_or("?")),
            _ => json!({"delivered": true}).to_string(),
        })
    }
}

// ── the stores, and a crash at a chosen write ───────────────────────────────

// The engine's store, whose writes stop at a chosen row: from the first
// write `trigger` matches, every write of the process fails, as if it died
// there. `revive` is the next process, on the same rows.
// Whether a write (its row's kind, its value as JSON text) is where the
// process dies.
type Trigger = Box<dyn Fn(&str, &str) -> bool + Send + Sync>;

#[derive(Default)]
struct Crash {
    trigger: Mutex<Option<(Trigger, CancellationToken)>>,
    dead: AtomicBool,
}

impl Crash {
    // `trigger(kind, value as JSON text)`.
    fn arm(&self, trigger: impl Fn(&str, &str) -> bool + Send + Sync + 'static) -> CancellationToken {
        let crashed = CancellationToken::new();
        *self.trigger.lock().unwrap() = Some((Box::new(trigger), crashed.clone()));
        crashed
    }
    fn revive(&self) {
        self.trigger.lock().unwrap().take();
        self.dead.store(false, Ordering::SeqCst);
    }
    fn check(&self, kind: &str, value: &Value) -> anyhow::Result<()> {
        if self.dead.load(Ordering::SeqCst) {
            anyhow::bail!("the process died");
        }
        let mut trigger = self.trigger.lock().unwrap();
        if trigger.as_ref().is_some_and(|(matches, _)| matches(kind, &value.to_string())) {
            let (_, crashed) = trigger.take().unwrap();
            self.dead.store(true, Ordering::SeqCst);
            crashed.cancel();
            anyhow::bail!("the process died at this write");
        }
        Ok(())
    }
}

struct Crashing {
    inner: Arc<MemoryBackend>,
    crash: Arc<Crash>,
}

struct CrashingTx<'a> {
    tx: Box<dyn Transaction + 'a>,
    crash: &'a Crash,
    wrote: bool,
}

#[async_trait]
impl AtomicBackend for Crashing {
    async fn begin(&self) -> anyhow::Result<Box<dyn Transaction + '_>> {
        Ok(Box::new(CrashingTx { tx: self.inner.begin().await?, crash: &self.crash, wrote: false }))
    }
    async fn snapshot(&self) -> anyhow::Result<Snapshot> {
        self.inner.snapshot().await
    }
}

#[async_trait]
impl Transaction for CrashingTx<'_> {
    async fn get(&mut self, kind: &str, id: &str) -> anyhow::Result<Option<Value>> {
        self.tx.get(kind, id).await
    }
    async fn scan(&mut self, kind: &str, start: &str, end: Option<&str>, limit: usize) -> anyhow::Result<Rows> {
        self.tx.scan(kind, start, end, limit).await
    }
    async fn put(&mut self, kind: &str, id: &str, value: &Value) -> anyhow::Result<()> {
        self.wrote = true;
        self.crash.check(kind, value)?;
        self.tx.put(kind, id, value).await
    }
    async fn delete(&mut self, kind: &str, id: &str) -> anyhow::Result<()> {
        self.wrote = true;
        self.crash.check(kind, &Value::Null)?;
        self.tx.delete(kind, id).await
    }
    async fn commit(self: Box<Self>) -> anyhow::Result<()> {
        if self.wrote && self.crash.dead.load(Ordering::SeqCst) {
            self.tx.rollback().await;
            anyhow::bail!("the process died");
        }
        self.tx.commit().await
    }
    async fn rollback(self: Box<Self>) {
        self.tx.rollback().await
    }
}

// What outlives a process: the engine's rows and agent.db.
struct Stores {
    backend: Arc<MemoryBackend>,
    crash: Arc<Crash>,
    db: Arc<AgentDb>,
}

fn stores() -> Stores {
    Stores {
        backend: Arc::new(MemoryBackend::default()),
        crash: Arc::default(),
        db: Arc::new(AgentDb::open(":memory:").unwrap()),
    }
}

// The time of a process started after another died holding its leases.
struct Ahead(i64);

impl ouro_runtime::durable::Clock for Ahead {
    fn now_ms(&self) -> i64 {
        chrono::Utc::now().timestamp_millis() + self.0
    }
}

fn guard(label: &str) -> Arc<GuardApi> {
    let api = Arc::new(GuardApi::default());
    *api.label.lock().unwrap() = Some(label.into());
    api
}

// A process on `s`, its clock `ahead_ms` ahead.
fn engine_on(
    s: &Stores,
    model: Arc<dyn ChatProvider>,
    world: Arc<World>,
    api: &Arc<GuardApi>,
    ahead_ms: i64,
) -> Arc<Engine> {
    engine_with(s, model, world, api.clone(), ahead_ms)
}

// The same, with `api` answering Jev's questions.
fn engine_with(
    s: &Stores,
    model: Arc<dyn ChatProvider>,
    world: Arc<World>,
    api: Arc<dyn agent::providers::JsonApi>,
    ahead_ms: i64,
) -> Arc<Engine> {
    let p = |model: &str| ModelPreset { model: model.into(), reasoning_effort: ReasoningEffort::Disabled };
    let backend = Arc::new(Crashing { inner: s.backend.clone(), crash: s.crash.clone() });
    let view: Arc<dyn AtomicBackend> = Arc::new(s.backend.read_only_view());
    let tracer = agent::local_recorder::local_recorder_tracer(s.db.clone());
    let deps = EngineDeps {
        providers: Providers { openai: model.clone(), gemini: model.clone(), deepseek: model, openrouter: None },
        mcp: world,
        presets: Presets {
            base: p("gpt-5.4-mini"),
            smart: p("gpt-5.4"),
            smartest: p("gpt-5.4"),
            compiler: p("gpt-5.4"),
            compose: p("gpt-5.4"),
        },
        skills: Vec::new(),
        skill_store: Arc::new(AnySkill),
        memory: s.db.clone(),
        tracer: Arc::new(tracer),
        codex: Arc::new(NoCodex),
        decisions: Some(agent::decisions::decision_client("guard-test".into(), api)),
        env: Default::default(),
        db: (*s.db).clone(),
        store: Arc::new(TransactionalStore::new(backend)),
        dry_store: Arc::new(TransactionalStore::new(view)),
        schedule: Arc::new(agent::schedule::Schedules::in_memory()),
    };
    create_engine_on(deps, agent::runs::config(), Arc::new(Ahead(ahead_ms)))
}

fn options(id: &str) -> LoopOptions {
    let mut opts = LoopOptions::new(id, task_context(id));
    opts.skills = vec!["any".into()];
    opts.include_engine_skills = false;
    opts
}

async fn start(engine: &Arc<Engine>, id: &str) -> Arc<AgentLoop> {
    engine.start_loop(options(id)).await.unwrap()
}

async fn ended(engine: &Engine, run: &str) -> Status {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = engine.runtime.get(run).await.unwrap().and_then(|r| r.outcome) {
            return status;
        }
        assert!(tokio::time::Instant::now() < deadline, "{run} never ended");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// Every run of definition `name`, live or ended.
async fn named(engine: &Engine, name: &str) -> Vec<ouro_runtime::durable::SavedRun> {
    let live = agent::runs::Runs::default();
    let ended = agent::runs::Runs { since_ms: Some(0), before_ms: Some(i64::MAX / 4), ..Default::default() };
    let mut ids: Vec<String> = Vec::new();
    for page in
        [engine.control.list(&live, None, 500).await.unwrap(), engine.control.history(&ended, None, 500).await.unwrap()]
    {
        for run in page.items {
            if run.admission.name == name && !ids.contains(&run.admission.id) {
                ids.push(run.admission.id);
            }
        }
    }
    let mut runs = Vec::new();
    for id in ids {
        runs.push(engine.runtime.get(&id).await.unwrap().unwrap());
    }
    runs
}

// What the model was given in its `request`th request, as text.
fn seen(model: &Scripted, request: usize) -> String {
    serde_json::to_string(&model.seen.lock().unwrap()[request].messages).unwrap()
}

fn system(model: &Scripted, request: usize) -> String {
    model.seen.lock().unwrap()[request].messages[0]["content"].as_str().unwrap().to_owned()
}

fn observations(db: &AgentDb, trace: &str) -> Vec<Observation> {
    db.get_trace(trace).unwrap().unwrap_or_else(|| panic!("no trace {trace}")).1
}

// The names of the observations above `id`, nearest first.
fn ancestors(observations: &[Observation], id: &str) -> Vec<String> {
    let by_id: std::collections::HashMap<&str, &Observation> =
        observations.iter().map(|o| (o.id.as_str(), o)).collect();
    let mut chain = Vec::new();
    let mut at = by_id.get(id).and_then(|o| o.parent_observation_id.clone());
    while let Some(parent) = at {
        let Some(o) = by_id.get(parent.as_str()) else { break };
        chain.push(o.name.clone());
        at = o.parent_observation_id.clone();
    }
    chain
}

// Waits for the process to die where the test made it.
async fn fired(stop: &CancellationToken) {
    tokio::time::timeout(Duration::from_secs(30), stop.cancelled()).await.expect("the process never got there");
}

fn verdict_uses(db: &AgentDb) -> i64 {
    db.conn().query_row("SELECT count(*) FROM guard_verdict_uses", [], |r| r.get(0)).unwrap()
}

// ── the reflexes ────────────────────────────────────────────────────────────

fn reflex(name: &str, description: &str, input: Value, required: &[&str], body: &str) -> String {
    let declaration = json!({"reflex": {"description": description, "input": input, "required": required}});
    format!(
        "document: {{ dsl: '1.0.4', namespace: agent, name: {name}, version: '1.0.0', metadata: {declaration} }}\n{body}"
    )
}

fn news_window() -> String {
    reflex(
        "news-window",
        "Lists the news of a time window.",
        json!({"since": "the window's start, ISO", "query": "what to look for"}),
        &["since"],
        "do:\n  - list:\n      call: news_list\n      with: { since: '${ $workflow.input.since }', query: '${ $workflow.input.query // \"\" }' }\n",
    )
}

fn news_send() -> String {
    reflex(
        "news-send",
        "Sends a note to the news channel.",
        json!({"text": "the note"}),
        &["text"],
        "do:\n  - post:\n      call: send\n      with: { text: '${ $workflow.input.text }' }\noutput:\n  as: '${ \"sent: \" + $workflow.input.text }'\n",
    )
}

fn fetch_page() -> String {
    reflex(
        "fetch-page",
        "Reads one web page.",
        json!({"url": "the page"}),
        &["url"],
        "do:\n  - read:\n      call: fetch_url\n      with: { url: '${ $workflow.input.url }' }\n",
    )
}

// A reflex that starts `child` with `input` and answers with its output:
// engine tasks only.
fn calling(name: &str, description: &str, params: Value, required: &[&str], child: &str, input: &str) -> String {
    let body = format!(
        "do:\n  - start:\n      call: start_child\n      with: {{ name: {child}, host: inherit, input: {input} }}\n      export: {{ as: '${{ $context + {{ child: . }} }}' }}\n  - wait:\n      listen: {{ to: {{ one: {{ with: {{ type: '${{ $context.child.done }}' }} }} }} }}\n  - outcome:\n      call: child_outcome\n      with: {{ id: '${{ $context.child.id }}', key: '${{ $context.child.key }}' }}\noutput:\n  as: '${{ .Completed }}'\n"
    );
    reflex(name, description, params, required, &body)
}

fn news_about() -> String {
    calling(
        "news-about",
        "The news about a topic since a time.",
        json!({"since": "start, ISO", "topic": "the topic"}),
        &["since", "topic"],
        "news-window",
        "{ since: '${ $workflow.input.since }', query: '${ $workflow.input.topic }' }",
    )
}

fn news_brief() -> String {
    calling(
        "news-brief",
        "A brief on a topic.",
        json!({"since": "start, ISO", "topic": "the topic"}),
        &["since", "topic"],
        "news-about",
        "{ since: '${ $workflow.input.since }', topic: '${ $workflow.input.topic }' }",
    )
}

// Saves, marks tried and activates `definition` without starting the
// engine's worker: a process that only changes the catalog.
async fn activate_quietly(engine: &Arc<Engine>, name: &str, definition: &str) {
    let saved = agent::flows::tool(
        engine,
        "save_workflow",
        json!({"name": name, "definition": definition}).as_object().unwrap(),
    )
    .await
    .unwrap_or_else(|e| panic!("saving {name}: {e:#}"));
    let version = serde_json::from_str::<Value>(&saved).unwrap()["version"].as_u64().unwrap();
    support::mark_tried(engine, name, definition);
    agent::flows::tool(engine, "activate_workflow", json!({"name": name, "version": version}).as_object().unwrap())
        .await
        .unwrap_or_else(|e| panic!("activating {name}: {e:#}"));
}

async fn save(engine: &Arc<Engine>, name: &str, definition: &str) -> anyhow::Result<Value> {
    let saved = agent::flows::tool(
        engine,
        "save_workflow",
        json!({"name": name, "definition": definition}).as_object().unwrap(),
    )
    .await?;
    Ok(serde_json::from_str(&saved)?)
}

// ── 1. a read reflex ────────────────────────────────────────────────────────

// Test 1: a closed-world read reflex answers in the round that called it,
// with no review; its work traces under the call's span.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_reflex_answers_in_the_same_round_without_a_review() {
    let s = stores();
    let model = Arc::new(Scripted::new(vec![
        calls(&[("run_reflex", json!({"name": "news-window", "input": {"since": "2026-10-27", "query": "rust"}}))]),
        calls(&[("finish", json!({"result": "done"}))]),
    ]));
    let world = Arc::new(World::default());
    let api = guard("allow");
    let engine = engine_on(&s, model.clone(), world.clone(), &api, 0);
    support::activate_definition(&engine, "news-window", &news_window()).await;
    assert_eq!(start(&engine, "r1").await.send("what about rust".into()).await.unwrap(), "done");
    assert_eq!(world.args("news_list"), [json!({"since": "2026-10-27", "query": "rust"})]);
    let reply = seen(&model, 1);
    assert!(reply.contains("news since 2026-10-27"), "the reflex's output is the reply: {reply}");
    assert!(api.requests.lock().unwrap().is_empty(), "a closed-world read needs no review");
    let runs = named(&engine, "news-window").await;
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].child.as_ref().unwrap().parent, "r1", "a host child of the loop's run");
    assert!(runs[0].admission.input.get("env").is_none(), "its input is its parameters: {}", runs[0].admission.input);
    engine.shutdown().await;
    let observations = observations(&s.db, "r1");
    let list = observations.iter().find(|o| o.name == "news_list").expect("the reflex's call");
    let above = ancestors(&observations, &list.id);
    assert!(above.contains(&"run_reflex".to_owned()), "the reflex nests under the call's span: {above:?}");
}

// R2: a reflex that reads the open world is reviewed like a direct
// open-world read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reflex_reading_the_open_world_goes_to_the_guard() {
    let s = stores();
    let model = Arc::new(Scripted::new(vec![
        calls(&[("run_reflex", json!({"name": "fetch-page", "input": {"url": "https://example.com/a"}}))]),
        assistant("done"),
    ]));
    let world = Arc::new(World::default());
    let api = guard("allow");
    let engine = engine_on(&s, model.clone(), world.clone(), &api, 0);
    support::activate_definition(&engine, "fetch-page", &fetch_page()).await;
    assert_eq!(start(&engine, "r2").await.send("read it".into()).await.unwrap(), "done");
    let requests = api.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 1, "reviewed once");
    assert_eq!(requests[0]["state"]["calls"][0]["name"], "run_reflex");
    assert!(requests[0]["state"]["calls"][0]["reflex"].to_string().contains("call: fetch_url"), "{}", requests[0]);
    assert_eq!(world.count("fetch_url"), 1);
    assert!(seen(&model, 1).contains("page of https://example.com/a"));
    engine.shutdown().await;
}

// ── 2. a write reflex ───────────────────────────────────────────────────────

// Test 2: a write reflex goes through the guard, which sees its YAML and
// its input; a block starts nothing, an allow starts it once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_reflex_is_reviewed_with_its_yaml_and_input_and_starts_only_when_allowed() {
    for label in ["misaligned", "allow"] {
        let s = stores();
        let model = Arc::new(Scripted::new(vec![
            calls(&[("run_reflex", json!({"name": "news-send", "input": {"text": "hello"}}))]),
            assistant("done"),
        ]));
        let world = Arc::new(World::default());
        let api = guard(label);
        let engine = engine_on(&s, model.clone(), world.clone(), &api, 0);
        support::activate_definition(&engine, "news-send", &news_send()).await;
        assert_eq!(start(&engine, "w").await.send("post hello".into()).await.unwrap(), "done", "{label}");
        let requests = api.requests.lock().unwrap().clone();
        assert_eq!(requests.len(), 1, "{label}");
        let call = &requests[0]["state"]["calls"][0];
        assert_eq!(call["name"], "run_reflex");
        assert!(call["arguments"].as_str().unwrap().contains("hello"), "the input: {call}");
        let evidence = call["reflex"].to_string();
        assert!(evidence.contains("news-send") && evidence.contains("call: send"), "the YAML: {evidence}");
        let reply = seen(&model, 1);
        if label == "misaligned" {
            assert_eq!(world.count("send"), 0, "blocked: nothing sent");
            assert!(named(&engine, "news-send").await.is_empty(), "blocked: nothing started");
            assert!(reply.contains("[guard] blocked: misaligned"), "{reply}");
        } else {
            assert_eq!(world.args("send"), [json!({"text": "hello"})]);
            assert_eq!(named(&engine, "news-send").await.len(), 1, "started once");
            assert!(reply.contains("sent: hello"), "{reply}");
        }
        engine.shutdown().await;
    }
}

// Test 2: the process dies in the reflex's write; the next one joins the
// same run and never sends it again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_inside_a_write_reflex_joins_its_run_and_never_sends_twice() {
    let s = stores();
    let world = Arc::new(World::default());
    let stop = world.stop_on("send");
    let api = guard("allow");
    let first = Arc::new(Scripted::new(vec![calls(&[(
        "run_reflex",
        json!({"name": "news-send", "input": {"text": "hello"}}),
    )])]));
    let engine1 = engine_on(&s, first.clone(), world.clone(), &api, 0);
    support::activate_definition(&engine1, "news-send", &news_send()).await;
    let sent = tokio::spawn(start(&engine1, "w3").await.send("post hello".into()));
    fired(&stop).await;
    engine1.shutdown().await;
    let _ = sent.await;
    let runs = named(&engine1, "news-send").await;
    assert_eq!(runs.len(), 1);
    assert!(runs[0].outcome.is_none(), "cut in the middle");

    let second = Arc::new(Scripted::new(vec![assistant("after the restart")]));
    let engine2 = engine_on(&s, second.clone(), world.clone(), &api, 0);
    engine2.ready().await.unwrap();
    assert_eq!(ended(&engine2, "w3").await, Status::Completed(json!("after the restart")));
    assert_eq!(world.count("send"), 1, "the write is never sent again");
    assert_eq!(named(&engine2, "news-send").await.len(), 1, "the same run, joined");
    assert!(seen(&second, 0).contains("[run_reflex error]"), "the model hears it may have run: {}", seen(&second, 0));
    assert_eq!(api.requests.lock().unwrap().len(), 1, "reviewed once");
    engine2.shutdown().await;
}

// R4: the process dies after the start spent the call's verdict, before the
// reflex was admitted. The next one replays the spent verdict and starts
// the reflex once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_after_the_verdict_is_spent_and_before_the_admission_starts_the_reflex_once() {
    let s = stores();
    let world = Arc::new(World::default());
    let api = guard("allow");
    let first = Arc::new(Scripted::new(vec![calls(&[(
        "run_reflex",
        json!({"name": "news-send", "input": {"text": "hello"}}),
    )])]));
    let engine1 = engine_on(&s, first.clone(), world.clone(), &api, 0);
    support::activate_definition(&engine1, "news-send", &news_send()).await;
    // The reflex's own admission (the round's guard child, admitted
    // before, names it in its input).
    let crashed = s.crash.arm(|kind, value| {
        kind == "run" && value.contains("\"name\":\"news-send\"") && !value.contains("\"name\":\"guard\"")
    });
    let sent = tokio::spawn(start(&engine1, "w4").await.send("post hello".into()));
    fired(&crashed).await;
    engine1.shutdown().await;
    let _ = sent.await;
    assert!(named(&engine1, "news-send").await.is_empty(), "never admitted");
    assert_eq!(verdict_uses(&s.db), 1, "the verdict was spent");

    s.crash.revive();
    let second = Arc::new(Scripted::new(vec![assistant("done")]));
    let engine2 = engine_on(&s, second.clone(), world.clone(), &api, 60_000);
    engine2.ready().await.unwrap();
    assert_eq!(ended(&engine2, "w4").await, Status::Completed(json!("done")));
    assert_eq!(world.args("send"), [json!({"text": "hello"})], "sent once");
    assert_eq!(named(&engine2, "news-send").await.len(), 1, "started once");
    assert_eq!(verdict_uses(&s.db), 1, "the same verdict, replayed");
    assert_eq!(api.requests.lock().unwrap().len(), 1, "not reviewed again");
    assert!(seen(&second, 0).contains("sent: hello"), "{}", seen(&second, 0));
    engine2.shutdown().await;
}

// R4: the process dies after the reflex was admitted, before the start's
// reply was checkpointed. The next one joins the reflex.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_after_the_admission_and_before_the_starts_checkpoint_joins_the_reflex() {
    let s = stores();
    let world = Arc::new(World::default());
    let api = guard("allow");
    let first = Arc::new(Scripted::new(vec![calls(&[(
        "run_reflex",
        json!({"name": "news-send", "input": {"text": "hello"}}),
    )])]));
    let engine1 = engine_on(&s, first.clone(), world.clone(), &api, 0);
    support::activate_definition(&engine1, "news-send", &news_send()).await;
    let crashed = s.crash.arm(|kind, value| {
        kind.starts_with("record:") && value.contains("\"function\":\"start_reflex\"") && value.contains("\"Done\"")
    });
    let sent = tokio::spawn(start(&engine1, "w5").await.send("post hello".into()));
    fired(&crashed).await;
    engine1.shutdown().await;
    let _ = sent.await;
    assert_eq!(named(&engine1, "news-send").await.len(), 1, "admitted");

    s.crash.revive();
    let second = Arc::new(Scripted::new(vec![assistant("done")]));
    let engine2 = engine_on(&s, second.clone(), world.clone(), &api, 60_000);
    engine2.ready().await.unwrap();
    assert_eq!(ended(&engine2, "w5").await, Status::Completed(json!("done")));
    assert_eq!(named(&engine2, "news-send").await.len(), 1, "joined, not started again");
    assert_eq!(world.count("send"), 1, "sent once");
    assert_eq!(verdict_uses(&s.db), 1);
    engine2.shutdown().await;
}

// ── 3. a reflex calls a reflex ──────────────────────────────────────────────

// Test 3: run_reflex(news-brief) → news-about (engine tasks only) →
// news-window (MCP). The process dies in the innermost call; the next one
// finishes it, the output reaches the loop, and the innermost work traces
// under news-about under news-brief under the call's span, one span each.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reflex_composes_reflexes_three_deep_across_a_restart() {
    let s = stores();
    let world = Arc::new(World::default());
    let stop = world.stop_on("news_list");
    let api = guard("allow");
    let first = Arc::new(Scripted::new(vec![calls(&[(
        "run_reflex",
        json!({"name": "news-brief", "input": {"since": "2026-10-27", "topic": "rust"}}),
    )])]));
    let engine1 = engine_on(&s, first.clone(), world.clone(), &api, 0);
    support::activate_definition(&engine1, "news-window", &news_window()).await;
    support::activate_definition(&engine1, "news-about", &news_about()).await;
    support::activate_definition(&engine1, "news-brief", &news_brief()).await;
    let sent = tokio::spawn(start(&engine1, "c1").await.send("brief me".into()));
    fired(&stop).await;
    engine1.shutdown().await;
    let _ = sent.await;

    let second = Arc::new(Scripted::new(vec![assistant("briefed")]));
    let engine2 = engine_on(&s, second.clone(), world.clone(), &api, 0);
    engine2.ready().await.unwrap();
    assert_eq!(ended(&engine2, "c1").await, Status::Completed(json!("briefed")));
    let reply = seen(&second, 0);
    assert!(reply.contains("news since 2026-10-27") && reply.contains("rust"), "the output reaches the loop: {reply}");
    assert_eq!(world.count("news_list"), 2, "the read was asked again after the restart");
    for name in ["news-brief", "news-about", "news-window"] {
        assert_eq!(named(&engine2, name).await.len(), 1, "{name} ran once");
    }
    assert!(api.requests.lock().unwrap().is_empty(), "a composed closed-world read needs no review");
    engine2.shutdown().await;

    let observations = observations(&s.db, "c1");
    for name in ["news-brief", "news-about", "news-window"] {
        let spans = observations.iter().filter(|o| o.name == name).count();
        assert_eq!(spans, 1, "one span for {name}: {:?}", observations.iter().map(|o| &o.name).collect::<Vec<_>>());
    }
    let list = observations.iter().rev().find(|o| o.name == "news_list").expect("the innermost call");
    let above: Vec<String> = ancestors(&observations, &list.id)
        .into_iter()
        .filter(|n| ["news-window", "news-about", "news-brief", "run_reflex"].contains(&n.as_str()))
        .collect();
    assert_eq!(above, ["news-window", "news-about", "news-brief", "run_reflex"], "{above:?}");
}

// ── 4. the catalog ──────────────────────────────────────────────────────────

// Test 4: the loop's instructions list the active, tried reflexes with their
// parameters and whether they write; an untried or inactive one is not
// listed; a restart keeps the block the loop was admitted with.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_catalog_lists_active_tried_reflexes_and_a_restart_keeps_the_admitted_block() {
    let s = stores();
    let world = Arc::new(World::default());
    let stop = world.stop_on("send");
    let api = guard("allow");
    let first = Arc::new(Scripted::new(vec![calls(&[("send", json!({"text": "x"}))])]));
    let engine1 = engine_on(&s, first.clone(), world.clone(), &api, 0);
    support::activate_definition(&engine1, "news-window", &news_window()).await;
    support::activate_definition(&engine1, "news-send", &news_send()).await;
    support::store_definition(&engine1, "news-untried", &news_window().replace("news-window", "news-untried")).await;
    save(&engine1, "news-saved", &news_window().replace("news-window", "news-saved")).await.unwrap();
    let sent = tokio::spawn(start(&engine1, "k1").await.send("go".into()));
    fired(&stop).await;
    engine1.shutdown().await;
    let _ = sent.await;
    let admitted = system(&first, 0);
    assert!(admitted.contains("news-window(since*, query)"), "{admitted}");
    assert!(admitted.contains("Lists the news of a time window."), "{admitted}");
    let send_line = admitted.lines().find(|l| l.contains("news-send(")).expect("news-send is listed");
    assert!(send_line.contains("text*") && send_line.contains("[writes]"), "{send_line}");
    let window_line = admitted.lines().find(|l| l.contains("news-window(")).unwrap();
    assert!(!window_line.contains("[writes]"), "{window_line}");
    assert!(!admitted.contains("news-untried"), "an untried version is not listed");
    assert!(!admitted.contains("news-saved"), "a draft is not listed");

    // Another reflex is activated while the loop is down.
    let quiet = engine_on(&s, Arc::new(Scripted::new(vec![])), world.clone(), &api, 0);
    activate_quietly(&quiet, "news-late", &news_window().replace("news-window", "news-late")).await;
    quiet.shutdown().await;

    let second = Arc::new(Scripted::new(vec![assistant("done"), assistant("fresh")]));
    let engine2 = engine_on(&s, second.clone(), world.clone(), &api, 0);
    engine2.ready().await.unwrap();
    assert_eq!(ended(&engine2, "k1").await, Status::Completed(json!("done")));
    assert_eq!(system(&second, 0), admitted, "the admitted block, after a restart");
    assert!(!system(&second, 0).contains("news-late"));
    assert_eq!(start(&engine2, "k2").await.send("go".into()).await.unwrap(), "fresh");
    assert!(system(&second, 1).contains("news-late("), "a new loop sees it");
    engine2.shutdown().await;
}

// Jev, judging one reflex useful for every request and the others not; the
// guard's questions answered as `GuardApi` answers them.
#[derive(Default)]
struct Chooser {
    requests: Mutex<Vec<Value>>,
}
#[async_trait]
impl agent::providers::JsonApi for Chooser {
    async fn post(&self, url: &str, body: Value, cancel: &CancellationToken) -> anyhow::Result<Value> {
        let Some(reflexes) = body["state"]["reflexes"].as_array().cloned() else {
            return GuardApi::default().post(url, body, cancel).await;
        };
        self.requests.lock().unwrap().push(body["state"].clone());
        let answers: Map<String, Value> = reflexes
            .iter()
            .enumerate()
            .map(|(n, r)| {
                (
                    format!("reflex_{n}"),
                    json!({"type": "noul", "noul": if r["name"] == "news-window" { 0.95 } else { 0.1 }}),
                )
            })
            .collect();
        Ok(json!({"model": "jev-test", "answers": answers}))
    }
}

// Past a few reflexes, the loop's instructions list only those Jev chose for
// its request, and find_reflexes searches them all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn past_a_few_reflexes_the_instructions_list_the_ones_chosen_for_the_request() {
    let s = stores();
    let jev = Arc::new(Chooser::default());
    let model = Arc::new(Scripted::new(vec![
        calls(&[("find_reflexes", json!({"query": "the news of today"}))]),
        assistant("done"),
    ]));
    let engine = engine_with(&s, model.clone(), Arc::new(World::default()), jev.clone(), 0);
    support::activate_definition(&engine, "news-window", &news_window()).await;
    for n in ["a", "b", "c", "d", "e"] {
        let name = format!("news-{n}");
        support::activate_definition(&engine, &name, &news_window().replace("news-window", &name)).await;
    }
    let mut opts = options("k1");
    opts.reflex_request = Some("what is in the news".into());
    assert_eq!(engine.start_loop(opts).await.unwrap().send("go".into()).await.unwrap(), "done");

    let admitted = system(&model, 0);
    assert!(
        admitted.contains("Chosen for this request, 1 of 6; find_reflexes(query) searches the others"),
        "{admitted}"
    );
    assert!(admitted.contains("- news-window(since*, query) [reads]"), "{admitted}");
    assert!(!admitted.contains("news-a("), "an unchosen reflex is not listed: {admitted}");
    let asked = jev.requests.lock().unwrap().clone();
    assert_eq!(asked.len(), 2, "the block, chosen once, and the search");
    assert_eq!(asked[0]["current_request"], "what is in the news");
    assert_eq!(asked[0]["reflexes"].as_array().unwrap().len(), 6);
    assert_eq!(asked[1]["current_request"], "the news of today");

    let reply = seen(&model, 1);
    assert!(
        reply.contains("news-window(since*, query)") && reply.contains("saved") && reply.contains(":6}"),
        "{reply}"
    );
    assert!(!reply.contains("news-a("), "{reply}");
    engine.shutdown().await;
}

// R6: the catalog and list_workflows read the active version: a newer
// untried draft with other parameters changes neither the line nor the
// active declaration list_workflows gives.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_catalog_reads_the_active_version_not_a_newer_draft() {
    let s = stores();
    let model = Arc::new(Scripted::new(vec![assistant("ok")]));
    let world = Arc::new(World::default());
    let api = guard("allow");
    let engine = engine_on(&s, model.clone(), world.clone(), &api, 0);
    support::activate_definition(&engine, "news-window", &news_window()).await;
    let v2 = news_window()
        .replace("\"since\":\"the window's start, ISO\"", "\"from\":\"the start\"")
        .replace("required\":[\"since\"]", "required\":[\"from\"]");
    assert_ne!(v2, news_window());
    save(&engine, "news-window", &v2).await.unwrap();
    assert_eq!(start(&engine, "k3").await.send("go".into()).await.unwrap(), "ok");
    let admitted = system(&model, 0);
    assert!(admitted.contains("news-window(since*, query)"), "{admitted}");
    assert!(!admitted.contains("news-window(from"), "{admitted}");
    let listed = agent::flows::tool(&engine, "list_workflows", &Map::new()).await.unwrap();
    let listed: Value = serde_json::from_str(&listed).unwrap();
    let entry = listed["workflows"].as_array().unwrap().iter().find(|w| w["name"] == "news-window").unwrap().clone();
    assert_eq!(entry["active_version"], 1);
    assert_eq!(entry["latest_version"], 2);
    assert!(entry["active_reflex"]["input"].get("since").is_some(), "{entry}");
    assert_eq!(entry["active_reflex"]["writes"], false, "{entry}");
    assert!(entry["latest_reflex"]["input"].get("from").is_some(), "{entry}");
    engine.shutdown().await;
}

// ── 5, 6. calls that start nothing ──────────────────────────────────────────

// Test 5: an unknown or a missing parameter, or a name that is no reflex, is
// an error reply, and no run starts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bad_input_is_an_error_reply_and_starts_nothing() {
    let s = stores();
    let model = Arc::new(Scripted::new(vec![
        calls(&[
            ("run_reflex", json!({"name": "news-window", "input": {"query": "rust"}})),
            ("run_reflex", json!({"name": "news-window", "input": {"since": "2026-10-27", "colour": "red"}})),
            ("run_reflex", json!({"name": "nope", "input": {}})),
        ]),
        assistant("done"),
    ]));
    let world = Arc::new(World::default());
    let api = guard("allow");
    let engine = engine_on(&s, model.clone(), world.clone(), &api, 0);
    support::activate_definition(&engine, "news-window", &news_window()).await;
    assert_eq!(start(&engine, "b1").await.send("go".into()).await.unwrap(), "done");
    let replies: Vec<String> = model.seen.lock().unwrap()[1]
        .messages
        .iter()
        .filter(|m| m["role"] == "tool")
        .map(|m| m["content"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(replies.len(), 3);
    assert!(replies[0].contains("[run_reflex error]") && replies[0].contains("since"), "{}", replies[0]);
    assert!(replies[1].contains("[run_reflex error]") && replies[1].contains("colour"), "{}", replies[1]);
    assert!(replies[2].contains("[run_reflex error]") && replies[2].contains("nope"), "{}", replies[2]);
    assert!(named(&engine, "news-window").await.is_empty(), "nothing started");
    assert_eq!(world.count("news_list"), 0);
    engine.shutdown().await;
}

// Test 6: a reflex an operator paused, or one its breaker tripped, is an
// error reply; nothing runs in its place.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_paused_or_tripped_reflex_is_an_error_reply_and_nothing_runs_in_its_place() {
    let s = stores();
    let broken = reflex(
        "news-broken",
        "Always fails.",
        json!({}),
        &[],
        "do:\n  - boom:\n      raise: { error: { type: 'https://example.com/boom', status: 500, detail: boom } }\n",
    );
    let call = |name: &str| calls(&[("run_reflex", json!({"name": name, "input": {"since": "2026-10-27"}}))]);
    let call_broken = || calls(&[("run_reflex", json!({"name": "news-broken"}))]);
    let model = Arc::new(Scripted::new(vec![
        call("news-window"),
        assistant("paused"),
        call_broken(),
        call_broken(),
        call_broken(),
        call_broken(),
        assistant("tripped"),
    ]));
    let world = Arc::new(World::default());
    let api = guard("allow");
    let engine = engine_on(&s, model.clone(), world.clone(), &api, 0);
    support::activate_definition(&engine, "news-window", &news_window()).await;
    support::activate_definition(&engine, "news-broken", &broken).await;
    let request =
        ouro_runtime::control::Request { id: "pause".into(), actor: "operator".into(), reason: "test".into() };
    engine.control.pause(&request, "news-window", "maintenance").await.unwrap();
    assert_eq!(start(&engine, "p1").await.send("go".into()).await.unwrap(), "paused");
    let reply = seen(&model, 1);
    assert!(reply.contains("[run_reflex error]") && reply.contains("paused"), "{reply}");
    assert!(named(&engine, "news-window").await.is_empty(), "nothing ran");
    assert_eq!(world.count("news_list"), 0);

    assert_eq!(start(&engine, "p2").await.send("go".into()).await.unwrap(), "tripped");
    for request in 3..6 {
        assert!(seen(&model, request).contains("[run_reflex error]"), "fault {}", request - 2);
    }
    let last = seen(&model, 6);
    assert!(last.contains("[run_reflex error]") && last.contains("tripped"), "{last}");
    assert_eq!(named(&engine, "news-broken").await.len(), 3, "the fourth call started nothing");
    engine.shutdown().await;
}

// ── 7. the gate ─────────────────────────────────────────────────────────────

// Test 7: the gate checks the declaration; a reflex receives everything
// through its parameters, never `$workflow.input.env`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_gate_checks_a_reflexs_declaration_and_refuses_the_env() {
    let s = stores();
    let engine = engine_on(&s, Arc::new(Scripted::new(vec![])), Arc::new(World::default()), &guard("allow"), 0);
    engine.ready().await.unwrap();
    let body = "do:\n  - list:\n      call: news_list\n      with: { since: '${ $workflow.input.since }' }\n";
    let cases = [
        (reflex("r-a", "", json!({"since": "start"}), &["since"], body), "description"),
        (reflex("r-b", "   ", json!({"since": "start"}), &["since"], body), "description"),
        (
            reflex(
                "r-c",
                "Reads the env.",
                json!({}),
                &[],
                "do:\n  - list:\n      call: news_list\n      with: { since: '${ $workflow.input.env.now }' }\n",
            ),
            "$workflow.input.env",
        ),
        (reflex("r-d", "Lists.", json!({"since": "start"}), &["until"], body), "until"),
        (reflex("r-e", "Lists.", json!({"since-when": "start"}), &[], body), "since-when"),
    ];
    for (definition, problem) in cases {
        let name = definition.split("name: ").nth(1).unwrap().split(',').next().unwrap().to_owned();
        let refused = save(&engine, &name, &definition).await.expect_err(&name).to_string();
        assert!(refused.contains("metadata.reflex") || refused.contains("reflex"), "{name}: {refused}");
        assert!(refused.contains(problem), "{name}: {refused}");
    }
    save(&engine, "news-window", &news_window()).await.unwrap();
    engine.shutdown().await;
}

// ── R5. a dry run of a reflex ───────────────────────────────────────────────

// R5: try_workflow passes a reflex draft its input, checked against its
// declaration; a required parameter that determines a write's arguments
// shows in the recorded write, and only such a try marks it tried.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reflex_draft_is_tried_on_input_its_declaration_accepts() {
    let s = stores();
    let world = Arc::new(World::default());
    let engine = engine_on(&s, Arc::new(Scripted::new(vec![])), world.clone(), &guard("allow"), 0);
    engine.ready().await.unwrap();
    save(&engine, "news-send", &news_send()).await.unwrap();
    let tried = || {
        let digest = ouro_runtime::durable::digest_of(&news_send());
        agent::flows::WorkflowMeta::get(&engine.deps.db, "news-send", &digest)
            .unwrap()
            .is_some_and(|m| m.tried_ms.is_some())
    };
    let attempt = |args: Value| {
        let engine = engine.clone();
        async move {
            let scope = agent::tracing::null_scope();
            let reply = agent::sandbox::try_workflow(
                &engine,
                args.as_object().unwrap(),
                &task_context("author"),
                &scope,
                &CancellationToken::new(),
            )
            .await?;
            anyhow::Ok(serde_json::from_str::<Value>(&reply)?)
        }
    };
    let missing = attempt(json!({"name": "news-send"})).await.unwrap_err().to_string();
    assert!(missing.contains("text"), "{missing}");
    let unknown = attempt(json!({"name": "news-send", "input": {"text": "hi", "colour": "red"}})).await;
    assert!(unknown.unwrap_err().to_string().contains("colour"));
    assert!(!tried(), "no try with valid input yet");
    let report = attempt(json!({"name": "news-send", "input": {"text": "hello"}})).await.unwrap();
    assert_eq!(report["status"], "completed", "{report:#}");
    assert_eq!(report["writes"][0]["function"], "send", "{report:#}");
    assert_eq!(report["writes"][0]["arguments"]["text"], "hello", "{report:#}");
    assert_eq!(world.count("send"), 0, "a dry run sends nothing");
    assert!(tried());
    engine.shutdown().await;
}
