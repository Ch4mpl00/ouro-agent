// Scheduled tasks on the engine's triggers (docs/designs/06-schedules.md,
// tests 1–18; test 14 is a unit test in src/schedule.rs; test 12, the seeded
// `dreaming` task, went with the seed, LOG.md 2026-10-26). Every scenario
// runs on a moved clock: the engine's worker fires a task's trigger at its slot, `fire_scheduled`
// records the fire, and the supervisor's drain runs it as a signal.
mod support;
use agent::{
    agent::{Engine, EngineDeps},
    db::TraceStore,
    providers::{ChatProvider, CompletionParams, CompletionResult, Message, ProviderKind, assistant},
    schedule::{Drained, Fire, FireStore, RoundEnd, Schedules, Task},
    supervisor::{Supervisor, create_supervisor},
    tracing::{Trace, TraceStart, Tracer},
};
use async_trait::async_trait;
use serde_json::{Map, Value, json};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use support::*;
use tokio_util::sync::CancellationToken;

const SKILLS: &[(&str, &str)] = &[
    ("orchestrator", "---\ntools: []\n---\nORCHESTRATOR"),
    ("routing", "---\ntools: []\n---\nROUTING"),
    ("scheduler", "---\ntools: *\n---\nSCHEDULER"),
    ("dreaming", "---\ntools: []\n---\nDREAMING"),
    ("recovery", "---\ntools: []\n---\nRECOVERY"),
    ("worker", "---\ntools: *\n---\nWORKER"),
];

// ── the world ───────────────────────────────────────────────────────────────

// The engine's time: the system clock moved to an instant a test names.
#[derive(Default)]
struct Clock(AtomicI64);

impl ouro_runtime::durable::Clock for Clock {
    fn now_ms(&self) -> i64 {
        chrono::Utc::now().timestamp_millis() + self.0.load(Ordering::SeqCst)
    }
}

impl Clock {
    fn set(&self, at: &str) {
        let target = ms(at);
        self.0.store(target - chrono::Utc::now().timestamp_millis(), Ordering::SeqCst);
    }
}

fn ms(at: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(at).unwrap().timestamp_millis()
}

// The model: every request's user text is kept; it answers from its script,
// then "done". A request whose text contains HOLD waits until `released`,
// one that contains FAIL fails once.
#[derive(Default)]
struct Model {
    inputs: Mutex<Vec<String>>,
    script: Mutex<VecDeque<Message>>,
    released: AtomicBool,
    failed: AtomicBool,
}

impl Model {
    fn inputs(&self) -> Vec<String> {
        self.inputs.lock().unwrap().clone()
    }
    // The requests that carried a scheduled fire, in order.
    fn fires(&self) -> Vec<String> {
        self.inputs().into_iter().filter(|i| i.contains("Scheduled task #")).collect()
    }
}

#[async_trait]
impl ChatProvider for Model {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Openai
    }
    async fn complete(&self, p: &mut CompletionParams<'_>) -> anyhow::Result<CompletionResult> {
        let text: Vec<String> = p
            .messages
            .iter()
            .filter(|m| m["role"] == "user")
            .filter_map(|m| m["content"].as_str().map(str::to_owned))
            .collect();
        let text = text.join("\n");
        self.inputs.lock().unwrap().push(text.clone());
        while text.contains("HOLD") && !self.released.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        if text.contains("FAIL") && !self.failed.swap(true, Ordering::SeqCst) {
            anyhow::bail!("the model failed");
        }
        let message = self.script.lock().unwrap().pop_front().unwrap_or_else(|| assistant("done"));
        Ok(CompletionResult { message, finish_reason: Some("stop".into()), usage: None })
    }
}

// Every trace a run starts, by id: a second start of one signal shows here.
struct Counting {
    inner: Arc<dyn Tracer>,
    ids: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl Tracer for Counting {
    fn trace(&self, opts: TraceStart) -> Arc<dyn Trace> {
        self.ids.lock().unwrap().push(opts.id.clone());
        self.inner.trace(opts)
    }
    async fn shutdown(&self) {
        self.inner.shutdown().await
    }
}

// The fire table, failing its first `failing` records (agent.db down).
struct Flaky {
    inner: Arc<dyn FireStore>,
    failing: AtomicUsize,
    attempts: AtomicUsize,
}

#[async_trait]
impl FireStore for Flaky {
    async fn record(&self, run_id: &str, task: &Task, slot_ms: i64) -> anyhow::Result<i64> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        if self.failing.load(Ordering::SeqCst) > 0 {
            self.failing.fetch_sub(1, Ordering::SeqCst);
            anyhow::bail!("database is locked");
        }
        self.inner.record(run_id, task, slot_ms).await
    }
    async fn next_pending(&self) -> anyhow::Result<Option<Fire>> {
        self.inner.next_pending().await
    }
    async fn done(&self, id: i64) -> anyhow::Result<()> {
        self.inner.done(id).await
    }
    async fn latest(&self, task_id: i64) -> anyhow::Result<Option<Fire>> {
        self.inner.latest(task_id).await
    }
    async fn next_task_id(&self) -> anyhow::Result<i64> {
        self.inner.next_task_id().await
    }
}

struct World {
    f: Fixture,
    model: Arc<Model>,
    clock: Arc<Clock>,
    supervisor: Arc<Supervisor>,
    traces: Arc<Mutex<Vec<String>>>,
}

fn configure(model: &Arc<Model>, traces: &Arc<Mutex<Vec<String>>>) -> impl FnOnce(&mut EngineDeps) + use<> {
    let (model, traces) = (model.clone(), traces.clone());
    move |deps: &mut EngineDeps| {
        deps.skills = vec!["orchestrator".into(), "routing".into()];
        deps.providers.openai = model.clone();
        deps.providers.gemini = model.clone();
        deps.providers.deepseek = model;
        deps.tracer = Arc::new(Counting { inner: deps.tracer.clone(), ids: traces });
    }
}

// A world whose clock reads `at`.
async fn world_at(at: &str) -> World {
    world_with(at, |_| {}).await
}

async fn world_with(at: &str, extra: impl FnOnce(&mut EngineDeps)) -> World {
    world_configured(at, agent::runs::config(), extra).await
}

async fn world_configured(at: &str, config: ouro_runtime::Config, extra: impl FnOnce(&mut EngineDeps)) -> World {
    let model = Arc::new(Model::default());
    let clock = Arc::new(Clock::default());
    clock.set(at);
    let traces = Arc::new(Mutex::new(Vec::new()));
    let base = configure(&model, &traces);
    let f = fixture_configured(model.clone(), &[], SKILLS, clock.clone(), config, |deps| {
        base(deps);
        extra(deps);
    });
    f.engine.ready().await.unwrap();
    let supervisor = create_supervisor(f.engine.clone());
    World { f, model, clock, supervisor, traces }
}

impl World {
    fn engine(&self) -> &Arc<Engine> {
        &self.f.engine
    }

    async fn tool(&self, tz: &str, name: &str, args: Value) -> Value {
        let reply = agent::schedule::tool(self.engine(), tz, name, args.as_object().unwrap()).await;
        serde_json::from_str(&reply.unwrap_or_else(|e| panic!("{name}: {e:#}"))).unwrap()
    }

    async fn schedule(&self, tz: &str, cron: &str, recurring: bool, prompt: &str) -> i64 {
        let reply =
            self.tool(tz, "schedule_task", json!({"cron_expr": cron, "recurring": recurring, "prompt": prompt})).await;
        assert_eq!(reply["ok"], true, "{reply}");
        reply["task"]["id"].as_i64().unwrap()
    }

    async fn list(&self) -> Value {
        self.tool("UTC", "list_scheduled_tasks", json!({})).await
    }

    async fn listed(&self, id: i64) -> Option<Value> {
        self.list().await["tasks"].as_array().unwrap().iter().find(|t| t["id"] == id).cloned()
    }

    async fn cancel(&self, id: i64) -> Value {
        self.tool("UTC", "cancel_scheduled_task", json!({"id": id})).await
    }

    // (id, task id, slot, state) of every fire row, oldest first.
    fn fires(&self) -> Vec<(i64, i64, i64, String)> {
        let conn = self.f.db.conn();
        let mut statement = conn.prepare("SELECT id, task_id, slot, state FROM scheduled_fires ORDER BY id").unwrap();
        statement
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    // Waits until the table holds `n` fires.
    async fn until_fires(&self, n: usize) -> Vec<(i64, i64, i64, String)> {
        until(&format!("{n} fires"), || {
            let fires = self.fires();
            (fires.len() >= n).then_some(fires)
        })
        .await
    }

    // Moves the clock to `at` and waits until fire number `n` is recorded.
    async fn fire_at(&self, at: &str, n: usize) -> (i64, i64, i64, String) {
        self.clock.set(at);
        self.until_fires(n).await[n - 1].clone()
    }

    async fn drain(&self) -> Drained {
        agent::schedule::drain(&self.supervisor, None).await.unwrap()
    }

    async fn versions(&self, id: i64) -> usize {
        self.engine().runtime.definitions(&format!("task-{id}")).await.unwrap().versions.len()
    }

    async fn active_task(&self, id: i64) -> Task {
        let definitions = self.engine().runtime.definitions(&format!("task-{id}")).await.unwrap();
        let yaml = &definitions.versions[&definitions.active.unwrap()].yaml;
        agent::schedule::task_of(yaml).unwrap()
    }

    fn trace_starts(&self, id: &str) -> usize {
        self.traces.lock().unwrap().iter().filter(|t| *t == id).count()
    }
}

async fn until<T>(what: &str, mut probe: impl FnMut() -> Option<T>) -> T {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(found) = probe() {
            return found;
        }
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// Nothing more is recorded over several worker ticks.
async fn settles(w: &World, n: usize) {
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(w.fires().len(), n, "{:?}", w.fires());
}

fn header(input: &str, line: &str) -> String {
    input
        .lines()
        .find_map(|l| l.strip_prefix(&format!("{line}: ")))
        .unwrap_or_else(|| panic!("no {line}: {input}"))
        .into()
}

async fn ended(engine: &Engine, run: &str) -> agent::runs::Status {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = engine.runtime.get(run).await.unwrap().and_then(|r| r.outcome) {
            return status;
        }
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for {run}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// The run of task `id` the engine's trigger started (its slot's id).
async fn task_runs(engine: &Engine, id: i64) -> Vec<ouro_runtime::durable::SavedRun> {
    engine.runtime.list(Some(&format!("task-{id}")), "schedule:").await.unwrap()
}

const PROMPT: &str = "Summarize the news.";

// ── 1. a recurring task fires as a signal ───────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recurring_task_fires_at_its_slot_as_one_scheduler_signal() {
    let w = world_at("2026-11-10T06:00:00Z").await;
    let id = w.schedule("Europe/Kyiv", "0 9 * * *", true, PROMPT).await;
    let definitions = w.engine().runtime.definitions(&format!("task-{id}")).await.unwrap();
    let yaml = &definitions.versions[&definitions.active.unwrap()].yaml;
    let wf = ouro_runtime::ows::model::parse_yaml(yaml).unwrap();
    assert_eq!(wf.definition["schedule"]["cron"], "0 9 * * *");
    assert_eq!(wf.definition["document"]["metadata"]["ouro"]["timezone"], "Europe/Kyiv");
    assert_eq!(wf.definition["document"]["metadata"]["ouro"]["breaker"], "off");
    settles(&w, 0).await;

    let (fire, task, slot, state) = w.fire_at("2026-11-10T07:00:00.300Z", 1).await;
    assert!(fire > agent::schedule::FIRST_FIRE_ID, "above every id the old sequence used");
    assert_eq!((task, slot, state.as_str()), (id, ms("2026-11-10T07:00:00Z"), "pending"));
    assert_eq!(w.drain().await, Drained { ran: 1, end: RoundEnd::Empty });
    assert_eq!(w.fires()[0].3, "done");
    let inputs = w.model.fires();
    assert_eq!(inputs.len(), 1, "{inputs:?}");
    let input = &inputs[0];
    assert!(
        input.contains(&format!(
            "Scheduled task #{id} fired.\nCron: 0 9 * * *\nSlot: 2026-11-10T07:00:00.000Z\nNow: 2026-11-10T07:00:"
        )),
        "{input}"
    );
    assert!(
        input.contains("\nPrevious fire: never (this is the first run)\nRecurring: yes\n\nSummarize the news."),
        "{input}"
    );
    assert!(w.f.db.get_trace(&format!("scheduler:{fire}")).unwrap().is_some(), "a scheduler signal's trace");

    let (second, _, _, _) = w.fire_at("2026-11-11T07:00:00.300Z", 2).await;
    assert!(second > fire);
    assert_eq!(w.drain().await.ran, 1);
    let input = &w.model.fires()[1];
    assert_eq!(header(input, "Slot"), "2026-11-11T07:00:00.000Z");
    assert_eq!(header(input, "Previous fire"), "2026-11-10T07:00:00.000Z");
    w.supervisor.shutdown().await;
}

// ── 2. the prompt is never an expression ────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_prompt_that_looks_like_an_expression_arrives_verbatim() {
    let w = world_at("2026-11-10T06:00:00Z").await;
    let prompt = "Report ${ .x } and $run, \"quoted\" # not a comment: really\nsecond line: '${ 1 + 1 }'";
    // A prompt that is a whole expression, which the engine would evaluate.
    let whole = "${ .x }";
    let id = w.schedule("UTC", "now", false, prompt).await;
    w.until_fires(1).await;
    w.schedule("UTC", "now", false, whole).await;
    w.until_fires(2).await;
    assert_eq!(w.drain().await.ran, 2);
    let inputs = w.model.fires();
    assert!(inputs[0].ends_with(&format!("Recurring: no (one-shot)\n\n{prompt}")), "{inputs:?}");
    assert!(inputs[1].ends_with(&format!("Recurring: no (one-shot)\n\n{whole}")), "{inputs:?}");
    assert_eq!(w.active_task(id).await.prompt, prompt);
    // Crons a formatted YAML would break round-trip too (R2-L4).
    for cron in ["*/5 * * * *", "0 9 * * 1,3"] {
        let id = w.schedule("UTC", cron, true, prompt).await;
        let listed = w.listed(id).await.unwrap();
        assert_eq!((listed["cron_expr"].as_str(), listed["prompt"].as_str()), (Some(cron), Some(prompt)));
    }
    w.supervisor.shutdown().await;
}

// ── 3. one-shots ────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_shots_fire_once_leave_the_list_and_never_fire_again_after_a_restart() {
    let w = world_at("2026-11-10T06:00:00Z").await;
    let now = w.schedule("UTC", "now", true, "right away").await;
    let later = w.schedule("UTC", "30 6 * * *", false, "at half past").await;
    let (_, task, _, _) = w.until_fires(1).await[0].clone();
    assert_eq!(task, now, "`now` fires at the next tick, recurring or not");
    let (_, task, slot, _) = w.fire_at("2026-11-10T06:30:00.300Z", 2).await;
    assert_eq!((task, slot), (later, ms("2026-11-10T06:30:00Z")));
    // Fired but not yet run: listed as consumed until its signal ran.
    for id in [now, later] {
        let listed = w.listed(id).await.unwrap();
        assert_eq!((listed["consumed"].as_bool(), listed["next_fire"].is_null()), (Some(true), true), "{listed}");
    }
    assert_eq!(w.drain().await.ran, 2);
    assert_eq!(w.list().await["count"], 0, "consumed one-shots leave the list");
    w.clock.set("2026-11-11T06:30:00.300Z");
    settles(&w, 2).await;

    let (f, model, clock) = (&w.f, w.model.clone(), w.clock.clone());
    w.supervisor.shutdown().await;
    let engine = reopen(f, model.clone(), clock, configure(&model, &w.traces));
    engine.ready().await.unwrap();
    let supervisor = create_supervisor(engine.clone());
    settles(&w, 2).await;
    assert_eq!(agent::schedule::drain(&supervisor, None).await.unwrap().ran, 0);
    assert_eq!(model.fires().len(), 2);
    supervisor.shutdown().await;
}

// ── 4. one run, one row, one signal ─────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fire_scheduled_dispatched_twice_for_one_run_records_one_fire() {
    let w = world_at("2026-11-10T06:00:00Z").await;
    let id = w.schedule("UTC", "now", false, PROMPT).await;
    let (fire, _, slot, _) = w.until_fires(1).await[0].clone();
    let runs = task_runs(w.engine(), id).await;
    assert_eq!(runs.len(), 1);
    let slot = chrono::DateTime::from_timestamp_millis(slot).unwrap().to_rfc3339();
    // The engine sends an at-least-once call again after a crash.
    let again = agent::schedule::fire(w.engine(), &runs[0], &json!(slot)).await.unwrap();
    assert_eq!(again, json!({"fire": fire}));
    assert_eq!(w.fires().len(), 1);
    assert_eq!(w.drain().await.ran, 1);
    assert_eq!(w.drain().await.ran, 0);
    assert_eq!(w.model.fires().len(), 1);
    w.supervisor.shutdown().await;
}

// ── 5. the drain's one rule ─────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fire_the_stopping_supervisor_did_not_start_runs_once_on_the_next_start() {
    let w = world_at("2026-11-10T06:00:00Z").await;
    w.schedule("UTC", "now", false, PROMPT).await;
    w.until_fires(1).await;
    w.supervisor.shutdown().await;
    assert_eq!(w.drain().await.end, RoundEnd::Stopping);
    assert_eq!(w.fires()[0].3, "pending");
    assert!(w.model.fires().is_empty());

    let engine = reopen(&w.f, w.model.clone(), w.clock.clone(), configure(&w.model, &w.traces));
    engine.ready().await.unwrap();
    let supervisor = create_supervisor(engine);
    assert_eq!(agent::schedule::drain(&supervisor, None).await.unwrap().ran, 1);
    assert_eq!(agent::schedule::drain(&supervisor, None).await.unwrap().ran, 0);
    assert_eq!(w.fires()[0].3, "done");
    assert_eq!(w.model.fires().len(), 1);
    supervisor.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fire_whose_signal_run_exists_is_marked_done_without_a_second_start() {
    let w = world_at("2026-11-10T06:00:00Z").await;
    w.schedule("UTC", "now", false, PROMPT).await;
    w.until_fires(1).await;
    // A hard crash after the signal run was admitted: the run exists, the
    // row is still pending.
    let fire = w.engine().deps.schedule.fires.next_pending().await.unwrap().unwrap();
    let signal = agent::schedule::signal_of(&fire, None, chrono::Utc::now());
    w.supervisor.start(&signal).await.unwrap().unwrap();
    let trace = format!("scheduler:{}", fire.id);
    assert_eq!(w.trace_starts(&trace), 1);
    assert_eq!(w.fires()[0].3, "pending");

    assert_eq!(w.drain().await, Drained { ran: 0, end: RoundEnd::Empty });
    assert_eq!(w.fires()[0].3, "done");
    assert_eq!(w.trace_starts(&trace), 1, "no second start, no second trace");
    assert_eq!(w.model.fires().len(), 1);
    w.supervisor.shutdown().await;
}

async fn operator(engine: &Engine, name: &str, pause: bool) {
    let request = ouro_runtime::control::Request {
        id: format!("{}-{name}", if pause { "pause" } else { "resume" }),
        actor: "operator".into(),
        reason: "test".into(),
    };
    if pause {
        engine.control.pause(&request, name, "maintenance").await.unwrap();
    } else {
        engine.control.resume(&request, name).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fire_whose_signal_was_not_admitted_stays_pending_and_runs_later() {
    let w = world_at("2026-11-10T06:00:00Z").await;
    w.schedule("UTC", "now", false, PROMPT).await;
    let (fire, _, _, _) = w.until_fires(1).await[0].clone();
    // The dispatcher paused with nothing to run in its place: no admission.
    operator(w.engine(), "signal", true).await;
    assert_eq!(w.drain().await, Drained { ran: 0, end: RoundEnd::Pending(fire) });
    assert_eq!(w.fires()[0].3, "pending");
    assert!(w.engine().runtime.get(&format!("signal:{fire}")).await.unwrap().is_none());
    operator(w.engine(), "signal", false).await;
    assert_eq!(w.drain().await.ran, 1);
    assert_eq!(w.fires()[0].3, "done");
    w.supervisor.shutdown().await;
}

// ── 6. fires wait for a running signal, in order ────────────────────────────

// The clock moves a minute while the held signal's run is in its model call:
// in steps shorter than the engine's 30 s lease, each renewed by a short
// heartbeat, so the run keeps its lease as it would in real time.
async fn walk(w: &World, from: &str, to: &str) {
    let (mut at, to) = (ms(from), ms(to));
    while at < to {
        at = (at + 10_000).min(to);
        w.clock.0.store(at - chrono::Utc::now().timestamp_millis(), Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fires_due_while_a_signal_runs_wait_in_order_and_the_drain_wakes_on_the_record() {
    let config = ouro_runtime::Config { heartbeat_interval: Duration::from_millis(50), ..agent::runs::config() };
    let w = world_configured("2026-11-10T06:00:30Z", config, |_| {}).await;
    let cancel = CancellationToken::new();
    let drain = tokio::spawn(agent::schedule::run_drain(w.supervisor.clone(), None, cancel.clone()));
    let held = {
        let supervisor = w.supervisor.clone();
        let signal = agent::supervisor::PendingSignal {
            id: 7,
            source: "domain".into(),
            content: "HOLD the line".into(),
            env_context: None,
            created_at: "2026-11-10 06:00:30".into(),
            payload: None,
        };
        tokio::spawn(async move { supervisor.run_signal(&signal).await })
    };
    until("the held signal's turn", || (!w.model.inputs().is_empty()).then_some(())).await;
    let id = w.schedule("UTC", "* * * * *", true, PROMPT).await;
    // Two slots, due on separate worker ticks.
    walk(&w, "2026-11-10T06:00:31Z", "2026-11-10T06:01:00.300Z").await;
    w.until_fires(1).await;
    walk(&w, "2026-11-10T06:01:01Z", "2026-11-10T06:02:00.300Z").await;
    w.until_fires(2).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(w.model.fires().is_empty(), "the running signal holds the permit");
    let released = tokio::time::Instant::now();
    w.model.released.store(true, Ordering::SeqCst);
    held.await.unwrap().unwrap();
    until("both fires ran", || w.fires().iter().all(|f| f.3 == "done").then_some(())).await;
    assert!(released.elapsed() < Duration::from_secs(10), "woken by the record, not the 30 s round");
    let slots: Vec<String> = w.model.fires().iter().map(|i| header(i, "Slot")).collect();
    assert_eq!(slots, ["2026-11-10T06:01:00.000Z", "2026-11-10T06:02:00.000Z"], "in order, one run each");
    // One signal at a time: every fire's turn came after the held signal's.
    let inputs = w.model.inputs();
    let last_held = inputs.iter().rposition(|i| i.contains("HOLD")).unwrap();
    let first_fire = inputs.iter().position(|i| i.contains("Scheduled task #")).unwrap();
    assert!(last_held < first_fire, "{inputs:?}");
    assert!(w.fires().iter().all(|f| f.1 == id));
    cancel.cancel();
    drain.await.unwrap();
    w.supervisor.shutdown().await;
}

// ── 7. faults ───────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_faulted_signal_marks_its_fire_done_and_leaves_the_task_live() {
    let w = world_at("2026-11-10T06:00:00Z").await;
    let id = w.schedule("UTC", "0 7 * * *", true, "FAIL please").await;
    let (fire, _, _, _) = w.fire_at("2026-11-10T07:00:00.300Z", 1).await;
    assert_eq!(w.drain().await.ran, 1);
    let status = ended(w.engine(), &format!("signal:{fire}")).await;
    assert!(matches!(status, agent::runs::Status::Faulted(_)), "{status:?}");
    assert_eq!(w.fires()[0].3, "done");
    let definitions = w.engine().runtime.definitions(&format!("task-{id}")).await.unwrap();
    assert!(definitions.tripped.is_none());
    assert_eq!(definitions.consecutive_faults, 0, "the task's own run completed");
    assert!(w.listed(id).await.unwrap()["next_fire"].is_string(), "still live");
    w.supervisor.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_storage_error_in_fire_scheduled_is_resent_and_recorded_once() {
    let flaky: Arc<Mutex<Option<Arc<Flaky>>>> = Default::default();
    let slot = flaky.clone();
    let w = world_with("2026-11-10T06:00:00Z", move |deps| {
        let wrapped =
            Arc::new(Flaky { inner: deps.schedule.fires.clone(), failing: AtomicUsize::new(1), attempts: 0.into() });
        *slot.lock().unwrap() = Some(wrapped.clone());
        deps.schedule = Arc::new(Schedules::new(wrapped));
    })
    .await;
    let flaky = flaky.lock().unwrap().clone().unwrap();
    let id = w.schedule("UTC", "now", false, PROMPT).await;
    w.until_fires(1).await;
    assert_eq!(flaky.attempts.load(Ordering::SeqCst), 2, "interrupted, then sent again");
    let runs = task_runs(w.engine(), id).await;
    let status = ended(w.engine(), &runs[0].admission.id).await;
    assert!(matches!(status, agent::runs::Status::Completed(_)), "{status:?}");
    settles(&w, 1).await;
    assert_eq!(w.engine().runtime.definitions(&format!("task-{id}")).await.unwrap().consecutive_faults, 0);
    w.supervisor.shutdown().await;
}

// ── 8. cancel ───────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancelled_task_fires_no_more_and_a_run_admitted_before_records_nothing() {
    let w = world_at("2026-11-10T06:00:00Z").await;
    let id = w.schedule("UTC", "0 7 * * *", true, PROMPT).await;
    let version = w.engine().runtime.definitions(&format!("task-{id}")).await.unwrap().active.unwrap();
    assert_eq!(w.cancel(id).await, json!({"ok": true, "id": id}));
    assert!(w.listed(id).await.is_none());
    w.clock.set("2026-11-10T07:00:00.300Z");
    settles(&w, 0).await;
    assert_eq!(w.cancel(id).await["ok"], false, "cancelled already");
    assert_eq!(w.cancel(9_999).await["ok"], false, "unknown");
    let once = w.schedule("UTC", "now", false, PROMPT).await;
    w.until_fires(1).await;
    assert_eq!(w.cancel(once).await["ok"], false, "a consumed one-shot");
    assert!(once > id);
    assert!(w.schedule("UTC", "0 8 * * *", true, PROMPT).await > once, "ids are never reused");

    // A run of the task admitted before the cancel, sent after it.
    let run = format!("schedule:task-{id}:cron:{}", ms("2026-11-10T07:00:00Z"));
    let start = ouro_runtime::Start { version: Some(version), host: Value::Null };
    w.engine().runtime.start_with(&run, &format!("task-{id}"), json!({}), start).await.unwrap();
    let status = ended(w.engine(), &run).await;
    assert_eq!(status, agent::runs::Status::Completed(json!({"fire": null, "cancelled": true})));
    assert_eq!(w.fires().len(), 1);
    w.supervisor.shutdown().await;
}

// ── 9. a task that runs a saved workflow ────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_task_naming_a_saved_workflow_runs_its_active_version() {
    let w = world_at("2026-11-10T06:00:00Z").await;
    let version = activate_definition(w.engine(), "planner", &variant("planner")).await;
    let reply = w
        .tool(
            "UTC",
            "schedule_task",
            json!({"cron_expr":"now","recurring":false,"prompt":"Plan.","workflow":"planner"}),
        )
        .await;
    assert_eq!(reply["ok"], true, "{reply}");
    assert_eq!(reply["task"]["workflow"], "planner");
    let (fire, _, _, _) = w.until_fires(1).await[0].clone();
    assert_eq!(w.drain().await.ran, 1);
    let filter = agent::runs::Runs { parent: Some(format!("signal:{fire}")), ..Default::default() };
    let children = w.engine().control.list(&filter, None, 50).await.unwrap().items;
    let saved = children.iter().find(|r| r.admission.name == "planner").expect("the saved workflow's run");
    assert_eq!(saved.admission.version, version);
    for (workflow, why) in [("missing", "no active version"), ("task-1", "a task's definition"), ("guard", "reserved")]
    {
        let reply = w
            .tool("UTC", "schedule_task", json!({"cron_expr":"now","recurring":false,"prompt":"x","workflow":workflow}))
            .await;
        assert_eq!(reply["ok"], false, "{why}: {reply}");
    }
    settles(&w, 1).await;
    w.supervisor.shutdown().await;
}

// ── 10. the zone watch ──────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_zone_watch_moves_recurring_tasks_once_and_never_resurrects_one() {
    use chrono_tz::Europe::{Berlin, Kyiv};
    let w = world_at("2026-11-10T06:00:00Z").await;
    let daily = w.schedule("Europe/Kyiv", "0 9 * * *", true, PROMPT).await;
    let once = w.schedule("Europe/Kyiv", "0 12 * * *", false, "noon").await;
    let paused = w.schedule("Europe/Kyiv", "0 10 * * *", true, "paused").await;
    operator(w.engine(), &format!("task-{paused}"), true).await;
    // 09:00 Kyiv (UTC+2 in winter).
    w.fire_at("2026-11-10T07:00:00.300Z", 1).await;
    assert_eq!(w.drain().await.ran, 1);
    assert_eq!(agent::schedule::zone_round(w.engine(), Kyiv).await.unwrap(), 0, "the same zone saves nothing");
    // 10:30 Kyiv, 09:30 Berlin: the user moves.
    w.clock.set("2026-11-10T08:30:00Z");
    assert_eq!(agent::schedule::zone_round(w.engine(), Berlin).await.unwrap(), 1);
    assert_eq!(w.versions(daily).await, 2);
    assert_eq!(w.versions(once).await, 1, "one-shots keep their instant");
    assert_eq!(w.versions(paused).await, 1, "a task set aside is not re-activated");
    assert_eq!(w.active_task(daily).await.timezone, "Europe/Berlin");
    assert_eq!(w.listed(daily).await.unwrap()["next_fire"], "2026-11-11T08:00:00.000Z", "09:00 Berlin");
    assert_eq!(agent::schedule::zone_round(w.engine(), Berlin).await.unwrap(), 0, "a second round saves nothing");
    assert_eq!(w.versions(daily).await, 2);
    // 12:00 Kyiv: the one-shot.
    w.fire_at("2026-11-10T10:00:00.300Z", 2).await;
    let (_, task, slot, _) = w.fire_at("2026-11-11T08:00:00.300Z", 3).await;
    assert_eq!((task, slot), (daily, ms("2026-11-11T08:00:00Z")));
    assert_eq!(w.drain().await.ran, 2);
    let berlin = w.model.fires().into_iter().rfind(|i| i.contains(&format!("#{daily} fired"))).unwrap();
    assert_eq!(header(&berlin, "Previous fire"), "2026-11-10T07:00:00.000Z", "the 09:00 Kyiv slot");

    // A cancel racing the watch stays cancelled.
    let raced = w.schedule("Europe/Kyiv", "0 9 * * *", true, "raced").await;
    let (cancelled, moved) = tokio::join!(w.cancel(raced), agent::schedule::zone_round(w.engine(), Berlin));
    assert_eq!(cancelled["ok"], true);
    moved.unwrap();
    assert!(w.active_task(raced).await.cancelled);
    assert_eq!(agent::schedule::zone_round(w.engine(), Kyiv).await.unwrap(), 1, "only the daily task moves back");
    assert!(w.active_task(raced).await.cancelled);
    w.supervisor.shutdown().await;
}

// ── 11. refusals ────────────────────────────────────────────────────────────

async fn flows_tool(engine: &Engine, name: &str, args: Value) -> anyhow::Result<String> {
    agent::flows::tool(engine, name, args.as_object().unwrap()).await
}

fn document(name: &str, rest: &str) -> String {
    format!("document: {{ dsl: '1.0.4', namespace: agent, name: {name}, version: '1.0.0' }}\n{rest}")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_agent_cannot_write_or_fire_a_tasks_definition() {
    let w = world_at("2026-11-10T06:00:00Z").await;
    let id = w.schedule("UTC", "0 9 * * *", true, PROMPT).await;
    let task = format!("task-{id}");
    let plain = "do: [ { a: { set: { x: 1 } } } ]\n";
    let refused = [
        (task.clone(), document(&task, plain)),
        ("timed".into(), document("timed", &format!("schedule: {{ cron: '0 9 * * *' }}\n{plain}"))),
        (
            "at".into(),
            "document: { dsl: '1.0.4', namespace: agent, name: at, version: '1.0.0', metadata: { ouro: { at: '2030-01-31T09:00:00+01:00' } } }\ndo: [ { a: { set: { x: 1 } } } ]\n".into(),
        ),
        ("firing".into(), document("firing", "do: [ { f: { call: fire_scheduled, with: { slot: '2026-11-10T07:00:00Z' } } } ]\n")),
        ("literal".into(), document("literal", &format!("do: [ {{ c: {{ call: start_child, with: {{ name: {task}, input: {{}} }} }} }} ]\n"))),
    ];
    for (name, definition) in &refused {
        let saved = flows_tool(w.engine(), "save_workflow", json!({"name": name, "definition": definition})).await;
        assert!(saved.is_err(), "{name} must be refused: {saved:?}");
    }
    let activated = flows_tool(w.engine(), "activate_workflow", json!({"name": task, "version": 1})).await;
    assert!(activated.is_err(), "{activated:?}");
    let catalog = agent::flows::functions(w.engine(), &Map::new()).unwrap();
    assert!(!catalog.contains("fire_scheduled"), "{catalog}");
    assert!(agent::flows::functions(w.engine(), json!({"name":"fire_scheduled"}).as_object().unwrap()).is_err());

    // Named by an expression, the gate cannot see it: the child's
    // fire_scheduled refuses at run time and records nothing.
    let caller = document(
        "caller",
        r#"do:
  - start:
      call: start_child
      with: { name: '${ .task }', input: {} }
      export: { as: '${ $context + { child: . } }' }
  - wait:
      listen: { to: { one: { with: { type: '${ $context.child.done }' } } } }
  - result:
      call: child_outcome
      with: { id: '${ $context.child.id }', key: '${ $context.child.key }' }
"#,
    );
    activate_definition(w.engine(), "caller", &caller).await;
    w.engine().runtime.start("caller-1", "caller", json!({"task": task})).await.unwrap();
    let status = ended(w.engine(), "caller-1").await;
    assert_eq!(
        status,
        agent::runs::Status::Completed(json!({"Completed": {"fire": null, "refused": "not a trigger's run"}}))
    );
    assert!(w.fires().is_empty());
    // And the schedule goes on.
    w.fire_at("2026-11-10T09:00:00.300Z", 1).await;
    assert_eq!(w.drain().await.ran, 1);
    w.supervisor.shutdown().await;
}

// ── 13. validation and the cap ──────────────────────────────────────────────

async fn tasks_named(w: &World) -> Vec<String> {
    let page = w.engine().control.definitions(None, 1_000).await.unwrap();
    page.items.into_iter().map(|d| d.name).filter(|n| n.starts_with("task-")).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_crons_and_the_task_past_the_cap_are_refused_and_save_nothing() {
    let w = world_at("2026-11-10T06:00:00Z").await;
    for cron in ["0 9 * * * *", "0 0 31 11 *", "not a cron"] {
        let reply = w.tool("UTC", "schedule_task", json!({"cron_expr":cron,"recurring":true,"prompt":"x"})).await;
        assert_eq!(reply["ok"], false, "{cron}: {reply}");
    }
    assert!(tasks_named(&w).await.is_empty());
    for _ in 0..agent::schedule::MAX_LIVE_TASKS {
        w.schedule("UTC", "0 9 * * *", true, "x").await;
    }
    assert_eq!(agent::schedule::MAX_LIVE_TASKS, 100);
    let reply = w.tool("UTC", "schedule_task", json!({"cron_expr":"0 9 * * *","recurring":true,"prompt":"x"})).await;
    assert_eq!(reply["ok"], false, "{reply}");
    assert_eq!(tasks_named(&w).await.len(), 100);
    w.supervisor.shutdown().await;
}

// ── 15. a try schedules nothing ─────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dry_run_that_schedules_records_a_mocked_write_and_keeps_nothing() {
    let w = world_at("2026-11-10T06:00:00Z").await;
    activate_definition(w.engine(), "planner", &variant("planner")).await;
    let draft = variant("planner").replace("General ReAct loop", "A planning loop");
    let args = json!({"name": "planner", "definition": draft});
    flows_tool(w.engine(), "save_workflow", args).await.unwrap();
    w.model.script.lock().unwrap().push_back(calls(&[(
        "schedule_task",
        json!({"cron_expr":"0 9 * * *","recurring":true,"prompt":"every morning"}),
    )]));
    let trace =
        w.engine().deps.tracer.trace(TraceStart { id: "author".into(), name: "author".into(), ..Default::default() });
    let scope: agent::tracing::Scope = trace.clone();
    let args =
        json!({"name":"planner","signal":{"source":"scheduler","content":"Plan.","payload":{"workflow":"planner"}}});
    let reply = agent::sandbox::try_signal(
        w.engine(),
        args.as_object().unwrap(),
        &context(),
        &scope,
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    trace.end();
    let report: Value = serde_json::from_str(&reply).unwrap();
    assert_eq!(report["status"], "completed", "{report:#}");
    let writes: Vec<(&str, &str)> = report["writes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|w| (w["function"].as_str().unwrap(), w["via"].as_str().unwrap()))
        .collect();
    assert!(writes.contains(&("schedule_task", "tool")), "{report:#}");
    assert!(tasks_named(&w).await.is_empty(), "no task- definition");
    assert!(w.fires().is_empty(), "no fire row");
    w.supervisor.shutdown().await;
}

// ── 16. the migration ───────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_migration_continues_both_sequences_and_settles_the_old_fires() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("agent.db");
    {
        // agent.db as the tick left it: every migration before this step's.
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            r#"CREATE TABLE "__drizzle_migrations" (id SERIAL PRIMARY KEY, hash text NOT NULL, created_at numeric)"#,
        )
        .unwrap();
        for m in agent::db::migrations().unwrap().into_iter().filter(|m| m.tag != "0017_engine_schedules") {
            for statement in &m.statements {
                conn.execute_batch(statement).unwrap();
            }
            conn.execute(
                r#"INSERT INTO "__drizzle_migrations" (hash, created_at) VALUES (?1, ?2)"#,
                rusqlite::params![m.hash, m.when],
            )
            .unwrap();
        }
        conn.execute("DELETE FROM scheduled_tasks", []).unwrap();
        for id in 1..=7 {
            conn.execute(
                "INSERT INTO scheduled_tasks (id, cron_expr, recurring, prompt, created_ms) VALUES (?1, '0 9 * * *', 1, 'old', 0)",
                [id],
            )
            .unwrap();
        }
        conn.execute("DELETE FROM scheduled_tasks WHERE id = 7", []).unwrap();
        for n in 1..=3 {
            conn.execute("INSERT INTO scheduled_fires (task_id, slot) VALUES (1, ?1)", [1_790_000_000 + n]).unwrap();
        }
    }
    let db = Arc::new(agent::db::AgentDb::open(&path).unwrap());
    let tables: i64 = db
        .conn()
        .query_row("SELECT count(*) FROM sqlite_master WHERE name = 'scheduled_tasks'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(tables, 0, "the old task table is gone");
    let states: Vec<String> = {
        let conn = db.conn();
        let mut s = conn.prepare("SELECT state FROM scheduled_fires").unwrap();
        s.query_map([], |r| r.get(0)).unwrap().map(Result::unwrap).collect()
    };
    assert_eq!(states, ["done", "done", "done"]);
    let fires: Arc<dyn FireStore> = db.clone();
    assert_eq!(fires.next_task_id().await.unwrap(), 8, "task ids continue the old sequence");
    assert!(fires.next_pending().await.unwrap().is_none(), "the history never reaches the drain");
    let task = Task {
        id: 8,
        cron: "now".into(),
        recurring: false,
        prompt: "p".into(),
        source: None,
        workflow: None,
        timezone: "UTC".into(),
        created: "2026-11-10T06:00:00.000Z".into(),
        cancelled: false,
    };
    assert_eq!(fires.record("schedule:task-8:at:1", &task, 1).await.unwrap(), agent::schedule::FIRST_FIRE_ID + 4);

    // A supervisor on it starts no signal for the old fires.
    let model = Arc::new(Model::default());
    let f = fixture_on(model.clone(), &[], SKILLS, |deps| {
        deps.schedule = Arc::new(Schedules::new(db.clone()));
        deps.db = (*db).clone();
    });
    fires.done(agent::schedule::FIRST_FIRE_ID + 4).await.unwrap();
    let supervisor = create_supervisor(f.engine.clone());
    assert_eq!(agent::schedule::drain(&supervisor, None).await.unwrap(), Drained { ran: 0, end: RoundEnd::Empty });
    assert!(model.inputs().is_empty());
    supervisor.shutdown().await;
}

// ── 17. catch-up ────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn after_downtime_over_three_slots_the_oldest_missed_one_fires_once() {
    let w = world_at("2026-11-10T06:10:00Z").await;
    let id = w.schedule("UTC", "0 * * * *", true, PROMPT).await;
    w.fire_at("2026-11-10T07:00:00.300Z", 1).await;
    assert_eq!(w.drain().await.ran, 1);
    w.supervisor.shutdown().await;
    // Down over 08:00, 09:00 and 10:00.
    w.clock.set("2026-11-10T10:30:00Z");
    let engine = reopen(&w.f, w.model.clone(), w.clock.clone(), configure(&w.model, &w.traces));
    engine.ready().await.unwrap();
    let supervisor = create_supervisor(engine.clone());
    let fires = w.until_fires(2).await;
    settles(&w, 2).await;
    assert_eq!((fires[1].1, fires[1].2), (id, ms("2026-11-10T08:00:00Z")), "the oldest missed slot");
    assert_eq!(agent::schedule::drain(&supervisor, None).await.unwrap().ran, 1);
    let input = w.model.fires().last().unwrap().clone();
    assert_eq!(header(&input, "Slot"), "2026-11-10T08:00:00.000Z");
    assert_eq!(header(&input, "Previous fire"), "2026-11-10T07:00:00.000Z");
    let reply = agent::schedule::tool(&engine, "UTC", "list_scheduled_tasks", &Map::new()).await.unwrap();
    let listed: Value = serde_json::from_str(&reply).unwrap();
    assert_eq!(listed["tasks"][0]["next_fire"], "2026-11-10T11:00:00.000Z", "the next one after now");
    supervisor.shutdown().await;
}

// ── 18. the list ────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_list_shows_each_live_task_with_its_next_and_last_fire_and_its_faults() {
    let w = world_at("2026-11-10T06:00:00Z").await;
    let prompt = "Morning \"digest\":\n- news\n- weather: 'sunny'";
    let daily = w.schedule("Europe/Kyiv", "0 9 * * 1,3", true, prompt).await;
    let once = w.schedule("UTC", "now", false, "one-shot").await;
    let cancelled = w.schedule("UTC", "0 7 * * *", true, "gone").await;
    assert_eq!(w.cancel(cancelled).await["ok"], true);
    w.until_fires(1).await;

    // A task whose trigger run faults: its slot is not a time.
    let faulty = w.engine().deps.schedule.fires.next_task_id().await.unwrap();
    let task = Task {
        id: faulty,
        cron: "* * * * *".into(),
        recurring: true,
        prompt: "faulty".into(),
        source: None,
        workflow: None,
        timezone: "UTC".into(),
        created: "2026-11-10T06:00:00.000Z".into(),
        cancelled: false,
    };
    let yaml = agent::schedule::definition(&task, &agent::schedule::Trigger::Cron(chrono_tz::UTC))
        .replace("${ $run.schedule.slot.iso8601 }", "not a time");
    let name = format!("task-{faulty}");
    let version = w.engine().runtime.save(&name, &yaml).await.unwrap().version;
    w.engine().runtime.activate_as(&name, version, "operator", None).await.unwrap();
    w.clock.set("2026-11-10T06:01:00.300Z");
    let engine = w.engine().clone();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while engine.runtime.definitions(&name).await.unwrap().consecutive_faults == 0 {
        assert!(tokio::time::Instant::now() < deadline, "the faulty task never faulted");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let list = w.list().await;
    let ids: Vec<i64> = list["tasks"].as_array().unwrap().iter().map(|t| t["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, [daily, once, faulty], "the cancelled task is absent: {list:#}");
    let listed = &list["tasks"][0];
    assert_eq!(listed["cron_expr"], "0 9 * * 1,3");
    assert_eq!(listed["prompt"], prompt, "quotes and newlines round-trip");
    assert_eq!((listed["recurring"].as_bool(), listed["timezone"].as_str()), (Some(true), Some("Europe/Kyiv")));
    assert!(listed["source"].is_null() && listed["workflow"].is_null());
    // The test clock runs on from its offset, so under load the task is made
    // a few seconds after 06:00, never a minute.
    let created = ms(listed["created_at"].as_str().unwrap());
    assert!((ms("2026-11-10T06:00:00Z")..ms("2026-11-10T06:01:00Z")).contains(&created), "{list:#}");
    // Wednesday 2026-11-11 09:00 Kyiv.
    assert_eq!(listed["next_fire"], "2026-11-11T07:00:00.000Z");
    assert!(listed["last_fire"].is_null());
    assert!(listed.get("consumed").is_none() && listed.get("faults").is_none());
    let listed = &list["tasks"][1];
    assert_eq!((listed["consumed"].as_bool(), listed["recurring"].as_bool()), (Some(true), Some(false)));
    assert!(listed["next_fire"].is_null());
    assert!(listed["last_fire"].is_string());
    let listed = &list["tasks"][2];
    assert!(listed["faults"].as_u64().unwrap() >= 1, "{listed}");
    assert!(listed["last_error"].as_str().unwrap().contains("slot must be"), "{listed}");
    assert!(listed["next_fire"].is_string(), "breaker off: it keeps firing");

    // After a fire, its slot is the last one.
    w.fire_at("2026-11-11T07:00:00.300Z", 2).await;
    assert_eq!(w.listed(daily).await.unwrap()["last_fire"], "2026-11-11T07:00:00.000Z");
    w.supervisor.shutdown().await;
}
