// The agent loop as an `agent-loop` run, over generated conversations: the
// round protocol and `finish` hold, a round's calls stay within the loop's
// limit, and a process that dies at any checkpoint (before or after the
// commit) is resumed by a fresh process to the same conversation. A write is
// never sent twice, a completed turn is never asked of the model again, and
// the only trace a crash may leave is a write in flight reported to the
// model as "outcome unknown".
mod support;
use agent::{
    agent::{Engine, EngineDeps, LoopOptions, Providers, create_engine},
    flows::MemoryWorkflows,
    mcp::Mcp,
    models::{ModelPreset, Presets, ReasoningEffort},
    providers::{
        ChatProvider, CompletionParams, CompletionResult, Message, ProviderKind, ToolDef, assistant, role, tool_calls,
    },
    tracing::{SpanStart, null_scope},
};
use async_trait::async_trait;
use owf::{
    durable::{Journal, Record, check_transition},
    run::Status,
};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use support::{AnySkill, NoCodex, PersistLog, task_context};
use tokio_util::sync::CancellationToken;

fn runs(default: u32) -> u32 {
    std::env::var("PROPERTY_RUNS").ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

// ── the conversation ────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
enum Call {
    // A read-only MCP tool.
    Read,
    // An MCP tool that changes the world.
    Write,
    // A read-only tool that answers with an error.
    Broken,
    // Working memory: put a key of its own, then get one put in an earlier
    // round (the n-th of them, cyclically; `Get(None)`, or none put yet: a
    // key nobody put, an error).
    Put,
    Get(Option<usize>),
    Finish,
}

fn round() -> impl Strategy<Value = Vec<Call>> {
    prop::collection::vec(
        prop_oneof![
            3 => Just(Call::Read),
            3 => Just(Call::Write),
            1 => Just(Call::Broken),
            2 => Just(Call::Put),
            2 => prop::option::of(0usize..8).prop_map(Call::Get),
            2 => Just(Call::Finish),
        ],
        1..=4,
    )
}

// Rounds of calls; after the last the model answers in text.
fn conversation() -> impl Strategy<Value = Vec<Vec<Call>>> {
    prop::collection::vec(round(), 0..=4)
}

// The model: turn `i` (the assistant messages already in the transcript)
// always gets the same reply, so a turn asked again would be the same turn,
// and the requests show which turns were asked.
struct Model {
    rounds: Vec<Vec<Call>>,
    // Every request: the turn asked and the transcript it was asked on.
    asked: Mutex<Vec<(usize, Vec<Message>)>>,
}

fn call_id(turn: usize, at: usize) -> String {
    format!("t{turn}c{at}")
}

impl Model {
    fn reply(&self, turn: usize) -> Message {
        let Some(round) = self.rounds.get(turn) else { return assistant("answer") };
        // The keys put so far, in order: `Get(Some(n))` reads the n-th.
        let puts: Vec<String> = self.rounds[..turn]
            .iter()
            .enumerate()
            .flat_map(|(t, r)| {
                r.iter().enumerate().filter(|(_, c)| matches!(c, Call::Put)).map(move |(j, _)| call_id(t, j))
            })
            .collect();
        let calls: Vec<Value> = round
            .iter()
            .enumerate()
            .map(|(j, call)| {
                let id = call_id(turn, j);
                let (name, args) = match call {
                    Call::Read => ("lookup", json!({"q": id})),
                    Call::Write => ("send", json!({"q": id})),
                    Call::Broken => ("broken", json!({"q": id})),
                    Call::Put => ("working_memory_put", json!({"key": id, "value": format!("value of {id}")})),
                    Call::Get(n) => {
                        let key = n
                            .filter(|_| !puts.is_empty())
                            .map(|n| puts[n % puts.len()].clone())
                            .unwrap_or_else(|| "nobody".into());
                        ("working_memory_get", json!({"key": key}))
                    }
                    Call::Finish => ("finish", json!({"result": format!("done at {id}")})),
                };
                json!({"id": id, "type": "function", "function": {"name": name, "arguments": args.to_string()}})
            })
            .collect();
        json!({"role": "assistant", "content": null, "tool_calls": calls}).as_object().unwrap().clone()
    }
}

#[async_trait]
impl ChatProvider for Model {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Openai
    }
    async fn complete(&self, p: &mut CompletionParams<'_>) -> anyhow::Result<CompletionResult> {
        let turn = p.messages.iter().filter(|m| role(m) == "assistant").count();
        self.asked.lock().unwrap().push((turn, p.messages.clone()));
        Ok(CompletionResult { message: self.reply(turn), finish_reason: Some("stop".into()), usage: None })
    }
}

// The tools, with every dispatch counted by its call (`q`) and the most
// running at once.
#[derive(Default)]
struct World {
    sent: Mutex<HashMap<String, usize>>,
    running: AtomicUsize,
    peak: AtomicUsize,
}

#[async_trait]
impl Mcp for World {
    fn tools(&self) -> Vec<ToolDef> {
        let schema = json!({"type": "object", "properties": {"q": {}}});
        ["lookup", "send", "broken"].into_iter().map(|n| ToolDef::new(n, n, schema.clone())).collect()
    }
    fn read_only(&self, name: &str) -> bool {
        name != "send"
    }
    async fn call_tool(&self, name: &str, args: Map<String, Value>, _: &CancellationToken) -> anyhow::Result<String> {
        let now = self.running.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        tokio::task::yield_now().await;
        self.running.fetch_sub(1, Ordering::SeqCst);
        let q = args["q"].as_str().unwrap_or_default().to_owned();
        *self.sent.lock().unwrap().entry(format!("{name}:{q}")).or_default() += 1;
        Ok(if name == "broken" { "[tool error] broken".into() } else { format!("{name} answered {q}") })
    }
}

fn engine(model: Arc<Model>, world: Arc<World>) -> Arc<Engine> {
    let p = |name: &str| ModelPreset { model: name.into(), reasoning_effort: ReasoningEffort::Disabled };
    create_engine(EngineDeps {
        providers: Providers { openai: model.clone(), gemini: model.clone(), deepseek: model, openrouter: None },
        mcp: world,
        presets: Presets {
            base: p("gpt-5.4-mini"),
            smart: p("gpt-5.4"),
            smartest: p("gpt-5.4"),
            compiler: p("gpt-5.4"),
        },
        skills: Vec::new(),
        skill_store: Arc::new(AnySkill),
        memory: Arc::new(PersistLog::default()),
        tracer: Arc::new(agent::tracing::NullTracer),
        codex: Arc::new(NoCodex),
        decisions: Some(support::allow_guard()),
        workflows: Arc::new(MemoryWorkflows::default()),
        schedule: Arc::new(agent::schedule::MemorySchedule::default()),
    })
}

const ID: &str = "peer:1/loop";

async fn start(engine: &Arc<Engine>, limit: usize) -> Value {
    let mut opts = LoopOptions::new(ID, task_context("peer:1"));
    opts.skills = vec!["any".into()];
    opts.include_engine_skills = false;
    opts.max_concurrent_tools = limit;
    engine.start_loop(opts).await.unwrap().workflow_input("go").await
}

static AGENT_LOOP: LazyLock<owf::model::Workflow> =
    LazyLock::new(|| owf::model::parse_yaml(agent::agent::AGENT_LOOP).unwrap());

// One process's attempt at the run on `journal` (None: no checkpoints).
async fn attempt(engine: &Arc<Engine>, input: &Value, journal: Option<&dyn Journal>) -> Status {
    let root = null_scope().span(SpanStart::new("runner", agent::tracing::SpanKind::Chain));
    let host = agent::flows::loop_host(engine, &root, input.clone());
    let cancel = CancellationToken::new();
    let outcome = match journal {
        None => owf::run::run(&host, &AGENT_LOOP, ID, input.clone(), &cancel).await,
        Some(journal) => owf::run::resume(&host, &AGENT_LOOP, ID, input.clone(), &cancel, journal).await,
    };
    outcome.status
}

// Memory keys of stored tool results are fresh UUIDs: equal up to those.
fn normal(messages: &[Message]) -> String {
    static KEY: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"tool\.[0-9a-f-]{36}").unwrap());
    KEY.replace_all(&serde_json::to_string(messages).unwrap(), "tool.KEY").into_owned()
}

impl Model {
    fn new(rounds: Vec<Vec<Call>>) -> Arc<Model> {
        Arc::new(Model { rounds, asked: Mutex::default() })
    }
    fn turns(&self) -> Vec<usize> {
        self.asked.lock().unwrap().iter().map(|(t, _)| *t).collect()
    }
    // The transcript of the last request plus its reply.
    fn last(&self) -> Vec<Message> {
        let asked = self.asked.lock().unwrap();
        let (turn, messages) = asked.last().expect("the model was asked");
        let mut all = messages.clone();
        all.push(self.reply(*turn));
        all
    }
}

// The round that ends the session with `finish`, from the conversation
// alone: the first with one `finish` and no other call failing (a broken
// tool, a key nobody put). None: the model's text answer ends it.
fn finishing_round(rounds: &[Vec<Call>]) -> Option<usize> {
    let mut puts = 0;
    for (r, round) in rounds.iter().enumerate() {
        let finishes = round.iter().filter(|c| matches!(c, Call::Finish)).count();
        let fails = round.iter().any(|c| match c {
            Call::Broken => true,
            Call::Get(n) => n.is_none() || puts == 0,
            _ => false,
        });
        if finishes == 1 && !fails {
            return Some(r);
        }
        puts += round.iter().filter(|c| matches!(c, Call::Put)).count();
    }
    None
}

// Each assistant message with calls is followed by exactly one reply per
// call, in call order; nothing else comes between turns.
fn paired(messages: &[Message]) -> Result<(), String> {
    let mut i = 0;
    while i < messages.len() {
        if role(&messages[i]) == "assistant" {
            let ids: Vec<String> = tool_calls(&messages[i]).into_iter().map(|c| c.id).collect();
            for (k, id) in ids.iter().enumerate() {
                let reply = messages.get(i + 1 + k).ok_or(format!("no reply to {id}"))?;
                if role(reply) != "tool" || reply["tool_call_id"] != json!(id) {
                    return Err(format!("reply {k} after turn at {i} is {reply:?}, not {id}'s"));
                }
            }
            i += 1 + ids.len();
        } else {
            i += 1;
        }
    }
    Ok(())
}

fn block<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(f)
}

// ── without a crash ─────────────────────────────────────────────────────────

#[test]
fn rounds_pair_their_replies_finish_ends_where_it_may_and_calls_stay_within_the_limit() {
    let mut runner = TestRunner::new(Config { cases: runs(120), ..Config::default() });
    runner
        .run(&(conversation(), 1usize..=4), |(rounds, limit)| {
            let (model, world) = (Model::new(rounds.clone()), Arc::new(World::default()));
            let engine = engine(model.clone(), world.clone());
            let status = block(async {
                let input = start(&engine, limit).await;
                attempt(&engine, &input, None).await
            });
            let ends = finishing_round(&rounds);
            let want = match ends {
                Some(r) => {
                    let j = rounds[r].iter().position(|c| matches!(c, Call::Finish)).unwrap();
                    format!("done at {}", call_id(r, j))
                }
                None => "answer".into(),
            };
            prop_assert_eq!(&status, &Status::Completed(json!(want)), "{:?}", rounds);
            // Turns 0..=r, each asked once: a round that ends with finish
            // takes no closing turn.
            let last = ends.unwrap_or(rounds.len());
            prop_assert_eq!(model.turns(), (0..=last).collect::<Vec<_>>());
            for (_, messages) in model.asked.lock().unwrap().iter() {
                paired(messages).map_err(TestCaseError::fail)?;
            }
            // Every write of a round that ran was sent once.
            let sent = world.sent.lock().unwrap();
            for (r, round) in rounds.iter().enumerate().take(ends.map_or(rounds.len(), |r| r + 1)) {
                for (j, call) in round.iter().enumerate() {
                    if matches!(call, Call::Write) {
                        prop_assert_eq!(sent.get(&format!("send:{}", call_id(r, j))), Some(&1));
                    }
                }
            }
            prop_assert!(world.peak.load(Ordering::SeqCst) <= limit, "peak over {}", limit);
            Ok(())
        })
        .unwrap();
}

// ── crashes ─────────────────────────────────────────────────────────────────

// A journal whose process dies at the given saves (counted across
// processes), each before or after it commits, and saves nothing after
// until `revive`, the next process.
#[derive(Default)]
struct Store {
    records: Mutex<BTreeMap<String, Record>>,
    saves: AtomicUsize,
    deaths: Vec<(usize, bool)>,
    dead: AtomicBool,
}

impl Store {
    fn dying(deaths: Vec<(usize, bool)>) -> Store {
        Store { deaths, ..Store::default() }
    }
    fn revive(&self) {
        self.dead.store(false, Ordering::SeqCst);
    }
}

impl Journal for Store {
    fn load(&self) -> anyhow::Result<BTreeMap<String, Record>> {
        Ok(self.records.lock().unwrap().clone())
    }
    fn save(&self, key: &str, record: &Record) -> anyhow::Result<()> {
        anyhow::ensure!(!self.dead.load(Ordering::SeqCst), "the process is gone");
        let n = self.saves.fetch_add(1, Ordering::SeqCst) + 1;
        let death = self.deaths.iter().find(|(at, _)| *at == n).copied();
        if let Some((_, after_commit)) = death {
            self.dead.store(true, Ordering::SeqCst);
            if !after_commit {
                anyhow::bail!("died before commit {n}");
            }
        }
        let mut records = self.records.lock().unwrap();
        if let Some(old) = records.get(key) {
            check_transition(old, record)?;
        }
        records.insert(key.into(), record.clone());
        anyhow::ensure!(death.is_none(), "died after commit {n}");
        Ok(())
    }
}

const UNKNOWN: &str = "[tool error] outcome unknown: send";

// Where the process dies: one or two saves, each before or after its commit.
// Indices pick among the run's own saves, so every case crashes.
fn deaths() -> impl Strategy<Value = Vec<(prop::sample::Index, bool)>> {
    prop::collection::vec((any::<prop::sample::Index>(), any::<bool>()), 1..=2)
}

#[test]
fn a_crash_at_any_checkpoint_resumes_to_the_same_conversation_and_never_sends_a_write_twice() {
    let mut runner = TestRunner::new(Config { cases: runs(150), ..Config::default() });
    runner
        .run(&(conversation(), deaths()), |(rounds, picks)| {
            // The run without a crash, on a journal: what a crash must reproduce.
            let baseline = Store::default();
            let (model, world) = (Model::new(rounds.clone()), Arc::new(World::default()));
            let first = engine(model.clone(), world);
            let (input, done) = block(async {
                let input = start(&first, 4).await;
                let done = attempt(&first, &input, Some(&baseline)).await;
                (input, done)
            });
            prop_assert!(matches!(done, Status::Completed(_)), "{:?}", done);
            let want = normal(&model.last());
            let checkpoints = baseline.saves.load(Ordering::SeqCst);
            // The second death may fall in the longer run a first one causes.
            let mut deaths: Vec<(usize, bool)> = picks
                .iter()
                .enumerate()
                .map(|(k, (at, after))| (at.index(checkpoints * (k + 1)) + 1, *after))
                .collect();
            deaths.sort();
            deaths.dedup_by_key(|(at, _)| *at);
            let at = format!("deaths {deaths:?} of {checkpoints} saves in {rounds:?}");

            let store = Store::dying(deaths.clone());
            let (model, world) = (Model::new(rounds.clone()), Arc::new(World::default()));
            let status = block(async {
                let dying = engine(model.clone(), world.clone());
                start(&dying, 4).await;
                let mut status = attempt(&dying, &input, Some(&store)).await;
                // Each later process is a fresh engine on the same journal.
                for _ in 0..=deaths.len() {
                    if matches!(status, Status::Completed(_) | Status::Faulted(_)) {
                        break;
                    }
                    store.revive();
                    status = attempt(&engine(model.clone(), world.clone()), &input, Some(&store)).await;
                }
                status
            });
            prop_assert!(matches!(status, Status::Completed(_)), "{}: {:?}", at, status);
            let sent = world.sent.lock().unwrap();
            prop_assert!(
                sent.iter().all(|(call, n)| !call.starts_with("send:") || *n == 1),
                "{}: a write went twice: {:?}",
                at,
                sent
            );
            // A turn is asked again only when a death cut it.
            let turns = model.turns();
            let mut unique = turns.clone();
            unique.dedup();
            prop_assert!(turns.len() - unique.len() <= deaths.len(), "{}: turns {:?}", at, turns);
            prop_assert!(unique.windows(2).all(|w| w[1] == w[0] + 1), "{}: turns in order {:?}", at, turns);
            let got = model.last();
            // The last reply is the model's own: its calls have no replies yet.
            paired(&got[..got.len() - 1]).map_err(|e| TestCaseError::fail(format!("{at}: {e}")))?;
            let got = normal(&got);
            prop_assert!(!got.contains("outcome unknown: lookup"), "{}: a read is rerun, never unknown", at);
            match got.find(UNKNOWN) {
                None => {
                    prop_assert_eq!(&status, &done, "{}", at);
                    prop_assert_eq!(&got, &want, "{}", at);
                }
                // A write in flight is the only thing a death may lose: up to
                // the turn that asked for it, the conversation is the same.
                Some(cut) => {
                    let turn = got[..cut].rfind(r#""role":"assistant""#).unwrap();
                    prop_assert_eq!(&got[..turn], &want[..turn.min(want.len())], "{}", at);
                }
            }
            Ok(())
        })
        .unwrap();
}
