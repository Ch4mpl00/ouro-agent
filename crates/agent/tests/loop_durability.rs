// The agent loop as an engine run of `agent-loop`, over generated
// conversations: the round protocol and `finish` hold, a round's calls stay
// within the loop's limit, and a process that dies at any call to the world
// (a model request or a tool's dispatch, before it reached the world or
// after) is resumed by a fresh process on the same stores to the same
// conversation. A write is never sent twice (invariant 4), a completed turn
// is never asked of the model again, and the only trace a death may leave is
// a write in flight reported to the model as "outcome unknown". Deaths at
// the store's own commits are the engine's conformance suite's.
mod support;
use agent::{
    agent::{Engine, EngineDeps, LoopOptions, Providers, create_engine},
    db::AgentDb,
    mcp::Mcp,
    models::{ModelPreset, Presets, ReasoningEffort},
    providers::{
        ChatProvider, CompletionParams, CompletionResult, Message, ProviderKind, ToolDef, assistant, role, tool_calls,
    },
};
use async_trait::async_trait;
use ouro_runtime::ows::run::Status;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use serde_json::{Map, Value, json};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;
use support::{AnySkill, NoCodex, PersistLog, task_context};
use tokio_util::sync::CancellationToken;

// Engine runs take the worker's ticks: fewer cases than a store-only
// property by default; PROPERTY_RUNS raises them.
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

fn call_id(turn: usize, at: usize) -> String {
    format!("t{turn}c{at}")
}

// ── deaths ──────────────────────────────────────────────────────────────────

// Where the process dies: the n-th call to the world (counted across
// processes), before it reached the world or after. A death cancels `died`
// (the test then stops the engine) and the call never answers.
#[derive(Default)]
struct Deaths {
    calls: AtomicUsize,
    at: Mutex<Vec<(usize, bool)>>,
    died: Mutex<CancellationToken>,
}

impl Deaths {
    // Some(after) when this call dies.
    fn next(&self) -> Option<bool> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        self.at.lock().unwrap().iter().find(|(at, _)| *at == n).map(|(_, after)| *after)
    }
    async fn die(&self) -> ! {
        self.died.lock().unwrap().cancel();
        std::future::pending::<()>().await;
        unreachable!("a dead call never answers")
    }
}

// The model: turn `i` (the assistant messages already in the transcript)
// always gets the same reply, so a turn asked again would be the same turn,
// and the requests show which turns were asked.
struct Model {
    rounds: Vec<Vec<Call>>,
    // Every request answered: the turn asked and the transcript it was asked on.
    asked: Mutex<Vec<(usize, Vec<Message>)>>,
    deaths: Arc<Deaths>,
}

impl Model {
    fn new(rounds: Vec<Vec<Call>>, deaths: Arc<Deaths>) -> Arc<Model> {
        Arc::new(Model { rounds, asked: Mutex::default(), deaths })
    }
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

#[async_trait]
impl ChatProvider for Model {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Openai
    }
    async fn complete(&self, p: &mut CompletionParams<'_>) -> anyhow::Result<CompletionResult> {
        let turn = p.messages.iter().filter(|m| role(m) == "assistant").count();
        let death = self.deaths.next();
        if death == Some(false) {
            self.deaths.die().await;
        }
        self.asked.lock().unwrap().push((turn, p.messages.clone()));
        if death == Some(true) {
            self.deaths.die().await;
        }
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
    deaths: Arc<Deaths>,
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
    fn open_world(&self, _name: &str) -> bool {
        false
    }
    async fn call_tool(&self, name: &str, args: Map<String, Value>, _: &CancellationToken) -> anyhow::Result<String> {
        let death = self.deaths.next();
        if death == Some(false) {
            self.deaths.die().await;
        }
        let now = self.running.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        tokio::task::yield_now().await;
        self.running.fetch_sub(1, Ordering::SeqCst);
        let q = args["q"].as_str().unwrap_or_default().to_owned();
        *self.sent.lock().unwrap().entry(format!("{name}:{q}")).or_default() += 1;
        if death == Some(true) {
            self.deaths.die().await;
        }
        Ok(if name == "broken" { "[tool error] broken".into() } else { format!("{name} answered {q}") })
    }
}

// What outlives an engine: the stores, the world and the model.
struct Parts {
    store: Arc<dyn ouro_runtime::durable::RunStore>,
    db: AgentDb,
    model: Arc<Model>,
    world: Arc<World>,
    deaths: Arc<Deaths>,
}

fn parts(rounds: &[Vec<Call>]) -> Parts {
    let deaths = Arc::new(Deaths::default());
    Parts {
        store: agent::runs::memory_store(),
        db: AgentDb::open(":memory:").unwrap(),
        model: Model::new(rounds.to_vec(), deaths.clone()),
        world: Arc::new(World { deaths: deaths.clone(), ..World::default() }),
        deaths,
    }
}

fn engine(p: &Parts) -> Arc<Engine> {
    let preset = |name: &str| ModelPreset { model: name.into(), reasoning_effort: ReasoningEffort::Disabled };
    create_engine(EngineDeps {
        providers: Providers {
            openai: p.model.clone(),
            gemini: p.model.clone(),
            deepseek: p.model.clone(),
            openrouter: None,
        },
        mcp: p.world.clone(),
        presets: Presets {
            base: preset("gpt-5.4-mini"),
            smart: preset("gpt-5.4"),
            smartest: preset("gpt-5.4"),
            compiler: preset("gpt-5.4"),
            compose: preset("gpt-5.4"),
        },
        skills: Vec::new(),
        skill_store: Arc::new(AnySkill),
        memory: Arc::new(PersistLog::default()),
        tracer: Arc::new(agent::tracing::NullTracer),
        codex: Arc::new(NoCodex),
        decisions: Some(support::allow_guard()),
        env: Default::default(),
        db: p.db.clone(),
        store: p.store.clone(),
        schedule: Arc::new(agent::schedule::MemorySchedule::default()),
    })
}

const ID: &str = "peer:1/loop";

// One process's attempt at the conversation: the first starts the loop's
// run, a later one resumes it from the store. Ends with the run's outcome,
// or None when the process died first.
async fn attempt(p: &Parts, limit: usize, first: bool) -> Option<Status> {
    let died = CancellationToken::new();
    *p.deaths.died.lock().unwrap() = died.clone();
    let engine = engine(p);
    let mut sent = None;
    if first {
        let mut opts = LoopOptions::new(ID, task_context("peer:1"));
        opts.skills = vec!["any".into()];
        opts.include_engine_skills = false;
        opts.max_concurrent_tools = limit;
        let live = engine.start_loop(opts).await.unwrap();
        sent = Some(tokio::spawn(live.send("go".into())));
    } else {
        engine.ready().await.unwrap();
    }
    let ended = async {
        loop {
            if let Some(status) = engine.runtime.get(ID).await.unwrap().and_then(|r| r.outcome) {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    let outcome = tokio::select! {
        status = ended => Some(status),
        _ = died.cancelled() => None,
        _ = tokio::time::sleep(Duration::from_secs(60)) => panic!("the run neither ended nor died"),
    };
    engine.shutdown().await;
    if let Some(sent) = sent {
        let _ = sent.await;
    }
    outcome
}

// Memory keys of stored tool results are fresh UUIDs: equal up to those.
fn normal(messages: &[Message]) -> String {
    static KEY: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"tool\.[0-9a-f-]{36}").unwrap());
    KEY.replace_all(&serde_json::to_string(messages).unwrap(), "tool.KEY").into_owned()
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
    tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap().block_on(f)
}

// ── without a death ─────────────────────────────────────────────────────────

#[test]
fn rounds_pair_their_replies_finish_ends_where_it_may_and_calls_stay_within_the_limit() {
    let mut runner = TestRunner::new(Config { cases: runs(24), ..Config::default() });
    runner
        .run(&(conversation(), 1usize..=4), |(rounds, limit)| {
            let p = parts(&rounds);
            let status = block(attempt(&p, limit, true)).expect("nothing dies");
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
            prop_assert_eq!(p.model.turns(), (0..=last).collect::<Vec<_>>());
            for (_, messages) in p.model.asked.lock().unwrap().iter() {
                paired(messages).map_err(TestCaseError::fail)?;
            }
            // Every write of a round that ran was sent once.
            let sent = p.world.sent.lock().unwrap();
            for (r, round) in rounds.iter().enumerate().take(ends.map_or(rounds.len(), |r| r + 1)) {
                for (j, call) in round.iter().enumerate() {
                    if matches!(call, Call::Write) {
                        prop_assert_eq!(sent.get(&format!("send:{}", call_id(r, j))), Some(&1));
                    }
                }
            }
            prop_assert!(p.world.peak.load(Ordering::SeqCst) <= limit, "peak over {}", limit);
            Ok(())
        })
        .unwrap();
}

// ── deaths ──────────────────────────────────────────────────────────────────

const UNKNOWN: &str = "[tool error] outcome unknown: send";

// One or two deaths, each at a call to the world, before it got there or
// after. Indices pick among the run's own calls, so every case dies.
fn deaths() -> impl Strategy<Value = Vec<(prop::sample::Index, bool)>> {
    prop::collection::vec((any::<prop::sample::Index>(), any::<bool>()), 1..=2)
}

#[test]
fn a_death_at_any_call_resumes_to_the_same_conversation_and_never_sends_a_write_twice() {
    let mut runner = TestRunner::new(Config { cases: runs(24), ..Config::default() });
    runner
        .run(&(conversation(), deaths()), |(rounds, picks)| {
            // The run without a death: what a death must reproduce.
            let baseline = parts(&rounds);
            let done = block(attempt(&baseline, 4, true)).expect("nothing dies");
            prop_assert!(matches!(done, Status::Completed(_)), "{:?}", done);
            let want = normal(&baseline.model.last());
            let calls = baseline.deaths.calls.load(Ordering::SeqCst);
            // The second death may fall in the longer run a first one causes.
            let mut deaths: Vec<(usize, bool)> =
                picks.iter().enumerate().map(|(k, (at, after))| (at.index(calls * (k + 1)) + 1, *after)).collect();
            deaths.sort();
            deaths.dedup_by_key(|(at, _)| *at);
            let at = format!("deaths {deaths:?} of {calls} calls in {rounds:?}");

            let p = parts(&rounds);
            *p.deaths.at.lock().unwrap() = deaths.clone();
            let status = block(async {
                let mut status = attempt(&p, 4, true).await;
                // Each later process is a fresh engine on the same stores.
                for _ in 0..=deaths.len() {
                    if status.is_some() {
                        break;
                    }
                    status = attempt(&p, 4, false).await;
                }
                status
            });
            prop_assert!(matches!(status, Some(Status::Completed(_))), "{}: {:?}", at, status);
            let sent = p.world.sent.lock().unwrap();
            prop_assert!(
                sent.iter().all(|(call, n)| !call.starts_with("send:") || *n == 1),
                "{}: a write went twice: {:?}",
                at,
                sent
            );
            // A turn is asked again only when a death cut it.
            let turns = p.model.turns();
            let mut unique = turns.clone();
            unique.dedup();
            prop_assert!(turns.len() - unique.len() <= deaths.len(), "{}: turns {:?}", at, turns);
            prop_assert!(unique.windows(2).all(|w| w[1] == w[0] + 1), "{}: turns in order {:?}", at, turns);
            let got = p.model.last();
            // The last reply is the model's own: its calls have no replies yet.
            paired(&got[..got.len() - 1]).map_err(|e| TestCaseError::fail(format!("{at}: {e}")))?;
            let got = normal(&got);
            prop_assert!(!got.contains("outcome unknown: lookup"), "{}: a read is rerun, never unknown", at);
            match got.find(UNKNOWN) {
                None => {
                    prop_assert_eq!(status.as_ref(), Some(&done), "{}", at);
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
