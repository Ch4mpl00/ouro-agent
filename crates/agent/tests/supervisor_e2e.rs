// Port of test/supervisor.test.ts: the supervisor end to end — the real poll
// loop, supervisor, engine, shipped skills and the sqlite trace mirror —
// driven over the real MCP
// client protocol against an in-process MCP server that plays the queue and
// Telegram, with a scripted LLM at the provider boundary.
//
// Each generated signal carries a plan for what "the model" does with it
// (send messages, some of which the tool rejects; answer; crash). The rules
// say, from the plan alone, what must have reached the chat, which paths ran and what was
// traced — per signal, in queue order.
mod support;
use agent::{
    agent::{EngineDeps, Providers, create_engine},
    db::{AgentDb, TraceStore},
    local_recorder::local_recorder_tracer,
    mcp::{Client, ConnectOptions, McpClient, Open, SharedMcp},
    models::{ModelPreset, Presets, ReasoningEffort},
    providers::{ChatProvider, CompletionParams, CompletionResult, Message, ProviderKind, role},
    skills::FsSkillStore,
    supervisor::{create_supervisor, run_poll_loop},
    tracing::TokenUsage,
};
use async_trait::async_trait;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ClientConfig, ContentBlock, Implementation,
    ListToolsResult, PaginatedRequestParams, ServerCapabilities, ServerConfig, Tool,
};
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, ServerHandler, ServiceExt};
use serde_json::{Value, json};
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::{Arc, LazyLock, Mutex};
use support::NoCodex;
use tokio_util::sync::CancellationToken;

const CHAT: i64 = 42;

fn presets() -> Presets {
    let p = |model: &str, reasoning_effort| ModelPreset { model: model.into(), reasoning_effort };
    Presets {
        base: p("m-base", ReasoningEffort::Disabled),
        smart: p("m-smart", ReasoningEffort::Medium),
        smartest: p("m-smartest", ReasoningEffort::Max),
        compiler: p("m-compiler", ReasoningEffort::Medium),
        compose: p("m-compiler", ReasoningEffort::Medium),
    }
}

// ── plans ───────────────────────────────────────────────────────────────────

// What the model does on each turn of a loop. A send the tool rejects comes
// back to the model as a tool error and the loop goes on.
#[derive(Clone, Copy, Debug)]
enum Act {
    Send(bool),
    Answer,
    Crash,
}

// `disguised`: a non-Telegram signal whose content starts like a Telegram
// one — it must still be treated as what its source says.
#[derive(Clone, Debug)]
enum Plan {
    Telegram(Vec<Act>),
    Domain { source: &'static str, acts: Vec<Act>, disguised: bool },
    Scheduler(Vec<Act>),
}

impl Plan {
    fn source(&self) -> &'static str {
        match self {
            Plan::Telegram(_) => "telegram",
            Plan::Domain { source, .. } => source,
            Plan::Scheduler(_) => "scheduler",
        }
    }
}

// A loop always ends: in an answer or a crash.
fn loop_acts() -> impl Strategy<Value = Vec<Act>> {
    (
        prop::collection::vec(any::<bool>().prop_map(Act::Send), 0..=3),
        prop::sample::select(vec![Act::Answer, Act::Crash]),
    )
        .prop_map(|(mut sends, end)| {
            sends.push(end);
            sends
        })
}

fn plan() -> impl Strategy<Value = Plan> {
    prop_oneof![
        loop_acts().prop_map(Plan::Telegram),
        (prop::sample::select(vec!["nashdom-bill", "dreaming"]), loop_acts(), any::<bool>())
            .prop_map(|(source, acts, disguised)| Plan::Domain { source, acts, disguised }),
        loop_acts().prop_map(Plan::Scheduler),
    ]
}

fn tag(id: i64) -> String {
    format!("⟦sig {id}⟧")
}

fn send_text(id: i64, i: usize, ok: bool) -> String {
    format!("{} part {i}{}", tag(id), if ok { "" } else { " ⚠reject" })
}

fn recovery_text(id: i64) -> String {
    format!("{} recovery report", tag(id))
}

struct Expected {
    texts: Vec<String>,
    failed: bool,
}

// What must reach the chat for a signal, from its plan alone.
fn expected(id: i64, p: &Plan) -> Expected {
    let run_loop = |acts: &[Act]| {
        let mut texts = Vec::new();
        for (i, act) in acts.iter().enumerate() {
            match act {
                Act::Send(true) => texts.push(send_text(id, i, true)),
                Act::Send(false) => {}
                Act::Crash => {
                    texts.push(recovery_text(id));
                    return (texts, true);
                }
                Act::Answer => break,
            }
        }
        (texts, false)
    };
    let acts = match p {
        Plan::Telegram(acts) | Plan::Domain { acts, .. } | Plan::Scheduler(acts) => acts,
    };
    let (texts, failed) = run_loop(acts);
    Expected { texts, failed }
}

fn content_of(id: i64, p: &Plan) -> String {
    let telegram = format!("Telegram message in chat {CHAT}.\nText: {}", json!(format!("{} hello", tag(id))));
    match p {
        Plan::Telegram(_) | Plan::Domain { disguised: true, .. } => telegram,
        _ => format!("{} {} event", tag(id), p.source()),
    }
}

// ── the world: queue, chat, and a log of everything that happened ───────────

#[derive(Clone, Debug)]
enum Event {
    Pop(Option<i64>),
    Delivered(String),
    Rejected(String),
    History,
    Llm { role: &'static str, signal: i64, first_user: String },
}

struct World {
    queue: Mutex<VecDeque<(i64, &'static str, String)>>,
    events: Mutex<Vec<Event>>,
    drained: CancellationToken,
}

impl World {
    fn log(&self, e: Event) {
        self.events.lock().unwrap().push(e);
    }
}

#[derive(Clone)]
struct FakeMcp(Arc<World>);

fn json_reply(value: Value) -> CallToolResponse {
    CallToolResult::success(vec![ContentBlock::text(value.to_string())]).into()
}

impl ServerHandler for FakeMcp {
    fn get_info(&self) -> ServerConfig {
        let mut info = ServerConfig::default();
        info.capabilities = ServerCapabilities::builder().enable_tools().build();
        info
    }
    async fn list_tools(
        &self,
        _: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let tool = |name: &'static str, description: &'static str, schema: Value| {
            Tool::new(name, description, Arc::new(schema.as_object().unwrap().clone()))
        };
        Ok(ListToolsResult::with_all_items(vec![
            tool("get_next_signal", "Pop the oldest pending signal.", json!({"type":"object","properties":{}})),
            tool(
                "send_telegram_message",
                "Send a Telegram message.",
                json!({"type":"object","properties":{"text":{"type":"string"},"chatId":{"type":["string","number"]}},"required":["text"]}),
            ),
            tool(
                "get_telegram_chat_history",
                "Chat history.",
                json!({"type":"object","properties":{"chatId":{"type":["string","number"]},"limit":{"type":"number"},"threadId":{"type":"number"}},"required":["chatId"]}),
            ),
            tool("get_timezone", "Timezone.", json!({"type":"object","properties":{}})),
            // No status tool: the built-in Telegram route (design 08) is not
            // installed, and Telegram signals take the default path whose
            // ordering and delivery this property is about.
        ]))
    }
    async fn call_tool(
        &self,
        p: CallToolRequestParams,
        _: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let world = &self.0;
        let args = p.arguments.unwrap_or_default();
        Ok(match p.name.as_ref() {
            "get_next_signal" => {
                let next = world.queue.lock().unwrap().pop_front();
                world.log(Event::Pop(next.as_ref().map(|n| n.0)));
                if next.is_none() {
                    world.drained.cancel();
                }
                let pending = world.queue.lock().unwrap().len();
                json_reply(json!({
                    "signal": next.map(|(id, source, content)| json!({
                        "id": id, "source": source, "content": content,
                        "envContext": format!("Default Telegram chat id: {CHAT}."),
                        "created_at": "2026-10-12 10:00:00",
                    })),
                    "pendingAfter": pending,
                }))
            }
            "send_telegram_message" => {
                let text = args.get("text").and_then(Value::as_str).unwrap_or_default().to_owned();
                if text.contains("⚠reject") {
                    world.log(Event::Rejected(text));
                    CallToolResult::error(vec![ContentBlock::text("Telegram sendMessage failed (400): Bad Request")])
                        .into()
                } else {
                    world.log(Event::Delivered(text));
                    let n = world.events.lock().unwrap().len();
                    json_reply(json!({"delivered":true,"messageId":n}))
                }
            }
            "get_telegram_chat_history" => {
                world.log(Event::History);
                json_reply(json!({"messages":[]}))
            }
            "get_timezone" => json_reply(json!({"timezone":"UTC","local_now":"2026-10-12 10:00"})),
            other => return Err(ErrorData::invalid_params(format!("Tool {other} not found"), None)),
        })
    }
}

// ── the scripted model ──────────────────────────────────────────────────────

static SIG: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"⟦sig (\d+)⟧").unwrap());

fn text(m: &Message) -> String {
    m.get("content").and_then(Value::as_str).unwrap_or_default().to_owned()
}

struct ScriptedModel {
    world: Arc<World>,
    plans: HashMap<i64, Plan>,
}

fn reply(message: Value) -> CompletionResult {
    CompletionResult {
        message: message.as_object().unwrap().clone(),
        finish_reason: Some("stop".into()),
        usage: Some(TokenUsage { input: 1, output: 1, total: 2, cached: Some(0) }),
    }
}

fn answer(content: &str) -> CompletionResult {
    reply(json!({"role":"assistant","content":content}))
}

fn send(body: &str, turn: usize) -> CompletionResult {
    reply(
        json!({"role":"assistant","content":null,"tool_calls":[{"id":format!("call_{turn}"),"type":"function","function":{"name":"send_telegram_message","arguments":json!({"text":body,"chatId":CHAT}).to_string()}}]}),
    )
}

#[async_trait]
impl ChatProvider for ScriptedModel {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Openai
    }
    async fn complete(&self, params: &mut CompletionParams<'_>) -> anyhow::Result<CompletionResult> {
        let all: Vec<String> = params.messages.iter().map(text).collect();
        let all = all.join("\n");
        let Some(m) = SIG.captures(&all) else {
            anyhow::bail!("scripted model: no signal tag in the prompt: {}", all.chars().take(300).collect::<String>())
        };
        let id: i64 = m[1].parse().unwrap();
        let p = self.plans.get(&id).ok_or_else(|| anyhow::anyhow!("scripted model: no plan for signal {id}"))?;
        let turn = params.messages.iter().filter(|x| role(x) == "assistant").count();
        let recovery = params.messages.iter().any(|x| role(x) == "user" && text(x).starts_with("Original signal ("));
        let role_name = if recovery { "recovery" } else { "primary" };
        let first_user = params.messages.iter().find(|x| role(x) == "user").map(text).unwrap_or_default();
        self.world.log(Event::Llm { role: role_name, signal: id, first_user });
        if role_name == "recovery" {
            return Ok(if turn == 0 { send(&recovery_text(id), turn) } else { answer("reported") });
        }
        let acts: &[Act] = match p {
            Plan::Telegram(acts) | Plan::Domain { acts, .. } | Plan::Scheduler(acts) => acts,
        };
        match acts.get(turn) {
            None if p.source() == "telegram" && matches!(acts.last(), Some(Act::Answer)) => Ok(answer("done")),
            None => anyhow::bail!("scripted model: signal {id} has no turn {turn}"),
            Some(Act::Crash) => anyhow::bail!("400 invalid request: scripted crash"),
            Some(Act::Answer) => Ok(answer("done")),
            Some(Act::Send(ok)) => Ok(send(&send_text(id, turn, *ok), turn)),
        }
    }
}

// ── suite ───────────────────────────────────────────────────────────────────

fn in_process(world: Arc<World>) -> Open {
    Arc::new(move || {
        let world = world.clone();
        Box::pin(async move {
            let (client_side, server_side) = tokio::io::duplex(1 << 20);
            tokio::spawn(async move {
                if let Ok(server) = FakeMcp(world).serve(server_side).await {
                    let _ = server.waiting().await;
                }
            });
            let mut info = ClientConfig::default();
            info.client_info = Implementation::new("agent-loop", "0.1.0");
            let client: Client = info.serve(client_side).await?;
            Ok(client)
        })
    })
}

struct Run {
    events: Vec<Event>,
    traced: HashMap<i64, Option<String>>,
    leaked: Vec<String>,
}

async fn run(plans: &[(i64, Plan)]) -> Run {
    let dir = tempfile::tempdir().unwrap();
    let world = Arc::new(World {
        queue: Mutex::new(plans.iter().map(|(id, p)| (*id, p.source(), content_of(*id, p))).collect()),
        events: Mutex::default(),
        drained: CancellationToken::new(),
    });
    let db = Arc::new(AgentDb::open(dir.path().join("agent.db")).unwrap());
    let mcp: SharedMcp =
        Arc::new(McpClient::connect(in_process(world.clone()), ConnectOptions::default()).await.unwrap());
    let provider: Arc<dyn ChatProvider> =
        Arc::new(ScriptedModel { world: world.clone(), plans: plans.iter().cloned().collect() });
    let repo = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let live_dir = dir.path().join("skills");
    std::fs::create_dir(&live_dir).unwrap();
    std::fs::write(live_dir.join("telegram.md"), "---\ntools: *\n---\nFixture Telegram transport rules.\n").unwrap();
    let skill_store = Arc::new(FsSkillStore { live_dir, defaults_dir: repo.join("skills.default") });
    let (store, dry_store) = agent::runs::memory_store_with_view();
    let engine = create_engine(EngineDeps {
        providers: Providers {
            openai: provider.clone(),
            gemini: provider.clone(),
            deepseek: provider.clone(),
            openrouter: Some(provider),
        },
        mcp: mcp.clone(),
        presets: presets(),
        skills: vec!["orchestrator".into(), "routing".into()],
        skill_store: skill_store.clone(),
        memory: db.clone(),
        tracer: Arc::new(local_recorder_tracer(db.clone())),
        codex: Arc::new(NoCodex),
        decisions: Some(support::allow_guard()),
        env: agent::session::EnvConfig { user_email: None, telegram_chat_id: Some(CHAT.to_string()) },
        db: (*db).clone(),
        store,
        dry_store,
        schedule: std::sync::Arc::new(agent::schedule::Schedules::in_memory()),
    });
    let supervisor = create_supervisor(engine.clone());
    run_poll_loop(mcp, supervisor.clone(), world.drained.clone(), std::time::Duration::ZERO).await.unwrap();

    let traced = plans
        .iter()
        .map(|(id, p)| (*id, db.get_trace(&format!("{}:{id}", p.source())).unwrap().map(|(t, _)| t.name)))
        .collect();
    // No agent loop is left open.
    let leaked = engine.loop_ids().await;
    supervisor.shutdown().await;
    let events = world.events.lock().unwrap().clone();
    Run { events, traced, leaked }
}

fn check_signal(id: i64, p: &Plan, segment: &[Event], stored: Option<&String>) -> Result<(), TestCaseError> {
    let want = expected(id, p);
    let at = format!("signal {id} {p:?}");
    let delivered: Vec<String> =
        segment.iter().filter_map(|e| if let Event::Delivered(t) = e { Some(t.clone()) } else { None }).collect();
    prop_assert_eq!(&delivered, &want.texts, "{}: exactly the planned messages reach the chat, in order", at);
    prop_assert!(delivered.iter().all(|t| t.starts_with(&tag(id))), "{}: nothing from another signal", at);
    let rejected = segment.iter().filter_map(|e| if let Event::Rejected(t) = e { Some(t) } else { None });
    prop_assert!(rejected.into_iter().all(|t| t.starts_with(&tag(id))), "{}: no rejected send from another signal", at);
    let llm: Vec<(&str, i64, &String)> = segment
        .iter()
        .filter_map(|e| {
            if let Event::Llm { role, signal, first_user } = e { Some((*role, *signal, first_user)) } else { None }
        })
        .collect();
    prop_assert!(llm.iter().all(|(_, s, _)| *s == id), "{}: the model only ever sees this signal", at);
    prop_assert_eq!(llm.iter().any(|(r, _, _)| *r == "primary"), true, "{}: every signal runs the primary loop", at);
    prop_assert_eq!(
        llm.iter().any(|(r, _, _)| *r == "recovery"),
        want.failed,
        "{}: recovery runs iff the signal failed",
        at
    );
    let history = segment.iter().filter(|e| matches!(e, Event::History)).count();
    prop_assert_eq!(
        history,
        usize::from(p.source() == "telegram"),
        "{}: Telegram history is preloaded once, only for telegram signals",
        at
    );
    if history > 0 {
        let h = segment.iter().position(|e| matches!(e, Event::History)).unwrap();
        let first = segment.iter().position(|e| matches!(e, Event::Llm { role: "primary", .. }));
        prop_assert!(first.is_some_and(|f| h < f), "{}: history before the first primary turn", at);
    }
    if p.source() != "telegram"
        && let Some((_, _, first_user)) = llm.iter().find(|(r, _, _)| *r == "primary")
    {
        prop_assert_eq!(*first_user, &content_of(id, p), "{}: a non-Telegram signal reaches the model verbatim", at);
    }
    prop_assert_eq!(
        stored,
        Some(&format!("signal:{}", p.source())),
        "{}: one trace per signal, named by its source",
        at
    );
    Ok(())
}

#[test]
fn handles_every_signal_once_in_order_along_the_path_its_plan_calls_for() {
    let runs = std::env::var("PROPERTY_RUNS").ok().and_then(|v| v.parse().ok()).unwrap_or(25);
    let witnessed = Mutex::new(BTreeSet::new());
    let mut runner = TestRunner::new(Config { cases: runs, ..Config::default() });
    let ids = prop::collection::btree_set(1i64..=10_000, 1..=6)
        .prop_map(|s| s.into_iter().collect::<Vec<_>>())
        .prop_shuffle();
    runner
        .run(&(ids, prop::collection::vec(plan(), 6..=6)), |(ids, ps)| {
            let plans: Vec<(i64, Plan)> = ids.into_iter().zip(ps).collect();
            let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
            let Run { events, traced, leaked } = runtime.block_on(run(&plans));
            let pops: Vec<Option<i64>> =
                events.iter().filter_map(|e| if let Event::Pop(id) = e { Some(*id) } else { None }).collect();
            let mut want_pops: Vec<Option<i64>> = plans.iter().map(|(id, _)| Some(*id)).collect();
            want_pops.push(None);
            prop_assert_eq!(pops, want_pops, "each signal taken once, in queue order, then the queue is found empty");
            // Sequential: everything between two pops belongs to the signal taken first.
            let starts: Vec<usize> =
                events.iter().enumerate().filter(|(_, e)| matches!(e, Event::Pop(_))).map(|(i, _)| i).collect();
            for (k, (id, p)) in plans.iter().enumerate() {
                let segment = &events[starts[k] + 1..starts[k + 1]];
                check_signal(*id, p, segment, traced[id].as_ref())?;
                let want = expected(*id, p);
                witnessed.lock().unwrap().insert(format!("loop → {}", if want.failed { "recovery" } else { "ok" }));
                if segment.iter().any(|e| matches!(e, Event::Rejected(_))) {
                    witnessed.lock().unwrap().insert("a rejected send".to_owned());
                }
            }
            prop_assert!(leaked.is_empty(), "no agent loop left open: {:?}", leaked);
            Ok(())
        })
        .unwrap();
    let mut want = vec!["a rejected send", "loop → ok", "loop → recovery"];
    want.sort();
    assert_eq!(witnessed.into_inner().unwrap().into_iter().collect::<Vec<_>>(), want);
}
