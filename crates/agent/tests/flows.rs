// Workflows (`flows.rs`): the gate a definition passes, the authoring
// tools over the engine's catalog, the author's catalog and probes, the
// model steps, and a scheduled task's saved workflow as the signal's primary
// run (design 04), and a dry run of a draft without a signal (the sandbox
// step, design 05, test 2).
mod support;
use agent::{
    agent::{Engine, EngineDeps, Providers, create_engine},
    flows::check,
    mcp::Mcp,
    models::{ModelPreset, Presets, ReasoningEffort},
    providers::{ToolDef, assistant},
};
use agent::{
    supervisor::{PendingSignal, create_supervisor},
    trace_model::Observation,
};
use async_trait::async_trait;
use serde_json::{Map, Value, json};
use std::sync::Arc;
use support::{AnySkill, NoCodex, PersistLog, Scripted, ToolMcp, activate_definition, mark_tried, recording};
use tokio_util::sync::CancellationToken;

// ── the store ───────────────────────────────────────────────────────────────

// The language card of workflow-author.md stays true to the engine: its
// reference definitions parse and validate in the engine's dialect, and pass
// the gate's own rules for an agent version (design 04). Their function
// names stand for real ones, so any name is callable here.
#[test]
fn the_authoring_skills_reference_definition_is_valid() {
    struct Any;
    impl ouro_runtime::ows::model::Catalog for Any {
        fn has(&self, _: &str) -> bool {
            true
        }
        fn events(&self) -> bool {
            true
        }
        fn children(&self) -> bool {
            true
        }
    }
    let skill =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../skills.default/workflow-author.md"))
            .unwrap();
    let examples: Vec<&str> = skill.split("```yaml\n").skip(1).filter_map(|block| block.split("```").next()).collect();
    assert!(!examples.is_empty(), "workflow-author shows a reference");
    for example in examples {
        let wf = ouro_runtime::ows::model::parse_yaml(example).unwrap_or_else(|e| panic!("{e}\n{example}"));
        ouro_runtime::ows::model::validate(&wf, &Any).unwrap_or_else(|e| panic!("{e}\n{example}"));
        agent::flows::agent_rules(&wf).unwrap_or_else(|e| panic!("{e}\n{example}"));
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
    written: Arc<support::TraceLog>,
    persist: Arc<PersistLog>,
    store: (Arc<dyn ouro_runtime::durable::RunStore>, Arc<dyn ouro_runtime::durable::RunStore>),
}

fn world(replies: Vec<&str>) -> World {
    world_with(replies, None)
}

fn world_with(replies: Vec<&str>, decisions: Option<Arc<agent::decisions::DecisionClient>>) -> World {
    world_on(replies, decisions, agent::runs::memory_store_with_view(), &[])
}

// A world on the engine store `store` (another process's, after a restart),
// MCP offering `extra` tools too.
fn world_on(
    replies: Vec<&str>,
    decisions: Option<Arc<agent::decisions::DecisionClient>>,
    store: (Arc<dyn ouro_runtime::durable::RunStore>, Arc<dyn ouro_runtime::durable::RunStore>),
    extra: &[&str],
) -> World {
    let llm: Arc<Scripted> = Arc::new(Scripted::new(replies.into_iter().map(assistant).collect()));
    let schema = |properties: &[&str], required: &[&str]| {
        let properties: Map<String, Value> = properties.iter().map(|p| (p.to_string(), json!({}))).collect();
        json!({"type": "object", "properties": properties, "required": required})
    };
    let defs = vec![
        ToolDef::new("search_news", "search", schema(&["query", "k"], &[])),
        ToolDef::new("send_telegram_message", "send", schema(&["chatId", "text"], &["text"])),
        ToolDef::new("get_next_signal", "pop", schema(&[], &[])),
    ]
    .into_iter()
    .chain(extra.iter().map(|name| ToolDef::new(name, name, json!({"type":"object"}))))
    .collect();
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
        env: Default::default(),
        db: agent::db::AgentDb::open(":memory:").unwrap(),
        store: store.0.clone(),
        dry_store: store.1.clone(),
        schedule: std::sync::Arc::new(agent::schedule::Schedules::in_memory()),
    });
    World { engine, mcp, written, persist, store }
}

impl World {
    fn calls(&self) -> Vec<(String, Map<String, Value>)> {
        self.mcp.calls.lock().unwrap().clone()
    }

    async fn tool(&self, name: &str, args: Value) -> anyhow::Result<Value> {
        self.engine.ready().await?;
        let reply = agent::flows::tool(&self.engine, name, args.as_object().unwrap()).await?;
        Ok(serde_json::from_str(&reply).unwrap())
    }

    // A scheduled task's fire (signal `id`) naming the saved workflow `name`:
    // the signal's answer, and what its trace holds once it ended.
    async fn run(&self, name: &str, id: i64) -> (anyhow::Result<String>, Vec<Observation>) {
        let supervisor = create_supervisor(self.engine.clone());
        let signal = PendingSignal {
            id,
            source: "scheduler".into(),
            content: "Daily digest tick.".into(),
            env_context: None,
            created_at: "2026-09-06 12:00:00".into(),
            payload: Some(json!({"workflow": name})),
        };
        let result = supervisor.run_signal(&signal).await;
        let trace = format!("scheduler:{id}");
        let observations =
            self.written.all().into_iter().filter(|t| t.id == trace).flat_map(|t| t.observations).collect();
        (result, observations)
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
    let parse = |text: &str| ouro_runtime::ows::model::parse_yaml(text).unwrap_or_else(|e| panic!("{e}\n{text}"));
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

// A workflow sends to the user's chat from its input, not from an id
// written into the definition.
// The author's catalog: every callable function one line each, one in
// full on request; the probe runs reads only, cut to their shape.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_author_can_inspect_signal_and_loop_hosts_but_probes_cannot_execute_them() {
    use agent::flows::{functions, probe};
    let w = world(vec![]);
    let catalog = functions(&w.engine, &Map::new()).unwrap();
    // Follow the definitions an author actually copies, rather than a second
    // list of expected names that could omit the next host function too.
    for definition in [agent::supervisor::SIGNAL, agent::agent::AGENT_LOOP] {
        let wf = ouro_runtime::ows::model::parse_yaml(definition).unwrap();
        for site in ouro_runtime::ows::model::call_sites(&wf) {
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
    for function in agent::functions::HOST_FUNCTIONS.iter().filter(|f| f.context.starts_with("signal")) {
        let detail: Value =
            serde_json::from_str(&functions(&w.engine, json!({"name":function.name}).as_object().unwrap()).unwrap())
                .unwrap();
        // The first-turn context functions serve a loop too.
        let loop_context = ["signal_input", "recent_activity", "pending_confirmations", "preselect_skills"];
        let context = if loop_context.contains(&function.name) { "signal|agent-loop" } else { "signal" };
        assert_eq!(detail["context"], context, "{}", function.name);
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
    let primary: Value =
        serde_json::from_str(&functions(&w.engine, json!({"name":"primary_loop"}).as_object().unwrap()).unwrap())
            .unwrap();
    assert!(primary["parameters"]["properties"].get("loop").is_some());
    for name in ["agent_turn", "tool_once", "tool_repeat", "classify", "agent_settle", "loop_setup"] {
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

// ── authoring tools ─────────────────────────────────────────────────────────

// The engine keeps the versions: numbered, one active per name, the one
// before it retired, each with the note and the dry run the agent recorded
// by its content (the store contract the agent's own table had).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_catalog_keeps_versions_and_one_active_each() {
    let w = world(Vec::new());
    let text = |name: &str, n: u32| {
        format!(
            "document: {{ dsl: '1.0.4', namespace: agent, name: {name}, version: '1.0.0' }}\ndo: [ {{ a: {{ set: {{ n: {n} }} }} }} ]\n"
        )
    };
    assert_eq!(w.tool("read_workflow", json!({"name":"digest"})).await.unwrap()["found"], false);
    let v1 =
        w.tool("save_workflow", json!({"name":"digest","definition":text("digest", 1),"note":"first"})).await.unwrap();
    let v2 = w.tool("save_workflow", json!({"name":"digest","definition":text("digest", 2)})).await.unwrap();
    w.tool("save_workflow", json!({"name":"other","definition":text("other", 1)})).await.unwrap();
    assert_eq!(
        (v1["version"].as_u64(), v1["status"].as_str(), v2["version"].as_u64()),
        (Some(1), Some("draft"), Some(2))
    );
    let read = |version: u64| w.tool("read_workflow", json!({"name":"digest","version":version}));
    assert_eq!(w.engine.runtime.definitions("digest").await.unwrap().active, None, "a saved version is a draft");

    assert_eq!(read(1).await.unwrap()["tried"], false, "a saved version is untried");
    mark_tried(&w.engine, "digest", &text("digest", 1));
    assert_eq!(read(1).await.unwrap()["tried"], true);
    assert_eq!(read(2).await.unwrap()["tried"], false, "per version");
    let unknown = ouro_runtime::durable::digest_of(&text("digest", 9));
    assert!(agent::flows::WorkflowMeta::mark_tried(&w.engine.deps.db, "digest", &unknown).is_err());
    w.tool("activate_workflow", json!({"name":"digest","version":1})).await.unwrap();
    assert_eq!(w.tool("read_workflow", json!({"name":"digest"})).await.unwrap()["definition"], text("digest", 1));
    mark_tried(&w.engine, "digest", &text("digest", 2));
    w.tool("activate_workflow", json!({"name":"digest","version":2})).await.unwrap();
    assert_eq!(w.engine.runtime.definitions("digest").await.unwrap().active, Some(2));
    assert_eq!(read(1).await.unwrap()["status"], "retired");
    assert_eq!(
        (read(2).await.unwrap()["version"].as_u64(), read(2).await.unwrap()["note"].clone()),
        (Some(2), Value::Null)
    );
    assert_eq!(read(1).await.unwrap()["note"], "first");

    assert!(w.tool("activate_workflow", json!({"name":"digest","version":9})).await.is_err());
    assert_eq!(
        w.engine.runtime.definitions("digest").await.unwrap().active,
        Some(2),
        "a failed activation changes nothing"
    );
    let listed = w.tool("list_workflows", json!({})).await.unwrap();
    let mine: Vec<(String, u64)> = listed["workflows"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|w| !w["builtin"].as_bool().unwrap())
        .map(|w| (w["name"].as_str().unwrap().to_owned(), w["latest_version"].as_u64().unwrap()))
        .collect();
    assert_eq!(mine, [("digest".to_string(), 2), ("other".to_string(), 1)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_authoring_tools_save_drafts_and_activate_only_valid_versions() {
    let w = world(Vec::new());
    // Every tool is offered to the model, so the loop can reach it.
    let offered: Vec<&str> = agent::agent::synthetic_definitions().iter().map(|t| t.name.as_str()).collect();
    assert!(agent::flows::TOOLS.iter().all(|t| offered.contains(t)), "{offered:?}");

    let broken = w
        .tool("save_workflow", json!({"name":"digest","definition":DIGEST.replace("call: search_news", "call: nope")}))
        .await;
    let broken = broken.unwrap_err().to_string();
    assert!(broken.contains("/do/0/search") && broken.contains("nope"), "{broken}");
    let renamed = w.tool("save_workflow", json!({"name":"other","definition":DIGEST})).await;
    assert!(renamed.unwrap_err().to_string().contains("document.name is `digest`"));

    let saved = w.tool("save_workflow", json!({"name":"digest","definition":DIGEST,"note":"first"})).await.unwrap();
    assert_eq!((saved["version"].clone(), saved["status"].clone()), (json!(1), json!("draft")));
    let deliveries: Vec<(String, String)> = saved["calls"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| (c["function"].as_str().unwrap().into(), c["delivery"].as_str().unwrap().into()))
        .collect();
    assert_eq!(
        deliveries,
        [
            ("search_news".to_string(), "at_least_once".to_string()),
            ("llm_compose".into(), "at_least_once".into()),
            ("send_telegram_message".into(), "at_most_once".into()),
            ("set_memory".into(), "at_most_once".into()),
        ]
    );
    let digest = |listed: &Value| {
        listed["workflows"].as_array().unwrap().iter().find(|w| w["name"] == "digest").unwrap().clone()
    };
    let listed = w.tool("list_workflows", json!({})).await.unwrap();
    assert_eq!(digest(&listed)["active_version"], Value::Null);

    assert!(w.tool("activate_workflow", json!({"name":"digest","version":2})).await.is_err());
    let untried = w.tool("activate_workflow", json!({"name":"digest","version":1})).await.unwrap_err();
    assert!(untried.to_string().contains("has not completed a dry run"), "{untried}");
    mark_tried(&w.engine, "digest", DIGEST);
    w.tool("activate_workflow", json!({"name":"digest","version":1})).await.unwrap();
    let listed = w.tool("list_workflows", json!({})).await.unwrap();
    assert_eq!(digest(&listed)["active_version"], json!(1));
    let read = w.tool("read_workflow", json!({"name":"digest"})).await.unwrap();
    assert_eq!((read["definition"].as_str(), read["status"].as_str()), (Some(DIGEST), Some("active")));
    assert_eq!(w.tool("read_workflow", json!({"name":"nope"})).await.unwrap()["found"], false);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_gate_checks_definitions_against_what_the_agent_can_call() {
    let w = world(Vec::new());
    let save = |definition: String| w.tool("save_workflow", json!({"name":"digest","definition":definition}));
    let unknown = save(DIGEST.replace("call: search_news", "call: search_web")).await.unwrap_err().to_string();
    assert!(unknown.contains("/do/0/search") && unknown.contains("search_web"), "{unknown}");
    let withheld = save(DIGEST.replace("call: search_news", "call: get_next_signal")).await.unwrap_err().to_string();
    assert!(withheld.contains("get_next_signal"), "{withheld}");
    assert!(save("not: [ a workflow".into()).await.is_err());
    // Argument names are checked against the tool's schema and the model steps'.
    let misspelt = save(DIGEST.replace("with: { query:", "with: { qeury:")).await.unwrap_err().to_string();
    assert!(
        misspelt.contains("/do/0/search/with: `search_news` takes no argument `qeury` (it takes: query, k)"),
        "{misspelt}"
    );
    let missing =
        save(DIGEST.replace("with: { text: '${ . }' }", "with: { chatId: 1 }")).await.unwrap_err().to_string();
    assert!(missing.contains("/do/2/send/with: `send_telegram_message` needs the argument `text`"), "{missing}");
    let model = save(DIGEST.replace("skill: news-digest", "skills: news-digest")).await.unwrap_err().to_string();
    assert!(model.contains("`llm_compose` takes no argument `skills`"), "{model}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn guard_is_reserved_readable_and_its_private_functions_are_not_in_the_catalog() {
    let w = world(vec![]);
    for name in ["save_workflow", "activate_workflow"] {
        let args = json!({"name":"guard","version":1,"definition":agent::flows::built_in("guard")});
        assert!(w.tool(name, args).await.unwrap_err().to_string().contains("reserved"));
    }
    let read = w.tool("read_workflow", json!({"name":"guard"})).await.unwrap();
    assert!(read["definition"].as_str().unwrap().contains("call: review"));
    // Even a directly stored override never replaces the reserved
    // definition: the rounds start the built-in text (engine_runs.rs,
    // an_activated_guard_version_never_reviews_a_round).
    let forged = "document: { dsl: '1.0.4', namespace: agent, name: guard, version: '9.0.0' }\ndo: [ { allow: { set: { verdicts: [] } } } ]\n";
    let version = w.engine.runtime.save("guard", forged).await.unwrap().version;
    w.engine.runtime.activate("guard", version).await.unwrap();
    let builtin = agent::flows::builtin_version(&w.engine.runtime, "guard").await.unwrap().unwrap();
    let definitions = w.engine.runtime.definitions("guard").await.unwrap();
    assert_eq!(definitions.versions[&builtin].yaml, agent::flows::built_in("guard").unwrap());
    assert_eq!(w.tool("read_workflow", json!({"name":"guard"})).await.unwrap()["definition"], agent::flows::GUARD);
    for function in ["review", "confirm_request", "resolve_review"] {
        let def = format!(
            "document: {{ dsl: '1.0.4', namespace: t, name: other, version: '1.0.0' }}\ndo: [ {{ step: {{ call: {function} }} }} ]"
        );
        assert!(check(&*w.engine.deps.mcp, &def).is_err(), "{function}");
        let refused = w.tool("save_workflow", json!({"name":"other","definition":def})).await.unwrap_err();
        assert!(refused.to_string().contains("reserved guard round"), "{function}: {refused}");
        assert!(agent::flows::functions(&w.engine, json!({"name":function}).as_object().unwrap()).is_err());
    }
}

// ── a scheduled task's saved workflow (design 04) ───────────────────────────
// It runs as the signal's primary run; one that faulted before any write
// hands its task to the agent loop, one that faulted after a write goes to
// the report.

fn first<'a>(observations: &'a [Observation], name: &str) -> &'a Observation {
    observations.iter().find(|o| o.name == name).unwrap_or_else(|| {
        panic!("no observation {name} in {:?}", observations.iter().map(|o| &o.name).collect::<Vec<_>>())
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fault_before_any_write_reports_that_nothing_changed() {
    let w = world(vec!["did the task myself"]);
    let failing =
        DIGEST.replace("call: search_news", "call: search_news\n      output: { as: '${ error(\"no news\") }' }");
    activate_definition(&w.engine, "digest", &failing).await;
    let (result, observations) = w.run("digest", 7).await;
    assert_eq!(result.unwrap(), "did the task myself", "handed over");
    let fallback = first(&observations, "fallback");
    let error = &fallback.meta("error").unwrap();
    assert_eq!(fallback.meta("effects_started"), Some(&json!(false)));
    assert_eq!(error["instance"], "/do/0/search");
    assert!(error["type"].as_str().unwrap().contains("expression"), "{error}");
    assert_eq!(first(&observations, "runner").level, "ERROR");
    assert!(w.calls().iter().all(|(n, _)| n != "send_telegram_message"), "nothing was written");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fault_after_a_write_reports_that_something_may_have_changed() {
    let w = world(vec!["text", "reported"]);
    let failing = DIGEST.replace(
        "  - mark:",
        "  - stop: { raise: { error: { type: 'https://x/e', status: 500, detail: stopped } } }\n  - mark:",
    );
    activate_definition(&w.engine, "digest", &failing).await;
    let (result, observations) = w.run("digest", 7).await;
    let error = result.unwrap_err().to_string();
    assert!(error.contains("may have changed something") && error.contains("stopped"), "{error}");
    assert!(observations.iter().any(|o| o.name == "recovery"), "the failure is reported");
    assert!(w.calls().iter().any(|(n, _)| n == "send_telegram_message"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_an_active_version_runs() {
    let w = world(vec!["handled 1", "handled 2"]);
    let (result, _) = w.run("digest", 7).await;
    assert_eq!(result.unwrap(), "handled 1");
    w.tool("save_workflow", json!({"name":"digest","definition":DIGEST})).await.unwrap();
    let (result, observations) = w.run("digest", 8).await;
    assert_eq!(result.unwrap(), "handled 2", "a draft does not run");
    assert!(observations.iter().any(|o| o.name == "fallback"));
    let digest = ["search_news", "send_telegram_message"];
    assert!(w.calls().iter().all(|(n, _)| !digest.contains(&n.as_str())), "the draft ran nothing");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_definition_that_no_longer_validates_changes_nothing() {
    let before = world_on(vec![], None, agent::runs::memory_store_with_view(), &["search_web"]);
    activate_definition(&before.engine, "digest", &DIGEST.replace("call: search_news", "call: search_web")).await;
    before.engine.halt().await;
    // The next process's MCP has no `search_web` (and no snapshot of it).
    let w = world_on(vec!["handled"], None, before.store.clone(), &[]);
    let (result, observations) = w.run("digest", 7).await;
    assert_eq!(result.unwrap(), "handled", "handed over, nothing ran");
    let fallback = first(&observations, "fallback");
    let record = fallback.meta("fallback").unwrap();
    assert_eq!(record["reason"], "invalid");
    assert!(record["error"].as_str().unwrap().contains("search_web"), "{record}");
    let digest = ["search_web", "search_news", "send_telegram_message"];
    assert!(w.calls().iter().all(|(n, _)| !digest.contains(&n.as_str())), "nothing of it ran");
}

// A saved workflow waiting on its own child (a workflow's `start_child`)
// suspends the signal; the event wakes it in the engine's worker, and the
// signal's trace goes on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_waiting_saved_child_recovers_in_its_parents_session() {
    let w = world(vec![]);
    let child = r#"document: { dsl: '1.0.4', namespace: t, name: child, version: '1.0.0' }
do:
 - event: { listen: { to: { one: { with: { type: child-answer } } } } }
 - result: { set: '${ 42 }' }
"#;
    activate_definition(&w.engine, "child", child).await;
    let parent = r#"document: { dsl: '1.0.4', namespace: t, name: parent, version: '1.0.0' }
do:
 - start:
     call: start_child
     with: { name: child, input: {} }
     export: { as: '${ $context + { child: . } }' }
 - wait:
     listen: { to: { one: { with: { type: '${ $context.child.done }' } } } }
 - result:
     call: child_outcome
     with: { id: '${ $context.child.id }', key: '${ $context.child.key }' }
"#;
    activate_definition(&w.engine, "parent", parent).await;
    let (result, _) = w.run("parent", 2).await;
    assert_eq!(serde_json::from_str::<Value>(&result.unwrap()).unwrap()["resumes"], true);
    let event = json!({"source":"test","id":"answer-1","type":"child-answer","data":{}});
    w.engine.runtime.deliver(event, None).await.unwrap();
    let ended = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        agent::runs::wait_end(&w.engine, "signal:2", &CancellationToken::new()),
    )
    .await
    .unwrap()
    .unwrap();
    let agent::runs::Status::Completed(output) = ended else { panic!("{ended:?}") };
    assert_eq!(output["workflow"], "parent");
    assert_eq!(output["output"]["Completed"], 42);
    agent::runs::flushed(&w.engine, std::time::Duration::from_secs(2)).await;
    let traces: Vec<_> = w.written.all().into_iter().filter(|t| t.id == "scheduler:2").collect();
    assert!(!traces.is_empty());
    assert!(traces.iter().all(|t| t.session_id.as_deref() == Some("scheduler:2")), "one session");
}

// ── choose: a jump where Jev decides ────────────────────────────────────────

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

#[test]
fn a_switch_that_misses_or_invents_a_choose_option_is_a_warning() {
    let parse = |text: &str| ouro_runtime::ows::model::parse_yaml(text).unwrap_or_else(|e| panic!("{e}\n{text}"));
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

// ── dry runs without a signal (design 05, test 2) ───────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dry_run_records_the_writes_and_changes_nothing() {
    let w = world(vec!["Today: Rust 2.0"]);
    let draft = DIGEST.replace("do:\n", "do:\n  - pause: { wait: PT1H }\n");
    let saved = w.tool("save_workflow", json!({"name": "digest", "definition": draft})).await.unwrap();
    assert_eq!(saved["version"], 1, "{saved}");
    let session = support::task_context("author");
    let cancel = CancellationToken::new();
    let trace = w.engine.deps.tracer.trace(agent::tracing::TraceStart {
        id: "author".into(),
        name: "t".into(),
        ..Default::default()
    });
    let scope: agent::tracing::Scope = trace.clone();
    let args = |v: Value| v.as_object().unwrap().clone();
    let try_draft = agent::sandbox::try_draft;
    let reply = try_draft(&w.engine, &args(json!({"name": "digest"})), &session, &scope, &cancel).await.unwrap();
    trace.end();
    let reply: Value = serde_json::from_str(&reply).unwrap();
    assert_eq!((reply["status"].as_str(), reply["version"].as_u64()), (Some("completed"), Some(1)), "{reply}");

    assert_eq!(w.calls().iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(), ["search_news"], "only the read ran");
    assert!(w.persist.0.lock().unwrap().is_empty(), "no watermark moved");
    let writes: Vec<(&str, &str, &str)> = reply["writes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|w| (w["function"].as_str().unwrap(), w["task"].as_str().unwrap(), w["via"].as_str().unwrap()))
        .collect();
    assert_eq!(writes, [("send_telegram_message", "/do/3/send", "yaml"), ("set_memory", "/do/4/mark", "yaml")]);
    assert_eq!(reply["writes"][0]["arguments"]["text"], "Today: Rust 2.0");
    let compose = reply["steps"].as_array().unwrap().iter().find(|s| s["task"] == "/do/2/compose").unwrap();
    assert_eq!(compose["output"], "Today: Rust 2.0");
    assert_eq!(reply["path"], json!(["pause", "search", "compose", "send", "mark"]));
    let digest = ouro_runtime::durable::digest_of(&draft);
    let meta = agent::flows::WorkflowMeta::get(&w.engine.deps.db, "digest", &digest).unwrap().unwrap();
    assert!(meta.tried_ms.is_some(), "a completed dry run marks it");
    assert!(reply["next"].as_str().unwrap().contains("fix"), "{reply}");
    let observations = w.written.all().into_iter().find(|t| t.id == "author").unwrap().observations;
    let run = observations.iter().find(|o| o.name == "dry-run").unwrap();
    assert_eq!(run.metadata.as_ref().unwrap()["workflow"], "digest");
    let search = observations.iter().find(|o| o.name == "search_news").unwrap();
    assert_eq!(search.parent_observation_id.as_deref(), Some(run.id.as_str()));
    assert!(try_draft(&w.engine, &args(json!({"name": "nope"})), &session, &scope, &cancel).await.is_err());
}

// A version that no longer passes the gate (a tool went away) is not tried.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dry_run_of_a_version_that_no_longer_fits_the_tools_is_refused() {
    let before = world_on(vec![], None, agent::runs::memory_store_with_view(), &["search_web"]);
    let calling = DIGEST.replace("call: search_news", "call: search_web").replace("query:", "q:");
    before.tool("save_workflow", json!({"name": "digest", "definition": calling})).await.unwrap();
    let w = world_on(vec![], None, before.store.clone(), &[]);
    let session = support::task_context("author");
    let args = json!({"name": "digest", "version": 1});
    let invalid = agent::sandbox::try_draft(
        &w.engine,
        args.as_object().unwrap(),
        &session,
        &agent::tracing::null_scope(),
        &CancellationToken::new(),
    )
    .await;
    assert!(invalid.unwrap_err().to_string().contains("search_web"));
}
