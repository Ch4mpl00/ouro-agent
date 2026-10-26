// Workflows (`flows.rs`): validation against what the agent can call, the
// author's catalog and probes, and the model steps. Runs of saved workflows
// return with the engine migration's versions step (design 03, decision 9);
// their tests are parked in tests/pending/flows.rs until then.
mod support;
use agent::{
    agent::{Engine, EngineDeps, Providers, create_engine},
    flows::check,
    mcp::Mcp,
    models::{ModelPreset, Presets, ReasoningEffort},
    providers::{ToolDef, assistant},
};
use async_trait::async_trait;
use serde_json::{Map, Value, json};
use std::sync::Arc;
use support::{AnySkill, NoCodex, PersistLog, Scripted, ToolMcp, recording};
use tokio_util::sync::CancellationToken;

// ── the store ───────────────────────────────────────────────────────────────

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
    let (tracer, _written) = recording();
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
        store: agent::runs::memory_store(),
        schedule: std::sync::Arc::new(agent::schedule::MemorySchedule::default()),
    });
    World { engine, mcp }
}

impl World {
    fn calls(&self) -> Vec<(String, Map<String, Value>)> {
        self.mcp.calls.lock().unwrap().clone()
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
