// Port of src/workflow/execute.test.ts, one test per case. The executor runs
// on a real engine: tools go to a recording MCP, compose / agent steps to a
// scripted provider, set_memory to the engine's KV, code_agent to Codex and
// decide to a decision client over a fake API. Where the TS test mocks
// startAgentLoop, the llm_agent step here starts a real loop and the test
// reads what the loop sent and the trace it left.
mod support;
use agent::{
    agent::{EngineDeps, Providers, create_engine},
    codex::Codex,
    decisions::{DecisionClient, decision_client},
    models::{ModelPreset, Presets, ReasoningEffort},
    providers::{
        ChatProvider, CompletionParams, CompletionResult, JsonApi, Message, ProviderKind, ToolDef, assistant, role,
    },
    session::Session,
    skills::{PATCH_MARKER, SkillEntry, SkillFile, SkillSource, SkillStore, SkillTools, Written},
    trace_model::Observation,
    tracing::{Scope, Trace, TraceStart, Tracer},
    workflow::{
        ExecContext, ExecResult, VariableStore, Workflow, WorkflowError, WorkflowRunner, WorkflowSchema,
        classify_error, create_workflow_runner, render_input_xml,
    },
};
use async_trait::async_trait;
use serde_json::{Map, Value, json};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use support::{PersistLog, ToolMcp, TraceLog, call, recording, task_context};
use tokio_util::sync::CancellationToken;

fn presets() -> Presets {
    let p = |model: &str, reasoning_effort| ModelPreset { model: model.into(), reasoning_effort };
    Presets {
        base: p("gpt-5.4-mini", ReasoningEffort::Disabled),
        smart: p("deepseek-v4-pro", ReasoningEffort::Max),
        smartest: p("gpt-5.4", ReasoningEffort::Max),
        compiler: p("gemini-3-flash-preview", ReasoningEffort::Low),
    }
}

// ── shared mocks ────────────────────────────────────────────────────────────

#[derive(Clone)]
struct LlmCall {
    model: String,
    messages: Vec<Message>,
    tools: Vec<ToolDef>,
}

// Replies in order (an empty answer once the queue runs out); an Err reply
// fails the call.
#[derive(Default)]
struct Llm {
    replies: Mutex<VecDeque<Result<Message, &'static str>>>,
    calls: Mutex<Vec<LlmCall>>,
}

#[async_trait]
impl ChatProvider for Llm {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Openai
    }
    async fn complete(&self, p: &mut CompletionParams<'_>) -> anyhow::Result<CompletionResult> {
        self.calls.lock().unwrap().push(LlmCall {
            model: p.model.clone(),
            messages: p.messages.clone(),
            tools: p.tools.clone(),
        });
        let next = self.replies.lock().unwrap().pop_front().unwrap_or_else(|| Ok(assistant("")));
        Ok(CompletionResult {
            message: next.map_err(|e| anyhow::anyhow!(e))?,
            finish_reason: Some("stop".into()),
            usage: None,
        })
    }
}

#[derive(Default)]
struct Skills {
    bodies: HashMap<String, String>,
    patches: HashMap<String, String>,
}

#[async_trait]
impl SkillStore for Skills {
    async fn read_skill(&self, name: &str) -> anyhow::Result<Option<SkillFile>> {
        Ok(self.bodies.get(name).map(|body| SkillFile {
            body: body.clone(),
            tools: SkillTools::All,
            source: SkillSource::Default,
        }))
    }
    async fn read_patch(&self, name: &str) -> anyhow::Result<Option<String>> {
        Ok(self.patches.get(name).cloned())
    }
    async fn save_skill(&self, _: &str, _: &str) -> anyhow::Result<Written> {
        anyhow::bail!("read-only")
    }
    async fn list_skills(&self) -> anyhow::Result<Vec<SkillEntry>> {
        Ok(Vec::new())
    }
}

#[derive(Default)]
struct Codexes {
    runs: Mutex<Vec<Value>>,
    reply: Option<Result<&'static str, &'static str>>,
}

#[async_trait]
impl Codex for Codexes {
    async fn run(&self, request: Value, _: &CancellationToken) -> anyhow::Result<String> {
        self.runs.lock().unwrap().push(request);
        match self.reply {
            None => anyhow::bail!("code_agent: codex backend not configured"),
            Some(Ok(content)) => Ok(content.into()),
            Some(Err(message)) => anyhow::bail!(message),
        }
    }
}

// The decisions API: answers each filter question from the item itself and
// records the rules each request carried.
struct FakeDecisions {
    p_of: Box<dyn Fn(&Value) -> f64 + Send + Sync>,
    fail: Option<&'static str>,
    rules: Mutex<Vec<String>>,
}

#[async_trait]
impl JsonApi for FakeDecisions {
    async fn post(&self, _: &str, body: Value, _: &CancellationToken) -> anyhow::Result<Value> {
        if let Some(fail) = self.fail {
            anyhow::bail!(fail);
        }
        self.rules.lock().unwrap().push(body["state"]["rules"].as_str().unwrap().into());
        let answers: Map<String, Value> = body["state"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| (format!("item_{}", i["n"]), json!({"type":"noul","noul":(self.p_of)(&i["item"])})))
            .collect();
        Ok(json!({"model":"fake","answers":answers}))
    }
}

fn fake_decisions(
    p_of: impl Fn(&Value) -> f64 + Send + Sync + 'static,
    fail: Option<&'static str>,
) -> (Arc<DecisionClient>, Arc<FakeDecisions>) {
    let api = Arc::new(FakeDecisions { p_of: Box::new(p_of), fail, rules: Mutex::default() });
    (decision_client("fake".into(), api.clone()), api)
}

enum Reply {
    Text(&'static str),
    Json(Value),
}

#[derive(Default)]
struct Setup {
    tools: Vec<(&'static str, Reply)>,
    llm: Vec<Result<Message, &'static str>>,
    skills: Vec<(&'static str, &'static str)>,
    patches: Vec<(&'static str, &'static str)>,
    codex: Option<Result<&'static str, &'static str>>,
    decisions: Option<Arc<DecisionClient>>,
}

struct Exec {
    runner: Arc<WorkflowRunner>,
    engine: Arc<agent::agent::Engine>,
    mcp: Arc<ToolMcp>,
    llm: Arc<Llm>,
    persist: Arc<PersistLog>,
    codex: Arc<Codexes>,
    tracer: Arc<dyn Tracer>,
    written: Arc<TraceLog>,
}

impl Exec {
    fn new(setup: Setup) -> Exec {
        let llm = Arc::new(Llm { replies: Mutex::new(setup.llm.into()), calls: Mutex::default() });
        let provider: Arc<dyn ChatProvider> = llm.clone();
        let defs = ["search_news", "list_news", "send_telegram_message"]
            .iter()
            .map(|n| ToolDef::new(n, n, json!({"type":"object","properties":{}})))
            .collect();
        let mcp = ToolMcp::returning(defs, "");
        let responses: HashMap<&'static str, Reply> = setup.tools.into_iter().collect();
        mcp.always(Arc::new(move |name, _, _| {
            let out = match responses.get(name.as_str()) {
                None => format!("[tool error] unknown tool {name}"),
                Some(Reply::Text(t)) => (*t).to_owned(),
                Some(Reply::Json(v)) => v.to_string(),
            };
            Box::pin(async move { Ok(out) })
        }));
        let mut skills = Skills::default();
        skills.bodies.insert("orchestrator".into(), "ORCHESTRATOR".into());
        for (name, body) in setup.skills {
            skills.bodies.insert(name.into(), body.into());
        }
        for (name, patch) in setup.patches {
            skills.patches.insert(name.into(), patch.into());
        }
        let persist = Arc::new(PersistLog::default());
        let codex = Arc::new(Codexes { runs: Mutex::default(), reply: setup.codex });
        let (tracer, written) = recording();
        let engine = create_engine(EngineDeps {
            providers: Providers {
                openai: provider.clone(),
                gemini: provider.clone(),
                deepseek: provider,
                openrouter: None,
            },
            mcp: mcp.clone(),
            presets: presets(),
            skills: vec!["orchestrator".into()],
            skill_store: Arc::new(skills),
            memory: persist.clone(),
            tracer: tracer.clone(),
            codex: codex.clone(),
            decisions: setup.decisions,
            workflows: std::sync::Arc::new(agent::flows::MemoryWorkflows::default()),
        });
        let runner = create_workflow_runner(engine.clone(), vec!["news-query".into()]).unwrap();
        Exec { runner, engine, mcp, llm, persist, codex, tracer, written }
    }

    fn tool_calls(&self) -> Vec<(String, Map<String, Value>)> {
        self.mcp.calls.lock().unwrap().clone()
    }

    fn llm_call(&self, i: usize) -> LlmCall {
        self.llm.calls.lock().unwrap()[i].clone()
    }

    // Runs the plan in a fresh trace and returns the result with the trace's
    // observations.
    async fn run(&self, plan: Value, ctx: &Ctx) -> (ExecResult, Vec<Observation>) {
        let workflow: Workflow = serde_json::from_value(plan).unwrap();
        let trace: Arc<dyn Trace> =
            self.tracer.trace(TraceStart { id: "t".into(), name: "t".into(), ..Default::default() });
        let scope: Scope = trace.clone();
        let exec = ExecContext {
            session: ctx.session.clone(),
            store: ctx.store.clone(),
            scope,
            signal_label: "test:1".into(),
            cancel: CancellationToken::new(),
        };
        let result = self.runner.execute(&workflow, &exec).await.unwrap();
        trace.end();
        let observations = self.written.all().pop().map(|t| t.observations).unwrap_or_default();
        (result, observations)
    }
}

struct Ctx {
    session: Session,
    store: Arc<VariableStore>,
}

fn base_ctx() -> Ctx {
    Ctx {
        session: task_context("test:1"),
        store: VariableStore::new(json!({"env":{"chatId":42}}).as_object().unwrap().clone()),
    }
}

fn ok(result: &ExecResult) -> bool {
    matches!(result, ExecResult::Ok { .. })
}

fn failure(result: &ExecResult) -> (&'static str, usize, &'static str, String) {
    match result {
        ExecResult::Failed { reason, step_index, step, error, .. } => {
            (reason, *step_index, step.kind(), error.to_string())
        }
        ExecResult::Ok { .. } => panic!("expected a failure"),
    }
}

fn end_output<'a>(observations: &'a [Observation], prefix: &str) -> &'a Value {
    &observations.iter().find(|o| o.name.starts_with(prefix)).unwrap_or_else(|| panic!("no span named {prefix}")).output
}

fn answer(text: &str) -> Result<Message, &'static str> {
    Ok(assistant(text))
}

fn plan(steps: Value) -> Value {
    json!({"version":1,"steps":steps})
}

// ── tool step ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn calls_mcp_with_substituted_args_and_binds_the_parsed_json_result() {
    let e = Exec::new(Setup {
        tools: vec![("list_news", Reply::Json(json!({"count":2,"items":[{"id":1},{"id":2}]})))],
        ..Setup::default()
    });
    let ctx = base_ctx();
    let (r, _) = e.run(plan(json!([{"kind":"tool","tool":"list_news","args":{"chatId":"${env.chatId}","limit":5},"bind":"posts"},{"kind":"terminal"}])), &ctx).await;
    assert!(ok(&r));
    assert_eq!(e.tool_calls(), [("list_news".to_owned(), json!({"chatId":42,"limit":5}).as_object().unwrap().clone())]);
    assert_eq!(ctx.store.get("posts").unwrap(), json!({"count":2,"items":[{"id":1},{"id":2}]}));
}

#[tokio::test]
async fn records_the_real_tool_output_on_the_step_span() {
    let e = Exec::new(Setup {
        tools: vec![("search_news", Reply::Json(json!({"hits":[{"id":7,"title":"x"}]})))],
        ..Setup::default()
    });
    let ctx = Ctx { store: VariableStore::new(json!({"env":{}}).as_object().unwrap().clone()), ..base_ctx() };
    let (r, obs) = e.run(plan(json!([{"kind":"tool","tool":"search_news","args":{"queries":["ai"]},"bind":"res"},{"kind":"terminal"}])), &ctx).await;
    assert!(ok(&r));
    // The step span carries the parsed tool result, not `{ ok: true }`.
    assert_eq!(end_output(&obs, "step[0]:tool"), &json!({"hits":[{"id":7,"title":"x"}]}));
}

#[tokio::test]
async fn works_without_bind() {
    let e = Exec::new(Setup {
        tools: vec![("send_telegram_message", Reply::Json(json!({"delivered":true})))],
        ..Setup::default()
    });
    let (r, _) = e.run(plan(json!([{"kind":"tool","tool":"send_telegram_message","args":{"chatId":1,"text":"hi"}},{"kind":"terminal"}])), &base_ctx()).await;
    assert!(ok(&r));
}

#[tokio::test]
async fn preserves_the_raw_string_when_mcp_returns_non_json() {
    let e = Exec::new(Setup { tools: vec![("ping", Reply::Text("pong"))], ..Setup::default() });
    let ctx = base_ctx();
    e.run(plan(json!([{"kind":"tool","tool":"ping","args":{},"bind":"out"},{"kind":"terminal"}])), &ctx).await;
    assert_eq!(ctx.store.get("out").unwrap(), "pong");
}

#[tokio::test]
async fn propagates_a_tool_error_as_failure_with_tool_error_reason() {
    let e = Exec::new(Setup {
        tools: vec![("broken_tool", Reply::Text("[tool error] something broke"))],
        ..Setup::default()
    });
    let (r, _) = e
        .run(plan(json!([{"kind":"tool","tool":"broken_tool","args":{},"bind":"x"},{"kind":"terminal"}])), &base_ctx())
        .await;
    let (reason, index, kind, _) = failure(&r);
    assert_eq!((reason, index, kind), ("tool_error", 0, "tool"));
}

#[tokio::test]
async fn a_missing_binding_in_args_surfaces_as_missing_binding() {
    let e = Exec::new(Setup::default());
    let (r, _) = e
        .run(
            plan(json!([{"kind":"tool","tool":"ping","args":{"ref":"${posts}"},"bind":"x"},{"kind":"terminal"}])),
            &base_ctx(),
        )
        .await;
    assert_eq!(failure(&r).0, "missing_binding");
    // The tool was never called: substitution failed first.
    assert!(e.tool_calls().is_empty());
}

// ── set_memory step (agent-side builtin) ────────────────────────────────────

#[tokio::test]
async fn dispatches_set_memory_to_the_engines_writer_not_mcp_and_binds_an_ack() {
    let e = Exec::new(Setup::default());
    let ctx = base_ctx();
    ctx.store.set("now", json!("2026-06-03T05:05:00Z")).unwrap();
    let (r, _) = e
        .run(plan(json!([{"kind":"tool","tool":"set_memory","args":{"key":"news_digest.last_read_at","value":"${now}"},"bind":"wm"},{"kind":"terminal"}])), &ctx)
        .await;
    assert!(ok(&r));
    assert_eq!(
        *e.persist.0.lock().unwrap(),
        [("news_digest.last_read_at".to_owned(), "2026-06-03T05:05:00Z".to_owned())]
    );
    // Never forwarded to MCP — set_memory has no MCP counterpart.
    assert!(e.tool_calls().is_empty());
    assert_eq!(ctx.store.get("wm").unwrap(), json!({"ok":true,"key":"news_digest.last_read_at"}));
}

#[tokio::test]
async fn rejects_a_non_string_value_as_tool_error_without_writing() {
    let e = Exec::new(Setup::default());
    let (r, _) = e.run(plan(json!([{"kind":"tool","tool":"set_memory","args":{"key":"k","value":123},"bind":"x"},{"kind":"terminal"}])), &base_ctx()).await;
    assert_eq!(failure(&r).0, "tool_error");
    assert!(e.persist.0.lock().unwrap().is_empty());
}

// ── code_agent step (Codex delegation) ──────────────────────────────────────

#[tokio::test]
async fn substitutes_task_and_data_delegates_to_codex_and_binds_the_trimmed_result() {
    let e = Exec::new(Setup { codex: Some(Ok("  42\n")), ..Setup::default() });
    let ctx = base_ctx();
    ctx.store.set("csv", json!("a,b\n1,2")).unwrap();
    let (r, _) = e
        .run(
            plan(json!([{"kind":"code_agent","task":"sum column b","data":"${csv}","bind":"n"},{"kind":"terminal"}])),
            &ctx,
        )
        .await;
    assert!(ok(&r));
    let runs = e.codex.runs.lock().unwrap().clone();
    assert_eq!(runs.len(), 1);
    assert!(runs[0]["prompt"].as_str().unwrap().contains("sum column b"));
    // Data substituted from the store.
    assert_eq!(runs[0]["input"], "a,b\n1,2");
    assert_eq!(ctx.store.get("n").unwrap(), "42");
    assert!(e.tool_calls().is_empty());
}

#[tokio::test]
async fn fails_as_step_failed_when_no_codex_backend_is_configured() {
    let e = Exec::new(Setup::default());
    let (r, _) =
        e.run(plan(json!([{"kind":"code_agent","task":"2+2","bind":"x"},{"kind":"terminal"}])), &base_ctx()).await;
    assert_eq!(failure(&r).0, "step_failed");
}

#[tokio::test]
async fn surfaces_a_codex_failure_as_step_failed() {
    let e = Exec::new(Setup { codex: Some(Err("codex service returned 502")), ..Setup::default() });
    let (r, _) =
        e.run(plan(json!([{"kind":"code_agent","task":"2+2","bind":"x"},{"kind":"terminal"}])), &base_ctx()).await;
    assert_eq!(failure(&r).0, "step_failed");
}

// ── decide step ─────────────────────────────────────────────────────────────

fn posts() -> Value {
    json!([
        {"id":1,"body":"ФОП: новая ставка ЕСВ"},
        {"id":2,"body":"Доброе утро!"},
        {"id":3,"body":"Бронирование: новые правила"},
    ])
}

fn decide_plan(extra: Value) -> Value {
    let mut step = json!({"kind":"decide","preset":"filter","items":"${posts.items}","prompt":"Правила для ${env.chatId}","bind":"kept"});
    for (k, v) in extra.as_object().unwrap() {
        step[k] = v.clone();
    }
    plan(json!([step, {"kind":"terminal"}]))
}

fn ctx_with_posts() -> Ctx {
    let ctx = base_ctx();
    ctx.store.set("posts", json!({"count":3,"items":posts()})).unwrap();
    ctx
}

#[tokio::test]
async fn filters_the_bound_array_with_the_substituted_prompt_and_binds_the_kept_items() {
    let (client, api) = fake_decisions(|item| if item.to_string().contains("Доброе") { 0.05 } else { 0.8 }, None);
    let e = Exec::new(Setup { decisions: Some(client), ..Setup::default() });
    let ctx = ctx_with_posts();
    let (r, _) = e.run(decide_plan(json!({})), &ctx).await;
    assert!(ok(&r));
    assert_eq!(ctx.store.get("kept").unwrap(), json!([posts()[0], posts()[2]]));
    assert_eq!(*api.rules.lock().unwrap(), ["Правила для 42"]);
}

#[tokio::test]
async fn passes_the_steps_threshold_to_the_preset() {
    let (client, _) = fake_decisions(|_| 0.5, None);
    let e = Exec::new(Setup { decisions: Some(client), ..Setup::default() });
    let ctx = ctx_with_posts();
    e.run(decide_plan(json!({"threshold":0.6})), &ctx).await;
    assert_eq!(ctx.store.get("kept").unwrap(), json!([]));
}

#[tokio::test]
async fn fails_as_decision_error_when_the_decision_model_fails_nothing_passes_through() {
    let (client, _) = fake_decisions(|_| 1.0, Some("openrouter 503"));
    let e = Exec::new(Setup { decisions: Some(client), ..Setup::default() });
    let ctx = ctx_with_posts();
    let (r, _) = e.run(decide_plan(json!({})), &ctx).await;
    let (reason, _, _, error) = failure(&r);
    assert_eq!(reason, "decision_error");
    assert!(error.contains("openrouter 503"), "{error}");
    assert!(!ctx.store.has("kept"));
}

#[tokio::test]
async fn fails_as_decision_error_when_no_decision_model_is_configured() {
    let e = Exec::new(Setup::default());
    let (r, _) = e.run(decide_plan(json!({})), &ctx_with_posts()).await;
    assert_eq!(failure(&r).0, "decision_error");
}

#[tokio::test]
async fn fails_as_decision_error_when_items_do_not_resolve_to_an_array() {
    let (client, _) = fake_decisions(|_| 1.0, None);
    let e = Exec::new(Setup { decisions: Some(client), ..Setup::default() });
    let (r, _) = e.run(decide_plan(json!({"items":"${posts}"})), &ctx_with_posts()).await;
    let (reason, _, _, error) = failure(&r);
    assert_eq!(reason, "decision_error");
    assert!(error.contains("must resolve to an array, got object"), "{error}");
}

// ── llm_compose step ────────────────────────────────────────────────────────

#[tokio::test]
async fn loads_the_skill_as_system_builds_user_from_prompt_and_xml_input_calls_the_llm_without_tools() {
    let e = Exec::new(Setup {
        llm: vec![answer("composed digest")],
        skills: vec![("news-digest", "RULES go here")],
        ..Setup::default()
    });
    let ctx = base_ctx();
    ctx.store.set("posts", json!([{"id":1}])).unwrap();
    let (r, _) = e
        .run(plan(json!([{"kind":"llm_compose","preset":"smartest","skill":"news-digest","input":{"posts":"${posts}"},"bind":"digest"},{"kind":"terminal"}])), &ctx)
        .await;
    assert!(ok(&r));
    assert_eq!(ctx.store.get("digest").unwrap(), "composed digest");
    assert_eq!(e.llm.calls.lock().unwrap().len(), 1);
    let body = e.llm_call(0);
    assert_eq!(body.model, "gpt-5.4");
    // No tools: the model can't emit tool_calls.
    assert!(body.tools.is_empty());
    assert_eq!(Value::Object(body.messages[0].clone()), json!({"role":"system","content":"RULES go here"}));
    let user = body.messages[1]["content"].as_str().unwrap();
    assert!(user.contains("<posts>"));
    assert!(user.contains("\"id\": 1"));
}

#[tokio::test]
async fn appends_the_improver_patch_to_the_compose_system_message() {
    let e = Exec::new(Setup {
        llm: vec![answer("composed")],
        skills: vec![("news-digest", "RULES go here")],
        patches: vec![("news-digest", "PATCH LESSON")],
        ..Setup::default()
    });
    let ctx = base_ctx();
    ctx.store.set("posts", json!([{"id":1}])).unwrap();
    let (r, _) = e
        .run(plan(json!([{"kind":"llm_compose","preset":"smartest","skill":"news-digest","input":{},"bind":"d"},{"kind":"terminal"}])), &ctx)
        .await;
    assert!(ok(&r));
    let system = e.llm_call(0).messages[0]["content"].as_str().unwrap().to_owned();
    // Body first, the patch glued onto the END with the marker.
    assert!(system.starts_with("RULES go here"));
    assert!(system.contains(PATCH_MARKER));
    assert!(system.contains("PATCH LESSON"));
}

#[tokio::test]
async fn prepends_the_base_composer_skill_ahead_of_the_step_skill() {
    let e = Exec::new(Setup {
        llm: vec![answer("composed")],
        skills: vec![("composer", "BASE COMPOSE RULES"), ("news-digest", "RULES go here")],
        ..Setup::default()
    });
    let (r, _) = e
        .run(plan(json!([{"kind":"llm_compose","preset":"smartest","skill":"news-digest","input":{},"bind":"d"},{"kind":"terminal"}])), &base_ctx())
        .await;
    assert!(ok(&r));
    let system = e.llm_call(0).messages[0]["content"].as_str().unwrap().to_owned();
    // Base rules form the stable prefix; the domain skill is layered on top.
    assert!(system.starts_with("BASE COMPOSE RULES"));
    assert!(system.contains("RULES go here"));
    assert!(system.find("BASE COMPOSE RULES").unwrap() < system.find("RULES go here").unwrap());
}

#[tokio::test]
async fn applies_the_base_composer_skill_even_when_the_step_has_no_skill() {
    let e = Exec::new(Setup {
        llm: vec![answer("composed")],
        skills: vec![("composer", "BASE COMPOSE RULES")],
        ..Setup::default()
    });
    let (r, _) = e
        .run(
            plan(
                json!([{"kind":"llm_compose","preset":"base","input":{},"prompt":"hi","bind":"d"},{"kind":"terminal"}]),
            ),
            &base_ctx(),
        )
        .await;
    assert!(ok(&r));
    // A compiled compose step carries no skill — it must still get the base rules.
    assert_eq!(
        Value::Object(e.llm_call(0).messages[0].clone()),
        json!({"role":"system","content":"BASE COMPOSE RULES"})
    );
}

#[tokio::test]
async fn stringifies_a_whole_placeholder_prompt_bound_to_a_non_string_value() {
    // A prompt must stay text: a raw array in message.content is an API error.
    let e = Exec::new(Setup { llm: vec![answer("ok")], ..Setup::default() });
    let ctx = base_ctx();
    ctx.store.set("posts", json!([{"id":1}])).unwrap();
    let (r, _) = e.run(plan(json!([{"kind":"llm_compose","preset":"base","prompt":"${posts}","input":{},"bind":"out"},{"kind":"terminal"}])), &ctx).await;
    assert!(ok(&r));
    let content = &e.llm_call(0).messages[0]["content"];
    assert!(content.is_string());
    assert!(content.as_str().unwrap().contains("\"id\": 1"));
}

#[tokio::test]
async fn parses_json_output_and_binds_the_object_so_later_steps_can_dot_into_it() {
    let e = Exec::new(Setup {
        llm: vec![answer(&json!({"cancelId":10,"cron_expr":"0 9 L * *"}).to_string())],
        tools: vec![("cancel_scheduled_task", Reply::Json(json!({"cancelled":true})))],
        ..Setup::default()
    });
    let ctx = base_ctx();
    let (r, _) = e
        .run(plan(json!([
            {"kind":"llm_compose","preset":"base","prompt":"Emit the reschedule plan as JSON","input":{},"bind":"target"},
            {"kind":"tool","tool":"cancel_scheduled_task","args":{"id":"${target.cancelId}"}},
            {"kind":"terminal"},
        ])), &ctx)
        .await;
    assert!(ok(&r));
    // Bound as a parsed object, not a raw JSON string.
    assert_eq!(ctx.store.get("target").unwrap(), json!({"cancelId":10,"cron_expr":"0 9 L * *"}));
    // ${target.cancelId} resolved to the literal 10.
    let call = e.tool_calls().into_iter().find(|(n, _)| n == "cancel_scheduled_task").unwrap();
    assert_eq!(Value::Object(call.1), json!({"id":10}));
}

#[tokio::test]
async fn resolves_deep_dot_access_into_a_nested_parsed_object() {
    let e = Exec::new(Setup {
        llm: vec![answer(&json!({"schedule":{"cron":"0 9 L * *","recurring":true}}).to_string())],
        tools: vec![("schedule_task", Reply::Json(json!({"id":11})))],
        ..Setup::default()
    });
    let (r, _) = e
        .run(plan(json!([
            {"kind":"llm_compose","preset":"base","prompt":"plan","input":{},"bind":"plan"},
            {"kind":"tool","tool":"schedule_task","args":{"cron_expr":"${plan.schedule.cron}","recurring":"${plan.schedule.recurring}"}},
            {"kind":"terminal"},
        ])), &base_ctx())
        .await;
    assert!(ok(&r));
    let call = e.tool_calls().into_iter().find(|(n, _)| n == "schedule_task").unwrap();
    // Whole-string placeholders preserve type.
    assert_eq!(Value::Object(call.1), json!({"cron_expr":"0 9 L * *","recurring":true}));
}

#[tokio::test]
async fn parses_a_json_array_output_and_binds_it_as_an_array() {
    let e = Exec::new(Setup {
        llm: vec![answer(&json!([{"id":1},{"id":2}]).to_string())],
        tools: vec![("send_batch", Reply::Json(json!({"ok":true})))],
        ..Setup::default()
    });
    let ctx = base_ctx();
    let (r, _) = e
        .run(
            plan(json!([
                {"kind":"llm_compose","preset":"base","prompt":"list","input":{},"bind":"rows"},
                {"kind":"tool","tool":"send_batch","args":{"rows":"${rows}"}},
                {"kind":"terminal"},
            ])),
            &ctx,
        )
        .await;
    assert!(ok(&r));
    assert_eq!(ctx.store.get("rows").unwrap(), json!([{"id":1},{"id":2}]));
    let call = e.tool_calls().into_iter().find(|(n, _)| n == "send_batch").unwrap();
    assert_eq!(call.1["rows"], json!([{"id":1},{"id":2}]));
}

#[tokio::test]
async fn interpolates_a_parsed_objects_field_into_a_mixed_string() {
    let e = Exec::new(Setup {
        llm: vec![answer(&json!({"cancelId":10}).to_string())],
        tools: vec![("send_telegram_message", Reply::Json(json!({"delivered":true})))],
        ..Setup::default()
    });
    let (r, _) = e
        .run(plan(json!([
            {"kind":"llm_compose","preset":"base","prompt":"plan","input":{},"bind":"target"},
            {"kind":"tool","tool":"send_telegram_message","args":{"chatId":1,"text":"Cancelled task ${target.cancelId}"}},
            {"kind":"terminal"},
        ])), &base_ctx())
        .await;
    assert!(ok(&r));
    let call = e.tool_calls().into_iter().find(|(n, _)| n == "send_telegram_message").unwrap();
    assert_eq!(call.1["text"], "Cancelled task 10");
}

#[tokio::test]
async fn keeps_plain_prose_as_a_string() {
    let e = Exec::new(Setup {
        llm: vec![answer("Привет! Вот твой дайджест за сегодня.")], ..Setup::default()
    });
    let ctx = base_ctx();
    let (r, _) = e.run(plan(json!([{"kind":"llm_compose","preset":"base","prompt":"greet","input":{},"bind":"reply"},{"kind":"terminal"}])), &ctx).await;
    assert!(ok(&r));
    assert_eq!(ctx.store.get("reply").unwrap(), "Привет! Вот твой дайджест за сегодня.");
}

#[tokio::test]
async fn keeps_a_bare_primitive_output_as_a_string() {
    let e = Exec::new(Setup { llm: vec![answer("42")], ..Setup::default() });
    let ctx = base_ctx();
    let (r, _) = e.run(plan(json!([{"kind":"llm_compose","preset":"base","prompt":"count","input":{},"bind":"n"},{"kind":"terminal"}])), &ctx).await;
    assert!(ok(&r));
    // "42" doesn't start with { or [, so it stays the raw string.
    assert_eq!(ctx.store.get("n").unwrap(), json!("42"));
}

#[tokio::test]
async fn keeps_json_looking_but_invalid_output_as_the_raw_string() {
    let raw = "{ \"cancelId\": 10  // oops, not JSON";
    let e = Exec::new(Setup { llm: vec![answer(raw)], ..Setup::default() });
    let ctx = base_ctx();
    let (r, _) = e.run(plan(json!([{"kind":"llm_compose","preset":"base","prompt":"plan","input":{},"bind":"target"},{"kind":"terminal"}])), &ctx).await;
    assert!(ok(&r));
    assert_eq!(ctx.store.get("target").unwrap(), raw);
}

#[tokio::test]
async fn fails_with_missing_binding_when_dotting_into_a_field_the_parsed_object_lacks() {
    let e = Exec::new(Setup {
        llm: vec![answer(&json!({"cancelId":10}).to_string())],
        tools: vec![("cancel_scheduled_task", Reply::Json(json!({"cancelled":true})))],
        ..Setup::default()
    });
    let (r, _) = e
        .run(
            plan(json!([
                {"kind":"llm_compose","preset":"base","prompt":"plan","input":{},"bind":"target"},
                {"kind":"tool","tool":"cancel_scheduled_task","args":{"id":"${target.missingField}"}},
                {"kind":"terminal"},
            ])),
            &base_ctx(),
        )
        .await;
    let (reason, index, _, _) = failure(&r);
    assert_eq!((reason, index), ("missing_binding", 1));
    // The tool never ran — substitution failed before dispatch.
    assert!(!e.tool_calls().iter().any(|(n, _)| n == "cancel_scheduled_task"));
}

#[tokio::test]
async fn fails_with_missing_binding_when_dotting_into_a_string_valued_compose_result() {
    // Regression guard: a prose compose bound as a string, then a step that
    // dots into it, must fail — not silently expand to "undefined".
    let e = Exec::new(Setup {
        llm: vec![answer("just some prose, no fields here")],
        tools: vec![("cancel_scheduled_task", Reply::Json(json!({"cancelled":true})))],
        ..Setup::default()
    });
    let (r, _) = e
        .run(
            plan(json!([
                {"kind":"llm_compose","preset":"base","prompt":"plan","input":{},"bind":"target"},
                {"kind":"tool","tool":"cancel_scheduled_task","args":{"id":"${target.cancelId}"}},
                {"kind":"terminal"},
            ])),
            &base_ctx(),
        )
        .await;
    assert_eq!(failure(&r).0, "missing_binding");
}

#[tokio::test]
async fn works_with_prompt_only_no_skill() {
    let e = Exec::new(Setup { llm: vec![answer("A: 5")], ..Setup::default() });
    let ctx = base_ctx();
    ctx.store.set("question", json!("what is 2+3")).unwrap();
    let (r, _) = e.run(plan(json!([{"kind":"llm_compose","preset":"base","prompt":"Answer: ${question}","input":{},"bind":"answer"},{"kind":"terminal"}])), &ctx).await;
    assert!(ok(&r));
    assert_eq!(ctx.store.get("answer").unwrap(), "A: 5");
    let body = e.llm_call(0);
    // No system, just user.
    assert_eq!(body.messages.len(), 1);
    assert_eq!(body.messages[0]["content"], "Answer: what is 2+3");
}

#[tokio::test]
async fn appends_xml_blocks_after_the_prompt_when_both_are_present() {
    let e = Exec::new(Setup { llm: vec![answer("ok")], ..Setup::default() });
    let ctx = base_ctx();
    ctx.store.set("items", json!(["a", "b"])).unwrap();
    e.run(plan(json!([{"kind":"llm_compose","preset":"base","prompt":"Inspect this:","input":{"items":"${items}"},"bind":"out"},{"kind":"terminal"}])), &ctx).await;
    let user = e.llm_call(0).messages[0]["content"].as_str().unwrap().to_owned();
    assert!(user.starts_with("Inspect this:"));
    assert!(user.contains("<items>"));
}

#[tokio::test]
async fn skill_not_found_when_the_skill_is_named_but_missing() {
    let e = Exec::new(Setup::default());
    let (r, _) = e.run(plan(json!([{"kind":"llm_compose","preset":"base","skill":"ghost","input":{},"bind":"out"},{"kind":"terminal"}])), &base_ctx()).await;
    assert_eq!(failure(&r).0, "skill_not_found");
}

// ── llm_agent step ──────────────────────────────────────────────────────────

fn agent_step(prompt: &str, tools: Value, max_iterations: u32, bind: &str) -> Value {
    json!({"kind":"llm_agent","preset":"smart","skill":"news-query","prompt":prompt,"tools":tools,"maxIterations":max_iterations,"bind":bind})
}

#[tokio::test]
async fn spawns_a_child_session_with_the_tool_whitelist_and_binds_its_result() {
    let e = Exec::new(Setup {
        llm: vec![Ok(call("search_news", json!({"query":"Одесса"}))), answer("agent answer")],
        skills: vec![("news-query", "NEWS QUERY RULES")],
        tools: vec![("search_news", Reply::Text("found"))],
        ..Setup::default()
    });
    let ctx = base_ctx();
    ctx.store.set("query", json!("что в Одессе")).unwrap();
    let (r, obs) = e
        .run(
            plan(json!([agent_step("${query}", json!(["search_news","list_news"]), 5, "answer"), {"kind":"terminal"}])),
            &ctx,
        )
        .await;
    assert!(ok(&r));
    assert_eq!(ctx.store.get("answer").unwrap(), "agent answer");

    let first = e.llm_call(0);
    let offered: HashSet<&str> = first
        .tools
        .iter()
        .map(|t| t.name.as_str())
        .filter(|n| ["search_news", "list_news", "send_telegram_message"].contains(n))
        .collect();
    assert_eq!(offered, HashSet::from(["search_news", "list_news"]));
    assert_eq!(first.model, "deepseek-v4-pro");
    let system = first.messages[0]["content"].as_str().unwrap();
    assert!(system.contains("NEWS QUERY RULES"));
    // includeEngineSkills: false.
    assert!(!system.contains("ORCHESTRATOR"));
    assert_eq!(first.messages.iter().find(|m| role(m) == "user").unwrap()["content"], "что в Одессе");
    // The child shares the executor's session: its tool result is in it.
    assert_eq!(ctx.session.memory.list().len(), 1);
    let step = obs.iter().find(|o| o.name == "step[0]:llm_agent").unwrap();
    assert_eq!(step.meta("skills"), Some(&json!(["news-query"])));
    assert_eq!(step.meta("preset"), Some(&json!("smart")));
    assert_eq!(step.meta("max_iterations"), Some(&json!(5)));
    assert_eq!(step.meta("parent_id"), Some(&json!("test:1")));
}

#[tokio::test]
async fn ends_the_spawned_session_on_success() {
    let e = Exec::new(Setup { llm: vec![answer("x")], skills: vec![("news-query", "RULES")], ..Setup::default() });
    let (r, obs) =
        e.run(plan(json!([agent_step("q", json!(["search_news"]), 3, "a"), {"kind":"terminal"}])), &base_ctx()).await;
    assert!(ok(&r));
    assert!(e.engine.loop_ids().await.is_empty());
    let id = obs
        .iter()
        .find(|o| o.name == "step[0]:llm_agent")
        .unwrap()
        .meta("agent_id")
        .unwrap()
        .as_str()
        .unwrap()
        .to_owned();
    assert!(id.ends_with("__agent:a"), "{id}");
}

#[tokio::test]
async fn ends_the_spawned_session_even_when_the_child_fails() {
    let e =
        Exec::new(Setup { llm: vec![Err("child crashed")], skills: vec![("news-query", "RULES")], ..Setup::default() });
    let (r, _) =
        e.run(plan(json!([agent_step("q", json!(["search_news"]), 3, "a"), {"kind":"terminal"}])), &base_ctx()).await;
    assert!(!ok(&r));
    assert!(e.engine.loop_ids().await.is_empty());
}

// ── parallel step ───────────────────────────────────────────────────────────

#[tokio::test]
async fn interrupts_a_sibling_after_failure_and_prevents_late_writes_to_the_store() {
    let e = Exec::new(Setup::default());
    let slow_signal: Arc<Mutex<Option<CancellationToken>>> = Arc::default();
    let seen = slow_signal.clone();
    let slow_started = Arc::new(tokio::sync::Notify::new());
    e.mcp.always(Arc::new(move |name, _, cancel| {
        let slow_started = slow_started.clone();
        if name == "bad" {
            // Fails once its sibling is running.
            return Box::pin(async move {
                slow_started.notified().await;
                Err(anyhow::anyhow!("failed branch"))
            });
        }
        *seen.lock().unwrap() = Some(cancel.clone());
        slow_started.notify_one();
        // Finishes "successfully", but only after it was interrupted.
        Box::pin(async move {
            cancel.cancelled().await;
            Ok("late success".into())
        })
    }));
    let ctx = base_ctx();
    let (r, _) = e
        .run(plan(json!([{"kind":"parallel","steps":[{"kind":"tool","tool":"bad","args":{}},{"kind":"tool","tool":"slow","args":{},"bind":"late"}]}])), &ctx)
        .await;
    assert!(!ok(&r));
    assert!(slow_signal.lock().unwrap().as_ref().unwrap().is_cancelled());
    assert!(!ctx.store.has("late"));
}

#[tokio::test]
async fn runs_children_concurrently_and_binds_each() {
    let e = Exec::new(Setup::default());
    let started = Arc::new(AtomicUsize::new(0));
    let both = Arc::new(tokio::sync::Notify::new());
    {
        let (started, both) = (started.clone(), both.clone());
        e.mcp.always(Arc::new(move |name, _, _| {
            let (started, both) = (started.clone(), both.clone());
            Box::pin(async move {
                if started.fetch_add(1, Ordering::SeqCst) + 1 == 2 {
                    both.notify_waiters();
                } else {
                    // Waits for its sibling: only finishes if both run at once.
                    tokio::time::timeout(std::time::Duration::from_secs(5), both.notified()).await?;
                }
                Ok(name.to_uppercase())
            })
        }));
    }
    let ctx = base_ctx();
    let (r, _) = e
        .run(plan(json!([{"kind":"parallel","steps":[{"kind":"tool","tool":"a","args":{},"bind":"x"},{"kind":"tool","tool":"b","args":{},"bind":"y"}]},{"kind":"terminal"}])), &ctx)
        .await;
    assert!(ok(&r));
    assert_eq!(ctx.store.get("x").unwrap(), "A");
    assert_eq!(ctx.store.get("y").unwrap(), "B");
}

#[tokio::test]
async fn fails_fast_when_any_child_step_fails() {
    let e = Exec::new(Setup {
        tools: vec![("good", Reply::Text("ok")), ("bad", Reply::Text("[tool error] nope"))],
        ..Setup::default()
    });
    let (r, _) = e
        .run(plan(json!([{"kind":"parallel","steps":[{"kind":"tool","tool":"good","args":{},"bind":"g"},{"kind":"tool","tool":"bad","args":{},"bind":"b"}]},{"kind":"terminal"}])), &base_ctx())
        .await;
    let (reason, _, kind, _) = failure(&r);
    assert_eq!((reason, kind), ("tool_error", "parallel"));
}

// ── terminal and end-of-list ────────────────────────────────────────────────

#[tokio::test]
async fn stops_at_an_explicit_terminal_mid_plan() {
    let e = Exec::new(Setup { tools: vec![("a", Reply::Text("A")), ("b", Reply::Text("B"))], ..Setup::default() });
    let ctx = base_ctx();
    let (r, _) = e
        .run(plan(json!([{"kind":"tool","tool":"a","args":{},"bind":"x"},{"kind":"terminal"},{"kind":"tool","tool":"b","args":{},"bind":"y"}])), &ctx)
        .await;
    assert!(ok(&r));
    assert!(ctx.store.has("x"));
    assert!(!ctx.store.has("y"));
    assert_eq!(e.tool_calls().len(), 1);
}

#[tokio::test]
async fn succeeds_when_the_plan_ends_without_an_explicit_terminal() {
    let e = Exec::new(Setup { tools: vec![("a", Reply::Text("A"))], ..Setup::default() });
    let ctx = base_ctx();
    let (r, _) = e.run(plan(json!([{"kind":"tool","tool":"a","args":{},"bind":"x"}])), &ctx).await;
    assert!(ok(&r));
    assert_eq!(ctx.store.get("x").unwrap(), "A");
}

// ── replan step ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn stops_the_pass_and_returns_the_named_bindings_as_replan_context() {
    let e = Exec::new(Setup {
        tools: vec![("get_history", Reply::Json(json!([{"id":1},{"id":2}]))), ("later", Reply::Text("L"))],
        ..Setup::default()
    });
    let ctx = base_ctx();
    let (r, _) = e
        .run(
            plan(json!([
                {"kind":"tool","tool":"get_history","args":{},"bind":"history"},
                {"kind":"replan","context":["history"],"note":"decide"},
                {"kind":"tool","tool":"later","args":{},"bind":"after"},
            ])),
            &ctx,
        )
        .await;
    let ExecResult::Ok { replan: Some(replan), .. } = r else { panic!("expected a replan") };
    assert_eq!(replan.note.as_deref(), Some("decide"));
    assert_eq!(Value::Object(replan.context), json!({"history":[{"id":1},{"id":2}]}));
    // Steps after replan don't run — the pass terminates there.
    assert!(!ctx.store.has("after"));
    assert_eq!(e.tool_calls().into_iter().map(|(n, _)| n).collect::<Vec<_>>(), ["get_history"]);
}

#[tokio::test]
async fn drops_context_names_that_were_never_bound_keeping_the_rest() {
    let e = Exec::new(Setup { tools: vec![("a", Reply::Text("A"))], ..Setup::default() });
    let (r, _) = e
        .run(
            plan(json!([{"kind":"tool","tool":"a","args":{},"bind":"x"},{"kind":"replan","context":["x","missing"]}])),
            &base_ctx(),
        )
        .await;
    let ExecResult::Ok { replan: Some(replan), .. } = r else { panic!("expected a replan") };
    assert_eq!(Value::Object(replan.context), json!({"x":"A"}));
}

// ── duplicate binding ───────────────────────────────────────────────────────

#[tokio::test]
async fn surfaces_duplicate_binding_reason() {
    let e = Exec::new(Setup { tools: vec![("a", Reply::Text("A")), ("b", Reply::Text("B"))], ..Setup::default() });
    let (r, _) = e
        .run(plan(json!([{"kind":"tool","tool":"a","args":{},"bind":"x"},{"kind":"tool","tool":"b","args":{},"bind":"x"},{"kind":"terminal"}])), &base_ctx())
        .await;
    let (reason, index, _, _) = failure(&r);
    assert_eq!((reason, index), ("duplicate_binding", 1));
}

// ── end-to-end fixture ──────────────────────────────────────────────────────

#[tokio::test]
async fn runs_a_news_digest_shaped_plan_to_completion() {
    let e = Exec::new(Setup {
        tools: vec![
            ("list_news", Reply::Json(json!({"count":1,"items":[{"id":1,"body":"post"}]}))),
            ("get_telegram_chat_history", Reply::Json(json!({"messages":[]}))),
            ("send_telegram_message", Reply::Json(json!({"delivered":true}))),
            // No set_memory on purpose: it is not an MCP tool.
        ],
        llm: vec![answer("📰 Новости · 3 июня\n• fake digest")],
        skills: vec![("news-digest", "DIGEST RULES")],
        ..Setup::default()
    });
    let ctx = base_ctx();
    ctx.store.set("watermark", json!("2026-06-02T18:00:00Z")).unwrap();
    ctx.store.set("now", json!("2026-06-03T05:05:00Z")).unwrap();
    let p = plan(json!([
        {"kind":"parallel","steps":[
            {"kind":"tool","tool":"list_news","args":{"source":"channel","sinceISO":"${watermark}"},"bind":"posts"},
            {"kind":"tool","tool":"get_telegram_chat_history","args":{"chatId":"${env.chatId}","limit":5},"bind":"history"},
        ]},
        {"kind":"llm_compose","preset":"smart","skill":"news-digest","input":{"posts":"${posts}","history":"${history}"},"bind":"digest"},
        {"kind":"parallel","steps":[
            {"kind":"tool","tool":"send_telegram_message","args":{"chatId":"${env.chatId}","text":"${digest}"}},
            {"kind":"tool","tool":"set_memory","args":{"key":"news_digest.last_read_at","value":"${now}"}},
        ]},
        {"kind":"terminal"},
    ]));
    // Validated against the schema first, like the real compiler would.
    let schema = WorkflowSchema::new(
        ["list_news", "get_telegram_chat_history", "send_telegram_message", "set_memory"].map(String::from).to_vec(),
        vec!["news-digest".into()],
    )
    .unwrap();
    assert!(schema.parse(p.clone(), None).is_ok());

    let (r, _) = e.run(p, &ctx).await;
    assert!(ok(&r));
    assert!(ctx.store.get("digest").unwrap().as_str().unwrap().contains("📰 Новости"));
    // Telegram fired via MCP; the watermark went to the engine's writer.
    let names: Vec<String> = e.tool_calls().into_iter().map(|(n, _)| n).collect();
    assert!(names.contains(&"send_telegram_message".to_owned()));
    assert!(!names.contains(&"set_memory".to_owned()));
    assert_eq!(
        *e.persist.0.lock().unwrap(),
        [("news_digest.last_read_at".to_owned(), "2026-06-03T05:05:00Z".to_owned())]
    );
    let send = e.tool_calls().into_iter().find(|(n, _)| n == "send_telegram_message").unwrap();
    assert_eq!(Value::Object(send.1), json!({"chatId":42,"text":"📰 Новости · 3 июня\n• fake digest"}));
}

// ── renderInputAsXml ────────────────────────────────────────────────────────

#[test]
fn returns_an_empty_string_for_empty_input() {
    assert_eq!(render_input_xml(&Map::new()), "");
}

#[test]
fn emits_a_tag_per_entry() {
    assert_eq!(render_input_xml(json!({"a":"x","b":"y"}).as_object().unwrap()), "<a>\nx\n</a>\n\n<b>\ny\n</b>");
}

#[test]
fn json_stringifies_non_string_values() {
    assert!(render_input_xml(json!({"x":{"v":1}}).as_object().unwrap()).contains("\"v\": 1"));
}

// ── classifyError ───────────────────────────────────────────────────────────

#[test]
fn recognises_a_tool_call_error() {
    assert_eq!(classify_error(&WorkflowError::Tool { tool: "x".into(), message: "msg".into() }.into()), "tool_error");
}

#[test]
fn recognises_a_skill_not_found_error() {
    assert_eq!(classify_error(&WorkflowError::Skill("ghost".into()).into()), "skill_not_found");
}

#[test]
fn recognises_an_llm_call_error() {
    assert_eq!(classify_error(&WorkflowError::Llm("oops".into()).into()), "llm_error");
}

#[test]
fn falls_back_to_step_failed_for_unknown_errors() {
    assert_eq!(classify_error(&anyhow::anyhow!("?")), "step_failed");
}

// Rust-only: the binding errors classify too.
#[test]
fn recognises_binding_errors() {
    assert_eq!(classify_error(&WorkflowError::Missing("a".into()).into()), "missing_binding");
    assert_eq!(classify_error(&WorkflowError::Duplicate("a".into()).into()), "duplicate_binding");
}
