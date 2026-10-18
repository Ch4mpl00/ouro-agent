// Port of src/workflow/compile.test.ts, one test per case. The compiler is
// the workflow runner's compile step: its tools come from the engine's MCP,
// the planner prompt from the skill store.
mod support;
use agent::{
    agent::{EngineDeps, Providers, create_engine},
    models::{ModelPreset, Presets, ReasoningEffort},
    providers::{ChatProvider, CompletionParams, CompletionResult, Message, ProviderKind, ToolDef, assistant, role},
    session::EnvData,
    skills::{SkillEntry, SkillFile, SkillSource, SkillStore, SkillTools, Written},
    tracing::{
        ContextUpdate, EventStart, Generation, GenerationEnd, GenerationStart, Metadata, Scope, Span, SpanStart,
        TokenUsage, TraceContext, null_scope,
    },
    workflow::{CompileResult, WorkflowRunner, WorkflowSignal, create_workflow_runner},
};
use async_trait::async_trait;
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use support::{NoCodex, PersistLog, ToolMcp};
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

// Replies in order; an Err reply fails the call. Records each request.
struct Mock {
    replies: Mutex<VecDeque<Result<String, &'static str>>>,
    calls: Mutex<Vec<(String, Vec<Message>)>>,
}

#[async_trait]
impl ChatProvider for Mock {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Openai
    }
    async fn complete(&self, p: &mut CompletionParams<'_>) -> anyhow::Result<CompletionResult> {
        self.calls.lock().unwrap().push((p.model.clone(), p.messages.clone()));
        let next = self.replies.lock().unwrap().pop_front().unwrap_or(Ok(String::new()));
        let text = next.map_err(|e| anyhow::anyhow!(e))?;
        Ok(CompletionResult {
            message: assistant(&text),
            finish_reason: Some("stop".into()),
            usage: Some(TokenUsage { input: 100, output: 50, total: 150, cached: None }),
        })
    }
}

impl Mock {
    fn call(&self, i: usize) -> Vec<Message> {
        self.calls.lock().unwrap()[i].1.clone()
    }
    fn count(&self) -> usize {
        self.calls.lock().unwrap().len()
    }
}

// The planner skill, or none.
struct Planner(Option<&'static str>);

#[async_trait]
impl SkillStore for Planner {
    async fn read_skill(&self, name: &str) -> anyhow::Result<Option<SkillFile>> {
        Ok(self.0.filter(|_| name == "planner").map(|body| SkillFile {
            body: body.into(),
            tools: SkillTools::Named(Vec::new()),
            source: SkillSource::Default,
        }))
    }
    async fn read_patch(&self, _: &str) -> anyhow::Result<Option<String>> {
        Ok(None)
    }
    async fn save_skill(&self, _: &str, _: &str) -> anyhow::Result<Written> {
        anyhow::bail!("read-only")
    }
    async fn list_skills(&self) -> anyhow::Result<Vec<SkillEntry>> {
        Ok(Vec::new())
    }
}

fn fake_tools() -> Vec<ToolDef> {
    vec![
        ToolDef::new(
            "search_news",
            "Vector search over the news store",
            json!({"type":"object","properties":{"query":{"type":"string"},"k":{"type":"number"},"sinceISO":{"type":"string"}},"required":["query"]}),
        ),
        ToolDef::new(
            "send_telegram_message",
            "Send a message to a chat",
            json!({"type":"object","properties":{"chatId":{"type":"number"},"text":{"type":"string"}},"required":["chatId","text"]}),
        ),
    ]
}

fn valid_plan() -> String {
    json!({"version":1,"steps":[
        {"kind":"llm_agent","preset":"smart","skill":"telegram","prompt":"что нового","tools":["search_news"],"maxIterations":5,"bind":"answer"},
        {"kind":"terminal"},
    ]})
    .to_string()
}

fn invalid_plan() -> String {
    json!({"version":1,"steps":[{"kind":"tool","tool":"ghost_tool","args":{},"bind":"x"},{"kind":"terminal"}]})
        .to_string()
}

fn compiler(
    replies: Vec<Result<String, &'static str>>,
    planner: Option<&'static str>,
    max_attempts: Option<usize>,
) -> (Arc<WorkflowRunner>, Arc<Mock>) {
    let mock = Arc::new(Mock { replies: Mutex::new(replies.into()), calls: Mutex::default() });
    let provider: Arc<dyn ChatProvider> = mock.clone();
    let engine = create_engine(EngineDeps {
        providers: Providers {
            openai: provider.clone(),
            gemini: provider.clone(),
            deepseek: provider,
            openrouter: None,
        },
        mcp: ToolMcp::returning(fake_tools(), ""),
        presets: presets(),
        skills: Vec::new(),
        skill_store: Arc::new(Planner(planner)),
        memory: Arc::new(PersistLog::default()),
        tracer: Arc::new(agent::tracing::NullTracer),
        codex: Arc::new(NoCodex),
        decisions: None,
        workflows: std::sync::Arc::new(agent::flows::MemoryWorkflows::default()),
        schedule: std::sync::Arc::new(agent::schedule::MemorySchedule::default()),
    });
    let mut runner = create_workflow_runner(engine, vec!["telegram".into(), "news-digest".into()]).unwrap();
    if let Some(n) = max_attempts {
        Arc::get_mut(&mut runner).unwrap().max_attempts = n;
    }
    (runner, mock)
}

fn signal() -> WorkflowSignal {
    WorkflowSignal {
        id: 1,
        source: "telegram".into(),
        content: "что нового".into(),
        env_context: Some("Default chat id: 285083560".into()),
    }
}

fn env() -> EnvData {
    EnvData {
        now: "2026-06-03T05:05:00Z".parse().unwrap(),
        timezone: "Europe/Kiev".into(),
        user_email: Some("user@example.com".into()),
        news_last_read_at: Some("2026-06-02T18:00:00Z".into()),
        tech_last_read_at: Some("2026-06-02T05:00:00Z".into()),
        telegram_chat_id: None,
    }
}

async fn compile(runner: &WorkflowRunner, scope: &Scope) -> CompileResult {
    runner.compile(&signal(), &env(), scope, None, &CancellationToken::new()).await.unwrap()
}

fn text(m: &Message) -> String {
    m["content"].as_str().unwrap().to_owned()
}

// ── happy path ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn returns_ok_with_the_parsed_plan_on_the_first_valid_response() {
    let (runner, mock) = compiler(vec![Ok(valid_plan())], Some("PLANNER RULES"), None);
    let CompileResult::Ok { workflow, attempts } = compile(&runner, &null_scope()).await else { panic!("not ok") };
    assert_eq!(attempts, 1);
    assert_eq!(workflow.steps[0].kind(), "llm_agent");
    assert_eq!(mock.count(), 1);
}

#[tokio::test]
async fn splits_the_prompt_static_tools_and_skills_in_system_variable_signal_in_user() {
    let (runner, mock) = compiler(vec![Ok(valid_plan())], Some("PLANNER RULES"), None);
    compile(&runner, &null_scope()).await;
    let messages = mock.call(0);
    // System = planner skill + the static reference (tools + skills), led by
    // the skill verbatim so the whole static block is a stable cache prefix.
    assert_eq!(role(&messages[0]), "system");
    let system = text(&messages[0]);
    assert!(system.starts_with("PLANNER RULES"));
    assert!(system.contains("<tools>"));
    // Compact signatures with required / optional parameter names and types.
    assert!(system.contains("- search_news(query: string, k?: number, sinceISO?: string)"), "{system}");
    assert!(system.contains("<skills>"));
    assert!(system.contains("- telegram"));

    // User = only the per-signal variable content.
    let user = text(&messages[1]);
    for part in [
        "<signal>",
        "Source: telegram",
        "что нового",
        "<env>",
        "Europe/Kiev",
        "News last read at: 2026-06-02T18:00:00Z",
        "Tech last read at: 2026-06-02T05:00:00Z",
        "<envContext>",
        "285083560",
    ] {
        assert!(user.contains(part), "{part}");
    }
    assert!(!user.contains("<tools>"));
    assert!(!user.contains("<skills>"));
}

#[tokio::test]
async fn tool_signatures_expose_required_vs_optional_params_correctly() {
    let (runner, mock) = compiler(vec![Ok(valid_plan())], Some("RULES"), None);
    compile(&runner, &null_scope()).await;
    // chatId + text are required, so no `?`.
    assert!(text(&mock.call(0)[0]).contains("- send_telegram_message(chatId: number, text: string)"));
}

#[tokio::test]
async fn uses_the_compiler_presets_model_in_the_request() {
    let (runner, mock) = compiler(vec![Ok(valid_plan())], Some("RULES"), None);
    compile(&runner, &null_scope()).await;
    assert_eq!(mock.count(), 1);
    assert_eq!(mock.calls.lock().unwrap()[0].0, "gemini-3-flash-preview");
}

// ── skill missing ───────────────────────────────────────────────────────────

#[tokio::test]
async fn returns_skill_not_found_without_calling_the_llm() {
    let (runner, mock) = compiler(Vec::new(), None, None);
    let CompileResult::Failed { reason, attempts, .. } = compile(&runner, &null_scope()).await else { panic!("ok") };
    assert_eq!((reason, attempts), ("skill_not_found", 0));
    assert_eq!(mock.count(), 0);
}

// ── LLM error ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn returns_llm_error_and_stops_without_retrying() {
    let (runner, mock) = compiler(vec![Err("rate limit")], Some("RULES"), None);
    let CompileResult::Failed { reason, errors, attempts } = compile(&runner, &null_scope()).await else {
        panic!("ok")
    };
    assert_eq!((reason, attempts), ("llm_error", 1));
    assert!(errors[0].contains("rate limit"));
    assert_eq!(mock.count(), 1);
}

// ── retry loop ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn retries_on_invalid_json_with_error_feedback() {
    let (runner, mock) = compiler(vec![Ok("not json".into()), Ok(valid_plan())], Some("RULES"), None);
    let CompileResult::Ok { attempts, .. } = compile(&runner, &null_scope()).await else { panic!("not ok") };
    assert_eq!(attempts, 2);
    // The second call appended the assistant reply and a corrective user turn.
    assert_eq!(mock.count(), 2);
    let retry = mock.call(1);
    assert_eq!(retry.len(), 4);
    assert_eq!((role(&retry[2]), text(&retry[2]).as_str()), ("assistant", "not json"));
    assert_eq!(role(&retry[3]), "user");
    assert!(text(&retry[3]).contains("invalid JSON"));
}

#[tokio::test]
async fn retries_on_schema_failure_with_formatted_errors() {
    let (runner, mock) = compiler(vec![Ok(invalid_plan()), Ok(valid_plan())], Some("RULES"), None);
    let CompileResult::Ok { attempts, .. } = compile(&runner, &null_scope()).await else { panic!("not ok") };
    assert_eq!(attempts, 2);
    // The retry names the offending tool, so the model can fix it.
    assert!(text(&mock.call(1)[3]).contains("ghost_tool"));
}

#[tokio::test]
async fn returns_schema_invalid_after_exhausting_all_attempts_on_schema_errors() {
    let (runner, _) =
        compiler(vec![Ok(invalid_plan()), Ok(invalid_plan()), Ok(invalid_plan())], Some("RULES"), Some(3));
    let CompileResult::Failed { reason, errors, attempts } = compile(&runner, &null_scope()).await else {
        panic!("ok")
    };
    assert_eq!((reason, attempts), ("schema_invalid", 3));
    assert!(!errors.is_empty());
}

#[tokio::test]
async fn returns_invalid_json_after_exhausting_all_attempts_on_json_errors() {
    let replies = vec![Ok("not json".into()), Ok("still not json".into()), Ok("definitely not".into())];
    let (runner, _) = compiler(replies, Some("RULES"), Some(3));
    let CompileResult::Failed { reason, attempts, .. } = compile(&runner, &null_scope()).await else { panic!("ok") };
    assert_eq!((reason, attempts), ("invalid_json", 3));
}

// Records every generation's name and the metadata seen at start and at end.
type Gens = Arc<Mutex<Vec<(String, Option<Metadata>, Option<Metadata>)>>>;

#[derive(Default)]
struct Collecting {
    gens: Gens,
}

struct Recorded(Gens, usize);

impl Generation for Recorded {
    fn id(&self) -> String {
        "test-gen".into()
    }
    fn end(&self, opts: GenerationEnd) {
        self.0.lock().unwrap()[self.1].2 = opts.metadata;
    }
}

impl TraceContext for Collecting {
    fn update(&self, _: ContextUpdate) {}
    fn generation(&self, opts: GenerationStart) -> Arc<dyn Generation> {
        let mut gens = self.gens.lock().unwrap();
        gens.push((opts.name, opts.metadata, None));
        Arc::new(Recorded(self.gens.clone(), gens.len() - 1))
    }
    fn span(&self, _: SpanStart) -> Arc<dyn Span> {
        Arc::new(CollectingSpan(Collecting { gens: self.gens.clone() }))
    }
    fn event(&self, _: EventStart) {}
}

struct CollectingSpan(Collecting);

impl TraceContext for CollectingSpan {
    fn update(&self, _: ContextUpdate) {}
    fn generation(&self, opts: GenerationStart) -> Arc<dyn Generation> {
        self.0.generation(opts)
    }
    fn span(&self, opts: SpanStart) -> Arc<dyn Span> {
        self.0.span(opts)
    }
    fn event(&self, _: EventStart) {}
}

impl Span for CollectingSpan {
    fn id(&self) -> String {
        "test-span".into()
    }
    fn end(&self, _: agent::tracing::SpanEnd) {}
}

#[tokio::test]
async fn tags_only_the_accepted_attempt_as_the_planner_judge_node_never_failed_retries() {
    let (runner, _) = compiler(vec![Ok("not json".into()), Ok(valid_plan())], Some("RULES"), None);
    let trace = Arc::new(Collecting::default());
    let scope: Scope = trace.clone();
    assert!(matches!(compile(&runner, &scope).await, CompileResult::Ok { .. }));
    let gens = trace.gens.lock().unwrap().clone();
    let attempts: Vec<_> = gens.iter().filter(|g| g.0.starts_with("attempt-")).collect();
    assert_eq!(attempts.len(), 2);
    // Identity is never set at start — `attempt-N` is display-only.
    assert!(attempts.iter().all(|g| g.1.is_none()));
    // The failed attempt stays untagged, so the per-node judge skips it.
    assert!(attempts[0].2.is_none());
    // Only the winning attempt is the judgeable planner node.
    assert_eq!(attempts[1].2.as_ref().map(|m| Value::Object(m.clone())), Some(json!({"judge_node":"planner"})));
}

#[tokio::test]
async fn respects_a_custom_max_attempts_of_1_no_retries() {
    let (runner, _) = compiler(vec![Ok("not json".into())], Some("RULES"), Some(1));
    let CompileResult::Failed { attempts, .. } = compile(&runner, &null_scope()).await else { panic!("ok") };
    assert_eq!(attempts, 1);
}
