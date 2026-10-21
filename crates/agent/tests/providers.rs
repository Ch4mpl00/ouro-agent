// Port of src/providers/providers.test.ts, one test per case. The fake API
// stands where the TS test fakes the OpenAI client: it captures the request
// body and returns a canned completion.
use agent::{
    models::ReasoningEffort as Effort,
    providers::*,
    tracing::{
        ContextUpdate, EventStart, Generation, GenerationStart, Level, Scope, Span, SpanStart, TokenUsage,
        TraceContext, null_scope,
    },
};
use async_trait::async_trait;
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

struct Api {
    bodies: Mutex<Vec<Value>>,
    usage: Value,
}

#[async_trait]
impl JsonApi for Api {
    async fn post(&self, _: &str, body: Value, _: &CancellationToken) -> anyhow::Result<Value> {
        self.bodies.lock().unwrap().push(body);
        Ok(json!({
            "choices": [{ "message": { "role": "assistant", "content": "ok" }, "finish_reason": "stop" }],
            "usage": self.usage,
        }))
    }
}

fn openai_usage() -> Value {
    json!({ "prompt_tokens": 100, "completion_tokens": 20, "total_tokens": 120, "prompt_tokens_details": { "cached_tokens": 64 } })
}

fn deepseek_usage() -> Value {
    json!({ "prompt_tokens": 100, "completion_tokens": 20, "total_tokens": 120, "prompt_cache_hit_tokens": 48, "prompt_cache_miss_tokens": 52 })
}

fn fake(kind: ProviderKind, usage: Value) -> (ApiProvider, Arc<Api>) {
    let api = Arc::new(Api { bodies: Mutex::new(Vec::new()), usage });
    (provider(kind, api.clone()), api)
}

fn body(api: &Api, i: usize) -> Value {
    api.bodies.lock().unwrap()[i].clone()
}

struct Call {
    model: &'static str,
    messages: Vec<Message>,
    effort: Effort,
    tools: Vec<ToolDef>,
    response_format: Option<Value>,
    trace: Option<Scope>,
}

impl Call {
    fn new(model: &'static str, effort: Effort) -> Call {
        Call { model, messages: Vec::new(), effort, tools: Vec::new(), response_format: None, trace: None }
    }
    fn messages(mut self, messages: Vec<Message>) -> Call {
        self.messages = messages;
        self
    }
    fn tools_and_format(mut self) -> Call {
        self.tools = vec![ToolDef { name: "t".into(), description: None, parameters: json!({}) }];
        self.response_format = Some(json!({ "type": "json_object" }));
        self
    }
    fn trace(mut self, trace: Scope) -> Call {
        self.trace = Some(trace);
        self
    }
    async fn run(&mut self, provider: &dyn ChatProvider) -> anyhow::Result<CompletionResult> {
        provider
            .complete(&mut CompletionParams {
                model: self.model.into(),
                messages: &mut self.messages,
                reasoning_effort: self.effort,
                tools: self.tools.clone(),
                response_format: self.response_format.clone(),
                trace: self.trace.clone(),
                cancel: CancellationToken::new(),
            })
            .await
    }
}

fn tools_json() -> Value {
    json!([{ "type": "function", "function": { "name": "t", "parameters": {} } }])
}

fn usage(cached: u64) -> Option<TokenUsage> {
    Some(TokenUsage { input: 100, output: 20, total: 120, cached: Some(cached) })
}

// ── openai provider ─────────────────────────────────────────────────────────

#[tokio::test]
async fn openai_sends_no_thinking_no_reasoning_effort_omits_empty_tools() {
    let (p, api) = fake(ProviderKind::Openai, openai_usage());
    Call::new("gpt-5.4", Effort::Max).messages(vec![user("x")]).run(&p).await.unwrap();
    assert_eq!(body(&api, 0), json!({ "model": "gpt-5.4", "messages": [{ "role": "user", "content": "x" }] }));
}

#[tokio::test]
async fn openai_passes_tools_and_response_format_through_when_provided() {
    let (p, api) = fake(ProviderKind::Openai, openai_usage());
    Call::new("gpt-5.4", Effort::Disabled).tools_and_format().run(&p).await.unwrap();
    assert_eq!(body(&api, 0)["tools"], tools_json());
    assert_eq!(body(&api, 0)["response_format"], json!({ "type": "json_object" }));
}

#[tokio::test]
async fn openai_normalizes_usage_incl_cached_from_prompt_tokens_details() {
    let (p, _) = fake(ProviderKind::Openai, openai_usage());
    let r = Call::new("gpt-5.4", Effort::Disabled).run(&p).await.unwrap();
    assert_eq!(r.usage, usage(64));
    assert_eq!(r.finish_reason.as_deref(), Some("stop"));
    assert_eq!(r.message["content"], "ok");
}

// ── deepseek provider ───────────────────────────────────────────────────────

#[tokio::test]
async fn deepseek_disabled_effort_thinking_disabled_no_reasoning_effort() {
    let (p, api) = fake(ProviderKind::Deepseek, deepseek_usage());
    Call::new("deepseek-v4-pro", Effort::Disabled).run(&p).await.unwrap();
    assert_eq!(body(&api, 0)["thinking"], json!({ "type": "disabled" }));
    assert!(body(&api, 0).get("reasoning_effort").is_none());
}

#[tokio::test]
async fn deepseek_enabled_effort_thinking_enabled_plus_reasoning_effort() {
    let (p, api) = fake(ProviderKind::Deepseek, deepseek_usage());
    Call::new("deepseek-v4-pro", Effort::Max).run(&p).await.unwrap();
    assert_eq!(body(&api, 0)["thinking"], json!({ "type": "enabled" }));
    assert_eq!(body(&api, 0)["reasoning_effort"], "max");
}

#[tokio::test]
async fn deepseek_stamps_reasoning_content_on_prior_assistant_turns_when_thinking_enabled() {
    let (p, _) = fake(ProviderKind::Deepseek, deepseek_usage());
    let mut call = Call::new("deepseek-v4-pro", Effort::Max).messages(vec![user("hi"), assistant("prior")]);
    call.run(&p).await.unwrap();
    assert_eq!(call.messages[1]["reasoning_content"], "");
    // Rust-only: a user turn is never stamped.
    assert!(call.messages[0].get("reasoning_content").is_none());
}

#[tokio::test]
async fn deepseek_does_not_stamp_reasoning_content_when_thinking_disabled() {
    let (p, _) = fake(ProviderKind::Deepseek, deepseek_usage());
    let mut call = Call::new("deepseek-v4-pro", Effort::Disabled).messages(vec![assistant("prior")]);
    call.run(&p).await.unwrap();
    assert!(call.messages[0].get("reasoning_content").is_none());
}

#[tokio::test]
async fn deepseek_normalizes_usage_incl_cached_from_prompt_cache_hit_tokens() {
    let (p, _) = fake(ProviderKind::Deepseek, deepseek_usage());
    let r = Call::new("deepseek-v4-pro", Effort::Max).run(&p).await.unwrap();
    assert_eq!(r.usage, usage(48));
}

// ── gemini provider ─────────────────────────────────────────────────────────

#[tokio::test]
async fn gemini_disabled_effort_omits_reasoning_effort() {
    let (p, api) = fake(ProviderKind::Gemini, openai_usage());
    Call::new("gemini-3.5-flash", Effort::Disabled).messages(vec![user("x")]).run(&p).await.unwrap();
    assert_eq!(body(&api, 0), json!({ "model": "gemini-3.5-flash", "messages": [{ "role": "user", "content": "x" }] }));
}

#[tokio::test]
async fn gemini_non_disabled_effort_maps_to_reasoning_effort_high() {
    let (p, api) = fake(ProviderKind::Gemini, openai_usage());
    Call::new("gemini-3.5-flash", Effort::Max).run(&p).await.unwrap();
    assert_eq!(body(&api, 0)["reasoning_effort"], "high");
}

#[tokio::test]
async fn gemini_low_effort_maps_to_reasoning_effort_low() {
    let (p, api) = fake(ProviderKind::Gemini, openai_usage());
    Call::new("gemini-3-flash-preview", Effort::Low).run(&p).await.unwrap();
    assert_eq!(body(&api, 0)["reasoning_effort"], "low");
}

#[tokio::test]
async fn gemini_passes_tools_and_response_format_through_normalizes_usage_like_openai() {
    let (p, api) = fake(ProviderKind::Gemini, openai_usage());
    let r = Call::new("gemini-3.5-flash", Effort::Disabled).tools_and_format().run(&p).await.unwrap();
    assert_eq!(body(&api, 0)["tools"], tools_json());
    assert_eq!(body(&api, 0)["response_format"], json!({ "type": "json_object" }));
    assert_eq!(r.usage, usage(64));
}

// ── openrouter provider ─────────────────────────────────────────────────────

#[tokio::test]
async fn openrouter_strips_our_routing_prefix_from_the_model_it_sends() {
    let (p, api) = fake(ProviderKind::Openrouter, openai_usage());
    Call::new("openrouter/anthropic/claude-sonnet-5.5", Effort::Disabled)
        .messages(vec![user("x")])
        .run(&p)
        .await
        .unwrap();
    assert_eq!(
        body(&api, 0),
        json!({ "model": "anthropic/claude-sonnet-5.5", "messages": [{ "role": "user", "content": "x" }],
            "reasoning": { "enabled": false } })
    );
}

#[tokio::test]
async fn openrouter_disabled_effort_turns_reasoning_off() {
    let (p, api) = fake(ProviderKind::Openrouter, openai_usage());
    Call::new("openrouter/qwen/qwen3", Effort::Disabled).run(&p).await.unwrap();
    assert_eq!(body(&api, 0)["reasoning"], json!({ "enabled": false }));
    assert!(body(&api, 0).get("reasoning_effort").is_none());
}

#[tokio::test]
async fn openrouter_maps_effort_onto_the_unified_reasoning_field_max_to_high() {
    let (p, api) = fake(ProviderKind::Openrouter, openai_usage());
    Call::new("openrouter/qwen/qwen3", Effort::Low).run(&p).await.unwrap();
    Call::new("openrouter/qwen/qwen3", Effort::Max).run(&p).await.unwrap();
    assert_eq!(body(&api, 0)["reasoning"], json!({ "effort": "low" }));
    assert_eq!(body(&api, 1)["reasoning"], json!({ "effort": "high" }));
}

#[tokio::test]
async fn openrouter_passes_tools_and_response_format_through_normalizes_usage_like_openai() {
    let (p, api) = fake(ProviderKind::Openrouter, openai_usage());
    let r = Call::new("openrouter/qwen/qwen3", Effort::Disabled).tools_and_format().run(&p).await.unwrap();
    assert_eq!(body(&api, 0)["tools"], tools_json());
    assert_eq!(body(&api, 0)["response_format"], json!({ "type": "json_object" }));
    assert_eq!(r.usage, usage(64));
}

// Rust-only: replies the SDK would have rejected or typed away.

#[tokio::test]
async fn keeps_unmodelled_reply_fields_and_tolerates_missing_usage() {
    struct Raw;
    #[async_trait]
    impl JsonApi for Raw {
        async fn post(&self, _: &str, _: Value, _: &CancellationToken) -> anyhow::Result<Value> {
            Ok(
                json!({"choices":[{"message":{"role":"assistant","content":"ok","reasoning_content":"keep","extra_content":{"google":{}}},"finish_reason":"stop"}]}),
            )
        }
    }
    let p = provider(ProviderKind::Gemini, Arc::new(Raw));
    let r = Call::new("gemini-3.5-flash", Effort::Disabled).run(&p).await.unwrap();
    assert_eq!(r.message["reasoning_content"], "keep");
    assert_eq!(r.message["extra_content"], json!({"google":{}}));
    assert!(r.usage.is_none());
}

#[tokio::test]
async fn a_reply_without_choices_is_an_error_naming_the_model() {
    struct Empty;
    #[async_trait]
    impl JsonApi for Empty {
        async fn post(&self, _: &str, _: Value, _: &CancellationToken) -> anyhow::Result<Value> {
            Ok(json!({"model":"test","choices":[]}))
        }
    }
    let p = provider(ProviderKind::Openai, Arc::new(Empty));
    let err = Call::new("test", Effort::Disabled).run(&p).await.unwrap_err();
    assert_eq!(err.to_string(), "provider returned no choices (model=test)");
}

// ── withRetry decorator ─────────────────────────────────────────────────────

// Throws the queued errors first, then succeeds.
struct Flaky {
    errors: Mutex<VecDeque<anyhow::Error>>,
    calls: AtomicUsize,
}

#[async_trait]
impl ChatProvider for Flaky {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Openai
    }
    async fn complete(&self, _: &mut CompletionParams<'_>) -> anyhow::Result<CompletionResult> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(err) = self.errors.lock().unwrap().pop_front() {
            return Err(err);
        }
        Ok(CompletionResult { message: assistant("ok"), finish_reason: Some("stop".into()), usage: None })
    }
}

fn flaky(errors: Vec<anyhow::Error>) -> Arc<Flaky> {
    Arc::new(Flaky { errors: Mutex::new(errors.into()), calls: AtomicUsize::new(0) })
}

fn api_error(status: u16) -> anyhow::Error {
    ApiError::Status { status, message: format!("status {status}") }.into()
}

// Records the events; everything else is a no-op.
#[derive(Default)]
struct Capture {
    events: Mutex<Vec<EventStart>>,
}

impl TraceContext for Capture {
    fn update(&self, _: ContextUpdate) {}
    fn generation(&self, opts: GenerationStart) -> Arc<dyn Generation> {
        null_scope().generation(opts)
    }
    fn span(&self, opts: SpanStart) -> Arc<dyn Span> {
        null_scope().span(opts)
    }
    fn event(&self, opts: EventStart) {
        self.events.lock().unwrap().push(opts);
    }
}

fn retrying(inner: Arc<Flaky>, max_retries: u32) -> RetryingProvider {
    with_retry(inner, RetryOpts { max_retries, base_delay_ms: 1, ..RetryOpts::default() })
}

#[tokio::test]
async fn with_retry_retries_429_5xx_and_emits_a_warning_llm_retry_event_per_attempt_on_the_trace() {
    let inner = flaky(vec![api_error(429), api_error(503)]);
    let capture = Arc::new(Capture::default());
    let r = Call::new("gpt-5.4-mini", Effort::Disabled)
        .trace(capture.clone())
        .run(&retrying(inner.clone(), 4))
        .await
        .unwrap();
    assert_eq!(r.message["content"], "ok");
    assert_eq!(inner.calls.load(Ordering::SeqCst), 3);
    let events = capture.events.lock().unwrap();
    assert_eq!(events.iter().map(|e| e.name.as_str()).collect::<Vec<_>>(), ["llm_retry", "llm_retry"]);
    assert!(events.iter().all(|e| e.level == Some(Level::Warning)));
    let first = events[0].metadata.as_ref().unwrap();
    assert_eq!(
        (&first["attempt"], &first["status"], &first["model"]),
        (&json!(1), &json!(429), &json!("gpt-5.4-mini"))
    );
    let second = events[1].metadata.as_ref().unwrap();
    assert_eq!((&second["attempt"], &second["status"]), (&json!(2), &json!(503)));
}

#[tokio::test]
async fn with_retry_retries_a_connection_level_failure() {
    let inner = flaky(vec![ApiError::Connection("Premature close".into()).into()]);
    let capture = Arc::new(Capture::default());
    let r = Call::new("gpt-5.4-mini", Effort::Disabled)
        .trace(capture.clone())
        .run(&retrying(inner.clone(), 4))
        .await
        .unwrap();
    assert_eq!(r.message["content"], "ok");
    assert_eq!(inner.calls.load(Ordering::SeqCst), 2);
    let events = capture.events.lock().unwrap();
    assert_eq!(events.iter().map(|e| e.name.as_str()).collect::<Vec<_>>(), ["llm_retry"]);
    let metadata = events[0].metadata.as_ref().unwrap();
    assert_eq!((&metadata["attempt"], &metadata["status"]), (&json!(1), &Value::Null));
}

#[tokio::test]
async fn with_retry_rethrows_non_retryable_4xx_immediately_no_events() {
    let inner = flaky(vec![api_error(400)]);
    let capture = Arc::new(Capture::default());
    let err = Call::new("gpt-5.4-mini", Effort::Disabled)
        .trace(capture.clone())
        .run(&retrying(inner.clone(), 4))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("status 400"));
    assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
    assert!(capture.events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn with_retry_gives_up_after_max_retries_and_rethrows_the_last_error() {
    let inner = flaky(vec![api_error(429), api_error(429), api_error(429)]);
    let err = Call::new("gpt-5.4-mini", Effort::Disabled).run(&retrying(inner.clone(), 2)).await.unwrap_err();
    assert!(err.to_string().contains("status 429"));
    assert_eq!(inner.calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn with_retry_works_without_a_trace() {
    let inner = flaky(vec![api_error(500)]);
    let r = Call::new("gpt-5.4-mini", Effort::Disabled).run(&retrying(inner.clone(), 4)).await.unwrap();
    assert_eq!(r.message["content"], "ok");
    assert_eq!(inner.calls.load(Ordering::SeqCst), 2);
}

// Rust-only: the transient classification, which the SDK's error classes
// carried in TS.
#[test]
fn transient_means_408_429_5xx_or_no_response() {
    for status in [408, 429, 500, 503] {
        assert!(is_transient(&api_error(status)));
    }
    for status in [400, 401, 403, 404] {
        assert!(!is_transient(&api_error(status)));
    }
    assert!(is_transient(&ApiError::Connection("reset".into()).into()));
    assert!(!is_transient(&anyhow::anyhow!("programming error")));
}
