// Chat providers. OpenAI, DeepSeek, Gemini (its OpenAI-compatible endpoint)
// and OpenRouter all speak the OpenAI Chat Completions wire format, but they
// differ in the request (DeepSeek's `thinking` + `reasoning_effort` and the
// `reasoning_content` stamp, Gemini's effort enum, OpenRouter's `reasoning`)
// and in where usage reports the cached prompt. `ChatProvider::complete`
// hides both behind one call; a new provider is a new kind, no call-site edit.
//
// Messages are kept as the JSON objects the API speaks: an assistant message
// goes back into the history exactly as it came — with fields no type here
// models (DeepSeek's reasoning_content, Gemini's tool-call extras), which the
// next request must carry.
//
// Sections:
//   1. messages and tools
//   2. the HTTP API
//   3. providers
//   4. retry

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Map, Value, json};
use tokio_util::sync::CancellationToken;

use crate::errors::{aborted, or_cancel};
use crate::models::ReasoningEffort;
use crate::tracing::{EventStart, Level, Scope, TokenUsage};

// ── 1. messages and tools ───────────────────────────────────────────────────

pub type Message = Map<String, Value>;

fn message(role: &str, content: &str) -> Message {
    let mut m = Map::new();
    m.insert("role".into(), role.into());
    m.insert("content".into(), content.into());
    m
}

pub fn system(content: &str) -> Message {
    message("system", content)
}

pub fn user(content: &str) -> Message {
    message("user", content)
}

pub fn assistant(content: &str) -> Message {
    message("assistant", content)
}

pub fn tool_reply(tool_call_id: &str, content: &str) -> Message {
    let mut m = Map::new();
    m.insert("role".into(), "tool".into());
    m.insert("tool_call_id".into(), tool_call_id.into());
    m.insert("content".into(), content.into());
    m
}

pub fn role(m: &Message) -> &str {
    m.get("role").and_then(Value::as_str).unwrap_or("")
}

// The text content, or None when it is null / absent (a tool-calling turn).
pub fn content(m: &Message) -> Option<&str> {
    m.get("content").and_then(Value::as_str)
}

#[derive(Clone, Debug, PartialEq)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: String,
}

pub fn tool_calls(m: &Message) -> Vec<ToolCall> {
    let Some(Value::Array(calls)) = m.get("tool_calls") else { return Vec::new() };
    calls
        .iter()
        .map(|c| ToolCall {
            id: c["id"].as_str().unwrap_or_default().to_owned(),
            name: c["function"]["name"].as_str().unwrap_or_default().to_owned(),
            arguments: c["function"]["arguments"].as_str().unwrap_or_default().to_owned(),
        })
        .collect()
}

// A function tool as the API takes it.
#[derive(Clone, Debug, PartialEq)]
pub struct ToolDef {
    pub name: String,
    pub description: Option<String>,
    pub parameters: Value,
}

impl ToolDef {
    pub fn new(name: &str, description: &str, parameters: Value) -> ToolDef {
        ToolDef { name: name.to_owned(), description: Some(description.to_owned()), parameters }
    }

    pub fn to_json(&self) -> Value {
        let mut function = Map::new();
        function.insert("name".into(), self.name.clone().into());
        if let Some(d) = &self.description {
            function.insert("description".into(), d.clone().into());
        }
        function.insert("parameters".into(), self.parameters.clone());
        json!({ "type": "function", "function": function })
    }
}

// ── 2. the HTTP API ─────────────────────────────────────────────────────────

// A failed API request: an HTTP error status, or no response at all.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("{status} {message}")]
    Status { status: u16, message: String },
    #[error("{0}")]
    Connection(String),
}

impl ApiError {
    pub fn status(&self) -> Option<u16> {
        match self {
            ApiError::Status { status, .. } => Some(*status),
            ApiError::Connection(_) => None,
        }
    }

    // 408 / 429 / 5xx and connection failures come back; any other status
    // is permanent.
    pub fn is_transient(&self) -> bool {
        match self {
            ApiError::Connection(_) => true,
            ApiError::Status { status, .. } => *status == 408 || *status == 429 || *status >= 500,
        }
    }
}

// An OpenAI-compatible JSON endpoint. `path` is relative to the base URL
// ("chat/completions") or an absolute URL (the decisions API).
#[async_trait]
pub trait JsonApi: Send + Sync {
    async fn post(&self, path: &str, body: Value, cancel: &CancellationToken) -> anyhow::Result<Value>;
}

pub struct HttpApi {
    client: reqwest::Client,
    base_url: String,
    api_key: String,
    headers: Vec<(String, String)>,
}

impl HttpApi {
    pub fn new(base_url: &str, api_key: &str) -> HttpApi {
        HttpApi {
            client: reqwest::Client::new(),
            base_url: base_url.trim_end_matches('/').to_owned(),
            api_key: api_key.to_owned(),
            headers: Vec::new(),
        }
    }

    pub fn with_header(mut self, name: &str, value: &str) -> HttpApi {
        self.headers.push((name.to_owned(), value.to_owned()));
        self
    }
}

#[async_trait]
impl JsonApi for HttpApi {
    async fn post(&self, path: &str, body: Value, cancel: &CancellationToken) -> anyhow::Result<Value> {
        let url = if path.starts_with("http://") || path.starts_with("https://") {
            path.to_owned()
        } else {
            format!("{}/{}", self.base_url, path.trim_start_matches('/'))
        };
        let mut req = self.client.post(url).bearer_auth(&self.api_key).json(&body);
        for (name, value) in &self.headers {
            req = req.header(name, value);
        }
        or_cancel(cancel, async {
            let res = req.send().await.map_err(|e| ApiError::Connection(format!("Connection error: {e}")))?;
            let status = res.status();
            let text = res.text().await.map_err(|e| ApiError::Connection(format!("Connection error: {e}")))?;
            if !status.is_success() {
                let parsed: Option<Value> = serde_json::from_str(&text).ok();
                let message = parsed
                    .as_ref()
                    .and_then(|v| v["error"]["message"].as_str().or(v["message"].as_str()).map(str::to_owned))
                    .unwrap_or_else(|| if text.is_empty() { "(no body)".into() } else { text.clone() });
                return Err(ApiError::Status { status: status.as_u16(), message }.into());
            }
            Ok(serde_json::from_str(&text)?)
        })
        .await
    }
}

pub const DEEPSEEK_BASE_URL: &str = "https://api.deepseek.com";
pub const GEMINI_BASE_URL: &str = "https://generativelanguage.googleapis.com/v1beta/openai/";
pub const OPENAI_BASE_URL: &str = "https://api.openai.com/v1";
pub const OPENROUTER_BASE_URL: &str = "https://openrouter.ai/api/v1";
pub const OPENROUTER_MODEL_PREFIX: &str = "openrouter/";

// The OpenRouter client every caller uses; the title labels our traffic on
// the OpenRouter dashboard.
pub fn openrouter_api(api_key: &str) -> HttpApi {
    HttpApi::new(OPENROUTER_BASE_URL, api_key).with_header("X-Title", "mcp-tools")
}

pub fn is_openrouter_model(model: &str) -> bool {
    model.starts_with(OPENROUTER_MODEL_PREFIX)
}

// ── 3. providers ────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderKind {
    Deepseek,
    Openai,
    Gemini,
    Openrouter,
}

impl ProviderKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ProviderKind::Deepseek => "deepseek",
            ProviderKind::Openai => "openai",
            ProviderKind::Gemini => "gemini",
            ProviderKind::Openrouter => "openrouter",
        }
    }
}

pub struct CompletionParams<'a> {
    pub model: String,
    // The conversation. DeepSeek repairs it in place (see below): it is the
    // history the next call must send.
    pub messages: &'a mut Vec<Message>,
    pub reasoning_effort: ReasoningEffort,
    // Empty → no `tools` sent.
    pub tools: Vec<ToolDef>,
    pub response_format: Option<Value>,
    // The call site's trace scope, for the retry wrapper's `llm_retry`
    // events; concrete providers never touch it.
    pub trace: Option<Scope>,
    pub cancel: CancellationToken,
}

#[derive(Clone, Debug)]
pub struct CompletionResult {
    pub message: Message,
    pub finish_reason: Option<String>,
    pub usage: Option<TokenUsage>,
}

#[async_trait]
pub trait ChatProvider: Send + Sync {
    fn kind(&self) -> ProviderKind;
    async fn complete(&self, params: &mut CompletionParams<'_>) -> anyhow::Result<CompletionResult>;
}

pub type SharedProvider = Arc<dyn ChatProvider>;

pub struct ApiProvider {
    kind: ProviderKind,
    api: Arc<dyn JsonApi>,
}

pub fn provider(kind: ProviderKind, api: Arc<dyn JsonApi>) -> ApiProvider {
    ApiProvider { kind, api }
}

fn number(v: &Value) -> u64 {
    v.as_u64().unwrap_or_default()
}

// OpenAI: the cached prompt is under prompt_tokens_details.cached_tokens;
// DeepSeek: prompt_cache_hit_tokens.
fn normalize_usage(kind: ProviderKind, usage: &Value) -> Option<TokenUsage> {
    if !usage.is_object() {
        return None;
    }
    let cached = match kind {
        ProviderKind::Deepseek => usage["prompt_cache_hit_tokens"].as_u64(),
        _ => usage["prompt_tokens_details"]["cached_tokens"].as_u64(),
    };
    Some(TokenUsage {
        input: number(&usage["prompt_tokens"]),
        output: number(&usage["completion_tokens"]),
        total: number(&usage["total_tokens"]),
        cached,
    })
}

// DeepSeek requires `reasoning_content` on every prior assistant turn of a
// thinking-enabled call, even an empty one; turns made with thinking off (or
// by another provider) lack it.
fn stamp_reasoning_content(messages: &mut [Message]) {
    for m in messages.iter_mut() {
        if role(m) == "assistant" && !m.contains_key("reasoning_content") {
            m.insert("reasoning_content".into(), "".into());
        }
    }
}

#[async_trait]
impl ChatProvider for ApiProvider {
    fn kind(&self) -> ProviderKind {
        self.kind
    }

    async fn complete(&self, p: &mut CompletionParams<'_>) -> anyhow::Result<CompletionResult> {
        let effort = p.reasoning_effort;
        let thinking = effort != ReasoningEffort::Disabled;
        if self.kind == ProviderKind::Deepseek && thinking {
            stamp_reasoning_content(p.messages);
        }
        let model = match self.kind {
            // Our routing prefix; OpenRouter knows the model without it.
            ProviderKind::Openrouter => p.model.strip_prefix(OPENROUTER_MODEL_PREFIX).unwrap_or(&p.model).to_owned(),
            _ => p.model.clone(),
        };
        let mut body = Map::new();
        body.insert("model".into(), model.into());
        body.insert("messages".into(), Value::Array(p.messages.iter().cloned().map(Value::Object).collect()));
        if !p.tools.is_empty() {
            body.insert("tools".into(), Value::Array(p.tools.iter().map(ToolDef::to_json).collect()));
        }
        if let Some(format) = &p.response_format {
            body.insert("response_format".into(), format.clone());
        }
        match self.kind {
            // Never sent: the default, effort-less request measured as the
            // best speed / quality / price point on the OpenAI models we use.
            ProviderKind::Openai => {}
            ProviderKind::Deepseek => {
                if thinking {
                    body.insert("thinking".into(), json!({ "type": "enabled" }));
                    body.insert("reasoning_effort".into(), effort.as_str().into());
                } else {
                    body.insert("thinking".into(), json!({ "type": "disabled" }));
                }
            }
            // Gemini's enum is none|low|medium|high: "disabled" omits it (the
            // dynamic budget), "low" is the compiler's latency knob, the rest
            // map to "high".
            ProviderKind::Gemini => match effort {
                ReasoningEffort::Disabled => {}
                ReasoningEffort::Low => {
                    body.insert("reasoning_effort".into(), "low".into());
                }
                _ => {
                    body.insert("reasoning_effort".into(), "high".into());
                }
            },
            // OpenRouter's unified field, translated per upstream; it tops out
            // at "high". Disabled says so: a model that reasons by default
            // (MiMo) would otherwise think for minutes on every turn.
            ProviderKind::Openrouter => {
                let reasoning = if thinking {
                    json!({ "effort": if effort == ReasoningEffort::Max { "high" } else { effort.as_str() } })
                } else {
                    json!({ "enabled": false })
                };
                body.insert("reasoning".into(), reasoning);
            }
        }
        let raw = self.api.post("chat/completions", Value::Object(body), &p.cancel).await?;
        let Some(choice) = raw["choices"].get(0) else {
            anyhow::bail!("provider returned no choices (model={})", raw["model"].as_str().unwrap_or("undefined"));
        };
        Ok(CompletionResult {
            message: choice["message"].as_object().cloned().unwrap_or_default(),
            finish_reason: choice["finish_reason"].as_str().map(str::to_owned),
            usage: normalize_usage(self.kind, &raw["usage"]),
        })
    }
}

// ── 4. retry ────────────────────────────────────────────────────────────────

// Transient-failure retry, applied to EVERY provider at startup rather than
// living in one: the compiler is on the hot path whichever provider its model
// routes to. Each retry is visible — a WARNING `llm_retry` event on the call
// site's trace, and a line on stderr — so a retried call never reads as one
// slow call.

#[derive(Clone, Debug)]
pub struct RetryOpts {
    // Retries after the first attempt.
    pub max_retries: u32,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
    // ±20% on each delay, so concurrent callers don't retry in step.
    pub jitter: bool,
}

impl Default for RetryOpts {
    fn default() -> Self {
        RetryOpts { max_retries: 4, base_delay_ms: 2000, max_delay_ms: 30_000, jitter: true }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RetryInfo {
    // 1-based number of the attempt that just failed.
    pub attempt: u32,
    pub status: Option<u16>,
    pub delay_ms: u64,
}

pub fn is_transient(err: &anyhow::Error) -> bool {
    err.downcast_ref::<ApiError>().is_some_and(ApiError::is_transient)
}

// Exponential backoff: base · 2^(attempt − 1), jittered, capped.
pub fn retry_delay(opts: &RetryOpts, attempt: u32) -> u64 {
    let exponential = opts.base_delay_ms.saturating_mul(1u64 << (attempt - 1).min(32));
    let jittered = if opts.jitter {
        let factor = 0.8 + rand::random::<f64>() * 0.4;
        (exponential as f64 * factor) as u64
    } else {
        exponential
    };
    jittered.min(opts.max_delay_ms)
}

// The retry after a failed attempt, or None when the failure is permanent
// or the budget is spent.
pub fn next_retry(opts: &RetryOpts, attempt: u32, err: &anyhow::Error) -> Option<RetryInfo> {
    if !is_transient(err) || attempt > opts.max_retries {
        return None;
    }
    Some(RetryInfo {
        attempt,
        status: err.downcast_ref::<ApiError>().and_then(ApiError::status),
        delay_ms: retry_delay(opts, attempt),
    })
}

// The backoff sleep; ends early, with `Aborted`, on cancellation.
pub async fn backoff(cancel: &CancellationToken, info: &RetryInfo) -> anyhow::Result<()> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(aborted()),
        _ = tokio::time::sleep(Duration::from_millis(info.delay_ms)) => Ok(()),
    }
}

// Runs `attempt_fn` until it succeeds, fails permanently, or the retry budget
// is spent.
pub async fn retry_on_transient<T, F, Fut>(
    opts: &RetryOpts,
    cancel: &CancellationToken,
    mut on_retry: impl FnMut(&RetryInfo),
    mut attempt_fn: F,
) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    let mut attempt = 1;
    loop {
        let err = match or_cancel(cancel, attempt_fn()).await {
            Ok(value) => return Ok(value),
            Err(err) => err,
        };
        let Some(info) = next_retry(opts, attempt, &err) else { return Err(err) };
        on_retry(&info);
        backoff(cancel, &info).await?;
        attempt += 1;
    }
}

pub struct RetryingProvider {
    inner: SharedProvider,
    opts: RetryOpts,
}

pub fn with_retry(inner: SharedProvider, opts: RetryOpts) -> RetryingProvider {
    RetryingProvider { inner, opts }
}

// The WARNING event a retry leaves on the trace.
pub fn retry_event(provider: &str, model: &str, info: &RetryInfo) -> EventStart {
    let mut metadata = Map::new();
    metadata.insert("provider".into(), provider.into());
    metadata.insert("model".into(), model.into());
    metadata.insert("attempt".into(), info.attempt.into());
    metadata.insert("status".into(), info.status.map_or(Value::Null, Value::from));
    metadata.insert("delay_ms".into(), info.delay_ms.into());
    EventStart { name: "llm_retry".into(), level: Some(Level::Warning), metadata: Some(metadata), input: None }
}

pub fn log_retry(what: &str, info: &RetryInfo) {
    eprintln!(
        "[retry] {what} attempt {} failed (status={}), retrying in {}ms",
        info.attempt,
        info.status.map_or("?".to_owned(), |s| s.to_string()),
        info.delay_ms
    );
}

#[async_trait]
impl ChatProvider for RetryingProvider {
    fn kind(&self) -> ProviderKind {
        self.inner.kind()
    }

    async fn complete(&self, p: &mut CompletionParams<'_>) -> anyhow::Result<CompletionResult> {
        let kind = self.inner.kind().as_str();
        let cancel = p.cancel.clone();
        let mut attempt = 1;
        loop {
            let err = match or_cancel(&cancel, self.inner.complete(p)).await {
                Ok(result) => return Ok(result),
                Err(err) => err,
            };
            let Some(info) = next_retry(&self.opts, attempt, &err) else { return Err(err) };
            log_retry(&format!("{kind}/{}", p.model), &info);
            if let Some(trace) = &p.trace {
                trace.event(retry_event(kind, &p.model, &info));
            }
            backoff(&cancel, &info).await?;
            attempt += 1;
        }
    }
}
