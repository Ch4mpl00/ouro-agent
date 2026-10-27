// A judge backend runs ONE structured-output completion: a system instruction
// + user content + a strict JSON schema → the raw parsed JSON object, which the
// caller validates. This is the SINGLE primitive that differs between judge
// providers (today only OpenAI chat). Everything provider-agnostic —
// rubric selection, prompt building, the scorecard+faithfulness split — lives
// in node_judge.rs and depends only on this trait.

use std::sync::Arc;

use agent::providers::{HttpApi, JsonApi, OPENAI_BASE_URL, RetryOpts, retry_on_transient};
use async_trait::async_trait;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::schema::JUDGE_MODEL;

pub struct JudgeCompletion<'a> {
    // Schema name (OpenAI's json_schema label, e.g. "scorecard"); ignored by
    // backends that don't take one.
    pub name: &'a str,
    pub system: &'a str,
    pub user: &'a str,
    pub schema: &'a Value,
}

#[async_trait]
pub trait JudgeBackend: Send + Sync {
    async fn complete(&self, req: &JudgeCompletion<'_>) -> anyhow::Result<Value>;
}

pub type SharedBackend = Arc<dyn JudgeBackend>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JudgeProvider {
    OpenAi,
}

impl JudgeProvider {
    pub fn as_str(self) -> &'static str {
        match self {
            JudgeProvider::OpenAi => "openai",
        }
    }

    pub fn parse(value: &str) -> anyhow::Result<JudgeProvider> {
        match value {
            "openai" => Ok(JudgeProvider::OpenAi),
            _ => anyhow::bail!("--provider must be openai"),
        }
    }
}

// The SDK's default: two retries on a transient failure.
fn openai_retry() -> RetryOpts {
    RetryOpts { max_retries: 2, base_delay_ms: 500, max_delay_ms: 8_000, jitter: true }
}

pub struct OpenAiBackend {
    api: Arc<dyn JsonApi>,
}

impl OpenAiBackend {
    pub fn new(api: Arc<dyn JsonApi>) -> OpenAiBackend {
        OpenAiBackend { api }
    }

    pub fn from_env() -> anyhow::Result<OpenAiBackend> {
        let key = std::env::var("OPENAI_API_KEY").ok().filter(|k| !k.is_empty());
        let key = key.ok_or_else(|| anyhow::anyhow!("OPENAI_API_KEY missing in env"))?;
        Ok(OpenAiBackend::new(Arc::new(HttpApi::new(OPENAI_BASE_URL, &key))))
    }
}

// One strict json_schema chat completion, parsed.
pub async fn openai_structured(
    api: &dyn JsonApi,
    model: &str,
    name: &str,
    system: &str,
    user: &str,
    schema: &Value,
    retry: &RetryOpts,
) -> anyhow::Result<Value> {
    let body = json!({
        "model": model,
        "messages": [{ "role": "system", "content": system }, { "role": "user", "content": user }],
        "response_format": { "type": "json_schema", "json_schema": { "name": name, "strict": true, "schema": schema } },
    });
    let cancel = CancellationToken::new();
    let res =
        retry_on_transient(retry, &cancel, |_| {}, || api.post("chat/completions", body.clone(), &cancel)).await?;
    let content = res["choices"][0]["message"]["content"].as_str().filter(|c| !c.is_empty());
    let content = content.ok_or_else(|| anyhow::anyhow!("judge returned empty content"))?;
    Ok(serde_json::from_str(content)?)
}

#[async_trait]
impl JudgeBackend for OpenAiBackend {
    async fn complete(&self, req: &JudgeCompletion<'_>) -> anyhow::Result<Value> {
        openai_structured(&*self.api, JUDGE_MODEL, req.name, req.system, req.user, req.schema, &openai_retry()).await
    }
}

// Resolve a provider name to its backend.
pub fn judge_backend(provider: JudgeProvider) -> anyhow::Result<SharedBackend> {
    Ok(match provider {
        JudgeProvider::OpenAi => Arc::new(OpenAiBackend::from_env()?),
    })
}
