// One LLM call as a generation in the trace. One budget covers the request
// and every retry and backoff, rather than restarting on each attempt; the
// generation is closed however the call ends — success, failure, deadline,
// cancellation, or its caller dropping it.

use std::future::Future;
use std::time::Duration;

use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::errors::{aborted, or_cancel};
use crate::providers::{CompletionParams, CompletionResult, SharedProvider};
use crate::tracing::{Generation, GenerationEnd, GenerationStart, Level, Metadata, Scope};
use std::sync::Arc;

pub const DEFAULT_GENERATION_TIMEOUT: Duration = Duration::from_millis(600_000);

// Runs `run` as a generation of `scope`. `describe` turns a success into what
// the generation records; a failure records its message at level ERROR.
pub async fn trace_generation<A, F>(
    scope: &Scope,
    observation: GenerationStart,
    timeout: Option<Duration>,
    cancel: &CancellationToken,
    run: impl FnOnce(CancellationToken) -> F,
    describe: impl FnOnce(&A) -> GenerationEnd,
) -> anyhow::Result<A>
where
    F: Future<Output = anyhow::Result<A>>,
{
    let timeout = timeout.unwrap_or(DEFAULT_GENERATION_TIMEOUT);
    let mut generation = Unfinished(Some(scope.generation(observation)));
    let operation_cancel = cancel.child_token();
    let _cancel_on_exit = operation_cancel.clone().drop_guard();
    let outcome = or_cancel(cancel, async {
        match tokio::time::timeout(timeout, run(operation_cancel)).await {
            Ok(result) => result,
            Err(_) => Err(anyhow::anyhow!("Generation timed out after {}ms", timeout.as_millis())),
        }
    })
    .await;
    let generation = generation.0.take().expect("ended once");
    match &outcome {
        Ok(value) => generation.end(describe(value)),
        Err(err) => {
            let message = if cancel.is_cancelled() { aborted().to_string() } else { err.to_string() };
            generation.end(failure(message));
        }
    }
    outcome
}

fn failure(message: String) -> GenerationEnd {
    GenerationEnd {
        output: Some(json!({ "error": message })),
        level: Some(Level::Error),
        status_message: Some(message),
        ..GenerationEnd::default()
    }
}

// A generation whose caller dropped it mid-call (a durable run released on
// shutdown) still ends, as aborted.
struct Unfinished(Option<Arc<dyn Generation>>);

impl Drop for Unfinished {
    fn drop(&mut self) {
        if let Some(generation) = self.0.take() {
            generation.end(failure(aborted().to_string()));
        }
    }
}

// What a generation of an LLM call is named and tagged with; the model and
// the input (the messages) come from the call itself.
#[derive(Clone, Debug, Default)]
pub struct Observation {
    pub name: String,
    pub model_parameters: Option<Metadata>,
    pub metadata: Option<Metadata>,
}

pub fn messages_json(messages: &[crate::providers::Message]) -> Value {
    Value::Array(messages.iter().cloned().map(Value::Object).collect())
}

// The default record of a completion: the message and the usage.
pub fn describe_completion(result: &CompletionResult) -> GenerationEnd {
    GenerationEnd {
        output: Some(Value::Object(result.message.clone())),
        usage: result.usage.clone(),
        ..GenerationEnd::default()
    }
}

// A provider call as a generation: the input is the messages as they were
// sent, captured before the provider touches them.
pub async fn generation(
    provider: &SharedProvider,
    params: &mut CompletionParams<'_>,
    scope: &Scope,
    observation: Observation,
    timeout: Option<Duration>,
    describe: impl FnOnce(&CompletionResult) -> GenerationEnd,
) -> anyhow::Result<CompletionResult> {
    let start = GenerationStart {
        name: observation.name,
        model: params.model.clone(),
        model_parameters: observation.model_parameters,
        input: Some(messages_json(params.messages)),
        metadata: observation.metadata,
        id: None,
    };
    params.trace = Some(scope.clone());
    let cancel = params.cancel.clone();
    trace_generation(
        scope,
        start,
        timeout,
        &cancel,
        |operation_cancel| async move {
            params.cancel = operation_cancel;
            provider.complete(params).await
        },
        describe,
    )
    .await
}
