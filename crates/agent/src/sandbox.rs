// Dry runs of a workflow on a signal (`try_workflow`). They come back with
// the engine migration's sandbox step (design 01, step 4): a second
// in-memory `Runtime` with the drafts activated and a dry registry. Until
// then `try_workflow` answers that it is unavailable (design 03, decision 9).
use serde_json::{Map, Value};

/// What a dry run answers a write with (kept for the sandbox step).
pub const MOCK_REPLY: &str = r#"{"mock":true,"note":"dry run: not executed","delivered":true}"#;

/// `try_workflow`: unavailable until the sandbox step.
pub fn try_workflow(_args: &Map<String, Value>) -> anyhow::Result<String> {
    Ok(crate::flows::unavailable("try_workflow", "sandbox"))
}

/// A dry run of a signal through the chain: unavailable until the sandbox step.
pub async fn try_signal(
    _engine: &std::sync::Arc<crate::agent::Engine>,
    args: &Map<String, Value>,
    _session: &crate::session::Session,
    _scope: &crate::tracing::Scope,
    _cancel: &tokio_util::sync::CancellationToken,
) -> anyhow::Result<String> {
    try_workflow(args)
}
