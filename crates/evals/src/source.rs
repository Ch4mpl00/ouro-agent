// Where the judge reads runs from. Two implementations — the local mirror
// (fast, complete, Langfuse-independent) and the Langfuse public API — behind
// one trait so material assembly is source-agnostic. The online worker reads
// local (the forward-looking corpus the self-improvement loop is built on);
// the manual CLI keeps listing from Langfuse.

use std::sync::Arc;

use agent::db::{AgentDb, TraceStore, TraceSummary};
use agent::trace_model::{Observation, Trace};
use async_trait::async_trait;

use crate::langfuse_api::Langfuse;

#[async_trait]
pub trait TraceSource: Send + Sync {
    async fn get_trace(&self, id: &str) -> anyhow::Result<(Trace, Vec<Observation>)>;
    // Newest-first. `unjudged_for` (local only) returns just the runs without
    // a judgement for that (provider, prompt version) — the dedup lives in
    // the query, not a separate KV.
    async fn recent_traces(&self, limit: u32, unjudged_for: Option<(&str, &str)>) -> anyhow::Result<Vec<TraceSummary>>;
}

pub type SharedSource = Arc<dyn TraceSource>;

#[async_trait]
impl TraceSource for Langfuse {
    async fn get_trace(&self, id: &str) -> anyhow::Result<(Trace, Vec<Observation>)> {
        self.trace_by_id(id).await
    }

    async fn recent_traces(&self, limit: u32, _: Option<(&str, &str)>) -> anyhow::Result<Vec<TraceSummary>> {
        Langfuse::recent_traces(self, limit).await
    }
}

// Reads from the local store. Individual segments land on trace.end(); the
// unjudged query excludes requests with live or not-yet-recorded anchored
// runs, so the worker judges the final tree once. Explicit reads still show
// suspended requests, which is useful for inspection.
// `fallback` (e.g. Langfuse) covers get_trace for ids not mirrored locally.
pub struct LocalSource {
    pub db: AgentDb,
    pub fallback: Option<SharedSource>,
}

#[async_trait]
impl TraceSource for LocalSource {
    async fn get_trace(&self, id: &str) -> anyhow::Result<(Trace, Vec<Observation>)> {
        if let Some(local) = self.db.get_trace(id)? {
            return Ok(local);
        }
        match &self.fallback {
            Some(fallback) => fallback.get_trace(id).await,
            None => anyhow::bail!("trace {id} not in local store and no fallback configured"),
        }
    }

    async fn recent_traces(&self, limit: u32, unjudged_for: Option<(&str, &str)>) -> anyhow::Result<Vec<TraceSummary>> {
        self.db.list_recent(limit, unjudged_for)
    }
}

// The local mirror with Langfuse behind it when its credentials are set.
pub fn local_with_langfuse(db: AgentDb) -> anyhow::Result<LocalSource> {
    let fallback: Option<SharedSource> = if Langfuse::enabled() { Some(Arc::new(Langfuse::from_env()?)) } else { None };
    Ok(LocalSource { db, fallback })
}
