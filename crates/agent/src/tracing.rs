// Provider-agnostic tracing interfaces. The agent loop, the workflow and the
// supervisor speak ONLY these types — concrete backends (Langfuse, the local
// recorder) live behind them. Replacing the backend means adding an adapter
// and swapping it in the composition root (main.rs).
//
// Hierarchy: a `TraceContext` is anything that can host nested children
// (generations, spans, events). The root of a run is a Trace; spans can host
// children too, so a sub-agent's iterations render inside the parent's
// `invoke_sub_agent` span.
//
// `None` in an optional field means "not given" (JS `undefined`), as opposed
// to `Some(Value::Null)`: the backends treat the two differently.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

pub type Metadata = Map<String, Value>;

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct TokenUsage {
    pub input: u64,
    pub output: u64,
    pub total: u64,
    // Portion of `input` served from the provider's prompt cache, when the
    // provider reports it. Informational — surfaced so cache hits show.
    pub cached: Option<u64>,
}

impl TokenUsage {
    // The usageDetails shape both backends record.
    pub fn details(&self) -> Value {
        let mut out = Map::new();
        out.insert("input".into(), self.input.into());
        out.insert("output".into(), self.output.into());
        out.insert("total".into(), self.total.into());
        if let Some(cached) = self.cached {
            out.insert("cached".into(), cached.into());
        }
        Value::Object(out)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Level {
    Warning,
    Error,
}

impl Level {
    pub fn as_str(self) -> &'static str {
        match self {
            Level::Warning => "WARNING",
            Level::Error => "ERROR",
        }
    }
}

// How a span renders in the tracing UI; purely presentational.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SpanKind {
    Tool,
    Agent,
    Chain,
    #[default]
    Span,
}

#[derive(Clone, Debug, Default)]
pub struct SpanEnd {
    pub output: Option<Value>,
    pub level: Option<Level>,
    pub status_message: Option<String>,
}

impl SpanEnd {
    pub fn output(output: impl Into<Value>) -> Self {
        SpanEnd { output: Some(output.into()), ..SpanEnd::default() }
    }

    // level ERROR + statusMessage, the shape every failure ends with.
    pub fn error(message: impl Into<String>) -> Self {
        SpanEnd { level: Some(Level::Error), status_message: Some(message.into()), ..SpanEnd::default() }
    }

    pub fn with_output(mut self, output: impl Into<Value>) -> Self {
        self.output = Some(output.into());
        self
    }
}

#[derive(Clone, Debug, Default)]
pub struct GenerationEnd {
    pub output: Option<Value>,
    pub level: Option<Level>,
    pub status_message: Option<String>,
    pub usage: Option<TokenUsage>,
    // Known only at end time, merged over what the start set.
    pub metadata: Option<Metadata>,
}

#[derive(Clone, Debug, Default)]
pub struct ContextUpdate {
    pub session_id: Option<String>,
    pub input: Option<Value>,
    pub output: Option<Value>,
    pub metadata: Option<Metadata>,
}

impl ContextUpdate {
    pub fn input(input: impl Into<Value>) -> Self {
        ContextUpdate { input: Some(input.into()), ..ContextUpdate::default() }
    }

    pub fn output(output: impl Into<Value>) -> Self {
        ContextUpdate { output: Some(output.into()), ..ContextUpdate::default() }
    }

    pub fn metadata(metadata: Metadata) -> Self {
        ContextUpdate { metadata: Some(metadata), ..ContextUpdate::default() }
    }
}

#[derive(Clone, Debug, Default)]
pub struct GenerationStart {
    pub name: String,
    pub model: String,
    // Forced observation id (the tee passes the primary backend's id so the
    // mirror shares it). Backends that own their ids ignore it.
    pub id: Option<String>,
    pub model_parameters: Option<Metadata>,
    pub input: Option<Value>,
    pub metadata: Option<Metadata>,
}

#[derive(Clone, Debug, Default)]
pub struct SpanStart {
    pub name: String,
    pub input: Option<Value>,
    pub metadata: Option<Metadata>,
    pub kind: SpanKind,
    pub id: Option<String>,
}

impl SpanStart {
    pub fn new(name: impl Into<String>, kind: SpanKind) -> Self {
        SpanStart { name: name.into(), kind, ..SpanStart::default() }
    }

    pub fn input(mut self, input: impl Into<Value>) -> Self {
        self.input = Some(input.into());
        self
    }

    pub fn metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = Some(metadata);
        self
    }
}

// A point-in-time marker: no duration, no children, nothing to close.
#[derive(Clone, Debug, Default)]
pub struct EventStart {
    pub name: String,
    pub input: Option<Value>,
    pub metadata: Option<Metadata>,
    pub level: Option<Level>,
}

// Host-owned address of a scope, persisted with durable runs rather than
// exposed in their expression context. The trace id also addresses the mirror.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceAnchor {
    pub trace_id: String,
    pub parent_span_id: String,
    pub session_id: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct TraceStart {
    pub continuation: Option<TraceAnchor>,
    // Forced root observation id for the secondary leg of a tee.
    pub root_id: Option<String>,
    pub id: String,
    pub name: String,
    pub session_id: Option<String>,
    pub tags: Option<Vec<String>>,
    pub metadata: Option<Metadata>,
    pub kind: SpanKind,
}

pub trait Generation: Send + Sync {
    // The backend's observation id (Langfuse: the OTel span id).
    fn id(&self) -> String;
    fn end(&self, opts: GenerationEnd);
}

pub trait TraceContext: Send + Sync {
    fn anchor(&self) -> Option<TraceAnchor> {
        None
    }
    fn update(&self, data: ContextUpdate);
    fn generation(&self, opts: GenerationStart) -> Arc<dyn Generation>;
    fn span(&self, opts: SpanStart) -> Arc<dyn Span>;
    fn event(&self, opts: EventStart);
}

pub trait Span: TraceContext {
    fn id(&self) -> String;
    fn end(&self, opts: SpanEnd);
}

// The run-level root. `end` closes the root span: OTel backends keep a trace
// "in progress" until it is.
pub trait Trace: TraceContext {
    // The backend's trace id, known at creation.
    fn id(&self) -> String;
    fn end(&self);
}

#[async_trait]
pub trait Tracer: Send + Sync {
    fn trace(&self, opts: TraceStart) -> Arc<dyn Trace>;
    // Flush buffered spans. Called once on shutdown.
    async fn shutdown(&self);
}

pub type Scope = Arc<dyn TraceContext>;

// ── null tracer ─────────────────────────────────────────────────────────────

struct Noop;

impl Generation for Noop {
    fn id(&self) -> String {
        String::new()
    }
    fn end(&self, _: GenerationEnd) {}
}

impl TraceContext for Noop {
    fn update(&self, _: ContextUpdate) {}
    fn generation(&self, _: GenerationStart) -> Arc<dyn Generation> {
        Arc::new(Noop)
    }
    fn span(&self, _: SpanStart) -> Arc<dyn Span> {
        Arc::new(Noop)
    }
    fn event(&self, _: EventStart) {}
}

impl Span for Noop {
    fn id(&self) -> String {
        String::new()
    }
    fn end(&self, _: SpanEnd) {}
}

impl Trace for Noop {
    fn id(&self) -> String {
        String::new()
    }
    fn end(&self) {}
}

// Used when external tracing is off: concrete no-op objects, so call sites
// never null-check.
pub struct NullTracer;

#[async_trait]
impl Tracer for NullTracer {
    fn trace(&self, _: TraceStart) -> Arc<dyn Trace> {
        Arc::new(Noop)
    }
    async fn shutdown(&self) {}
}

pub fn null_scope() -> Scope {
    Arc::new(Noop)
}

// ── tee ─────────────────────────────────────────────────────────────────────

// Fans one tracer's calls out to two backends, like unix `tee`: every run is
// mirrored into the local store while it still ships to Langfuse. The trace
// id comes from the PRIMARY and is forced onto the secondary, and so are the
// children's ids — the mirror's observation ids are the Langfuse ones, so a
// judge score written later links back to the right step.

struct TeeGeneration(Arc<dyn Generation>, Arc<dyn Generation>);

impl Generation for TeeGeneration {
    fn id(&self) -> String {
        let id = self.0.id();
        if id.is_empty() { self.1.id() } else { id }
    }
    fn end(&self, opts: GenerationEnd) {
        self.0.end(opts.clone());
        self.1.end(opts);
    }
}

fn fork_generation(a: &dyn TraceContext, b: &dyn TraceContext, opts: GenerationStart) -> Arc<dyn Generation> {
    let ga = a.generation(opts.clone());
    let gb = b.generation(GenerationStart { id: Some(ga.id()).filter(|id| !id.is_empty()), ..opts });
    Arc::new(TeeGeneration(ga, gb))
}

fn fork_span(a: &dyn TraceContext, b: &dyn TraceContext, opts: SpanStart) -> Arc<dyn Span> {
    let sa = a.span(opts.clone());
    let sb = b.span(SpanStart { id: Some(sa.id()).filter(|id| !id.is_empty()), ..opts });
    Arc::new(TeeSpan(sa, sb))
}

struct TeeSpan(Arc<dyn Span>, Arc<dyn Span>);

impl TraceContext for TeeSpan {
    fn anchor(&self) -> Option<TraceAnchor> {
        self.0.anchor().or_else(|| self.1.anchor())
    }
    fn update(&self, data: ContextUpdate) {
        self.0.update(data.clone());
        self.1.update(data);
    }
    fn generation(&self, opts: GenerationStart) -> Arc<dyn Generation> {
        fork_generation(self.0.as_ref(), self.1.as_ref(), opts)
    }
    fn span(&self, opts: SpanStart) -> Arc<dyn Span> {
        fork_span(self.0.as_ref(), self.1.as_ref(), opts)
    }
    fn event(&self, opts: EventStart) {
        self.0.event(opts.clone());
        self.1.event(opts);
    }
}

impl Span for TeeSpan {
    fn id(&self) -> String {
        let id = self.0.id();
        if id.is_empty() { self.1.id() } else { id }
    }
    fn end(&self, opts: SpanEnd) {
        self.0.end(opts.clone());
        self.1.end(opts);
    }
}

struct TeeTrace(Arc<dyn Trace>, Arc<dyn Trace>);

impl TraceContext for TeeTrace {
    fn anchor(&self) -> Option<TraceAnchor> {
        self.0.anchor().or_else(|| self.1.anchor())
    }
    fn update(&self, data: ContextUpdate) {
        self.0.update(data.clone());
        self.1.update(data);
    }
    fn generation(&self, opts: GenerationStart) -> Arc<dyn Generation> {
        fork_generation(self.0.as_ref(), self.1.as_ref(), opts)
    }
    fn span(&self, opts: SpanStart) -> Arc<dyn Span> {
        fork_span(self.0.as_ref(), self.1.as_ref(), opts)
    }
    fn event(&self, opts: EventStart) {
        self.0.event(opts.clone());
        self.1.event(opts);
    }
}

impl Trace for TeeTrace {
    fn id(&self) -> String {
        let id = self.0.id();
        if id.is_empty() { self.1.id() } else { id }
    }
    fn end(&self) {
        self.0.end();
        self.1.end();
    }
}

pub struct TeeTracer {
    primary: Arc<dyn Tracer>,
    secondary: Arc<dyn Tracer>,
}

pub fn tee_tracer(primary: Arc<dyn Tracer>, secondary: Arc<dyn Tracer>) -> TeeTracer {
    TeeTracer { primary, secondary }
}

#[async_trait]
impl Tracer for TeeTracer {
    fn trace(&self, opts: TraceStart) -> Arc<dyn Trace> {
        let a = self.primary.trace(opts.clone());
        let id = a.id();
        let b = self.secondary.trace(TraceStart {
            id: if id.is_empty() { opts.id.clone() } else { id },
            root_id: a.anchor().map(|a| a.parent_span_id),
            ..opts
        });
        Arc::new(TeeTrace(a, b))
    }
    async fn shutdown(&self) {
        futures::join!(self.primary.shutdown(), self.secondary.shutdown());
    }
}
