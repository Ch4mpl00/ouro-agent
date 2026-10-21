// Langfuse adapter. Langfuse ingests OpenTelemetry: every observation is an
// OTel span carrying `langfuse.*` attributes, posted as OTLP/HTTP JSON to
// `${LANGFUSE_BASE_URL}/api/public/otel/v1/traces`. This module produces the
// spans the JS SDK (@langfuse/tracing + @langfuse/otel 5.3) produced for the
// TS agent, attribute for attribute — the judge reads traces back from
// Langfuse and must not tell the agent's runs before and after the port apart:
//
//   - observation type, input, output, level, status message, model, model
//     parameters, usage details: `langfuse.observation.*`, objects as JSON;
//   - metadata flattened to `langfuse.observation.metadata.<key>`, strings as
//     they are, other values as JSON, null and "" left out;
//   - the root span names the trace (`langfuse.trace.name`), its session
//     (`session.id`) and tags (`langfuse.trace.tags`, a string array).
//
// Ids are minted here, OTel-style: the trace id is 16 random bytes, an
// observation id 8, both hex — the ids Langfuse shows and the scores API
// targets. Ended spans are batched and exported every few seconds, like the
// SDK's BatchSpanProcessor; shutdown flushes what is left.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use serde_json::{Value, json};
use tokio::sync::Notify;

use crate::tracing::{
    ContextUpdate, EventStart, Generation, GenerationEnd, GenerationStart, Level, Metadata, Span, SpanEnd, SpanKind,
    SpanStart, Trace, TraceAnchor, TraceContext, TraceStart, Tracer,
};

const SCOPE_NAME: &str = "langfuse-sdk";
const SCOPE_VERSION: &str = "5.3.0";
const MAX_BATCH: usize = 512;
const FLUSH_EVERY: Duration = Duration::from_secs(5);
const EXPORT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, PartialEq)]
pub enum Attr {
    Str(String),
    StrArray(Vec<String>),
}

#[derive(Clone, Debug)]
pub struct FinishedSpan {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub name: String,
    pub start_ns: u128,
    pub end_ns: u128,
    pub attributes: Vec<(String, Attr)>,
}

fn now_ns() -> u128 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or_default()
}

fn random_hex(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    loop {
        for b in &mut buf {
            *b = rand::random();
        }
        // An all-zero id is invalid in OTel.
        if buf.iter().any(|b| *b != 0) {
            return hex::encode(&buf);
        }
    }
}

// @langfuse/tracing's _serialize: a string as it is, null left out,
// anything else as JSON.
fn serialize(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

// The attributes createObservationAttributes(type, attrs) sets for the
// fields an update carries.
#[derive(Default)]
struct ObservationAttrs<'a> {
    input: Option<&'a Value>,
    output: Option<&'a Value>,
    level: Option<&'a str>,
    status_message: Option<&'a str>,
    model: Option<&'a str>,
    model_parameters: Option<&'a Metadata>,
    usage_details: Option<Value>,
    metadata: Option<&'a Metadata>,
}

fn observation_attributes(kind: &str, a: ObservationAttrs<'_>) -> Vec<(String, Attr)> {
    let mut out = vec![("langfuse.observation.type".to_owned(), Attr::Str(kind.to_owned()))];
    let mut put = |key: &str, value: Option<String>| {
        if let Some(v) = value {
            out.push((key.to_owned(), Attr::Str(v)));
        }
    };
    put("langfuse.observation.level", a.level.map(str::to_owned));
    put("langfuse.observation.status_message", a.status_message.map(str::to_owned));
    put("langfuse.observation.input", a.input.and_then(serialize));
    put("langfuse.observation.output", a.output.and_then(serialize));
    put("langfuse.observation.model.name", a.model.map(str::to_owned));
    put("langfuse.observation.usage_details", a.usage_details.as_ref().and_then(serialize));
    put("langfuse.observation.model.parameters", a.model_parameters.map(|p| Value::Object(p.clone()).to_string()));
    if let Some(metadata) = a.metadata {
        for (key, value) in metadata {
            if let Some(serialized) = serialize(value).filter(|s| !s.is_empty()) {
                out.push((format!("langfuse.observation.metadata.{key}"), Attr::Str(serialized)));
            }
        }
    }
    out
}

fn kind_name(kind: SpanKind) -> &'static str {
    match kind {
        SpanKind::Tool => "tool",
        SpanKind::Agent => "agent",
        SpanKind::Chain => "chain",
        SpanKind::Span => "span",
    }
}

// ── export ──────────────────────────────────────────────────────────────────

// Where finished spans go. The HTTP exporter in production; tests collect.
pub trait SpanSink: Send + Sync {
    fn finished(&self, span: FinishedSpan);
}

struct Batcher {
    queue: Mutex<Vec<FinishedSpan>>,
    wake: Notify,
}

impl SpanSink for Batcher {
    fn finished(&self, span: FinishedSpan) {
        let len = {
            let mut q = self.queue.lock().unwrap_or_else(|e| e.into_inner());
            q.push(span);
            q.len()
        };
        if len >= MAX_BATCH {
            self.wake.notify_one();
        }
    }
}

impl Batcher {
    fn take(&self) -> Vec<FinishedSpan> {
        std::mem::take(&mut *self.queue.lock().unwrap_or_else(|e| e.into_inner()))
    }
}

fn attr_json(key: &str, value: &Attr) -> Value {
    let value = match value {
        Attr::Str(s) => json!({ "stringValue": s }),
        Attr::StrArray(items) => {
            json!({ "arrayValue": { "values": items.iter().map(|s| json!({ "stringValue": s })).collect::<Vec<_>>() } })
        }
    };
    json!({ "key": key, "value": value })
}

// One OTLP/HTTP JSON export request.
pub fn otlp_body(spans: &[FinishedSpan]) -> Value {
    let spans: Vec<Value> = spans
        .iter()
        .map(|s| {
            let mut span = json!({
                "traceId": s.trace_id,
                "spanId": s.span_id,
                "name": s.name,
                "kind": 1,
                "startTimeUnixNano": s.start_ns.to_string(),
                "endTimeUnixNano": s.end_ns.to_string(),
                "attributes": s.attributes.iter().map(|(k, v)| attr_json(k, v)).collect::<Vec<_>>(),
                "status": { "code": 0 },
            });
            if let Some(parent) = &s.parent_span_id {
                span["parentSpanId"] = json!(parent);
            }
            span
        })
        .collect();
    json!({
        "resourceSpans": [{
            "resource": { "attributes": [attr_json("service.name", &Attr::Str("ouro-agent".into()))] },
            "scopeSpans": [{ "scope": { "name": SCOPE_NAME, "version": SCOPE_VERSION }, "spans": spans }],
        }]
    })
}

struct HttpExporter {
    client: reqwest::Client,
    url: String,
    authorization: String,
    public_key: String,
}

impl HttpExporter {
    async fn export(&self, spans: Vec<FinishedSpan>) {
        for chunk in spans.chunks(MAX_BATCH) {
            let sent = self
                .client
                .post(&self.url)
                .timeout(EXPORT_TIMEOUT)
                .header("Authorization", &self.authorization)
                .header("x-langfuse-sdk-name", "javascript")
                .header("x-langfuse-sdk-version", SCOPE_VERSION)
                .header("x-langfuse-public-key", &self.public_key)
                .json(&otlp_body(chunk))
                .send()
                .await;
            match sent {
                Ok(res) if res.status().is_success() => {}
                Ok(res) => {
                    let status = res.status();
                    let body = res.text().await.unwrap_or_default();
                    eprintln!(
                        "[langfuse] export of {} spans failed: {status} {}",
                        chunk.len(),
                        body.chars().take(300).collect::<String>()
                    );
                }
                Err(err) => eprintln!("[langfuse] export of {} spans failed: {err}", chunk.len()),
            }
        }
    }
}

// ── handles ─────────────────────────────────────────────────────────────────

struct LfSpan {
    sink: Arc<dyn SpanSink>,
    trace_id: String,
    session_id: Mutex<Option<String>>,
    span_id: String,
    parent: Option<String>,
    name: String,
    kind: &'static str,
    start_ns: u128,
    attributes: Mutex<Vec<(String, Attr)>>,
    ended: AtomicBool,
}

impl LfSpan {
    fn start(
        sink: &Arc<dyn SpanSink>,
        trace_id: &str,
        parent: Option<&str>,
        name: &str,
        kind: &'static str,
        attrs: ObservationAttrs<'_>,
    ) -> LfSpan {
        LfSpan {
            sink: sink.clone(),
            trace_id: trace_id.to_owned(),
            session_id: Mutex::new(None),
            span_id: random_hex(8),
            parent: parent.map(str::to_owned),
            name: name.to_owned(),
            kind,
            start_ns: now_ns(),
            attributes: Mutex::new(observation_attributes(kind, attrs)),
            ended: AtomicBool::new(false),
        }
    }

    // setAttributes: a key set again replaces its value, in place.
    fn set(&self, attrs: Vec<(String, Attr)>) {
        let mut current = self.attributes.lock().unwrap_or_else(|e| e.into_inner());
        for (key, value) in attrs {
            match current.iter_mut().find(|(k, _)| *k == key) {
                Some(slot) => slot.1 = value,
                None => current.push((key, value)),
            }
        }
    }

    fn update(&self, a: ObservationAttrs<'_>) {
        self.set(observation_attributes(self.kind, a));
    }

    // An OTel span is exported once, when it ends; later calls are ignored.
    fn finish(&self) {
        if self.ended.swap(true, Ordering::SeqCst) {
            return;
        }
        let attributes = self.attributes.lock().unwrap_or_else(|e| e.into_inner()).clone();
        self.sink.finished(FinishedSpan {
            trace_id: self.trace_id.clone(),
            span_id: self.span_id.clone(),
            parent_span_id: self.parent.clone(),
            name: self.name.clone(),
            start_ns: self.start_ns,
            end_ns: now_ns(),
            attributes,
        });
    }

    fn anchor(&self) -> TraceAnchor {
        TraceAnchor {
            trace_id: self.trace_id.clone(),
            parent_span_id: self.span_id.clone(),
            session_id: self.session_id.lock().unwrap_or_else(|e| e.into_inner()).clone(),
        }
    }

    fn child_generation(&self, opts: GenerationStart) -> Arc<dyn Generation> {
        Arc::new(LfGeneration(LfSpan::start(
            &self.sink,
            &self.trace_id,
            Some(&self.span_id),
            &opts.name,
            "generation",
            ObservationAttrs {
                input: opts.input.as_ref(),
                model: Some(&opts.model),
                model_parameters: opts.model_parameters.as_ref(),
                metadata: opts.metadata.as_ref(),
                ..ObservationAttrs::default()
            },
        )))
    }

    fn child_span(&self, opts: SpanStart) -> Arc<dyn Span> {
        let child = LfSpan::start(
            &self.sink,
            &self.trace_id,
            Some(&self.span_id),
            &opts.name,
            kind_name(opts.kind),
            ObservationAttrs {
                input: opts.input.as_ref(),
                metadata: opts.metadata.as_ref(),
                ..ObservationAttrs::default()
            },
        );
        *child.session_id.lock().unwrap_or_else(|e| e.into_inner()) = self.anchor().session_id;
        Arc::new(LfChild(child))
    }

    // Events end the moment they start.
    fn child_event(&self, opts: EventStart) {
        LfSpan::start(
            &self.sink,
            &self.trace_id,
            Some(&self.span_id),
            &opts.name,
            "event",
            ObservationAttrs {
                input: opts.input.as_ref(),
                metadata: opts.metadata.as_ref(),
                level: opts.level.map(Level::as_str),
                ..ObservationAttrs::default()
            },
        )
        .finish();
    }

    fn apply(&self, data: &ContextUpdate) {
        if data.input.is_none() && data.output.is_none() && data.metadata.is_none() {
            return;
        }
        self.update(ObservationAttrs {
            input: data.input.as_ref(),
            output: data.output.as_ref(),
            metadata: data.metadata.as_ref(),
            ..ObservationAttrs::default()
        });
    }
}

struct LfGeneration(LfSpan);

impl Generation for LfGeneration {
    fn id(&self) -> String {
        self.0.span_id.clone()
    }

    fn end(&self, o: GenerationEnd) {
        self.0.update(ObservationAttrs {
            output: o.output.as_ref(),
            level: o.level.map(Level::as_str),
            status_message: o.status_message.as_deref(),
            metadata: o.metadata.as_ref(),
            usage_details: o.usage.as_ref().map(|u| u.details()),
            ..ObservationAttrs::default()
        });
        self.0.finish();
    }
}

struct LfChild(LfSpan);

impl TraceContext for LfChild {
    fn anchor(&self) -> Option<TraceAnchor> {
        Some(self.0.anchor())
    }
    fn update(&self, data: ContextUpdate) {
        self.0.apply(&data);
    }
    fn generation(&self, opts: GenerationStart) -> Arc<dyn Generation> {
        self.0.child_generation(opts)
    }
    fn span(&self, opts: SpanStart) -> Arc<dyn Span> {
        self.0.child_span(opts)
    }
    fn event(&self, opts: EventStart) {
        self.0.child_event(opts)
    }
}

impl Span for LfChild {
    fn id(&self) -> String {
        self.0.span_id.clone()
    }

    fn end(&self, o: SpanEnd) {
        if o.output.is_some() || o.level.is_some() || o.status_message.is_some() {
            self.0.update(ObservationAttrs {
                output: o.output.as_ref(),
                level: o.level.map(Level::as_str),
                status_message: o.status_message.as_deref(),
                ..ObservationAttrs::default()
            });
        }
        self.0.finish();
    }
}

struct LfTrace(LfSpan);

impl TraceContext for LfTrace {
    fn anchor(&self) -> Option<TraceAnchor> {
        Some(self.0.anchor())
    }
    fn update(&self, data: ContextUpdate) {
        if let Some(session) = &data.session_id {
            *self.0.session_id.lock().unwrap_or_else(|e| e.into_inner()) = Some(session.clone());
            self.0.set(vec![("session.id".into(), Attr::Str(session.clone()))]);
        }
        let mut trace_attrs = Vec::new();
        for (key, value) in [("input", &data.input), ("output", &data.output)] {
            if let Some(value) = value.as_ref().and_then(serialize) {
                trace_attrs.push((format!("langfuse.trace.{key}"), Attr::Str(value)));
            }
        }
        if data.output.is_some() {
            trace_attrs.push((
                format!("langfuse.observation.metadata.{}", crate::trace_model::TRACE_OUTPUT_META),
                Attr::Str("true".into()),
            ));
        }
        self.0.set(trace_attrs);
        self.0.apply(&data);
    }
    fn generation(&self, opts: GenerationStart) -> Arc<dyn Generation> {
        self.0.child_generation(opts)
    }
    fn span(&self, opts: SpanStart) -> Arc<dyn Span> {
        self.0.child_span(opts)
    }
    fn event(&self, opts: EventStart) {
        self.0.child_event(opts)
    }
}

impl Trace for LfTrace {
    // The OTel trace id — the Langfuse trace id.
    fn id(&self) -> String {
        self.0.trace_id.clone()
    }

    fn end(&self) {
        self.0.finish();
    }
}

// ── tracer ──────────────────────────────────────────────────────────────────

pub struct LangfuseTracer {
    sink: Arc<dyn SpanSink>,
    batcher: Option<Arc<Batcher>>,
    exporter: Option<Arc<HttpExporter>>,
    stop: tokio_util::sync::CancellationToken,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

pub struct LangfuseConfig {
    pub public_key: String,
    pub secret_key: String,
    pub base_url: String,
}

impl LangfuseTracer {
    // From LANGFUSE_PUBLIC_KEY / LANGFUSE_SECRET_KEY (+ LANGFUSE_BASE_URL);
    // None without keys — the caller records locally only.
    pub fn from_env() -> Option<LangfuseConfig> {
        let public_key = std::env::var("LANGFUSE_PUBLIC_KEY").ok().filter(|v| !v.is_empty())?;
        let secret_key = std::env::var("LANGFUSE_SECRET_KEY").ok().filter(|v| !v.is_empty())?;
        let base_url = std::env::var("LANGFUSE_BASE_URL")
            .ok()
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| "https://cloud.langfuse.com".into());
        Some(LangfuseConfig { public_key, secret_key, base_url })
    }

    // Must run inside a tokio runtime: the batch export is a task.
    pub fn new(config: LangfuseConfig) -> LangfuseTracer {
        let batcher = Arc::new(Batcher { queue: Mutex::new(Vec::new()), wake: Notify::new() });
        let credentials =
            base64::engine::general_purpose::STANDARD.encode(format!("{}:{}", config.public_key, config.secret_key));
        let exporter = Arc::new(HttpExporter {
            client: reqwest::Client::new(),
            url: format!("{}/api/public/otel/v1/traces", config.base_url.trim_end_matches('/')),
            authorization: format!("Basic {credentials}"),
            public_key: config.public_key,
        });
        let stop = tokio_util::sync::CancellationToken::new();
        let worker = {
            let (batcher, exporter, stop) = (batcher.clone(), exporter.clone(), stop.clone());
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = stop.cancelled() => return,
                        _ = tokio::time::sleep(FLUSH_EVERY) => {}
                        _ = batcher.wake.notified() => {}
                    }
                    let spans = batcher.take();
                    if !spans.is_empty() {
                        exporter.export(spans).await;
                    }
                }
            })
        };
        LangfuseTracer {
            sink: batcher.clone(),
            batcher: Some(batcher),
            exporter: Some(exporter),
            stop,
            worker: Mutex::new(Some(worker)),
        }
    }

    // Spans go to `sink` instead of Langfuse — for tests of the attributes.
    pub fn with_sink(sink: Arc<dyn SpanSink>) -> LangfuseTracer {
        LangfuseTracer {
            sink,
            batcher: None,
            exporter: None,
            stop: tokio_util::sync::CancellationToken::new(),
            worker: Mutex::new(None),
        }
    }
}

#[async_trait]
impl Tracer for LangfuseTracer {
    fn trace(&self, opts: TraceStart) -> Arc<dyn Trace> {
        let trace_id = opts.continuation.as_ref().map(|a| a.trace_id.clone()).unwrap_or_else(|| random_hex(16));
        let root = LfSpan::start(
            &self.sink,
            &trace_id,
            opts.continuation.as_ref().map(|a| a.parent_span_id.as_str()),
            &opts.name,
            kind_name(opts.kind),
            ObservationAttrs { metadata: opts.metadata.as_ref(), ..ObservationAttrs::default() },
        );
        // A continuation names its observation, never the original trace.
        let mut trace_attrs = Vec::new();
        if opts.continuation.is_none() {
            trace_attrs.push(("langfuse.trace.name".to_owned(), Attr::Str(opts.name.clone())));
        }
        let session_id = opts.session_id.or_else(|| opts.continuation.as_ref().and_then(|a| a.session_id.clone()));
        *root.session_id.lock().unwrap_or_else(|e| e.into_inner()) = session_id.clone();
        if let Some(session) = &session_id {
            trace_attrs.push(("session.id".to_owned(), Attr::Str(session.clone())));
        }
        if let Some(tags) = opts.tags.as_ref().filter(|t| !t.is_empty()) {
            trace_attrs.push(("langfuse.trace.tags".to_owned(), Attr::StrArray(tags.clone())));
        }
        root.set(trace_attrs);
        Arc::new(LfTrace(root))
    }

    // Ships whatever is buffered, then stops the export loop.
    async fn shutdown(&self) {
        self.stop.cancel();
        let worker = self.worker.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(worker) = worker {
            let _ = worker.await;
        }
        if let (Some(batcher), Some(exporter)) = (&self.batcher, &self.exporter) {
            let spans = batcher.take();
            if !spans.is_empty() {
                exporter.export(spans).await;
            }
        }
    }
}
