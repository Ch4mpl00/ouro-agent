// Local-recording tracer. Mirrors the calls the Langfuse adapter receives into
// an in-memory observation tree and writes one row to the trace store on
// trace.end(). The secondary leg of the tee: every run lands in agent.db
// regardless of Langfuse uptime, in the canonical READ shape — what comes back
// out of the store is structurally a Langfuse fetch.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{Map, Value};

use crate::db::{StoredTrace, TraceStore};
use crate::trace_model::{Observation, ObservationType, Usage, resolve_skill};
use crate::tracing::{
    ContextUpdate, EventStart, Generation, GenerationEnd, GenerationStart, Metadata, Span, SpanEnd, SpanKind,
    SpanStart, Trace, TraceAnchor, TraceContext, TraceStart, Tracer,
};

pub fn now_iso() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

fn kind_to_type(kind: SpanKind) -> ObservationType {
    match kind {
        SpanKind::Tool => ObservationType::Tool,
        SpanKind::Agent => ObservationType::Agent,
        SpanKind::Chain => ObservationType::Chain,
        SpanKind::Span => ObservationType::Span,
    }
}

fn merge_meta(base: Option<Map<String, Value>>, patch: Option<Metadata>) -> Option<Map<String, Value>> {
    match patch {
        None => base,
        Some(patch) => {
            let mut out = base.unwrap_or_default();
            out.extend(patch);
            Some(out)
        }
    }
}

// One run's accumulator, shared by the root and every nested handle.
struct RunState {
    id: String,
    name: String,
    session_id: Option<String>,
    tags: Vec<String>,
    metadata: Option<Map<String, Value>>,
    input: Option<Value>,
    output: Option<Value>,
    started_at: String,
    observations: Vec<Observation>,
}

type Shared = Arc<Mutex<RunState>>;

fn lock(state: &Shared) -> std::sync::MutexGuard<'_, RunState> {
    state.lock().unwrap_or_else(|e| e.into_inner())
}

fn new_observation(parent: Option<String>, name: &str, kind: ObservationType) -> Observation {
    let start = now_iso();
    Observation {
        id: uuid::Uuid::new_v4().to_string(),
        name: name.to_owned(),
        kind,
        parent_observation_id: parent,
        start_time: start.clone(),
        end_time: start,
        level: "DEFAULT".into(),
        status_message: None,
        input: Value::Null,
        output: Value::Null,
        metadata: None,
        model: None,
        model_parameters: None,
        usage: None,
        usage_details: None,
        calculated_total_cost: None,
        latency: 0.0,
    }
}

// Edits the observation with `id` in place; the tree is append-only, so it is
// always found.
fn with_obs(state: &Shared, id: &str, f: impl FnOnce(&mut Observation)) {
    let mut s = lock(state);
    if let Some(o) = s.observations.iter_mut().rev().find(|o| o.id == id) {
        f(o);
    }
}

fn push_generation(state: &Shared, parent: &str, opts: GenerationStart) -> Arc<dyn Generation> {
    let mut obs = new_observation(Some(parent.to_owned()), &opts.name, ObservationType::Generation);
    // The forced id (the tee passes the Langfuse observation id) makes judge
    // scores written against this node link back to the right Langfuse step.
    if let Some(id) = opts.id {
        obs.id = id;
    }
    obs.input = opts.input.unwrap_or(Value::Null);
    obs.model = Some(opts.model);
    obs.model_parameters = opts.model_parameters;
    obs.metadata = opts.metadata;
    let id = obs.id.clone();
    lock(state).observations.push(obs);
    Arc::new(LocalGeneration { state: state.clone(), id })
}

fn push_span(state: &Shared, parent: &str, opts: SpanStart) -> Arc<dyn Span> {
    let mut obs = new_observation(Some(parent.to_owned()), &opts.name, kind_to_type(opts.kind));
    if let Some(id) = opts.id {
        obs.id = id;
    }
    obs.input = opts.input.unwrap_or(Value::Null);
    obs.metadata = opts.metadata;
    let id = obs.id.clone();
    lock(state).observations.push(obs);
    Arc::new(LocalSpan { state: state.clone(), id })
}

fn push_event(state: &Shared, parent: &str, opts: EventStart) {
    let mut obs = new_observation(Some(parent.to_owned()), &opts.name, ObservationType::Event);
    obs.input = opts.input.unwrap_or(Value::Null);
    obs.metadata = opts.metadata;
    obs.level = opts.level.map_or("DEFAULT", |l| l.as_str()).to_owned();
    lock(state).observations.push(obs);
}

struct LocalGeneration {
    state: Shared,
    id: String,
}

impl Generation for LocalGeneration {
    fn id(&self) -> String {
        self.id.clone()
    }

    fn end(&self, o: GenerationEnd) {
        with_obs(&self.state, &self.id, |obs| {
            obs.end_time = now_iso();
            if let Some(output) = o.output {
                obs.output = output;
            }
            if let Some(level) = o.level {
                obs.level = level.as_str().to_owned();
            }
            if let Some(message) = o.status_message {
                obs.status_message = Some(message);
            }
            if o.metadata.is_some() {
                obs.metadata = merge_meta(obs.metadata.take(), o.metadata);
            }
            if let Some(usage) = o.usage {
                obs.usage = Some(Usage { input: usage.input, output: usage.output, total: usage.total });
                obs.usage_details = usage.details().as_object().cloned();
            }
        });
    }
}

struct LocalSpan {
    state: Shared,
    id: String,
}

impl TraceContext for LocalSpan {
    fn anchor(&self) -> Option<TraceAnchor> {
        let s = lock(&self.state);
        Some(TraceAnchor { trace_id: s.id.clone(), parent_span_id: self.id.clone(), session_id: s.session_id.clone() })
    }
    fn update(&self, data: ContextUpdate) {
        with_obs(&self.state, &self.id, |obs| {
            if let Some(input) = data.input {
                obs.input = input;
            }
            if let Some(output) = data.output {
                obs.output = output;
            }
            obs.metadata = merge_meta(obs.metadata.take(), data.metadata);
        });
    }

    fn generation(&self, opts: GenerationStart) -> Arc<dyn Generation> {
        push_generation(&self.state, &self.id, opts)
    }

    fn span(&self, opts: SpanStart) -> Arc<dyn Span> {
        push_span(&self.state, &self.id, opts)
    }

    fn event(&self, opts: EventStart) {
        push_event(&self.state, &self.id, opts)
    }
}

impl Span for LocalSpan {
    fn id(&self) -> String {
        self.id.clone()
    }

    fn end(&self, o: SpanEnd) {
        with_obs(&self.state, &self.id, |obs| {
            obs.end_time = now_iso();
            if let Some(output) = o.output {
                obs.output = output;
            }
            if let Some(level) = o.level {
                obs.level = level.as_str().to_owned();
            }
            if let Some(message) = o.status_message {
                obs.status_message = Some(message);
            }
        });
    }
}

struct LocalTrace {
    state: Shared,
    root: String,
    store: Arc<dyn TraceStore>,
    continuation: bool,
}

impl TraceContext for LocalTrace {
    fn anchor(&self) -> Option<TraceAnchor> {
        let s = lock(&self.state);
        Some(TraceAnchor {
            trace_id: s.id.clone(),
            parent_span_id: self.root.clone(),
            session_id: s.session_id.clone(),
        })
    }
    fn update(&self, data: ContextUpdate) {
        with_obs(&self.state, &self.root, |obs| {
            if let Some(input) = &data.input {
                obs.input = input.clone();
            }
            if let Some(output) = &data.output {
                obs.output = output.clone();
                obs.metadata
                    .get_or_insert_default()
                    .insert(crate::trace_model::TRACE_OUTPUT_META.into(), Value::Bool(true));
            }
            obs.metadata = merge_meta(obs.metadata.take(), data.metadata.clone());
        });
        let mut s = lock(&self.state);
        if let Some(session) = data.session_id {
            s.session_id = Some(session);
        }
        if let Some(input) = data.input {
            s.input = Some(input);
        }
        if let Some(output) = data.output {
            s.output = Some(output);
        }
        s.metadata = merge_meta(s.metadata.take(), data.metadata);
    }

    fn generation(&self, opts: GenerationStart) -> Arc<dyn Generation> {
        push_generation(&self.state, &self.root, opts)
    }

    fn span(&self, opts: SpanStart) -> Arc<dyn Span> {
        push_span(&self.state, &self.root, opts)
    }

    fn event(&self, opts: EventStart) {
        push_event(&self.state, &self.root, opts)
    }
}

impl Trace for LocalTrace {
    fn id(&self) -> String {
        lock(&self.state).id.clone()
    }

    fn end(&self) {
        with_obs(&self.state, &self.root, |obs| obs.end_time = now_iso());
        let s = lock(&self.state);
        let stored = StoredTrace {
            id: s.id.clone(),
            name: s.name.clone(),
            source: s.tags.first().cloned(),
            skill: resolve_skill(&s.observations, s.metadata.as_ref()),
            session_id: s.session_id.clone(),
            tags: s.tags.clone(),
            input: s.input.clone(),
            output: s.output.clone(),
            metadata: s.metadata.clone(),
            observations: s.observations.clone(),
            started_at: s.started_at.clone(),
        };
        drop(s);
        if let Err(err) = self.store.write_segment(&stored, self.continuation) {
            eprintln!("[local-recorder] writing trace {} failed: {err:#}", stored.id);
        }
    }
}

pub struct LocalRecorderTracer {
    store: Arc<dyn TraceStore>,
}

pub fn local_recorder_tracer(store: Arc<dyn TraceStore>) -> LocalRecorderTracer {
    LocalRecorderTracer { store }
}

#[async_trait]
impl Tracer for LocalRecorderTracer {
    fn trace(&self, opts: TraceStart) -> Arc<dyn Trace> {
        // The root observation mirrors the trace; the judge skips it (it
        // matches trace.name with a null parent) but it keeps the tree whole.
        let continuation = opts.continuation.is_some();
        let mut root = new_observation(
            opts.continuation.as_ref().map(|a| a.parent_span_id.clone()),
            &opts.name,
            kind_to_type(opts.kind),
        );
        if let Some(id) = opts.root_id {
            root.id = id;
        }
        root.metadata = opts.metadata.clone();
        let root_id = root.id.clone();
        let state = RunState {
            id: opts.continuation.as_ref().map(|a| a.trace_id.clone()).unwrap_or(opts.id),
            name: opts.name,
            session_id: opts.session_id.or_else(|| opts.continuation.and_then(|a| a.session_id)),
            tags: opts.tags.unwrap_or_default(),
            metadata: opts.metadata,
            input: None,
            output: None,
            started_at: now_iso(),
            observations: vec![root],
        };
        Arc::new(LocalTrace {
            state: Arc::new(Mutex::new(state)),
            root: root_id,
            store: self.store.clone(),
            continuation,
        })
    }

    // Writes happen on trace.end() — nothing buffered.
    async fn shutdown(&self) {}
}
