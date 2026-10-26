// Port of tracing/tee.test.ts; the OTLP / local read-shape checks after it
// are Rust-only.
use agent::{
    db::{AgentDb, StoredTrace, TraceStore},
    langfuse::{Attr, FinishedSpan, LangfuseTracer, SpanSink, otlp_body},
    local_recorder::local_recorder_tracer,
    tracing::*,
};
use async_trait::async_trait;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

// A fake "Langfuse" tracer that hands out deterministic ids, standing in for
// the backend whose observation ids the local mirror must adopt.
#[derive(Clone)]
struct FakeSpan {
    id: String,
    n: Arc<AtomicUsize>,
}
impl FakeSpan {
    fn next(&self, prefix: &str) -> String {
        format!("lf-{prefix}-{}", self.n.fetch_add(1, Ordering::SeqCst))
    }
}
struct FakeGeneration(String);
impl Generation for FakeGeneration {
    fn id(&self) -> String {
        self.0.clone()
    }
    fn end(&self, _: GenerationEnd) {}
}
impl TraceContext for FakeSpan {
    fn update(&self, _: ContextUpdate) {}
    fn generation(&self, _: GenerationStart) -> Arc<dyn Generation> {
        Arc::new(FakeGeneration(self.next("gen")))
    }
    fn span(&self, _: SpanStart) -> Arc<dyn Span> {
        Arc::new(FakeSpan { id: self.next("span"), n: self.n.clone() })
    }
    fn event(&self, _: EventStart) {}
}
impl Span for FakeSpan {
    fn id(&self) -> String {
        self.id.clone()
    }
    fn end(&self, _: SpanEnd) {}
}
impl Trace for FakeSpan {
    fn id(&self) -> String {
        self.id.clone()
    }
    fn end(&self) {}
}
struct FakePrimary;
#[async_trait]
impl Tracer for FakePrimary {
    fn trace(&self, _: TraceStart) -> Arc<dyn Trace> {
        Arc::new(FakeSpan { id: "lf-trace".into(), n: Arc::new(AtomicUsize::new(0)) })
    }
    async fn shutdown(&self) {}
}

#[derive(Default)]
struct CaptureStore(Mutex<Vec<StoredTrace>>);
impl TraceStore for CaptureStore {
    fn write_trace(&self, t: &StoredTrace) -> anyhow::Result<()> {
        self.0.lock().unwrap().push(t.clone());
        Ok(())
    }
    fn get_trace(
        &self,
        _: &str,
    ) -> anyhow::Result<Option<(agent::trace_model::Trace, Vec<agent::trace_model::Observation>)>> {
        Ok(None)
    }
}

#[test]
fn the_local_mirror_records_each_child_with_the_primarys_observation_id() {
    let store = Arc::new(CaptureStore::default());
    let tracer = tee_tracer(Arc::new(FakePrimary), Arc::new(local_recorder_tracer(store.clone())));
    let trace = tracer.trace(TraceStart {
        id: "ignored".into(),
        name: "signal:telegram".into(),
        kind: SpanKind::Agent,
        ..Default::default()
    });
    let generation =
        trace.generation(GenerationStart { name: "attempt-1".into(), model: "m".into(), ..Default::default() });
    let step = trace.span(SpanStart::new("step[0]:tool", SpanKind::Tool));
    let inner =
        step.generation(GenerationStart { name: "llm_compose:x".into(), model: "m".into(), ..Default::default() });
    generation.end(GenerationEnd { output: Some(json!("{}")), ..Default::default() });
    inner.end(GenerationEnd { output: Some(json!("ok")), ..Default::default() });
    step.end(SpanEnd::output("ok"));
    trace.end();

    let written = store.0.lock().unwrap();
    assert_eq!(written.len(), 1);
    let by_name = |name: &str| written[0].observations.iter().find(|o| o.name == name).unwrap().id.clone();
    // The trace id is the primary's.
    assert_eq!(written[0].id, "lf-trace");
    // Every teed handle's id is what the mirror stored — the primary's, not
    // a local random UUID (the orphan-score bug).
    assert_eq!(generation.id(), by_name("attempt-1"));
    assert_eq!(step.id(), by_name("step[0]:tool"));
    assert_eq!(inner.id(), by_name("llm_compose:x"));
    assert!(by_name("attempt-1").starts_with("lf-gen-"));
    assert!(by_name("step[0]:tool").starts_with("lf-span-"));
    assert!(by_name("llm_compose:x").starts_with("lf-gen-"));
}
#[derive(Default)]
struct Sink(Mutex<Vec<FinishedSpan>>);
impl SpanSink for Sink {
    fn finished(&self, span: FinishedSpan) {
        self.0.lock().unwrap().push(span);
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tee_forces_primary_ids_and_records_nested_spans_usage_and_merged_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(AgentDb::open(dir.path().join("agent.db")).unwrap());
    let sink = Arc::new(Sink::default());
    let tracer =
        tee_tracer(Arc::new(LangfuseTracer::with_sink(sink.clone())), Arc::new(local_recorder_tracer(db.clone())));
    let trace = tracer.trace(TraceStart {
        id: "ignored".into(),
        name: "signal:telegram".into(),
        kind: SpanKind::Agent,
        session_id: Some("telegram:42".into()),
        tags: Some(vec!["telegram".into()]),
        metadata: Some(json!({"skills":["telegram"],"signal_source":"telegram"}).as_object().unwrap().clone()),
        ..Default::default()
    });
    trace.update(ContextUpdate::input("user input"));
    let generation =
        trace.generation(GenerationStart { name: "attempt-1".into(), model: "m".into(), ..Default::default() });
    let span = trace.span(
        SpanStart::new("step[0]:tool", SpanKind::Tool).metadata(json!({"tool":"echo"}).as_object().unwrap().clone()),
    );
    let inner = span.generation(GenerationStart {
        name: "llm_compose:x".into(),
        model: "m".into(),
        metadata: Some(json!({"skill":"digest"}).as_object().unwrap().clone()),
        ..Default::default()
    });
    generation.end(GenerationEnd {
        output: Some(json!("{}")),
        usage: Some(TokenUsage { input: 100, output: 20, total: 120, cached: Some(64) }),
        ..Default::default()
    });
    inner.end(GenerationEnd {
        output: Some(json!("ok")),
        metadata: Some(json!({"judge_node":"compose"}).as_object().unwrap().clone()),
        ..Default::default()
    });
    span.end(SpanEnd::output("ok"));
    trace.update(ContextUpdate::output("answer"));
    trace.end();
    let (stored, obs) = db.get_trace(&trace.id()).unwrap().unwrap();
    assert_eq!(stored.id, trace.id());
    assert_eq!(stored.session_id.as_deref(), Some("telegram:42"));
    assert_eq!(stored.input, "user input");
    assert_eq!(stored.output, "answer");
    for (name, id) in [("attempt-1", generation.id()), ("step[0]:tool", span.id()), ("llm_compose:x", inner.id())] {
        assert_eq!(obs.iter().find(|o| o.name == name).unwrap().id, id);
    }
    let inner_obs = obs.iter().find(|o| o.name == "llm_compose:x").unwrap();
    assert_eq!(inner_obs.parent_observation_id.as_deref(), Some(span.id().as_str()));
    assert_eq!(inner_obs.metadata.as_ref().unwrap()["judge_node"], "compose");
    assert_eq!(obs.iter().find(|o| o.name == "attempt-1").unwrap().usage_details.as_ref().unwrap()["cached"], 64);
    let wire = sink.0.lock().unwrap().clone();
    assert_eq!(wire.len(), 4);
    let root = wire.iter().find(|s| s.parent_span_id.is_none()).unwrap();
    assert_eq!(root.trace_id, stored.id);
    assert!(root.attributes.iter().any(|(k, v)| k == "session.id" && *v == Attr::Str("telegram:42".into())));
    let body = otlp_body(&wire);
    assert_eq!(body["resourceSpans"][0]["scopeSpans"][0]["scope"]["name"], "langfuse-sdk");
    assert_eq!(body["resourceSpans"][0]["scopeSpans"][0]["spans"].as_array().unwrap().len(), 4);
    assert!(body.to_string().contains("langfuse.observation.usage_details"));
    drop(wire);
    tracer.shutdown().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn error_and_retry_event_attributes_match_local_records() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(AgentDb::open(dir.path().join("agent.db")).unwrap());
    let tracer = local_recorder_tracer(db.clone());
    let trace = tracer.trace(TraceStart { id: "error".into(), name: "error".into(), ..Default::default() });
    trace.event(EventStart {
        name: "llm_retry".into(),
        level: Some(Level::Warning),
        metadata: Some(json!({"attempt":1}).as_object().unwrap().clone()),
        ..Default::default()
    });
    let span = trace.span(SpanStart::new("tool", SpanKind::Tool));
    span.end(SpanEnd::error("failure").with_output("[tool error] failure"));
    trace.end();
    let (_, obs) = db.get_trace("error").unwrap().unwrap();
    assert_eq!(obs.iter().find(|o| o.name == "llm_retry").unwrap().level, "WARNING");
    let tool = obs.iter().find(|o| o.name == "tool").unwrap();
    assert_eq!(tool.level, "ERROR");
    assert_eq!(tool.status_message.as_deref(), Some("failure"));
    assert_eq!(tool.output, "[tool error] failure");
}

#[test]
fn continuation_keeps_langfuse_trace_identity_parent_name_and_final_output() {
    let sink = Arc::new(Sink::default());
    let tracer = LangfuseTracer::with_sink(sink.clone());
    let first = tracer.trace(TraceStart {
        name: "signal:telegram".into(),
        session_id: Some("telegram:42".into()),
        ..Default::default()
    });
    let parent = first.span(SpanStart::new("agent_loop", SpanKind::Chain));
    let anchor = parent.anchor().unwrap();
    parent.end(SpanEnd::default());
    first.update(ContextUpdate::output(json!({"resumes":true})));
    first.end();
    let resumed = tracer.trace(TraceStart {
        name: "resume:agent-loop".into(),
        continuation: Some(anchor.clone()),
        ..Default::default()
    });
    resumed.update(ContextUpdate::output("final answer"));
    resumed.end();
    let spans = sink.0.lock().unwrap();
    let segment = spans.iter().find(|s| s.name == "resume:agent-loop").unwrap();
    assert_eq!(segment.trace_id, first.id());
    assert_eq!(segment.parent_span_id.as_deref(), Some(anchor.parent_span_id.as_str()));
    assert!(!segment.attributes.iter().any(|(k, _)| k == "langfuse.trace.name"));
    assert!(segment.attributes.contains(&("langfuse.trace.output".into(), Attr::Str("final answer".into()))));
    assert!(segment.attributes.contains(&("session.id".into(), Attr::Str("telegram:42".into()))));
    assert_eq!(spans.iter().filter(|s| s.attributes.iter().any(|(k, _)| k == "langfuse.trace.name")).count(), 1);
}

#[test]
fn mirror_merges_segments_even_before_the_signal_ends_and_across_connections() {
    for late_signal in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(AgentDb::open(dir.path().join("agent.db")).unwrap());
        let sink = Arc::new(Sink::default());
        let tracer = tee_tracer(Arc::new(LangfuseTracer::with_sink(sink)), Arc::new(local_recorder_tracer(db.clone())));
        let first = tracer.trace(TraceStart {
            id: "signal".into(),
            name: "signal:telegram".into(),
            session_id: Some("telegram:42".into()),
            tags: Some(vec!["telegram".into()]),
            metadata: Some(json!({"activity":"run"}).as_object().unwrap().clone()),
            ..Default::default()
        });
        first.update(ContextUpdate::input("request"));
        let parent = first.span(SpanStart::new("guard", SpanKind::Tool));
        let anchor = parent.anchor().unwrap();
        parent.event(EventStart { name: "card".into(), ..Default::default() });
        parent.end(SpanEnd::default());
        first.update(ContextUpdate::output(json!({"resumes":true})));
        if !late_signal {
            first.end();
        }
        let reopened = Arc::new(AgentDb::open(dir.path().join("agent.db")).unwrap());
        let recorder = local_recorder_tracer(reopened);
        let resumed = recorder.trace(TraceStart {
            name: "resume:agent-loop".into(),
            continuation: Some(anchor.clone()),
            ..Default::default()
        });
        resumed.event(EventStart { name: "consumed".into(), ..Default::default() });
        resumed.update(ContextUpdate::output("final answer"));
        resumed.end();
        // Repeated persistence must not duplicate observations.
        resumed.end();
        if late_signal {
            first.end();
        }
        let (trace, observations) = db.get_trace(&first.id()).unwrap().unwrap();
        assert_eq!(trace.name, "signal:telegram");
        assert_eq!(trace.input, "request");
        assert_eq!(trace.output, "final answer");
        assert_eq!(trace.tags, ["telegram"]);
        assert_eq!(trace.session_id.as_deref(), Some("telegram:42"));
        assert_eq!(trace.metadata.unwrap()["activity"], "run");
        assert_eq!(observations.len(), 5);
        assert_eq!(
            observations.iter().find(|o| o.name == "resume:agent-loop").unwrap().parent_observation_id.as_deref(),
            Some(parent.id().as_str())
        );
        assert!(
            observations
                .iter()
                .all(|o| o.parent_observation_id.as_ref().is_none_or(|p| observations.iter().any(|a| a.id == *p)))
        );
        assert_eq!(db.list_recent(20, None).unwrap().len(), 1);
        let legacy =
            recorder.trace(TraceStart { id: "legacy".into(), name: "resume:guard".into(), ..Default::default() });
        legacy.end();
        assert_eq!(db.list_recent(20, None).unwrap().len(), 2);
    }
}

#[test]
fn a_null_primary_keeps_local_trace_and_observation_anchors() {
    let store = Arc::new(CaptureStore::default());
    let tracer = tee_tracer(Arc::new(NullTracer), Arc::new(local_recorder_tracer(store.clone())));
    let trace = tracer.trace(TraceStart { id: "local".into(), ..Default::default() });
    let span = trace.span(SpanStart::new("loop", SpanKind::Chain));
    assert_eq!(trace.id(), "local");
    assert_eq!(span.anchor().unwrap().trace_id, "local");
    assert!(!span.id().is_empty());
    assert_eq!(span.anchor().unwrap().parent_span_id, span.id());
    trace.end();
    assert_eq!(store.0.lock().unwrap()[0].id, "local");
}

#[test]
fn simultaneous_segments_do_not_lose_each_others_observations() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("agent.db");
    let db = Arc::new(AgentDb::open(&path).unwrap());
    let first = local_recorder_tracer(db.clone()).trace(TraceStart {
        id: "concurrent".into(),
        name: "signal".into(),
        ..Default::default()
    });
    let anchor = first.anchor().unwrap();
    let gate = std::sync::Barrier::new(8);
    std::thread::scope(|threads| {
        for i in 0..8 {
            let (path, anchor, gate) = (&path, &anchor, &gate);
            threads.spawn(move || {
                let recorder = local_recorder_tracer(Arc::new(AgentDb::open(path).unwrap()));
                let segment = recorder.trace(TraceStart {
                    name: format!("resume:{i}"),
                    continuation: Some(anchor.clone()),
                    ..Default::default()
                });
                segment.event(EventStart { name: format!("event:{i}"), ..Default::default() });
                gate.wait();
                segment.end();
            });
        }
    });
    first.update(ContextUpdate::output("answer"));
    first.end();
    let (trace, observations) = db.get_trace("concurrent").unwrap().unwrap();
    assert_eq!(trace.name, "signal");
    assert_eq!(trace.output, "answer");
    assert_eq!(observations.len(), 17);
}
