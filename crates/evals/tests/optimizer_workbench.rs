use agent::{
    agent::Providers,
    models::default_presets,
    providers::{ChatProvider, CompletionParams, CompletionResult, ProviderKind, assistant},
    skills::{FsSkillStore, SkillStore},
    tracing::{SpanKind, SpanStart, TokenUsage, null_scope},
};
use async_trait::async_trait;
use evals::optimizer::*;
use owf::run::{CallInfo, Effect, Functions, WorkflowError};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;

const HEADER: &str = "document: { dsl: '1.0.4', namespace: tests, name: candidate, version: '1.0.0' }\n";
fn yaml(body: &str) -> String {
    format!("{HEADER}{body}")
}
fn scenario(body: &str) -> Scenario {
    Scenario { yaml: yaml(body), input: json!({}), mocks: vec![], label: None }
}
fn mock(name: &str, args: Value, value: Value) -> Mock {
    Mock {
        function: name.into(),
        arguments: args,
        response: MockResponse::Result { value },
        provenance: Provenance::Synthetic,
        note: None,
    }
}
fn capability(name: &str, effect: Effect) -> Capability {
    Capability { name: name.into(), effect, parameters: None, provenance: "test adapter contract".into() }
}
#[derive(Default)]
struct Adapter {
    calls: AtomicUsize,
    active: AtomicUsize,
    peak: AtomicUsize,
}
#[async_trait]
impl Functions for Adapter {
    fn effect(&self, name: &str) -> Option<Effect> {
        match name {
            "read" | "llm_compose" | "agent_turn" => Some(Effect::Read),
            "write" => Some(Effect::Write),
            _ => None,
        }
    }
    async fn call(&self, name: &str, args: Value, _: &CallInfo<'_>) -> Result<Value, WorkflowError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_ne!(name, "write", "write adapter must never execute");
        let n = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(n, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(if args["i"] == 0 { 12 } else { 2 })).await;
        self.active.fetch_sub(1, Ordering::SeqCst);
        Ok(if name == "llm_compose" { json!({"answer":self.calls.load(Ordering::SeqCst)}) } else { args })
    }
}
fn bench(adapter: Arc<Adapter>, allowed: &[&str], evidence: Evidence) -> Workbench {
    Workbench::new(
        adapter,
        OfflineCatalog(vec![capability("read", Effect::Read), capability("write", Effect::Write)]),
        evidence,
        allowed.iter().map(|s| s.to_string()).collect(),
        json!({"model":"fake","effort":"high"}),
    )
    .unwrap()
}
#[tokio::test]
async fn exact_values_canonical_keys_and_fixture_precedence() {
    let adapter = Arc::new(Adapter::default());
    let bench = bench(adapter.clone(), &["read"], Evidence::default());
    let mut s = scenario("do:\n  - a:\n      call: read\n      with: {b: [2, 3], a: 1}\n");
    s.mocks.push(mock("read", json!({"a":1,"b":[2,3]}), json!("mock")));
    let r = bench.run(&s, &CancellationToken::new()).await.unwrap();
    assert_eq!(r.output, Some(json!("mock")));
    assert_eq!(r.calls[0].mode, "mocked");
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 0);
    assert_eq!(canonical(&json!({"x":{"b":2,"a":1}})), canonical(&json!({"x":{"a":1,"b":2}})));
    let strict = Workbench::new(
        adapter.clone(),
        OfflineCatalog(vec![capability("read", Effect::Read)]),
        Evidence::default(),
        BTreeSet::new(),
        json!({}),
    )
    .unwrap();
    for different in [json!({"a":"1","b":[2,3]}), json!({"a":1,"b":[3,2]}), json!({"a":1,"b":[2,3],"c":null})] {
        let mut s = s.clone();
        s.mocks[0].arguments = different;
        let r = strict.run(&s, &CancellationToken::new()).await.unwrap();
        assert_eq!(r.status, "needs_mock");
        assert_eq!(r.calls[0].mode, "missing");
    }
}
#[tokio::test]
async fn missing_mock_escapes_catch_but_injected_error_is_catchable_and_no_live_fallback() {
    let adapter = Arc::new(Adapter::default());
    let bench = bench(adapter.clone(), &[], Evidence::default());
    let mut s = scenario(
        "do:\n  - attempt:\n      try:\n        - send:\n            call: write\n            with: {amount: 5}\n      catch:\n        do:\n          - fallback: {set: {status: caught}}\n  - after: {set: {status: swallowed}}\n",
    );
    let r = bench.run(&s, &CancellationToken::new()).await.unwrap();
    assert_eq!(r.status, "needs_mock");
    assert_eq!(r.calls[0].arguments, json!({"amount":5}));
    assert!(r.output.is_none());
    assert_eq!(r.writes.len(), 1);
    assert!(
        !r.path.iter().any(|p| p["task"].as_str().is_some_and(|t| t.ends_with("after") || t.ends_with("fallback")))
    );
    s.mocks.push(Mock {
        function: "write".into(),
        arguments: json!({"amount":5}),
        response: MockResponse::Error { error: WorkflowError::communication(403, "refused") },
        provenance: Provenance::Synthetic,
        note: None,
    });
    let r = bench.run(&s, &CancellationToken::new()).await.unwrap();
    assert_eq!(r.status, "completed");
    assert_eq!(r.output, Some(json!({"status":"swallowed"})));
    assert!(r.calls[0].error.is_some());
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 0);
    assert!(
        Workbench::new(
            adapter,
            OfflineCatalog::default(),
            Evidence::default(),
            BTreeSet::from(["write".into()]),
            json!({})
        )
        .is_err()
    );
}
#[tokio::test]
async fn real_cache_survives_fixture_changes_but_never_execution_state_or_candidate_changes() {
    let adapter = Arc::new(Adapter::default());
    let bench = bench(adapter.clone(), &["llm_compose"], Evidence::default());
    let mut s = scenario(
        "do:\n  - model:\n      call: llm_compose\n      with: {prompt: parse}\n      export: {as: '${ $context + { parsed: . } }'}\n  - write:\n      call: write\n      with: {value: '${ $context.parsed.answer }'}\n",
    );
    let r = bench.run(&s, &CancellationToken::new()).await.unwrap();
    assert_eq!(r.status, "needs_mock");
    assert!(r.cache_reset);
    let write_args = r.calls[1].arguments.clone();
    s.mocks.push(mock("write", write_args, json!({"saved":true})));
    let r = bench.run(&s, &CancellationToken::new()).await.unwrap();
    assert_eq!(r.status, "completed");
    assert!(!r.cache_reset);
    assert_eq!(r.calls[0].mode, "cached");
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);
    s.mocks.push(mock("llm_compose", json!({"prompt":"parse"}), json!({"answer":99})));
    let r = bench.run(&s, &CancellationToken::new()).await.unwrap();
    assert_eq!(r.status, "needs_mock");
    assert_eq!(r.calls[0].mode, "mocked");
    assert_eq!(r.calls[1].arguments, json!({"value":99}));
    s.mocks.pop();
    s.input = json!({"changed":true});
    let r = bench.run(&s, &CancellationToken::new()).await.unwrap();
    assert!(r.cache_reset);
    assert_eq!(r.calls[0].mode, "real");
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 2);
    s.yaml.push_str("# changed candidate\n");
    let r = bench.run(&s, &CancellationToken::new()).await.unwrap();
    assert!(r.cache_reset);
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 3);
}
#[tokio::test]
async fn parallel_calls_are_independent_bounded_and_match_values_not_arrival_order() {
    let adapter = Arc::new(Adapter::default());
    let bench = bench(adapter.clone(), &["read"], Evidence::default());
    let mut s = scenario(
        "do:\n  - items:\n      for: {each: item, in: '${ [0,1,2,3,4,5] }'}\n      metadata: {concurrency: 3}\n      do:\n        - get:\n            call: read\n            with: {i: '${ $item }'}\n",
    );
    s.mocks.push(mock("read", json!({"i":2}), json!({"i":"mock two"})));
    let r = bench.run(&s, &CancellationToken::new()).await.unwrap();
    assert_eq!(r.status, "completed");
    assert_eq!(r.output, Some(json!([{"i":0},{"i":1},{"i":"mock two"},{"i":3},{"i":4},{"i":5}])));
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 5);
    assert_eq!(adapter.peak.load(Ordering::SeqCst), 3);
}
#[tokio::test]
async fn rejected_context_unknown_function_and_duplicate_fixtures_do_not_execute() {
    let adapter = Arc::new(Adapter::default());
    let bench = bench(adapter.clone(), &[], Evidence::default());
    for body in [
        "do:\n  - turn: {call: agent_turn, with: {}}\n",
        "do:\n  - call: {call: invented, with: {}}\n",
        "do:\n  - wait: {listen: {to: {one: {with: {type: hello}}}}}\n",
    ] {
        assert!(bench.run(&scenario(body), &CancellationToken::new()).await.is_err());
    }
    let mut s = scenario("do:\n  - get: {call: read, with: {}}\n");
    s.mocks = vec![mock("read", json!({}), json!(1)), mock("read", json!({}), json!(2))];
    assert!(bench.run(&s, &CancellationToken::new()).await.unwrap_err().to_string().contains("duplicate fixture"));
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 0);
}

fn observation(id: &str, output: Value, error: bool) -> agent::trace_model::Observation {
    evals::langfuse_api::observation(json!({"id":id,"name":"write","type":"TOOL","startTime":"2026-10-22T00:00:00Z","endTime":"2026-10-22T00:00:01Z","level":if error {"ERROR"} else {"DEFAULT"},"input":{"i":id},"output":output,"metadata":{"tool":"write"},"latency":1})).unwrap()
}
fn evidence(observations: Vec<agent::trace_model::Observation>) -> Evidence {
    let trace = evals::langfuse_api::trace_from(&json!({"id":"sample"}), vec![]);
    let mut evidence = Evidence::default();
    evidence.add(&trace, &observations);
    evidence
}
#[tokio::test]
async fn full_recorded_refs_preserve_json_wire_strings_and_errors_without_previews() {
    let long = "full payload ".repeat(1000);
    let evidence = evidence(vec![
        observation("json", json!({"body":long,"cached":true}), false),
        observation("wire", json!(json!({"body":long}).to_string()), false),
        observation("string", json!("a literal string"), false),
        observation("error", json!("[tool error] forbidden"), true),
    ]);
    assert_eq!(evidence.read("trace:sample/observation:json/output").unwrap()["body"], long);
    let bench = bench(Arc::new(Adapter::default()), &[], evidence);
    for (id, wire, expected_status, expected) in [
        ("json", true, "completed", json!({"body":long,"cached":true})),
        ("wire", true, "completed", json!({"body":long})),
        ("wire", false, "completed", json!(json!({"body":long}).to_string())),
        ("string", true, "completed", json!("a literal string")),
        ("error", true, "faulted", Value::Null),
    ] {
        let mut s = scenario("do:\n  - send: {call: write, with: {}}\n");
        s.mocks.push(Mock {
            function: "write".into(),
            arguments: json!({}),
            response: MockResponse::Recorded { reference: format!("trace:sample/observation:{id}/output"), wire },
            provenance: Provenance::Recorded,
            note: None,
        });
        let r = bench.run(&s, &CancellationToken::new()).await.unwrap();
        assert_eq!(r.status, expected_status);
        if expected_status == "completed" {
            assert_eq!(r.output, Some(expected));
        } else {
            assert!(r.calls[0].error.as_ref().unwrap().detail.as_ref().unwrap().contains("forbidden"));
        }
    }
}

// The concrete 20-item mixed pipeline from the motivating shape, with entirely
// synthetic URLs and article bodies. No private trace contents are committed.
const PIPELINE: &str = r#"do:
  - read:
      try:
        - get: {call: read, with: {url: feed}}
      catch:
        do:
          - failed: {set: {status: read_error}, then: end}
  - parse:
      try:
        - model: {call: llm_compose, with: {prompt: parse, input: {feed: '${ .content }'}, json: true}}
      catch:
        do:
          - failed: {set: {status: parse_error}, then: end}
  - validate:
      switch:
        - malformed: {when: '${ (.items | type) != "array" }', then: bad_parse}
        - valid: {then: save}
  - bad_parse: {set: {status: parse_error}, then: end}
  - save:
      for: {each: item, in: '${ .items }'}
      metadata: {concurrency: 16}
      do:
        - one:
            try:
              - store: {call: write, with: {url: '${ $item }'}}
              - ok: {set: '${ {ok:true,cached:.cached} }'}
            catch:
              do:
                - failed: {set: {ok: false}}
  - summarize:
      set: '${ { total:length, saved:([.[]|select(.ok)]|length), cached:([.[]|select(.cached)]|length) } | . + { status: (if .total == 0 then "empty" elif .saved == 0 then "all_failed" elif .saved < .total then "partial" else "success" end) } }'
"#;
#[tokio::test]
async fn twenty_recorded_responses_partial_all_failed_empty_parse_and_read_failure() {
    let observations = (0..20)
        .map(|i| {
            observation(
                &i.to_string(),
                if i < 18 {
                    json!({"title":format!("Article {i}"),"cached":true,"body":"full body".repeat(1000)})
                } else {
                    json!("[tool error] source blocked")
                },
                i >= 18,
            )
        })
        .collect();
    let bench = bench(Arc::new(Adapter::default()), &[], evidence(observations));
    let mut s = scenario(PIPELINE);
    s.mocks.push(mock("read", json!({"url":"feed"}), json!({"content":"synthetic feed"})));
    let parse_args = json!({"prompt":"parse","input":{"feed":"synthetic feed"},"json":true});
    s.mocks.push(mock(
        "llm_compose",
        parse_args.clone(),
        json!({"items":(0..20).map(|i|format!("https://example.test/{i}")).collect::<Vec<_>>()}),
    ));
    for i in 0..20 {
        s.mocks.push(Mock {
            function: "write".into(),
            arguments: json!({"url":format!("https://example.test/{i}")}),
            response: MockResponse::Recorded { reference: format!("trace:sample/observation:{i}/output"), wire: true },
            provenance: Provenance::Recorded,
            note: None,
        });
    }
    let r = bench.run(&s, &CancellationToken::new()).await.unwrap();
    assert_eq!(r.output, Some(json!({"total":20,"saved":18,"cached":18,"status":"partial"})));
    assert_eq!(r.writes.len(), 20);
    for m in &mut s.mocks[2..] {
        m.response = MockResponse::Error { error: WorkflowError::communication(403, "blocked") };
    }
    assert_eq!(bench.run(&s, &CancellationToken::new()).await.unwrap().output.unwrap()["status"], "all_failed");
    s.mocks[1] = mock("llm_compose", parse_args.clone(), json!({"items":[]}));
    assert_eq!(bench.run(&s, &CancellationToken::new()).await.unwrap().output.unwrap()["status"], "empty");
    s.mocks[1] = mock("llm_compose", parse_args, json!({"items":null}));
    assert_eq!(bench.run(&s, &CancellationToken::new()).await.unwrap().output.unwrap()["status"], "parse_error");
    s.mocks[0].response = MockResponse::Error { error: WorkflowError::communication(503, "unavailable") };
    let r = bench.run(&s, &CancellationToken::new()).await.unwrap();
    assert_eq!(r.output.unwrap()["status"], "read_error");
    assert!(r.writes.is_empty());
}

struct Scripted {
    turn: Mutex<usize>,
    candidate: String,
}

#[tokio::test]
async fn private_optimizer_skill_validates_without_registering_eval_tools_in_production() {
    let dir = tempfile::tempdir().unwrap();
    let defaults = dir.path().join("skills.default");
    std::fs::create_dir(&defaults).unwrap();
    std::fs::write(
        defaults.join("workflow-optimizer.md"),
        include_str!("../../../skills.default/workflow-optimizer.md"),
    )
    .unwrap();
    let store = FsSkillStore::new(dir.path());
    store.validate_all(&["production_read".into()]).await.unwrap();
    assert!(!store.read_skill("workflow-optimizer").await.unwrap().unwrap().routable);
}

fn tool(name: &str, args: Value) -> agent::providers::Message {
    json!({"role":"assistant","content":null,"tool_calls":[{"id":"call","type":"function","function":{"name":name,"arguments":args.to_string()}}]}).as_object().unwrap().clone()
}
#[async_trait]
impl ChatProvider for Scripted {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Openai
    }
    async fn complete(&self, p: &mut CompletionParams<'_>) -> anyhow::Result<CompletionResult> {
        for name in ["optimizer_capabilities", "optimizer_evidence", "optimizer_try", "working_memory_get"] {
            assert!(p.tools.iter().any(|t| t.name == name), "tool not admitted: {name}");
        }
        let mut turn = self.turn.lock().unwrap();
        let message = match *turn {
            0 => tool("optimizer_capabilities", json!({})),
            1 => tool("optimizer_try", json!({"yaml":self.candidate,"input":{},"mocks":[]})),
            2 => {
                let result = p.messages.last().unwrap()["content"].as_str().unwrap();
                assert!(result.contains("needs_mock"), "{result}");
                tool(
                    "optimizer_try",
                    json!({"yaml":self.candidate,"input":{},"mocks":[{"function":"write","arguments":{"value":7},"response":{"kind":"result","value":{"saved":true}},"provenance":"synthetic"}]}),
                )
            }
            3 => {
                assert!(p.messages.last().unwrap()["content"].as_str().unwrap().contains("completed"));
                tool("optimizer_evidence", json!({"reference":"trial:2/call:0/output"}))
            }
            4 => {
                assert!(p.messages.last().unwrap()["content"].as_str().unwrap().contains("saved"));
                assistant(&format!("Tested trial:2.\n```yaml\n{}```", self.candidate))
            }
            _ => anyhow::bail!("unexpected model call"),
        };
        *turn += 1;
        Ok(CompletionResult {
            message,
            finish_reason: Some("stop".into()),
            usage: Some(TokenUsage { input: 10, output: 5, total: 15, cached: None }),
        })
    }
}
#[tokio::test]
async fn existing_agent_loop_inspects_missing_mock_adds_fixture_and_reruns() {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let skills = Arc::new(FsSkillStore::new(&root));
    let candidate = yaml("do:\n  - send: {call: write, with: {value: 7}}\n");
    let provider: Arc<dyn ChatProvider> = Arc::new(Scripted { turn: Mutex::new(0), candidate: candidate.clone() });
    let tools = Arc::new(OptimizerTools::new(
        Arc::new(bench(Arc::new(Adapter::default()), &[], Evidence::default())),
        Evidence::default(),
    ));
    let engine = isolated_engine(
        Providers {
            openai: provider.clone(),
            deepseek: provider.clone(),
            gemini: provider.clone(),
            openrouter: Some(provider),
        },
        default_presets(),
        skills,
        tools.clone(),
    )
    .unwrap();
    let prompt = workbench_prompt("author", "optimizer", &candidate, &[json!({"reference":"sample"})]).unwrap();
    let defaults = IterativeOptions::default();
    assert_eq!(defaults.max_iterations, 30);
    assert!(defaults.timeout.is_none());
    let error = optimize_iterative(
        &engine,
        &prompt,
        &IterativeOptions { max_iterations: 31, timeout: None },
        CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("between 1 and 30"));
    let report = optimize_iterative(&engine, &prompt, &defaults, CancellationToken::new()).await.unwrap();
    assert_eq!(proposal_yaml(&report).unwrap(), candidate);
    let trials = tools.trials();
    assert_eq!(trials.len(), 2);
    assert_eq!(trials[0].report["status"], "needs_mock");
    assert_eq!(trials[1].report["status"], "completed");
    engine.shutdown().await;
}

#[tokio::test]
async fn real_compose_reuses_existing_function_adapter_and_fake_provider() {
    struct Compose;
    #[async_trait]
    impl ChatProvider for Compose {
        fn kind(&self) -> ProviderKind {
            ProviderKind::Openai
        }
        async fn complete(&self, p: &mut CompletionParams<'_>) -> anyhow::Result<CompletionResult> {
            assert!(p.tools.is_empty());
            assert_eq!(p.response_format, Some(json!({"type":"json_object"})));
            assert!(p.messages.last().unwrap()["content"].as_str().unwrap().contains("Reply with one JSON object"));
            Ok(CompletionResult {
                message: assistant("{\"items\":[1,2]}"),
                finish_reason: Some("stop".into()),
                usage: None,
            })
        }
    }
    let provider: Arc<dyn ChatProvider> = Arc::new(Compose);
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let engine = isolated_engine(
        Providers {
            deepseek: provider.clone(),
            openai: provider.clone(),
            gemini: provider.clone(),
            openrouter: Some(provider),
        },
        default_presets(),
        Arc::new(FsSkillStore::new(&root)),
        Arc::new(OfflineCatalog::default()),
    )
    .unwrap();
    let functions =
        agent::flows::standalone_host(&engine, &null_scope().span(SpanStart::new("test", SpanKind::Chain))).functions;
    let bench = Workbench::new(
        functions,
        OfflineCatalog::default(),
        Evidence::default(),
        BTreeSet::from(["llm_compose".into()]),
        json!({"model":"fake"}),
    )
    .unwrap();
    let s = scenario("do:\n  - parse: {call: llm_compose, with: {prompt: parse, json: true}}\n");
    let r = bench.run(&s, &CancellationToken::new()).await.unwrap();
    assert_eq!(r.output, Some(json!({"items":[1,2]})));
    assert_eq!(r.calls[0].mode, "real");
    engine.shutdown().await;
}

#[tokio::test]
async fn dynamic_write_effect_is_never_permitted_real_and_cancelled_calls_stay_reported() {
    struct Dynamic;
    #[async_trait]
    impl Functions for Dynamic {
        fn effect(&self, _: &str) -> Option<Effect> {
            Some(Effect::Read)
        }
        fn effect_of(&self, _: &str, args: &Value) -> Option<Effect> {
            Some(if args["write"] == true { Effect::Write } else { Effect::Read })
        }
        async fn call(&self, _: &str, args: Value, _: &CallInfo<'_>) -> Result<Value, WorkflowError> {
            assert_ne!(args["write"], true, "dynamic write must not execute");
            tokio::time::sleep(Duration::from_secs(60)).await;
            Ok(json!({}))
        }
    }
    let mut bench = Workbench::new(
        Arc::new(Dynamic),
        OfflineCatalog(vec![capability("dynamic", Effect::Read)]),
        Evidence::default(),
        BTreeSet::from(["dynamic".into()]),
        json!({}),
    )
    .unwrap();
    let s = scenario("do:\n  - call: {call: dynamic, with: {write: true}}\n");
    let r = bench.run(&s, &CancellationToken::new()).await.unwrap();
    assert_eq!(r.status, "needs_mock");
    assert_eq!(r.writes.len(), 1);
    bench.timeout = Duration::from_millis(20);
    let s = scenario("do:\n  - call: {call: dynamic, with: {write: false}}\n");
    let r = bench.run(&s, &CancellationToken::new()).await.unwrap();
    assert_eq!(r.status, "timed_out");
    assert_eq!(r.calls.len(), 1);
    assert_eq!(r.calls[0].mode, "real");
    assert!(r.calls[0].error.as_ref().unwrap().detail.as_ref().unwrap().contains("interrupted"));
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(bench.run(&s, &cancel).await.is_err());
}

#[tokio::test]
async fn provider_meter_tracks_large_cached_usage_without_stopping_execution() {
    struct Usage;
    #[async_trait]
    impl ChatProvider for Usage {
        fn kind(&self) -> ProviderKind {
            ProviderKind::Openai
        }
        async fn complete(&self, _: &mut CompletionParams<'_>) -> anyhow::Result<CompletionResult> {
            Ok(CompletionResult {
                message: assistant("{\"ok\":true}"),
                finish_reason: Some("stop".into()),
                usage: Some(TokenUsage { input: 1_100_000, output: 4, total: 1_100_004, cached: Some(1_000_000) }),
            })
        }
    }
    let meter = Arc::new(Mutex::new(Meter::default()));
    let provider: Arc<dyn ChatProvider> = Arc::new(MeteredProvider { provider: Arc::new(Usage), meter: meter.clone() });
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let engine = isolated_engine(
        Providers {
            deepseek: provider.clone(),
            openai: provider.clone(),
            gemini: provider.clone(),
            openrouter: Some(provider),
        },
        default_presets(),
        Arc::new(FsSkillStore::new(&root)),
        Arc::new(OfflineCatalog::default()),
    )
    .unwrap();
    let adapter =
        agent::flows::standalone_host(&engine, &null_scope().span(SpanStart::new("meter", SpanKind::Chain))).functions;
    let mut bench = Workbench::new(
        adapter,
        OfflineCatalog::default(),
        Evidence::default(),
        BTreeSet::from(["llm_compose".into()]),
        json!({"model":"fake"}),
    )
    .unwrap();
    bench.meter = Some(meter.clone());
    let mut s = scenario("do:\n  - compose: {call: llm_compose, with: {prompt: first, json: true}}\n");
    let r = bench.run(&s, &CancellationToken::new()).await.unwrap();
    let usage = r.usage.unwrap();
    assert_eq!(usage.calls, 1);
    assert_eq!(usage.input, 1_100_000);
    assert_eq!(usage.output, 4);
    assert_eq!(usage.cached, 1_000_000);
    let r = bench.run(&s, &CancellationToken::new()).await.unwrap();
    assert_eq!(r.calls[0].mode, "cached");
    assert_eq!(r.usage.unwrap().calls, 0);
    s.yaml.push_str("# fresh execution cache\n");
    let r = bench.run(&s, &CancellationToken::new()).await.unwrap();
    assert_eq!(r.status, "completed");
    assert_eq!(r.calls[0].mode, "real");
    let totals = meter.lock().unwrap().clone();
    assert_eq!(totals.calls, 2);
    assert_eq!(totals.input, 2_200_000);
    assert_eq!(totals.cached, 2_000_000);
    engine.shutdown().await;
}

#[tokio::test]
async fn candidate_compose_uses_actual_preset_family() {
    struct Family(&'static str);
    #[async_trait]
    impl ChatProvider for Family {
        fn kind(&self) -> ProviderKind {
            ProviderKind::Openai
        }
        async fn complete(&self, p: &mut CompletionParams<'_>) -> anyhow::Result<CompletionResult> {
            assert!(p.tools.is_empty());
            Ok(CompletionResult {
                message: assistant(&json!({"family":self.0,"model":p.model}).to_string()),
                finish_reason: Some("stop".into()),
                usage: None,
            })
        }
    }
    let mut presets = default_presets();
    presets.base.model = "openrouter/test/base".into();
    presets.compose.model = "gpt-compose".into();
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let engine = isolated_engine(
        Providers {
            deepseek: Arc::new(Family("deepseek")),
            openai: Arc::new(Family("openai")),
            gemini: Arc::new(Family("gemini")),
            openrouter: Some(Arc::new(Family("openrouter"))),
        },
        presets,
        Arc::new(FsSkillStore::new(&root)),
        Arc::new(OfflineCatalog::default()),
    )
    .unwrap();
    let adapter =
        agent::flows::standalone_host(&engine, &null_scope().span(SpanStart::new("presets", SpanKind::Chain)))
            .functions;
    let bench = Workbench::new(
        adapter,
        OfflineCatalog::default(),
        Evidence::default(),
        BTreeSet::from(["llm_compose".into()]),
        json!({"settings":"test"}),
    )
    .unwrap();
    for (preset, family, model) in
        [("base", "openrouter", "openrouter/test/base"), ("compose", "openai", "gpt-compose")]
    {
        let s = scenario(&format!(
            "do:\n  - compose: {{call: llm_compose, with: {{preset: {preset}, prompt: test, json: true}}}}\n"
        ));
        let r = bench.run(&s, &CancellationToken::new()).await.unwrap();
        assert_eq!(r.output, Some(json!({"family":family,"model":model})));
    }
    engine.shutdown().await;
    // The prior iterative-loop test uses a workbench with an empty real
    // allow-list and an independently injected optimizer provider. Its model
    // still sees all local tools and completes the fixture repair loop.
}

#[tokio::test]
async fn competing_branch_missing_mock_cannot_leave_a_success_output() {
    // The winner may finish after the losing branch has already discovered a
    // missing fixture. The report must not mislabel that incomplete scenario.
    let adapter = Arc::new(Adapter::default());
    let bench = bench(adapter, &["read"], Evidence::default());
    let s = scenario(
        "do:\n  - race:\n      fork:\n        compete: true\n        branches:\n          - missing: {call: write, with: {}}\n          - slow: {call: read, with: {i: 0}}\n",
    );
    let r = bench.run(&s, &CancellationToken::new()).await.unwrap();
    assert_eq!(r.status, "needs_mock");
    assert!(r.output.is_none());
    assert!(r.error.is_some());
}
