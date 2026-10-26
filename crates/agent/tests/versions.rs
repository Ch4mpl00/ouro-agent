// The agent's workflow versions and the breaker on the engine
// (docs/designs/04-versions-and-breaker.md): the design's tests 1–20, one
// section each, on the real engine store. The engine is the version store;
// the agent keeps notes by content (`workflow_meta`) and starts every name it
// may have overridden through one decision (runs.rs §10).
mod support;
use agent::{
    agent::{AGENT_LOOP, Engine, LoopOptions},
    flows::{self, WorkflowMeta},
    providers::assistant,
    runs::{Choice, Site},
    supervisor::{PendingSignal, SIGNAL, create_supervisor},
};
use ouro_runtime::durable::{SavedRun, digest_of};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use support::*;

const SKILLS: &[(&str, &str)] = &[
    ("worker", "---\ntools: *\n---\nWORKER"),
    ("scheduler", "---\ntools: *\n---\nSCHEDULER"),
    ("recovery", "---\ntools: []\n---\nRECOVERY"),
    ("peer", "---\ntools: *\n---\nPEER"),
    ("telegram", "---\ntools: *\n---\nTELEGRAM"),
];

async fn tool(engine: &Engine, name: &str, args: Value) -> anyhow::Result<Value> {
    let reply = flows::tool(engine, name, args.as_object().unwrap()).await?;
    Ok(serde_json::from_str(&reply).unwrap())
}

async fn save(engine: &Engine, name: &str, definition: &str) -> u64 {
    let saved = tool(engine, "save_workflow", json!({"name": name, "definition": definition})).await;
    saved.unwrap_or_else(|e| panic!("saving {name}: {e:#}"))["version"].as_u64().unwrap()
}

async fn activate(engine: &Engine, name: &str, version: u64) -> anyhow::Result<Value> {
    tool(engine, "activate_workflow", json!({"name": name, "version": version})).await
}

async fn ready(provider: Arc<dyn agent::providers::ChatProvider>, tools: &[&str]) -> Fixture {
    let f = fixture(provider, tools, SKILLS);
    f.engine.ready().await.unwrap();
    f
}

fn signal(id: i64, source: &str, content: &str) -> PendingSignal {
    PendingSignal {
        id,
        source: source.into(),
        content: content.into(),
        env_context: None,
        created_at: "2026-09-06 12:00:00".into(),
        payload: None,
    }
}

fn telegram(id: i64, text: &str) -> PendingSignal {
    signal(id, "telegram", &format!("Telegram message in chat 42.\nText: {}", json!(text)))
}

fn scheduled(id: i64, workflow: &str) -> PendingSignal {
    PendingSignal { payload: Some(json!({"workflow": workflow})), ..signal(id, "scheduler", "do the task") }
}

// The runs started under `run` (its owned children), ended ones too, as
// the runtime reads them (the control API leaves host data out).
async fn children(engine: &Engine, run: &str) -> Vec<SavedRun> {
    let filter = agent::runs::Runs { parent: Some(run.into()), ..Default::default() };
    let mut ids: Vec<String> =
        engine.control.list(&filter, None, 200).await.unwrap().items.into_iter().map(|r| r.admission.id).collect();
    let window = agent::runs::Runs { since_ms: Some(0), before_ms: Some(i64::MAX / 4), ..filter };
    for ended in engine.control.history(&window, None, 200).await.unwrap().items {
        if !ids.contains(&ended.admission.id) {
            ids.push(ended.admission.id);
        }
    }
    let mut runs = Vec::new();
    for id in ids {
        runs.push(engine.runtime.get(&id).await.unwrap().unwrap());
    }
    runs
}

// The child of `run` started under the key ending in `key`.
async fn child(engine: &Engine, run: &str, key: &str) -> SavedRun {
    let all = children(engine, run).await;
    let found = all.iter().find(|r| r.child.as_ref().is_some_and(|l| l.key.ends_with(&format!("/{key}"))));
    found.unwrap_or_else(|| panic!("no child {key} of {run}")).clone()
}

fn system_of(seen: &Seen) -> String {
    seen.messages[0]["content"].as_str().unwrap().to_owned()
}

async fn builtin(engine: &Engine, name: &str) -> u64 {
    flows::builtin_version(&engine.runtime, name).await.unwrap().unwrap()
}

async fn send(engine: &Arc<Engine>, id: &str) -> anyhow::Result<String> {
    let mut opts = LoopOptions::new(id, context());
    opts.skills = vec!["worker".into()];
    engine.start_loop(opts).await.unwrap().send("go".into()).await
}

// ── 1. save → draft; activation needs a dry run; retired derived ────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_saved_version_is_a_draft_until_tried_and_activated_and_the_previous_one_retires() {
    let f = ready(Arc::new(Scripted::new(vec![])), &[]).await;
    let first = variant("research-loop");
    let saved = tool(&f.engine, "save_workflow", json!({"name":"research-loop","definition":first,"note":"first"}))
        .await
        .unwrap();
    assert_eq!((saved["version"].as_u64(), saved["status"].as_str()), (Some(1), Some("draft")));
    let untried = activate(&f.engine, "research-loop", 1).await.unwrap_err().to_string();
    assert!(untried.contains("has not completed a dry run"), "{untried}");
    mark_tried(&f.engine, "research-loop", &first);
    activate(&f.engine, "research-loop", 1).await.unwrap();
    let read = tool(&f.engine, "read_workflow", json!({"name":"research-loop"})).await.unwrap();
    assert_eq!((read["version"].as_u64(), read["status"].as_str()), (Some(1), Some("active")));
    assert_eq!((read["tried"].as_bool(), read["note"].as_str()), (Some(true), Some("first")));

    let second = first.replace("General ReAct loop", "Research loop");
    assert_eq!(save(&f.engine, "research-loop", &second).await, 2);
    let draft = tool(&f.engine, "read_workflow", json!({"name":"research-loop","version":2})).await.unwrap();
    assert_eq!(draft["status"], "draft");
    mark_tried(&f.engine, "research-loop", &second);
    activate(&f.engine, "research-loop", 2).await.unwrap();
    let old = tool(&f.engine, "read_workflow", json!({"name":"research-loop","version":1})).await.unwrap();
    assert_eq!(old["status"], "retired", "derived from the engine's activations");
    let listed = tool(&f.engine, "list_workflows", json!({})).await.unwrap();
    let entry = listed["workflows"].as_array().unwrap().iter().find(|w| w["name"] == "research-loop").unwrap();
    assert_eq!((entry["latest_version"].as_u64(), entry["active_version"].as_u64()), (Some(2), Some(2)));
    assert_eq!((entry["status"].as_str(), entry["tried"].as_bool()), (Some("active"), Some(true)));
}

// ── 2. the gate, at save and at activation ──────────────────────────────────

fn document(name: &str, rest: &str) -> String {
    format!("document: {{ dsl: '1.0.4', namespace: agent, name: {name}, version: '1.0.0' }}\n{rest}")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_gate_admits_built_in_copies_and_refuses_what_agent_versions_cannot_declare() {
    let f = ready(Arc::new(Scripted::new(vec![])), &["t_write"]).await;
    // Verbatim built-ins minus their breaker policy save under their names.
    save(&f.engine, "agent-loop", &without_breaker(AGENT_LOOP)).await;
    save(&f.engine, "signal", &without_breaker(SIGNAL)).await;
    // The engine's own start_child is allowed.
    let starter = document("starter", "do: [ { c: { call: start_child, with: { name: other, input: {} } } } ]\n");
    save(&f.engine, "starter", &starter).await;

    let refused = |name: &'static str, definition: String, why: &'static str| (name, definition, why);
    let cases = [
        refused("guard", agent::flows::GUARD.to_owned(), "reserved"),
        refused("emitter", document("emitter", "do: [ { e: { emit: { event: { with: { type: x, source: y } } } } } ]\n"), "emit"),
        refused(
            "runner",
            document("runner", "do: [ { r: { run: { workflow: { namespace: agent, name: other, version: '1.0.0' } } } } ]\n"),
            "run: workflow",
        ),
        refused("every", document("every", "schedule: { every: PT1H }\ndo: [ { a: { set: { ok: true } } } ]\n"), "schedule"),
        refused("cron", document("cron", "schedule: { cron: '0 * * * *' }\ndo: [ { a: { set: { ok: true } } } ]\n"), "schedule"),
        refused(
            "on",
            document("on", "schedule: { on: { one: { with: { type: tick } } } }\ndo: [ { a: { set: { ok: true } } } ]\n"),
            "schedule",
        ),
        refused(
            "at",
            "document: { dsl: '1.0.4', namespace: agent, name: at, version: '1.0.0', metadata: { ouro: { at: '2030-01-01T00:00:00Z' } } }\ndo: [ { a: { set: { ok: true } } } ]\n".into(),
            "schedule",
        ),
        refused(
            "breaker",
            "document: { dsl: '1.0.4', namespace: agent, name: breaker, version: '1.0.0', metadata: { ouro: { breaker: { threshold: 9 } } } }\ndo: [ { a: { set: { ok: true } } } ]\n".into(),
            "remove `metadata.ouro.breaker`",
        ),
        refused("agent-loop", AGENT_LOOP.to_owned(), "remove `metadata.ouro.breaker`"),
        refused(
            "signal",
            without_breaker(SIGNAL).replace("version: '1.0.0' }", "version: '1.0.0', metadata: { route: { when: { source: peer } } } }"),
            "cannot route to itself",
        ),
        refused(
            "signal",
            without_breaker(SIGNAL).replace("version: '1.0.0' }", "version: '1.0.0', metadata: { loop: { description: x } } }"),
            "not a loop",
        ),
    ];
    for (name, definition, why) in &cases {
        let error = tool(&f.engine, "save_workflow", json!({"name": name, "definition": definition}))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains(why), "{name}: {error}");
    }
    // What the engine itself stores (an operator's, older rules') is gated
    // again at activation, and a refused activation arms no trigger.
    // (The built-in's own text is how an override is undone: never gated.)
    for (name, definition, why) in
        cases.iter().filter(|(name, d, _)| !["guard", "emitter"].contains(name) && *d != AGENT_LOOP)
    {
        let version = f.engine.runtime.save(name, definition).await.unwrap().version;
        WorkflowMeta::record(&f.engine.deps.db, name, &digest_of(definition), None, false).unwrap();
        mark_tried(&f.engine, name, definition);
        let error = activate(&f.engine, name, version).await.unwrap_err().to_string();
        assert!(error.contains(why), "{name} at activation: {error}");
        let definitions = f.engine.runtime.definitions(name).await.unwrap();
        assert!(definitions.triggers.is_empty(), "{name}: no trigger armed");
        assert_ne!(definitions.active, Some(version), "{name} not activated");
    }
    let guard = activate(&f.engine, "guard", 1).await.unwrap_err().to_string();
    assert!(guard.contains("reserved"), "{guard}");
}

// ── 3. an agent-loop override trips; the built-in runs in its place ─────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tripped_agent_loop_override_gives_way_to_the_built_in_with_its_line() {
    let model =
        Arc::new(Scripted::new(vec![assistant("fine"), assistant("from the built-in"), assistant("fine again")]));
    let f = ready(model.clone(), &[]).await;
    let supervisor = create_supervisor(f.engine.clone());
    let broken = faulting(&without_breaker(AGENT_LOOP));
    let version = activate_definition(&f.engine, "agent-loop", &broken).await;
    for n in 1..=3 {
        let error = send(&f.engine, &format!("loop-{n}")).await.unwrap_err().to_string();
        assert!(error.contains("boom"), "{error}");
    }
    let definitions = f.engine.runtime.definitions("agent-loop").await.unwrap();
    assert!(definitions.tripped.as_ref().is_some_and(|t| t.breaker), "three faults in a row trip it");

    // The next loop runs the built-in, pinned, under its own id and trace.
    assert_eq!(send(&f.engine, "loop-4").await.unwrap(), "fine");
    let run = f.engine.runtime.get("loop-4").await.unwrap().unwrap();
    assert_eq!(
        (run.admission.name.as_str(), run.admission.version),
        ("agent-loop", builtin(&f.engine, "agent-loop").await)
    );
    assert!(agent::runs::ran_of(&run).pinned);
    let fallback = agent::runs::ran_of(&run).fallback.expect("its fallback record");
    assert_eq!(
        (fallback.name.as_str(), fallback.version, fallback.reason),
        ("agent-loop", Some(version), agent::runs::FallbackReason::Tripped)
    );
    let line = format!("Workflow `agent-loop` v{version} was set aside (its breaker tripped after 3 faults in a row");
    assert!(system_of(&model.seen.lock().unwrap()[0]).contains(&line), "the line in its instructions");
    let (trace, observations) = agent::db::TraceStore::get_trace(&*f.db, "loop-4").unwrap().unwrap();
    assert_eq!(trace.metadata.as_ref().unwrap()["fallback"]["reason"], "tripped");
    assert!(observations.iter().any(|o| o.name == "fallback"));
    assert_eq!(f.engine.runtime.definitions("agent-loop").await.unwrap().active, Some(version), "it stays active");

    // A signal's primary loop runs the built-in too, with the line.
    let out = supervisor.run_signal(&signal(31, "scheduler", "do it")).await.unwrap();
    assert_eq!(out, "from the built-in");
    let primary = child(&f.engine, "signal:31", "primary").await;
    assert_eq!(primary.admission.version, builtin(&f.engine, "agent-loop").await);
    assert!(system_of(&model.seen.lock().unwrap()[1]).contains(&line));

    // Activating a new version clears the trip and starts a fresh count.
    let louder = without_breaker(AGENT_LOOP).replace("output: .content", r#"output: ("v2 " + .content)"#);
    activate_definition(&f.engine, "agent-loop", &louder).await;
    assert!(f.engine.runtime.definitions("agent-loop").await.unwrap().tripped.is_none());
    assert_eq!(send(&f.engine, "loop-5").await.unwrap(), "v2 fine again");
    supervisor.shutdown().await;
}

// ── 4. a signal override and a worker's loop variant trip the same way ──────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tripped_signal_override_gives_way_to_the_built_in_and_its_line_reaches_the_primary() {
    let model = Arc::new(Scripted::new(vec![assistant("handled")]));
    let f = ready(model.clone(), &[]).await;
    let supervisor = create_supervisor(f.engine.clone());
    let version = activate_definition(&f.engine, "signal", &faulting(&without_breaker(SIGNAL))).await;
    for id in 41..=43 {
        let error = supervisor.run_signal(&signal(id, "peer", "hello")).await.unwrap_err().to_string();
        assert!(error.contains("boom"), "{error}");
    }
    assert_eq!(supervisor.run_signal(&signal(44, "peer", "hello")).await.unwrap(), "handled");
    let run = f.engine.runtime.get("signal:44").await.unwrap().unwrap();
    assert_eq!(run.admission.version, builtin(&f.engine, "signal").await);
    let line = format!("Workflow `signal` v{version} was set aside (its breaker tripped after 3 faults in a row");
    assert!(run.admission.input["signal"]["fallback"].as_str().unwrap().contains(&line));
    assert!(system_of(&model.seen.lock().unwrap()[0]).contains(&line), "the primary hears it");
    let (trace, _) = agent::db::TraceStore::get_trace(&*f.db, "peer:44").unwrap().unwrap();
    assert_eq!(trace.metadata.as_ref().unwrap()["fallback"]["name"], "signal");
    supervisor.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tripped_loop_variant_gives_its_worker_to_the_built_in_agent_loop() {
    let delegate = |n: u32| calls(&[("invoke_sub_agent", json!({"prompt": format!("r{n}"), "loop": "research-loop"}))]);
    let model = Arc::new(Scripted::new(vec![
        delegate(1),
        delegate(2),
        delegate(3),
        delegate(4),
        assistant("worker ok"),
        assistant("done"),
    ]));
    let f = ready(model.clone(), &[]).await;
    let version = activate_definition(&f.engine, "research-loop", &faulting(&variant("research-loop"))).await;
    assert_eq!(send(&f.engine, "parent").await.unwrap(), "done");
    let worker = child(&f.engine, "parent", "worker:4.0").await;
    assert_eq!(
        (worker.admission.name.as_str(), worker.admission.version),
        ("agent-loop", builtin(&f.engine, "agent-loop").await)
    );
    let fallback = agent::runs::ran_of(&worker).fallback.unwrap();
    assert_eq!((fallback.name.as_str(), fallback.reason), ("research-loop", agent::runs::FallbackReason::Tripped));
    let worker_system = system_of(&model.seen.lock().unwrap()[4]);
    let line =
        format!("Workflow `research-loop` v{version} was set aside (its breaker tripped after 3 faults in a row");
    assert!(worker_system.contains(&line), "the worker hears it: {worker_system}");
    for n in 1..=3 {
        let crashed = child(&f.engine, "parent", &format!("worker:{n}.0")).await;
        assert_eq!(crashed.admission.name, "research-loop");
    }
}

// ── 5. a set-aside route gives way to the next match, then the default path ─

fn route(name: &str, when: &str) -> String {
    variant(name).replace(
        "metadata: { loop: { description: 'General ReAct loop with guarded tools and workers.' } }",
        &format!("metadata: {{ route: {{ when: {{ {when} }} }} }}"),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_set_aside_route_gives_way_to_the_next_match_then_to_the_default_path() {
    let mut script = vec![assistant("reported"), assistant("reported"), assistant("reported")];
    script.push(assistant("general answer"));
    script.extend([assistant("reported"), assistant("reported"), assistant("reported")]);
    script.push(assistant("default answer"));
    let model = Arc::new(Scripted::new(script));
    let f = ready(model.clone(), &[]).await;
    let supervisor = create_supervisor(f.engine.clone());
    let research = |id: i64| PendingSignal { payload: Some(json!({"topic":"research"})), ..telegram(id, "hi") };
    let specific = activate_definition(
        &f.engine,
        "tg-specific",
        &faulting(&route("tg-specific", "source: telegram, topic: research")),
    )
    .await;
    activate_definition(&f.engine, "tg-general", &route("tg-general", "source: telegram")).await;
    for id in 51..=53 {
        assert!(supervisor.run_signal(&research(id)).await.is_err(), "the specific route faults");
    }
    assert_eq!(supervisor.run_signal(&research(54)).await.unwrap(), "general answer");
    assert_eq!(child(&f.engine, "signal:54", "route").await.admission.name, "tg-general");
    let (_, observations) = agent::db::TraceStore::get_trace(&*f.db, "telegram:54").unwrap().unwrap();
    assert!(observations.iter().any(|o| o.name == "route:tg-general"));
    assert!(observations.iter().any(|o| o.name == "fallback"), "the set-aside route is recorded");

    activate_definition(&f.engine, "tg-general", &faulting(&route("tg-general", "source: telegram"))).await;
    for id in 55..=57 {
        assert!(supervisor.run_signal(&telegram(id, "hi")).await.is_err());
    }
    assert_eq!(supervisor.run_signal(&research(58)).await.unwrap(), "default answer");
    let primary = child(&f.engine, "signal:58", "primary").await;
    assert_eq!(primary.admission.name, "agent-loop", "the default path's loop");
    let system = system_of(model.seen.lock().unwrap().last().unwrap());
    let line = format!("Workflow `tg-specific` v{specific} was set aside (its breaker tripped after 3 faults in a row");
    assert!(system.contains(&line) && system.contains("the default path handles this signal"), "{system}");
    assert!(system.contains("Signal source: telegram"), "the default path gathered its context");
    supervisor.shutdown().await;
}

// ── 6. a scheduled saved workflow: missing, tripped, faulted after a write ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_saved_workflow_missing_or_tripped_hands_over_and_one_that_wrote_goes_to_recovery() {
    let mut script: Vec<_> = (0..5).map(|n| assistant(&format!("did it {n}"))).collect();
    script.push(assistant("reported"));
    let model = Arc::new(Scripted::new(script));
    let f = ready(model.clone(), &["t_write"]).await;
    let supervisor = create_supervisor(f.engine.clone());
    let last_system = || system_of(model.seen.lock().unwrap().last().unwrap());

    assert_eq!(supervisor.run_signal(&scheduled(61, "nightly")).await.unwrap(), "did it 0");
    assert!(last_system().contains("Workflow `nightly` was set aside (it has no active version)"), "{}", last_system());
    assert!(last_system().contains("Do the task yourself."));

    let nightly = faulting(&document("nightly", "do: [ { a: { set: { ok: true } } } ]\n"));
    let version = activate_definition(&f.engine, "nightly", &nightly).await;
    for id in 62..=64 {
        supervisor.run_signal(&scheduled(id, "nightly")).await.unwrap();
        assert!(last_system().contains("failed before changing anything"), "{}", last_system());
    }
    supervisor.run_signal(&scheduled(65, "nightly")).await.unwrap();
    let line = format!("Workflow `nightly` v{version} was set aside (its breaker tripped after 3 faults in a row");
    assert!(last_system().contains(&line), "{}", last_system());
    assert!(children(&f.engine, "signal:65").await.iter().all(|c| c.admission.name != "nightly"), "it never started");

    let writer = document(
        "writer",
        "do: [ { w: { call: t_write } }, { x: { raise: { error: { type: 'https://example.com/e', status: 500, detail: late } } } } ]\n",
    );
    activate_definition(&f.engine, "writer", &writer).await;
    let error = supervisor.run_signal(&scheduled(66, "writer")).await.unwrap_err().to_string();
    assert!(error.contains("may have changed something"), "{error}");
    assert!(last_system().contains("RECOVERY"), "only a report");
    assert_eq!(f.mcp.called.lock().unwrap().iter().filter(|(n, _)| n == "t_write").count(), 1);
    assert!(
        children(&f.engine, "signal:66").await.iter().all(|c| !c.child.as_ref().unwrap().key.ends_with("/primary"))
    );
    supervisor.shutdown().await;
}

// ── 7. a version calling a tool this host no longer has ─────────────────────

// An engine on `f`'s engine store with an agent.db of its own (no tool
// snapshot) and MCP offering `tools` only: a host whose tools changed.
fn changed_host(f: &Fixture, provider: Arc<dyn agent::providers::ChatProvider>, tools: &[&str]) -> Arc<Engine> {
    let db = Arc::new(agent::db::AgentDb::open(f.root.path().join("other.db")).unwrap());
    let mcp = Arc::new(FakeMcp {
        defs: tools.iter().map(|t| agent::providers::ToolDef::new(t, t, json!({"type":"object"}))).collect(),
        ..Default::default()
    });
    agent::agent::create_engine(agent::agent::EngineDeps {
        providers: agent::agent::Providers {
            openai: provider.clone(),
            gemini: provider.clone(),
            deepseek: provider.clone(),
            openrouter: Some(provider),
        },
        mcp,
        presets: agent::models::default_presets(),
        skills: Vec::new(),
        skill_store: f.skills.clone(),
        memory: db.clone(),
        tracer: Arc::new(agent::local_recorder::local_recorder_tracer(db.clone())),
        codex: Arc::new(NoCodex),
        decisions: Some(allow_guard()),
        schedule: Arc::new(agent::schedule::MemorySchedule::default()),
        env: Default::default(),
        db: (*db).clone(),
        store: agent::runs::open_store(&f.root.path().join("agent.db")).unwrap(),
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_version_calling_a_tool_the_host_no_longer_has_is_set_aside() {
    let model = Arc::new(Scripted::new(vec![assistant("handed over"), assistant("built-in answer")]));
    let f = ready(model.clone(), &["t_gone"]).await;
    activate_definition(&f.engine, "gone-task", &document("gone-task", "do: [ { g: { call: t_gone } } ]\n")).await;
    let loop_ = variant("agent-loop").replacen("do:\n", "do:\n  - note: { call: t_gone }\n", 1);
    activate_definition(&f.engine, "agent-loop", &loop_).await;
    f.engine.halt().await;

    let host = changed_host(&f, model.clone(), &[]);
    host.ready().await.unwrap();
    let supervisor = create_supervisor(host.clone());
    assert_eq!(supervisor.run_signal(&scheduled(71, "gone-task")).await.unwrap(), "handed over");
    let system = system_of(&model.seen.lock().unwrap()[0]);
    assert!(system.contains("Workflow `gone-task` v1 was set aside (it no longer fits the tools"), "{system}");
    assert!(system.contains("t_gone"), "the reason names the tool: {system}");
    // The primary itself ran the built-in loop in place of the override.
    let primary = child(&host, "signal:71", "primary").await;
    assert_eq!(primary.admission.version, builtin(&host, "agent-loop").await);
    assert_eq!(agent::runs::ran_of(&primary).fallback.unwrap().reason, agent::runs::FallbackReason::Invalid);
    supervisor.shutdown().await;
}

// ── 8. a start pins the version it read: an activation in between ───────────

// A skill store that activates `version` of `name` when the worker's
// admission is built (the third read of `racer`: its loop preference, the
// starter's check that its skills exist, then its admission).
struct Racing {
    inner: Arc<agent::skills::FsSkillStore>,
    reads: std::sync::atomic::AtomicUsize,
    engine: Mutex<Option<std::sync::Weak<Engine>>>,
    activate: Mutex<Option<(String, u64)>>,
    fired: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl agent::skills::SkillStore for Racing {
    async fn read_skill(&self, name: &str) -> anyhow::Result<Option<agent::skills::SkillFile>> {
        if name == "racer" && self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1 == 3 {
            let engine = self.engine.lock().unwrap().clone().and_then(|w| w.upgrade());
            let target = self.activate.lock().unwrap().clone();
            if let (Some(engine), Some((name, version))) = (engine, target) {
                engine.runtime.activate_as(&name, version, "racer", None).await?;
                self.fired.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
        self.inner.read_skill(name).await
    }
    async fn read_patch(&self, name: &str) -> anyhow::Result<Option<String>> {
        self.inner.read_patch(name).await
    }
    async fn save_skill(&self, name: &str, content: &str) -> anyhow::Result<agent::skills::Written> {
        self.inner.save_skill(name, content).await
    }
    async fn list_skills(&self) -> anyhow::Result<Vec<agent::skills::SkillEntry>> {
        self.inner.list_skills().await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_start_runs_the_version_it_read_so_an_activation_between_cannot_put_a_non_loop_at_a_loop_site() {
    let model = Arc::new(Scripted::new(vec![
        calls(&[("invoke_sub_agent", json!({"prompt":"research","skills":["racer"]}))]),
        assistant("worker answer"),
        calls(&[("invoke_sub_agent", json!({"prompt":"again","skills":["racer"]}))]),
        assistant("second worker"),
        assistant("done"),
    ]));
    let skills: &[(&str, &str)] =
        &[("worker", "---\ntools: *\n---\nWORKER"), ("racer", "---\ntools: *\nloop: research-loop\n---\nRACER")];
    // The fixture's own engine is never started: the racing one runs.
    let f = fixture(model.clone(), &[], skills);
    let racing = Arc::new(Racing {
        inner: f.skills.clone(),
        reads: Default::default(),
        engine: Mutex::default(),
        activate: Mutex::default(),
        fired: Default::default(),
    });
    let engine = reopen(&f, model.clone(), Arc::new(ouro_runtime::durable::SystemClock), |deps| {
        deps.skill_store = racing.clone();
    });
    *racing.engine.lock().unwrap() = Some(Arc::downgrade(&engine));
    engine.ready().await.unwrap();
    let loop_ = variant("research-loop").replace("output: .content", r#"output: ("v1: " + .content)"#);
    let v1 = activate_definition(&engine, "research-loop", &loop_).await;
    let not_a_loop = document("research-loop", "do: [ { a: { set: { ok: true } } } ]\n");
    let v2 = engine.runtime.save("research-loop", &not_a_loop).await.unwrap().version;
    *racing.activate.lock().unwrap() = Some(("research-loop".into(), v2));
    assert_eq!(send(&engine, "parent").await.unwrap(), "done");
    assert!(racing.fired.load(std::sync::atomic::Ordering::SeqCst), "the activation raced the first start");
    let first = child(&engine, "parent", "worker:1.0").await;
    assert_eq!((first.admission.name.as_str(), first.admission.version), ("research-loop", v1));
    assert!(agent::runs::ran_of(&first).pinned);
    assert_eq!(first.outcome, Some(agent::runs::Status::Completed(json!("v1: worker answer"))));
    // Started after it, the non-loop now active is no loop: agent-loop runs.
    let second = child(&engine, "parent", "worker:2.0").await;
    assert_eq!(second.admission.name, "agent-loop");
    assert_eq!(agent::runs::ran_of(&second).fallback.unwrap().reason, agent::runs::FallbackReason::NotALoop);
}

// ── 9. a worker naming a write workflow or `signal` runs agent-loop ─────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_naming_a_write_workflow_or_signal_runs_agent_loop_and_writes_nothing() {
    let model = Arc::new(Scripted::new(vec![
        calls(&[("invoke_sub_agent", json!({"prompt":"one","loop":"sender"}))]),
        assistant("w1"),
        calls(&[("invoke_sub_agent", json!({"prompt":"two","loop":"signal"}))]),
        assistant("w2"),
        assistant("done"),
    ]));
    let f = ready(model.clone(), &["t_write"]).await;
    activate_definition(&f.engine, "sender", &document("sender", "do: [ { s: { call: t_write } } ]\n")).await;
    assert_eq!(send(&f.engine, "parent").await.unwrap(), "done");
    for (key, named) in [("worker:1.0", "sender"), ("worker:2.0", "signal")] {
        let worker = child(&f.engine, "parent", key).await;
        assert_eq!(worker.admission.name, "agent-loop", "{named}");
        let fallback = agent::runs::ran_of(&worker).fallback.unwrap();
        assert_eq!((fallback.name.as_str(), fallback.reason), (named, agent::runs::FallbackReason::NotALoop));
    }
    assert!(f.mcp.called.lock().unwrap().iter().all(|(n, _)| n != "t_write"), "no write ran");
}

// ── 10. loop selection precedence ───────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_runs_the_yaml_loop_then_the_calls_then_its_skills_then_agent_loop() {
    let delegate = |args: Value| calls(&[("invoke_sub_agent", args)]);
    let both = json!({"prompt":"p","loop":"b-loop","skills":["prefers-c"]});
    let model = Arc::new(Scripted::new(vec![
        delegate(both.clone()),
        assistant("a"),
        assistant("done a"),
        delegate(both),
        assistant("b"),
        assistant("done b"),
        delegate(json!({"prompt":"p","skills":["prefers-c"]})),
        assistant("c"),
        assistant("done c"),
        delegate(json!({"prompt":"p"})),
        assistant("d"),
        assistant("done d"),
    ]));
    let skills: &[(&str, &str)] =
        &[("worker", "---\ntools: *\n---\nWORKER"), ("prefers-c", "---\ntools: *\nloop: c-loop\n---\nC")];
    let f = fixture(model.clone(), &[], skills);
    f.engine.ready().await.unwrap();
    for name in ["a-loop", "b-loop", "c-loop"] {
        let prefixed = variant(name).replace("output: .content", &format!(r#"output: ("{name}: " + .content)"#));
        activate_definition(&f.engine, name, &prefixed).await;
    }
    let yaml_loop = without_breaker(AGENT_LOOP).replace(
        "call: start_worker\n                    with: { call: '${ $entry.value }',",
        "call: start_worker\n                    with: { loop: a-loop, call: '${ $entry.value }',",
    );
    assert_ne!(yaml_loop, without_breaker(AGENT_LOOP));
    activate_definition(&f.engine, "agent-loop", &yaml_loop).await;
    assert_eq!(send(&f.engine, "yaml").await.unwrap(), "done a");
    // Back to the built-in agent-loop: its digest needs no dry run.
    let builtin = builtin(&f.engine, "agent-loop").await;
    assert_eq!(activate(&f.engine, "agent-loop", builtin).await.unwrap()["builtin"], true);
    assert_eq!(send(&f.engine, "call").await.unwrap(), "done b");
    assert_eq!(send(&f.engine, "skill").await.unwrap(), "done c");
    assert_eq!(send(&f.engine, "plain").await.unwrap(), "done d");
    for (parent, ran, output) in [
        ("yaml", "a-loop", "a-loop: a"),
        ("call", "b-loop", "b-loop: b"),
        ("skill", "c-loop", "c-loop: c"),
        ("plain", "agent-loop", "d"),
    ] {
        let worker = child(&f.engine, parent, "worker:1.0").await;
        assert_eq!(worker.admission.name, ran, "{parent}");
        assert_eq!(worker.outcome, Some(agent::runs::Status::Completed(json!(output))), "{parent}");
    }
}

// ── 11. an operator's pause ─────────────────────────────────────────────────

async fn pause(engine: &Engine, name: &str, note: &str) {
    let request =
        ouro_runtime::control::Request { id: format!("pause-{name}"), actor: "operator".into(), reason: "test".into() };
    engine.control.pause(&request, name, note).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_paused_name_falls_back_to_another_name_and_never_to_itself() {
    let model = Arc::new(Scripted::new(vec![
        assistant("default answer"),
        calls(&[("invoke_sub_agent", json!({"prompt":"p","loop":"paused-loop"}))]),
        assistant("worker"),
        assistant("parent done"),
        assistant("did the task"),
    ]));
    let f = ready(model.clone(), &[]).await;
    let supervisor = create_supervisor(f.engine.clone());
    let last_system = || system_of(model.seen.lock().unwrap().last().unwrap());
    activate_definition(&f.engine, "tg-route", &route("tg-route", "source: telegram")).await;
    activate_definition(&f.engine, "paused-loop", &variant("paused-loop")).await;
    activate_definition(&f.engine, "paused-task", &document("paused-task", "do: [ { a: { set: { ok: true } } } ]\n"))
        .await;
    for name in ["tg-route", "paused-loop", "paused-task"] {
        pause(&f.engine, name, "maintenance window").await;
    }
    // A route: the default path.
    assert_eq!(supervisor.run_signal(&telegram(81, "hi")).await.unwrap(), "default answer");
    assert!(last_system().contains("Workflow `tg-route` v1 was set aside (an operator paused it"), "{}", last_system());
    // A loop variant: the built-in agent-loop.
    assert_eq!(send(&f.engine, "parent").await.unwrap(), "parent done");
    let worker = child(&f.engine, "parent", "worker:1.0").await;
    assert_eq!(worker.admission.name, "agent-loop");
    assert_eq!(agent::runs::ran_of(&worker).fallback.unwrap().reason, agent::runs::FallbackReason::Paused);
    // A saved workflow: handed over.
    assert_eq!(supervisor.run_signal(&scheduled(82, "paused-task")).await.unwrap(), "did the task");
    assert!(last_system().contains("Workflow `paused-task` v1 was set aside (an operator paused it"));

    // agent-loop itself: nothing to fall back to; the start fails with the
    // note, and no recovery report runs (it would run agent-loop too).
    pause(&f.engine, "agent-loop", "frozen by the operator").await;
    let before = model.seen.lock().unwrap().len();
    let error = supervisor.run_signal(&signal(83, "peer", "hello")).await.unwrap_err().to_string();
    assert!(error.contains("frozen by the operator"), "{error}");
    assert_eq!(model.seen.lock().unwrap().len(), before, "no turn, no report");
    assert!(f.engine.runtime.get("recovery:83").await.unwrap().is_none());
    // The agent cannot lift it; the boot leaves it alone.
    let builtin = builtin(&f.engine, "agent-loop").await;
    let refused = activate(&f.engine, "agent-loop", builtin).await.unwrap_err().to_string();
    assert!(refused.contains("paused by an operator"), "{refused}");
    let newer = AGENT_LOOP.replace("General ReAct loop", "A newer built-in loop");
    agent::runs::install_builtins(&f.engine.runtime, &f.engine.deps.db, &[("agent-loop", newer.as_str())])
        .await
        .unwrap();
    let definitions = f.engine.runtime.definitions("agent-loop").await.unwrap();
    assert!(definitions.versions.values().all(|d| d.digest != digest_of(&newer)), "skipped while paused");
    assert!(definitions.tripped.as_ref().is_some_and(|t| t.manual.is_some()), "still paused");
    supervisor.shutdown().await;
}

// ── 12. a starter dispatched again joins what its first dispatch ran ────────

// A call of a child starter, dispatched again after its first dispatch
// committed: the child is stored, and the engine joins it only under the
// name it was admitted with.
struct Redispatched {
    engine: Arc<Engine>,
    id: String,
    asked: Mutex<Vec<ouro_runtime::ChildStart>>,
}

#[async_trait::async_trait]
impl agent::runs::ChildStarts for Redispatched {
    fn child_id(&self, _key: &str) -> String {
        self.id.clone()
    }
    async fn start_child(
        &self,
        child: ouro_runtime::ChildStart,
    ) -> Result<ouro_runtime::ChildRef, ouro_runtime::ChildError> {
        self.asked.lock().unwrap().push(child.clone());
        let stored = self.engine.runtime.get(&self.id).await.map_err(ouro_runtime::ChildError::Interrupted)?;
        if let Some(stored) = stored {
            // The engine's join: another name is a mismatch.
            if stored.admission.name != child.name {
                return Err(ouro_runtime::ChildError::Refused(ouro_runtime::ChildRefusal::Mismatch(
                    ouro_runtime::durable::AdmissionMismatch {
                        id: self.id.clone(),
                        field: ouro_runtime::durable::AdmissionField::Name,
                    },
                )));
            }
        } else {
            let start = ouro_runtime::Start { version: child.version, host: child.host };
            self.engine
                .runtime
                .start_with(&self.id, &child.name, child.input, start)
                .await
                .map_err(ouro_runtime::ChildError::Interrupted)?;
        }
        Ok(ouro_runtime::ChildRef {
            id: self.id.clone(),
            key: child.key,
            done: format!("done:{}", self.id),
            waiting: format!("waiting:{}", self.id),
        })
    }
}

fn plain_builder(
    engine: &Arc<Engine>,
    built: Arc<std::sync::atomic::AtomicUsize>,
) -> impl Fn(Choice) -> futures::future::BoxFuture<'static, anyhow::Result<(Value, Value)>> {
    let engine = engine.clone();
    move |choice: Choice| {
        let (engine, built) = (engine.clone(), built.clone());
        Box::pin(async move {
            built.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut opts = LoopOptions::new("redispatched", context());
            opts.workflow = choice.name.clone();
            if let Some(fallback) = &choice.fallback {
                opts.system_prompt = Some(fallback.line.clone());
            }
            let mut host = agent::agent::loop_host(&engine, &opts, agent::runs::WORKER, "redispatched", "").await?;
            host.fallback = choice.fallback;
            let input =
                json!({"messages": [{"role":"system","content":"s"},{"role":"user","content":"u"}], "memory": []});
            Ok((input, serde_json::to_value(&host)?))
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_starter_dispatched_again_after_a_fallback_joins_the_run_under_the_name_that_ran() {
    // No engine worker here: the stored child is never driven.
    let quiet = fixture(Arc::new(Scripted::new(vec![])), &[], SKILLS);
    agent::runs::install_builtins(&quiet.engine.runtime, &quiet.engine.deps.db, &flows::builtins()).await.unwrap();
    let builtin = self::builtin(&quiet.engine, "agent-loop").await;
    // A cross-name child: `tg-loop` (no active version) gave way to agent-loop.
    let call =
        Redispatched { engine: quiet.engine.clone(), id: "hostchild:redispatched".into(), asked: Mutex::default() };
    let built = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    for _ in 0..2 {
        let build = plain_builder(&quiet.engine, built.clone());
        let started = agent::runs::start_named_child(&quiet.engine, &call, "worker:1.0", "tg-loop", Site::Loop, build)
            .await
            .unwrap();
        let agent::runs::Named::Started(child, ran) = started else { panic!("set aside at a loop site") };
        assert_eq!(child.id, "hostchild:redispatched");
        assert_eq!((ran.name.as_str(), ran.version, ran.pinned), ("agent-loop", builtin, true));
        assert_eq!(ran.fallback.unwrap().name, "tg-loop");
    }
    let asked: Vec<String> = call.asked.lock().unwrap().iter().map(|c| c.name.clone()).collect();
    assert_eq!(asked, ["agent-loop", "agent-loop"]);
    assert_eq!(built.load(std::sync::atomic::Ordering::SeqCst), 1, "the joined call builds nothing");

    // recovery_loop: its host carries the line, so a start again with the
    // host recomputed would differ; it joins the stored run.
    let turns = Turns::new(vec![fail("model down"), turn(|_| answer("reported"))]);
    let f = ready(turns.clone(), &[]).await;
    let broken = faulting(&without_breaker(AGENT_LOOP));
    activate_definition(&f.engine, "agent-loop", &broken).await;
    for n in 1..=3 {
        assert!(send(&f.engine, &format!("trip-{n}")).await.is_err());
    }
    let supervisor = create_supervisor(f.engine.clone());
    // The primary faults (the built-in in place of the tripped override, the
    // model's script exhausted after the report), and the report runs too.
    let error = supervisor.run_signal(&signal(121, "peer", "hello")).await.unwrap_err().to_string();
    assert!(error.contains("model down"), "{error}");
    let recovery = f.engine.runtime.get("recovery:121").await.unwrap().expect("the report ran");
    let ran = agent::runs::ran_of(&recovery);
    assert_eq!((ran.name.as_str(), ran.version), ("agent-loop", self::builtin(&f.engine, "agent-loop").await));
    assert_eq!(ran.fallback.as_ref().unwrap().reason, agent::runs::FallbackReason::Tripped);
    let rebuilt = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (again, joined) = agent::runs::start_named_root(
        &f.engine,
        "recovery:121",
        "agent-loop",
        Site::Loop,
        plain_builder(&f.engine, rebuilt.clone()),
    )
    .await
    .unwrap();
    assert_eq!(again.admission.id, "recovery:121");
    assert_eq!(joined, ran, "it reports what ran, not a mismatch");
    assert_eq!(rebuilt.load(std::sync::atomic::Ordering::SeqCst), 0);
    supervisor.shutdown().await;
}

// ── 13. the same text is the same version; never-activated versions capped ─

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn saving_the_same_text_reuses_its_version_and_drafts_are_capped() {
    let f = ready(Arc::new(Scripted::new(vec![])), &[]).await;
    let text = |n: usize| document("drafts", &format!("do: [ {{ a: {{ set: {{ n: {n} }} }} }} ]\n"));
    let first =
        tool(&f.engine, "save_workflow", json!({"name":"drafts","definition":text(0),"note":"one"})).await.unwrap();
    let again =
        tool(&f.engine, "save_workflow", json!({"name":"drafts","definition":text(0),"note":"two"})).await.unwrap();
    assert_eq!(first["version"], again["version"]);
    assert!(again["reused"].is_string(), "{again}");
    assert_eq!(tool(&f.engine, "read_workflow", json!({"name":"drafts"})).await.unwrap()["note"], "two");
    for n in 1..flows::MAX_DRAFTS {
        save(&f.engine, "drafts", &text(n)).await;
    }
    let capped = tool(&f.engine, "save_workflow", json!({"name":"drafts","definition":text(99)})).await.unwrap_err();
    let capped = capped.to_string();
    assert!(
        capped.contains("20 versions that were never activated") && capped.contains(flows::PRUNING_ISSUE),
        "{capped}"
    );
    // An activated version no longer counts.
    mark_tried(&f.engine, "drafts", &text(3));
    activate(&f.engine, "drafts", 4).await.unwrap();
    save(&f.engine, "drafts", &text(99)).await;
    assert_eq!(f.engine.runtime.definitions("drafts").await.unwrap().versions.len(), 21);
}

// ── 14. the built-in text is how an override is undone ──────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_current_built_in_reactivates_without_a_dry_run_and_the_next_boot_upgrades_it() {
    let f = fixture(Arc::new(Scripted::new(vec![])), &[], SKILLS);
    let older = AGENT_LOOP.replace("General ReAct loop", "An older built-in loop");
    agent::runs::install_builtins(&f.engine.runtime, &f.engine.deps.db, &[("agent-loop", older.as_str())])
        .await
        .unwrap();
    f.engine.ready().await.unwrap();
    let current = builtin(&f.engine, "agent-loop").await;
    let definitions = f.engine.runtime.definitions("agent-loop").await.unwrap();
    assert_eq!(definitions.active, Some(current), "the boot upgraded the older built-in");
    let old = definitions.versions.values().find(|d| d.digest == digest_of(&older)).unwrap().version;

    let own = variant("agent-loop").replace("General ReAct loop", "My loop");
    activate_definition(&f.engine, "agent-loop", &own).await;
    let undone = activate(&f.engine, "agent-loop", current).await.unwrap();
    assert_eq!(undone["builtin"], true, "no dry run needed: {undone}");
    assert_eq!(f.engine.runtime.definitions("agent-loop").await.unwrap().active, Some(current));
    // An older built-in text is not the current one: it goes through the gate.
    let error = activate(&f.engine, "agent-loop", old).await.unwrap_err().to_string();
    assert!(error.contains("metadata.ouro.breaker"), "{error}");
    // Re-activated, the built-in is upgraded by the next boot's text.
    let newer = AGENT_LOOP.replace("General ReAct loop", "A newer built-in loop");
    agent::runs::install_builtins(&f.engine.runtime, &f.engine.deps.db, &[("agent-loop", newer.as_str())])
        .await
        .unwrap();
    let definitions = f.engine.runtime.definitions("agent-loop").await.unwrap();
    let active = &definitions.versions[&definitions.active.unwrap()];
    assert_eq!(active.digest, digest_of(&newer));
}

// ── 15. the boot keeps an override; 0016 imports the built-in digests ───────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_boot_keeps_an_agent_override_across_a_restart() {
    let model = Arc::new(Scripted::new(vec![]));
    let f = ready(model.clone(), &[]).await;
    let own = variant("agent-loop").replace("General ReAct loop", "My loop");
    let version = activate_definition(&f.engine, "agent-loop", &own).await;
    f.engine.halt().await;
    let restarted = reopen(&f, model, Arc::new(ouro_runtime::durable::SystemClock), |_| {});
    agent::runs::boot(&restarted).await.unwrap();
    assert_eq!(restarted.runtime.definitions("agent-loop").await.unwrap().active, Some(version));
    let newer = AGENT_LOOP.replace("General ReAct loop", "A newer built-in loop");
    agent::runs::install_builtins(&restarted.runtime, &restarted.deps.db, &[("agent-loop", newer.as_str())])
        .await
        .unwrap();
    let definitions = restarted.runtime.definitions("agent-loop").await.unwrap();
    assert_eq!(definitions.active, Some(version), "an override is never replaced at boot");
    assert!(definitions.versions.values().any(|d| d.digest == digest_of(&newer)), "the new text is saved");
}

#[test]
fn migration_0016_imports_the_built_in_digests_the_memory_kv_kept() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("agent.db");
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(r#"CREATE TABLE IF NOT EXISTS "__drizzle_migrations" (id SERIAL PRIMARY KEY, hash text NOT NULL, created_at numeric)"#).unwrap();
        for m in agent::db::migrations().unwrap().into_iter().filter(|m| m.tag.as_str() < "0016") {
            for statement in &m.statements {
                conn.execute_batch(statement).unwrap();
            }
            conn.execute(
                r#"INSERT INTO "__drizzle_migrations" ("hash", "created_at") VALUES(?1, ?2)"#,
                rusqlite::params![m.hash, m.when],
            )
            .unwrap();
        }
        conn.execute(
            "INSERT INTO memory (key, value) VALUES ('engine.builtin_digests.agent-loop', '[\"d1\",\"d2\"]'), ('engine.builtin_digests.signal', '[\"d3\"]'), ('other', 'x')",
            [],
        )
        .unwrap();
    }
    let db = agent::db::AgentDb::open(&path).unwrap();
    for (name, digest) in [("agent-loop", "d1"), ("agent-loop", "d2"), ("signal", "d3")] {
        let meta = WorkflowMeta::get(&db, name, digest).unwrap().unwrap_or_else(|| panic!("{name} {digest}"));
        assert!(meta.builtin && meta.tried_ms.is_none() && meta.note.is_none());
    }
    assert_eq!(WorkflowMeta::names(&db).unwrap(), ["agent-loop", "signal"]);
}

// ── 16. route priority ──────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn routes_at_equal_specificity_need_distinct_priorities_and_the_most_specific_wins() {
    let model = Arc::new(Scripted::new(vec![assistant("specific")]));
    let f = ready(model.clone(), &[]).await;
    activate_definition(&f.engine, "route-a", &route("route-a", "source: telegram")).await;
    let tie = route("route-b", "source: telegram");
    let version = save(&f.engine, "route-b", &tie).await;
    mark_tried(&f.engine, "route-b", &tie);
    let error = activate(&f.engine, "route-b", version).await.unwrap_err().to_string();
    assert!(error.contains("route-a") && error.contains("priority"), "{error}");
    let prioritized = route("route-b", "source: telegram")
        .replace("when: { source: telegram } }", "when: { source: telegram }, priority: 1 }");
    activate_definition(&f.engine, "route-b", &prioritized).await;
    activate_definition(&f.engine, "route-c", &route("route-c", "source: telegram, topic: research")).await;
    let names = |routes: Vec<(String, u64)>| routes.into_iter().map(|(n, _)| n).collect::<Vec<_>>();
    assert_eq!(names(flows::signal_routes(&f.engine, "telegram", &Value::Null).await.unwrap()), ["route-b", "route-a"]);
    let research = json!({"topic":"research"});
    assert_eq!(names(flows::signal_routes(&f.engine, "telegram", &research).await.unwrap())[0], "route-c");
    let supervisor = create_supervisor(f.engine.clone());
    let incoming = PendingSignal { payload: Some(research), ..telegram(161, "hi") };
    assert_eq!(supervisor.run_signal(&incoming).await.unwrap(), "specific");
    assert_eq!(child(&f.engine, "signal:161", "route").await.admission.name, "route-c");
    supervisor.shutdown().await;
}

// ── 17. list_workflows and read_workflow ────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_catalog_shows_built_ins_derived_status_and_set_aside_versions() {
    let f = ready(Arc::new(Scripted::new(vec![])), &[]).await;
    let listed = tool(&f.engine, "list_workflows", json!({})).await.unwrap();
    for name in ["signal", "agent-loop", "guard"] {
        let entry = listed["workflows"].as_array().unwrap().iter().find(|w| w["name"] == name).unwrap();
        assert_eq!((entry["builtin"].as_bool(), entry["status"].as_str()), (Some(true), Some("active")), "{name}");
    }
    let read = tool(&f.engine, "read_workflow", json!({"name":"signal"})).await.unwrap();
    assert_eq!((read["builtin"].as_bool(), read["definition"].as_str()), (Some(true), Some(SIGNAL)));
    let guard = tool(&f.engine, "read_workflow", json!({"name":"guard"})).await.unwrap();
    assert_eq!(guard["definition"], agent::flows::GUARD);

    activate_definition(&f.engine, "tripping", &faulting(&variant("tripping"))).await;
    for n in 1..=3 {
        let mut opts = LoopOptions::new(format!("t{n}"), context());
        opts.workflow = "tripping".into();
        opts.skills = vec!["worker".into()];
        assert!(f.engine.start_loop(opts).await.unwrap().send("go".into()).await.is_err());
    }
    activate_definition(&f.engine, "held", &variant("held")).await;
    pause(&f.engine, "held", "hold on").await;
    let listed = tool(&f.engine, "list_workflows", json!({})).await.unwrap();
    let entry =
        |name: &str| listed["workflows"].as_array().unwrap().iter().find(|w| w["name"] == name).unwrap().clone();
    assert_eq!(
        (entry("tripping")["tripped"].as_bool(), entry("tripping")["paused"].as_bool()),
        (Some(true), Some(false))
    );
    assert_eq!((entry("held")["tripped"].as_bool(), entry("held")["paused"].as_bool()), (Some(false), Some(true)));
    let read = tool(&f.engine, "read_workflow", json!({"name":"tripping"})).await.unwrap();
    assert_eq!(
        (read["status"].as_str(), read["tripped"].as_bool(), read["builtin"].as_bool()),
        (Some("active"), Some(true), Some(false))
    );
}

// ── 18. route and saved launches ────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn route_and_saved_launches_are_admitted_with_their_defaults_and_settled() {
    let model = Arc::new(Scripted::new(vec![assistant("routed"), assistant("saved"), assistant("after handover")]));
    let f = fixture_on(model.clone(), &["t_write"], SKILLS, |deps| {
        deps.env.telegram_chat_id = Some("999".into());
    });
    f.engine.ready().await.unwrap();
    let supervisor = create_supervisor(f.engine.clone());
    activate_definition(&f.engine, "tg-route", &route("tg-route", "source: telegram")).await;
    let incoming = PendingSignal { env_context: Some("Telegram note.".into()), ..telegram(181, "hi") };
    assert_eq!(supervisor.run_signal(&incoming).await.unwrap(), "routed");
    let run = child(&f.engine, "signal:181", "route").await;
    let host = run.admission.host().unwrap();
    assert_eq!(
        (host["kind"].as_str(), host["preset"].as_str(), host["loop"].as_str()),
        (Some("primary"), Some("smart"), Some("tg-route"))
    );
    assert_eq!(host["confirm"], json!({"chatId":"42","messageThreadId":null}));
    assert!(host["skills"].as_array().unwrap().is_empty(), "a route sets up its own skills");
    assert_eq!(run.admission.input["signal"]["source"], "telegram");
    assert!(run.admission.input["env"]["now"].is_string());
    assert_eq!(run.admission.input["messages"][1]["content"], incoming.content);
    let system = system_of(&model.seen.lock().unwrap()[0]);
    assert!(system.contains("Signal source: telegram") && system.contains("Telegram note."), "{system}");
    let (trace, observations) = agent::db::TraceStore::get_trace(&*f.db, "telegram:181").unwrap().unwrap();
    assert_eq!(trace.output, json!("routed"));
    assert!(observations.iter().any(|o| o.name == "route:tg-route"));

    // A saved workflow: the base preset, the source's skill, the default chat.
    activate_definition(
        &f.engine,
        "daily",
        &variant("daily").replace(
            "metadata: { loop: { description: 'General ReAct loop with guarded tools and workers.' } }",
            "metadata: {}",
        ),
    )
    .await;
    let out = supervisor.run_signal(&scheduled(182, "daily")).await.unwrap();
    assert_eq!(serde_json::from_str::<Value>(&out).unwrap(), json!({"ok":true,"workflow":"daily","output":"saved"}));
    let run = child(&f.engine, "signal:182", "saved").await;
    let host = run.admission.host().unwrap();
    assert_eq!((host["preset"].as_str(), host["skills"].clone()), (Some("base"), json!(["scheduler"])));
    assert_eq!(host["confirm"], json!({"chatId":"999","messageThreadId":null}));
    assert!(system_of(&model.seen.lock().unwrap()[1]).contains("SCHEDULER"));
    let (_, observations) = agent::db::TraceStore::get_trace(&*f.db, "scheduler:182").unwrap().unwrap();
    assert!(observations.iter().any(|o| o.name == "runner" && o.meta("workflow") == Some(&json!("daily"))));

    // A saved workflow that changed nothing hands its task to routing, once:
    // the matching route runs with the handover in its instructions.
    activate_definition(&f.engine, "sched-route", &route("sched-route", "source: scheduler")).await;
    activate_definition(&f.engine, "fails", &faulting(&document("fails", "do: [ { a: { set: { ok: true } } } ]\n")))
        .await;
    assert_eq!(supervisor.run_signal(&scheduled(183, "fails")).await.unwrap(), "after handover");
    let system = system_of(&model.seen.lock().unwrap()[2]);
    assert!(system.contains("The saved workflow `fails` for this task failed before changing anything"), "{system}");
    let started: Vec<String> = children(&f.engine, "signal:183").await.into_iter().map(|c| c.admission.name).collect();
    assert_eq!(started.len(), 2, "the saved run and the route: {started:?}");
    assert!(started.contains(&"fails".to_owned()) && started.contains(&"sched-route".to_owned()));
    supervisor.shutdown().await;
}

// ── 19. a faulted route's report reads the route's transcript ───────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_faulted_routes_recovery_report_has_the_routes_transcript() {
    let turns = Turns::new(vec![
        turn(|_| call("t_marker", json!({"marker":"route transcript marker"}))),
        fail("model down"),
        turn(|_| answer("reported")),
    ]);
    let f = fixture(turns.clone(), &["t_marker"], SKILLS);
    f.engine.ready().await.unwrap();
    let supervisor = create_supervisor(f.engine.clone());
    activate_definition(&f.engine, "tg-route", &route("tg-route", "source: telegram")).await;
    let error = supervisor.run_signal(&telegram(191, "hi")).await.unwrap_err().to_string();
    assert!(error.contains("model down"), "{error}");
    let report = turns.requests.lock().unwrap().last().unwrap().clone();
    let prompt = report.iter().find(|m| m["role"] == "user").unwrap()["content"].as_str().unwrap().to_owned();
    assert!(prompt.contains("route transcript marker"), "{prompt}");
    assert!(prompt.contains("model down"));
    supervisor.shutdown().await;
}

// ── 20. workflow_meta, orphans, the loop catalog ────────────────────────────

fn meta_contract(meta: &dyn WorkflowMeta) {
    assert_eq!(meta.get("w", "d").unwrap(), None);
    meta.record("w", "d", Some("first"), false).unwrap();
    let created = meta.get("w", "d").unwrap().unwrap();
    assert_eq!((created.note.as_deref(), created.builtin, created.tried_ms), (Some("first"), false, None));
    meta.record("w", "d", None, false).unwrap();
    assert_eq!(meta.get("w", "d").unwrap().unwrap().note.as_deref(), Some("first"), "no note keeps the stored one");
    meta.record("w", "d", Some("second"), true).unwrap();
    meta.mark_tried("w", "d").unwrap();
    meta.record("w", "d", None, false).unwrap();
    let row = meta.get("w", "d").unwrap().unwrap();
    assert_eq!((row.note.as_deref(), row.builtin), (Some("second"), true), "builtin only goes 0 → 1");
    assert!(row.tried_ms.is_some(), "tried is never cleared");
    assert_eq!(row.created_ms, created.created_ms, "the first write sets created");
    assert!(meta.mark_tried("w", "other").is_err());
    meta.record("a", "x", None, false).unwrap();
    assert_eq!(meta.names().unwrap(), ["a", "w"]);
}

#[test]
fn workflow_meta_records_upsert_by_content_on_both_stores() {
    meta_contract(&flows::MemoryMeta::default());
    let dir = tempfile::tempdir().unwrap();
    meta_contract(&agent::db::AgentDb::open(dir.path().join("agent.db")).unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_catalog_skips_orphans_shows_tried_and_marks_set_aside_loops() {
    let f = ready(Arc::new(Scripted::new(vec![])), &[]).await;
    WorkflowMeta::record(&f.engine.deps.db, "ghost", "never-saved", None, false).unwrap();
    let draft = variant("drafted");
    save(&f.engine, "drafted", &draft).await;
    let listed = tool(&f.engine, "list_workflows", json!({})).await.unwrap();
    let names: Vec<&str> =
        listed["workflows"].as_array().unwrap().iter().map(|w| w["name"].as_str().unwrap()).collect();
    assert!(!names.contains(&"ghost"), "an orphan row is skipped: {names:?}");
    let drafted = || async {
        let listed = tool(&f.engine, "list_workflows", json!({})).await.unwrap();
        listed["workflows"].as_array().unwrap().iter().find(|w| w["name"] == "drafted").unwrap()["tried"].clone()
    };
    assert_eq!(drafted().await, false);
    mark_tried(&f.engine, "drafted", &draft);
    assert_eq!(drafted().await, true);
    // A draft never shows in the loop catalog; an active loop does, and is
    // marked once set aside.
    assert!(!flows::loop_catalog(&f.engine).await.unwrap().contains("drafted:"));
    activate_definition(&f.engine, "tripping", &faulting(&variant("tripping"))).await;
    let catalog = flows::loop_catalog(&f.engine).await.unwrap();
    assert!(
        catalog.contains("agent-loop: General ReAct loop") && catalog.contains("tripping: General ReAct loop"),
        "{catalog}"
    );
    assert!(!catalog.contains("(set aside"), "{catalog}");
    for n in 1..=3 {
        let mut opts = LoopOptions::new(format!("t{n}"), context());
        opts.workflow = "tripping".into();
        opts.skills = vec!["worker".into()];
        assert!(f.engine.start_loop(opts).await.unwrap().send("go".into()).await.is_err());
    }
    let catalog = flows::loop_catalog(&f.engine).await.unwrap();
    assert!(catalog.lines().any(|l| l.starts_with("tripping:") && l.contains("(set aside")), "{catalog}");
}
