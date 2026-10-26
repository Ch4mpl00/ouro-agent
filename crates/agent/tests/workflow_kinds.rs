// One kind of workflow (#60): any workflow the runtime starts on a signal is
// admitted with the model's parameters (host data), so it may mix
// deterministic steps with agent turns, and a recurring task can start as a
// copy of agent-loop. The guarded round goes wherever agent_turn does; a run
// a workflow starts itself is never admitted (engine_runs.rs,
// a_loop_a_workflow_starts_itself_is_never_admitted). Restored from
// tests/pending on the engine (design 04): a scheduled task's saved workflow
// is the signal's primary run (`run_saved_workflow`); the dry run of the
// first test stays parked for the sandbox step (tests/pending/workflow_kinds.rs).
mod support;
use agent::{
    agent::{AGENT_LOOP, Engine},
    db::TraceStore,
    mcp::Mcp,
    providers::{ToolDef, assistant},
    schedule::{FIRST_FIRE_ID, Fire, FireSink, SupervisorSink},
    supervisor::{Supervisor, create_supervisor},
    trace_model::Observation,
};
use async_trait::async_trait;
use serde_json::{Map, Value, json};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use support::*;
use tokio_util::sync::CancellationToken;

const SKILLS: &[(&str, &str)] = &[
    ("scheduler", "---\ntools: *\n---\nSCHEDULER"),
    ("recovery", "---\ntools: []\n---\nRECOVERY"),
    ("worker", "---\ntools: *\n---\nWORKER"),
];
const READS: &[&str] = &["fetch_page", "get_timezone", "journal_tail"];
const WRITES: &[&str] = &["send_telegram_message"];
const LOOP_HEADER: &str = "name: agent-loop, version: '1.0.0', metadata: { loop: { description: 'General ReAct loop with guarded tools and workers.' }, ouro: { breaker: 'off' } } }";

#[derive(Default)]
struct World {
    calls: Mutex<Vec<(String, Map<String, Value>)>>,
}

impl World {
    fn called(&self, name: &str) -> Vec<Map<String, Value>> {
        self.calls.lock().unwrap().iter().filter(|(n, _)| n == name).map(|(_, a)| a.clone()).collect()
    }
}

#[async_trait]
impl Mcp for World {
    fn tools(&self) -> Vec<ToolDef> {
        READS.iter().chain(WRITES).map(|n| ToolDef::new(n, n, json!({"type":"object"}))).collect()
    }
    fn read_only(&self, name: &str) -> bool {
        READS.contains(&name)
    }
    fn open_world(&self, _: &str) -> bool {
        false
    }
    async fn call_tool(&self, name: &str, args: Map<String, Value>, _: &CancellationToken) -> anyhow::Result<String> {
        self.calls.lock().unwrap().push((name.into(), args.clone()));
        Ok(match name {
            "fetch_page" => {
                json!({"url": args["url"], "body": format!("Body of {}", args["url"].as_str().unwrap_or("?"))})
            }
            "get_timezone" => json!({"timezone":"UTC"}),
            "journal_tail" => json!({"entries":[]}),
            _ => json!({"delivered":true,"messageId":7}),
        }
        .to_string())
    }
}

struct Agent {
    f: Fixture,
    world: Arc<World>,
    model: Arc<Scripted>,
    supervisor: Arc<Supervisor>,
    fires: AtomicI64,
}

fn agent(script: Vec<agent::providers::Message>) -> Agent {
    let model = Arc::new(Scripted::new(script));
    let world = Arc::new(World::default());
    let f = fixture_on(model.clone(), &[], SKILLS, |deps| {
        deps.mcp = world.clone();
        deps.env.telegram_chat_id = Some("999".into());
    });
    let supervisor = create_supervisor(f.engine.clone());
    Agent { f, world, model, supervisor, fires: AtomicI64::new(0) }
}

impl Agent {
    async fn tool(&self, name: &str, args: Value) -> anyhow::Result<Value> {
        self.f.engine.ready().await?;
        let reply = agent::flows::tool(&self.f.engine, name, args.as_object().unwrap()).await?;
        Ok(serde_json::from_str(&reply)?)
    }

    // A fire of a task that names `workflow`, as the schedule's tick hands
    // it to the supervisor (scheduling one with `schedule_task(workflow:)`
    // returns with the schedules step); the trace id of the fire.
    async fn fire(&self, workflow: &str, prompt: &str) -> String {
        let n = self.fires.fetch_add(1, Ordering::SeqCst) + 1;
        let sink = SupervisorSink { supervisor: self.supervisor.clone(), env_context: None };
        let fire = Fire {
            id: FIRST_FIRE_ID + n,
            task_id: n,
            source: "scheduler".into(),
            content: prompt.into(),
            workflow: Some(workflow.into()),
            slot: chrono::Utc::now(),
        };
        assert_eq!(sink.fire(fire).await, agent::schedule::Fired::Started);
        format!("scheduler:{}", FIRST_FIRE_ID + n)
    }

    fn trace(&self, id: &str) -> (agent::trace_model::Trace, Vec<Observation>) {
        self.f.db.get_trace(id).unwrap().unwrap()
    }

    // The saved workflow's run of fire `id` (trace id `scheduler:<n>`).
    async fn saved_run(&self, id: &str) -> ouro_runtime::durable::SavedRun {
        let signal = format!("signal:{}", id.trim_start_matches("scheduler:"));
        saved_child(&self.f.engine, &signal).await
    }
}

async fn saved_child(engine: &Engine, signal: &str) -> ouro_runtime::durable::SavedRun {
    let filter = agent::runs::Runs {
        parent: Some(signal.into()),
        since_ms: Some(0),
        before_ms: Some(i64::MAX / 4),
        ..Default::default()
    };
    let mut ids: Vec<String> =
        engine.control.history(&filter, None, 50).await.unwrap().items.into_iter().map(|r| r.admission.id).collect();
    let live = agent::runs::Runs { parent: Some(signal.into()), ..Default::default() };
    ids.extend(engine.control.list(&live, None, 50).await.unwrap().items.into_iter().map(|r| r.admission.id));
    for id in ids {
        let run = engine.runtime.get(&id).await.unwrap().unwrap();
        if run.child.as_ref().is_some_and(|l| l.key.ends_with("/saved")) {
            return run;
        }
    }
    panic!("no saved run under {signal}")
}

// The names of an observation's ancestors, nearest first; every parent must
// be in the same trace.
fn ancestors<'a>(observations: &'a [Observation], of: &Observation) -> Vec<&'a str> {
    let mut names = Vec::new();
    let mut parent = of.parent_observation_id.clone();
    while let Some(id) = parent {
        let o = observations.iter().find(|o| o.id == id).unwrap_or_else(|| panic!("{} has a parent outside", of.name));
        names.push(o.name.as_str());
        parent = o.parent_observation_id.clone();
    }
    names
}

fn system(seen: &Seen) -> &str {
    seen.messages[0]["content"].as_str().unwrap()
}

// A deterministic `for` over the pages, calling a read directly, then the
// guarded round of agent-loop: the model starts from what the steps read.
fn pages() -> String {
    let steps = r#"  - fetch:
      for: { each: url, in: '${ ["https://a.example/1", "https://b.example/2"] }' }
      metadata: { ouro: { concurrency: 4 } }
      do:
        - page:
            call: fetch_page
            with: { url: '${ $url }' }
      export: { as: '${ $context + { pages: . } }' }
  - setup:
      call: loop_setup
      with:
        system: '${ [ "Pages fetched before your first turn:\n" + ($context.pages | map(.body) | join("\n")) ] }'
      export: { as: '${ $context + { messages: .messages, memory: .memory } }' }
"#;
    let pages = AGENT_LOOP.replace(LOOP_HEADER, "name: pages, version: '1.0.0' }").replacen(
        "do:\n",
        &format!("do:\n{steps}"),
        1,
    );
    assert_ne!(pages, AGENT_LOOP);
    pages
}

const SEND: (&str, &str) = ("send_telegram_message", "Two pages: A and B");

fn send() -> agent::providers::Message {
    calls(&[(SEND.0, json!({"chatId":"999","text":SEND.1}))])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_semi_deterministic_workflow_saves_tries_and_runs_on_a_scheduled_signal() {
    let a = agent(vec![send(), assistant("Sent the summary.")]);
    let definition = pages();
    assert!(!definition.contains("metadata: { loop"), "not a loop: a scheduled task's workflow");
    let saved = a.tool("save_workflow", json!({"name":"pages","definition":definition})).await.unwrap();
    assert!(saved["next"].as_str().unwrap().contains("payload: {workflow: \"pages\"}"), "{saved}");
    // The dry run (try_workflow on a fire of its task) returns with the
    // sandbox step; its record of a completed dry run is what activation needs.
    mark_tried(&a.f.engine, "pages", &definition);
    a.tool("activate_workflow", json!({"name":"pages","version":1})).await.unwrap();

    let id = a.fire("pages", "Summarize the pages for me.").await;
    let urls: Vec<Value> = a.world.called("fetch_page").iter().map(|args| args["url"].clone()).collect();
    assert_eq!(urls.len(), 2, "each page read once by the steps, not by the model");
    assert!(urls.contains(&json!("https://a.example/1")) && urls.contains(&json!("https://b.example/2")));
    assert_eq!(a.world.called(SEND.0).len(), 1);
    let seen = a.model.seen.lock().unwrap().clone();
    let first = &seen[0];
    for part in ["Body of https://a.example/1", "Body of https://b.example/2", "SCHEDULER"] {
        assert!(system(first).contains(part), "{part}: {}", system(first));
    }
    assert!(first.messages[1]["content"].as_str().unwrap().contains("Summarize the pages for me."));
    assert_eq!(first.model, agent::models::default_presets().base.model, "a scheduled task's default preset");

    // One trace, one tree: the steps' calls and the model's turns under the signal's primary run.
    let (trace, observations) = a.trace(&id);
    assert_eq!(trace.output["output"], "Sent the summary.");
    assert_eq!(trace.tags, ["scheduler", "saved-workflow"]);
    let primary = observations.iter().find(|o| o.name == "agent_loop").unwrap();
    assert_eq!(primary.meta("activity"), Some(&json!("primary_loop")));
    let pages: Vec<&Observation> =
        observations.iter().filter(|o| o.meta("tool") == Some(&json!("fetch_page"))).collect();
    assert_eq!(pages.len(), 2);
    for page in pages {
        let up = ancestors(&observations, page);
        assert!(up.contains(&"runner") && up.contains(&"agent_loop"), "{up:?}");
    }
    for turn in ["iter-0", "iter-1"] {
        let generation = observations.iter().find(|o| o.name == turn).unwrap_or_else(|| panic!("{turn}"));
        assert!(ancestors(&observations, generation).contains(&"agent_loop"));
    }
    assert!(observations.iter().any(|o| o.name == "guard"), "the model's send went through the guard child");
    let run = a.saved_run(&id).await;
    assert_eq!(
        (run.admission.name.as_str(), run.admission.host().unwrap()["preset"].as_str()),
        ("pages", Some("base"))
    );
    a.supervisor.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deterministic_calls_keep_full_results_and_model_calls_are_not_duplicated() {
    for loop_metadata in [false, true] {
        let a = agent(vec![send(), assistant("Sent the summary.")]);
        let mut definition = pages()
            .replace(
                "with: { url: '${ $url }' }",
                "with: { url: '${ $url }' }\n            output: { as: '${ {body: .body} }' }",
            )
            .replacen(
                "do:\n",
                "do:\n  - remember: { call: set_memory, with: { key: pages.checked, value: yes } }\n",
                1,
            );
        if loop_metadata {
            definition = definition.replace(
                "name: pages, version: '1.0.0' }",
                "name: pages, version: '1.0.0', metadata: {loop: {description: Pages with fixed steps}} }",
            );
        }
        activate_definition(&a.f.engine, "pages", &definition).await;
        let id = a.fire("pages", "Summarize the pages.").await;
        let (trace, observations) = a.trace(&id);
        let run = a.saved_run(&id).await;
        let reads: Vec<_> = observations.iter().filter(|o| o.meta("tool") == Some(&json!("fetch_page"))).collect();
        assert_eq!(reads.len(), 2, "metadata.loop={loop_metadata}: each fixed call has one span");
        for read in reads {
            let url = read.input["url"].as_str().unwrap();
            assert_eq!(read.meta("workflow_run"), Some(&json!(run.admission.id)));
            let raw: Value = serde_json::from_str(read.output.as_str().unwrap()).unwrap();
            assert_eq!(raw, json!({"url":url,"body":format!("Body of {url}")}), "the tool's reply, whole");
            assert_eq!(read.level, "DEFAULT");
            assert!(!read.end_time.is_empty());
            assert!(ancestors(&observations, read).contains(&"runner"));
        }
        assert_eq!(
            observations.iter().filter(|o| o.meta("tool") == Some(&json!(SEND.0))).count(),
            1,
            "the model's existing tool span is not duplicated"
        );
        let remembered: Vec<_> = observations.iter().filter(|o| o.meta("tool") == Some(&json!("set_memory"))).collect();
        assert_eq!(remembered.len(), 1);
        assert_eq!(remembered[0].input, json!({"key":"pages.checked","value":"yes"}));
        assert_eq!(remembered[0].output, json!({"ok":true,"key":"pages.checked"}));
        let runner = observations.iter().find(|o| o.name == "runner" && o.meta("workflow") == Some(&json!("pages")));
        assert!(runner.is_some(), "the saved run's span");
        // Fixed calls keep the admitted input: the model's parameters stay the host's.
        assert_eq!(run.admission.host().unwrap()["preset"], "base");
        assert!(run.admission.input["messages"].is_array());
        let packet = agent::activity::project_trace(&trace, &observations, None, &Default::default());
        assert_eq!(packet["actions"].as_array().unwrap().iter().filter(|a| a["tool"] == "fetch_page").count(), 2);
        assert!(
            !packet["actions"].as_array().unwrap().iter().any(|a| a["tool"] == "call:page"),
            "the activity journal records the call, not its task wrapper again"
        );
        a.supervisor.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn agent_turns_need_the_guarded_round_and_steps_alone_need_none() {
    let a = agent(vec![]);
    let bare = "document: {dsl: '1.0.4', namespace: t, name: bare, version: '1.0.0'}\ndo: [{turn: {call: agent_turn, with: {messages: '${ .messages }'}}}]\n";
    let refused = a.tool("save_workflow", json!({"name":"bare","definition":bare})).await.unwrap_err().to_string();
    assert!(refused.contains("requires a `call: start_guard`") && refused.contains("copy agent-loop"), "{refused}");
    let version = a.f.engine.runtime.save("bare", bare).await.unwrap().version;
    agent::flows::WorkflowMeta::record(
        &a.f.engine.deps.db,
        "bare",
        &ouro_runtime::durable::digest_of(bare),
        None,
        false,
    )
    .unwrap();
    mark_tried(&a.f.engine, "bare", bare);
    let refused = a.tool("activate_workflow", json!({"name":"bare","version":version})).await.unwrap_err();
    assert!(refused.to_string().contains("requires a `call: start_guard`"));
    // Steps alone need no guard, a loop's included.
    for (name, metadata) in [("steps", ""), ("step-loop", ", metadata: {loop: {description: Steps}}")] {
        let definition = format!(
            "document: {{dsl: '1.0.4', namespace: t, name: {name}, version: '1.0.0'{metadata}}}\ndo: [{{page: {{call: fetch_page, with: {{url: x}}}}}}]\n"
        );
        a.tool("save_workflow", json!({"name":name,"definition":definition})).await.unwrap();
    }
    // A run the runtime admits may declare the model's parameters; a
    // workflow that calls primary_loop is a signal override and declares none.
    let declared = "document: {dsl: '1.0.4', namespace: t, name: declared, version: '1.0.0', metadata: {agent: {preset: smartest, confirm: none}}}\ndo: [{page: {call: fetch_page, with: {url: x}}}]\n";
    a.tool("save_workflow", json!({"name":"declared","definition":declared})).await.unwrap();
    let typo = declared.replace("confirm: none", "confirm: nobody");
    assert!(a.tool("save_workflow", json!({"name":"declared","definition":typo})).await.is_err());
    let legacy = "document: {dsl: '1.0.4', namespace: t, name: legacy, version: '1.0.0', metadata: {agent: {preset: base}, route: {when: {source: peer}}}}\ndo: [{p: {call: primary_loop, with: {input: x}}}]\n";
    assert!(
        a.tool("save_workflow", json!({"name":"legacy","definition":legacy}))
            .await
            .unwrap_err()
            .to_string()
            .contains("primary_loop")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_task_copied_from_agent_loop_runs_as_the_primary_loop_and_falls_back_after_three_faults() {
    let mut script = vec![calls(&[("fetch_page", json!({"url":"https://c.example"}))]), assistant("Daily summary: C")];
    script.extend((1..=4).map(|n| assistant(&format!("handled {n}"))));
    let a = agent(script);
    // A copy of agent-loop under the task's own name, its metadata and all
    // but the breaker policy only the built-ins declare.
    let task = variant("daily-task");
    activate_definition(&a.f.engine, "daily-task", &task).await;
    let id = a.fire("daily-task", "Summarize https://c.example").await;
    let seen = a.model.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2, "two turns of the task's own loop");
    assert!(system(&seen[0]).contains("SCHEDULER") && system(&seen[0]).contains("Signal source: scheduler"));
    assert!(seen[0].messages[1]["content"].as_str().unwrap().contains("Summarize https://c.example"));
    let (trace, observations) = a.trace(&id);
    assert_eq!(trace.output, json!({"ok":true,"workflow":"daily-task","output":"Daily summary: C"}));
    let primary = observations.iter().find(|o| o.name == "agent_loop").unwrap();
    assert_eq!(primary.meta("activity"), Some(&json!("primary_loop")));
    assert_eq!(primary.output, json!({"ok":true,"workflow":"daily-task","output":"Daily summary: C"})["output"]);
    let turns = observations.iter().filter(|o| o.name.starts_with("iter-")).count();
    assert_eq!(turns, 2);
    assert!(!observations.iter().any(|o| o.name == "fallback"));
    let run = a.saved_run(&id).await;
    assert_eq!(run.admission.name, "daily-task");
    assert_eq!(run.admission.host().unwrap()["skills"], json!(["scheduler"]));

    // Three faults of its version in a row: the agent loop takes the task.
    let broken = faulting(&task);
    activate_definition(&a.f.engine, "daily-task", &broken).await;
    for n in 1..=4 {
        let id = a.fire("daily-task", "Summarize https://c.example").await;
        let (trace, observations) = a.trace(&id);
        assert_eq!(trace.output, json!(format!("handled {n}")), "the agent loop did the task");
        assert_eq!(trace.metadata.as_ref().unwrap()["fallback"], "agent-loop");
        let ran = observations.iter().any(|o| o.name == "runner" && o.meta("workflow") == Some(&json!("daily-task")));
        assert_eq!(ran, n <= 3, "fire {n}");
        let note = system(a.model.seen.lock().unwrap().last().unwrap()).to_owned();
        let expected = if n <= 3 { "failed before changing anything" } else { "faulted 3 times in a row" };
        assert!(note.contains(expected), "fire {n}: {note}");
    }
    let trip = a.f.engine.runtime.definitions("daily-task").await.unwrap().tripped.unwrap();
    assert_eq!((trip.faults, trip.breaker), (3, true));
    a.supervisor.shutdown().await;
}

// A scheduled task's admission: what its definition declares, else the
// scheduled defaults, pinned in the run's host data.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_workflow_declares_the_model_parameters_it_is_admitted_with() {
    let a = agent(vec![]);
    let declared = "document: {dsl: '1.0.4', namespace: t, name: declared, version: '1.0.0', metadata: {agent: {preset: smartest, confirm: none}}}\ndo: [{page: {call: fetch_page, with: {url: x}}}]\n";
    activate_definition(&a.f.engine, "declared", declared).await;
    let plain = declared
        .replace("name: declared", "name: plain")
        .replace(", metadata: {agent: {preset: smartest, confirm: none}}", "");
    activate_definition(&a.f.engine, "plain", &plain).await;
    for (name, preset, confirm) in
        [("declared", "smartest", Value::Null), ("plain", "base", json!({"chatId":"999","messageThreadId":null}))]
    {
        let id = a.fire(name, "go").await;
        let run = a.saved_run(&id).await;
        let host = run.admission.host().unwrap();
        assert_eq!(host["preset"], preset, "{name}");
        assert_eq!(host["confirm"], confirm, "{name}");
        assert_eq!(run.admission.input["signal"]["payload"], json!({"workflow":name}));
        assert!(run.admission.input["env"]["now"].is_string(), "the env block scheduled workflows read");
        assert!(matches!(run.outcome, Some(agent::runs::Status::Completed(_))), "{name}: {:?}", run.outcome);
    }
    assert!(a.model.seen.lock().unwrap().is_empty(), "steps alone never call the model");
    a.supervisor.shutdown().await;
}
