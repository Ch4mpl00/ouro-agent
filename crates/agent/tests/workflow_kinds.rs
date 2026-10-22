// One kind of workflow (#60): any workflow the runtime starts on a signal is
// admitted with the model's parameters, so it may mix deterministic steps
// with agent turns, and a recurring task can start as a copy of agent-loop.
// The guarded round goes wherever agent_turn does; a run a workflow starts
// itself is never admitted.
mod support;
use agent::{
    agent::AGENT_LOOP,
    db::TraceStore,
    durable::RunStore,
    flows::{FlowResult, WorkflowSignal, create_durable_flows},
    mcp::Mcp,
    providers::{ToolDef, assistant},
    schedule::{SupervisorSink, tick},
    session::EnvDeps,
    supervisor::{Supervisor, create_supervisor},
    trace_model::Observation,
    tracing::{Scope, TraceStart},
};
use async_trait::async_trait;
use serde_json::{Map, Value, json};
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
const LOOP_HEADER: &str = "name: agent-loop, version: '1.0.0', metadata: { loop: { description: 'General ReAct loop with guarded tools and workers.' } } }";

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
}

fn agent(script: Vec<agent::providers::Message>) -> Agent {
    let model = Arc::new(Scripted::new(script));
    let mut f = fixture(model.clone(), &[], SKILLS);
    let world = Arc::new(World::default());
    Arc::get_mut(&mut f.engine).unwrap().deps.mcp = world.clone();
    let supervisor = create_supervisor(
        f.engine.clone(),
        EnvDeps { mcp: world.clone(), memory: f.db.clone(), user_email: None, telegram_chat_id: Some("999".into()) },
        create_durable_flows(f.engine.clone(), (*f.db).clone()),
    );
    Agent { f, world, model, supervisor }
}

impl Agent {
    fn tool(&self, name: &str, args: Value) -> anyhow::Result<Value> {
        let reply = agent::flows::tool(&self.f.engine, name, args.as_object().unwrap())?;
        Ok(serde_json::from_str(&reply)?)
    }

    // Schedules the task as the agent would and fires it as the tick does;
    // the run id of the fire.
    async fn fire(&self, workflow: &str, prompt: &str) -> String {
        let store = &*self.f.engine.deps.schedule;
        let args = json!({"cron_expr":"now","recurring":false,"prompt":prompt,"workflow":workflow});
        let now = chrono::Utc::now();
        agent::schedule::tool(
            store,
            &*self.f.engine.deps.workflows,
            "UTC",
            "schedule_task",
            args.as_object().unwrap(),
            now,
        )
        .await
        .unwrap();
        let before = self.fires();
        let sink = SupervisorSink { supervisor: self.supervisor.clone(), env_context: None };
        assert_eq!(tick(store, &sink, chrono_tz::Tz::UTC, now).await.unwrap(), 1);
        let after = self.fires();
        after.into_iter().find(|id| !before.contains(id)).expect("a new fire")
    }

    // The signals the supervisor ran, by trace id.
    fn fires(&self) -> Vec<String> {
        self.f
            .db
            .list_recent(50, None)
            .unwrap()
            .into_iter()
            .map(|t| t.id)
            .filter(|id| id.starts_with("scheduler:"))
            .collect()
    }

    fn trace(&self, id: &str) -> (agent::trace_model::Trace, Vec<Observation>) {
        self.f.db.get_trace(id).unwrap().unwrap()
    }
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
      metadata: { concurrency: 4 }
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
    AGENT_LOOP.replace(LOOP_HEADER, "name: pages, version: '1.0.0' }").replacen("do:\n", &format!("do:\n{steps}"), 1)
}

const SEND: (&str, &str) = ("send_telegram_message", "Two pages: A and B");

fn send() -> agent::providers::Message {
    calls(&[(SEND.0, json!({"chatId":"999","text":SEND.1}))])
}

#[tokio::test]
async fn a_semi_deterministic_workflow_saves_tries_and_runs_on_a_scheduled_signal() {
    let a = agent(vec![send(), assistant("Sent the summary."), send(), assistant("Sent the summary.")]);
    let definition = pages();
    assert!(!definition.contains("metadata: { loop"), "not a loop: a scheduled task's workflow");
    let saved = a.tool("save_workflow", json!({"name":"pages","definition":definition})).unwrap();
    assert!(saved["next"].as_str().unwrap().contains("payload: {workflow: \"pages\"}"), "{saved}");
    // The scheduled form of a dry run has no admitted input for its turns.
    let trace =
        a.f.engine.deps.tracer.trace(TraceStart { id: "author".into(), name: "author".into(), ..Default::default() });
    let scope: Scope = trace.clone();
    let cancel = CancellationToken::new();
    let env = context().env.clone();
    let plain = json!({"name":"pages"});
    let plain = agent::flows::try_draft(&a.f.engine, plain.as_object().unwrap(), &env, &scope, &cancel).await;
    assert!(plain.unwrap_err().to_string().contains("takes agent turns"));

    // Dry run on a fire of its task: the same admission, writes mocked.
    let author = agent::session::session(
        "author",
        agent::session::EnvData { telegram_chat_id: Some("999".into()), ..context().env.clone() },
    );
    let args = json!({"name":"pages","signal":{"source":"scheduler","content":"Summarize the pages for me.","payload":{"workflow":"pages"}}});
    let report: Value = serde_json::from_str(
        &agent::sandbox::try_signal(&a.f.engine, args.as_object().unwrap(), &author, &scope, &cancel).await.unwrap(),
    )
    .unwrap();
    trace.end();
    assert_eq!(report["status"], "completed", "{report:#}");
    assert_eq!(report["drafts"], json!([{"name":"pages","version":1,"ran":true}]));
    assert_eq!(report["writes"], json!([{"function":SEND.0,"arguments":{"chatId":"999","text":SEND.1}}]));
    assert_eq!((report["ended"]["by"].as_str(), report["ended"]["turns"].as_u64()), (Some("answer"), Some(2)));
    assert!(a.world.called(SEND.0).is_empty(), "the dry run sent nothing");
    a.tool("activate_workflow", json!({"name":"pages","version":1})).unwrap();

    let id = a.fire("pages", "Summarize the pages for me.").await;
    let urls: Vec<Value> = a.world.called("fetch_page").iter().skip(2).map(|args| args["url"].clone()).collect();
    assert_eq!(urls.len(), 2, "each page read once by the steps, not by the model");
    assert!(urls.contains(&json!("https://a.example/1")) && urls.contains(&json!("https://b.example/2")));
    assert_eq!(a.world.called(SEND.0).len(), 1);
    let seen = a.model.seen.lock().unwrap().clone();
    let first = &seen[2];
    for part in ["Body of https://a.example/1", "Body of https://b.example/2", "SCHEDULER"] {
        assert!(system(first).contains(part), "{part}: {}", system(first));
    }
    assert!(first.messages[1]["content"].as_str().unwrap().contains("Summarize the pages for me."));
    assert_eq!(first.model, agent::models::default_presets().base.model, "a scheduled task's default preset");

    // One trace, one tree: the steps' spans and the model's turns under the signal's primary run.
    let (trace, observations) = a.trace(&id);
    assert_eq!(trace.output["output"], "Sent the summary.");
    assert_eq!(trace.tags, ["scheduler", "saved-workflow"]);
    let primary = observations.iter().find(|o| o.name == "agent_loop").unwrap();
    assert_eq!(primary.meta("activity"), Some(&json!("primary_loop")));
    let pages: Vec<&Observation> = observations.iter().filter(|o| o.name == "call:page").collect();
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
    let run = RunStore::new((*a.f.db).clone()).get(&id).unwrap().unwrap();
    assert_eq!((run.name.as_str(), run.input["loop"]["preset"].as_str()), ("pages", Some("base")));
    a.supervisor.shutdown().await;
}

#[tokio::test]
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
        activate_definition(&a.f.engine, "pages", &definition);
        let id = a.fire("pages", "Summarize the pages.").await;
        let (trace, observations) = a.trace(&id);
        let reads: Vec<_> = observations.iter().filter(|o| o.meta("tool") == Some(&json!("fetch_page"))).collect();
        assert_eq!(reads.len(), 2, "metadata.loop={loop_metadata}: each fixed call has one span");
        for read in reads {
            let url = read.input["url"].as_str().unwrap();
            assert_eq!(read.meta("workflow_run"), Some(&json!(id)));
            let raw: Value = serde_json::from_str(read.output.as_str().unwrap()).unwrap();
            assert_eq!(raw, json!({"url":url,"body":format!("Body of {url}")}));
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
        let runner =
            observations.iter().find(|o| o.name == "runner" && o.meta("workflow") == Some(&json!("pages"))).unwrap();
        assert!(runner.input["loop"].is_object(), "fixed calls preserve the admitted input");
        if !loop_metadata {
            let tasks: Vec<_> = observations.iter().filter(|o| o.name == "call:page").collect();
            assert_eq!(tasks.len(), 2);
            for task in tasks {
                assert!(task.output.get("url").is_none(), "task output is transformed, tool reply stays intact");
                assert!(task.meta("tool_span").is_some());
            }
        }
        let packet = agent::activity::project_trace(&trace, &observations, None, &Default::default());
        assert_eq!(packet["actions"].as_array().unwrap().iter().filter(|a| a["tool"] == "fetch_page").count(), 2);
        assert!(
            !packet["actions"].as_array().unwrap().iter().any(|a| a["tool"] == "call:page"),
            "the activity journal records the call, not its task wrapper again"
        );
        a.supervisor.shutdown().await;
    }
}

#[tokio::test]
async fn agent_turns_need_the_guarded_round_and_steps_alone_need_none() {
    let a = agent(vec![]);
    let bare = "document: {dsl: '1.0.4', namespace: t, name: bare, version: '1.0.0'}\ndo: [{turn: {call: agent_turn, with: {messages: '${ .messages }'}}}]\n";
    let refused = a.tool("save_workflow", json!({"name":"bare","definition":bare})).unwrap_err().to_string();
    assert!(refused.contains("requires a `call: start_child`") && refused.contains("copy agent-loop"), "{refused}");
    let stored = a.f.engine.deps.workflows.save("bare", bare, None).unwrap();
    a.f.engine.deps.workflows.mark_tried("bare", stored.version).unwrap();
    let refused = a.tool("activate_workflow", json!({"name":"bare","version":stored.version})).unwrap_err();
    assert!(refused.to_string().contains("requires a `call: start_child`"));
    // Steps alone need no guard, a loop's included.
    for (name, metadata) in [("steps", ""), ("step-loop", ", metadata: {loop: {description: Steps}}")] {
        let definition = format!(
            "document: {{dsl: '1.0.4', namespace: t, name: {name}, version: '1.0.0'{metadata}}}\ndo: [{{page: {{call: fetch_page, with: {{url: x}}}}}}]\n"
        );
        a.tool("save_workflow", json!({"name":name,"definition":definition})).unwrap();
    }
    // A run the runtime admits may declare the model's parameters; a signal
    // route that calls primary_loop passes them there instead.
    let declared = "document: {dsl: '1.0.4', namespace: t, name: declared, version: '1.0.0', metadata: {agent: {preset: smartest, confirm: none}}}\ndo: [{page: {call: fetch_page, with: {url: x}}}]\n";
    a.tool("save_workflow", json!({"name":"declared","definition":declared})).unwrap();
    let typo = declared.replace("confirm: none", "confirm: nobody");
    assert!(a.tool("save_workflow", json!({"name":"declared","definition":typo})).is_err());
    let legacy = "document: {dsl: '1.0.4', namespace: t, name: legacy, version: '1.0.0', metadata: {agent: {preset: base}, route: {when: {source: peer}}}}\ndo: [{p: {call: primary_loop, with: {input: x}}}]\n";
    assert!(
        a.tool("save_workflow", json!({"name":"legacy","definition":legacy}))
            .unwrap_err()
            .to_string()
            .contains("primary_loop")
    );
}

// The model's parameters are the host's: a child a workflow starts itself is
// never admitted, and cannot be handed a spec through its input.
#[tokio::test]
async fn a_child_a_workflow_starts_is_never_admitted() {
    let a = agent(vec![]);
    let activate = |name: &str, body: &str| {
        let definition = format!("document: {{dsl: '1.0.4', namespace: t, name: {name}, version: '1.0.0'}}\n{body}");
        let saved = a.f.engine.deps.workflows.save(name, &definition, None).unwrap();
        a.f.engine.deps.workflows.activate(name, saved.version).unwrap();
    };
    activate("child", "do: [{turn: {call: agent_turn, with: {messages: []}}}]\n");
    activate(
        "forger",
        "do: [{start: {call: start_child, with: {workflow: child, key: one, input: {loop: '${ $workflow.input.loop }'}}}}]\n",
    );
    activate(
        "parent",
        r#"do:
 - start: { call: start_child, with: { workflow: child, key: one }, export: { as: '${ . }' } }
 - wait: { listen: { to: { one: { with: { type: '${ "done:" + $context.run }' } } } }, metadata: { hold: PT5S } }
 - result: { call: child_outcome, with: { run: '${ $context.run }', key: one } }
"#,
    );
    let flows = create_durable_flows(a.f.engine.clone(), (*a.f.db).clone());
    let run = |name: &'static str, id: i64| {
        let flows = flows.clone();
        async move {
            let signal = WorkflowSignal { id, source: "scheduler".into(), content: "go".into(), env_context: None };
            let scope = agent::tracing::null_scope();
            flows.run(name, &signal, &task_context(&format!("scheduler:{id}")), &scope, &CancellationToken::new()).await
        }
    };
    let FlowResult::Faulted { error, .. } = run("forger", 1).await else { panic!("a forged spec was taken") };
    assert!(error.to_string().contains("cannot carry `loop`"), "{error}");
    let FlowResult::Completed(outcome) = run("parent", 2).await else { panic!("the parent did not end") };
    assert!(outcome.to_string().contains("requires the host's admitted model input"), "{outcome}");
    assert!(a.model.seen.lock().unwrap().is_empty(), "no turn was taken");
}

#[tokio::test]
async fn a_task_copied_from_agent_loop_runs_as_the_primary_loop_and_falls_back_after_three_faults() {
    let mut script = vec![calls(&[("fetch_page", json!({"url":"https://c.example"}))]), assistant("Daily summary: C")];
    script.extend((1..=4).map(|n| assistant(&format!("handled {n}"))));
    let a = agent(script);
    // A copy of agent-loop under the task's own name, its metadata and all.
    let task = AGENT_LOOP.replace("name: agent-loop,", "name: daily-task,");
    activate_definition(&a.f.engine, "daily-task", &task);
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
    let run = RunStore::new((*a.f.db).clone()).get(&id).unwrap().unwrap();
    assert_eq!(run.name, "daily-task");
    assert_eq!(run.input["loop"]["skills"], json!(["scheduler"]));

    // Three faults of its version in a row: the agent loop takes the task.
    let broken = task.replacen(
        "do:\n",
        "do:\n  - broken: { raise: { error: { type: 'https://example.com/fault', status: 500 } } }\n",
        1,
    );
    activate_definition(&a.f.engine, "daily-task", &broken);
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
    let breaker: Value =
        serde_json::from_str(&a.f.engine.deps.memory.get("daily_task.breaker").unwrap().unwrap()).unwrap();
    assert_eq!((breaker["faults"].as_u64(), breaker["open"].as_bool()), (Some(3), Some(true)));
    a.supervisor.shutdown().await;
}

// A scheduled task's admission: what its definition declares, else the
// scheduled defaults, pinned in the run's input.
#[tokio::test]
async fn a_workflow_declares_the_model_parameters_it_is_admitted_with() {
    let a = agent(vec![]);
    let declared = "document: {dsl: '1.0.4', namespace: t, name: declared, version: '1.0.0', metadata: {agent: {preset: smartest, confirm: none}}}\ndo: [{page: {call: fetch_page, with: {url: x}}}]\n";
    activate_definition(&a.f.engine, "declared", declared);
    let plain = declared
        .replace("name: declared", "name: plain")
        .replace(", metadata: {agent: {preset: smartest, confirm: none}}", "");
    activate_definition(&a.f.engine, "plain", &plain);
    let runs = RunStore::new((*a.f.db).clone());
    for (name, preset, confirm) in
        [("declared", "smartest", Value::Null), ("plain", "base", json!({"chatId":"999","messageThreadId":null}))]
    {
        let id = a.fire(name, "go").await;
        let run = runs.get(&id).unwrap().unwrap();
        assert_eq!(run.input["loop"]["preset"], preset, "{name}");
        assert_eq!(run.input["loop"]["confirm"], confirm, "{name}");
        assert_eq!(run.input["signal"]["payload"], json!({"workflow":name}));
        assert!(run.input["env"]["now"].is_string(), "the env block scheduled workflows read");
        assert_eq!(run.outcome.as_ref().map(|o| matches!(o, owf::run::Status::Completed(_))), Some(true), "{name}");
    }
    assert!(a.model.seen.lock().unwrap().is_empty(), "steps alone never call the model");
    a.supervisor.shutdown().await;
}
