// PARKED (not compiled), for the sandbox step (design 01, step 4): the dry
// run (`try_draft`, `try_signal`) of a scheduled task's semi-deterministic
// workflow. The versions step (design 04) restored the rest of this suite in
// tests/workflow_kinds.rs, this test's run included (activation there marks
// the content tried as this dry run would). a_child_a_workflow_starts_is_never_admitted
// is covered on the engine by engine_runs.rs a_loop_a_workflow_starts_itself_is_never_admitted.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
