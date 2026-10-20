// The agent loop as a durable `agent-loop` run (`agent.rs` section 4,
// `flows.rs` `run_loop`): it answers like the in-process loop, and a process
// that stops in the middle of a round is resumed by the next one from the
// run's checkpoints, without a second generation of that round and without
// sending a write again.
mod support;
use agent::{
    agent::{Engine, EngineDeps, LoopOptions, Providers, create_engine},
    db::AgentDb,
    durable::RunStore,
    flows::{FlowResult, FlowRun, MemoryWorkflows, create_durable_flows, create_flows},
    mcp::Mcp,
    models::{ModelPreset, Presets, ReasoningEffort},
    providers::{ToolDef, assistant},
    tracing::{Scope, TraceStart},
};
use async_trait::async_trait;
use owf::run::Status;
use serde_json::{Map, Value, json};
use std::sync::{Arc, Mutex};
use support::{AnySkill, NoCodex, PersistLog, Scripted, calls, recording, task_context};
use tokio_util::sync::CancellationToken;

// `search` is read-only; `send` changes the world. With `stop` armed, a
// `send` stops the process (cancels the run) and never answers.
struct World {
    calls: Mutex<Vec<String>>,
    stop: Option<CancellationToken>,
}

#[async_trait]
impl Mcp for World {
    fn tools(&self) -> Vec<ToolDef> {
        let schema = json!({"type": "object", "properties": {"q": {}}});
        vec![ToolDef::new("search", "search", schema.clone()), ToolDef::new("send", "send", schema)]
    }
    fn read_only(&self, name: &str) -> bool {
        name == "search"
    }
    async fn call_tool(&self, name: &str, _: Map<String, Value>, _: &CancellationToken) -> anyhow::Result<String> {
        self.calls.lock().unwrap().push(name.into());
        if name == "send"
            && let Some(stop) = &self.stop
        {
            stop.cancel();
            std::future::pending::<()>().await;
        }
        Ok(format!("{name} ok"))
    }
}

fn engine(llm: Arc<Scripted>, mcp: Arc<World>) -> Arc<Engine> {
    let p = |model: &str| ModelPreset { model: model.into(), reasoning_effort: ReasoningEffort::Disabled };
    let (tracer, _) = recording();
    create_engine(EngineDeps {
        providers: Providers { openai: llm.clone(), gemini: llm.clone(), deepseek: llm, openrouter: None },
        mcp,
        presets: Presets {
            base: p("gpt-5.4-mini"),
            smart: p("gpt-5.4"),
            smartest: p("gpt-5.4"),
            compiler: p("gpt-5.4"),
        },
        skills: Vec::new(),
        skill_store: Arc::new(AnySkill),
        memory: Arc::new(PersistLog::default()),
        tracer,
        codex: Arc::new(NoCodex),
        decisions: None,
        workflows: Arc::new(MemoryWorkflows::default()),
        schedule: Arc::new(agent::schedule::MemorySchedule::default()),
    })
}

async fn input(engine: &Arc<Engine>, id: &str) -> Value {
    let mut opts = LoopOptions::new(id, task_context(id));
    opts.skills = vec!["any".into()];
    opts.include_engine_skills = false;
    let started = engine.start_loop(opts).await.unwrap();
    started.workflow_input("go").await
}

fn scope(engine: &Engine) -> Scope {
    engine.deps.tracer.trace(TraceStart { id: "t".into(), name: "t".into(), ..Default::default() })
}

#[tokio::test]
async fn a_loop_run_answers_like_the_loop_and_settles_its_rounds() {
    let llm = Arc::new(Scripted::new(vec![
        calls(&[("search", json!({"q": "rust"})), ("working_memory_put", json!({"key": "note", "value": "kept"}))]),
        calls(&[("working_memory_get", json!({"key": "note"}))]),
        calls(&[("finish", json!({"result": "all done"}))]),
    ]));
    let world = Arc::new(World { calls: Mutex::default(), stop: None });
    let engine = engine(llm.clone(), world.clone());
    let flows = create_flows(engine.clone());
    let input = input(&engine, "peer:1").await;
    let result = flows.run_loop("peer:1/loop", input, &scope(&engine), &CancellationToken::new()).await;
    let FlowResult::Completed(output) = result else { panic!("{result:?}") };
    assert_eq!(output, json!("all done"), "finish ends the run with its result");
    assert_eq!(*world.calls.lock().unwrap(), ["search"]);
    // The second turn saw the first round's replies, and the memory a call
    // of that round put, read back in the next one.
    let seen = llm.seen.lock().unwrap();
    let second = serde_json::to_string(&seen[1].messages).unwrap();
    assert!(second.contains("search ok"), "{second}");
    assert_eq!(seen[1].messages.iter().filter(|m| m["role"] == "tool").count(), 2);
    let third = &seen[2].messages;
    assert_eq!(third.last().unwrap()["content"], "kept", "{third:?}");
}

#[tokio::test]
async fn a_plain_answer_ends_the_run() {
    let llm = Arc::new(Scripted::new(vec![assistant("hello")]));
    let engine = engine(llm, Arc::new(World { calls: Mutex::default(), stop: None }));
    let input = input(&engine, "peer:2").await;
    let result =
        create_flows(engine.clone()).run_loop("peer:2/loop", input, &scope(&engine), &CancellationToken::new()).await;
    assert!(matches!(result, FlowResult::Completed(ref v) if v == "hello"), "{result:?}");
}

#[tokio::test]
async fn a_round_cut_by_a_restart_resumes_without_regenerating_or_resending() {
    let dir = tempfile::tempdir().unwrap();
    let db = AgentDb::open(dir.path().join("agent.db")).unwrap();
    let stop = CancellationToken::new();

    // The first process: the model asks for a read and a write; the write
    // takes the process down before it answers.
    let first = Arc::new(Scripted::new(vec![calls(&[("search", json!({"q": "a"})), ("send", json!({"q": "b"}))])]));
    let world = Arc::new(World { calls: Mutex::default(), stop: Some(stop.clone()) });
    let engine1 = engine(first.clone(), world.clone());
    let input = input(&engine1, "peer:3").await;
    let result =
        create_durable_flows(engine1.clone(), db.clone()).run_loop("peer:3/loop", input, &scope(&engine1), &stop).await;
    assert!(matches!(result, FlowResult::Interrupted { .. } | FlowResult::Waiting { .. }), "{result:?}");

    // The next process resumes it from the checkpoints.
    let second = Arc::new(Scripted::new(vec![assistant("checked, all done")]));
    let world2 = Arc::new(World { calls: Mutex::default(), stop: None });
    let engine2 = engine(second.clone(), world2.clone());
    let resumed = create_durable_flows(engine2.clone(), db.clone()).recover_once(&CancellationToken::new()).await;
    assert_eq!(resumed.unwrap(), 1);
    let run = RunStore::new(db).get("peer:3/loop").unwrap().unwrap();
    assert_eq!(run.outcome, Some(Status::Completed(json!("checked, all done"))));

    assert_eq!(first.seen.lock().unwrap().len(), 1);
    assert_eq!(second.seen.lock().unwrap().len(), 1, "the cut round was not generated again");
    assert_eq!(world.calls.lock().unwrap().iter().filter(|n| *n == "send").count(), 1);
    assert!(!world2.calls.lock().unwrap().contains(&"send".to_owned()), "the write is never sent again");
    let seen = serde_json::to_string(&second.seen.lock().unwrap()[0].messages).unwrap();
    assert!(seen.contains("outcome unknown: send"), "the model hears the write may have run: {seen}");
    assert!(seen.contains("search ok"), "the read has its reply: {seen}");
}
