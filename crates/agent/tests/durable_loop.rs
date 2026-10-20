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
    engine_with(llm, mcp, Arc::new(PersistLog::default()))
}

fn engine_with(llm: Arc<Scripted>, mcp: Arc<World>, memory: Arc<dyn agent::db::MemoryStore>) -> Arc<Engine> {
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
        memory,
        tracer,
        codex: Arc::new(NoCodex),
        decisions: Some(support::allow_guard()),
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

// A durable loop's sub-agent is a durable run of its own, keyed by the call
// that started it. A restart in the middle of the sub-agent's round resumes
// that run; the resumed parent joins it instead of starting a second worker,
// and neither regenerates a turn nor resends the child's write.
#[tokio::test]
async fn a_sub_agent_cut_by_a_restart_is_resumed_and_joined_by_its_parent() {
    let dir = tempfile::tempdir().unwrap();
    let db = AgentDb::open(dir.path().join("agent.db")).unwrap();
    let stop = CancellationToken::new();

    // The first process: the parent delegates; the worker reads and writes,
    // and the write takes the process down.
    let first = Arc::new(Scripted::new(vec![
        calls(&[("invoke_sub_agent", json!({"prompt": "post it"}))]),
        calls(&[("search", json!({"q": "a"})), ("send", json!({"q": "b"}))]),
    ]));
    let world = Arc::new(World { calls: Mutex::default(), stop: Some(stop.clone()) });
    let engine1 = engine(first.clone(), world.clone());
    let mut opts = LoopOptions::new("peer:4", task_context("peer:4"));
    opts.skills = vec!["any".into()];
    opts.include_engine_skills = false;
    opts.cancel = stop.clone();
    let input = engine1.start_loop(opts).await.unwrap().workflow_input("go").await;
    let result =
        create_durable_flows(engine1.clone(), db.clone()).run_loop("peer:4/loop", input, &scope(&engine1), &stop).await;
    assert!(matches!(result, FlowResult::Interrupted { .. } | FlowResult::Waiting { .. }), "{result:?}");
    let runs = RunStore::new(db.clone());
    let child = "peer:4__sub1.0/loop";
    assert!(runs.get(child).unwrap().unwrap().outcome.is_none());
    assert!(runs.due().unwrap().contains(&child.to_owned()), "the stopped parent handed its worker back at once");

    // The next process resumes both; the parent waits for its worker.
    let second = Arc::new(Scripted::new(vec![assistant("worker result"), assistant("all done")]));
    let world2 = Arc::new(World { calls: Mutex::default(), stop: None });
    let engine2 = engine(second.clone(), world2.clone());
    let resumed = create_durable_flows(engine2.clone(), db.clone()).recover_once(&CancellationToken::new()).await;
    assert_eq!(resumed.unwrap(), 2);
    assert_eq!(runs.get(child).unwrap().unwrap().outcome, Some(Status::Completed(json!("worker result"))));
    assert_eq!(runs.get("peer:4/loop").unwrap().unwrap().outcome, Some(Status::Completed(json!("all done"))));

    assert_eq!(first.seen.lock().unwrap().len(), 2);
    let seen = second.seen.lock().unwrap();
    assert_eq!(seen.len(), 2, "no turn was generated again");
    let worker = serde_json::to_string(&seen[0].messages).unwrap();
    assert!(worker.contains("outcome unknown: send"), "the worker hears its write may have run: {worker}");
    let parent = serde_json::to_string(&seen[1].messages).unwrap();
    assert!(parent.contains("worker result"), "the parent has its worker's answer: {parent}");
    assert_eq!(world.calls.lock().unwrap().iter().filter(|n| *n == "send").count(), 1);
    assert!(!world2.calls.lock().unwrap().contains(&"send".to_owned()), "the write is never sent again");
}

// The agent may replace the loop: the version it activates under
// `agent-loop` drives every loop started after, with no dry run needed.
// LOOP_BREAKER faults in a row of that version put the built-in loop back.
#[tokio::test]
async fn an_activated_agent_loop_runs_and_three_faults_in_a_row_put_the_built_in_one_back() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(AgentDb::open(dir.path().join("agent.db")).unwrap());
    let replies = (0..6).map(|_| assistant("hello")).collect();
    let llm = Arc::new(Scripted::new(replies));
    let engine = engine_with(llm, Arc::new(World { calls: Mutex::default(), stop: None }), db);
    let activate = |definition: &str| {
        let saved = engine.deps.workflows.save("agent-loop", definition, None).unwrap();
        let args = json!({"name": "agent-loop", "version": saved.version});
        agent::flows::tool(&engine, "activate_workflow", args.as_object().unwrap()).unwrap()
    };
    let run = |n: usize| {
        let engine = engine.clone();
        async move {
            let id = format!("peer:{n}");
            let input = input(&engine, &id).await;
            let result =
                create_flows(engine.clone()).run_loop(&id, input, &scope(&engine), &CancellationToken::new()).await;
            engine.end_loop(&id).await;
            result
        }
    };

    let louder = agent::agent::AGENT_LOOP.replace("output: .content", r#"output: ("v2 " + .content)"#);
    assert_ne!(louder, agent::agent::AGENT_LOOP);
    activate(&louder);
    assert!(matches!(run(1).await, FlowResult::Completed(ref v) if v == "v2 hello"));

    let broken = "document: { dsl: '1.0.4', namespace: agent, name: agent-loop, version: '9.0.0' }\n\
        do: [ { boom: { raise: { error: { type: 'https://example.com/boom', status: 500 } } } }, { guard: { call: guard, with: { loop: '${ $context.loop }', calls: [] } } } ]\n";
    activate(broken);
    for n in 2..5 {
        assert!(matches!(run(n).await, FlowResult::Faulted { .. }), "fault {}", n - 1);
    }
    assert!(matches!(run(5).await, FlowResult::Completed(ref v) if v == "hello"), "the built-in loop is back");
    // Activating a version again closes the breaker for it.
    activate(&louder);
    assert!(matches!(run(6).await, FlowResult::Completed(ref v) if v == "v2 hello"));
}

#[tokio::test]
async fn a_parent_reads_what_its_durable_worker_did() {
    let dir = tempfile::tempdir().unwrap();
    let db = AgentDb::open(dir.path().join("agent.db")).unwrap();
    let child = "peer:history__sub1.0/loop";
    let llm = Arc::new(Scripted::new(vec![
        calls(&[("invoke_sub_agent", json!({"prompt": "check the violet telescope"}))]),
        calls(&[("search", json!({"q": "violet telescope", "after": "2026-10-19", "sources": ["Habr"]}))]),
        assistant("worker found three telescopes"),
        calls(&[("read_run", json!({}))]),
        calls(&[("read_run", json!({"id": child}))]),
        calls(&[("read_run", json!({"id": "peer:14__sub1/loop"}))]),
        assistant("checked the worker's evidence"),
    ]));
    let world = Arc::new(World { calls: Mutex::default(), stop: None });
    let engine = engine(llm.clone(), world.clone());
    let input = input(&engine, "peer:history").await;
    let result = create_durable_flows(engine.clone(), db)
        .run_loop("peer:history/loop", input, &scope(&engine), &CancellationToken::new())
        .await;
    assert!(matches!(result, FlowResult::Completed(_)), "{result:?}");
    assert_eq!(*world.calls.lock().unwrap(), ["search"], "read_run never goes to MCP");
    assert_eq!(agent::agent::tool_effect(&engine, "read_run"), owf::run::Effect::Read);
    let seen = llm.seen.lock().unwrap();
    assert!(seen[1].tools.iter().any(|tool| tool.name == "read_run"), "workers can read history too");
    let delegated = seen[3].messages.last().unwrap()["content"].as_str().unwrap();
    let delegated: Value = serde_json::from_str(delegated).unwrap();
    assert_eq!(delegated["worker_run"], child, "the reply names the worker's run");
    assert_eq!(delegated["content"], "worker found three telescopes", "beside the answer, not in it");
    let listing = seen[4].messages.last().unwrap()["content"].as_str().unwrap();
    for expected in [child, "completed", "check the violet telescope", "worker found three telescopes"] {
        assert!(listing.contains(expected), "{listing}");
    }
    let transcript = seen[5].messages.last().unwrap()["content"].as_str().unwrap();
    for expected in
        [child, "search", "violet telescope", "2026-10-19", "Habr", "search ok", "worker found three telescopes"]
    {
        assert!(transcript.contains(expected), "{transcript}");
    }
    assert!(!transcript.contains("You are a focused worker"), "system messages stay hidden");
    let guessed = seen[6].messages.last().unwrap()["content"].as_str().unwrap();
    assert!(guessed.contains(child), "a guessed id is answered with the real ones: {guessed}");
}

async fn read_history(engine: &Arc<Engine>, input: &Value, runs: Option<&RunStore>, args: Value) -> Value {
    agent::agent::loop_tool(
        engine,
        &json!({
            "loop": input["loop"],
            "call": {"id": "history", "name": "read_run", "arguments": args.to_string()},
        }),
        &scope(engine),
        &CancellationToken::new(),
        runs,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn read_run_refuses_other_workflows_and_explains_absent_history() {
    let dir = tempfile::tempdir().unwrap();
    let runs = RunStore::new(AgentDb::open(dir.path().join("agent.db")).unwrap());
    runs.admit("other", "digest", "unused", &json!({})).unwrap();
    runs.admit("empty", "agent-loop", "unused", &json!({})).unwrap();
    let engine = engine(Arc::new(Scripted::new(vec![])), Arc::new(World { calls: Mutex::default(), stop: None }));
    let input = input(&engine, "reader").await;
    let refused = read_history(&engine, &input, Some(&runs), json!({"id": "other"})).await;
    assert_eq!(refused["failed"], true);
    assert!(refused["presented"].as_str().unwrap().contains("only reads agent-loop"));
    let missing = read_history(&engine, &input, Some(&runs), json!({"id": "absent"})).await;
    assert!(missing["presented"].as_str().unwrap().contains("In-process workers have no run history"));
    let empty = read_history(&engine, &input, Some(&runs), json!({"id": "empty"})).await;
    assert!(empty["presented"].as_str().unwrap().contains("No transcript records"));
    for args in [json!({"limit": 0}), json!({"offset": -1}), json!({"limit": 101}), json!({"id": ""})] {
        assert_eq!(read_history(&engine, &input, Some(&runs), args).await["failed"], true);
    }
}

#[tokio::test]
async fn an_in_process_loop_reports_no_run_history() {
    let llm = Arc::new(Scripted::new(vec![calls(&[("read_run", json!({}))]), assistant("done")]));
    let engine = engine(llm.clone(), Arc::new(World { calls: Mutex::default(), stop: None }));
    let input = input(&engine, "in-process").await;
    let result = create_flows(engine.clone())
        .run_loop("in-process/loop", input, &scope(&engine), &CancellationToken::new())
        .await;
    assert!(matches!(result, FlowResult::Completed(_)), "{result:?}");
    let seen = llm.seen.lock().unwrap();
    assert!(seen[1].messages.last().unwrap()["content"].as_str().unwrap().contains("No run history is available"));
}

#[tokio::test]
async fn read_run_pages_the_longest_checkpoint_and_bounds_text_and_failed_replies() {
    use owf::durable::{Journal, Record, StepOutcome, TaskResult};
    let dir = tempfile::tempdir().unwrap();
    let db = AgentDb::open(dir.path().join("agent.db")).unwrap();
    let runs = RunStore::new(db);
    let engine = engine(Arc::new(Scripted::new(vec![])), Arc::new(World { calls: Mutex::default(), stop: None }));
    let input = input(&engine, "reader_%").await;
    let id = "reader_%__sub1.0/loop";
    let mut messages = vec![
        json!({"role": "system", "content": "hidden instructions"}),
        json!({"role": "user", "content": "original task"}),
    ];
    let lease = runs.start(id, "agent-loop", "unused", &json!({"messages": messages})).unwrap().unwrap();
    let checkpoint = |messages: Vec<Value>| Record::Task {
        started_ms: 0,
        result: Some(TaskResult {
            context: json!({"messages": messages}),
            outcome: StepOutcome::Completed { output: Value::Null, flow: owf::model::Flow::Continue },
        }),
    };
    for index in 0..20 {
        let mut turn = calls(&[("search", json!({"q": "界".repeat(900), "tail": "argument tail"}))]);
        turn.insert("content".into(), json!(format!("assistant-{index} {} assistant tail", "я".repeat(900))));
        messages.push(json!(turn));
        messages.push(json!({"role": "tool", "tool_call_id": "call-0", "content": json!({
            "memory_key": "tool.example", "content": format!("[tool error] reply-{index} {} reply tail", "🎯".repeat(900)),
        }).to_string()}));
    }
    lease.save("a-longest", &checkpoint(messages.clone())).unwrap();
    lease.save("z-shorter", &checkpoint(messages[..4].to_vec())).unwrap();
    lease.finish(&Status::Completed(json!("complete answer"))).unwrap();
    let page = read_history(&engine, &input, Some(&runs), json!({"id": id, "offset": 1, "limit": 1})).await;
    let text = page["presented"].as_str().unwrap();
    for expected in ["Turn 1", "assistant-1", "reply-1", "[FAILED]", "[cut]", "offset=2", "complete answer"] {
        assert!(text.contains(expected), "{text}");
    }
    for absent in ["hidden instructions", "Turn 0", "Turn 2", "argument tail", "assistant tail", "reply tail"] {
        assert!(!text.contains(absent), "{absent} leaked: {text}");
    }
    let mut offset = 0;
    while offset < 20 {
        let page = read_history(&engine, &input, Some(&runs), json!({"id": id, "offset": offset, "limit": 100})).await;
        let text = page["presented"].as_str().unwrap();
        assert!(text.chars().count() <= 8000, "{} characters", text.chars().count());
        assert!(page["stored"].is_null(), "bounded replies bypass working-memory previews");
        let count = text.lines().filter(|line| line.starts_with("Turn ")).count();
        assert!(count > 0);
        for index in offset..offset + count {
            assert!(text.contains(&format!("Turn {index}\n")));
        }
        offset += count;
        if offset < 20 {
            assert!(text.contains(&format!("offset={offset},")));
        }
    }
    let beyond = read_history(&engine, &input, Some(&runs), json!({"id": id, "offset": 200})).await;
    assert!(!beyond["presented"].as_str().unwrap().contains("Turn "));

    // Literal prefixes and workflow names keep unrelated runs out of the list.
    for (other, name) in [
        ("readerXX__sub2.0/loop", "agent-loop"),
        ("reader_%__sub3.0/loop", "digest"),
        ("reader_%__sub1.0__sub1.0/loop", "agent-loop"),
    ] {
        runs.admit(other, name, "unused", &json!({})).unwrap();
    }
    let list = read_history(&engine, &input, Some(&runs), json!({})).await;
    assert!(list["presented"].as_str().unwrap().contains("Worker runs: 1"));
    assert!(list["presented"].as_str().unwrap().contains("original task"));
    assert_eq!(runs.sweep(0).unwrap(), 2);
    let swept = read_history(&engine, &input, Some(&runs), json!({"id": id})).await;
    let text = swept["presented"].as_str().unwrap();
    assert!(text.contains("complete answer") && text.contains("swept"), "{text}");
    let list = read_history(&engine, &input, Some(&runs), json!({})).await;
    assert!(list["presented"].as_str().unwrap().contains("original task"));
}

#[tokio::test]
async fn read_run_bounds_worker_lists_and_an_oversized_parallel_turn() {
    use owf::durable::{Journal, Record, StepOutcome, TaskResult};
    let dir = tempfile::tempdir().unwrap();
    let runs = RunStore::new(AgentDb::open(dir.path().join("agent.db")).unwrap());
    let engine = engine(Arc::new(Scripted::new(vec![])), Arc::new(World { calls: Mutex::default(), stop: None }));
    let input = input(&engine, "reader").await;
    let long = "🌱".repeat(1000);
    let tool_calls: Vec<_> = (0..20).map(|i| ("search", json!({"q": long, "index": i}))).collect();
    let messages = json!([
        {"role": "user", "content": long},
        calls(&[("search", json!({"q": "first"}))]),
        {"role": "tool", "tool_call_id": "call-0", "content": json!({
            "memory_key": "tool.long-error", "truncated": true, "preview": "[tool error] a long failure",
        }).to_string()},
        calls(&tool_calls),
        assistant("last turn"),
    ]);
    for i in 0..20 {
        let id = format!("reader__sub1.{i:02}/loop");
        let lease = runs.start(&id, "agent-loop", "unused", &json!({"messages": messages})).unwrap().unwrap();
        lease
            .save(
                "checkpoint",
                &Record::Task {
                    started_ms: 0,
                    result: Some(TaskResult {
                        context: json!({"messages": messages}),
                        outcome: StepOutcome::Completed { output: Value::Null, flow: owf::model::Flow::Continue },
                    }),
                },
            )
            .unwrap();
        lease.finish(&Status::Completed(json!(long))).unwrap();
    }
    let list = read_history(&engine, &input, Some(&runs), json!({"limit": 100})).await;
    let text = list["presented"].as_str().unwrap();
    assert!(text.chars().count() <= 8000);
    let count = text.lines().filter(|line| line.starts_with("Run: ")).count();
    assert!(count > 0 && count < 20);
    assert!(text.contains(&format!("offset={count},")), "{text}");
    let next = read_history(&engine, &input, Some(&runs), json!({"offset": count, "limit": 1})).await;
    assert!(next["presented"].as_str().unwrap().contains(&format!("reader__sub1.{count:02}/loop")));
    let id = "reader__sub1.00/loop";
    let first = read_history(&engine, &input, Some(&runs), json!({"id": id, "limit": 1})).await;
    assert!(first["presented"].as_str().unwrap().contains("[FAILED] [stored preview]: [tool error] a long failure"));
    let mut entry_offset = 0;
    let mut viewed = String::new();
    loop {
        let huge = read_history(
            &engine,
            &input,
            Some(&runs),
            json!({
                "id": id, "offset": 1, "entry_offset": entry_offset, "limit": 1,
            }),
        )
        .await;
        let text = huge["presented"].as_str().unwrap();
        assert!(text.chars().count() <= 8000);
        viewed.push_str(text);
        if !text.contains("More in this turn:") {
            assert!(text.contains("offset=2,"), "{text}");
            break;
        }
        let next: usize = text.split("entry_offset=").nth(1).unwrap().split(',').next().unwrap().parse().unwrap();
        assert!(next > entry_offset, "continuations always progress");
        entry_offset = next;
    }
    assert!(entry_offset > 0, "the parallel turn needed more than one page");
    for index in 0..20 {
        assert_eq!(viewed.matches(&format!("call call-{index} search:")).count(), 1, "no calls lost or repeated");
    }
    let last = read_history(&engine, &input, Some(&runs), json!({"id": id, "offset": 2})).await;
    assert!(last["presented"].as_str().unwrap().contains("last turn"));
}

// ── host guard ──────────────────────────────────────────────────────────────
fn guarded(
    script: Vec<agent::providers::Message>,
    label: &str,
) -> (Arc<Engine>, Arc<World>, Arc<support::GuardApi>, Arc<Scripted>) {
    let model = Arc::new(Scripted::new(script));
    let world = Arc::new(World { calls: Mutex::default(), stop: None });
    let mut engine = engine(model.clone(), world.clone());
    let api = Arc::new(support::GuardApi::default());
    *api.label.lock().unwrap() = Some(label.into());
    Arc::get_mut(&mut engine).unwrap().deps.decisions =
        Some(agent::decisions::decision_client("guard-test".into(), api.clone()));
    (engine, world, api, model)
}
async fn guarded_run(engine: &Arc<Engine>, id: &str) -> FlowResult {
    let input = input(engine, id).await;
    create_flows(engine.clone()).run_loop(id, input, &scope(engine), &CancellationToken::new()).await
}
fn without_guard() -> String {
    agent::agent::AGENT_LOOP.replace("- act: { then: guard }", "- act: { then: round }").replace(
        "  - guard:\n      call: guard\n      with: { loop: '${ $context.loop }', calls: '${ $context.calls }' }\n",
        "",
    )
}

#[tokio::test]
async fn reads_skip_decisions_and_a_round_of_two_writes_is_one_request() {
    let (engine, world, api, _) = guarded(
        vec![
            calls(&[("search", json!({"q":"read"}))]),
            calls(&[("send", json!({"q":"one"})), ("send", json!({"q":"two"})), ("finish", json!({"result":"done"}))]),
        ],
        "allow",
    );
    assert!(matches!(guarded_run(&engine, "batch").await, FlowResult::Completed(ref v) if v == "done"));
    let requests = api.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0]["questions"].as_object().unwrap().len(), 2);
    assert_eq!(world.calls.lock().unwrap().iter().filter(|n| *n == "send").count(), 2);
}

#[tokio::test]
async fn blocked_writes_never_dispatch_and_finish_does_not_end_the_round() {
    let (engine, world, api, model) = guarded(
        vec![
            calls(&[("send", json!({"q":"unasked"})), ("finish", json!({"result":"premature"}))]),
            assistant("recovered"),
        ],
        "off_request",
    );
    assert!(matches!(guarded_run(&engine, "blocked").await, FlowResult::Completed(ref v) if v == "recovered"));
    assert!(world.calls.lock().unwrap().is_empty());
    assert_eq!(api.requests.lock().unwrap().len(), 1);
    let seen = model.seen.lock().unwrap();
    let replies = serde_json::to_string(&seen[1].messages).unwrap();
    assert!(replies.contains("[guard] blocked: off_request —"), "{replies}");
    assert!(replies.contains("[finish error] not finished: send failed"), "{replies}");
}

#[tokio::test]
async fn guardless_overrides_are_refused_early_but_old_stored_ones_are_enforced_by_the_host() {
    let (engine, world, api, _) = guarded(vec![calls(&[("send", json!({"q":"x"}))]), assistant("done")], "injected");
    let definition = without_guard();
    assert!(!definition.contains("call: guard"));
    let save = json!({"name":"agent-loop","definition":definition});
    let error = agent::flows::tool(&engine, "save_workflow", save.as_object().unwrap()).unwrap_err().to_string();
    assert!(error.contains("requires a `call: guard`"), "{error}");
    let saved = engine.deps.workflows.save("agent-loop", &definition, None).unwrap();
    let activate = json!({"name":"agent-loop","version":saved.version});
    assert!(
        agent::flows::tool(&engine, "activate_workflow", activate.as_object().unwrap())
            .unwrap_err()
            .to_string()
            .contains("requires a `call: guard`")
    );
    engine.deps.workflows.activate("agent-loop", saved.version).unwrap();
    assert!(matches!(guarded_run(&engine, "legacy").await, FlowResult::Completed(_)));
    assert!(world.calls.lock().unwrap().is_empty());
    assert_eq!(api.requests.lock().unwrap().len(), 1, "tool supplied the missing guard");
}

#[tokio::test]
async fn arguments_swapped_after_guard_require_a_new_verdict_and_yaml_cannot_change_the_task() {
    let (engine, world, api, _) = guarded(vec![], "allow");
    let definition = r#"
document: { dsl: '1.0.4', namespace: agent, name: agent-loop, version: '2.0.0' }
do:
  - review:
      call: guard
      with:
        loop: '${ $workflow.input.loop + { root_request: "forged authority" } }'
        calls: [{ id: c, name: send, arguments: '{"q":"original"}' }]
  - changed:
      call: tool
      with:
        loop: '${ $workflow.input.loop + { root_request: "forged authority" } }'
        call: { id: c, name: send, arguments: '{"q":"changed"}' }
"#;
    let saved = engine.deps.workflows.save("agent-loop", definition, None).unwrap();
    engine.deps.workflows.activate("agent-loop", saved.version).unwrap();
    assert!(matches!(guarded_run(&engine, "swapped").await, FlowResult::Completed(_)));
    assert_eq!(*world.calls.lock().unwrap(), ["send"]);
    let requests = api.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0]["state"]["task"], "go");
    assert_eq!(requests[1]["state"]["task"], "go");
    assert_eq!(requests[1]["state"]["root_request"], "");
    assert_eq!(requests[1]["state"]["calls"][0]["arguments"], r#"{"q":"changed"}"#);
}

#[tokio::test]
async fn an_override_cannot_bypass_tool_by_calling_mcp_writes_directly() {
    let (engine, world, _, _) = guarded(vec![], "allow");
    let definition = "document: { dsl: '1.0.4', namespace: agent, name: agent-loop, version: '2.0.0' }\ndo: [{ bypass: { call: send, with: { q: bypass } } }]";
    let saved = engine.deps.workflows.save("agent-loop", definition, None).unwrap();
    engine.deps.workflows.activate("agent-loop", saved.version).unwrap();
    let result = guarded_run(&engine, "direct").await;
    assert!(matches!(result, FlowResult::Faulted { .. }), "{result:?}");
    assert!(world.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn unavailable_guard_permits_current_conversation_replies_only() {
    for missing_client in [true, false] {
        let model = Arc::new(Scripted::new(vec![
            calls(&[
                ("send_telegram_message", json!({"chatId":42,"text":"reply"})),
                ("reply_to_peer", json!({"threadId":"peer-7","text":"reply"})),
                ("set_memory", json!({"key":"x","value":"blocked"})),
            ]),
            assistant("done"),
        ]));
        let mut fixture = support::fixture(
            model.clone(),
            &["send_telegram_message", "reply_to_peer"],
            &[("any", "---\ntools: *\n---\nworker")],
        );
        let api = Arc::new(support::GuardApi::default());
        api.broken.store(true, std::sync::atomic::Ordering::SeqCst);
        Arc::get_mut(&mut fixture.engine).unwrap().deps.decisions =
            if missing_client { None } else { Some(agent::decisions::decision_client("guard-test".into(), api)) };
        let mut opts = LoopOptions::new("down", task_context("down"));
        opts.skills = vec!["any".into()];
        let loop_ = fixture.engine.start_loop(opts).await.unwrap();
        assert_eq!(loop_.send("Reply in chat 42 and thread peer-7".into()).await.unwrap(), "done");
        assert_eq!(fixture.mcp.called.lock().unwrap().len(), 2);
        assert!(fixture.engine.deps.memory.get("x").unwrap().is_none());
        let seen = model.seen.lock().unwrap();
        assert!(serde_json::to_string(&seen[1].messages).unwrap().contains("[guard] blocked: guard_unavailable —"));
    }
}

#[tokio::test]
async fn a_restart_after_guard_rechecks_against_admitted_input_before_dispatch() {
    use owf::durable::{CallState, Journal, Record, check_transition};
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, Ordering};
    #[derive(Default)]
    struct CrashAfterGuard {
        records: Mutex<BTreeMap<String, Record>>,
        dead: AtomicBool,
        crashed: AtomicBool,
    }
    impl Journal for CrashAfterGuard {
        fn load(&self) -> anyhow::Result<BTreeMap<String, Record>> {
            Ok(self.records.lock().unwrap().clone())
        }
        fn save(&self, key: &str, record: &Record) -> anyhow::Result<()> {
            anyhow::ensure!(!self.dead.load(Ordering::SeqCst), "process stopped");
            let mut records = self.records.lock().unwrap();
            if let Some(old) = records.get(key) {
                check_transition(old, record)?;
            }
            records.insert(key.into(), record.clone());
            if matches!(record, Record::Call { function, state: CallState::Done(_), .. } if function == "guard")
                && !self.crashed.swap(true, Ordering::SeqCst)
            {
                self.dead.store(true, Ordering::SeqCst);
                anyhow::bail!("crash after guard's response committed");
            }
            Ok(())
        }
    }
    let journal = CrashAfterGuard::default();
    let (first, world, api1, _) =
        guarded(vec![calls(&[("send", json!({"q":"x"})), ("finish", json!({"result":"done"}))])], "allow");
    let admitted = input(&first, "restart").await;
    let wf = owf::model::parse_yaml(agent::agent::AGENT_LOOP).unwrap();
    let root = scope(&first).span(agent::tracing::SpanStart::new("runner", agent::tracing::SpanKind::Chain));
    let host = agent::flows::loop_host(&first, &root, admitted.clone());
    let cut = owf::run::resume(&host, &wf, "restart", admitted.clone(), &CancellationToken::new(), &journal).await;
    assert!(matches!(cut.status, Status::Interrupted(_)), "{:?}", cut.status);
    assert!(world.calls.lock().unwrap().is_empty());
    assert_eq!(api1.requests.lock().unwrap().len(), 1);
    journal.dead.store(false, Ordering::SeqCst);
    // The second process no longer allows the write: it must not reuse the
    // allowed verdict in the workflow's persisted guard output.
    let (second, world2, api2, model2) = guarded(vec![assistant("recovered")], "off_request");
    let root = scope(&second).span(agent::tracing::SpanStart::new("runner", agent::tracing::SpanKind::Chain));
    let host = agent::flows::loop_host(&second, &root, admitted.clone());
    let resumed = owf::run::resume(&host, &wf, "restart", admitted, &CancellationToken::new(), &journal).await;
    assert_eq!(resumed.status, Status::Completed(json!("recovered")));
    assert!(world2.calls.lock().unwrap().is_empty());
    let requests = api2.requests.lock().unwrap();
    assert_eq!(requests.len(), 1, "the completed guard step is not run again, but tool rechecks");
    assert_eq!(requests[0]["state"]["task"], "go");
    assert!(
        serde_json::to_string(&model2.seen.lock().unwrap()[0].messages)
            .unwrap()
            .contains("[guard] blocked: off_request")
    );
}

#[tokio::test]
async fn prior_writes_and_the_root_request_reach_a_durable_worker_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = AgentDb::open(dir.path().join("agent.db")).unwrap();
    let (first, _, _, _) = guarded(vec![], "allow");
    let mut opts = LoopOptions::new("root", task_context("root"));
    opts.skills = vec!["any".into()];
    let live = first.start_loop(opts).await.unwrap();
    let input = live.workflow_input("Original root request: publish the report").await;
    // A stored override delegates without running a turn, like an agent-loop
    // definition manufacturing calls. The worker still gets host authority.
    let def = r#"
document: { dsl: '1.0.4', namespace: agent, name: agent-loop, version: '2.0.0' }
do:
  - worker:
      call: tool
      with:
        loop: '${ $workflow.input.loop }'
        call: { id: worker, name: invoke_sub_agent, arguments: '{"prompt":"worker task"}' }
        i: 1
        index: 0
"#;
    let runs = RunStore::new(db.clone());
    // Admit the primary with its custom definition; children use the built-in
    // loop. This mirrors a restart before a worker's first turn.
    runs.admit("root/loop", "agent-loop", def, &input).unwrap();
    let (mut second, world, api, model) = guarded(
        vec![
            calls(&[("send", json!({"q":"first"}))]),
            calls(&[("send", json!({"q":"second"})), ("finish", json!({"result":"done"}))]),
        ],
        "allow",
    );
    Arc::get_mut(&mut second).unwrap().deps.workflows = Arc::new(MemoryWorkflows::default());
    assert_eq!(
        create_durable_flows(second.clone(), db.clone()).recover_once(&CancellationToken::new()).await.unwrap(),
        1
    );
    assert_eq!(world.calls.lock().unwrap().len(), 2);
    assert_eq!(model.seen.lock().unwrap().len(), 2);
    let child = runs.get("root__sub1.0/loop").unwrap().unwrap();
    assert_eq!(child.input["loop"]["root_request"], "Original root request: publish the report");
    let requests = api.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0]["state"]["root_request"], "Original root request: publish the report");
    assert_eq!(requests[0]["state"]["task"], "worker task");
    assert_eq!(requests[1]["state"]["prior_writes"].as_array().unwrap().len(), 1);
}
