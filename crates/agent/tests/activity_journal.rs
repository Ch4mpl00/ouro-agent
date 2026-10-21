// Port of test/activity-journal.test.ts: completed production traces become
// bounded packets (projection), the packets become one validated note each,
// and the worker appends them to the shared journal through MCP — on a real
// agent.db file, with the LLM and the MCP call at the boundary. The tests
// after "Rust-only" cover restart, cancellation and the background loop.
use agent::{
    activity::*,
    db::{AgentDb, StoredTrace, TraceStore},
    mcp::Mcp,
    providers::ToolDef,
    trace_model::{Observation, Trace},
};
use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use serde_json::{Map, Value, json};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

// ── trace builders ──────────────────────────────────────────────────────────

static SEQ: AtomicUsize = AtomicUsize::new(0);

fn obs(fields: Value) -> Observation {
    let seq = SEQ.fetch_add(1, Ordering::SeqCst) + 1;
    let at = (DateTime::parse_from_rfc3339("2026-10-13T10:00:00Z").unwrap() + TimeDelta::seconds(seq as i64))
        .with_timezone(&Utc)
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string();
    let mut o = json!({
        "id": format!("o{seq}"), "name": format!("obs-{seq}"), "parentObservationId": "root",
        "startTime": at, "endTime": at, "level": "DEFAULT", "statusMessage": null,
        "input": null, "output": null, "metadata": null, "model": null, "modelParameters": null,
        "usage": null, "usageDetails": null, "calculatedTotalCost": null, "latency": 0,
    });
    for (k, v) in fields.as_object().unwrap() {
        o[k] = v.clone();
    }
    serde_json::from_value(o).unwrap()
}

fn root() -> Observation {
    obs(json!({"id":"root","type":"AGENT","parentObservationId":null,"name":"signal:telegram"}))
}

fn tool(name: &str, input: Value, output: Value, extra: Value) -> Observation {
    let mut fields = json!({"type":"TOOL","name":name,"input":input,"output":output});
    let mut metadata = json!({"tool": name});
    for (k, v) in extra.as_object().unwrap() {
        if k == "metadata" {
            for (mk, mv) in v.as_object().unwrap() {
                metadata[mk] = mv.clone();
            }
        } else {
            fields[k] = v.clone();
        }
    }
    fields["metadata"] = metadata;
    obs(fields)
}

fn trace(id: &str, fields: Value) -> Trace {
    let mut t = json!({
        "id": id, "name": "signal:telegram", "sessionId": id, "timestamp": "2026-10-13T10:00:00.000Z",
        "input": "hello", "output": "done",
        "metadata": {"activity":"run","skills":["telegram","orchestrator","routing"]},
        "observations": [], "latency": 0, "totalCost": 0, "tags": ["telegram","agent-loop"],
    });
    for (k, v) in fields.as_object().unwrap() {
        t[k] = v.clone();
    }
    serde_json::from_value(t).unwrap()
}

fn project(t: &Trace, observations: &[Observation]) -> Value {
    project_trace(t, observations, Some("2026-10-13 10:05:00"), &ProjectionBudget::default())
}

// toMatchObject: every field of `want` is in `got`, recursively for objects.
fn assert_subset(got: &Value, want: &Value, at: &str) {
    match want {
        Value::Object(fields) => {
            for (k, v) in fields {
                assert_subset(got.get(k).unwrap_or(&Value::Null), v, &format!("{at}.{k}"));
            }
        }
        _ => assert_eq!(got, want, "{at}"),
    }
}

// ── projection ──────────────────────────────────────────────────────────────

#[test]
fn keeps_a_news_run_with_hundreds_of_full_articles_within_budget_without_bodies_or_llm_histories() {
    let body = "ARTICLE-BODY-SENTINEL ".repeat(400);
    let articles = |n: usize| {
        json!({"count":100,"items":(0..100).map(|i| json!({"id":n*100+i,"title":format!("Title {i}"),"url":format!("https://x/{i}"),"body":body})).collect::<Vec<_>>()}).to_string()
    };
    let worker = obs(
        json!({"type":"AGENT","name":"invoke_sub_agent","input":{"task":"Make the news digest","skill":"news-digest"},"output":"Digest: 5 stories…","metadata":{"tool":"invoke_sub_agent"}}),
    );
    let mut observations = vec![
        root(),
        obs(
            json!({"type":"GENERATION","input":[{"role":"user","content":"INTERMEDIATE-HISTORY-SENTINEL"}],"output":{"content":"thinking"}}),
        ),
        worker.clone(),
    ];
    for n in 0..5 {
        observations.push(tool(
            "list_news",
            json!({"source":"hackernews","since":format!("day-{n}")}),
            json!(articles(n)),
            json!({"parentObservationId":worker.id}),
        ));
    }
    for n in 0..3 {
        observations.push(tool(
            "search_news",
            json!({"query":format!("q{n}")}),
            json!(articles(10 + n)),
            json!({"parentObservationId":worker.id}),
        ));
    }
    observations.push(obs(json!({"type":"GENERATION","parentObservationId":worker.id,"input":[{"role":"tool","content":"INTERMEDIATE-HISTORY-SENTINEL"}]})));
    observations.push(tool(
        "send_telegram_message",
        json!({"text":"Digest: 5 stories…","chatId":"42"}),
        json!(json!({"delivered":true,"messageId":77,"messageIds":[77],"parts":1}).to_string()),
        json!({}),
    ));

    let packet = project(&trace("telegram:1", json!({"input":"give me the news digest"})), &observations);
    let text = packet.to_string();
    assert!(utf16_len(&text) <= ProjectionBudget::default().total_chars);
    assert!(!text.contains("ARTICLE-BODY-SENTINEL"));
    assert!(!text.contains("INTERMEDIATE-HISTORY-SENTINEL"));
    let actions = packet["actions"].as_array().unwrap();
    assert_subset(
        &actions[0],
        &json!({"ref":1,"parent":null,"tool":"invoke_sub_agent","kind":"agent","outcome":"ok"}),
        "digest",
    );
    // Repeated reads inside the worker collapse into one action each, under it.
    assert_subset(&actions[1], &json!({"tool":"list_news","parent":1,"calls":5}), "list_news");
    assert_subset(&actions[2], &json!({"tool":"search_news","parent":1,"calls":3}), "search_news");
    assert_subset(
        &actions[3],
        &json!({"tool":"send_telegram_message","parent":null,"outcome":"ok","result":{"delivered":true,"messageId":77}}),
        "send",
    );
    assert_eq!(actions.len(), 4);
}

#[test]
fn stays_within_the_total_even_when_every_field_is_huge_dropping_reads_before_deliveries() {
    let huge = "x".repeat(50_000);
    let mut observations = vec![root()];
    for i in 0..120 {
        observations.push(tool(
            if i % 2 == 1 { "read_doc" } else { "recall" },
            json!({"query":huge}),
            json!(huge),
            json!({}),
        ));
    }
    observations.push(tool(
        "send_telegram_message",
        json!({"text":huge}),
        json!(json!({"delivered":true,"messageId":5}).to_string()),
        json!({}),
    ));
    let packet = project(&trace("telegram:2", json!({"input":huge,"output":huge})), &observations);
    assert!(utf16_len(&packet.to_string()) <= ProjectionBudget::default().total_chars);
    assert!(packet["actions"].as_array().unwrap().iter().any(|a| a["tool"] == "send_telegram_message"));
    assert!(packet["omitted"]["actions_over_budget"].as_u64().unwrap() > 0);
}

#[test]
fn reads_a_scheduler_workflow_from_tool_metadata_not_from_step_names_or_the_execution_marker() {
    let observations = vec![
        root(),
        obs(json!({"type":"CHAIN","id":"runner","name":"runner"})),
        tool(
            "step[0]:tool",
            json!({"tool":"search_news","args":{"query":"rust"}}),
            json!({"count":2,"items":[{"title":"A"},{"title":"B"}]}),
            json!({"parentObservationId":"runner","metadata":{"tool":"search_news"}}),
        ),
        tool(
            "step[1]:tool",
            json!({"tool":"send_telegram_message","args":{"text":"Morning digest"}}),
            json!({"delivered":true,"messageId":9}),
            json!({"parentObservationId":"runner","metadata":{"tool":"send_telegram_message"}}),
        ),
        tool(
            "step[2]:tool",
            json!({"tool":"set_memory","args":{"key":"news_digest.last_read_at"}}),
            json!({"reason":"tool_error"}),
            json!({"parentObservationId":"runner","metadata":{"tool":"set_memory"},"level":"ERROR","statusMessage":"tool set_memory failed: disk full"}),
        ),
    ];
    let packet = project(
        &trace(
            "scheduler:3",
            json!({
                "tags": ["scheduler","planner-mode"],
                "input": "Run the morning digest",
                "output": {"error":"Workflow execution failed at step 2 (tool_error): tool set_memory failed: disk full"},
                "metadata": {"activity":"run","error":true},
            }),
        ),
        &observations,
    );
    let actions: Vec<(Value, Value)> =
        packet["actions"].as_array().unwrap().iter().map(|a| (a["tool"].clone(), a["outcome"].clone())).collect();
    assert_eq!(
        actions,
        [
            (json!("search_news"), json!("ok")),
            (json!("send_telegram_message"), json!("ok")),
            (json!("set_memory"), json!("error"))
        ]
    );
    assert_eq!(packet["actions"][1]["args"], json!({"text":"Morning digest"}));
    // Delivered, then a later failure: the run failed, the delivery stands.
    assert_eq!(packet["status"], "error");
    assert!(packet["error"].as_str().unwrap().contains("set_memory"));
    assert!(packet.get("final").is_none());
}

#[test]
fn keeps_a_partial_send_as_a_failed_send_with_its_delivery_evidence() {
    let error = "[tool error] send_telegram_message: delivered 2 of 3 parts (message ids 11, 12) before Telegram refused part 3";
    let packet = project(
        &trace("telegram:4", json!({})),
        &[
            root(),
            tool(
                "send_telegram_message",
                json!({"text":"long answer"}),
                json!(error),
                json!({"level":"ERROR","statusMessage":error}),
            ),
        ],
    );
    assert_eq!(packet["actions"][0]["outcome"], "error");
    assert!(packet["actions"][0]["error"].as_str().unwrap().contains("2 of 3 parts"));
    assert!(packet["actions"][0].get("result").is_none());
}

#[test]
fn gives_a_follow_up_the_conversation_the_primary_loop_was_given_not_the_raw_history_fetch() {
    let signal = "Telegram message in chat 42.\nText: \"yes\"";
    let given = [
        "## Recent Telegram history".to_owned(),
        json!([{"role":"assistant","text":"Set a reminder for 9:00?"}]).to_string(),
        String::new(),
        "## Current Telegram signal".to_owned(),
        signal.to_owned(),
    ]
    .join("\n");
    let packet = project(
        &trace("telegram:5", json!({"input":signal,"output":"Done, I'll remind you at 9:00"})),
        &[
            root(),
            tool(
                "get_telegram_chat_history",
                json!({"chatId":"42"}),
                json!(json!({"messages":[{"text":"OTHER-TOPIC-SENTINEL"}]}).to_string()),
                json!({"metadata":{"automatic":true}}),
            ),
            obs(
                json!({"type":"CHAIN","name":"agent_loop","input":given,"output":"Done, I'll remind you at 9:00","metadata":{"activity":"primary_loop"}}),
            ),
        ],
    );
    let context = packet["context"].as_str().unwrap();
    assert!(context.contains("Set a reminder for 9:00?"));
    assert!(!context.contains("Text: \"yes\""));
    assert!(!packet.to_string().contains("OTHER-TOPIC-SENTINEL"));
    assert_subset(&packet["omitted"], &json!({"automatic_context_fetch":1}), "omitted");
    assert_eq!(packet["final"], "Done, I'll remind you at 9:00");
    assert_eq!(packet["input"], signal);
}

#[test]
fn does_not_depend_on_observation_names() {
    let named = |name: &str| {
        vec![root(), tool("send_telegram_message", json!({"text":"hi"}), json!("{}"), json!({"name":name}))]
    };
    let a = project(&trace("telegram:6", json!({})), &named("send_telegram_message"));
    let b = project(&trace("telegram:6", json!({})), &named("renamed-span"));
    assert_eq!(b["actions"], a["actions"]);
}

// ── worker ──────────────────────────────────────────────────────────────────

type Scripted = Box<dyn FnOnce() -> futures::future::BoxFuture<'static, anyhow::Result<String>> + Send>;

#[derive(Default)]
struct Notes {
    next: Mutex<VecDeque<Scripted>>,
    generated: Mutex<Vec<Value>>,
}

#[async_trait]
impl NoteGenerator for Notes {
    async fn generate(&self, packet: &Value, _: &CancellationToken) -> anyhow::Result<String> {
        self.generated.lock().unwrap().push(packet.clone());
        let scripted = self.next.lock().unwrap().pop_front();
        match scripted {
            Some(f) => f().await,
            None => Ok(format!("Asked: {}\nDone.", packet["input"].as_str().unwrap_or_default())),
        }
    }
}

#[derive(Default)]
struct Journal {
    appended: Mutex<Vec<Map<String, Value>>>,
    fails: AtomicUsize,
}

#[async_trait]
impl Mcp for Journal {
    fn tools(&self) -> Vec<ToolDef> {
        Vec::new()
    }
    async fn call_tool(&self, name: &str, args: Map<String, Value>, _: &CancellationToken) -> anyhow::Result<String> {
        assert_eq!(name, "journal_append");
        if self.fails.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1)).is_ok() {
            return Ok("[tool error] journal_append: connection reset".into());
        }
        let mut appended = self.appended.lock().unwrap();
        appended.push(args);
        Ok(json!({"ok":true,"entry":{"id":appended.len()}}).to_string())
    }
}

struct Harness {
    _dir: tempfile::TempDir,
    db: Arc<AgentDb>,
    now: Mutex<DateTime<Utc>>,
    notes: Arc<Notes>,
    journal: Arc<Journal>,
    worker: Arc<ActivityWorker>,
}

fn options() -> ActivityOptions {
    ActivityOptions { max_generation_attempts: 2, ..ActivityOptions::default() }
}

impl Harness {
    fn new() -> Harness {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(AgentDb::open(dir.path().join("agent.db")).unwrap());
        // The cutoff a minute before the traces below are recorded.
        db.activity_cutoff(Utc::now() - TimeDelta::seconds(60)).unwrap();
        let notes = Arc::new(Notes::default());
        let journal = Arc::new(Journal::default());
        let worker = create_activity_worker(db.clone(), notes.clone(), journal.clone(), options()).unwrap();
        Harness { _dir: dir, db, now: Mutex::new("2026-10-13T10:00:00Z".parse().unwrap()), notes, journal, worker }
    }
    fn write(&self, id: &str, fields: Value) {
        let mut t = StoredTrace {
            id: id.into(),
            name: "signal:telegram".into(),
            source: Some("telegram".into()),
            skill: None,
            session_id: Some(id.into()),
            tags: vec!["telegram".into(), "agent-loop".into()],
            input: Some(json!(format!("input of {id}"))),
            output: Some(json!("ok")),
            metadata: Some(json!({"activity":"run"}).as_object().unwrap().clone()),
            observations: vec![root()],
            started_at: "2026-10-13T10:00:00.000Z".into(),
        };
        if let Some(m) = fields.get("metadata") {
            t.metadata = m.as_object().cloned();
        }
        if let Some(tags) = fields.get("tags") {
            t.tags = serde_json::from_value(tags.clone()).unwrap();
        }
        if let Some(at) = fields.get("startedAt") {
            t.started_at = at.as_str().unwrap().into();
        }
        self.db.write_trace(&t).unwrap();
    }
    fn now(&self) -> DateTime<Utc> {
        *self.now.lock().unwrap()
    }
    fn later(&self, ms: u64) {
        *self.now.lock().unwrap() += TimeDelta::milliseconds(ms as i64);
    }
    fn due(&self) -> Vec<String> {
        self.db.due_activity(100, self.now()).unwrap().into_iter().map(|d| d.trace_id).collect()
    }
    async fn tick(&self) -> usize {
        self.worker.tick(&CancellationToken::new(), self.now()).await.unwrap()
    }
    fn progress(&self, id: &str) -> Progress {
        self.db.activity_progress(id).unwrap().unwrap()
    }
    fn generated(&self) -> usize {
        self.notes.generated.lock().unwrap().len()
    }
}

#[test]
fn journals_only_production_runs_recorded_after_the_cutoff() {
    let h = Harness::new();
    h.write("telegram:old", json!({}));
    h.db.conn().execute("UPDATE traces SET created_at = '2020-01-01 00:00:00' WHERE id = 'telegram:old'", []).unwrap();
    h.write("gaia:run:1", json!({"metadata":{"gaia_task_id":"x"},"tags":["gaia","bench"]}));
    h.write("telegram:1", json!({}));
    h.write("scheduler:2", json!({}));
    assert_eq!(h.due(), ["telegram:1", "scheduler:2"]);
}

#[tokio::test]
async fn makes_no_llm_call_on_an_empty_poll() {
    let h = Harness::new();
    assert_eq!(h.tick().await, 0);
    assert_eq!(h.generated(), 0);
}

#[tokio::test]
async fn appends_one_note_per_run_with_code_supplied_provenance_then_skips_it() {
    let h = Harness::new();
    h.write("telegram:1", json!({}));
    assert_eq!(h.tick().await, 1);
    let appended = h.journal.appended.lock().unwrap().clone();
    assert_eq!(appended.len(), 1);
    assert_eq!(appended[0]["stream"], ACTIVITY_STREAM);
    assert_eq!(appended[0]["capacity"], 200);
    assert_eq!(appended[0]["body"], "2026-10-13 10:00 UTC · telegram\nAsked: input of telegram:1\nDone.");
    assert_subset(
        &appended[0]["metadata"],
        &json!({"trace_id":"telegram:1","source":"telegram","started_at":"2026-10-13T10:00:00.000Z"}),
        "metadata",
    );
    let progress = h.progress("telegram:1");
    assert_eq!((progress.status.as_str(), progress.entry_id), ("done", Some(1)));
    assert!(progress.draft.is_none());
    assert_eq!(h.tick().await, 0);
    assert_eq!(h.generated(), 1);
}

#[tokio::test]
async fn never_processes_one_trace_twice_at_once() {
    let h = Harness::new();
    h.write("telegram:1", json!({}));
    let (release, released) = tokio::sync::oneshot::channel::<String>();
    h.notes.next.lock().unwrap().push_back(Box::new(move || Box::pin(async move { Ok(released.await?) })));
    let item = h.db.due_activity(10, h.now()).unwrap().remove(0);
    let first = {
        let (worker, item, now) = (h.worker.clone(), item.clone(), h.now());
        tokio::spawn(async move { worker.process_trace(&item, &CancellationToken::new(), now).await })
    };
    while h.generated() == 0 {
        tokio::task::yield_now().await;
    }
    // A poll while the first is still generating takes nothing.
    assert_eq!(h.tick().await, 0);
    assert_eq!(h.worker.process_trace(&item, &CancellationToken::new(), h.now()).await.unwrap(), "skipped");
    release.send("Note.".into()).unwrap();
    assert_eq!(first.await.unwrap().unwrap(), "done");
    assert_eq!(h.generated(), 1);
    assert_eq!(h.journal.appended.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn retries_a_failed_append_with_the_note_it_already_generated() {
    let h = Harness::new();
    h.write("telegram:1", json!({}));
    h.journal.fails.store(1, Ordering::SeqCst);
    h.tick().await;
    let progress = h.progress("telegram:1");
    assert_eq!(progress.status, "pending");
    assert!(progress.draft.unwrap().body.contains("Done."));
    // Backing off.
    assert!(h.due().is_empty());
    h.later(ActivityOptions::default().retry_base_ms + 1);
    h.tick().await;
    assert_eq!(h.generated(), 1);
    assert_eq!(h.journal.appended.lock().unwrap().len(), 1);
    let progress = h.progress("telegram:1");
    assert_eq!(progress.status, "done");
    assert!(progress.draft.is_none());
}

#[tokio::test]
async fn backs_off_a_failing_trace_while_later_ones_go_through_and_gives_up_on_it() {
    let h = Harness::new();
    h.write("telegram:1", json!({}));
    h.write("telegram:2", json!({}));
    // Invalid: too many lines.
    h.notes.next.lock().unwrap().push_back(Box::new(|| Box::pin(async { Ok("line\n".repeat(30)) })));
    h.tick().await;
    let first = h.progress("telegram:1");
    assert_eq!((first.status.as_str(), first.attempts), ("pending", 1));
    assert!(first.error.unwrap().contains("invalid note"));
    assert_eq!(h.progress("telegram:2").status, "done");

    h.later(ActivityOptions::default().retry_base_ms + 1);
    h.notes.next.lock().unwrap().push_back(Box::new(|| Box::pin(async { Err(anyhow::anyhow!("model down")) })));
    h.tick().await;
    let first = h.progress("telegram:1");
    assert_eq!((first.status.as_str(), first.attempts), ("failed", 2));
    h.later(ActivityOptions::default().retry_max_ms);
    assert!(h.due().is_empty());
    let appended = h.journal.appended.lock().unwrap().clone();
    assert_eq!(appended.len(), 1);
    assert_eq!(appended[0]["metadata"]["trace_id"], "telegram:2");
}

#[test]
fn finds_a_long_run_that_started_first_but_finished_last() {
    let h = Harness::new();
    h.write("telegram:fast", json!({"startedAt":"2026-10-13T10:05:00.000Z"}));
    h.write("telegram:slow", json!({"startedAt":"2026-10-13T09:00:00.000Z"}));
    assert_eq!(h.due(), ["telegram:fast", "telegram:slow"]);
}

// ── Rust-only ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn a_saved_draft_survives_a_worker_restart_and_is_never_regenerated() {
    let h = Harness::new();
    h.write("telegram:1", json!({}));
    h.journal.fails.store(3, Ordering::SeqCst);
    let item = h.db.due_activity(10, h.now()).unwrap().remove(0);
    assert_eq!(h.worker.process_trace(&item, &CancellationToken::new(), h.now()).await.unwrap(), "retry");
    let draft = h.progress("telegram:1").draft.unwrap();
    let restarted = create_activity_worker(h.db.clone(), h.notes.clone(), h.journal.clone(), options()).unwrap();
    for _ in 0..2 {
        assert_eq!(restarted.process_trace(&item, &CancellationToken::new(), h.now()).await.unwrap(), "retry");
    }
    assert_eq!(restarted.process_trace(&item, &CancellationToken::new(), h.now()).await.unwrap(), "done");
    assert_eq!(h.generated(), 1);
    assert_eq!(h.journal.appended.lock().unwrap()[0]["body"], draft.body);
}

#[tokio::test]
async fn a_cancelled_generation_records_nothing_and_releases_the_trace() {
    let h = Harness::new();
    h.write("telegram:1", json!({}));
    h.notes.next.lock().unwrap().push_back(Box::new(|| Box::pin(std::future::pending())));
    let item = h.db.due_activity(10, h.now()).unwrap().remove(0);
    let cancel = CancellationToken::new();
    let running = {
        let (worker, item, cancel, now) = (h.worker.clone(), item.clone(), cancel.clone(), h.now());
        tokio::spawn(async move {
            agent::errors::or_cancel(&cancel.clone(), async move { worker.process_trace(&item, &cancel, now).await })
                .await
        })
    };
    while h.generated() == 0 {
        tokio::task::yield_now().await;
    }
    cancel.cancel();
    assert!(running.await.unwrap().is_err());
    assert!(h.db.activity_progress("telegram:1").unwrap().is_none());
    // Released: the next poll takes it.
    assert_eq!(h.worker.process_trace(&item, &CancellationToken::new(), h.now()).await.unwrap(), "done");
}

#[tokio::test]
async fn the_background_worker_stops_during_its_idle_wait() {
    let h = Harness::new();
    let cancel = CancellationToken::new();
    let task = {
        let (worker, cancel) = (h.worker.clone(), cancel.clone());
        tokio::spawn(async move { worker.run(cancel).await })
    };
    tokio::task::yield_now().await;
    cancel.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(2), task).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn an_anchored_request_is_projected_once_after_its_last_terminal_segment_is_recorded() {
    use agent::{durable::RunStore, local_recorder::local_recorder_tracer, tracing::*};
    let h = Harness::new();
    let tracer = local_recorder_tracer(h.db.clone());
    let first = tracer.trace(TraceStart {
        id: "request".into(),
        name: "signal:telegram".into(),
        tags: Some(vec!["telegram".into()]),
        metadata: Some(json!({"activity":"run"}).as_object().unwrap().clone()),
        ..Default::default()
    });
    first.update(ContextUpdate::input("save this"));
    let anchor = first.anchor().unwrap();
    let runs = RunStore::new((*h.db).clone());
    let lease = runs.start_anchored("run", "agent-loop", "definition", &json!({}), Some(&anchor)).unwrap().unwrap();
    first.update(ContextUpdate::output(json!({"resumes":true})));
    first.end();
    assert_eq!(h.tick().await, 0);
    assert!(h.db.list_recent(20, Some(("judge", "v1"))).unwrap().is_empty());
    lease.finish(&owf::run::Status::Completed(json!("saved"))).unwrap();
    // A committed run outcome alone is not enough: its final spans must land.
    assert_eq!(h.tick().await, 0);
    let resumed =
        tracer.trace(TraceStart { name: "resume:agent-loop".into(), continuation: Some(anchor), ..Default::default() });
    let runner = resumed.span(
        SpanStart::new("runner", SpanKind::Chain)
            .metadata(json!({"durable_run":"run","durable_terminal":true}).as_object().unwrap().clone()),
    );
    runner.end(SpanEnd::output("saved"));
    resumed.update(ContextUpdate::output("saved"));
    resumed.end();
    assert_eq!(h.tick().await, 1);
    assert_eq!(h.db.list_recent(20, Some(("judge", "v1"))).unwrap().len(), 1);
    assert_eq!(h.tick().await, 0);
    assert_eq!(h.generated(), 1);
    let packets = h.notes.generated.lock().unwrap();
    assert!(packets[0].to_string().contains("saved"));
    assert_eq!(h.journal.appended.lock().unwrap().len(), 1);
}
