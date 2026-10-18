// The executable specification's machinery. The specification itself is
// specs/*.feature (see specs/README.md); this file only teaches cucumber
// what each sentence means, by driving the real server over MCP:
//
// Sections:
//   1. environment — one Postgres + the real server, in-process (tests/support)
//   2. world       — per-scenario state: the MCP session, the last result
//   3. calls       — generic steps: call a tool, check the result
//   4. memory      — project / document sentences
//   5. signals     — the queue
//   6. surface     — the tool list
//   7. main        — sequential run, a clean database per scenario
//
//   cargo test --test specs                      (Docker needed for Postgres)
//   TEST_DATABASE_URL=postgres://… cargo test --test specs

mod support;

use std::collections::HashMap;
use std::sync::OnceLock;

use cucumber::gherkin::Step;
use cucumber::writer::Stats as _;
use cucumber::{World, given, then, when};
use serde_json::{Value, json};
use support::{Client, Outcome, Server, connect, start_test_server, test_database};

// ── 1. environment ───────────────────────────────────────────────────────────

// Built once; every scenario talks to the same server and wipes the data
// before it starts.
static ENV: OnceLock<Server> = OnceLock::new();

fn env() -> &'static Server {
    ENV.get().expect("environment is set up before the first scenario")
}

// Every table a scenario can touch, emptied: each scenario starts from a
// freshly migrated database.
async fn wipe() -> anyhow::Result<()> {
    let env = env();
    env.pool
        .get()
        .await?
        .batch_execute(
            "TRUNCATE news_items, knowledge_base_notes, memory_projects, memory_project_docs,
                      memory_doc_patches, memory_facts, memory_index, memory_journal_streams,
                      memory_journal_entries RESTART IDENTITY CASCADE",
        )
        .await?;
    env.db
        .client()
        .await?
        .batch_execute(
            "TRUNCATE signals, scheduled_tasks, settings, telegram_messages, telegram_kv, gmail_kv,
                      integration_account RESTART IDENTITY",
        )
        .await?;
    Ok(())
}

// ── 2. world ─────────────────────────────────────────────────────────────────

#[derive(World)]
#[world(init = Self::new)]
struct Spec {
    client: Option<Client>,
    last: Option<Outcome>,
    remembered: HashMap<String, Value>,
    delivered: Vec<i64>,
    queued: Vec<i64>,
}

impl std::fmt::Debug for Spec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Spec").field("last", &self.last).field("remembered", &self.remembered).finish()
    }
}

impl Spec {
    fn new() -> Self {
        Self { client: None, last: None, remembered: HashMap::new(), delivered: Vec::new(), queued: Vec::new() }
    }

    async fn client(&mut self) -> &Client {
        if self.client.is_none() {
            self.client = Some(connect(&env().url).await);
        }
        self.client.as_ref().expect("connected")
    }

    // `{{name}}` → a value remembered earlier in the scenario.
    fn substitute(&self, text: &str) -> String {
        self.remembered.iter().fold(text.to_owned(), |acc, (name, value)| {
            let plain = value.as_str().map_or_else(|| value.to_string(), str::to_owned);
            acc.replace(&format!("{{{{{name}}}}}"), &plain)
        })
    }

    async fn call(&mut self, tool: &str, args: Value) -> Outcome {
        let outcome = support::call(self.client().await, tool, args).await;
        self.last = Some(outcome.clone());
        outcome
    }

    fn result(&self) -> &Value {
        match &self.last {
            Some(Outcome::Ok(value)) => value,
            other => panic!("expected a successful call, got {other:?}"),
        }
    }
}

// ── 3. calls ─────────────────────────────────────────────────────────────────

fn docstring(step: &Step) -> &str {
    step.docstring.as_deref().expect("this step needs a \"\"\" block").trim_start_matches('\n')
}

fn table(step: &Step) -> &[Vec<String>] {
    &step.table.as_ref().expect("this step needs a | table |").rows
}

fn json_arg(spec: &Spec, step: &Step) -> Value {
    let raw = spec.substitute(docstring(step));
    serde_json::from_str(&raw).unwrap_or_else(|err| panic!("arguments are not JSON ({err}):\n{raw}"))
}

// "doc.body", "failures.0.reason"
fn at<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.').try_fold(value, |v, key| match key.parse::<usize>() {
        Ok(i) if v.is_array() => v.get(i),
        _ => v.get(key),
    })
}

// A table cell: JSON when it parses as JSON (2, true, null, "…", […]), the
// bare text otherwise.
fn cell(text: &str) -> Value {
    serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.to_owned()))
}

// Every key in `expected` must be present in `actual` with a matching value;
// extra keys in `actual` are fine. Arrays match element by element.
fn subset(expected: &Value, actual: &Value) -> bool {
    match (expected, actual) {
        (Value::Object(e), Value::Object(a)) => e.iter().all(|(k, v)| a.get(k).is_some_and(|av| subset(v, av))),
        (Value::Array(e), Value::Array(a)) => e.len() == a.len() && e.iter().zip(a).all(|(x, y)| subset(x, y)),
        _ => expected == actual,
    }
}

#[given(expr = "I call {string}")]
#[when(expr = "I call {string}")]
async fn call_bare(spec: &mut Spec, tool: String) {
    spec.call(&tool, json!({})).await;
}

#[given(expr = "I call {string} with:")]
#[when(expr = "I call {string} with:")]
async fn call_with(spec: &mut Spec, tool: String, step: &Step) {
    let args = json_arg(spec, step);
    spec.call(&tool, args).await;
}

#[then("the result is:")]
async fn result_is(spec: &mut Spec, step: &Step) {
    let result = spec.result().clone();
    for row in table(step).iter().skip(1) {
        let (path, expected) = (&row[0], cell(&spec.substitute(&row[1])));
        let actual = at(&result, path).unwrap_or_else(|| panic!("no `{path}` in the result:\n{result:#}"));
        assert_eq!(actual, &expected, "`{path}` in the result:\n{result:#}");
    }
}

#[then("the result includes:")]
async fn result_includes(spec: &mut Spec, step: &Step) {
    let expected = json_arg(spec, step);
    let actual = spec.result();
    assert!(subset(&expected, actual), "expected the result to include\n{expected:#}\nbut it was\n{actual:#}");
}

#[then(expr = "the call fails with {string}")]
async fn fails_with(spec: &mut Spec, text: String) {
    match &spec.last {
        Some(Outcome::ToolError(message)) => assert!(message.contains(&text), "tool error was: {message}"),
        other => panic!("expected a tool error containing {text:?}, got {other:?}"),
    }
}

#[then("the call is rejected as invalid parameters")]
async fn rejected(spec: &mut Spec) {
    match &spec.last {
        Some(Outcome::Rejected { code: -32602, message }) => assert!(!message.is_empty()),
        other => panic!("expected an invalid-params rejection, got {other:?}"),
    }
}

#[given(expr = "I remember {string} as {string}")]
#[then(expr = "I remember {string} as {string}")]
async fn remember(spec: &mut Spec, path: String, name: String) {
    let value = at(spec.result(), &path).unwrap_or_else(|| panic!("no `{path}` to remember")).clone();
    spec.remembered.insert(name, value);
}

// ── 4. memory ────────────────────────────────────────────────────────────────

#[given(expr = "a project {string} titled {string}")]
async fn project(spec: &mut Spec, slug: String, title: String) {
    let outcome = spec.call("create_project", json!({ "slug": slug, "title": title })).await;
    assert!(matches!(&outcome, Outcome::Ok(v) if v["ok"] == true), "creating the project: {outcome:?}");
}

#[given(expr = "{string} in {string} reads:")]
async fn document_exists(spec: &mut Spec, doc: String, project: String, step: &Step) {
    let body = docstring(step).to_owned();
    let outcome = spec.call("write_doc", json!({ "project": project, "doc": doc, "body": body })).await;
    assert!(matches!(&outcome, Outcome::Ok(v) if v["ok"] == true), "writing the document: {outcome:?}");
}

#[then(expr = "{string} in {string} reads:")]
async fn document_reads(spec: &mut Spec, doc: String, project: String, step: &Step) {
    let expected = docstring(step).to_owned();
    let saved = spec.last.clone();
    spec.call("read_doc", json!({ "project": project, "doc": doc })).await;
    let body = spec.result()["doc"]["body"].as_str().unwrap_or_default().to_owned();
    spec.last = saved;
    assert_eq!(body, expected, "the document's text");
}

#[then(expr = "{string} in {string} is at version {int}")]
async fn document_version(spec: &mut Spec, doc: String, project: String, version: i64) {
    let saved = spec.last.clone();
    spec.call("read_doc", json!({ "project": project, "doc": doc })).await;
    let actual = spec.result()["doc"]["version"].as_i64();
    spec.last = saved;
    assert_eq!(actual, Some(version), "the document's version");
}

// ── 5. signals ───────────────────────────────────────────────────────────────

#[given("the signal queue holds:")]
async fn queue_holds(spec: &mut Spec, step: &Step) {
    for row in table(step).iter().skip(1) {
        spec.queued.push(env().signals.record(&row[0], &row[1]).await.expect("signal recorded"));
    }
}

#[given(expr = "the signal queue holds {int} signals")]
async fn queue_holds_many(spec: &mut Spec, count: usize) {
    for i in 0..count {
        spec.queued.push(env().signals.record("telegram", &format!("message {i}")).await.expect("signal recorded"));
    }
}

#[when(expr = "{int} agents take signals at the same time until the queue is empty")]
async fn concurrent_pop(spec: &mut Spec, agents: usize) {
    spec.client().await;
    let client = spec.client.as_ref().expect("connected");
    let workers = (0..agents).map(|_| async {
        let mut got = Vec::new();
        loop {
            let Outcome::Ok(value) = support::call(client, "get_next_signal", json!({})).await else {
                panic!("get_next_signal failed")
            };
            match value["signal"]["id"].as_i64() {
                Some(id) => got.push(id),
                None => break got,
            }
        }
    });
    spec.delivered = futures::future::join_all(workers).await.into_iter().flatten().collect();
}

#[then("every signal was delivered exactly once")]
async fn exactly_once(spec: &mut Spec) {
    let mut counts: HashMap<i64, usize> = HashMap::new();
    for id in &spec.delivered {
        *counts.entry(*id).or_default() += 1;
    }
    let mut twice: Vec<i64> = counts.iter().filter(|(_, n)| **n > 1).map(|(id, _)| *id).collect();
    let mut lost: Vec<i64> = spec.queued.iter().copied().filter(|id| !counts.contains_key(id)).collect();
    twice.sort_unstable();
    lost.sort_unstable();
    assert!(
        twice.is_empty() && lost.is_empty(),
        "{} of {} signals were delivered more than once ({twice:?}); lost: {lost:?}",
        twice.len(),
        spec.queued.len()
    );
}

#[then(expr = "{int} signal(s) is/are waiting")]
async fn waiting(_spec: &mut Spec, count: i64) {
    assert_eq!(env().signals.count_pending().await.expect("count"), count, "signals waiting in the queue");
}

// ── 6. surface ───────────────────────────────────────────────────────────────

#[then("the server offers exactly these tools:")]
async fn offers_tools(spec: &mut Spec, step: &Step) {
    let mut expected: Vec<String> = docstring(step).split_whitespace().map(str::to_owned).collect();
    expected.sort();
    let mut actual: Vec<String> = spec
        .client()
        .await
        .list_all_tools()
        .await
        .expect("tools/list")
        .into_iter()
        .map(|t| t.name.into_owned())
        .collect();
    actual.sort();
    assert_eq!(actual, expected, "the tool surface");
}

// ── 7. main ──────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let db = test_database().await;
    let env = start_test_server(&db, None).await.expect("server starts");
    let _ = ENV.set(env);

    let result = Spec::cucumber()
        // One scenario at a time: they share the server and its database.
        .max_concurrent_scenarios(1)
        // A sentence nobody taught the harness is a failure, not a skip: the
        // specification must never pass on words that check nothing.
        .fail_on_skipped()
        .before(|_, _, _, _| Box::pin(async { wipe().await.expect("clean database") }))
        .run(concat!(env!("CARGO_MANIFEST_DIR"), "/specs"))
        .await;
    let failed = result.execution_has_failed() || result.parsing_errors() > 0;
    db.drop_all().await;
    if failed {
        std::process::exit(1);
    }
}
