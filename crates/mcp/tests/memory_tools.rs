// The memory tools' own contract over MCP, for the journal: the cases
// packages/mcp/src/tools/memory.test.ts added with it ("journal tools end to
// end"). The rest of that file's contract is in the shared specs and the
// memory unit tests.

mod support;

use serde_json::{Value, json};
use support::{Client, Outcome, Server, TestDatabase, call, connect, start_test_server_as};

const ACTOR: &str = "claude-code@laptop";

async fn setup() -> (TestDatabase, Server, Client) {
    let db = support::test_database().await;
    let server = start_test_server_as(&db, Some("memory"), ACTOR).await.expect("server starts");
    let client = connect(&server.url).await;
    (db, server, client)
}

async fn call_ok(client: &Client, tool: &str, args: Value) -> Value {
    match call(client, tool, args).await {
        Outcome::Ok(v) => v,
        other => panic!("{tool}: {other:?}"),
    }
}

// Every key of `expected` is in `actual` with the same value (recursively;
// arrays element by element), as vitest's toMatchObject.
fn matches(actual: &Value, expected: &Value) -> bool {
    match (actual, expected) {
        (Value::Object(a), Value::Object(e)) => e.iter().all(|(k, v)| a.get(k).is_some_and(|x| matches(x, v))),
        (Value::Array(a), Value::Array(e)) => a.len() == e.len() && a.iter().zip(e).all(|(x, y)| matches(x, y)),
        _ => actual == expected,
    }
}

#[tokio::test]
async fn journal_tools_end_to_end() {
    let (db, _server, client) = setup().await;

    // appends, tails newest-first and stamps the instance's actor
    call_ok(&client, "journal_append", json!({ "stream": "news-digest", "body": "Monday" })).await;
    let appended =
        call_ok(&client, "journal_append", json!({ "stream": "news-digest", "body": "Tuesday", "capacity": 2 })).await;
    let want = json!({
        "ok": true,
        "entry": { "stream": "news-digest", "body": "Tuesday", "actor": ACTOR },
        "stream": { "name": "news-digest", "capacity": 2, "size": 2 },
    });
    assert!(matches(&appended, &want), "{appended}");
    let tail = call_ok(&client, "journal_tail", json!({ "stream": "news-digest", "n": 5 })).await;
    let want = json!({ "ok": true, "stream": "news-digest", "entries": [{ "body": "Tuesday" }, { "body": "Monday" }] });
    assert!(matches(&tail, &want), "{tail}");

    // lists journal streams next to the projects
    call_ok(&client, "create_project", json!({ "slug": "graphs", "title": "Graphs" })).await;
    call_ok(&client, "journal_append", json!({ "stream": "digest/news", "body": "x" })).await;
    let listed = call_ok(&client, "list_memory", json!({})).await;
    let want = json!({
        "ok": true,
        "projects": [{ "slug": "graphs" }],
        "journals": [{ "name": "digest/news", "capacity": 100, "size": 1 }, { "name": "news-digest", "capacity": 2, "size": 2 }],
    });
    assert!(matches(&listed, &want), "{listed}");

    // returns a bad stream name as a result, not an exception
    let bad = call_ok(&client, "journal_append", json!({ "stream": "News", "body": "x" })).await;
    assert!(matches(&bad, &json!({ "ok": false, "error": "invalid_stream" })), "{bad}");

    db.drop_all().await;
}
