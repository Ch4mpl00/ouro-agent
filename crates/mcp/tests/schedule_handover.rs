// What MCP still serves the agent's schedule (crates/agent/src/schedule.rs)
// now that the agent owns it: the user's time zone, read on every tick; the
// environment note a fire carries; and its old task table, which the agent's
// first start imports. The import runs the agent's own client and code
// against the real server, so the two sides cannot drift apart.

mod support;

use agent::mcp::{ConnectOptions, Mcp, McpClient, open_http};
use agent::schedule::{self, MemorySchedule, ScheduleStore};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use support::{Outcome, call, connect, start_test_server};
use tokio_util::sync::CancellationToken;

fn value(outcome: Outcome, tool: &str) -> Value {
    match outcome {
        Outcome::Ok(v) => v,
        other => panic!("{tool}: expected a result, got {other:?}"),
    }
}

fn keys(v: &Value) -> BTreeSet<&str> {
    v.as_object().map(|m| m.keys().map(String::as_str).collect()).unwrap_or_default()
}

#[test]
fn the_agents_first_start_imports_the_tasks_mcp_still_holds() {
    support::runtime().block_on(async {
        let db = support::test_database().await;
        let server = start_test_server(&db, None).await.unwrap();
        let digest = server.scheduler.insert("0 9 * * *", true, "digest", None).await.unwrap();
        let dreaming = server.scheduler.insert("0 4 * * *", true, "dream", Some("dreaming")).await.unwrap();
        let fired = server.scheduler.insert("30 14 2 10 *", false, "pills", None).await.unwrap();
        let client = server.db.client().await.unwrap();
        for (id, slot) in [(digest.id, 1_782_896_400_i64), (fired.id, 1_791_037_800)] {
            client.execute("UPDATE scheduled_tasks SET last_run_at = $1 WHERE id = $2", &[&slot, &id]).await.unwrap();
        }

        let mcp = McpClient::connect(open_http(server.url.clone()), ConnectOptions::default()).await.unwrap();
        let store = MemorySchedule::default();
        assert_eq!(schedule::initialize(&store, &mcp, &CancellationToken::new()).await.unwrap(), 2);
        // cron, recurring, prompt, source, last fire, created (unix seconds)
        type Imported = (String, bool, String, Option<String>, Option<i64>, i64);
        let imported: Vec<Imported> = store
            .list_active()
            .await
            .unwrap()
            .into_iter()
            .map(|t| (t.cron_expr, t.recurring == 1, t.prompt, t.source, t.last_run_at, t.created.timestamp()))
            .collect();
        // The one-shot that already fired may never fire again, so it stays behind.
        assert_eq!(
            imported,
            [
                ("0 9 * * *".into(), true, "digest".into(), None, Some(1_782_896_400), digest.created.timestamp()),
                ("0 4 * * *".into(), true, "dream".into(), Some("dreaming".into()), None, dreaming.created.timestamp()),
            ]
        );
        mcp.close().await;
        db.drop_all().await;
    });
}

#[test]
fn set_timezone_takes_exactly_the_zones_iana_knows_and_get_timezone_reads_them_back() {
    support::runtime().block_on(async {
        let db = support::test_database().await;
        let server = start_test_server(&db, None).await.unwrap();
        let client = connect(&server.url).await;
        let read = || async { value(call(&client, "get_timezone", json!({})).await, "get_timezone") };
        assert_eq!(read().await["timezone"], "UTC", "UTC until set");
        let mut current = "UTC";
        for zone in ["Europe/Kiev", "Mars/Olympus_Mons", "America/New_York", "Europe/Kyiv_", "UTC+3", "Asia/Kolkata"] {
            let reply = value(call(&client, "set_timezone", json!({ "tz": zone })).await, "set_timezone");
            let known = zone.parse::<chrono_tz::Tz>().is_ok();
            assert_eq!(reply["ok"], known, "{zone}: {reply}");
            let shape: BTreeSet<&str> =
                if known { ["ok", "timezone", "local_now"].into() } else { ["ok", "error"].into() };
            assert_eq!(keys(&reply), shape, "{zone}: {reply}");
            if known {
                current = zone;
            }
            let got = read().await;
            assert_eq!(got["timezone"], current, "{zone}: stored when accepted, unchanged when refused");
            assert_eq!(keys(&got), ["local_now", "timezone"].into());
        }
        db.drop_all().await;
    });
}

// A fire the agent raises carries the note MCP attaches to a queued signal:
// the two sides build it from the same variables, so they must agree word for
// word for any chat id and topics.
#[test]
fn the_agents_note_on_a_fire_is_mcps_note_on_a_queued_signal() {
    use mcp_tools::telegram::TelegramConfig;
    use proptest::prelude::*;
    let topics = prop::collection::vec(("[a-z]{1,8}", -5i64..100_000), 0..4);
    proptest!(|(chat in prop::option::of("-?[0-9]{1,13}"), topics in topics)| {
        let mcp = TelegramConfig { default_chat_id: chat.clone(), topics: topics.clone(), ..Default::default() };
        prop_assert_eq!(
            agent::session::telegram_env_note(chat.as_deref(), &topics),
            mcp_tools::signals::env_context(&mcp)
        );
    });
    // And they read TELEGRAM_TOPICS_JSON alike, malformed or not.
    for raw in [Some(r#"{"bills":42,"bank":43}"#), Some(r#"{"x":"7","y":8}"#), Some("[1,2]"), Some("no"), None] {
        assert_eq!(agent::session::parse_telegram_topics(raw), mcp_tools::telegram::parse_topics(raw), "{raw:?}");
    }
}
