// What MCP still serves the agent's schedule (crates/agent/src/schedule.rs)
// now that scheduled tasks are engine definitions: the user's time zone,
// which the agent's zone watch reads, and the environment note a fire
// carries. MCP's old task table is no longer imported (design 06): the
// maintainer recreates the tasks it held.

mod support;

use serde_json::{Value, json};
use std::collections::BTreeSet;
use support::{Outcome, call, connect, start_test_server};

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
