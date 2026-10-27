// Port of src/session-context.test.ts, one test per case; the context-block
// rendering and env-gathering tests at the end are Rust-only.
mod support;
use agent::session::{Format, MemoryEntry, WorkingMemory, format_local_time, render_context};
use proptest::prelude::*;

fn entry(key: &str, format: Format, size_bytes: usize) -> MemoryEntry {
    MemoryEntry { key: key.into(), format, size_bytes }
}

fn memory() -> WorkingMemory {
    WorkingMemory::default()
}

#[test]
fn owns_its_identity_environment_and_a_stable_memory_instance() {
    let context = support::context();
    assert_eq!(context.id, "telegram:42");
    assert_eq!(
        context.env,
        agent::session::EnvData {
            now: "2026-09-06T12:00:00Z".parse().unwrap(),
            timezone: "Europe/Chisinau".into(),
            user_email: None,
            news_last_read_at: None,
            tech_last_read_at: None,
            telegram_chat_id: None,
        }
    );
    let memory = &context.memory;
    memory.put("news", "shared result", Format::Text).unwrap();
    assert!(std::ptr::eq(&context.memory, memory));
    assert_eq!(context.memory.get("news").unwrap(), "shared result");
}

#[test]
fn accepts_and_returns_strings_in_its_public_contract() {
    // The TS test pins the types; here the signatures do, at compile time.
    let memory = memory();
    let put: fn(&WorkingMemory, &str, &str, Format) -> Result<(), agent::session::MemoryError> = WorkingMemory::put;
    put(&memory, "k", "v", Format::Text).unwrap();
    let value: String = memory.get("k").unwrap();
    assert_eq!(value, "v");
}

#[test]
fn starts_empty_without_shared_state_between_instances() {
    let first = support::context();
    let second = support::context();
    assert!(first.memory.list().is_empty());
    assert!(second.memory.list().is_empty());
    first.memory.put("news", "first run", Format::Text).unwrap();
    assert!(second.memory.list().is_empty());
    assert_eq!(second.memory.get("news").unwrap_err().to_string(), "Key \"news\" not found");

    second.memory.put("news", "second run", Format::Text).unwrap();
    first.memory.delete("news");
    assert_eq!(second.memory.get("news").unwrap(), "second run");
}

#[test]
fn preserves_values_exactly_on_repeated_reads() {
    let long = "news\n".repeat(10_000);
    for value in [
        "",
        "  first\r\nsecond\n\t",
        "News 📰 e\u{0301}",
        "{ \"items\": [1, null, false] }",
        "${news.raw} {\"ref\":\"news\"}",
        long.as_str(),
    ] {
        let memory = memory();
        memory.put("data", value, Format::Text).unwrap();
        assert_eq!(memory.get("data").unwrap(), value);
        assert_eq!(memory.get("data").unwrap(), value);
        assert_eq!(memory.list().len(), 1);
    }
}

proptest! {
    // The table above, generalised: any string comes back byte for byte.
    #[test]
    fn preserves_arbitrary_strings(value in ".{0,500}") {
        let memory = memory();
        memory.put("key", &value, Format::Json).unwrap();
        prop_assert_eq!(memory.get("key").unwrap(), value.clone());
        prop_assert_eq!(memory.list(), vec![entry("key", Format::Json, value.len())]);
    }
}

#[test]
fn uses_exact_flat_keys_without_interpreting_paths_or_object_properties() {
    let memory = memory();
    let keys = ["news", "news.raw", "news[0]", "__proto__", "constructor", " news "];
    for key in keys {
        memory.put(key, &format!("value of {key}"), Format::Text).unwrap();
    }
    for key in keys {
        assert_eq!(memory.get(key).unwrap(), format!("value of {key}"));
    }
    assert_eq!(memory.get("news.raw.title").unwrap_err().to_string(), "Key \"news.raw.title\" not found");
    assert_eq!(memory.get("News").unwrap_err().to_string(), "Key \"News\" not found");
    assert_eq!(memory.list().len(), keys.len());
}

#[test]
fn rejects_an_empty_key_without_changing_stored_data() {
    let memory = memory();
    memory.put("existing", "keep", Format::Text).unwrap();
    assert_eq!(memory.put("", "value", Format::Text).unwrap_err().to_string(), "Key must not be empty");
    assert_eq!(memory.list(), vec![entry("existing", Format::Text, 4)]);
    assert_eq!(memory.get("existing").unwrap(), "keep");
}

#[test]
fn rejects_duplicate_keys_without_replacing_their_value_or_format() {
    let memory = memory();
    memory.put("result", "original", Format::Text).unwrap();
    let before = memory.list();
    assert_eq!(
        memory.put("result", "original", Format::Text).unwrap_err().to_string(),
        "Key \"result\" already exists"
    );
    assert_eq!(
        memory.put("result", "{\"new\":true}", Format::Json).unwrap_err().to_string(),
        "Key \"result\" already exists"
    );
    assert_eq!(memory.get("result").unwrap(), "original");
    assert_eq!(memory.list(), before);
}

#[test]
fn reports_a_missing_key_while_preserving_an_existing_empty_string() {
    let memory = memory();
    memory.put("empty", "", Format::Text).unwrap();
    assert_eq!(memory.get("empty").unwrap(), "");
    assert_eq!(memory.get("missing").unwrap_err().to_string(), "Key \"missing\" not found");
    assert_eq!(memory.list(), vec![entry("empty", Format::Text, 0)]);
}

#[test]
fn defaults_to_text_and_keeps_an_explicit_format_without_parsing_the_value() {
    let memory = memory();
    memory.put("text", "null", Format::Text).unwrap();
    memory.put("json", " { \"a\": [1, true] }\n", Format::Json).unwrap();
    memory.put("incomplete-json", "{\"a\":", Format::Json).unwrap();
    assert_eq!(memory.get("text").unwrap(), "null");
    assert_eq!(memory.get("json").unwrap(), " { \"a\": [1, true] }\n");
    assert_eq!(memory.get("incomplete-json").unwrap(), "{\"a\":");
    let formats: Vec<_> = memory.list().into_iter().map(|e| (e.key, e.format)).collect();
    assert_eq!(
        formats,
        vec![("text".into(), Format::Text), ("json".into(), Format::Json), ("incomplete-json".into(), Format::Json)]
    );
}

#[test]
fn lists_only_metadata_with_utf8_byte_sizes_without_exposing_payloads() {
    let memory = memory();
    memory.put("ascii", "abc", Format::Text).unwrap();
    memory.put("unicode", "Я📰", Format::Text).unwrap();
    memory.put("json", "[1,2]", Format::Json).unwrap();
    assert_eq!(
        memory.list(),
        vec![entry("ascii", Format::Text, 3), entry("unicode", Format::Text, 6), entry("json", Format::Json, 5)]
    );
}

#[test]
fn returns_detached_metadata_so_callers_cannot_modify_the_store() {
    let memory = memory();
    memory.put("news", "data", Format::Json).unwrap();
    let mut entries = memory.list();
    entries[0].key = "renamed".into();
    entries[0].format = Format::Text;
    entries[0].size_bytes = 999;
    entries.clear();
    assert_eq!(memory.get("news").unwrap(), "data");
    assert_eq!(memory.get("renamed").unwrap_err().to_string(), "Key \"renamed\" not found");
    assert_eq!(memory.list(), vec![entry("news", Format::Json, 4)]);
}

#[test]
fn deletes_only_the_exact_key_and_reports_whether_it_existed() {
    let memory = memory();
    memory.put("news", "", Format::Text).unwrap();
    memory.put("news.raw", "keep", Format::Text).unwrap();
    assert!(!memory.delete("missing"));
    assert!(memory.delete("news"));
    assert!(!memory.delete("news"));
    assert_eq!(memory.get("news").unwrap_err().to_string(), "Key \"news\" not found");
    assert_eq!(memory.get("news.raw").unwrap(), "keep");
    assert_eq!(memory.list(), vec![entry("news.raw", Format::Text, 4)]);
}

#[test]
fn allows_a_deleted_key_to_be_explicitly_reused() {
    let memory = memory();
    memory.put("result", "old", Format::Text).unwrap();
    memory.delete("result");
    memory.put("result", "[1]", Format::Json).unwrap();
    assert_eq!(memory.get("result").unwrap(), "[1]");
    assert_eq!(memory.list(), vec![entry("result", Format::Json, 3)]);
}

// Rust-only below: the context block and gathering the env.

#[test]
fn context_rendering_has_timezone_and_email_but_no_news_watermarks() {
    let mut env = support::context().env.clone();
    env.user_email = Some("user@example.test".into());
    env.news_last_read_at = Some("yesterday".into());
    let text = render_context(&env).unwrap();
    assert_eq!(
        text,
        "## Current context\n\
         - Local time: 2026-09-06 15:00 (Europe/Chisinau)\n\
         - User email: user@example.test"
    );
    assert_eq!(format_local_time("2026-10-14T00:05:00Z".parse().unwrap(), "UTC").unwrap(), "2026-10-14 00:05");
}

#[test]
fn invalid_timezone_is_rejected_like_intl() {
    assert!(format_local_time("2026-10-14T00:05:00Z".parse().unwrap(), "Bad/Zone").is_err());
}

struct BrokenMemory;
impl agent::db::MemoryStore for BrokenMemory {
    fn get(&self, _: &str) -> anyhow::Result<Option<String>> {
        anyhow::bail!("storage failed")
    }
    fn set(&self, _: &str, _: &str) -> anyhow::Result<()> {
        anyhow::bail!("storage failed")
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn environment_does_not_replace_storage_failures_with_bootstrap_watermarks() {
    let deps = agent::session::EnvDeps {
        mcp: std::sync::Arc::new(support::FakeMcp::default()),
        memory: std::sync::Arc::new(BrokenMemory),
        user_email: None,
        telegram_chat_id: None,
    };
    let error = agent::session::gather_env_data(&deps, &tokio_util::sync::CancellationToken::new()).await.unwrap_err();
    assert!(error.to_string().contains("storage failed"));
}
