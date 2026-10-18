// A session owns its environment and its working memory. Every loop working
// on one signal (the primary loop, its workers, the workflow's steps) shares
// one session; rendering the context block is a separate, pure step.

use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::db::{MemoryStore, NEWS_LAST_READ_AT, TECH_LAST_READ_AT};
use crate::errors::check;
use crate::mcp::Mcp;

// Structured env data — the source of both the markdown context block and
// the workflow's `env` binding.
#[derive(Clone, Debug, PartialEq)]
pub struct EnvData {
    pub now: DateTime<Utc>,
    pub timezone: String,
    pub user_email: Option<String>,
    pub news_last_read_at: Option<String>,
    pub tech_last_read_at: Option<String>,
    // TELEGRAM_DEFAULT_CHAT_ID: where a workflow sends by default.
    pub telegram_chat_id: Option<String>,
}

// A caller-supplied label; the store neither parses nor validates JSON.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Format {
    Text,
    Json,
}

impl Format {
    pub fn as_str(self) -> &'static str {
        match self {
            Format::Text => "text",
            Format::Json => "json",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryEntry {
    pub key: String,
    pub format: Format,
    pub size_bytes: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum MemoryError {
    #[error("Key must not be empty")]
    Empty,
    #[error("Key {0} already exists")]
    Exists(String),
    #[error("Key {0} not found")]
    Missing(String),
}

fn quoted(key: &str) -> String {
    Value::String(key.to_owned()).to_string()
}

// Temporary KV shared by the loops of one task: insert-only keys, exact
// strings, insertion order kept.
#[derive(Default)]
pub struct WorkingMemory {
    entries: Mutex<Vec<(String, String, Format)>>,
}

impl WorkingMemory {
    pub fn put(&self, key: &str, value: &str, format: Format) -> Result<(), MemoryError> {
        if key.is_empty() {
            return Err(MemoryError::Empty);
        }
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        if entries.iter().any(|(k, _, _)| k == key) {
            return Err(MemoryError::Exists(quoted(key)));
        }
        entries.push((key.to_owned(), value.to_owned(), format));
        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<String, MemoryError> {
        let entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries
            .iter()
            .find(|(k, _, _)| k == key)
            .map(|(_, v, _)| v.clone())
            .ok_or_else(|| MemoryError::Missing(quoted(key)))
    }

    // Metadata in insertion order; payloads stay out.
    pub fn list(&self) -> Vec<MemoryEntry> {
        let entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries.iter().map(|(k, v, f)| MemoryEntry { key: k.clone(), format: *f, size_bytes: v.len() }).collect()
    }

    // True if removed; the key may then be reused.
    pub fn delete(&self, key: &str) -> bool {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let before = entries.len();
        entries.retain(|(k, _, _)| k != key);
        entries.len() != before
    }
}

pub struct SessionContext {
    pub id: String,
    pub env: EnvData,
    pub memory: WorkingMemory,
}

pub type Session = Arc<SessionContext>;

// Each call makes independent memory; callers share the session explicitly.
pub fn session(id: &str, env: EnvData) -> Session {
    Arc::new(SessionContext { id: id.to_owned(), env, memory: WorkingMemory::default() })
}

// "YYYY-MM-DD HH:MM" in `tz`, with the same invalid-zone failure as Intl.
pub fn format_local_time(now: DateTime<Utc>, tz: &str) -> anyhow::Result<String> {
    let zone = tz.parse::<chrono_tz::Tz>().map_err(|_| anyhow::anyhow!("Invalid time zone specified: {tz}"))?;
    Ok(now.with_timezone(&zone).format("%Y-%m-%d %H:%M").to_string())
}

// What gathering env data needs: one MCP call, one memory read and the
// user's email (read from env once, in main).
pub struct EnvDeps {
    pub mcp: Arc<dyn Mcp>,
    pub memory: Arc<dyn MemoryStore>,
    pub user_email: Option<String>,
    pub telegram_chat_id: Option<String>,
}

// The integration-owned timezone, from MCP. "UTC" when the call fails: a
// slightly wrong zone beats a crashed session.
async fn read_timezone(deps: &EnvDeps, cancel: &CancellationToken) -> anyhow::Result<String> {
    check(cancel)?;
    let raw = deps.mcp.call_tool("get_timezone", serde_json::Map::new(), cancel).await;
    check(cancel)?;
    let Ok(raw) = raw else { return Ok("UTC".into()) };
    if raw.starts_with("[tool error]") {
        return Ok("UTC".into());
    }
    let parsed: Option<Value> = serde_json::from_str(&raw).ok();
    Ok(parsed.and_then(|v| v["timezone"].as_str().map(str::to_owned)).unwrap_or_else(|| "UTC".into()))
}

pub async fn gather_env_data(deps: &EnvDeps, cancel: &CancellationToken) -> anyhow::Result<EnvData> {
    let timezone = read_timezone(deps, cancel).await?;
    Ok(EnvData {
        now: Utc::now(),
        timezone,
        user_email: deps.user_email.clone(),
        news_last_read_at: deps.memory.get(NEWS_LAST_READ_AT)?,
        tech_last_read_at: deps.memory.get(TECH_LAST_READ_AT)?,
        telegram_chat_id: deps.telegram_chat_id.clone(),
    })
}

// Pure render of the gathered env data into the markdown block.
pub fn render_context(env: &EnvData) -> anyhow::Result<String> {
    let mut lines = vec![
        "## Current context".to_owned(),
        format!("- Local time: {} ({})", format_local_time(env.now, &env.timezone)?, env.timezone),
    ];
    if let Some(email) = env.user_email.as_deref().filter(|e| !e.is_empty()) {
        lines.push(format!("- User email: {email}"));
    }
    lines.push(format!(
        "- News last read at: {}",
        env.news_last_read_at.as_deref().unwrap_or("never (bootstrap with now - 24h)")
    ));
    lines.push(format!(
        "- Tech last read at: {}",
        env.tech_last_read_at.as_deref().unwrap_or("never (bootstrap with now - 24h)")
    ));
    Ok(lines.join("\n"))
}

// JS Date#toISOString: milliseconds, Z.
pub fn iso(t: DateTime<Utc>) -> String {
    t.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

// The environment note for a signal the agent raises itself (a scheduled
// fire): where to send by default and which forum topics exist. MCP attaches
// the same note to a queued signal (`mcp_tools::signals::env_context`), from
// the same TELEGRAM_DEFAULT_CHAT_ID / TELEGRAM_TOPICS_JSON, so a skill reads
// both alike; crates/mcp/tests/schedule_handover.rs holds the two together.
pub fn telegram_env_note(default_chat_id: Option<&str>, topics: &[(String, i64)]) -> Option<String> {
    let mut lines = Vec::new();
    if let Some(chat_id) = default_chat_id {
        lines.push(format!("Default Telegram chat id: {chat_id}."));
    }
    if !topics.is_empty() {
        lines.push("Available Telegram forum topics (name → thread_id):".to_owned());
        for (name, id) in topics {
            lines.push(format!("  - {name}: {id}"));
        }
        lines.push(
            "When sending a message that semantically belongs to one of these topics, \
             pass the matching messageThreadId to send_telegram_message."
                .to_owned(),
        );
    }
    if lines.is_empty() {
        return None;
    }
    Some(["", "## Environment"].into_iter().map(str::to_owned).chain(lines).collect::<Vec<_>>().join("\n"))
}

// TELEGRAM_TOPICS_JSON='{"bills":42}', in the order written. Lenient as MCP
// is: a malformed map is no topics, and an id that is not an integer is dropped.
pub fn parse_telegram_topics(raw: Option<&str>) -> Vec<(String, i64)> {
    let Some(Ok(Value::Object(entries))) = raw.map(serde_json::from_str::<Value>) else { return Vec::new() };
    entries.into_iter().filter_map(|(name, id)| id.as_i64().map(|id| (name, id))).collect()
}
