// Peer messaging: another agent (Claude Code, ChatGPT, …) talks to ouro. A
// peer's message opens or continues a thread and becomes a `peer` signal;
// ouro answers in the thread with reply_to_peer, and the peer reads the answer
// back with read_replies. The peer endpoint (the tunnel) and the supervisor's
// server are separate instances on one database, so a wait here wakes through
// the database's LISTEN (db.rs, wake-ups), not only an in-process Notify.
//
// A peer is trusted as the user (.claude/tasks/peer-messaging.md, P1). Who
// wrote a row is the instance's actor; the name a peer gives itself is a label.
//
// Sections:
//   1. store — threads, messages, the wait for a reply
//   2. tools — `peer` (ask_ouro, read_replies) for the peer endpoint,
//              `peer-reply` (reply_to_peer, get_peer_thread) for ouro

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::{schemars, tool, tool_router};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

use crate::db::{Db, sql_time};
use crate::pg::storable_text;
use crate::server::{McpTools, ToolResult, invalid_params, json_result};
use crate::signals::{MAX_WAIT_MS, Signals};

// ── 1. store ─────────────────────────────────────────────────────────────────

pub const SIGNAL_SOURCE: &str = "peer";

#[derive(Clone)]
pub struct Peer {
    db: Db,
    arrived: Arc<Notify>,
}

#[derive(Debug, Serialize)]
pub struct Asked {
    #[serde(rename = "threadId")]
    pub thread_id: i64,
    #[serde(rename = "messageId")]
    pub message_id: i64,
}

#[derive(Debug, Serialize)]
pub struct Reply {
    pub id: i64,
    pub text: String,
    pub created_at: String,
}

#[derive(Debug, Serialize)]
pub struct Message {
    pub id: i64,
    pub direction: String,
    pub actor: String,
    // The name the peer gave itself; null on ouro's replies.
    pub from: Option<String>,
    pub text: String,
    pub created_at: String,
}

impl Peer {
    pub fn new(db: Db) -> Self {
        Self { db, arrived: Arc::new(Notify::new()) }
    }

    pub fn arrivals(&self) -> Arc<Notify> {
        self.arrived.clone()
    }

    // The message and its signal in one transaction: a peer never gets a
    // thread id for a message ouro will not see.
    pub async fn ask(&self, actor: &str, from: Option<&str>, thread: Option<i64>, text: &str) -> anyhow::Result<Asked> {
        let mut client = self.db.client().await?;
        let tx = client.transaction().await?;
        let thread_id = match thread {
            Some(id) => {
                anyhow::ensure!(
                    tx.query_opt("SELECT 1 FROM peer_threads WHERE id = $1", &[&id]).await?.is_some(),
                    "Unknown thread {id}"
                );
                id
            }
            None => tx.query_one("INSERT INTO peer_threads DEFAULT VALUES RETURNING id", &[]).await?.get(0),
        };
        let from = from.map(|f| f.split_whitespace().collect::<Vec<_>>().join(" ")).filter(|f| !f.is_empty());
        let message_id: i64 = tx
            .query_one(
                "INSERT INTO peer_messages (thread_id, direction, actor, sender, text)
                 VALUES ($1, 'in', $2, $3, $4) RETURNING id",
                &[&thread_id, &actor, &from.as_deref().map(storable_text), &storable_text(text)],
            )
            .await?
            .get(0);
        Signals::insert(&*tx, SIGNAL_SOURCE, &signal_content(actor, from.as_deref(), thread_id, text)).await?;
        tx.commit().await?;
        Ok(Asked { thread_id, message_id })
    }

    pub async fn reply(&self, actor: &str, thread: i64, text: &str) -> anyhow::Result<i64> {
        self.require_thread(thread).await?;
        let id = self
            .db
            .client()
            .await?
            .query_one(
                "INSERT INTO peer_messages (thread_id, direction, actor, text)
                 VALUES ($1, 'out', $2, $3) RETURNING id",
                &[&thread, &actor, &storable_text(text)],
            )
            .await?
            .get(0);
        self.arrived.notify_waiters();
        Ok(id)
    }

    // Ouro's replies in the thread after `after`, waiting up to `timeout` for
    // the first one.
    pub async fn wait_for_replies(
        &self,
        thread: i64,
        after: Option<i64>,
        timeout: Duration,
    ) -> anyhow::Result<Vec<Reply>> {
        self.require_thread(thread).await?;
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            // Registered before the read, so a reply in between still wakes it.
            let arrived = self.arrived.notified();
            tokio::pin!(arrived);
            arrived.as_mut().enable();
            let replies = self.replies(thread, after).await?;
            if !replies.is_empty() {
                return Ok(replies);
            }
            tokio::select! {
                _ = &mut arrived => {}
                _ = tokio::time::sleep_until(deadline) => return self.replies(thread, after).await,
            }
        }
    }

    async fn replies(&self, thread: i64, after: Option<i64>) -> anyhow::Result<Vec<Reply>> {
        let rows = self
            .db
            .client()
            .await?
            .query(
                "SELECT id, text, created_at FROM peer_messages
                  WHERE thread_id = $1 AND direction = 'out' AND id > $2 ORDER BY id",
                &[&thread, &after.unwrap_or(0)],
            )
            .await?;
        Ok(rows.iter().map(|r| Reply { id: r.get(0), text: r.get(1), created_at: sql_time(r.get(2)) }).collect())
    }

    pub async fn thread(&self, thread: i64) -> anyhow::Result<Vec<Message>> {
        self.require_thread(thread).await?;
        let rows = self
            .db
            .client()
            .await?
            .query(
                "SELECT id, direction, actor, sender, text, created_at FROM peer_messages
                  WHERE thread_id = $1 ORDER BY id",
                &[&thread],
            )
            .await?;
        Ok(rows
            .iter()
            .map(|r| Message {
                id: r.get(0),
                direction: r.get(1),
                actor: r.get(2),
                from: r.get(3),
                text: r.get(4),
                created_at: sql_time(r.get::<_, DateTime<Utc>>(5)),
            })
            .collect())
    }

    async fn require_thread(&self, thread: i64) -> anyhow::Result<()> {
        let row = self.db.client().await?.query_opt("SELECT 1 FROM peer_threads WHERE id = $1", &[&thread]).await?;
        anyhow::ensure!(row.is_some(), "Unknown thread {thread}");
        Ok(())
    }
}

// The first user message of ouro's session: who is asking, through which
// endpoint, the thread to answer in, and the message.
fn signal_content(actor: &str, from: Option<&str>, thread: i64, text: &str) -> String {
    let who = match from {
        Some(name) => format!("peer agent \"{name}\""),
        None => "a peer agent".to_owned(),
    };
    format!("Message from {who} (via {actor}), thread {thread}:\n\n{text}")
}

// ── 2. tools ─────────────────────────────────────────────────────────────────

const DEFAULT_WAIT_MS: u64 = 25_000;

fn required_text(text: &str) -> Result<(), rmcp::ErrorData> {
    if text.trim().is_empty() { Err(invalid_params("text must not be empty")) } else { Ok(()) }
}

#[derive(Deserialize, schemars::JsonSchema)]
struct AskParams {
    /// The message for ouro.
    text: String,
    /// Your name, shown to ouro (e.g. "claude-code"). A label only.
    from: Option<String>,
    /// Continue this thread; omit to open a new one.
    #[serde(rename = "threadId")]
    thread_id: Option<i64>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct ReadRepliesParams {
    #[serde(rename = "threadId")]
    thread_id: i64,
    /// Only replies with an id above this (the last reply you read).
    #[serde(rename = "afterId")]
    after_id: Option<i64>,
    /// How long to wait for a reply, in milliseconds. Default 25000, max 50000.
    #[serde(rename = "timeoutMs")]
    #[schemars(range(min = 0, max = 50000))]
    timeout_ms: Option<u64>,
}

#[derive(Serialize)]
struct RepliesResult {
    #[serde(rename = "threadId")]
    thread_id: i64,
    replies: Vec<Reply>,
}

#[tool_router(router = peer_tools, vis = "pub(crate)")]
impl McpTools {
    #[tool(
        name = "ask_ouro",
        title = "Send a message to ouro",
        description = "Send a message to ouro, the user's personal agent, which acts \
            on it with the user's tools and answers in the thread. Omit `threadId` \
            to open a thread, pass it to continue one. Returns `{ threadId, \
            messageId }` at once — ouro handles messages one at a time and may \
            take minutes; read the answer with read_replies."
    )]
    async fn ask_ouro(&self, Parameters(AskParams { text, from, thread_id }): Parameters<AskParams>) -> ToolResult {
        required_text(&text)?;
        let deps = &self.deps;
        json_result(&crate::try_tool!(deps.peer.ask(&deps.memory_actor, from.as_deref(), thread_id, &text).await))
    }

    #[tool(
        name = "read_replies",
        annotations(read_only_hint = true),
        title = "Read ouro's replies",
        description = "Ouro's replies in a thread after `afterId`, oldest first, as \
            `{ threadId, replies: [{ id, text, created_at }] }`. Waits up to \
            `timeoutMs` (default 25000, max 50000) for the first one; an empty \
            list means none yet — call again with the same `afterId`."
    )]
    async fn read_replies(
        &self,
        Parameters(ReadRepliesParams { thread_id, after_id, timeout_ms }): Parameters<ReadRepliesParams>,
    ) -> ToolResult {
        let timeout_ms = timeout_ms.unwrap_or(DEFAULT_WAIT_MS);
        if timeout_ms > MAX_WAIT_MS {
            return Err(invalid_params("timeoutMs must be at most 50000"));
        }
        let wait = self.deps.peer.wait_for_replies(thread_id, after_id, Duration::from_millis(timeout_ms));
        json_result(&RepliesResult { thread_id, replies: crate::try_tool!(wait.await) })
    }
}

#[derive(Deserialize, schemars::JsonSchema)]
struct ReplyParams {
    /// The thread named in the peer signal.
    #[serde(rename = "threadId")]
    thread_id: i64,
    text: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct ThreadParams {
    #[serde(rename = "threadId")]
    thread_id: i64,
}

#[derive(Serialize)]
struct Replied {
    ok: bool,
    #[serde(rename = "messageId")]
    message_id: i64,
}

#[derive(Serialize)]
struct ThreadResult {
    #[serde(rename = "threadId")]
    thread_id: i64,
    messages: Vec<Message>,
}

#[tool_router(router = peer_reply_tools, vis = "pub(crate)")]
impl McpTools {
    #[tool(
        name = "reply_to_peer",
        title = "Reply to a peer agent",
        description = "Answer another agent in its thread — the `threadId` a `peer` \
            signal names. The peer reads it with read_replies. Returns `{ ok, \
            messageId }`."
    )]
    async fn reply_to_peer(&self, Parameters(ReplyParams { thread_id, text }): Parameters<ReplyParams>) -> ToolResult {
        required_text(&text)?;
        let deps = &self.deps;
        let message_id = crate::try_tool!(deps.peer.reply(&deps.memory_actor, thread_id, &text).await);
        json_result(&Replied { ok: true, message_id })
    }

    #[tool(
        name = "get_peer_thread",
        annotations(read_only_hint = true),
        title = "Read a peer thread",
        description = "The whole conversation in a peer thread, oldest first: \
            `{ threadId, messages: [{ id, direction ('in' from the peer, 'out' \
            from ouro), actor, from, text, created_at }] }`."
    )]
    async fn get_peer_thread(&self, Parameters(ThreadParams { thread_id }): Parameters<ThreadParams>) -> ToolResult {
        let messages = crate::try_tool!(self.deps.peer.thread(thread_id).await);
        json_result(&ThreadResult { thread_id, messages })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_the_peer_its_endpoint_and_the_thread() {
        assert_eq!(
            signal_content("chatgpt", Some("claude-code"), 7, "hi"),
            "Message from peer agent \"claude-code\" (via chatgpt), thread 7:\n\nhi"
        );
        assert_eq!(
            signal_content("chatgpt", None, 7, "hi"),
            "Message from a peer agent (via chatgpt), thread 7:\n\nhi"
        );
    }

    #[tokio::test]
    async fn a_question_queues_one_signal_and_a_reply_ends_the_wait() {
        let Some(db) = Db::test().await else { return };
        let (peer, signals) = (Peer::new(db.clone()), Signals::new(db));
        let asked = peer.ask("chatgpt", Some(" claude\ncode "), None, "ping").await.unwrap();
        let signal = signals.pop_next().await.unwrap().unwrap();
        assert_eq!(signal.source, "peer");
        assert!(signal.content.starts_with("Message from peer agent \"claude code\""), "{}", signal.content);
        let waiter = {
            let peer = peer.clone();
            tokio::spawn(async move { peer.wait_for_replies(asked.thread_id, None, Duration::from_secs(20)).await })
        };
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!waiter.is_finished(), "no reply yet");
        peer.reply("supervisor", asked.thread_id, "pong").await.unwrap();
        let replies = tokio::time::timeout(Duration::from_secs(5), waiter).await.unwrap().unwrap().unwrap();
        assert_eq!(replies.iter().map(|r| r.text.as_str()).collect::<Vec<_>>(), ["pong"]);
    }

    #[tokio::test]
    async fn an_unknown_thread_changes_nothing() {
        let Some(db) = Db::test().await else { return };
        let (peer, signals) = (Peer::new(db.clone()), Signals::new(db));
        assert_eq!(peer.ask("chatgpt", None, Some(42), "x").await.unwrap_err().to_string(), "Unknown thread 42");
        assert_eq!(peer.reply("supervisor", 42, "x").await.unwrap_err().to_string(), "Unknown thread 42");
        assert_eq!(signals.count_pending().await.unwrap(), 0);
    }
}
