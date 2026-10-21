// Telegram bot domain — the canonical Telegram surface. Incoming messages
// become signals; outgoing ones are tool calls; both land in one chat log.
//
// Sections:
//   1. config  — token, default chat, forum topic map (parsed once at boot)
//   2. bot api — the handful of Bot API methods used, over plain HTTP
//   3. chat log — `telegram_messages` + the poller's `telegram_kv` cursor
//   4. typing  — chat-action keep-alive (the indicator only lives ~5s)
//   5. status  — one live progress bubble edited in place, self-animating
//   6. poller  — getUpdates long-poll → chat log + `telegram` signals
//   7. tools   — `telegram-send` (send only) and `telegram` (everything)

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rmcp::handler::server::wrapper::Parameters;
use rmcp::{ErrorData, schemars, tool, tool_router};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::db::{Db, sql_time};
use crate::pg::storable_text;
use crate::server::{McpTools, ToolResult, invalid_params, json_result, respond};
use crate::signals::Signals;
use crate::time::iso_from_unix;

// ── 1. config ────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Default)]
pub struct TelegramConfig {
    // TELEGRAM_ASSISTANT_BOT_TOKEN (BotFather). Optional at boot: only the
    // calls that need it fail without it, as in the TS server.
    pub bot_token: Option<String>,
    // TELEGRAM_DEFAULT_CHAT_ID: the one chat the poller accepts messages from
    // and the default destination for agent replies.
    pub default_chat_id: Option<String>,
    // TELEGRAM_TOPICS_JSON='{"bills":42,"bank":43}' — name → forum thread id.
    // Bot API has no method to list forum topics, so this static map is the
    // only way the agent learns which topics exist. Kept in the order the
    // JSON lists them, which is the order the prompt shows them in.
    pub topics: Vec<(String, i64)>,
    // Where the Bot API lives; None is api.telegram.org. A test points it at
    // a stand-in.
    pub api_root: Option<String>,
}

const API_ROOT: &str = "https://api.telegram.org";

impl TelegramConfig {
    pub fn from_env() -> Self {
        let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
        Self {
            bot_token: var("TELEGRAM_ASSISTANT_BOT_TOKEN"),
            default_chat_id: var("TELEGRAM_DEFAULT_CHAT_ID"),
            topics: parse_topics(var("TELEGRAM_TOPICS_JSON").as_deref()),
            api_root: None,
        }
    }
}

// Lenient by design: a malformed map degrades to "no topics" rather than
// failing boot, and non-integer ids are dropped.
pub fn parse_topics(raw: Option<&str>) -> Vec<(String, i64)> {
    let Some(raw) = raw else { return Vec::new() };
    let Ok(Value::Object(entries)) = serde_json::from_str(raw) else {
        if !raw.trim().is_empty() {
            tracing::warn!("TELEGRAM_TOPICS_JSON is not a JSON object, ignoring");
        }
        return Vec::new();
    };
    entries.into_iter().filter_map(|(name, id)| id.as_i64().map(|id| (name, id))).collect()
}

// ── 2. bot api ───────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum TelegramError {
    #[error("TELEGRAM_ASSISTANT_BOT_TOKEN is not set. Add it to .env (BotFather token).")]
    NoToken,
    #[error("Telegram {method} failed ({status}): {description}")]
    Api { method: &'static str, status: u16, description: String },
    #[error(transparent)]
    Http(#[from] reqwest::Error),
}

impl TelegramError {
    fn description(&self) -> &str {
        match self {
            TelegramError::Api { description, .. } => description,
            _ => "",
        }
    }
}

#[derive(Clone)]
pub struct BotApi {
    http: reqwest::Client,
    token: Option<String>,
    root: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SentMessage {
    pub message_id: i64,
    pub date: i64,
    pub chat: SentChat,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SentChat {
    pub id: i64,
}

#[derive(Debug, Deserialize)]
pub struct Update {
    pub update_id: i64,
    pub message: Option<UpdateMessage>,
    pub edited_message: Option<UpdateMessage>,
    pub channel_post: Option<UpdateMessage>,
    pub callback_query: Option<CallbackQuery>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateMessage {
    pub message_id: i64,
    pub chat: UpdateChat,
    pub text: Option<String>,
    pub message_thread_id: Option<i64>,
    pub reply_markup: Option<InlineKeyboard>,
}

#[derive(Debug, Deserialize)]
pub struct CallbackQuery {
    pub id: String,
    pub message: Option<UpdateMessage>,
    pub data: Option<String>,
}
#[derive(Debug, Deserialize)]
pub struct InlineKeyboard { pub inline_keyboard: Vec<Vec<CallbackButton>> }
#[derive(Debug, Deserialize)]
pub struct CallbackButton { pub text: String, pub callback_data: Option<String> }

/// Rows of buttons. A press becomes a telegram_button signal or a workflow event.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct Button {
    pub text: String,
    pub data: String,
}
pub type Buttons = Vec<Vec<Button>>;

fn check_buttons(buttons: Option<&Buttons>) -> Result<(), ErrorData> {
    for b in buttons.into_iter().flatten().flatten() {
        if b.text.trim().is_empty() { return Err(invalid_params("button text must be non-empty")); }
        if !(1..=64).contains(&b.data.len()) { return Err(invalid_params("button data must be 1–64 bytes")); }
    }
    Ok(())
}
fn markup(buttons: &Buttons) -> Value {
    json!({"inline_keyboard":buttons.iter().map(|row| row.iter().map(|b|
        json!({"text":b.text,"callback_data":b.data})).collect::<Vec<_>>()).collect::<Vec<_>>()})
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpdateChat {
    pub id: i64,
    #[serde(rename = "type")]
    pub kind: String,
    pub title: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub username: Option<String>,
}

#[derive(Deserialize)]
struct Envelope {
    ok: bool,
    description: Option<String>,
    result: Option<Value>,
}

// Long-poll window inside getUpdates, seconds. The HTTP timeout must outlast it.
const LONG_POLL_TIMEOUT: u64 = 25;

impl BotApi {
    pub fn new(http: reqwest::Client, token: Option<String>) -> Self {
        Self::with_root(http, token, None)
    }

    pub fn with_root(http: reqwest::Client, token: Option<String>, root: Option<String>) -> Self {
        Self { http, token, root: root.unwrap_or_else(|| API_ROOT.to_owned()) }
    }

    async fn call<T: serde::de::DeserializeOwned>(
        &self,
        method: &'static str,
        body: Value,
    ) -> Result<T, TelegramError> {
        let token = self.token.as_deref().ok_or(TelegramError::NoToken)?;
        let res = self
            .http
            .post(format!("{}/bot{token}/{method}", self.root))
            .timeout(Duration::from_secs(LONG_POLL_TIMEOUT + 15))
            .json(&body)
            .send()
            .await?;
        let status = res.status().as_u16();
        let envelope: Envelope = res.json().await?;
        if !envelope.ok {
            let description = envelope.description.unwrap_or_else(|| "unknown error".into());
            return Err(TelegramError::Api { method, status, description });
        }
        let result = envelope.result.unwrap_or(Value::Null);
        serde_json::from_value(result).map_err(|err| TelegramError::Api {
            method,
            status,
            description: format!("unexpected result shape: {err}"),
        })
    }

    pub async fn send_message(
        &self,
        chat_id: &str,
        text: &str,
        thread_id: Option<i64>,
    ) -> Result<SentMessage, TelegramError> {
        self.send_message_buttons(chat_id, text, thread_id, None).await
    }

    pub async fn send_message_buttons(&self, chat_id: &str, text: &str, thread_id: Option<i64>, buttons: Option<&Buttons>) -> Result<SentMessage, TelegramError> {
        self.call(
            "sendMessage",
            without_nulls(json!({ "chat_id": chat_id, "text": text, "message_thread_id": thread_id, "reply_markup": buttons.map(markup) })),
        )
        .await
    }

    // "message is not modified" when the text is unchanged — a TelegramError
    // callers may ignore.
    pub async fn edit_message_text(
        &self,
        chat_id: &str,
        message_id: i64,
        text: &str,
    ) -> Result<SentMessage, TelegramError> {
        self.edit_message_buttons(chat_id, message_id, text, None).await
    }

    pub async fn edit_message_buttons(&self, chat_id: &str, message_id: i64, text: &str, buttons: Option<&Buttons>) -> Result<SentMessage, TelegramError> {
        self.call("editMessageText", json!({ "chat_id": chat_id, "message_id": message_id, "text": text,
            "reply_markup": markup(&buttons.cloned().unwrap_or_default()) })).await
    }

    pub async fn answer_callback_query(&self, id: &str) -> Result<(), TelegramError> {
        self.call::<Value>("answerCallbackQuery", json!({"callback_query_id":id})).await.map(drop)
    }

    // A bot may only delete its own messages, within 48h.
    pub async fn delete_message(&self, chat_id: &str, message_id: i64) -> Result<(), TelegramError> {
        self.call::<Value>("deleteMessage", json!({ "chat_id": chat_id, "message_id": message_id })).await.map(drop)
    }

    pub async fn send_chat_action(
        &self,
        chat_id: &str,
        action: ChatAction,
        thread_id: Option<i64>,
    ) -> Result<(), TelegramError> {
        let body = without_nulls(json!({ "chat_id": chat_id, "action": action, "message_thread_id": thread_id }));
        self.call::<Value>("sendChatAction", body).await.map(drop)
    }

    pub async fn get_updates(&self, offset: Option<i64>, timeout: Option<u64>) -> Result<Vec<Update>, TelegramError> {
        let body = without_nulls(json!({
            "offset": offset,
            "timeout": timeout,
            "allowed_updates": timeout.map(|_| ["message", "edited_message", "callback_query"]),
        }));
        self.call("getUpdates", body).await
    }
}

// JSON.stringify drops `undefined` fields; Telegram rejects explicit nulls
// for some of them (message_thread_id: null → "Bad Request").
fn without_nulls(mut value: Value) -> Value {
    if let Value::Object(map) = &mut value {
        map.retain(|_, v| !v.is_null());
    }
    value
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ChatAction {
    #[default]
    Typing,
    UploadPhoto,
    RecordVideo,
    UploadVideo,
    RecordVoice,
    UploadVoice,
    UploadDocument,
    ChooseSticker,
    FindLocation,
    RecordVideoNote,
    UploadVideoNote,
}

// ── 3. chat log ──────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct ChatLog {
    db: Db,
}

#[derive(Debug, Serialize)]
pub struct StoredMessage {
    pub id: i64,
    pub chat_id: i64,
    pub tg_message_id: Option<i64>,
    pub thread_id: Option<i64>,
    pub role: String,
    pub text: String,
    pub created_at: String,
}

#[derive(Clone, Copy)]
pub enum Role {
    User,
    Assistant,
}

impl Role {
    fn as_str(self) -> &'static str {
        match self {
            Role::User => "user",
            Role::Assistant => "assistant",
        }
    }
}

impl ChatLog {
    pub fn new(db: Db) -> Self {
        Self { db }
    }

    pub async fn record(
        &self,
        chat_id: i64,
        tg_message_id: Option<i64>,
        thread_id: Option<i64>,
        role: Role,
        text: &str,
    ) -> anyhow::Result<i64> {
        let row = self
            .db
            .client()
            .await?
            .query_one(
                "INSERT INTO telegram_messages (chat_id, tg_message_id, thread_id, role, text)
                 VALUES ($1, $2, $3, $4, $5) RETURNING id",
                &[&chat_id, &tg_message_id, &thread_id, &role.as_str(), &storable_text(text)],
            )
            .await?;
        Ok(row.get(0))
    }

    // Last `limit` messages, chronological. `thread_id` scopes to one forum
    // topic; None means every topic interleaved.
    pub async fn history(
        &self,
        chat_id: i64,
        limit: i64,
        thread_id: Option<i64>,
    ) -> anyhow::Result<Vec<StoredMessage>> {
        const COLUMNS: &str = "id, chat_id, tg_message_id, thread_id, role, text, created_at";
        let client = self.db.client().await?;
        let rows = match thread_id {
            None => {
                client
                    .query(&format!("SELECT {COLUMNS} FROM telegram_messages WHERE chat_id = $1 ORDER BY id DESC LIMIT $2"), &[
                        &chat_id, &limit,
                    ])
                    .await?
            }
            Some(thread) => {
                client
                    .query(
                        &format!(
                            "SELECT {COLUMNS} FROM telegram_messages WHERE chat_id = $1 AND thread_id = $2 ORDER BY id DESC LIMIT $3"
                        ),
                        &[&chat_id, &thread, &limit],
                    )
                    .await?
            }
        };
        Ok(rows
            .iter()
            .rev()
            .map(|r| StoredMessage {
                id: r.get(0),
                chat_id: r.get(1),
                tg_message_id: r.get(2),
                thread_id: r.get(3),
                role: r.get(4),
                text: r.get(5),
                created_at: sql_time(r.get(6)),
            })
            .collect())
    }

    // The bot edited one of its messages: the log shows what the chat shows.
    pub async fn edited(&self, chat_id: i64, tg_message_id: i64, text: &str) -> anyhow::Result<()> {
        self.db
            .client()
            .await?
            .execute(
                "UPDATE telegram_messages SET text = $3 WHERE chat_id = $1 AND tg_message_id = $2 AND role = 'assistant'",
                &[&chat_id, &tg_message_id, &storable_text(text)],
            )
            .await?;
        Ok(())
    }

    pub async fn last_update_id(&self) -> anyhow::Result<Option<i64>> {
        let row = self
            .db
            .client()
            .await?
            .query_opt("SELECT value FROM telegram_kv WHERE key = 'last_update_id'", &[])
            .await?;
        Ok(row.and_then(|r| r.get::<_, String>(0).parse().ok()))
    }

    pub async fn set_last_update_id(&self, id: i64) -> anyhow::Result<()> {
        self.db
            .client()
            .await?
            .execute(
                "INSERT INTO telegram_kv (key, value) VALUES ('last_update_id', $1)
                 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                &[&id.to_string()],
            )
            .await?;
        Ok(())
    }
}

// ── 4. typing ────────────────────────────────────────────────────────────────

// The agent calls start_typing once; this re-sends the action every ~4s until
// send_telegram_message to the same chat/thread clears it, or a safety TTL
// passes so a crashed session can't leave the dots on forever.
#[derive(Clone)]
pub struct Typing {
    bot: BotApi,
    active: Arc<Mutex<HashMap<String, TypingEntry>>>,
}

#[derive(Clone)]
struct TypingEntry {
    chat_id: String,
    action: ChatAction,
    thread_id: Option<i64>,
    expires_at: Instant,
}

const TYPING_TICK: Duration = Duration::from_secs(4);
const TYPING_TTL: Duration = Duration::from_secs(5 * 60);

fn typing_key(chat_id: &str, thread_id: Option<i64>) -> String {
    format!("{chat_id}:{}", thread_id.unwrap_or(0))
}

impl Typing {
    pub fn new(bot: BotApi) -> Self {
        Self { bot, active: Arc::default() }
    }

    fn entries(&self) -> std::sync::MutexGuard<'_, HashMap<String, TypingEntry>> {
        self.active.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub async fn start(&self, chat_id: &str, action: ChatAction, thread_id: Option<i64>) {
        let entry =
            TypingEntry { chat_id: chat_id.to_owned(), action, thread_id, expires_at: Instant::now() + TYPING_TTL };
        self.entries().insert(typing_key(chat_id, thread_id), entry);
        // Fire once now so the indicator shows without waiting for a tick.
        if let Err(err) = self.bot.send_chat_action(chat_id, action, thread_id).await {
            tracing::error!(chat_id, %err, "typing: initial send failed");
        }
    }

    pub fn stop(&self, chat_id: &str, thread_id: Option<i64>) {
        self.entries().remove(&typing_key(chat_id, thread_id));
    }

    pub async fn run(self, cancel: CancellationToken) {
        let mut interval = tokio::time::interval(TYPING_TICK);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = interval.tick() => {}
            }
            let due: Vec<(String, TypingEntry)> = {
                let mut active = self.entries();
                let now = Instant::now();
                active.retain(|_, e| e.expires_at >= now);
                active.iter().map(|(k, e)| (k.clone(), e.clone())).collect()
            };
            for (key, entry) in due {
                if let Err(err) = self.bot.send_chat_action(&entry.chat_id, entry.action, entry.thread_id).await {
                    tracing::error!(key, %err, "typing: keepalive failed");
                }
            }
        }
    }
}

// ── 5. status ────────────────────────────────────────────────────────────────

// One progress bubble per caller-chosen id (e.g. `status:<signalId>`): the
// first call sends it, later calls edit it, empty text deletes it. On top of
// that the bubble animates itself — trailing dots cycle 0 → . → .. → ... —
// so "aliveness" is MCP's job, not the workflow's. Never written to the chat
// log: it is ephemeral progress, not conversation.
#[derive(Clone)]
pub struct StatusBubbles {
    bot: BotApi,
    entries: Arc<Mutex<HashMap<String, StatusEntry>>>,
    // Calls for one id run one after another. An agent's tool calls run in
    // parallel, and two that both found no bubble each sent one — the first
    // was forgotten and stayed in the chat for good.
    queues: Arc<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
}

#[derive(Clone)]
struct StatusEntry {
    chat_id: String,
    message_id: i64,
    base_text: String,
    frame: usize,
    // Freeze (stop editing) once this passes — a forgotten bubble must not
    // edit forever.
    expires_at: Instant,
    // Back-off after a 429 / transient error.
    next_edit_at: Instant,
    // An edit is in flight; skip the tick rather than race it.
    busy: bool,
    animating: bool,
}

const STATUS_TICK: Duration = Duration::from_secs(1);
const STATUS_TTL: Duration = Duration::from_secs(3 * 60);
const STATUS_BACKOFF: Duration = Duration::from_secs(5);
// Adjacent frames always differ, so an edit is never "not modified".
const DOT_CYCLE: usize = 4;

// The animation dots only take the room left under Telegram's cap: on a text
// at the cap they used to push every frame over, the edits failed, and the
// bubble froze.
fn render_status(base: &str, frame: usize) -> String {
    let room = TELEGRAM_TEXT_LIMIT.saturating_sub(base.encode_utf16().count());
    format!("{base}{}", ".".repeat((frame % DOT_CYCLE).min(room)))
}

#[derive(Debug, Serialize)]
pub struct StatusResult {
    // created · updated · deleted · noop (clear with nothing tracked)
    action: &'static str,
    id: String,
    #[serde(rename = "chatId", skip_serializing_if = "Option::is_none")]
    chat_id: Option<String>,
    #[serde(rename = "messageId", skip_serializing_if = "Option::is_none")]
    message_id: Option<i64>,
    // Present (possibly null) only on `created`.
    #[serde(rename = "messageThreadId", skip_serializing_if = "Option::is_none")]
    message_thread_id: Option<Option<i64>>,
}

impl StatusBubbles {
    pub fn new(bot: BotApi) -> Self {
        Self { bot, entries: Arc::default(), queues: Arc::default() }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, StatusEntry>> {
        self.entries.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub async fn send(
        &self,
        id: &str,
        text: &str,
        chat_id: &str,
        thread_id: Option<i64>,
    ) -> Result<StatusResult, TelegramError> {
        let queue = self.queues.lock().unwrap_or_else(|p| p.into_inner()).entry(id.to_owned()).or_default().clone();
        let result = {
            let _turn = queue.lock().await;
            self.send_now(id, text, chat_id, thread_id).await
        };
        // Forget the queue once nobody else is waiting on it.
        let mut queues = self.queues.lock().unwrap_or_else(|p| p.into_inner());
        if queues.get(id).is_some_and(|q| Arc::ptr_eq(q, &queue) && Arc::strong_count(q) == 2) {
            queues.remove(id);
        }
        result
    }

    async fn send_now(
        &self,
        id: &str,
        text: &str,
        chat_id: &str,
        thread_id: Option<i64>,
    ) -> Result<StatusResult, TelegramError> {
        let text = text.trim();
        let existing = self.lock().get(id).cloned();

        if text.is_empty() {
            let Some(existing) = existing else {
                return Ok(StatusResult {
                    action: "noop",
                    id: id.into(),
                    chat_id: None,
                    message_id: None,
                    message_thread_id: None,
                });
            };
            self.lock().remove(id);
            match self.bot.delete_message(&existing.chat_id, existing.message_id).await {
                // Already gone (deleted by the user, or >48h old): cleared either way.
                Ok(()) | Err(TelegramError::Api { .. }) => {}
                Err(err) => return Err(err),
            }
            return Ok(StatusResult {
                action: "deleted",
                id: id.into(),
                chat_id: Some(existing.chat_id),
                message_id: Some(existing.message_id),
                message_thread_id: None,
            });
        }

        if let Some(existing) = existing {
            let refresh = |entries: &mut HashMap<String, StatusEntry>, reset_frame: bool| {
                if let Some(e) = entries.get_mut(id) {
                    e.base_text = text.to_owned();
                    if reset_frame {
                        e.frame = 0;
                        e.next_edit_at = Instant::now();
                    }
                    e.expires_at = Instant::now() + STATUS_TTL;
                    e.animating = true;
                }
            };
            match self.bot.edit_message_text(&existing.chat_id, existing.message_id, &render_status(text, 0)).await {
                Ok(edited) => {
                    refresh(&mut self.lock(), true);
                    return Ok(StatusResult {
                        action: "updated",
                        id: id.into(),
                        chat_id: Some(existing.chat_id),
                        message_id: Some(edited.message_id),
                        message_thread_id: None,
                    });
                }
                Err(err) if is_gone(&err) => {
                    // The bubble vanished upstream — re-create below.
                    self.lock().remove(id);
                }
                Err(err) if is_not_modified(&err) => {
                    refresh(&mut self.lock(), false);
                    return Ok(StatusResult {
                        action: "updated",
                        id: id.into(),
                        chat_id: Some(existing.chat_id),
                        message_id: Some(existing.message_id),
                        message_thread_id: None,
                    });
                }
                Err(err) => return Err(err),
            }
        }

        let sent = self.bot.send_message(chat_id, &render_status(text, 0), thread_id).await?;
        self.lock().insert(
            id.to_owned(),
            StatusEntry {
                chat_id: chat_id.to_owned(),
                message_id: sent.message_id,
                base_text: text.to_owned(),
                frame: 0,
                expires_at: Instant::now() + STATUS_TTL,
                next_edit_at: Instant::now(),
                busy: false,
                animating: true,
            },
        );
        Ok(StatusResult {
            action: "created",
            id: id.into(),
            chat_id: Some(chat_id.to_owned()),
            message_id: Some(sent.message_id),
            message_thread_id: Some(thread_id),
        })
    }

    pub async fn run(self, cancel: CancellationToken) {
        let mut interval = tokio::time::interval(STATUS_TICK);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = interval.tick() => {}
            }
            self.tick().await;
        }
    }

    async fn tick(&self) {
        let due: Vec<(String, StatusEntry)> = {
            let mut entries = self.lock();
            let now = Instant::now();
            let mut due = Vec::new();
            for (id, e) in entries.iter_mut() {
                if !e.animating {
                    continue;
                }
                if e.expires_at < now {
                    e.animating = false;
                    continue;
                }
                if e.busy || now < e.next_edit_at {
                    continue;
                }
                e.frame += 1;
                e.busy = true;
                due.push((id.clone(), e.clone()));
            }
            due
        };
        for (id, entry) in due {
            let result = self
                .bot
                .edit_message_text(&entry.chat_id, entry.message_id, &render_status(&entry.base_text, entry.frame))
                .await;
            let mut entries = self.lock();
            if let Some(e) = entries.get_mut(&id) {
                e.busy = false;
                if let Err(err) = &result
                    && !is_not_modified(err)
                {
                    e.next_edit_at = Instant::now() + STATUS_BACKOFF;
                }
            }
        }
    }
}

fn is_not_modified(err: &TelegramError) -> bool {
    matches!(err, TelegramError::Api { .. }) && err.description().to_lowercase().contains("not modified")
}

fn is_gone(err: &TelegramError) -> bool {
    let d = err.description().to_lowercase();
    matches!(err, TelegramError::Api { .. }) && (d.contains("not found") || d.contains("to edit"))
}

// ── 6. poller ────────────────────────────────────────────────────────────────

const ERROR_BACKOFF: Duration = Duration::from_secs(5);

// getUpdates allows one consumer per bot, so this lives in the MCP process —
// nothing else polls. Messages from any chat but the default are ignored:
// without a default chat the bot would accept anyone, so it stays off.
pub async fn run_poller(
    bot: BotApi,
    log: ChatLog,
    signals: Signals,
    config: TelegramConfig,
    cancel: CancellationToken,
) {
    let Some(raw) = config.default_chat_id else {
        tracing::warn!("TELEGRAM_DEFAULT_CHAT_ID is not set — telegram poller disabled (would accept anyone)");
        return;
    };
    let Ok(allowed_chat) = raw.parse::<i64>() else {
        tracing::warn!(raw, "TELEGRAM_DEFAULT_CHAT_ID is not a number — telegram poller disabled");
        return;
    };
    tracing::info!(chat = allowed_chat, timeout = LONG_POLL_TIMEOUT, "telegram poller started");

    let store = Stores { log, signals };
    loop {
        let round = tokio::select! {
            _ = cancel.cancelled() => break,
            round = poll_once(&bot, &store, allowed_chat, LONG_POLL_TIMEOUT) => round,
        };
        if let Err(err) = round {
            tracing::error!(error = %format!("{err:#}"), "telegram poll error");
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(ERROR_BACKOFF) => {}
            }
        }
    }
    tracing::info!("telegram poller stopped");
}

// The writes one poll round makes, behind a trait so a test can fail one of
// them partway, as a dropped connection or a crash between two writes would.
#[async_trait::async_trait]
pub trait PollerStore: Send + Sync {
    async fn last_update_id(&self) -> anyhow::Result<Option<i64>>;
    async fn set_last_update_id(&self, id: i64) -> anyhow::Result<()>;
    async fn record_message(
        &self,
        chat_id: i64,
        tg_message_id: i64,
        thread_id: Option<i64>,
        text: &str,
    ) -> anyhow::Result<()>;
    async fn record_signal(&self, source: &str, content: &str) -> anyhow::Result<()>;
}

// The real store: the chat log and the signal queue.
pub struct Stores {
    pub log: ChatLog,
    pub signals: Signals,
}

#[async_trait::async_trait]
impl PollerStore for Stores {
    async fn last_update_id(&self) -> anyhow::Result<Option<i64>> {
        self.log.last_update_id().await
    }

    async fn set_last_update_id(&self, id: i64) -> anyhow::Result<()> {
        self.log.set_last_update_id(id).await
    }

    async fn record_message(
        &self,
        chat_id: i64,
        tg_message_id: i64,
        thread_id: Option<i64>,
        text: &str,
    ) -> anyhow::Result<()> {
        self.log.record(chat_id, Some(tg_message_id), thread_id, Role::User, text).await.map(drop)
    }

    async fn record_signal(&self, source: &str, content: &str) -> anyhow::Result<()> {
        self.signals.record(source, content).await.map(drop)
    }
}

// One getUpdates round from the stored cursor: each text message from the
// allowed chat goes into the chat log and becomes a signal, and the cursor
// moves past every update, kept or not — an update left unconfirmed comes
// back on every getUpdates and stalls the cursor there for good. Returns how
// many updates came back.
pub async fn poll_once(
    bot: &BotApi,
    store: &dyn PollerStore,
    allowed_chat: i64,
    timeout: u64,
) -> anyhow::Result<usize> {
    let offset = store.last_update_id().await?.map(|id| id + 1);
    let updates = bot.get_updates(offset, Some(timeout)).await?;
    for update in &updates {
        if let Some(msg) = update.message.as_ref().or(update.edited_message.as_ref()) {
            if msg.chat.id != allowed_chat {
                tracing::warn!(chat = msg.chat.id, "ignoring message from a chat that is not the default one");
            } else if let Some(text) = msg.text.as_deref().filter(|t| !t.is_empty()) {
                store.record_message(msg.chat.id, msg.message_id, msg.message_thread_id, text).await?;
                store.record_signal("telegram", &signal_content(msg.chat.id, msg.message_thread_id, text)).await?;
                tracing::info!(message = msg.message_id, thread = ?msg.message_thread_id, "stored message + signal queued");
            }
        }
        if let Some(callback) = &update.callback_query
            && let Some(msg) = &callback.message
            && msg.chat.id == allowed_chat
            && let Some(data) = &callback.data
        {
            if let Err(error) = bot.answer_callback_query(&callback.id).await {
                tracing::warn!(%error, "could not answer button press");
            }
            let text = msg.reply_markup.as_ref().into_iter().flat_map(|k| &k.inline_keyboard)
                .flatten().find(|b| b.callback_data.as_ref() == Some(data)).map(|b| b.text.as_str());
            store.record_message(msg.chat.id, msg.message_id, msg.message_thread_id,
                &format!("[button] {}", text.unwrap_or(data))).await?;
            store.record_signal("telegram_button", &json!({"chatId":msg.chat.id,
                "messageThreadId":msg.message_thread_id,"messageId":msg.message_id,"data":data,"text":text}).to_string()).await?;
        }
        store.set_last_update_id(update.update_id).await?;
    }
    Ok(updates.len())
}

// Signal content is DATA, not instructions: the planner knows how to reply
// (send step + chat id from envContext). Text is JSON-quoted so a message
// can't masquerade as more header lines.
fn signal_content(chat_id: i64, thread_id: Option<i64>, text: &str) -> String {
    let topic = thread_id.map(|t| format!(" (forum topic thread_id={t})")).unwrap_or_default();
    let quoted = serde_json::to_string(text).unwrap_or_default();
    format!("Telegram message in chat {chat_id}{topic}.\nText: {quoted}")
}

// ── 6b. long texts ───────────────────────────────────────────────────────────

// Telegram caps a message at 4096 characters. A longer text — a digest that
// came out long — is sent as several messages instead of failing the send:
// the limit is the transport's, so the transport tool deals with it, for every
// caller at once. Plain text only: the send tool sets no parse_mode, so a cut
// can never break a Markdown or HTML entity.

// Counted the way JS strings count: UTF-16 code units, which never
// undercounts what Telegram measures.
pub const TELEGRAM_TEXT_LIMIT: usize = 4096;

// Where a part may end, best first. A cut lands right after the separator,
// so the separator stays with the part before it and nothing is lost.
const SEPARATORS: [&str; 3] = ["\n\n", "\n", " "];

// Splits `text` into parts of at most `limit` UTF-16 units whose
// concatenation is exactly `text`. Prefers ending a part at a blank line
// (between digest blocks), then at a line break, then at a space — each only
// if that keeps the part at least half full, so a stray early break doesn't
// produce a sliver of a message. Failing all of that, a hard cut that never
// splits a surrogate pair.
pub fn split_telegram_text(text: &str, limit: usize) -> Vec<String> {
    assert!(limit >= 2, "limit must be at least 2, got {limit}");
    let units: Vec<u16> = text.encode_utf16().collect();
    let mut parts = Vec::new();
    let mut rest = &units[..];
    while rest.len() > limit {
        let cut = cut_point(rest, limit);
        parts.push(String::from_utf16(&rest[..cut]).expect("cuts never split a surrogate pair"));
        rest = &rest[cut..];
    }
    if !rest.is_empty() {
        parts.push(String::from_utf16(rest).expect("whole code points"));
    }
    parts
}

fn last_index_of(window: &[u16], sep: &[u16]) -> Option<usize> {
    (0..=window.len().checked_sub(sep.len())?).rev().find(|&at| &window[at..at + sep.len()] == sep)
}

fn cut_point(text: &[u16], limit: usize) -> usize {
    let window = &text[..limit];
    for min_end in [limit.div_ceil(2), 1] {
        for sep in SEPARATORS {
            let sep: Vec<u16> = sep.encode_utf16().collect();
            if let Some(at) = last_index_of(window, &sep)
                && at + sep.len() >= min_end
            {
                return at + sep.len();
            }
        }
    }
    let high_surrogate = (0xd800..=0xdbff).contains(&text[limit - 1]);
    if high_surrogate { limit - 1 } else { limit }
}

// A send that stopped partway. The parts in `delivered` are already in the
// chat; the caller must not blindly resend the whole text.
#[derive(Debug, thiserror::Error)]
#[error("Telegram send stopped after {} of {} parts{}: {cause}", delivered.len(), parts.len(), ids_note(delivered))]
pub struct PartialSendError {
    pub delivered: Vec<SentMessage>,
    // Every part of the text, delivered or not; delivered[i] carried parts[i].
    pub parts: Vec<String>,
    cause: TelegramError,
}

fn ids_note(delivered: &[SentMessage]) -> String {
    if delivered.is_empty() {
        return String::new();
    }
    let ids: Vec<String> = delivered.iter().map(|m| m.message_id.to_string()).collect();
    format!(" (delivered message ids: {})", ids.join(", "))
}

#[derive(Debug, thiserror::Error)]
pub enum SendError {
    // Nothing delivered yet: the original error is the whole story.
    #[error(transparent)]
    Telegram(#[from] TelegramError),
    #[error(transparent)]
    Partial(#[from] PartialSendError),
}

// Sends the parts in order, one after another, into the same chat and topic.
// Parts that are only whitespace are skipped: Telegram rejects an empty
// message, and the text around them still arrives.
pub async fn send_long_message(
    bot: &BotApi,
    chat_id: &str,
    text: &str,
    thread_id: Option<i64>,
) -> Result<(Vec<String>, Vec<SentMessage>), SendError> {
    send_long_message_buttons(bot, chat_id, text, thread_id, None).await
}

pub async fn send_long_message_buttons(bot: &BotApi, chat_id: &str, text: &str, thread_id: Option<i64>, buttons: Option<&Buttons>) -> Result<(Vec<String>, Vec<SentMessage>), SendError> {
    let parts: Vec<_> = split_telegram_text(text, TELEGRAM_TEXT_LIMIT).into_iter().filter(|p| !p.trim().is_empty()).collect();
    let mut sent = Vec::new();
    for (i, part) in parts.iter().enumerate() {
        let keyboard = if i + 1 == parts.len() { buttons } else { None };
        match bot.send_message_buttons(chat_id, part, thread_id, keyboard).await {
            Ok(message) => sent.push(message),
            Err(cause) if sent.is_empty() => return Err(cause.into()),
            Err(cause) => return Err(PartialSendError { delivered: sent, parts, cause }.into()),
        }
    }
    Ok((parts, sent))
}

// The same with the sender injected (tests).
pub async fn send_parts<F, Fut>(text: &str, send: F) -> Result<(Vec<String>, Vec<SentMessage>), SendError>
where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = Result<SentMessage, TelegramError>>,
{
    let parts: Vec<String> =
        split_telegram_text(text, TELEGRAM_TEXT_LIMIT).into_iter().filter(|p| !p.trim().is_empty()).collect();
    let mut sent = Vec::new();
    for part in &parts {
        match send(part.clone()).await {
            Ok(message) => sent.push(message),
            Err(cause) if sent.is_empty() => return Err(cause.into()),
            Err(cause) => return Err(PartialSendError { delivered: sent, parts, cause }.into()),
        }
    }
    Ok((parts, sent))
}

// ── 7. tools ─────────────────────────────────────────────────────────────────

// Models send chat ids as strings and as numbers. Accept both, normalise to
// one string so the typing keep-alive key can't split into two entries.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
#[serde(untagged)]
pub enum ChatId {
    Text(String),
    Number(i64),
}

impl ChatId {
    fn into_string(self) -> String {
        match self {
            ChatId::Text(s) => s,
            ChatId::Number(n) => n.to_string(),
        }
    }
}

// The longest text send_telegram_message takes. Anything over Telegram's 4096
// goes out as several messages; the cap only stops a runaway text from
// becoming a flood.
const MAX_SEND_CHARS: usize = 4 * TELEGRAM_TEXT_LIMIT;

const NO_CHAT_TARGET: &str =
    "No chat target. Pass chatId, or set TELEGRAM_DEFAULT_CHAT_ID in .env (find your id with `telegram-get-chat-id`).";

fn check_text(text: &str, allow_empty: bool) -> Result<(), ErrorData> {
    let len = text.encode_utf16().count();
    if (!allow_empty && len == 0) || len > 4096 {
        return Err(invalid_params("text must be 1–4096 characters"));
    }
    Ok(())
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct SendParams {
    text: String,
    buttons: Option<Buttons>,
    /// Telegram chat id. Falls back to TELEGRAM_DEFAULT_CHAT_ID if omitted.
    chat_id: Option<ChatId>,
    /// Forum topic thread_id. Required to reply inside a topic; omit for non-topic chats or the General topic.
    message_thread_id: Option<i64>,
}

#[tool_router(router = telegram_send_tools, vis = "pub(crate)")]
impl McpTools {
    #[tool(
        name = "send_telegram_message",
        annotations(open_world_hint = true),
        title = "Send Telegram message",
        description = "Send a Telegram message via the assistant bot. If chatId is omitted, \
            TELEGRAM_DEFAULT_CHAT_ID env is used. Pass messageThreadId to send into \
            a specific forum topic (replies to a topic message must keep the same \
            messageThreadId so they land in the same topic; the system prompt \
            lists configured topic name → thread_id pairs if available). The \
            returned messageId can be persisted and passed to edit_telegram_message \
            later (e.g. to mark a bill as paid). The outgoing message is also \
            recorded in the local Telegram chat log so the conversation history \
            stays in sync.\n\n\
            A text over Telegram's 4096-character limit is sent as several \
            consecutive messages, split at blank lines where possible; \
            `messageIds` lists them all and `messageId` is the first. Optional buttons are rows of {text,data}; data is 1–64 bytes. Buttons appear on the last part. A press becomes a telegram_button signal or resumes a workflow waiting on that data."
    )]
    async fn send_telegram_message(&self, Parameters(p): Parameters<SendParams>) -> ToolResult {
        check_buttons(p.buttons.as_ref())?;
        let len = p.text.encode_utf16().count();
        if len == 0 || len > MAX_SEND_CHARS {
            return Err(invalid_params("text must be 1–16384 characters"));
        }
        let tg = &self.deps.telegram;
        respond(
            async {
                let target = p.chat_id.map(ChatId::into_string).or_else(|| tg.config.default_chat_id.clone());
                let target = target.ok_or_else(|| anyhow::anyhow!(NO_CHAT_TARGET))?;
                // One log row per Telegram message, so the chat history matches
                // what the user actually sees — including the parts of a send
                // that failed partway, which are in the chat all the same.
                let record = |messages: Vec<SentMessage>, parts: Vec<String>| async move {
                    for (message, part) in messages.iter().zip(parts) {
                        tg.log
                            .record(
                                message.chat.id,
                                Some(message.message_id),
                                p.message_thread_id,
                                Role::Assistant,
                                &part,
                            )
                            .await?;
                    }
                    anyhow::Ok(())
                };
                let (parts, sent) = match send_long_message_buttons(&tg.bot, &target, &p.text, p.message_thread_id, p.buttons.as_ref()).await {
                    Ok(result) => result,
                    Err(SendError::Partial(err)) => {
                        // The partial send is the story the model needs; a log
                        // failure on top of it must not replace it.
                        let _ = record(err.delivered.clone(), err.parts.clone()).await;
                        return Err(err.into());
                    }
                    Err(SendError::Telegram(err)) => return Err(err.into()),
                };
                let Some(first) = sent.first().cloned() else {
                    anyhow::bail!("Nothing to send: the text is only whitespace.");
                };
                // The outgoing message clears the indicator client-side; stop the
                // keep-alive so it doesn't bleed into the next session.
                tg.typing.stop(&target, p.message_thread_id);
                let message_ids: Vec<i64> = sent.iter().map(|m| m.message_id).collect();
                let count = sent.len();
                let mut reply = json!({
                    "delivered": true,
                    "chatId": target,
                    "messageId": first.message_id,
                    "messageIds": message_ids,
                    "parts": count,
                    "messageThreadId": p.message_thread_id,
                    "date": iso_from_unix(first.date),
                });
                // The message is in the chat now. A failed log write must not
                // turn that into a failure: the model would send it again.
                if let Err(err) = record(sent, parts).await {
                    let log_error = format!("delivered, but not recorded in the chat log: {err:#}");
                    tracing::error!("send_telegram_message: {log_error}");
                    reply["logError"] = log_error.into();
                }
                Ok(reply)
            }
            .await,
        )
    }
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct EditParams {
    buttons: Option<Buttons>,
    /// Telegram chat id (the one the original message was sent to).
    chat_id: ChatId,
    /// messageId returned by send_telegram_message.
    message_id: i64,
    text: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct StatusParams {
    /// Stable per-workflow status id, e.g. `status:<signalId>`. Same id edits the same message.
    id: String,
    /// Status text. Empty string deletes the status message.
    text: String,
    /// Telegram chat id. Falls back to TELEGRAM_DEFAULT_CHAT_ID if omitted.
    chat_id: Option<ChatId>,
    /// Forum topic thread_id (used when first creating the bubble).
    message_thread_id: Option<i64>,
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct StartTypingParams {
    /// Telegram chat id.
    chat_id: ChatId,
    /// Chat action to display. Defaults to 'typing'.
    action: Option<ChatAction>,
    /// Forum topic thread_id (display the indicator inside a specific topic).
    message_thread_id: Option<i64>,
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ChatActionParams {
    /// Telegram chat id.
    chat_id: ChatId,
    /// Chat action to display.
    action: ChatAction,
    message_thread_id: Option<i64>,
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct HistoryParams {
    /// Telegram chat id.
    chat_id: ChatId,
    /// Max messages. Default 50.
    #[schemars(range(min = 1, max = 500))]
    limit: Option<i64>,
    /// Forum topic thread_id. Restricts results to a single topic. Omit for unfiltered history.
    thread_id: Option<i64>,
}

#[tool_router(router = telegram_tools, vis = "pub(crate)")]
impl McpTools {
    #[tool(
        name = "edit_telegram_message",
        annotations(open_world_hint = true),
        title = "Edit Telegram message",
        description = "Edit a previously-sent Telegram message in place. Use to update a bill \
            notification when status changes (e.g. mark as PAID). messageId is the value \
            returned by send_telegram_message; chatId is the same chat the message was sent to. Optional buttons are rows of {text,data}; data is 1–64 bytes. Omitting buttons removes the keyboard. A press becomes a telegram_button signal or resumes a workflow waiting on that data."
    )]
    async fn edit_telegram_message(&self, Parameters(p): Parameters<EditParams>) -> ToolResult {
        check_text(&p.text, false)?;
        check_buttons(p.buttons.as_ref())?;
        let tg = &self.deps.telegram;
        let chat_id = p.chat_id.into_string();
        respond(
            async {
                let edited = tg.bot.edit_message_buttons(&chat_id, p.message_id, &p.text, p.buttons.as_ref()).await?;
                let mut reply = json!({
                    "edited": true,
                    "chatId": chat_id,
                    "messageId": edited.message_id,
                    "date": iso_from_unix(edited.date),
                });
                // The history the agent reads back should show what the user sees.
                let logged = match chat_id.trim().parse::<i64>() {
                    Ok(chat) => tg.log.edited(chat, p.message_id, &p.text).await,
                    Err(_) => Err(anyhow::anyhow!("chat id {chat_id} is not numeric")),
                };
                if let Err(err) = logged {
                    reply["logError"] = format!("edited, but not updated in the chat log: {err:#}").into();
                }
                Ok(reply)
            }
            .await,
        )
    }

    #[tool(
        name = "telegram_send_status",
        annotations(open_world_hint = true),
        title = "Send / update / clear a live status message",
        description = "Show live progress in a SINGLE Telegram message edited in place, \
            instead of posting a new message per step. Call with the same `id` \
            repeatedly to update the same bubble: the first call sends it, later \
            calls edit it. Call with an EMPTY `text` to delete the bubble when \
            the work is done. Use a stable per-workflow id like `status:<signalId>`. \
            chatId falls back to TELEGRAM_DEFAULT_CHAT_ID. Status messages are \
            ephemeral — they are NOT written to the chat log; ship the real answer \
            with send_telegram_message."
    )]
    async fn telegram_send_status(&self, Parameters(p): Parameters<StatusParams>) -> ToolResult {
        if p.id.is_empty() {
            return Err(invalid_params("id must be non-empty"));
        }
        check_text(&p.text, true)?;
        let tg = &self.deps.telegram;
        respond(
            async {
                let target = p.chat_id.map(ChatId::into_string).or_else(|| tg.config.default_chat_id.clone());
                let target = target.ok_or_else(|| anyhow::anyhow!(NO_CHAT_TARGET))?;
                Ok(tg.status.send(&p.id, &p.text, &target, p.message_thread_id).await?)
            }
            .await,
        )
    }

    #[tool(
        name = "start_typing",
        annotations(open_world_hint = true),
        title = "Start typing indicator (auto-refresh until reply)",
        description = "Show a chat action indicator (typing by default) in a Telegram chat \
            and keep it alive. MCP re-sends the action every ~4s in the \
            background — call this ONCE at the start of a session, no need to \
            ping it on every reasoning round. The indicator clears \
            automatically when your `send_telegram_message` to the same chat/\
            thread is delivered. A safety TTL stops the keep-alive after \
            5 minutes if no message ever ships."
    )]
    async fn start_typing(&self, Parameters(p): Parameters<StartTypingParams>) -> ToolResult {
        let tg = &self.deps.telegram;
        tg.typing.start(&p.chat_id.into_string(), p.action.unwrap_or_default(), p.message_thread_id).await;
        json_result(&json!({ "started": true }))
    }

    // An escape hatch for one-off non-typing actions (an `upload_photo` blip
    // before posting an image); start_typing is the self-refreshing one.
    #[tool(
        name = "send_telegram_chat_action",
        annotations(open_world_hint = true),
        title = "Send a one-shot Telegram chat action (no auto-refresh)",
        description = "Send a single chat action ping (~5s lifespan, no keep-alive). \
            Prefer `start_typing` for the common 'show typing while I work' \
            case — this one is for one-off non-typing actions like a brief \
            `upload_photo` before sending an image."
    )]
    async fn send_telegram_chat_action(&self, Parameters(p): Parameters<ChatActionParams>) -> ToolResult {
        let tg = &self.deps.telegram;
        let chat_id = p.chat_id.into_string();
        respond(
            async {
                tg.bot.send_chat_action(&chat_id, p.action, p.message_thread_id).await?;
                Ok(json!({ "sent": true }))
            }
            .await,
        )
    }

    #[tool(
        name = "get_telegram_chat_history",
        annotations(read_only_hint = true, open_world_hint = false),
        title = "Get Telegram chat history",
        description = "Read the last N messages of a Telegram chat from the local log, in \
            chronological order. Pass threadId to scope to a single forum topic \
            (reply context for a topic message). Omit threadId to see all topics \
            interleaved."
    )]
    async fn get_telegram_chat_history(&self, Parameters(p): Parameters<HistoryParams>) -> ToolResult {
        if p.limit.is_some_and(|l| !(1..=500).contains(&l)) {
            return Err(invalid_params("limit must be between 1 and 500"));
        }
        let tg = &self.deps.telegram;
        respond(
            async {
                let chat_id = match p.chat_id {
                    ChatId::Number(n) => n,
                    ChatId::Text(s) => {
                        s.trim().parse().map_err(|_| anyhow::anyhow!("chatId must be numeric, got {s}"))?
                    }
                };
                Ok(json!({ "messages": tg.log.history(chat_id, p.limit.unwrap_or(50), p.thread_id).await? }))
            }
            .await,
        )
    }
}

// Everything the telegram tools reach, bundled so `Deps` carries one field.
#[derive(Clone)]
pub struct TelegramModule {
    pub config: TelegramConfig,
    pub bot: BotApi,
    pub log: ChatLog,
    pub typing: Typing,
    pub status: StatusBubbles,
}

impl TelegramModule {
    pub fn new(config: TelegramConfig, http: reqwest::Client, db: Db) -> Self {
        let bot = BotApi::with_root(http, config.bot_token.clone(), config.api_root.clone());
        Self {
            typing: Typing::new(bot.clone()),
            status: StatusBubbles::new(bot.clone()),
            log: ChatLog::new(db),
            bot,
            config,
        }
    }
}

pub fn chat_label(chat: &UpdateChat) -> String {
    if let Some(title) = &chat.title {
        return title.clone();
    }
    [chat.first_name.clone(), chat.last_name.clone(), chat.username.as_ref().map(|u| format!("@{u}"))]
        .into_iter()
        .flatten()
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_topics_leniently_in_json_order() {
        assert!(parse_topics(None).is_empty());
        assert!(parse_topics(Some("[1,2]")).is_empty());
        assert!(parse_topics(Some("not json")).is_empty());
        let topics = parse_topics(Some(r#"{"news":7,"bank":"43","bills":42}"#));
        assert_eq!(topics, vec![("news".into(), 7), ("bills".into(), 42)]);
    }

    #[test]
    fn signal_content_quotes_the_text_and_names_the_topic() {
        assert_eq!(
            signal_content(5, Some(42), "hi \"there\"\nnext"),
            "Telegram message in chat 5 (forum topic thread_id=42).\nText: \"hi \\\"there\\\"\\nnext\""
        );
        assert_eq!(signal_content(5, None, "x"), "Telegram message in chat 5.\nText: \"x\"");
    }

    #[test]
    fn status_frames_cycle_trailing_dots() {
        let frames: Vec<String> = (0..5).map(|f| render_status("Working", f)).collect();
        assert_eq!(frames, ["Working", "Working.", "Working..", "Working...", "Working"]);
    }
}

// ── tests: splitting long texts ──────────────────────────────────────────────
// A port of packages/mcp/src/services/telegram/split.test.ts.

#[cfg(test)]
mod split_tests {
    use std::sync::Mutex;

    use proptest::prelude::*;

    use super::*;

    fn units(s: &str) -> usize {
        s.encode_utf16().count()
    }

    fn js_slice(s: &str, from: usize, to: usize) -> Vec<u16> {
        let u: Vec<u16> = s.encode_utf16().collect();
        u[from.min(u.len())..to.min(u.len())].to_vec()
    }

    // Text built from the pieces that matter to the cutter: words, line
    // breaks, blank lines, long unbroken runs and astral characters.
    fn text(max_pieces: usize) -> impl Strategy<Value = String> {
        proptest::collection::vec(
            prop_oneof![
                proptest::sample::select(vec!["\n\n", "\n", " ", "😀", "ё"]).prop_map(str::to_owned),
                "[ -~]{1,12}",
                (20usize..=120).prop_map(|n| "x".repeat(n)),
            ],
            1..=max_pieces,
        )
        .prop_map(|ps| ps.concat())
    }

    fn runs() -> u32 {
        std::env::var("MODEL_CASES").ok().and_then(|v| v.parse().ok()).unwrap_or(500)
    }

    proptest! {
        #![proptest_config(ProptestConfig { max_global_rejects: 100_000, ..ProptestConfig::with_cases(runs()) })]

        #[test]
        fn loses_nothing_keeps_order_and_never_exceeds_the_limit(text in text(60), limit in 2usize..=80) {
            let parts = split_telegram_text(&text, limit);
            prop_assert_eq!(parts.concat(), text);
            for part in &parts {
                prop_assert!(units(part) > 0);
                prop_assert!(units(part) <= limit);
            }
        }

        // A Rust String can't hold half a surrogate pair at all; what is
        // checked is that no part ends where a pair would have been cut.
        #[test]
        fn never_splits_a_surrogate_pair(text in text(60), limit in 2usize..=80) {
            let mut offset = 0;
            for part in split_telegram_text(&text, limit) {
                offset += units(&part);
                let all: Vec<u16> = text.encode_utf16().collect();
                if offset < all.len() {
                    prop_assert!(!(0xdc00..=0xdfff).contains(&all[offset]), "a part ends inside a pair");
                }
            }
        }

        #[test]
        fn a_text_within_the_limit_is_one_part_unchanged(text in text(10)) {
            prop_assume!(units(&text) <= TELEGRAM_TEXT_LIMIT);
            prop_assert_eq!(split_telegram_text(&text, TELEGRAM_TEXT_LIMIT), vec![text]);
        }

        #[test]
        fn ends_a_part_at_the_last_blank_line_when_that_keeps_it_at_least_half_full(text in text(60), limit in 4usize..=80) {
            let parts = split_telegram_text(&text, limit);
            let mut offset = 0;
            for part in &parts[..parts.len().saturating_sub(1)] {
                let window = js_slice(&text, offset, offset + limit);
                let blank: Vec<u16> = "\n\n".encode_utf16().collect();
                if let Some(at) = last_index_of(&window, &blank)
                    && at + 2 >= limit.div_ceil(2)
                {
                    prop_assert_eq!(units(part), at + 2);
                }
                offset += units(part);
            }
        }

        // A separator ends a part only where the part stays at least half
        // full: a separator early in the window is not worth a sliver of a
        // message when there is one further on.
        #[test]
        fn never_ends_a_part_in_the_first_half_when_a_separator_sits_in_the_second(
            text in proptest::collection::vec(prop_oneof!["[ -~]{1,12}", proptest::sample::select(vec!["\n\n", "\n", " "]).prop_map(str::to_owned)], 0..=40).prop_map(|cs| cs.concat()),
            limit in 4usize..=40,
        ) {
            let all: Vec<u16> = text.encode_utf16().collect();
            let mut rest = &all[..];
            for part in split_telegram_text(&text, limit) {
                let window = &rest[..limit.min(rest.len())];
                let late = SEPARATORS.iter().any(|sep| {
                    let sep: Vec<u16> = sep.encode_utf16().collect();
                    last_index_of(window, &sep).is_some_and(|at| at + sep.len() >= limit.div_ceil(2))
                });
                if rest.len() > limit && late {
                    prop_assert!(units(&part) >= limit.div_ceil(2));
                }
                rest = &rest[units(&part)..];
            }
        }
    }

    #[test]
    fn cuts_a_digest_between_blocks_not_inside_one() {
        let block = |n: usize| format!("Block {n}\n• {}\n", "headline ".repeat(300));
        let text = [1, 2, 3].map(block).join("\n");
        let parts = split_telegram_text(&text, TELEGRAM_TEXT_LIMIT);
        assert!(parts.len() > 1);
        assert_eq!(parts.concat(), text);
        for part in &parts[1..] {
            assert!(part.starts_with("Block "), "{part:.20}");
        }
    }

    #[test]
    fn hard_cuts_a_run_with_no_separator_at_the_limit() {
        let parts = split_telegram_text(&"x".repeat(10_000), TELEGRAM_TEXT_LIMIT);
        assert_eq!(parts.iter().map(|p| p.len()).collect::<Vec<_>>(), [4096, 4096, 1808]);
    }

    fn sent(id: i64) -> SentMessage {
        SentMessage { message_id: id, date: 1, chat: SentChat { id: 42 } }
    }

    // The TS fake numbers deliveries 101, 102, … and can fail on the n-th.
    async fn send_with(
        text: &str,
        fail_on: Option<usize>,
        texts: &Mutex<Vec<String>>,
    ) -> Result<(Vec<String>, Vec<SentMessage>), SendError> {
        send_parts(text, |part| async move {
            let mut t = texts.lock().unwrap();
            if Some(t.len()) == fail_on {
                return Err(TelegramError::Api {
                    method: "sendMessage",
                    status: 429,
                    description: "Too Many Requests".into(),
                });
            }
            t.push(part);
            Ok(sent(100 + t.len() as i64))
        })
        .await
    }

    #[tokio::test]
    async fn sends_every_part_in_order() {
        let texts = Mutex::new(Vec::new());
        let text = format!("{}\n\n{}", "a ".repeat(1500), "b ".repeat(1500));
        let (_, out) = send_with(&text, None, &texts).await.unwrap();
        assert_eq!(texts.lock().unwrap().concat(), text);
        assert_eq!(out.iter().map(|m| m.message_id).collect::<Vec<_>>(), [101, 102]);
    }

    #[tokio::test]
    async fn a_short_text_is_a_single_send() {
        let texts = Mutex::new(Vec::new());
        send_with("hi", None, &texts).await.unwrap();
        assert_eq!(*texts.lock().unwrap(), ["hi"]);
    }

    #[tokio::test]
    async fn a_failure_partway_names_what_was_already_delivered() {
        let texts = Mutex::new(Vec::new());
        let text = format!("{}\n\n{}", "a ".repeat(1500), "b ".repeat(1500));
        let Err(SendError::Partial(err)) = send_with(&text, Some(1), &texts).await else { panic!("a partial send") };
        assert_eq!(err.parts.len(), 2);
        assert_eq!(err.delivered.iter().map(|m| m.message_id).collect::<Vec<_>>(), [101]);
        assert!(err.to_string().contains("after 1 of 2 parts (delivered message ids: 101)"), "{err}");
    }

    #[tokio::test]
    async fn a_failure_on_the_first_part_is_the_original_error() {
        let texts = Mutex::new(Vec::new());
        let Err(SendError::Telegram(err)) = send_with(&"a".repeat(5000), Some(0), &texts).await else {
            panic!("the original error")
        };
        assert!(err.to_string().contains("Too Many Requests"));
    }
}

#[cfg(test)]
mod button_tests {
    use super::*;
    #[test]
    fn callback_data_limits_count_bytes_and_labels_are_required() {
        for (text, data, valid) in [("Yes", "a".repeat(64), true), ("Yes", "a".repeat(65), false),
            ("Yes", "🧾".repeat(16), true), ("Yes", "🧾".repeat(17), false), ("", "x".into(), false), ("Yes", "".into(), false)] {
            assert_eq!(check_buttons(Some(&vec![vec![Button { text: text.into(), data }]])).is_ok(), valid);
        }
    }
}
