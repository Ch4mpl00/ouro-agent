// A stand-in for api.telegram.org on a loopback port, for the real Bot API
// client pointed at it. A port of packages/mcp/test/support/fake-telegram.ts.
//
// Behaves like the Bot API where the code under test cares:
//   - getUpdates confirms every update below `offset` (they are gone for
//     good) and returns the rest, oldest first, at most `batch` at a time;
//   - sendMessage refuses empty / whitespace-only text and text over 4096
//     UTF-16 units, and numbers messages per chat;
//   - editMessageText needs a message this bot sent in that chat and refuses
//     an edit that changes nothing ("message is not modified");
//   - deleteMessage removes it (a second delete: "message to delete not
//     found").
// `fail(method, times, after)` lets `after` more calls of a method succeed,
// then fails the following `times` with a 500.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde_json::{Value, json};

const LIMIT: usize = 4096;

#[derive(Debug, Clone, PartialEq)]
pub struct Delivered {
    pub chat_id: i64,
    pub message_id: i64,
    pub thread_id: Option<i64>,
    pub text: String,
}

#[derive(Default)]
pub struct FakeState {
    // Updates Telegram still holds (not yet confirmed by an offset).
    pub pending: Vec<Value>,
    // Every message the bot has in a chat, in the order it was sent, with
    // its current text (edits applied).
    pub chat: Vec<Delivered>,
    // sendMessage calls that succeeded, in order.
    pub sent: Vec<Delivered>,
    // `offset` of every getUpdates call, in order.
    pub offsets: Vec<Option<i64>>,
    // sendChatAction calls, in order: chat, action, thread.
    pub actions: Vec<(i64, String, Option<i64>)>,
    pub batch: usize,
    pub requests: Vec<(String, Value)>,
    failures: HashMap<String, (usize, usize)>,
    next_message_id: i64,
}

#[derive(Clone)]
pub struct FakeTelegram {
    pub token: String,
    pub root: String,
    pub state: Arc<Mutex<FakeState>>,
}

impl FakeTelegram {
    pub fn lock(&self) -> std::sync::MutexGuard<'_, FakeState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn push(&self, update: Value) {
        self.lock().pending.push(update);
    }

    pub fn fail(&self, method: &str, times: usize, after: usize) {
        self.lock().failures.insert(method.to_owned(), (after, times));
    }

    // Back to an empty Telegram, as a fresh install of the TS fake.
    pub fn reset(&self) {
        let mut s = self.lock();
        *s = FakeState { batch: 100, next_message_id: 1000, ..FakeState::default() };
    }
}

fn ok(result: Value) -> (StatusCode, Json<Value>) {
    (StatusCode::OK, Json(json!({ "ok": true, "result": result })))
}

fn refuse(code: u16, description: &str) -> (StatusCode, Json<Value>) {
    (
        StatusCode::from_u16(code).unwrap_or(StatusCode::BAD_REQUEST),
        Json(json!({ "ok": false, "error_code": code, "description": description })),
    )
}

fn number(v: &Value) -> Option<i64> {
    v.as_i64().or_else(|| v.as_str().and_then(|s| s.trim().parse().ok()))
}

async fn handle(
    State((token, state)): State<(String, Arc<Mutex<FakeState>>)>,
    Path((bot, method)): Path<(String, String)>,
    body: Option<Json<Value>>,
) -> impl IntoResponse {
    if bot != format!("bot{token}") {
        return refuse(404, "Not Found");
    }
    let body = body.map(|Json(v)| v).unwrap_or_else(|| json!({}));
    let mut s = state.lock().unwrap_or_else(|e| e.into_inner());
    s.requests.push((method.clone(), body.clone()));
    if let Some((after, times)) = s.failures.get_mut(&method) {
        if *after > 0 {
            *after -= 1;
        } else if *times > 0 {
            *times -= 1;
            return refuse(500, "Internal Server Error: injected");
        }
    }
    match method.as_str() {
        "getUpdates" => {
            let offset = body["offset"].as_i64();
            s.offsets.push(offset);
            if let Some(offset) = offset {
                s.pending.retain(|u| u["update_id"].as_i64().unwrap_or(0) >= offset);
            }
            let batch = s.batch;
            ok(Value::Array(s.pending.iter().take(batch).cloned().collect()))
        }
        "sendMessage" => {
            let text = body["text"].as_str().unwrap_or_default().to_owned();
            if text.trim().is_empty() {
                return refuse(400, "Bad Request: message text is empty");
            }
            if text.encode_utf16().count() > LIMIT {
                return refuse(400, "Bad Request: message is too long");
            }
            let Some(chat_id) = number(&body["chat_id"]) else { return refuse(400, "Bad Request: chat not found") };
            let thread_id = body["message_thread_id"].as_i64();
            let message_id = s.next_message_id;
            s.next_message_id += 1;
            let message = Delivered { chat_id, message_id, thread_id, text };
            s.chat.push(message.clone());
            s.sent.push(message);
            ok(json!({ "message_id": message_id, "date": 1_790_000_000, "chat": { "id": chat_id, "type": "private" } }))
        }
        "editMessageText" => {
            let (chat_id, message_id) = (number(&body["chat_id"]), number(&body["message_id"]));
            let text = body["text"].as_str().unwrap_or_default().to_owned();
            let Some(message) =
                s.chat.iter_mut().find(|m| Some(m.chat_id) == chat_id && Some(m.message_id) == message_id)
            else {
                return refuse(400, "Bad Request: message to edit not found");
            };
            if text.trim().is_empty() {
                return refuse(400, "Bad Request: message text is empty");
            }
            if text.encode_utf16().count() > LIMIT {
                return refuse(400, "Bad Request: MESSAGE_TOO_LONG");
            }
            if text == message.text {
                return refuse(
                    400,
                    "Bad Request: message is not modified: specified new message content and reply markup are exactly the same",
                );
            }
            message.text = text;
            ok(
                json!({ "message_id": message.message_id, "date": 1_790_000_100, "chat": { "id": message.chat_id, "type": "private" } }),
            )
        }
        "deleteMessage" => {
            let (chat_id, message_id) = (number(&body["chat_id"]), number(&body["message_id"]));
            let before = s.chat.len();
            s.chat.retain(|m| !(Some(m.chat_id) == chat_id && Some(m.message_id) == message_id));
            if s.chat.len() == before {
                return refuse(400, "Bad Request: message to delete not found");
            }
            ok(json!(true))
        }
        "answerCallbackQuery" => ok(json!(true)),
        "sendChatAction" => {
            let action = (
                number(&body["chat_id"]).unwrap_or_default(),
                body["action"].as_str().unwrap_or_default().to_owned(),
                body["message_thread_id"].as_i64(),
            );
            s.actions.push(action);
            ok(json!(true))
        }
        _ => refuse(404, &format!("Not Found: method {method}")),
    }
}

pub async fn start(token: &str) -> FakeTelegram {
    let state = Arc::new(Mutex::new(FakeState { batch: 100, next_message_id: 1000, ..FakeState::default() }));
    let app = axum::Router::new()
        .route("/{bot}/{method}", axum::routing::post(handle))
        .with_state((token.to_owned(), state.clone()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    FakeTelegram { token: token.to_owned(), root: format!("http://127.0.0.1:{port}"), state }
}
