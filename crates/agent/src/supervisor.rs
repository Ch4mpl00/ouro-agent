// Supervisor domain: 1. signal / Telegram and peer context; 2. the signal as
// an engine run; 3. serial queue loop. Composition and OS signals live in main.rs.
use crate::{
    activity::ACTIVITY_STREAM,
    agent::{Engine, EngineDeps},
    errors::{aborted, check},
    mcp::{Mcp, SharedMcp},
    session::{Format, Session},
    skills::{FindSkillsParams, append_patch, find_skills, read_skill_index},
    tool_results::is_tool_error,
    tracing::{ContextUpdate, Scope, SpanEnd, SpanKind, SpanStart, TraceStart},
};
use ouro_runtime::ows::run::Status;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

// The pause after a failed round (MCP down, a malformed reply) before the
// next one, so an outage isn't hammered.
pub const ERROR_BACKOFF: Duration = Duration::from_secs(2);
// One `wait_for_signal` round: MCP answers as soon as a signal is queued, or
// after this with nothing pending. Under the 60 s request deadline.
pub const SIGNAL_WAIT: Duration = Duration::from_secs(25);
pub const TELEGRAM_HISTORY_KEY: &str = "telegram.history";
pub const TELEGRAM_HISTORY_LIMIT: usize = 20;
pub const TELEGRAM_HISTORY_MAX_BYTES: usize = 8000;
pub const PEER_SOURCE: &str = "peer";
pub const RECENT_ACTIVITY_LIMIT: usize = 10;
pub const RECENT_ACTIVITY_MAX_BYTES: usize = 8000;
// Skill preselection: a stricter bar than find_skills' default 0.5, since
// every inlined skill costs its size on every turn.
pub const PRESELECT_THRESHOLD: f64 = 0.7;
pub const PRESELECT_LIMIT: usize = 3;
const PRESELECT_REQUEST_CHARS: usize = 4000;
const PRESELECT_CONTEXT_CHARS: usize = 6000;
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PendingSignal {
    pub id: i64,
    pub source: String,
    pub content: String,
    #[serde(rename = "envContext", deserialize_with = "required_nullable")]
    pub env_context: Option<String>,
    pub created_at: String,
    // Structured data from the scheduled task that fired it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
}
impl PendingSignal {
    // The saved workflow a scheduled task runs, `payload.workflow`.
    pub fn saved_workflow(&self) -> Option<String> {
        if self.source != "scheduler" {
            return None;
        }
        self.payload.as_ref()?.get("workflow")?.as_str().filter(|s| !s.is_empty()).map(str::to_owned)
    }
}
#[derive(Deserialize)]
pub struct NextSignal {
    #[serde(deserialize_with = "required_nullable")]
    pub signal: Option<PendingSignal>,
    #[serde(rename = "pendingAfter")]
    pub pending_after: i64,
}
// Nullable MCP fields are required: serde's default for Option otherwise
// accepts absent fields which the TypeScript Zod contract rejects.
fn required_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}
pub fn timestamp(value: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(value).map(|t| t.timestamp_millis()).ok().or_else(|| {
        chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S").ok().map(|t| t.and_utc().timestamp_millis())
    })
}
static TELEGRAM_HEADER: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?s)^Telegram message in chat (-?\d+)(?: \(forum topic thread_id=(\d+)\))?\.\nText: (.+)$").unwrap()
});
pub fn inline_history(history: &[Value]) -> (Vec<Value>, bool) {
    let mut messages = Vec::new();
    for message in history.iter().rev() {
        let context = json!({"role":message["role"],"text":message["text"],"created_at":message["created_at"]});
        let mut trial = vec![context.clone()];
        trial.extend(messages.clone());
        if json!(trial).to_string().len() > TELEGRAM_HISTORY_MAX_BYTES {
            if messages.is_empty() {
                let mut clipped = context;
                clipped["text"] = json!("…");
                clipped["truncated"] = true.into();
                let mut remaining =
                    TELEGRAM_HISTORY_MAX_BYTES.saturating_sub(json!([clipped.clone()]).to_string().len());
                let text = message["text"].as_str().unwrap_or_default();
                let mut tail = Vec::new();
                for c in text.chars().rev() {
                    let bytes = json!(c.to_string()).to_string().len() - 2;
                    if bytes > remaining {
                        break;
                    }
                    remaining -= bytes;
                    tail.push(c);
                }
                clipped["text"] = json!(format!("…{}", tail.into_iter().rev().collect::<String>()));
                messages.push(clipped);
            }
            return (messages, true);
        }
        messages.insert(0, context);
    }
    (messages, false)
}
fn unavailable(signal: &PendingSignal) -> String {
    format!(
        "Recent Telegram history is unavailable. Use the current request; fetch context only if needed.\n\n## Current Telegram signal\n{}",
        signal.content
    )
}
#[derive(serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct TelegramButton {
    chat_id: i64,
    message_thread_id: Option<i64>,
    message_id: i64,
    data: String,
    text: Option<String>,
}
fn button(signal: &PendingSignal) -> Option<TelegramButton> {
    (signal.source == "telegram_button").then(|| serde_json::from_str(&signal.content).ok()).flatten()
}
pub fn conversation(signal: &PendingSignal) -> Option<crate::agent::ConfirmTarget> {
    if let Some(b) = button(signal) {
        return Some(crate::agent::ConfirmTarget {
            chat_id: b.chat_id.to_string(),
            message_thread_id: b.message_thread_id,
        });
    }
    if signal.source != "telegram" {
        return None;
    }
    let h = TELEGRAM_HEADER.captures(&signal.content)?;
    Some(crate::agent::ConfirmTarget {
        chat_id: h[1].to_owned(),
        message_thread_id: h.get(2).and_then(|m| m.as_str().parse().ok()),
    })
}

pub async fn prepare_telegram_input(
    signal: &PendingSignal,
    context: &Session,
    mcp: &dyn Mcp,
    trace: &Scope,
    cancel: &CancellationToken,
) -> anyhow::Result<String> {
    check(cancel)?;
    if signal.source != "telegram" {
        return Ok(signal.content.clone());
    }
    let Some(header) = TELEGRAM_HEADER.captures(&signal.content) else {
        return Ok(unavailable(signal));
    };
    let chat_id = &header[1];
    let thread_id = header.get(2).map(|s| s.as_str().parse::<i64>()).transpose()?;
    let mut args = json!({"chatId":chat_id,"limit":TELEGRAM_HISTORY_LIMIT}).as_object().unwrap().clone();
    if let Some(thread) = thread_id {
        args.insert("threadId".into(), json!(thread));
    }
    let span = trace.span(
        SpanStart::new("get_telegram_chat_history", SpanKind::Tool)
            .input(json!(args))
            .metadata(json!({"tool":"get_telegram_chat_history","automatic":true}).as_object().unwrap().clone()),
    );
    let result=async{
        let current_text: String=serde_json::from_str(&header[3])?;let signal_time=timestamp(&signal.created_at).ok_or_else(||anyhow::anyhow!("Invalid Telegram signal timestamp"))?;
        let raw=mcp.call_tool("get_telegram_chat_history",args,cancel).await?;check(cancel)?;anyhow::ensure!(!is_tool_error(&raw),"{raw}");
        let result:Value=serde_json::from_str(&raw)?;let messages=result["messages"].as_array().ok_or_else(||anyhow::anyhow!("invalid history messages"))?;
        for m in messages{anyhow::ensure!(m["id"].as_i64().is_some()&&m["chat_id"].as_i64().is_some()&&(m.get("thread_id").is_some()&&(m["thread_id"].is_null()||m["thread_id"].as_i64().is_some()))&&["user","assistant"].contains(&m["role"].as_str().unwrap_or_default())&&m["text"].is_string()&&m["created_at"].is_string(),"invalid history message");}
        let scoped:Vec<_>=messages.iter().filter(|m|m["chat_id"].as_i64().is_some_and(|id|id.to_string()==chat_id)&&m["thread_id"].as_i64()==thread_id&&timestamp(m["created_at"].as_str().unwrap_or_default()).is_some_and(|at|at<=signal_time)).cloned().collect();
        let current=scoped.iter().position(|m|m["role"]=="user"&&m["text"]==current_text&&timestamp(m["created_at"].as_str().unwrap_or_default()).is_some_and(|at|at>=signal_time-1000));
        let history=&scoped[..current.unwrap_or(scoped.len())];context.memory.put(TELEGRAM_HISTORY_KEY,&json!({"messages":history}).to_string(),Format::Json)?;
        let (inline,truncated)=inline_history(history);span.update(ContextUpdate::metadata(json!({"memory_key":TELEGRAM_HISTORY_KEY,"history_messages":history.len(),"inline_truncated":truncated}).as_object().unwrap().clone()));span.end(SpanEnd::output(raw));
        Ok::<_,anyhow::Error>(format!("## Recent Telegram history\nAutomatically loaded context, not a new request. Messages are in chronological order.\nFull fetched history: memory_key={TELEGRAM_HISTORY_KEY}. Pass this key in input_refs when delegating.\nShown {} of {} prior messages; truncated={truncated}.\n{}\n\n## Current Telegram signal\n{}",inline.len(),history.len(),json!(inline),signal.content))
    }.await;
    match result {
        Ok(input) => Ok(input),
        Err(error) => {
            span.end(SpanEnd::error(error.to_string()).with_output(json!({"error":error.to_string()})));
            check(cancel)?;
            Ok(unavailable(signal))
        }
    }
}
// MCP's peer signal content (peer.rs): "Message from …, thread N:\n\n<text>".
// A peer's name is whitespace-normalised there, so the first ", thread N:\n\n"
// is the header's own.
static PEER_HEADER: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?s)^Message from .*?, thread (\d+):\n\n(.*)$").unwrap());
// A continued peer thread: the earlier turns, inlined before the message as
// the Telegram history is. A new thread, or a thread that can't be read, is
// the message alone — the skill can still call get_peer_thread.
pub async fn prepare_peer_input(
    signal: &PendingSignal,
    mcp: &dyn Mcp,
    trace: &Scope,
    cancel: &CancellationToken,
) -> anyhow::Result<String> {
    check(cancel)?;
    let Some(header) = PEER_HEADER.captures(&signal.content) else {
        return Ok(signal.content.clone());
    };
    let args = json!({"threadId": header[1].parse::<i64>()?}).as_object().unwrap().clone();
    let span = trace.span(
        SpanStart::new("get_peer_thread", SpanKind::Tool)
            .input(json!(args))
            .metadata(json!({"tool":"get_peer_thread","automatic":true}).as_object().unwrap().clone()),
    );
    let result = async {
        let raw = mcp.call_tool("get_peer_thread", args, cancel).await?;
        check(cancel)?;
        anyhow::ensure!(!is_tool_error(&raw), "{raw}");
        let value: Value = serde_json::from_str(&raw)?;
        let messages = value["messages"].as_array().ok_or_else(|| anyhow::anyhow!("invalid peer thread"))?;
        // The message this signal carries was written in the signal's own
        // transaction, so it has the signal's timestamp; later ones are queued.
        let current = messages.iter().position(|m| {
            m["direction"] == "in" && m["text"] == header[2] && m["created_at"] == signal.created_at.as_str()
        });
        let earlier: Vec<Value> = messages[..current.unwrap_or(messages.len())]
            .iter()
            .map(|m| {
                let role = if m["direction"] == "out" { "ouro" } else { "peer" };
                json!({"role":role,"text":m["text"],"created_at":m["created_at"]})
            })
            .collect();
        span.update(ContextUpdate::metadata(json!({"history_messages":earlier.len()}).as_object().unwrap().clone()));
        span.end(SpanEnd::output(raw.clone()));
        if earlier.is_empty() {
            return Ok(signal.content.clone());
        }
        let (inline, truncated) = inline_history(&earlier);
        Ok::<_, anyhow::Error>(format!(
            "## Earlier in this peer thread\nAutomatically loaded context, not a new request. Oldest first; role \"ouro\" is you.\nShown {} of {} prior messages; truncated={truncated}.\n{}\n\n## Current peer signal\n{}",
            inline.len(),
            earlier.len(),
            json!(inline),
            signal.content
        ))
    }
    .await;
    match result {
        Ok(input) => Ok(input),
        Err(error) => {
            span.end(SpanEnd::error(error.to_string()).with_output(json!({"error":error.to_string()})));
            check(cancel)?;
            Ok(signal.content.clone())
        }
    }
}
pub async fn load_recent_activity(
    mcp: &dyn Mcp,
    trace: &Scope,
    cancel: &CancellationToken,
) -> anyhow::Result<Option<String>> {
    check(cancel)?;
    let args = json!({"stream":ACTIVITY_STREAM,"n":RECENT_ACTIVITY_LIMIT}).as_object().unwrap().clone();
    let span = trace.span(
        SpanStart::new("journal_tail", SpanKind::Tool)
            .input(json!(args))
            .metadata(json!({"tool":"journal_tail","automatic":true}).as_object().unwrap().clone()),
    );
    let result=async{
        let raw=mcp.call_tool("journal_tail",args,cancel).await?;check(cancel)?;anyhow::ensure!(!is_tool_error(&raw),"{raw}");let value:Value=serde_json::from_str(&raw)?;let entries=value["entries"].as_array().ok_or_else(||anyhow::anyhow!("invalid activity entries"))?;
        let mut kept=Vec::new();let mut bytes=0;for entry in entries{let body=entry["body"].as_str().ok_or_else(||anyhow::anyhow!("invalid activity body"))?;bytes+=body.len()+2;if bytes>RECENT_ACTIVITY_MAX_BYTES{break;}kept.push(body.to_owned());}
        span.update(ContextUpdate::metadata(json!({"activity_notes":kept.len()}).as_object().unwrap().clone()));span.end(SpanEnd::output(raw));if kept.is_empty(){return Ok(None);}
        Ok::<_,anyhow::Error>(Some(format!("## Recent Ouro activity\nThe last {} notes of your activity journal (stream \"{ACTIVITY_STREAM}\"), oldest first: what you did in recent runs across all chats, schedules and sources.\nBackground, not a request. Each note starts with its time (UTC) and source.\nFor older activity, call journal_tail with stream \"{ACTIVITY_STREAM}\" and a larger n (up to 100); each entry's metadata.trace_id identifies the run.\n\n{}",kept.len(),kept.into_iter().rev().collect::<Vec<_>>().join("\n\n"))))
    }.await;
    match result {
        Ok(notes) => Ok(notes),
        Err(error) => {
            span.end(SpanEnd::error(error.to_string()).with_output(json!({"error":error.to_string()})));
            check(cancel)?;
            Ok(None)
        }
    }
}

// The skills a signal needs, chosen before the first turn by the same Jev
// scoring as find_skills (with a stricter bar) and inlined whole, so the
// model starts with its channel's rules instead of spending two turns on
// find_skills + read_skill. Skills already loaded are skipped. Without a
// decision client, or on any failure, nothing is inlined and the model can
// still search itself.
pub async fn preselect_skills(
    deps: &EngineDeps,
    request: &str,
    context: &str,
    loaded: &[String],
    trace: &Scope,
    cancel: &CancellationToken,
) -> anyhow::Result<Option<(Vec<String>, String)>> {
    check(cancel)?;
    let Some(client) = deps.decisions.as_deref() else { return Ok(None) };
    let (store, mcp) = (deps.skill_store.as_ref(), deps.mcp.as_ref());
    let params = FindSkillsParams {
        query: request.chars().take(PRESELECT_REQUEST_CHARS).collect(),
        context: Some(tail_chars(context, PRESELECT_CONTEXT_CHARS)),
        threshold: Some(PRESELECT_THRESHOLD),
        limit: Some(PRESELECT_LIMIT),
    };
    let span = trace.span(
        SpanStart::new("find_skills", SpanKind::Tool)
            .input(json!(params))
            .metadata(json!({"tool":"find_skills","automatic":true}).as_object().unwrap().clone()),
    );
    let result = async {
        let index = read_skill_index(mcp, cancel).await?;
        let scope: Scope = span.clone();
        let found = find_skills(store, &index, client, &params, &scope, cancel).await?;
        check(cancel)?;
        let mut names = Vec::new();
        let mut sections = Vec::new();
        for skill in found["skills"].as_array().into_iter().flatten() {
            let Some(name) = skill["name"].as_str().filter(|n| !loaded.iter().any(|l| l == n)) else { continue };
            let Some(file) = store.read_skill(name).await? else { continue };
            let body = match store.read_patch(name).await? {
                Some(patch) => append_patch(&file.body, &patch),
                None => file.body,
            };
            sections.push(format!("### Skill `{name}`\n\n{}", body.trim()));
            names.push(name.to_owned());
        }
        span.update(ContextUpdate::metadata(json!({"preselected":names}).as_object().unwrap().clone()));
        span.end(SpanEnd::output(found));
        if names.is_empty() {
            return Ok(None);
        }
        Ok::<_, anyhow::Error>(Some((names, format!(
            "## Skills selected for this request\nChosen automatically by relevance before your first turn and already loaded: follow the ones that apply, never read_skill them again. A skill that needs tools you lack is for a worker: delegate to it by name.\n\n{}",
            sections.join("\n\n")
        ))))
    }
    .await;
    match result {
        Ok(selected) => Ok(selected),
        Err(error) => {
            span.end(SpanEnd::error(error.to_string()).with_output(json!({"error":error.to_string()})));
            check(cancel)?;
            Ok(None)
        }
    }
}

fn tail_chars(text: &str, max: usize) -> String {
    let skip = text.chars().count().saturating_sub(max);
    text.chars().skip(skip).collect()
}

/// The incoming message with its source's history (`signal_input`), and the
/// user's earlier messages in that conversation, captured right after the
/// host preload, before any loop can change the working-memory copy: the
/// guard's evidence.
pub async fn signal_input(
    signal: &PendingSignal,
    session: &Session,
    mcp: &dyn Mcp,
    scope: &Scope,
    cancel: &CancellationToken,
) -> anyhow::Result<(String, Vec<String>)> {
    let input = if let Some(b) = button(signal) {
        let mut telegram = signal.clone();
        telegram.source = "telegram".into();
        let pressed = format!("[button] {}", b.text.as_deref().unwrap_or(&b.data));
        let thread = b.message_thread_id.map(|t| format!(" (forum topic thread_id={t})")).unwrap_or_default();
        telegram.content = format!("Telegram message in chat {}{thread}.\nText: {}", b.chat_id, json!(pressed));
        let history = prepare_telegram_input(&telegram, session, mcp, scope, cancel).await?;
        format!(
            "{history}\n\nThe user pressed the button {} with data {} under message {} (chatId={}, messageThreadId={:?}).",
            json!(b.text),
            json!(b.data),
            b.message_id,
            b.chat_id,
            b.message_thread_id
        )
    } else if signal.source == PEER_SOURCE {
        // Peer history currently survives only as rendered text.
        prepare_peer_input(signal, mcp, scope, cancel).await?
    } else {
        prepare_telegram_input(signal, session, mcp, scope, cancel).await?
    };
    let mut earlier = Vec::new();
    if matches!(signal.source.as_str(), "telegram" | "telegram_button") {
        let history = session
            .memory
            .get(TELEGRAM_HISTORY_KEY)
            .ok()
            .and_then(|s| serde_json::from_str::<Value>(&s).ok())
            .unwrap_or(Value::Null);
        earlier = history["messages"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|m| m["role"] == "user")
            .filter_map(|m| m["text"].as_str().map(str::to_owned))
            .collect();
    }
    Ok((input, earlier))
}

/// `pending_confirmations`' answer: the cards waiting in a conversation as
/// information, or null.
pub fn pending_text(pending: Value) -> Value {
    if pending.as_array().is_some_and(|p| !p.is_empty()) {
        json!(format!(
            "Pending confirmations in this conversation (information only; the cards are waiting for the user):\n{pending}"
        ))
    } else {
        Value::Null
    }
}

/// A signal as workflows see it (`$context.signal`): the dispatcher's input,
/// and a signal's admitted run's.
pub fn signal_value(signal: &PendingSignal) -> Value {
    json!({
        "id": signal.id, "source": signal.source, "content": signal.content,
        "env_context": signal.env_context, "workflow": signal.saved_workflow(), "button": button(signal),
        "payload": signal.payload, "created_at": signal.created_at,
    })
}

/// The signal a run's admitted input carries, back.
pub fn admitted_signal(value: &Value) -> Option<PendingSignal> {
    Some(PendingSignal {
        id: value["id"].as_i64()?,
        source: value["source"].as_str()?.to_owned(),
        content: value["content"].as_str()?.to_owned(),
        env_context: value["env_context"].as_str().map(str::to_owned),
        created_at: value["created_at"].as_str()?.to_owned(),
        payload: value.get("payload").filter(|p| !p.is_null()).cloned(),
    })
}

// ── 2. the signal as an engine run ──────────────────────────────────────
// How a signal is handled is the workflow `signal` (`workflows/signal.yaml`),
// a durable engine run `signal:<id>` admitted with the signal as host data
// (design 03): its functions (functions.rs §7) act on the one signal it
// serves and trace under that signal's trace. The supervisor starts signals
// one at a time and waits until the signal's run tree is quiescent (decision
// 1): it ended, or every live run waits on an event or a timer; a suspended
// tree goes on in the engine worker when its event arrives (a button press
// a later signal delivers).

pub struct Supervisor {
    pub engine: Arc<Engine>,
    cancel: CancellationToken,
    active: Arc<tokio::sync::Semaphore>,
    shutdown: tokio::sync::Mutex<bool>,
}
pub fn create_supervisor(engine: Arc<Engine>) -> Arc<Supervisor> {
    create_supervisor_with_cancel(engine, CancellationToken::new())
}
pub fn create_supervisor_with_cancel(engine: Arc<Engine>, cancel: CancellationToken) -> Arc<Supervisor> {
    Arc::new(Supervisor {
        engine,
        cancel,
        active: Arc::new(tokio::sync::Semaphore::new(1)),
        shutdown: Default::default(),
    })
}
impl Supervisor {
    pub async fn shutdown(&self) {
        self.cancel.cancel();
        let mut done = self.shutdown.lock().await;
        if *done {
            return;
        }
        let _joined = self.active.acquire().await;
        self.engine.shutdown().await;
        *done = true;
    }
    pub async fn run_signal(self: &Arc<Self>, signal: &PendingSignal) -> anyhow::Result<String> {
        self.start(signal).await.unwrap_or_else(|| Err(aborted()))
    }
    // Runs `signal` unless the supervisor is stopping, which comes back as
    // None: nothing of the signal ran, so a scheduled fire can give its slot back.
    pub async fn start(self: &Arc<Self>, signal: &PendingSignal) -> Option<anyhow::Result<String>> {
        if self.cancel.is_cancelled() {
            return None;
        }
        let _active = self.active.acquire().await.ok()?;
        if self.cancel.is_cancelled() {
            return None;
        }
        Some(self.run_active(signal).await)
    }
    // Starts the run `signal:<id>` under the signal's trace and waits until
    // its tree (and its recovery report's) is quiescent. Its output is the
    // signal's when it ended; a suspended tree answers that it resumes.
    async fn run_active(self: &Arc<Self>, signal: &PendingSignal) -> anyhow::Result<String> {
        let engine = &self.engine;
        engine.ready().await.map_err(|e| e.context("booting the engine"))?;
        let run = crate::functions::signal_run(signal.id);
        let id = format!("{}:{}", signal.source, signal.id);
        let saved = signal.saved_workflow();
        let trace=engine.deps.tracer.trace(TraceStart{id:id.clone(),name:format!("signal:{}",signal.source),kind:SpanKind::Agent,session_id:Some(id.clone()),tags:Some(vec![signal.source.clone(),if saved.is_some(){"saved-workflow"}else{"agent-loop"}.into()]),metadata:Some(json!({"signal_id":signal.id,"signal_source":signal.source,"signal_created_at":signal.created_at,"activity":"run"}).as_object().unwrap().clone()),..Default::default()});
        trace.update(ContextUpdate::input(signal.content.clone()));
        let started = async {
            // The anchor and the evidence go in before the engine admits it;
            // a write may wait for the engine's lock on agent.db (runs.rs §2).
            let row = trace.anchor().map(|anchor| crate::runs::RunTrace {
                run_id: run.clone(),
                trace_id: anchor.trace_id,
                session_id: anchor.session_id,
                parent_span_id: Some(anchor.parent_span_id),
                recorded: false,
                created_ms: crate::runs::now_ms(),
            });
            let (evidence_run, content) = (run.clone(), signal.content.clone());
            let content = engine
                .deps
                .db
                .write(move |db| {
                    if let Some(row) = &row {
                        db.insert_run_trace(row)?;
                    }
                    db.put_evidence(&evidence_run, &content)
                })
                .await
                .map_err(|e| e.context("anchoring the signal run"))?;
            let host = crate::runs::SignalHost {
                kind: crate::runs::SIGNAL.into(),
                signal_id: signal.id,
                source: signal.source.clone(),
                created_ms: timestamp(&signal.created_at).unwrap_or_else(crate::runs::now_ms),
                evidence: crate::runs::SignalEvidence { content },
            };
            let scope: Scope = trace.clone();
            engine.scopes.borrow(&run, scope);
            engine
                .runtime
                .start_with(
                    &run,
                    SIGNAL_NAME,
                    json!({ "signal": signal_value(signal) }),
                    ouro_runtime::Start { version: None, host: serde_json::to_value(&host)? },
                )
                .await
                .map_err(|e| e.context("admitting the signal run"))?;
            let recovery = format!("{}:{}", crate::runs::RECOVERY, signal.id);
            crate::runs::wait_quiescent(engine, &[run.clone(), recovery], &self.cancel)
                .await
                .map_err(|e| e.context("waiting for the signal's tree"))
        }
        .await;
        if self.cancel.is_cancelled() {
            // Shutdown: the runs are released for recovery first, so their
            // calls in flight end (as aborted) before the trace does.
            engine.halt().await;
        }
        let ended = engine.runtime.get(&run).await.ok().flatten().and_then(|r| r.outcome);
        let outcome = match (started, &ended) {
            (Err(error), _) => Err(error),
            (Ok(()), Some(Status::Completed(Value::String(output)))) => Ok(output.clone()),
            (Ok(()), Some(Status::Completed(output))) => Ok(output.to_string()),
            (Ok(()), Some(Status::Cancelled)) => Err(aborted()),
            (Ok(()), Some(Status::Faulted(error))) => {
                Err(anyhow::anyhow!(error.detail.clone().unwrap_or_else(|| error.to_string())))
            }
            (Ok(()), _) => Ok(json!({"ok": true, "resumes": true}).to_string()),
        };
        match (&ended, &outcome) {
            (Some(status), _) => trace.update(crate::runs::final_output(status)),
            (None, Err(error)) => trace.update(ContextUpdate {
                output: Some(json!({"error":error.to_string()})),
                metadata: Some(json!({"error":true}).as_object().unwrap().clone()),
                ..Default::default()
            }),
            _ => {}
        }
        engine.scopes.release(&run);
        let recorded = ended.is_some().then(|| run.clone());
        engine
            .deps
            .db
            .write(move |db| {
                trace.end();
                recorded.map(|run| db.mark_recorded(&run)).transpose()
            })
            .await
            .map_err(|e| e.context("recording the signal run"))?;
        check(&self.cancel)?;
        outcome
    }
}

pub const SIGNAL: &str = include_str!("../workflows/signal.yaml");
pub const SIGNAL_NAME: &str = "signal";

// ── 3. polling ───────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct Waited {
    #[allow(dead_code)]
    pending: i64,
}

// One round: take a signal and run it, or — with an empty queue — wait on
// `wait_for_signal` until MCP has one queued (or the wait round ends).
async fn poll_once(mcp: &SharedMcp, supervisor: &Arc<Supervisor>, cancel: &CancellationToken) -> anyhow::Result<()> {
    let raw = mcp.call_tool("get_next_signal", serde_json::Map::new(), cancel).await?;
    let next: NextSignal = serde_json::from_str(&raw)?;
    check(cancel)?;
    if let Some(signal) = next.signal {
        eprintln!("[supervisor] signal #{} source={} ({} pending after)", signal.id, signal.source, next.pending_after);
        supervisor.run_signal(&signal).await?;
        return Ok(());
    }
    let mut args = serde_json::Map::new();
    args.insert("timeoutMs".into(), json!(SIGNAL_WAIT.as_millis() as u64));
    let raw = mcp.call_tool("wait_for_signal", args, cancel).await?;
    serde_json::from_str::<Waited>(&raw)
        .map_err(|e| anyhow::anyhow!("wait_for_signal: unexpected reply ({e}): {raw}"))?;
    Ok(())
}

// Takes signals one at a time. With an empty queue it waits for the next one
// through `wait_for_signal`, which MCP answers the moment a poller queues it,
// so a signal starts at once. The wait never takes a signal: a wait left
// behind by a dead supervisor can't lose one. A failed round backs off
// `backoff` before the next.
pub async fn run_poll_loop(
    mcp: SharedMcp,
    supervisor: Arc<Supervisor>,
    cancel: CancellationToken,
    backoff: Duration,
) -> anyhow::Result<()> {
    while !cancel.is_cancelled() {
        match poll_once(&mcp, &supervisor, &cancel).await {
            Ok(()) => continue,
            Err(error) if !cancel.is_cancelled() => eprintln!("[supervisor] loop error: {error}"),
            Err(_) => break,
        }
        tokio::select! {_=cancel.cancelled()=>break,_=tokio::time::sleep(backoff)=>{}}
    }
    Ok(())
}
