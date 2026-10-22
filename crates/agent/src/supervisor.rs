// Supervisor domain: 1. signal / Telegram and peer context; 2. routing and recovery;
// 2b. the signal workflow's functions; 3. serial queue loop. Composition and OS signals live in main.rs.
use crate::{
    activity::{ACTIVITY_STREAM, tail_utf16},
    agent::{AgentLoop, Engine, EngineDeps, LoopOptions},
    errors::{aborted, check, is_aborted},
    flows::{FlowResult, FlowRun, WorkflowSignal},
    mcp::{Mcp, SharedMcp},
    models::PresetName,
    session::{EnvDeps, Format, Session, gather_env_data, render_context, session},
    skills::{FindSkillsParams, append_patch, find_skills, read_skill_index},
    tool_results::is_tool_error,
    tracing::{ContextUpdate, EventStart, Level, Scope, Span, SpanEnd, SpanKind, SpanStart, Trace, TraceStart},
};
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
    fn workflow_signal(&self) -> WorkflowSignal {
        WorkflowSignal {
            id: self.id,
            source: self.source.clone(),
            content: self.content.clone(),
            env_context: self.env_context.clone(),
        }
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
fn conversation(signal: &PendingSignal) -> Option<crate::agent::ConfirmTarget> {
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

// ── 2. routing / recovery ────────────────────────────────────────────────
pub struct Supervisor {
    pub engine: Arc<Engine>,
    pub env: EnvDeps,
    pub flows: Arc<dyn FlowRun>,
    cancel: CancellationToken,
    active: Arc<tokio::sync::Semaphore>,
    shutdown: tokio::sync::Mutex<bool>,
}
pub fn create_supervisor(engine: Arc<Engine>, env: EnvDeps, flows: Arc<dyn FlowRun>) -> Arc<Supervisor> {
    create_supervisor_with_cancel(engine, env, flows, CancellationToken::new())
}
pub fn create_supervisor_with_cancel(
    engine: Arc<Engine>,
    env: EnvDeps,
    flows: Arc<dyn FlowRun>,
    cancel: CancellationToken,
) -> Arc<Supervisor> {
    Arc::new(Supervisor {
        engine,
        env,
        flows,
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
    // Runs the workflow `signal` for `signal` (§2b) under the signal's trace;
    // its catch handler decides whether to request a recovery report.
    async fn run_active(self: &Arc<Self>, signal: &PendingSignal) -> anyhow::Result<String> {
        let id = format!("{}:{}", signal.source, signal.id);
        let saved = signal.saved_workflow();
        let trace=self.engine.deps.tracer.trace(TraceStart{id:id.clone(),name:format!("signal:{}",signal.source),kind:SpanKind::Agent,session_id:Some(id.clone()),tags:Some(vec![signal.source.clone(),if saved.is_some(){"saved-workflow"}else{"agent-loop"}.into()]),metadata:Some(json!({"signal_id":signal.id,"signal_source":signal.source,"signal_created_at":signal.created_at,"activity":"run"}).as_object().unwrap().clone()),..Default::default()});
        trace.update(ContextUpdate::input(signal.content.clone()));
        let state = Arc::new(std::sync::Mutex::new(SignalState::default()));
        let outcome = self.handle(signal, &trace, &state).await;
        let SignalState { primary, span, .. } = std::mem::take(&mut *state.lock().unwrap_or_else(|e| e.into_inner()));
        if let Err(error) = &outcome {
            if let Some(span) = &span {
                span.end(SpanEnd::error(error.to_string()).with_output(json!({"error":error.to_string()})));
            }
            trace.update(ContextUpdate {
                output: Some(json!({"error":error.to_string()})),
                metadata: Some(json!({"error":true}).as_object().unwrap().clone()),
                ..Default::default()
            });
        }
        if primary.is_some() || span.is_some() {
            self.engine.end_loop(&id).await;
        }
        trace.end();
        check(&self.cancel)?;
        outcome
    }
    async fn handle(
        self: &Arc<Self>,
        signal: &PendingSignal,
        trace: &Arc<dyn Trace>,
        state: &Arc<std::sync::Mutex<SignalState>>,
    ) -> anyhow::Result<String> {
        let (definition, version) = crate::flows::definition(&self.engine, SIGNAL_NAME);
        let wf = owf::model::parse_yaml(&definition)?;
        let functions = SignalFunctions {
            routed: false,
            supervisor: self.clone(),
            signal: signal.clone(),
            trace: trace.clone(),
            state: state.clone(),
            errors: Default::default(),
        };
        let errors = functions.errors.clone();
        let mut host = owf::run::Host {
            functions: Arc::new(functions),
            observer: Arc::new(owf::run::Silent),
            clock: Arc::new(owf::run::SystemClock),
            limits: owf::run::Limits::default(),
            runtime: json!({"name":"agent-supervisor","version":env!("CARGO_PKG_VERSION")}),
        };
        let input = json!({ "signal": signal_value(signal) });
        let run_id = format!("{}:{}/signal", signal.source, signal.id);
        if let Some(sandbox) = self.engine.sandbox() {
            sandbox.watch(&mut host, &run_id, SIGNAL_NAME, version, None, None);
        }
        let outcome = owf::run::run(&host, &wf, &run_id, input, &self.cancel).await;
        let result = crate::flows::flow_result(outcome.status, outcome.effects_started);
        crate::flows::record_outcome(&self.engine, SIGNAL_NAME, version, &result);
        check(&self.cancel)?;
        match result {
            FlowResult::Completed(Value::String(output)) => Ok(output),
            FlowResult::Completed(output) => Ok(output.to_string()),
            FlowResult::Cancelled => Err(aborted()),
            // A function's own error, as it failed (an abort stays one).
            FlowResult::Faulted { error, .. } => Err(errors
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take()
                .unwrap_or_else(|| anyhow::anyhow!(error.detail.clone().unwrap_or_else(|| error.to_string())))),
            other => anyhow::bail!("the signal workflow did not end: {other:?}"),
        }
    }
}

// ── 2b. the signal workflow ──────────────────────────────────────────────
// How a signal is handled is the OWF workflow `signal` (`workflows/signal.yaml`, or
// the version the agent activates under that name, guarded by a breaker:
// flows.rs §4b): the routing, the context the first turn gets, the primary
// loop's skills and prompt, and the failure report. Its functions here act on the one signal its run
// serves and trace under that signal's trace, as the code before them did.
//
// One kind of workflow, one way to run it on a signal (`launch`): a
// scheduled task's workflow (`run_saved_workflow`), a route's (`route_signal`)
// and the default path's loop (`primary_loop`) are each a run admitted with
// the model's parameters (`admission`) and driven by `FlowRun::run_loop`.
// Any of them may take agent turns; a workflow with none never calls the
// model. What differs is the defaults (`Launch`) and what a failure means:
// a route's reaches the dispatcher's catch, a scheduled task's that changed
// nothing hands its task to the agent loop.

/// How a run the runtime starts on a signal begins when its definition
/// declares nothing (`flows::agent_start`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Launch {
    /// A scheduled task's workflow: the `base` preset and the source's own
    /// skill when it has one (`scheduler`), as the default path gives a
    /// scheduled signal.
    Scheduled,
    /// A route's: the `smart` preset; the workflow sets up its skills itself.
    Routed,
}

/// The model's parameters the host admits a run of workflow `name` on
/// `signal` with, pinned for the whole run (`agent_turn` and `tool` read
/// only these, never the YAML's): the preset and the confirmation target
/// its definition declares or `launch`'s defaults, the conversation the
/// signal came from, its skills, and instructions that say what the runtime
/// knows (the session, the source, its env addendum, a handover's note).
/// The rest of its first turn a workflow gathers and sets up itself
/// (agent.rs §4, `loop_setup`).
pub async fn admission(
    engine: &Engine,
    signal: &PendingSignal,
    session: &Session,
    name: &str,
    launch: Launch,
    handover: Option<&str>,
) -> anyhow::Result<LoopOptions> {
    let (definition, _) = crate::flows::run_definition(engine, name, false);
    let start = crate::flows::agent_start(&owf::model::parse_yaml(&definition)?)?;
    let mut opts = LoopOptions::new(format!("{}:{}", signal.source, signal.id), session.clone());
    opts.workflow = name.to_owned();
    opts.preset = start.preset.unwrap_or(match launch {
        Launch::Scheduled => PresetName::Base,
        Launch::Routed => PresetName::Smart,
    });
    opts.conversation = conversation(signal);
    opts.confirm = match start.confirm.unwrap_or(crate::flows::Confirm::Conversation) {
        crate::flows::Confirm::Conversation => conversation(signal).or_else(|| {
            let chat_id = session.env.telegram_chat_id.clone()?;
            Some(crate::agent::ConfirmTarget { chat_id, message_thread_id: None })
        }),
        crate::flows::Confirm::None => None,
    };
    if launch == Launch::Scheduled && engine.deps.skill_store.read_skill(&signal.source).await?.is_some() {
        opts.skills = vec![signal.source.clone()];
    }
    opts.guard_request = Some(signal.content.clone());
    let parts = [
        Some(render_context(&session.env)?),
        Some(format!("Signal source: {}", signal.source)),
        signal.env_context.clone(),
        handover.map(str::to_owned),
    ];
    opts.system_prompt = Some(parts.into_iter().flatten().filter(|p| !p.is_empty()).collect::<Vec<_>>().join("\n\n"));
    opts.signal = Some(signal_value(signal));
    Ok(opts)
}

/// A signal's run input: the admitted input of `agent`, which has not run
/// yet (`AgentLoop::workflow_input`, `text` its first user message), and
/// `env`, the user's clock and watermarks as scheduled workflows read them.
pub async fn run_input(agent: &AgentLoop, text: &str, signal: &PendingSignal, session: &Session) -> Value {
    let mut input = agent.workflow_input(text).await;
    if let Some(env) = crate::flows::signal_input(&signal.workflow_signal(), &session.env).remove("env") {
        input["env"] = env;
    }
    input
}

pub const SIGNAL: &str = include_str!("../workflows/signal.yaml");
pub const SIGNAL_NAME: &str = "signal";
/// The signal workflow's functions and their effects.
pub const SIGNAL_FUNCTIONS: &[crate::flows::HostFunction] = {
    use crate::flows::host_function;
    use owf::run::Effect::{Read, Write};
    &[
        host_function(
            "signal_session",
            Read,
            &[],
            &[],
            "Open the incoming signal's session. Returns context, engine_skills, conversation {chatId, messageThreadId} or null, and default_chat. The dispatcher already did this before entering a matching route.",
        ),
        host_function(
            "deliver_event",
            Write,
            &["type", "data"],
            &["type"],
            "Deliver an incoming event to waiting durable runs. Returns delivered and output. The dispatcher offers button events before opening the session or selecting a route.",
        ),
        host_function(
            "run_saved_workflow",
            Write,
            &["name"],
            &["name"],
            "Run the active saved workflow named by a scheduled signal, after signal_session, admitted as the signal's primary run (base preset and the scheduler skill unless metadata.agent says otherwise; it may take agent turns). Returns done and output, or handover for the primary loop when a missing, set-aside or failed workflow made no writes. The dispatcher does this before routing.",
        ),
        host_function(
            "signal_input",
            Read,
            &[],
            &[],
            "Prepare the incoming message with its source history. Returns the input string for primary_loop, or for loop_setup's input in a loop its route started; preloads Telegram history into working memory and captures conversation evidence for the guard.",
        ),
        host_function(
            "recent_activity",
            Read,
            &[],
            &[],
            "Read recent activity-journal notes as a context string, or null when unavailable. Pass it in primary_loop's or loop_setup's system array when useful.",
        ),
        host_function(
            "pending_confirmations",
            Read,
            &["target"],
            &["target"],
            "Read pending confirmation cards for target {chatId, messageThreadId}. Returns an informational string or null; it does not answer or consume a card.",
        ),
        host_function(
            "preselect_skills",
            Read,
            &["request", "context", "loaded"],
            &[],
            "Select relevant skills for this signal. request and context are strings, loaded is an array of already loaded skill names (a loop's own count already). Returns names and block, or null; pass block in primary_loop's or loop_setup's system array.",
        ),
        host_function(
            "primary_loop",
            Write,
            &["input", "loop", "confirm", "skills", "loaded", "preset", "allow_skill_writes", "system"],
            &["input"],
            "Run the signal's primary agent after signal_session: the dispatcher's default path. input is a string; loop selects a named variant (default agent-loop); confirm is {chatId, messageThreadId} or null; skills/loaded and system are string arrays. preset defaults to smart; allow_skill_writes defaults to false. Returns output. A loop that declares its own route needs none: the dispatcher starts it.",
        ),
        host_function(
            "route_signal",
            Write,
            &["context"],
            &["context"],
            "Dispatch the incoming signal to the most specific active metadata.route match by source and top-level payload fields. The matching workflow starts as the signal's primary run, admitted like a scheduled task's but with the smart preset (its failure reaches this workflow's catch); a signal route that calls primary_loop itself (superseded) runs with context as its input. Pass context: $context after session setup. Returns routed and output when matched, or routed:false. Dispatcher only: a matching route cannot dispatch routes again.",
        ),
        host_function(
            "signal_transcript",
            Read,
            &[],
            &[],
            "Read the bounded JSON transcript string of this signal's primary run for recovery. Returns [] as a string when none was started or it took no turn.",
        ),
        host_function(
            "recovery_loop",
            Write,
            &["error", "prompt", "skills", "preset", "max_iterations", "system"],
            &["prompt", "max_iterations"],
            "Run one failure-reporting agent after signal_session. prompt is a string, max_iterations a positive integer; skills and system are string arrays, preset defaults to base, error supplies detail for tracing. Returns output or error; skips cancellation and aborts. Keep the catch's final raise to preserve the original signal failure.",
        ),
    ]
};

// State shared by the workflow functions: the session, the primary loop
// (for its transcript) and its span (also closed on workflow failure).
#[derive(Default)]
struct SignalState {
    session: Option<Session>,
    primary: Option<Arc<AgentLoop>>,
    earlier_user_messages: Vec<String>,
    selected_skill_text: String,
    recovery_reported: bool,
    span: Option<Arc<dyn Span>>,
}

#[derive(Clone)]
struct SignalFunctions {
    routed: bool,
    supervisor: Arc<Supervisor>,
    signal: PendingSignal,
    trace: Arc<dyn Trace>,
    state: Arc<std::sync::Mutex<SignalState>>,
    // The error a function failed with, kept whole: a fault carries only
    // its text, and an abort must stay one.
    errors: Arc<std::sync::Mutex<Option<anyhow::Error>>>,
}

fn strings(args: &Value, key: &str) -> Vec<String> {
    args[key].as_array().into_iter().flatten().filter_map(Value::as_str).map(str::to_owned).collect()
}

impl SignalFunctions {
    fn state(&self) -> std::sync::MutexGuard<'_, SignalState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    // The signal's primary span (`agent_loop`), also closed on workflow failure.
    fn primary_span(&self) -> Arc<dyn Span> {
        let span = self.trace.span(
            SpanStart::new("agent_loop", SpanKind::Chain)
                .metadata(json!({"activity":"primary_loop"}).as_object().unwrap().clone()),
        );
        self.state().span = Some(span.clone());
        span
    }

    // The one way a workflow runs on this signal: run `run` admitted with
    // `opts`, its first user message `text`, driven to its next stop in the
    // signal's trace under `span`. Its loop is the signal's primary one: the
    // recovery report reads its transcript.
    async fn launch(
        &self,
        mut opts: LoopOptions,
        run: &str,
        text: &str,
        span: &Arc<dyn Span>,
    ) -> anyhow::Result<(Arc<AgentLoop>, FlowResult)> {
        let supervisor = &self.supervisor;
        opts.cancel = supervisor.cancel.clone();
        opts.trace_scope = Some(span.clone());
        let agent = supervisor.engine.start_loop(opts).await?;
        self.state().primary = Some(agent.clone());
        let input = run_input(&agent, text, &self.signal, &self.session()?).await;
        let scope: Scope = span.clone();
        Ok((agent, supervisor.flows.run_loop(run, input, &scope, &supervisor.cancel).await))
    }

    // The signal's primary run, `opts` with the signal's own parts added,
    // as `<signal>/loop`: its output is the signal's. A restart resumes it
    // instead of losing the signal. `loaded`: the skills the trace names,
    // else the loop's.
    async fn run_primary(
        &self,
        mut opts: LoopOptions,
        input: String,
        loaded: Option<Vec<String>>,
    ) -> anyhow::Result<String> {
        let (signal, trace) = (&self.signal, &self.trace);
        let primary = self.primary_span();
        opts.guard_request = Some(signal.content.clone());
        opts.conversation = conversation(signal);
        let run = format!("{}/loop", opts.id);
        let (agent, result) = self.launch(opts, &run, &input, &primary).await?;
        let output = match result {
            FlowResult::Completed(output) => output.as_str().map(str::to_owned).unwrap_or_else(|| output.to_string()),
            FlowResult::Cancelled => return Err(aborted()),
            FlowResult::Faulted { error, .. } => {
                anyhow::bail!(error.detail.clone().unwrap_or_else(|| error.to_string()))
            }
            // Released on shutdown or held elsewhere: the recovery worker resumes it.
            FlowResult::Waiting { .. } | FlowResult::Interrupted { .. } => {
                check(&self.supervisor.cancel)?;
                json!({"ok": true, "resumes": true}).to_string()
            }
            FlowResult::Missing => anyhow::bail!("run {run} has no workflow to run"),
        };
        primary.end(SpanEnd::output(output.clone()));
        let loaded = loaded.unwrap_or_else(|| agent.skill_names());
        trace.update(ContextUpdate {
            output: Some(json!(output)),
            metadata: Some(json!({"skills": loaded}).as_object().unwrap().clone()),
            ..Default::default()
        });
        Ok(output)
    }

    // A workflow that declares the signal's route runs as its primary run,
    // admitted with the route's defaults (`Launch::Routed`). The rest of its
    // first turn it gathers and sets up itself (agent.rs §4, `loop_setup`).
    // Its failures reach the dispatcher's catch, as a route's do.
    async fn route_launch(&self, name: &str, context: &Value) -> anyhow::Result<String> {
        let engine = &self.supervisor.engine;
        let handover = context["handover"].as_str();
        let opts = admission(engine, &self.signal, &self.session()?, name, Launch::Routed, handover).await?;
        self.run_primary(opts, self.signal.content.clone(), None).await
    }

    // A scheduled task's workflow, as the signal's primary run
    // `<signal>` (`Launch::Scheduled`); `Missing`, or a fault, when it
    // could not start. Its loop ends here: the agent loop may take the task
    // over under the same loop id.
    async fn scheduled_launch(&self, name: &str) -> anyhow::Result<FlowResult> {
        let (supervisor, signal) = (&self.supervisor, &self.signal);
        let engine = &supervisor.engine;
        if let Some(stop) = crate::flows::unrunnable(engine, name) {
            return Ok(stop);
        }
        let id = format!("{}:{}", signal.source, signal.id);
        let span = self.primary_span();
        let launched = async {
            let opts = admission(engine, signal, &self.session()?, name, Launch::Scheduled, None).await?;
            self.launch(opts, &id, &signal.content, &span).await
        }
        .await;
        engine.end_loop(&id).await;
        let result = match launched {
            Ok((_, result)) => result,
            Err(error) if is_aborted(&error) => return Err(error),
            // It could not start (a skill it names went away): nothing changed.
            Err(error) => FlowResult::Faulted {
                error: owf::run::WorkflowError::runtime(error.to_string()),
                effects_started: false,
            },
        };
        match &result {
            FlowResult::Completed(output) => span.end(SpanEnd::output(output.clone())),
            FlowResult::Waiting { .. } | FlowResult::Interrupted { .. } => span.end(SpanEnd::default()),
            // The agent loop takes it over under a primary span of its own.
            FlowResult::Faulted { error, effects_started: false } => {
                self.state().span = None;
                span.end(SpanEnd::error(error.to_string()));
            }
            // A failure after a write ends it as a primary loop's does: the
            // recovery report, or the signal's error.
            _ => {}
        }
        Ok(result)
    }
    fn session(&self) -> anyhow::Result<Session> {
        self.state().session.clone().ok_or_else(|| anyhow::anyhow!("signal_session has not run"))
    }
    async fn dispatch(&self, name: &str, args: &Value) -> anyhow::Result<Value> {
        let (supervisor, signal, trace) = (&self.supervisor, &self.signal, &self.trace);
        let (engine, cancel) = (&supervisor.engine, &supervisor.cancel);
        let id = format!("{}:{}", signal.source, signal.id);
        let scope: Scope = trace.clone();
        let mcp = engine.deps.mcp.as_ref();
        match name {
            "deliver_event" => {
                let type_ = args["type"].as_str().ok_or_else(|| anyhow::anyhow!("type: Required"))?;
                Ok(
                    json!({"delivered":supervisor.flows.deliver_event_in(type_, &args["data"], &scope)?,"output":"Button delivered"}),
                )
            }
            "signal_session" => {
                let session = session(&id, gather_env_data(&supervisor.env, cancel).await?);
                let context = render_context(&session.env)?;
                let default_chat = session.env.telegram_chat_id.clone();
                self.state().session = Some(session);
                Ok(json!({"context": context, "engine_skills": engine.deps.skills,
                    "conversation":conversation(signal),"default_chat":default_chat}))
            }
            // A saved workflow that failed before changing anything hands its task to the agent loop.
            "run_saved_workflow" => {
                let name = args["name"].as_str().ok_or_else(|| anyhow::anyhow!("name: Required"))?;
                let result = self.scheduled_launch(name).await?;
                check(cancel)?;
                let (handover, failure) = match result {
                    FlowResult::Completed(output) => {
                        let output = json!({"ok":true,"workflow":name,"output":output});
                        trace.update(ContextUpdate::output(output.clone()));
                        return Ok(json!({"done":true,"output":output.to_string()}));
                    }
                    FlowResult::Cancelled => return Err(aborted()),
                    FlowResult::Waiting { wake_at_ms } => {
                        let output = json!({"ok":true,"workflow":name,"waiting_until_ms":wake_at_ms});
                        trace.update(ContextUpdate::output(output.clone()));
                        return Ok(json!({"done":true,"output":output.to_string()}));
                    }
                    // Not a failure to report: the run stays recoverable and the durable worker resumes it.
                    FlowResult::Interrupted { error } => {
                        let output = json!({"ok":false,"workflow":name,"interrupted":error,"resumes":true});
                        trace.event(EventStart {
                            name: "interrupted".into(),
                            level: Some(Level::Warning),
                            metadata: output.as_object().cloned(),
                            ..Default::default()
                        });
                        trace.update(ContextUpdate::output(output.clone()));
                        return Ok(json!({"done":true,"output":output.to_string()}));
                    }
                    FlowResult::Faulted { error, effects_started: true } => {
                        trace.event(EventStart {
                            name: "fallback".into(),
                            level: Some(Level::Error),
                            metadata: Some(
                                json!({"stage":"saved_workflow","workflow":name,"error":error,"effects_started":true})
                                    .as_object()
                                    .unwrap()
                                    .clone(),
                            ),
                            ..Default::default()
                        });
                        anyhow::bail!("Saved workflow {name} failed after it may have changed something: {error}");
                    }
                    FlowResult::Faulted { error, .. } => (
                        format!(
                            "The saved workflow `{name}` for this task failed before changing anything: {error}. Do the task yourself."
                        ),
                        json!({"stage":"saved_workflow","workflow":name,"error":error,"effects_started":false}),
                    ),
                    FlowResult::Missing => (
                        format!(
                            "This task names the saved workflow `{name}`, which has no active version. Do the task yourself."
                        ),
                        json!({"stage":"saved_workflow","workflow":name,"reason":"missing"}),
                    ),
                };
                trace.event(EventStart {
                    name: "fallback".into(),
                    level: Some(Level::Warning),
                    metadata: failure.as_object().cloned(),
                    ..Default::default()
                });
                trace.update(ContextUpdate::metadata(
                    json!({"fallback":"agent-loop","workflow_failure_stage":"saved_workflow"})
                        .as_object()
                        .unwrap()
                        .clone(),
                ));
                Ok(json!({"done":false,"handover":handover}))
            }
            "route_signal" => {
                anyhow::ensure!(!self.routed, "a routed signal workflow cannot dispatch routes again");
                let Some((name, definition, version)) = crate::flows::signal_route(
                    engine,
                    &signal.source,
                    signal.payload.as_ref().unwrap_or(&Value::Null),
                )?
                else {
                    return Ok(json!({"routed":false}));
                };
                let wf = owf::model::parse_yaml(&definition)?;
                let span =
                    trace
                        .span(SpanStart::new(format!("route:{name}"), SpanKind::Chain).metadata(
                            json!({"workflow":name,"workflow_version":version}).as_object().unwrap().clone(),
                        ));
                // The routed workflow is the channel's whole flow, started
                // as any signal's run is. A signal route (calling
                // primary_loop itself) is superseded and runs below on the
                // dispatcher's host for the routes already active.
                if !crate::flows::calls_dispatcher(&wf) {
                    let result = self.route_launch(&name, &args["context"]).await;
                    match &result {
                        Ok(output) => span.end(SpanEnd::output(json!(output))),
                        Err(error) => span.end(SpanEnd::error(error.to_string())),
                    }
                    return Ok(json!({"routed":true,"output":result?}));
                }
                let mut functions = self.clone();
                functions.routed = true;
                let mut host = owf::run::Host {
                    functions: Arc::new(functions),
                    observer: Arc::new(owf::run::Silent),
                    clock: Arc::new(owf::run::SystemClock),
                    limits: owf::run::Limits::default(),
                    runtime: json!({"name":"agent-supervisor","version":env!("CARGO_PKG_VERSION")}),
                };
                let run = format!("{id}/{name}");
                if let Some(sandbox) = engine.sandbox() {
                    sandbox.watch(&mut host, &run, &name, version, None, None);
                }
                let outcome = Box::pin(owf::run::run(&host, &wf, &run, args["context"].clone(), cancel)).await;
                let result = crate::flows::flow_result(outcome.status, outcome.effects_started);
                crate::flows::record_outcome(engine, &name, version, &result);
                match result {
                    FlowResult::Completed(output) => {
                        span.end(SpanEnd::output(output.clone()));
                        Ok(json!({"routed":true,"output":output}))
                    }
                    FlowResult::Cancelled => {
                        span.end(SpanEnd::default());
                        Err(aborted())
                    }
                    FlowResult::Faulted { error, .. } => {
                        span.end(SpanEnd::error(error.to_string()));
                        // Preserve the original typed host error across both YAML
                        // catches, including aborts and the primary failure detail.
                        Err(self.errors.lock().unwrap_or_else(|e| e.into_inner()).take().unwrap_or_else(|| {
                            anyhow::anyhow!(error.detail.unwrap_or_else(|| error.title.unwrap_or_default()))
                        }))
                    }
                    other => {
                        span.end(SpanEnd::error(format!("{other:?}")));
                        anyhow::bail!("signal route {name} did not end: {other:?}")
                    }
                }
            }
            "signal_input" => {
                let (input, earlier) = signal_input(signal, &self.session()?, mcp, &scope, cancel).await?;
                self.state().earlier_user_messages = earlier;
                Ok(json!(input))
            }
            "recent_activity" => Ok(json!(load_recent_activity(mcp, &scope, cancel).await?)),
            "pending_confirmations" => {
                let target = serde_json::from_value(args["target"].clone())?;
                Ok(pending_text(supervisor.flows.pending_confirmations(&target)?))
            }
            "preselect_skills" => {
                let text = |key: &str| args[key].as_str().unwrap_or_default().to_owned();
                let loaded = strings(args, "loaded");
                let found =
                    preselect_skills(&engine.deps, &text("request"), &text("context"), &loaded, &scope, cancel).await?;
                self.state().selected_skill_text = found.as_ref().map(|(_, block)| block.clone()).unwrap_or_default();
                Ok(found.map_or(Value::Null, |(names, block)| json!({"names": names, "block": block})))
            }
            "signal_transcript" => {
                let original = self.state().primary.clone();
                // A run that took no turn (a scheduled workflow's steps) has
                // only its instructions: no transcript.
                let messages = match original {
                    Some(original) => original.messages.lock().await.clone(),
                    None => Vec::new(),
                };
                let transcript = if messages.iter().any(|m| crate::providers::role(m) != "system") {
                    crate::generation::messages_json(&messages).to_string()
                } else {
                    "[]".into()
                };
                Ok(json!(tail_utf16(&transcript, 20000)))
            }
            "recovery_loop" => {
                // These lifecycle guards also apply to an override's catch handler.
                let context = self.state().session.clone();
                let aborted = self.errors.lock().unwrap_or_else(|e| e.into_inner()).as_ref().is_some_and(is_aborted);
                let Some(context) = context.filter(|_| !cancel.is_cancelled() && !aborted) else {
                    return Ok(Value::Null);
                };
                if self.state().recovery_reported {
                    return Ok(Value::Null);
                }
                self.state().recovery_reported = true;
                let error = args["error"]["detail"].as_str().unwrap_or("signal failed");
                if let Some(primary) = self.state().span.take() {
                    primary.end(SpanEnd::error(error).with_output(json!({"error": error})));
                }
                let span = trace.span(
                    SpanStart::new("recovery", SpanKind::Agent)
                        .metadata(json!({"skill":"recovery"}).as_object().unwrap().clone()),
                );
                let id = format!("{}__recovery", context.id);
                // A reporting failure is an observation, never the signal's error.
                let result = async {
                    let prompt = args["prompt"].as_str().ok_or_else(|| anyhow::anyhow!("prompt: Required"))?;
                    let preset = args["preset"].as_str().unwrap_or("base");
                    let preset = crate::models::PresetName::selectable(preset)
                        .ok_or_else(|| anyhow::anyhow!("preset: unknown preset {preset}"))?;
                    let iterations = args["max_iterations"]
                        .as_u64()
                        .filter(|n| *n > 0)
                        .and_then(|n| usize::try_from(n).ok())
                        .ok_or_else(|| anyhow::anyhow!("max_iterations: Expected positive integer"))?;
                    let mut opts = LoopOptions::new(&id, context.clone());
                    opts.cancel = cancel.clone();
                    opts.parent_id = Some(context.id.clone());
                    opts.guard_request = Some(signal.content.clone());
                    opts.skills = strings(args, "skills");
                    opts.include_engine_skills = false;
                    opts.preset = preset;
                    opts.max_iterations = iterations;
                    opts.trace_scope = Some(span.clone());
                    opts.system_prompt = Some(strings(args, "system").join("\n\n"));
                    engine.start_loop(opts).await?.send(prompt.to_owned()).await
                }
                .await;
                let output = match result {
                    Ok(output) => {
                        span.end(SpanEnd::output(output.clone()));
                        json!({"output": output})
                    }
                    Err(error) => {
                        span.end(SpanEnd::error(error.to_string()).with_output(json!({"error":error.to_string()})));
                        eprintln!("[{}] recovery failed: {error}", context.id);
                        json!({"error": error.to_string()})
                    }
                };
                engine.end_loop(&id).await;
                Ok(output)
            }
            "primary_loop" => {
                let input = args["input"].as_str().ok_or_else(|| anyhow::anyhow!("input: Required"))?;
                let preset = args["preset"].as_str().unwrap_or("smart");
                let preset = crate::models::PresetName::selectable(preset)
                    .ok_or_else(|| anyhow::anyhow!("preset: unknown preset {preset}"))?;
                let mut opts = LoopOptions::new(&id, self.session()?);
                opts.workflow = args["loop"].as_str().unwrap_or(crate::agent::AGENT_LOOP_NAME).to_owned();
                opts.confirm = args
                    .get("confirm")
                    .filter(|v| !v.is_null())
                    .map(|v| serde_json::from_value(v.clone()))
                    .transpose()?;
                opts.earlier_user_messages = self.state().earlier_user_messages.clone();
                let selected = self.state().selected_skill_text.clone();
                if strings(args, "system").iter().any(|part| part.contains(&selected)) {
                    opts.loaded_skill_text = selected;
                }
                opts.skills = strings(args, "skills");
                opts.allow_skill_writes = args["allow_skill_writes"].as_bool().unwrap_or(false);
                opts.system_prompt = Some(strings(args, "system").join("\n\n"));
                opts.preset = preset;
                let output = self.run_primary(opts, input.to_owned(), Some(strings(args, "loaded"))).await?;
                Ok(json!({"output": output}))
            }
            _ => anyhow::bail!("unknown signal function {name}"),
        }
    }
}

#[async_trait::async_trait]
impl owf::run::Functions for SignalFunctions {
    fn effect(&self, name: &str) -> Option<owf::run::Effect> {
        SIGNAL_FUNCTIONS.iter().find(|f| f.name == name).map(|f| f.effect)
    }
    // Each stops on the supervisor's cancellation and ends its own spans.
    fn cooperative(&self, _name: &str) -> bool {
        true
    }
    async fn call(
        &self,
        name: &str,
        args: Value,
        _: &owf::run::CallInfo<'_>,
    ) -> Result<Value, owf::run::WorkflowError> {
        self.dispatch(name, &args).await.map_err(|error| {
            let failure = owf::run::WorkflowError::runtime(error.to_string());
            *self.errors.lock().unwrap_or_else(|e| e.into_inner()) = Some(error);
            failure
        })
    }
}

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
