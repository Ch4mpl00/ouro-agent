// Supervisor domain: 1. signal / Telegram and peer context; 2. routing and recovery;
// 3. serial queue loop. Composition and OS signals live in main.rs.
use crate::{
    activity::{ACTIVITY_STREAM, tail_utf16},
    agent::{AgentLoop, Engine, EngineDeps, LoopOptions},
    errors::{aborted, check, is_aborted},
    flows::{FlowResult, FlowRun},
    mcp::{Mcp, SharedMcp},
    session::{EnvDeps, Format, Session, gather_env_data, render_context, session},
    skills::{FindSkillsParams, append_patch, find_skills, read_skill_index},
    tool_results::is_tool_error,
    tracing::{ContextUpdate, EventStart, Level, Scope, Span, SpanEnd, SpanKind, SpanStart, Trace, TraceStart},
    workflow::{RunResult, WorkflowRun, WorkflowSignal},
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

// ── 2. routing / recovery ────────────────────────────────────────────────
pub struct Supervisor {
    pub engine: Arc<Engine>,
    pub env: EnvDeps,
    pub workflow: Arc<dyn WorkflowRun>,
    pub flows: Arc<dyn FlowRun>,
    cancel: CancellationToken,
    active: Arc<tokio::sync::Semaphore>,
    shutdown: tokio::sync::Mutex<bool>,
    // Sources whose primary loop runs as a durable `agent-loop` run (`all`
    // for every source): a restart resumes it instead of losing it.
    durable_loops: Vec<String>,
}
pub fn create_supervisor(
    engine: Arc<Engine>,
    env: EnvDeps,
    workflow: Arc<dyn WorkflowRun>,
    flows: Arc<dyn FlowRun>,
) -> Arc<Supervisor> {
    create_supervisor_with_cancel(engine, env, workflow, flows, CancellationToken::new(), Vec::new())
}
pub fn create_supervisor_with_cancel(
    engine: Arc<Engine>,
    env: EnvDeps,
    workflow: Arc<dyn WorkflowRun>,
    flows: Arc<dyn FlowRun>,
    cancel: CancellationToken,
    durable_loops: Vec<String>,
) -> Arc<Supervisor> {
    Arc::new(Supervisor {
        engine,
        env,
        workflow,
        flows,
        cancel,
        active: Arc::new(tokio::sync::Semaphore::new(1)),
        shutdown: Default::default(),
        durable_loops,
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
    pub async fn run_signal(&self, signal: &PendingSignal) -> anyhow::Result<String> {
        self.start(signal).await.unwrap_or_else(|| Err(aborted()))
    }
    // Runs `signal` unless the supervisor is stopping, which comes back as
    // None: nothing of the signal ran, so a scheduled fire can give its slot back.
    pub async fn start(&self, signal: &PendingSignal) -> Option<anyhow::Result<String>> {
        if self.cancel.is_cancelled() {
            return None;
        }
        let _active = self.active.acquire().await.ok()?;
        if self.cancel.is_cancelled() {
            return None;
        }
        Some(self.run_active(signal).await)
    }
    async fn run_active(&self, signal: &PendingSignal) -> anyhow::Result<String> {
        let id = format!("{}:{}", signal.source, signal.id);
        let saved = signal.saved_workflow();
        let use_workflow = signal.source == "scheduler" && saved.is_none();
        let trace=self.engine.deps.tracer.trace(TraceStart{id:id.clone(),name:format!("signal:{}",signal.source),kind:SpanKind::Agent,session_id:Some(id.clone()),tags:Some(vec![signal.source.clone(),if saved.is_some(){"saved-workflow"}else if use_workflow{"planner-mode"}else{"agent-loop"}.into()]),metadata:Some(json!({"signal_id":signal.id,"signal_source":signal.source,"signal_created_at":signal.created_at,"activity":"run"}).as_object().unwrap().clone())});
        trace.update(ContextUpdate::input(signal.content.clone()));
        let scope: Scope = trace.clone();
        let mut context = None;
        let mut loop_: Option<Arc<AgentLoop>> = None;
        let mut span: Option<Arc<dyn Span>> = None;
        let outcome=async{
            let session=session(&id,gather_env_data(&self.env,&self.cancel).await?);context=Some(session.clone());
            // A saved workflow that failed before changing anything hands its task to the agent loop.
            let mut handover=None;
            if let Some(name)=&saved{
                let result=self.flows.run(name,&signal.workflow_signal(),&session,&scope,&self.cancel).await;check(&self.cancel)?;
                let failure=match result{
                    FlowResult::Completed(output)=>{let output=json!({"ok":true,"workflow":name,"output":output});trace.update(ContextUpdate::output(output.clone()));return Ok(output.to_string());},
                    FlowResult::Cancelled=>return Err(aborted()),
                    FlowResult::Waiting{wake_at_ms}=>{let output=json!({"ok":true,"workflow":name,"waiting_until_ms":wake_at_ms});trace.update(ContextUpdate::output(output.clone()));return Ok(output.to_string());},
                    // Not a failure to report: the run stays recoverable and the durable worker resumes it.
                    FlowResult::Interrupted{error}=>{let output=json!({"ok":false,"workflow":name,"interrupted":error,"resumes":true});trace.event(EventStart{name:"interrupted".into(),level:Some(Level::Warning),metadata:output.as_object().cloned(),..Default::default()});trace.update(ContextUpdate::output(output.clone()));return Ok(output.to_string());},
                    FlowResult::Faulted{error,effects_started:true}=>{trace.event(EventStart{name:"fallback".into(),level:Some(Level::Error),metadata:Some(json!({"stage":"saved_workflow","workflow":name,"error":error,"effects_started":true}).as_object().unwrap().clone()),..Default::default()});anyhow::bail!("Saved workflow {name} failed after it may have changed something: {error}");},
                    FlowResult::Faulted{error,..}=>{handover=Some(format!("The saved workflow `{name}` for this task failed before changing anything: {error}. Do the task yourself."));json!({"stage":"saved_workflow","workflow":name,"error":error,"effects_started":false})},
                    FlowResult::Missing=>{handover=Some(format!("This task names the saved workflow `{name}`, which has no active version. Do the task yourself."));json!({"stage":"saved_workflow","workflow":name,"reason":"missing"})},
                };
                trace.event(EventStart{name:"fallback".into(),level:Some(Level::Warning),metadata:failure.as_object().cloned(),..Default::default()});trace.update(ContextUpdate::metadata(json!({"fallback":"agent-loop","workflow_failure_stage":"saved_workflow"}).as_object().unwrap().clone()));
            }
            if use_workflow{
                let result=self.workflow.run_signal(&signal.workflow_signal(),session.clone(),scope.clone(),&self.cancel).await?;check(&self.cancel)?;
                let fallback=match result{
                    RunResult::Ok{attempts,step_count,..}=>{let output=json!({"ok":true,"attempts":attempts,"stepCount":step_count});trace.update(ContextUpdate::output(output.clone()));return Ok(output.to_string());},
                    RunResult::ExecuteFailed{reason,error,step_index,..}=>{trace.event(EventStart{name:"fallback".into(),level:Some(Level::Error),metadata:Some(json!({"stage":"execute","reason":reason,"step_index":step_index}).as_object().unwrap().clone()),..Default::default()});anyhow::bail!("Workflow execution failed at step {step_index} ({reason}): {error}");},
                    RunResult::CompileFailed{reason,errors,attempts}=>json!({"stage":"compile","reason":reason,"errors":errors,"attempts":attempts}),
                    RunResult::ReplanExhausted{passes,attempts}=>json!({"stage":"replan_exhausted","reason":"replan_exhausted","passes":passes,"attempts":attempts}),
                };
                trace.event(EventStart{name:"fallback".into(),level:Some(Level::Warning),metadata:fallback.as_object().cloned(),..Default::default()});trace.update(ContextUpdate::metadata(json!({"fallback":"agent-loop","workflow_failure_stage":fallback["stage"]}).as_object().unwrap().clone()));
            }
            let primary=trace.span(SpanStart::new("agent_loop",SpanKind::Chain).metadata(json!({"activity":"primary_loop"}).as_object().unwrap().clone()));span=Some(primary.clone());
            let input=if signal.source==PEER_SOURCE{prepare_peer_input(signal,self.engine.deps.mcp.as_ref(),&scope,&self.cancel).await?}else{prepare_telegram_input(signal,&session,self.engine.deps.mcp.as_ref(),&scope,&self.cancel).await?};
            let conversation=signal.source=="telegram"||signal.source==PEER_SOURCE;
            let activity=if conversation{load_recent_activity(self.engine.deps.mcp.as_ref(),&scope,&self.cancel).await?}else{None};let transport=signal.source==PEER_SOURCE||signal.source=="scheduler";
            let loaded:Vec<String>=if transport{vec![signal.source.clone()]}else{Vec::new()}.into_iter().chain(self.engine.deps.skills.iter().cloned()).collect();
            let preselected=preselect_skills(&self.engine.deps,&signal.content,&format!("Signal source: {}\n\n{input}",signal.source),&loaded,&scope,&self.cancel).await?;
            let mut prompts=vec![render_context(&session.env)?];if let Some(activity)=activity{prompts.push(activity);}prompts.push(format!("Signal source: {}",signal.source));prompts.push("Call find_skills only when the task needs something specific that the skills already in your instructions don't cover; the source is only a hint. You own delivery.".into());
            let preselected_names=preselected.as_ref().map(|(names,_)|names.clone()).unwrap_or_default();if let Some((_,block))=preselected{prompts.push(block);}
            if signal.source=="telegram"{prompts.push("Delivery: use send_telegram_message for the current signal's chatId and optional messageThreadId. A final assistant message alone is not delivered to Telegram.".into());}
            if let Some(env)=signal.env_context.as_ref().filter(|s|!s.is_empty()){prompts.push(env.clone());}
            if let Some(note)=handover{prompts.push(note);}
            let mut opts=LoopOptions::new(&id,session);opts.cancel=self.cancel.clone();opts.skills=if transport{vec![signal.source.clone()]}else{Vec::new()};opts.allow_skill_writes=signal.source=="dreaming";opts.system_prompt=Some(prompts.join("\n\n"));opts.preset=if signal.source=="scheduler"{crate::models::PresetName::Base}else{crate::models::PresetName::Smart};opts.trace_scope=Some(primary.clone());
            let loaded_skills:Vec<_>=loaded.into_iter().chain(preselected_names).collect();let agent=self.engine.start_loop(opts).await?;loop_=Some(agent.clone());let output=self.drive(&agent,&id,input,&primary).await?;primary.end(SpanEnd::output(output.clone()));trace.update(ContextUpdate{output:Some(json!(output)),metadata:Some(json!({"skills":loaded_skills}).as_object().unwrap().clone()),..Default::default()});Ok::<_,anyhow::Error>(output)
        }.await;
        if let Err(error) = &outcome {
            if let Some(span) = &span {
                span.end(SpanEnd::error(error.to_string()).with_output(json!({"error":error.to_string()})));
            }
            trace.update(ContextUpdate {
                output: Some(json!({"error":error.to_string()})),
                metadata: Some(json!({"error":true}).as_object().unwrap().clone()),
                ..Default::default()
            });
            if let Some(context) = context
                && !self.cancel.is_cancelled()
                && !is_aborted(error)
            {
                self.recover(signal, context, error, loop_.as_ref(), &trace).await;
            }
        }
        if span.is_some() {
            self.engine.end_loop(&id).await;
        }
        trace.end();
        check(&self.cancel)?;
        outcome
    }
    // Runs the primary loop: as a durable `agent-loop` run for the sources
    // configured so, else in this process.
    async fn drive(
        &self,
        agent: &Arc<AgentLoop>,
        id: &str,
        input: String,
        primary: &Arc<dyn Span>,
    ) -> anyhow::Result<String> {
        let source = id.split(':').next().unwrap_or_default();
        if !self.durable_loops.iter().any(|s| s == source || s == "all") {
            return agent.send(input).await;
        }
        let scope: Scope = primary.clone();
        let run = agent.workflow_input(&input).await;
        match self.flows.run_loop(&format!("{id}/loop"), run, &scope, &self.cancel).await {
            FlowResult::Completed(output) => {
                Ok(output.as_str().map(str::to_owned).unwrap_or_else(|| output.to_string()))
            }
            FlowResult::Missing => agent.send(input).await,
            FlowResult::Cancelled => Err(aborted()),
            FlowResult::Faulted { error, .. } => Err(anyhow::anyhow!("{error}")),
            // Released on shutdown or held elsewhere: the recovery worker resumes it.
            FlowResult::Waiting { .. } | FlowResult::Interrupted { .. } => {
                check(&self.cancel)?;
                Ok(json!({"ok": true, "resumes": true}).to_string())
            }
        }
    }
    async fn recover(
        &self,
        signal: &PendingSignal,
        context: Session,
        error: &anyhow::Error,
        original: Option<&Arc<AgentLoop>>,
        trace: &Arc<dyn Trace>,
    ) {
        let span = trace.span(
            SpanStart::new("recovery", SpanKind::Agent)
                .metadata(json!({"skill":"recovery"}).as_object().unwrap().clone()),
        );
        let id = format!("{}__recovery", context.id);
        let result=async{
            let mut opts=LoopOptions::new(&id,context.clone());opts.cancel=self.cancel.clone();opts.parent_id=Some(context.id.clone());opts.skills=vec!["recovery".into()];opts.include_engine_skills=false;opts.preset=crate::models::PresetName::Base;opts.max_iterations=5;opts.trace_scope=Some(span.clone());
            let mut system=vec![render_context(&context.env)?];if let Some(env)=signal.env_context.as_ref().filter(|s|!s.is_empty()){system.push(env.clone());}opts.system_prompt=Some(system.join("\n\n"));
            let recovery=self.engine.start_loop(opts).await?;let transcript=if let Some(original)=original{crate::generation::messages_json(&original.messages.lock().await).to_string()}else{"[]".into()};
            recovery.send(format!("Original signal ({}):\n{}\n\nError: {error}\n\nRecent transcript (may be truncated):\n{}\n\nReport the failure only. Do not repeat the original actions; some may already have succeeded.",signal.source,signal.content,tail_utf16(&transcript,20000))).await
        }.await;
        match result {
            Ok(output) => span.end(SpanEnd::output(output)),
            Err(error) => {
                span.end(SpanEnd::error(error.to_string()).with_output(json!({"error":error.to_string()})));
                eprintln!("[{}] recovery failed: {error}", context.id);
            }
        }
        self.engine.end_loop(&id).await;
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
async fn poll_once(mcp: &SharedMcp, supervisor: &Supervisor, cancel: &CancellationToken) -> anyhow::Result<()> {
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
