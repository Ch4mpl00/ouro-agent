// Activity journal: 1. bounded projection; 2. note generation/validation;
// 3. SQLite progress; 4. background worker. Reads completed production traces
// only, and shares the supervisor's existing MCP session.
use crate::{
    db::{AgentDb, MemoryStore, TraceStore},
    errors::check,
    generation::{Observation as GenerationObservation, describe_completion, generation},
    mcp::SharedMcp,
    models::ReasoningEffort,
    providers::{self, CompletionParams, SharedProvider},
    session::iso,
    tool_results::is_tool_error,
    trace_model::{Observation, ObservationType, Trace},
    tracing::null_scope,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::{StreamExt, stream};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub const ACTIVITY_STREAM: &str = "ouro/activity";
pub const ACTIVITY_STREAM_CAPACITY: u32 = 200;
pub const ACTIVITY_PROJECTOR_VERSION: u32 = 1;
const NOISE: &[&str] = &[
    "working_memory_get",
    "working_memory_put",
    "working_memory_list",
    "working_memory_delete",
    "send_telegram_chat_action",
];
const READ: &[&str] = &[
    "search_news",
    "list_news",
    "fetch_article",
    "read_pdf",
    "read_file",
    "list_skills",
    "read_skill",
    "recall",
    "read_doc",
    "get_fact",
    "list_memory",
    "doc_history",
    "journal_tail",
    "get_telegram_chat_history",
    "list_userbot_dialogs",
    "list_channel_posts",
    "list_nashdom_mails",
    "list_monobank_transactions",
    "list_scheduled_tasks",
    "list_signals",
    "get_timezone",
];
const MESSAGE: &[&str] = &["send_telegram_message", "edit_telegram_message"];
#[derive(Clone, Debug)]
pub struct ProjectionBudget {
    pub total_chars: usize,
    pub input_chars: usize,
    pub context_chars: usize,
    pub final_chars: usize,
    pub arg_chars: usize,
    pub message_chars: usize,
    pub result_chars: usize,
    pub agent_result_chars: usize,
    pub error_chars: usize,
    pub max_actions: usize,
}
impl Default for ProjectionBudget {
    fn default() -> Self {
        Self {
            total_chars: 12000,
            input_chars: 1500,
            context_chars: 1200,
            final_chars: 1200,
            arg_chars: 300,
            message_chars: 700,
            result_chars: 300,
            agent_result_chars: 600,
            error_chars: 400,
            max_actions: 40,
        }
    }
}
pub fn utf16_len(text: &str) -> usize {
    text.encode_utf16().count()
}
pub fn head_utf16(text: &str, max: usize) -> &str {
    let mut units = 0;
    let mut end = 0;
    for (at, c) in text.char_indices() {
        if units + c.len_utf16() > max {
            break;
        }
        units += c.len_utf16();
        end = at + c.len_utf8();
    }
    &text[..end]
}
pub fn tail_utf16(text: &str, max: usize) -> &str {
    let mut units = 0;
    let mut start = text.len();
    for (at, c) in text.char_indices().rev() {
        if units + c.len_utf16() > max {
            break;
        }
        units += c.len_utf16();
        start = at;
    }
    &text[start..]
}
pub fn clip(text: &str, max: usize) -> String {
    let length = utf16_len(text);
    if length <= max { text.into() } else { format!("{}… [clipped, {length} chars]", head_utf16(text, max)) }
}
pub fn clip_tail(text: &str, max: usize) -> String {
    let length = utf16_len(text);
    if length <= max { text.into() } else { format!("[clipped, {length} chars] …{}", tail_utf16(text, max)) }
}
fn text(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        _ => value.to_string(),
    }
}
fn parse_maybe_json(value: &Value) -> Value {
    if let Some(s) = value.as_str()
        && s.trim_start().starts_with(['{', '['])
    {
        serde_json::from_str(s).unwrap_or_else(|_| value.clone())
    } else {
        value.clone()
    }
}
pub fn compact(value: &Value, str_chars: usize, drop_bodies: bool, depth: usize) -> Value {
    match value {
        Value::String(s) => json!(clip(s, str_chars)),
        Value::Array(a) => {
            if depth == 0 {
                return json!(format!("[array of {}]", a.len()));
            }
            let first: Vec<_> =
                a.iter().take(3).map(|v| compact(v, str_chars.min(160), drop_bodies, depth - 1)).collect();
            if a.len() <= 3 { json!(first) } else { json!({"count":a.len(),"first":first}) }
        }
        Value::Object(m) => {
            if depth == 0 {
                return json!(format!("[object with {} keys]", m.len()));
            }
            let mut out = Map::new();
            for (key, value) in m.iter().take(15) {
                out.insert(
                    key.clone(),
                    if drop_bodies
                        && ["body", "text", "content", "html", "markdown", "transcript"].contains(&key.as_str())
                        && value.as_str().is_some_and(|s| utf16_len(s) > 120)
                    {
                        json!(format!("[omitted, {} chars]", utf16_len(value.as_str().unwrap())))
                    } else {
                        compact(value, str_chars, drop_bodies, depth - 1)
                    },
                );
            }
            if m.len() > 15 {
                out.insert("…".into(), json!(format!("{} more keys", m.len() - 15)));
            }
            Value::Object(out)
        }
        _ => value.clone(),
    }
}
fn tool_name(o: &Observation) -> String {
    if let Some(tool) = o.meta("tool").and_then(Value::as_str).filter(|s| !s.is_empty()) {
        return tool.into();
    }
    if o.kind == ObservationType::Agent {
        return format!(
            "worker:{}",
            o.meta("skill").and_then(Value::as_str).filter(|s| !s.is_empty()).unwrap_or("agent")
        );
    }
    o.name.clone()
}
fn utc_iso(s: &str) -> String {
    if s.len() == 19 && s.as_bytes().get(10) == Some(&b' ') {
        format!("{}Z", s.replacen(' ', "T", 1))
    } else {
        s.into()
    }
}
#[derive(Clone)]
struct Projected {
    action: Value,
    clipped: bool,
    obs_id: String,
    args_seen: Vec<Value>,
    first_result: Value,
}
fn build_packet(
    trace: &Trace,
    observations: &[Observation],
    completed_at: Option<&str>,
    b: &ProjectionBudget,
    factor: f64,
) -> Value {
    let scaled = |size: usize| ((size as f64 * factor).floor() as usize).max(40);
    let by_id: HashMap<_, _> = observations.iter().map(|o| (o.id.as_str(), o)).collect();
    let mut omitted = Map::new();
    let mut refs = HashMap::new();
    let mut kept = Vec::new();
    // What a dry run did inside (sandbox.rs) did not happen: only the try
    // itself is an action of the run.
    let dry = |o: &Observation| {
        let mut parent = o.parent_observation_id.as_deref();
        let mut visited = HashSet::new();
        while let Some(id) = parent.filter(|id| visited.insert(*id)) {
            let Some(p) = by_id.get(id) else { break };
            if p.meta("dry_run") == Some(&json!(true)) {
                return true;
            }
            parent = p.parent_observation_id.as_deref();
        }
        false
    };
    for o in observations {
        if o.parent_observation_id.is_none()
            || !matches!(o.kind, ObservationType::Tool | ObservationType::Agent)
            || o.meta("tool_span").is_some()
            || dry(o)
        {
            continue;
        }
        let name = tool_name(o);
        let omit = if o.meta("automatic") == Some(&json!(true)) {
            Some("automatic_context_fetch")
        } else if NOISE.contains(&name.as_str()) {
            Some(name.as_str())
        } else {
            None
        };
        if let Some(name) = omit {
            let count = omitted.get(name).and_then(Value::as_u64).unwrap_or(0) + 1;
            omitted.insert(name.into(), json!(count));
            continue;
        }
        refs.insert(o.id.as_str(), refs.len() + 1);
        kept.push(o);
    }
    let mut projected: Vec<Projected> = Vec::new();
    for o in kept {
        let tool = tool_name(o);
        let agent = o.kind == ObservationType::Agent;
        let message = MESSAGE.contains(&tool.as_str());
        let mut parent = o.parent_observation_id.as_deref();
        let mut parent_ref = None;
        let mut visited = HashSet::new();
        while let Some(id) = parent {
            if !visited.insert(id) {
                break;
            }
            if let Some(reference) = refs.get(id) {
                parent_ref = Some(*reference);
                break;
            }
            parent = by_id.get(id).and_then(|o| o.parent_observation_id.as_deref());
        }
        let raw_args = if o.input["tool"].as_str() == Some(&tool) && o.input.get("args").is_some() {
            &o.input["args"]
        } else {
            &o.input
        };
        let args = compact(raw_args, scaled(if message { b.message_chars } else { b.arg_chars }), false, 3);
        let mut clipped = utf16_len(&raw_args.to_string()) > utf16_len(&args.to_string());
        let failed = o.level == "ERROR" || o.output.as_str().is_some_and(is_tool_error);
        let mut action = json!({"ref":refs[o.id.as_str()],"parent":parent_ref,"tool":tool,"kind":if agent{"agent"}else{"tool"},"outcome":if failed{"error"}else{"ok"},"args":args});
        if failed {
            let detail = o.status_message.clone().unwrap_or_else(|| text(o.output.get("error").unwrap_or(&o.output)));
            let max = scaled(if message { b.message_chars } else { b.error_chars });
            action["error"] = json!(clip(&detail, max));
            clipped |= utf16_len(&detail) > max;
        } else if !o.output.is_null() {
            let result = compact(
                &parse_maybe_json(&o.output),
                scaled(if agent { b.agent_result_chars } else { b.result_chars }),
                true,
                3,
            );
            clipped |= utf16_len(&text(&o.output)) > utf16_len(&result.to_string());
            action["result"] = result;
        }
        let mergeable = action["kind"] == "tool" && action["outcome"] == "ok" && READ.contains(&tool.as_str());
        if let Some(prev) = projected.last_mut()
            && mergeable
            && prev.action["tool"] == tool
            && prev.action["parent"] == action["parent"]
            && prev.action["kind"] == "tool"
            && prev.action["outcome"] == "ok"
        {
            let calls = prev.action["calls"].as_u64().unwrap_or(1) + 1;
            if prev.args_seen.len() < 3 {
                prev.args_seen.push(args.clone());
            }
            let mut args = prev.args_seen.clone();
            if calls > 3 {
                args.push(json!(format!("… {} more calls", calls - 3)));
            }
            prev.action["calls"] = json!(calls);
            prev.action["args"] = json!(args);
            prev.action["result"] = json!({"first_call":prev.first_result,"other_results_omitted":calls-1});
            prev.clipped |= clipped;
            continue;
        }
        projected.push(Projected {
            first_result: action.get("result").cloned().unwrap_or(Value::Null),
            action,
            clipped,
            obs_id: o.id.clone(),
            args_seen: vec![args],
        });
    }
    let actions: Vec<_> = projected
        .into_iter()
        .map(|mut p| {
            if p.clipped {
                p.action["obs"] = json!(p.obs_id);
            }
            p.action
        })
        .collect();
    let input = text(&trace.input);
    let mut packet = json!({"trace_id":trace.id,"source":trace.tags.first(),"started_at":utc_iso(&trace.timestamp),"completed_at":completed_at.map(utc_iso),"tags":trace.tags,"skills":trace.metadata.as_ref().and_then(|m|m.get("skills")).and_then(Value::as_array).map(|a|a.iter().filter(|s|s.is_string()).cloned().collect::<Vec<_>>()).unwrap_or_default(),"status":if trace.metadata.as_ref().and_then(|m|m.get("error"))==Some(&json!(true)){"error"}else{"ok"},"input":clip(&input,scaled(b.input_chars)),"actions":actions});
    // The primary run that did the work: the last, as a scheduled workflow
    // that handed its task to the agent loop comes before it.
    if let Some(primary) = observations.iter().rev().find(|o| o.meta("activity") == Some(&json!("primary_loop"))) {
        let given = text(&primary.input);
        let context = if !input.is_empty() {
            given
                .rfind(&input)
                .map(|at| given[..at].to_owned())
                .unwrap_or_else(|| if given == input { String::new() } else { given.clone() })
        } else {
            given.clone()
        };
        if !context.trim().is_empty() {
            packet["context"] = json!(clip_tail(context.trim(), scaled(b.context_chars)));
        }
        if primary.level != "ERROR"
            && let Some(final_text) = primary.output.as_str().filter(|s| !s.is_empty())
        {
            packet["final"] = json!(clip(final_text, scaled(b.final_chars)));
        }
    }
    if packet.get("final").is_none()
        && let Some(final_text) = trace.output.as_str().filter(|s| !s.is_empty())
    {
        packet["final"] = json!(clip(final_text, scaled(b.final_chars)));
    }
    if let Some(error) = trace.output.get("error").and_then(Value::as_str) {
        packet["error"] = json!(clip(error, scaled(b.error_chars)));
    }
    let mut events: Vec<Value> = Vec::new();
    for o in observations {
        if o.kind != ObservationType::Event || !["WARNING", "ERROR"].contains(&o.level.as_str()) {
            continue;
        }
        if let Some(event) = events.iter_mut().find(|e| e["name"] == o.name && e["level"] == o.level) {
            event["count"] = json!(event["count"].as_u64().unwrap() + 1);
        } else {
            events
                .push(json!({"name":o.name,"level":o.level,"count":1,"detail":compact(&json!(o.metadata),160,true,2)}));
        }
    }
    if !events.is_empty() {
        packet["events"] = json!(events);
    }
    if !omitted.is_empty() {
        packet["omitted"] = json!(omitted);
    }
    packet
}
fn drop_one(packet: &mut Value) -> bool {
    let actions = packet["actions"].as_array_mut().unwrap();
    if actions.is_empty() {
        return false;
    }
    let priority = |a: &Value| {
        if a["outcome"] == "ok" && a["kind"] == "tool" {
            if READ.contains(&a["tool"].as_str().unwrap_or_default()) { 0 } else { 1 }
        } else {
            2
        }
    };
    let lowest = actions.iter().map(priority).min().unwrap();
    let candidates: Vec<_> =
        actions.iter().enumerate().filter(|(_, a)| priority(a) == lowest).map(|(i, _)| i).collect();
    actions.remove(candidates[candidates.len() / 2]);
    let count = packet["omitted"]["actions_over_budget"].as_u64().unwrap_or(0) + 1;
    if packet.get("omitted").is_none() {
        packet["omitted"] = json!({});
    }
    packet["omitted"]["actions_over_budget"] = json!(count);
    true
}
pub fn project_trace(
    trace: &Trace,
    observations: &[Observation],
    completed_at: Option<&str>,
    budget: &ProjectionBudget,
) -> Value {
    let mut packet = Value::Null;
    for factor in [1.0, 0.5, 0.25] {
        packet = build_packet(trace, observations, completed_at, budget, factor);
        while packet["actions"].as_array().unwrap().len() > budget.max_actions && drop_one(&mut packet) {}
        if utf16_len(&packet.to_string()) <= budget.total_chars {
            break;
        }
    }
    while utf16_len(&packet.to_string()) > budget.total_chars && drop_one(&mut packet) {}
    packet
}

// ── 2. notes ─────────────────────────────────────────────────────────────
pub const NOTE_MAX_CHARS: usize = 1500;
pub const NOTE_MAX_LINES: usize = 10;
const NOTE_INSTRUCTIONS: &str = include_str!("activity_note.txt");
#[async_trait]
pub trait NoteGenerator: Send + Sync {
    async fn generate(&self, packet: &Value, cancel: &CancellationToken) -> anyhow::Result<String>;
}
pub struct Notes {
    pub provider: SharedProvider,
    pub model: String,
    pub reasoning_effort: ReasoningEffort,
    pub timeout: Duration,
}
#[async_trait]
impl NoteGenerator for Notes {
    async fn generate(&self, packet: &Value, cancel: &CancellationToken) -> anyhow::Result<String> {
        let mut messages = vec![providers::system(NOTE_INSTRUCTIONS), providers::user(&packet.to_string())];
        let mut params = CompletionParams {
            model: self.model.clone(),
            messages: &mut messages,
            reasoning_effort: self.reasoning_effort,
            tools: Vec::new(),
            response_format: None,
            trace: None,
            cancel: cancel.clone(),
        };
        let result = generation(
            &self.provider,
            &mut params,
            &null_scope(),
            GenerationObservation { name: "activity-note".into(), ..Default::default() },
            Some(self.timeout),
            describe_completion,
        )
        .await?;
        Ok(providers::content(&result.message).unwrap_or_default().into())
    }
}
pub fn validate_note(raw: &str) -> anyhow::Result<String> {
    let note = raw.split('\n').map(str::trim_end).filter(|s| !s.trim().is_empty()).collect::<Vec<_>>().join("\n");
    anyhow::ensure!(!note.is_empty(), "empty note");
    anyhow::ensure!(utf16_len(&note) <= NOTE_MAX_CHARS, "note is {} chars, max {NOTE_MAX_CHARS}", utf16_len(&note));
    anyhow::ensure!(
        note.lines().count() <= NOTE_MAX_LINES,
        "note is {} lines, max {NOTE_MAX_LINES}",
        note.lines().count()
    );
    anyhow::ensure!(!note.contains("```"), "note contains a code block");
    Ok(note)
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActivityDraft {
    pub body: String,
    pub metadata: Map<String, Value>,
}
pub fn render_draft(packet: &Value, note: &str) -> ActivityDraft {
    let when = packet["started_at"].as_str().unwrap_or_default();
    let when = DateTime::parse_from_rfc3339(when)
        .map(|t| t.format("%Y-%m-%d %H:%M UTC").to_string())
        .unwrap_or_else(|_| when.replace('T', " "));
    ActivityDraft{body:format!("{when} · {}{}\n{note}",packet["source"].as_str().unwrap_or("unknown"),if packet["status"]=="error"{" · failed"}else{""}),metadata:json!({"trace_id":packet["trace_id"],"source":packet["source"],"started_at":packet["started_at"],"completed_at":packet["completed_at"],"status":packet["status"],"projector_version":ACTIVITY_PROJECTOR_VERSION}).as_object().unwrap().clone()}
}

// ── 3. progress store ────────────────────────────────────────────────────
#[derive(Clone, Debug)]
pub struct DueTrace {
    pub trace_id: String,
    pub completed_at: String,
}
#[derive(Clone, Debug)]
pub struct Progress {
    pub status: String,
    pub attempts: u32,
    pub draft: Option<ActivityDraft>,
    pub next_attempt_at: Option<String>,
    pub entry_id: Option<i64>,
    // Why the last attempt failed.
    pub error: Option<String>,
}
impl AgentDb {
    pub fn activity_cutoff(&self, now: DateTime<Utc>) -> anyhow::Result<String> {
        if let Some(cutoff) = self.get("activity_journal.cutoff")? {
            return Ok(cutoff);
        }
        let cutoff = now.format("%Y-%m-%d %H:%M:%S").to_string();
        self.set("activity_journal.cutoff", &cutoff)?;
        Ok(cutoff)
    }
    pub fn due_activity(&self, limit: usize, now: DateTime<Utc>) -> anyhow::Result<Vec<DueTrace>> {
        let cutoff = self.activity_cutoff(now)?;
        let conn = self.conn();
        let mut stmt=conn.prepare("SELECT t.id,coalesce((SELECT max(json_extract(o.value,'$.endTime')) FROM json_each(t.observations) o),t.created_at) FROM traces t LEFT JOIN activity_projections p ON p.trace_id=t.id WHERE t.created_at >= ?1 AND json_extract(t.metadata,'$.activity')='run' AND NOT EXISTS (SELECT 1 FROM run_trace r WHERE r.trace_id=t.id AND r.recorded=0) AND (p.trace_id IS NULL OR (p.status='pending' AND (p.next_attempt_at IS NULL OR p.next_attempt_at<=?2))) ORDER BY t.created_at,t.rowid LIMIT ?3")?;
        Ok(stmt
            .query_map(params![cutoff, iso(now), i64::try_from(limit)?], |r| {
                Ok(DueTrace { trace_id: r.get(0)?, completed_at: r.get(1)? })
            })?
            .collect::<Result<_, _>>()?)
    }
    pub fn activity_progress(&self, id: &str) -> anyhow::Result<Option<Progress>> {
        type ProgressRow = (String, u32, Option<String>, Option<String>, Option<i64>, Option<String>);
        let row: Option<ProgressRow> = self
            .conn()
            .query_row(
                "SELECT status,attempts,draft,next_attempt_at,entry_id,error FROM activity_projections WHERE trace_id=?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?)),
            )
            .optional()?;
        row.map(|(status, attempts, draft, next_attempt_at, entry_id, error)| {
            Ok(Progress {
                status,
                attempts,
                draft: draft.map(|s| serde_json::from_str(&s)).transpose()?,
                next_attempt_at,
                entry_id,
                error,
            })
        })
        .transpose()
    }
    pub fn save_activity_draft(&self, id: &str, draft: &ActivityDraft) -> anyhow::Result<()> {
        self.conn().execute("INSERT INTO activity_projections(trace_id,status,draft) VALUES(?1,'pending',?2) ON CONFLICT(trace_id) DO UPDATE SET status='pending',draft=excluded.draft,error=NULL,next_attempt_at=NULL,updated_at=datetime('now')",params![id,serde_json::to_string(draft)?])?;
        Ok(())
    }
    pub fn activity_failure(
        &self,
        id: &str,
        error: &str,
        attempts: u32,
        next: Option<&str>,
        give_up: bool,
    ) -> anyhow::Result<()> {
        self.conn().execute("INSERT INTO activity_projections(trace_id,status,attempts,error,next_attempt_at) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(trace_id) DO UPDATE SET status=excluded.status,attempts=excluded.attempts,error=excluded.error,next_attempt_at=excluded.next_attempt_at,updated_at=datetime('now')",params![id,if give_up{"failed"}else{"pending"},attempts,error,next])?;
        Ok(())
    }
    pub fn activity_done(&self, id: &str, entry: Option<i64>) -> anyhow::Result<()> {
        self.conn().execute("INSERT INTO activity_projections(trace_id,status,entry_id) VALUES(?1,'done',?2) ON CONFLICT(trace_id) DO UPDATE SET status='done',entry_id=excluded.entry_id,draft=NULL,error=NULL,next_attempt_at=NULL,updated_at=datetime('now')",params![id,entry])?;
        Ok(())
    }
}

// ── 4. worker ─────────────────────────────────────────────────────────────
#[derive(Clone)]
pub struct ActivityOptions {
    pub poll_interval: Duration,
    pub batch_size: usize,
    pub concurrency: usize,
    pub max_generation_attempts: u32,
    pub retry_base_ms: u64,
    pub retry_max_ms: u64,
    pub budget: ProjectionBudget,
}
impl Default for ActivityOptions {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(10),
            batch_size: 10,
            concurrency: 1,
            max_generation_attempts: 5,
            retry_base_ms: 30000,
            retry_max_ms: 3600000,
            budget: ProjectionBudget::default(),
        }
    }
}
pub struct ActivityWorker {
    pub db: Arc<AgentDb>,
    pub notes: Arc<dyn NoteGenerator>,
    pub mcp: SharedMcp,
    pub options: ActivityOptions,
    in_flight: Mutex<HashSet<String>>,
}
pub fn create_activity_worker(
    db: Arc<AgentDb>,
    notes: Arc<dyn NoteGenerator>,
    mcp: SharedMcp,
    options: ActivityOptions,
) -> anyhow::Result<Arc<ActivityWorker>> {
    anyhow::ensure!(
        options.batch_size > 0
            && options.concurrency > 0
            && options.max_generation_attempts > 0
            && !options.poll_interval.is_zero(),
        "activity journal limits must be positive"
    );
    Ok(Arc::new(ActivityWorker { db, notes, mcp, options, in_flight: Default::default() }))
}
struct Reservation<'a> {
    ids: &'a Mutex<HashSet<String>>,
    id: String,
}
impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        self.ids.lock().unwrap_or_else(|e| e.into_inner()).remove(&self.id);
    }
}
impl ActivityWorker {
    fn next_attempt(&self, attempts: u32, now: DateTime<Utc>) -> String {
        iso(now
            + chrono::TimeDelta::milliseconds(
                self.options.retry_base_ms.saturating_mul(1u64 << (attempts - 1).min(32)).min(self.options.retry_max_ms)
                    as i64,
            ))
    }
    pub async fn process_trace(
        &self,
        due: &DueTrace,
        cancel: &CancellationToken,
        now: DateTime<Utc>,
    ) -> anyhow::Result<&'static str> {
        check(cancel)?;
        {
            let mut ids = self.in_flight.lock().unwrap_or_else(|e| e.into_inner());
            if !ids.insert(due.trace_id.clone()) {
                return Ok("skipped");
            }
        }
        let _reservation = Reservation { ids: &self.in_flight, id: due.trace_id.clone() };
        let row = self.db.activity_progress(&due.trace_id)?;
        if row.as_ref().is_some_and(|r| r.status == "done" || r.status == "failed") {
            return Ok("skipped");
        }
        let attempts = row.as_ref().map_or(0, |r| r.attempts) + 1;
        let draft = if let Some(draft) = row.and_then(|r| r.draft) {
            draft
        } else {
            let generated = async {
                let (trace, observations) =
                    self.db.get_trace(&due.trace_id)?.ok_or_else(|| anyhow::anyhow!("trace not found"))?;
                let packet = project_trace(&trace, &observations, Some(&due.completed_at), &self.options.budget);
                let note = validate_note(&self.notes.generate(&packet, cancel).await?)
                    .map_err(|e| anyhow::anyhow!("invalid note: {e}"))?;
                check(cancel)?;
                Ok::<_, anyhow::Error>(render_draft(&packet, &note))
            }
            .await;
            match generated {
                Ok(draft) => {
                    self.db.save_activity_draft(&due.trace_id, &draft)?;
                    draft
                }
                Err(error) => {
                    check(cancel)?;
                    let give_up = attempts >= self.options.max_generation_attempts;
                    let next = if give_up { None } else { Some(self.next_attempt(attempts, now)) };
                    self.db.activity_failure(&due.trace_id, &error.to_string(), attempts, next.as_deref(), give_up)?;
                    eprintln!("[activity-journal] {} generation failed: {error}", due.trace_id);
                    return Ok(if give_up { "failed" } else { "retry" });
                }
            }
        };
        let appended=async{
            let raw=self.mcp.call_tool("journal_append",json!({"stream":ACTIVITY_STREAM,"body":draft.body,"metadata":draft.metadata,"capacity":ACTIVITY_STREAM_CAPACITY}).as_object().unwrap().clone(),cancel).await?;check(cancel)?;
            anyhow::ensure!(!is_tool_error(&raw),"{raw}");let parsed=parse_maybe_json(&json!(raw));anyhow::ensure!(parsed.get("ok")!=Some(&json!(false)),"journal_append refused: {} {}",text(&parsed["error"]),text(&parsed["message"]));
            Ok::<_,anyhow::Error>(if parsed["ok"]==true{parsed["entry"]["id"].as_i64()}else{None})
        }.await;
        match appended {
            Ok(entry) => {
                self.db.activity_done(&due.trace_id, entry)?;
                Ok("done")
            }
            Err(error) => {
                check(cancel)?;
                self.db.activity_failure(
                    &due.trace_id,
                    &error.to_string(),
                    attempts,
                    Some(&self.next_attempt(attempts, now)),
                    false,
                )?;
                eprintln!("[activity-journal] {} append failed; draft kept: {error}", due.trace_id);
                Ok("retry")
            }
        }
    }
    pub async fn tick(&self, cancel: &CancellationToken, now: DateTime<Utc>) -> anyhow::Result<usize> {
        check(cancel)?;
        let due = self
            .db
            .due_activity(self.options.batch_size, now)?
            .into_iter()
            .filter(|d| !self.in_flight.lock().unwrap_or_else(|e| e.into_inner()).contains(&d.trace_id))
            .collect::<Vec<_>>();
        let taken = due.len();
        let results: Vec<_> =
            stream::iter(due.into_iter().map(|d| async move { self.process_trace(&d, cancel, now).await }))
                .buffer_unordered(self.options.concurrency)
                .collect()
                .await;
        for result in results {
            result?;
        }
        Ok(taken)
    }
    pub async fn run(&self, cancel: CancellationToken) -> anyhow::Result<()> {
        eprintln!("[activity-journal] start stream={ACTIVITY_STREAM} cutoff={}", self.db.activity_cutoff(Utc::now())?);
        while !cancel.is_cancelled() {
            let taken = match self.tick(&cancel, Utc::now()).await {
                Ok(taken) => taken,
                Err(error) => {
                    if cancel.is_cancelled() {
                        break;
                    }
                    eprintln!("[activity-journal] tick failed: {error}");
                    0
                }
            };
            if taken >= self.options.batch_size {
                continue;
            }
            tokio::select! {_ = cancel.cancelled()=>break,_=tokio::time::sleep(self.options.poll_interval)=>{}}
        }
        eprintln!("[activity-journal] stopped");
        Ok(())
    }
}
