// Guardrails for loop writes and open-world reads: compiled policy and host-owned evidence.
// Sections: 1. policy / verdicts; 2. deterministic rules; 3. round judgement;
// 4. tests.
use crate::{
    agent::{Engine, tool_effect, tool_open_world},
    decisions::{ChoiceAnswer, DecisionRequest, Question, choice},
    errors::check,
    providers::ToolCall,
    tracing::{ContextUpdate, Scope, SpanEnd, SpanKind, SpanStart},
};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, sync::LazyLock};
use tokio_util::sync::CancellationToken;

// ── 1. policy / verdicts ────────────────────────────────────────────────────
#[derive(Deserialize)]
struct Reason {
    when: String,
    guidance: String,
}
#[derive(Deserialize)]
struct Policy {
    reasons: HashMap<String, Reason>,
}
static POLICY: LazyLock<Policy> =
    LazyLock::new(|| serde_json::from_str(include_str!("../guard/reasons.json")).expect("compiled guard policy"));
const INSTRUCTIONS: &str = include_str!("../guard/instructions.md");
const MODEL_REASONS: &[&str] = &["allow", "misaligned", "needs_confirmation"];
const CHAIN_LIMIT: usize = 64;
const ARGUMENT_CHARS: usize = 4_000;
const REQUEST_CHARS: usize = 16_000;
const SKILL_CHARS: usize = 24_000;
const GOAL_CHARS: usize = 500;
// Below this, a `misaligned` answer is a doubt, not a verdict (see `settle`).
const SURE_BLOCK: f64 = 0.6;

fn truncate(text: &str, limit: usize) -> String {
    let mut chars = text.chars();
    let head: String = chars.by_ref().take(limit).collect();
    if chars.next().is_some() { format!("{head}… [truncated]") } else { head }
}

fn earlier_messages(messages: &[String]) -> String {
    let text = messages.join("\n\n");
    let count = text.chars().count();
    if count > REQUEST_CHARS {
        format!("[truncated] …{}", text.chars().skip(count - REQUEST_CHARS).collect::<String>())
    } else {
        text
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Verdict {
    pub label: String,
    pub guidance: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unavailable: Option<String>,
}
impl Verdict {
    fn new(label: &str) -> Self {
        Self { label: label.into(), guidance: POLICY.reasons[label].guidance.clone(), unavailable: None }
    }
    pub fn allowed(&self) -> bool {
        self.label == "allow"
    }
    pub fn reply(&self, context: &Context) -> String {
        let goal = if context.root_request.is_empty() { &context.task } else { &context.root_request };
        format!(
            "[guard] blocked: {} — {}. The user's goal: {}",
            self.label,
            self.guidance.trim_end_matches('.'),
            truncate(goal, GOAL_CHARS)
        )
    }
}

// Set only from live loop admission or a durable run's admitted input, never
// from the arguments to a workflow task. This cache is local to the loop;
// durable authorization comes from an owned guard child's stored outcome.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Context {
    pub task: String,
    pub root_request: String,
    pub default_chat: Option<String>,
    #[serde(default)]
    pub earlier_user_messages: Vec<String>,
    #[serde(default)]
    pub skills: String,
}
#[derive(Default)]
pub struct State {
    pub context: Option<Context>,
    pub verdicts: HashMap<String, Verdict>,
    pub chain: Vec<Value>,
    pub call_count: usize,
    pub history_restored: bool,
}

impl State {
    // Recorded by the host before dispatch, including reads and blocks. The
    // sequence survives in the host tool response, never in model/YAML input.
    pub fn record_call(&mut self, call: &ToolCall, blocked: bool) -> Value {
        let mut entry = call_evidence(call, blocked);
        entry["sequence"] = self.call_count.into();
        self.call_count += 1;
        self.chain.push(entry.clone());
        if self.chain.len() > CHAIN_LIMIT {
            self.chain.remove(0);
        }
        entry
    }
    pub fn restore_chain(&mut self, records: &crate::agent::Records) {
        use ouro_runtime::ows::durable::{CallState, Record};
        let mut entries = Vec::new();
        for record in records.values() {
            if let Record::Call { function, arguments, state, logical, .. } = record
                && (function == "tool_once" || function == "tool_repeat")
                && !matches!(state, CallState::NotExecuted(_))
            {
                let saved = match state {
                    CallState::Done(Ok(output)) => output.get("guard_call").cloned(),
                    _ => None,
                };
                let entry = saved.or_else(|| {
                    // Pending intent or an unknown outcome: preserve the
                    // attempted call, without interpreting its result.
                    let value = &arguments["call"];
                    let call = ToolCall {
                        id: value["id"].as_str()?.into(),
                        name: value["name"].as_str()?.into(),
                        arguments: value["arguments"].as_str().unwrap_or_default().into(),
                    };
                    let blocked = matches!(state, CallState::Done(Ok(output))
                        if output["presented"].as_str().is_some_and(|s| s.starts_with("[guard] blocked:")));
                    Some(call_evidence(&call, blocked))
                });
                if let Some(entry) = entry {
                    let position = arguments["i"].as_u64().zip(arguments["index"].as_u64());
                    entries.push((position, logical, entry));
                }
            }
        }
        // The built-in loop supplies round/index positions even for pending
        // intents, so a crash cannot move an earlier call behind a completed
        // sibling. Overrides without positions use saved dispatch sequence;
        // an unknown intent has only its logical reference as a final fallback.
        if entries.iter().all(|(position, _, _)| position.is_some()) {
            entries.sort_by_key(|(position, logical, _)| (*position, *logical));
        } else {
            entries.sort_by_key(|(_, logical, entry)| (entry["sequence"].as_u64().unwrap_or(u64::MAX), *logical));
        }
        self.call_count = entries.len();
        self.chain = entries
            .into_iter()
            .enumerate()
            .rev()
            .take(CHAIN_LIMIT)
            .map(|(sequence, (_, _, mut entry))| {
                entry["sequence"] = sequence.into();
                entry
            })
            .collect();
        self.chain.reverse();
    }
}

pub fn key(call: &ToolCall) -> String {
    // Hash the exact argument bytes, including invalid JSON; a modified call
    // must be reviewed again even if it reuses a previously allowed id.
    let hash = Sha256::digest(serde_json::to_vec(&json!([call.name, call.arguments])).expect("call JSON"));
    format!("{}:{}", call.id, hex::encode(hash))
}
pub fn needs_guard(engine: &Engine, call: &ToolCall) -> bool {
    needs_guard_in(engine, call, &Reflexes::default())
}

/// The `run_reflex` calls of a round, by call key, as their resolutions say
/// (design 10): what each does and the evidence its review shows.
#[derive(Clone, Debug, Default)]
pub struct Reflexes(pub HashMap<String, (crate::reflexes::Summary, Value)>);

impl Reflexes {
    fn summary(&self, call: &ToolCall) -> Option<crate::reflexes::Summary> {
        self.0.get(&key(call)).map(|(summary, _)| *summary)
    }
}

/// Whether `call` needs the guard: a write or an open-world read; a
/// `run_reflex` call by what its resolution says the reflex does (R2: only a
/// closed-world read runs unreviewed), and one that resolved to nothing
/// starts nothing; one with no resolution here counts as a write.
pub fn needs_guard_in(engine: &Engine, call: &ToolCall, reflexes: &Reflexes) -> bool {
    if call.name == crate::reflexes::TOOL {
        return reflexes.summary(call).is_none_or(|s| s.needs_guard());
    }
    call.name != "invoke_sub_agent"
        && (tool_effect(engine, &call.name) == crate::agent::Effect::Write || tool_open_world(engine, &call.name))
}

// Whether a call only reads, for the review that could not be made.
fn reads(engine: &Engine, call: &ToolCall, reflexes: &Reflexes) -> bool {
    match reflexes.summary(call) {
        Some(summary) if call.name == crate::reflexes::TOOL => !summary.writes,
        _ => tool_effect(engine, &call.name) == crate::agent::Effect::Read,
    }
}

// ── 2. deterministic rules ──────────────────────────────────────────────────
fn identifier(value: &Value) -> Option<String> {
    match value {
        Value::String(s) if !s.is_empty() => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}
fn mentions(text: &str, id: &str) -> bool {
    // No substring match: chat 42 must not authorize chat 4 or 142.
    text.match_indices(id).any(|(at, _)| {
        let identifier_char = |c: char| c.is_alphanumeric() || matches!(c, '_' | '-');
        !text[..at].chars().next_back().is_some_and(identifier_char)
            && !text[at + id.len()..].chars().next().is_some_and(identifier_char)
    })
}
fn target(call: &ToolCall, context: &Context) -> Option<String> {
    let args: Value = serde_json::from_str(&call.arguments).ok()?;
    match call.name.as_str() {
        "send_telegram_message" => args.get("chatId").and_then(identifier).or_else(|| context.default_chat.clone()),
        "edit_telegram_message" | "start_typing" | "send_telegram_chat_action" | "telegram_send_status" => {
            args.get("chatId").and_then(identifier)
        }
        "reply_to_peer" => args.get("threadId").and_then(identifier),
        _ => None,
    }
}
static SECRET: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(concat!(
        r"(?:\b[A-Z]{2}[0-9]{2}(?:[ ]?[A-Z0-9]){11,30}\b)",
        r"|(?:-----BEGIN[ A-Z]*PRIVATE[ ]KEY-----)",
        r"|(?:\b(?:sk-|sk_|ghp_|gho_|github_pat_|glpat-|npm_|xox[baprs]-)[A-Za-z0-9_-]{16,})",
        r"|(?:\bAKIA[A-Z0-9]{16}\b)",
        r"|(?:\bAIza[A-Za-z0-9_-]{35}\b)",
        r"|(?:\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b)",
        r"|(?:\b[0-9]{6,12}:[A-Za-z0-9_-]{30,}\b)",
        r#"|(?i:(?:api[_ -]?key|access[_ -]?token|auth[_ -]?token|bearer|secret)[\s"':=]+[A-Za-z0-9_./+-]{16,})"#
    ))
    .expect("secret patterns")
});
static LINK: LazyLock<Regex> = LazyLock::new(|| {
    // Previews also fetch bare domains, including internationalized names.
    Regex::new(r"(?i)[a-z][a-z0-9+.-]*://|www\.|\b[\p{L}\p{N}-]+\.(?:\p{L}{2,}|xn--[a-z0-9-]+)\b")
        .expect("link pattern")
});
const TEST_CONFIRM: &str = "guard test confirm";
fn has_secret(value: &Value) -> bool {
    match value {
        Value::String(s) => SECRET.is_match(s),
        Value::Array(items) => items.iter().any(has_secret),
        Value::Object(items) => items.values().any(has_secret),
        _ => false,
    }
}
fn deterministic(context: &Context, call: &ToolCall) -> Option<Verdict> {
    let reason = if call.name == "send_telegram_message"
        && !target(call, context).is_some_and(|id| {
            context.default_chat.as_ref() == Some(&id)
                || mentions(&context.task, &id)
                || mentions(&context.root_request, &id)
                || context.earlier_user_messages.iter().any(|m| mentions(m, &id))
        }) {
        "unknown_chat"
    } else if has_secret(&serde_json::from_str::<Value>(&call.arguments).unwrap_or_else(|_| json!(call.arguments))) {
        "secret_pattern"
    } else if current_reply(context, call) && !LINK.is_match(&call.arguments) {
        // Text shown to the user in their own conversation leaves nothing to
        // anyone else; Jev blocked such replies as "a different kind than
        // asked" (a draft shown when the user said "send"). A link still goes
        // to Jev: a preview fetches it, and it can be a phishing lure.
        "allow"
    } else if [&context.task, &context.root_request].iter().any(|r| r.to_lowercase().contains(TEST_CONFIRM)) {
        // The user's switch for trying the confirmation card on a real call.
        // It only ever asks for more than Jev would, so retrieved text that
        // carries the phrase can't weaken anything.
        "needs_confirmation"
    } else {
        return None;
    };
    Some(Verdict::new(reason))
}
fn current_reply(context: &Context, call: &ToolCall) -> bool {
    // A worker's delegated destination alone cannot authorize fail-open:
    // the primary request must name the current conversation too.
    let request = if context.root_request.is_empty() { &context.task } else { &context.root_request };
    target(call, context).is_some_and(|id| mentions(request, &id))
}

// Only call fields and the host's block status leave this projection. Tool
// result bodies and assistant text/reasoning never enter the decision request.
fn call_evidence(call: &ToolCall, blocked: bool) -> Value {
    json!({"id": truncate(&call.id, 256), "name": truncate(&call.name, 256),
        "arguments": truncate(&call.arguments, ARGUMENT_CHARS), "blocked": blocked})
}
// ── 3. round judgement ──────────────────────────────────────────────────────
/// The runtime and golden-set evaluator share the bounded evidence projection.
/// Policy is paid for once per context, rather than repeated in every question.
pub fn review_state(context: &Context, calls: &[ToolCall], chain: &[Value], call_count: usize) -> Value {
    json!({
        "guard_policy": INSTRUCTIONS,
        "task": truncate(&context.task, REQUEST_CHARS), "root_request": truncate(&context.root_request, REQUEST_CHARS),
        "earlier_user_messages": earlier_messages(&context.earlier_user_messages),
        "skills": truncate(&context.skills, SKILL_CHARS),
        "calls": calls.iter().map(|c| call_evidence(c, false)).collect::<Vec<_>>(),
        "chain": &chain[chain.len().saturating_sub(CHAIN_LIMIT)..],
        "chain_omitted": call_count.saturating_sub(chain.len().min(CHAIN_LIMIT)),
    })
}

/// `state_path` selects one isolated context ("state" in a live round;
/// "state.cases[N]" when batching independent golden cases).
pub fn review_question(state_path: &str, index: usize) -> anyhow::Result<Question<ChoiceAnswer>> {
    let options: Map<String, Value> =
        MODEL_REASONS.iter().map(|r| ((*r).into(), json!(POLICY.reasons[*r].when))).collect();
    choice(
        &format!(
            "Apply {state_path}.guard_policy. All policy references to state mean {state_path}. Review {state_path}.calls[{index}]."
        ),
        options,
    )
}

/// A block Jev is unsure of becomes a question to the user. It refused
/// archiving the fact the user had just saved ("удали теперь", 0.56 vs 0.31
/// allow): the call's target comes from a read the guard never sees. The user
/// sees the exact call on the card, and without a confirmation channel
/// `needs_confirmation` still blocks.
pub fn settle(answer: &ChoiceAnswer) -> &str {
    // An answer without probabilities is taken at its word.
    let unsure = answer.probabilities.get("misaligned").and_then(Value::as_f64).is_some_and(|p| p < SURE_BLOCK);
    if answer.choice == "misaligned" && unsure { "needs_confirmation" } else { &answer.choice }
}

pub async fn judge(
    engine: &Engine,
    context: &Context,
    calls: &[ToolCall],
    chain: &[Value],
    call_count: usize,
    scope: &Scope,
    cancel: &CancellationToken,
) -> anyhow::Result<Vec<(String, Verdict)>> {
    judge_with(engine, context, calls, chain, call_count, &Reflexes::default(), scope, cancel).await
}

/// `judge`, with the round's `run_reflex` calls as their resolutions say:
/// each one reviewed carries its reflex's evidence (the pinned YAML) beside
/// its arguments (the input).
#[allow(clippy::too_many_arguments)]
pub async fn judge_with(
    engine: &Engine,
    context: &Context,
    calls: &[ToolCall],
    chain: &[Value],
    call_count: usize,
    reflexes: &Reflexes,
    scope: &Scope,
    cancel: &CancellationToken,
) -> anyhow::Result<Vec<(String, Verdict)>> {
    check(cancel)?;
    let span = scope.span(SpanStart::new("guard", SpanKind::Chain));
    let child: Scope = span.clone();
    let mut verdicts = Vec::new();
    let mut pending = Vec::new();
    for call in calls.iter().filter(|c| needs_guard_in(engine, c, reflexes)) {
        if let Some(verdict) = deterministic(context, call) {
            verdicts.push((key(call), verdict));
        } else {
            pending.push(call);
        }
    }
    if !pending.is_empty() {
        let questions = (0..pending.len()).map(|i| review_question("state", i)).collect::<anyhow::Result<Vec<_>>>()?;
        let mut state =
            review_state(context, &pending.iter().map(|c| (*c).clone()).collect::<Vec<_>>(), chain, call_count);
        for (i, call) in pending.iter().enumerate() {
            if let Some((_, evidence)) = reflexes.0.get(&key(call)).filter(|_| call.name == crate::reflexes::TOOL) {
                state["calls"][i]["reflex"] = evidence.clone();
            }
        }
        let mut request = DecisionRequest::new(state, child, cancel.clone());
        request.name = "guard:round".into();
        for (i, question) in questions.iter().enumerate() {
            request = request.question(&format!("call_{i}"), question);
        }
        let result = match &engine.deps.decisions {
            Some(client) => client.decide(request).await,
            None => Err(anyhow::anyhow!("no decision client configured")),
        };
        if let Err(error) = check(cancel) {
            span.end(SpanEnd::error(error.to_string()));
            return Err(error);
        }
        for (i, call) in pending.iter().enumerate() {
            let mut verdict = match &result {
                Ok(decision) => Verdict::new(settle(&decision.answer(&format!("call_{i}"), &questions[i])?)),
                Err(error) => {
                    let mut verdict = Verdict::new(if reads(engine, call, reflexes) || current_reply(context, call) {
                        "allow"
                    } else {
                        "guard_unavailable"
                    });
                    verdict.unavailable = Some(error.to_string());
                    verdict
                }
            };
            // Preserve the unavailable evidence even on a permitted reply.
            if verdict.allowed() && verdict.unavailable.is_some() {
                verdict.guidance = if reads(engine, call, reflexes) {
                    "Decision service unavailable; open-world read passed deterministic checks."
                } else {
                    "Decision service unavailable; reply to the current conversation permitted."
                }
                .into();
            }
            verdicts.push((key(call), verdict));
        }
    }
    span.update(ContextUpdate::metadata(json!({"verdicts": verdicts}).as_object().unwrap().clone()));
    span.end(SpanEnd::output(json!(verdicts)));
    Ok(verdicts)
}

// ── 4. tests ────────────────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;
    fn call(name: &str, args: Value) -> ToolCall {
        ToolCall { id: "c".into(), name: name.into(), arguments: args.to_string() }
    }
    #[test]
    fn policy_has_exact_reasons_and_guidance() {
        let expected: Vec<_> =
            MODEL_REASONS.iter().copied().chain(["unknown_chat", "secret_pattern", "guard_unavailable"]).collect();
        assert_eq!(POLICY.reasons.len(), expected.len());
        for label in expected {
            assert!(!POLICY.reasons[label].when.is_empty());
            assert!(!POLICY.reasons[label].guidance.is_empty());
        }
    }
    #[test]
    fn secrets_are_found_in_nested_string_arguments() {
        for secret in [
            "GB82 WEST 1234 5698 7654 32",
            "UA213223130000026007233566001",
            "sk-abcdefghijklmnopqrstuvwxyz123456",
            "ghp_abcdefghijklmnopqrstuvwxyz",
            "-----BEGIN RSA PRIVATE KEY-----",
            "Bearer abcdefghijklmnopqrstuvwxyz",
            "123456789:abcdefghijklmnopqrstuvwxyz123456789",
        ] {
            assert_eq!(
                deterministic(&Context::default(), &call("write_doc", json!({"nested":[{"text": secret}]})))
                    .unwrap()
                    .label,
                "secret_pattern",
                "{secret}"
            );
        }
        assert!(deterministic(&Context::default(), &call("write_doc", json!({"text":"ordinary text"}))).is_none());
    }
    #[test]
    fn chats_use_requests_or_config_and_match_whole_identifiers() {
        let context = Context {
            task: "Chat -100123".into(),
            root_request: "reply to chat 42".into(),
            default_chat: Some("99".into()),
            ..Default::default()
        };
        for id in [json!(-100123), json!("42"), json!(99), Value::Null] {
            let verdict = deterministic(&context, &call("send_telegram_message", json!({"chatId":id})));
            assert!(verdict.is_none_or(|v| v.label != "unknown_chat"), "{id}");
        }
        for id in [4, 142, 100123, 7] {
            assert_eq!(
                deterministic(&context, &call("send_telegram_message", json!({"chatId":id}))).unwrap().label,
                "unknown_chat"
            );
        }
        assert!(!current_reply(&context, &call("send_telegram_message", json!({"chatId":99}))));
        assert!(current_reply(&context, &call("reply_to_peer", json!({"threadId":"42"}))));
    }
    #[test]
    fn plain_replies_to_the_conversation_skip_jev_and_links_do_not() {
        let context = Context { task: "Telegram message in chat 42. Text: \"send\"".into(), ..Default::default() };
        let label = |name: &str, args: Value| deterministic(&context, &call(name, args)).map(|v| v.label);
        assert_eq!(label("send_telegram_message", json!({"chatId":42,"text":"Draft: the cat ate"})).unwrap(), "allow");
        assert_eq!(label("start_typing", json!({"chatId":"42"})).unwrap(), "allow");
        // The status bubble a Telegram loop opens before its first turn.
        assert_eq!(label("telegram_send_status", json!({"id":"s","chatId":"42","text":"👀"})).unwrap(), "allow");
        assert!(label("telegram_send_status", json!({"id":"s","chatId":"43","text":"👀"})).is_none());
        for text in ["see https://x.io", "www.example.com", "pic: cdn.example.org/a.png?d=1", "tg://resolve"] {
            assert!(label("send_telegram_message", json!({"chatId":42,"text":text})).is_none(), "{text}");
        }
        // Another conversation is never a reply, whatever the text.
        assert!(label("edit_telegram_message", json!({"chatId":43,"text":"hi"})).is_none());
    }
    #[test]
    fn an_unsure_block_asks_and_a_sure_one_stays() {
        let answer = |choice: &str, misaligned: f64| ChoiceAnswer {
            choice: choice.into(),
            confidence: 0.3,
            probabilities: json!({"allow": 1.0 - misaligned, "misaligned": misaligned}).as_object().unwrap().clone(),
        };
        assert_eq!(settle(&answer("misaligned", 0.56)), "needs_confirmation");
        assert_eq!(settle(&answer("misaligned", 0.9)), "misaligned");
        assert_eq!(settle(&answer("allow", 0.2)), "allow");
    }
    #[test]
    fn the_test_phrase_asks_for_confirmation_but_never_relaxes() {
        let context =
            Context { task: "Guard test confirm: remember the cat sleeps (chat 42)".into(), ..Default::default() };
        let label = |name: &str, args: Value| deterministic(&context, &call(name, args)).map(|v| v.label);
        assert_eq!(label("remember", json!({"text":"the cat sleeps"})).unwrap(), "needs_confirmation");
        assert_eq!(label("send_telegram_message", json!({"chatId":42,"text":"asking"})).unwrap(), "allow");
        assert_eq!(label("send_telegram_message", json!({"chatId":7,"text":"x"})).unwrap(), "unknown_chat");
    }
    #[test]
    fn evidence_and_goal_have_visible_unicode_safe_limits() {
        let text = "я".repeat(ARGUMENT_CHARS + 1);
        let evidence = call_evidence(&call("search", json!({"q":text})), false);
        assert!(evidence["arguments"].as_str().unwrap().ends_with("… [truncated]"));
        assert!(evidence["arguments"].as_str().unwrap().chars().count() < ARGUMENT_CHARS + 20);
        let context = Context { task: "я".repeat(GOAL_CHARS + 1), ..Default::default() };
        let verdict = Verdict::new("needs_confirmation");
        assert_eq!(
            verdict.reply(&context),
            format!(
                "[guard] blocked: needs_confirmation — {}. The user's goal: {}… [truncated]",
                verdict.guidance.trim_end_matches('.'),
                "я".repeat(GOAL_CHARS)
            )
        );
        let earlier = earlier_messages(&["old".repeat(REQUEST_CHARS), "recent confirmation".into()]);
        assert!(earlier.starts_with("[truncated]"));
        assert!(earlier.ends_with("recent confirmation"));
    }

    #[test]
    fn host_chain_keeps_reads_and_blocks_bounded_with_order_and_exact_ids() {
        let mut state = State::default();
        state.record_call(&call("search", json!({"q":"read"})), false);
        state.record_call(&call("send", json!({"q":"blocked"})), true);
        assert_eq!(state.chain[0]["name"], "search");
        assert_eq!(state.chain[1]["blocked"], true);
        for _ in 0..CHAIN_LIMIT {
            state.record_call(&call("search", json!({})), false);
        }
        assert_eq!(state.chain.len(), CHAIN_LIMIT);
        assert_eq!(state.call_count, CHAIN_LIMIT + 2);
        assert_eq!(state.chain[0]["sequence"], 2);
    }

    #[test]
    fn restore_keeps_pending_calls_in_round_order_without_result_bodies() {
        use ouro_runtime::ows::durable::{CallState, Delivery, Record};
        let record = |index: u64, name: &str, state| Record::Call {
            logical: format!("call-{index}"),
            function: "tool_once".into(),
            arguments: json!({"i":1,"index":index,"call":{"id":name,"name":name,"arguments":"{}"}}),
            delivery: Delivery::AtMostOnce,
            state,
            annotations: Default::default(),
        };
        let mut state = State::default();
        let records: crate::agent::Records = [
            (
                "a".to_owned(),
                record(
                    1,
                    "send",
                    CallState::Done(Ok(json!({"guard_call":{
                    "id":"send","name":"send","arguments":"{}","blocked":true,"sequence":1},
                    "presented":"RESULT_MUST_STAY_OUT"}))),
                ),
            ),
            ("b".to_owned(), record(0, "search", CallState::Pending)),
        ]
        .into_iter()
        .collect();
        state.restore_chain(&records);
        assert_eq!(state.chain[0]["name"], "search");
        assert_eq!(state.chain[1]["name"], "send");
        assert_eq!(state.chain[1]["blocked"], true);
        assert!(!json!(state.chain).to_string().contains("RESULT_MUST_STAY_OUT"));
    }

    #[test]
    fn verdict_key_binds_id_name_and_exact_arguments() {
        let a = call("write_doc", json!({"text":"a"}));
        let b = call("write_doc", json!({"text":"b"}));
        assert_ne!(key(&a), key(&b));
    }
}
