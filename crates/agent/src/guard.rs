// Guardrails for loop writes: compiled policy and host-owned evidence.
// Sections: 1. policy / verdicts; 2. deterministic rules; 3. round judgement;
// 4. tests.
use crate::{
    agent::{Engine, tool_effect},
    decisions::{DecisionRequest, choice},
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
    denied_tools: Vec<String>,
    reasons: HashMap<String, Reason>,
}
static POLICY: LazyLock<Policy> =
    LazyLock::new(|| serde_json::from_str(include_str!("../guard/reasons.json")).expect("compiled guard policy"));
const INSTRUCTIONS: &str = include_str!("../guard/instructions.md");
const MODEL_REASONS: &[&str] =
    &["allow", "off_request", "injected", "wrong_target", "data_leak", "destructive", "repeat", "needs_confirmation"];

#[derive(Clone, Debug, Serialize)]
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
    pub fn reply(&self) -> String {
        format!("[guard] blocked: {} — {}", self.label, self.guidance)
    }
}

// Set only from live loop admission or a durable run's admitted input, never
// from the arguments to a workflow task. Verdicts intentionally do not persist.
#[derive(Clone, Default)]
pub struct Context {
    pub task: String,
    pub root_request: String,
    pub default_chat: Option<String>,
}
#[derive(Default)]
pub struct State {
    pub context: Option<Context>,
    pub verdicts: HashMap<String, Verdict>,
}

pub fn key(call: &ToolCall) -> String {
    // Hash the exact argument bytes, including invalid JSON; a modified call
    // must be reviewed again even if it reuses a previously allowed id.
    let hash = Sha256::digest(serde_json::to_vec(&json!([call.name, call.arguments])).expect("call JSON"));
    format!("{}:{}", call.id, hex::encode(hash))
}
pub fn needs_guard(engine: &Engine, call: &ToolCall) -> bool {
    call.name != "invoke_sub_agent" && tool_effect(engine, &call.name) == owf::run::Effect::Write
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
fn has_secret(value: &Value) -> bool {
    match value {
        Value::String(s) => SECRET.is_match(s),
        Value::Array(items) => items.iter().any(has_secret),
        Value::Object(items) => items.values().any(has_secret),
        _ => false,
    }
}
fn deterministic(context: &Context, call: &ToolCall) -> Option<Verdict> {
    let reason = if POLICY.denied_tools.contains(&call.name) {
        "denied_tool"
    } else if call.name == "send_telegram_message"
        && !target(call, context).is_some_and(|id| {
            context.default_chat.as_ref() == Some(&id)
                || mentions(&context.task, &id)
                || mentions(&context.root_request, &id)
        })
    {
        "unknown_chat"
    } else if has_secret(&serde_json::from_str::<Value>(&call.arguments).unwrap_or_else(|_| json!(call.arguments))) {
        "secret_pattern"
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

// ── 3. round judgement ──────────────────────────────────────────────────────
pub async fn judge(
    engine: &Engine,
    context: &Context,
    calls: &[ToolCall],
    writes: &[Value],
    scope: &Scope,
    cancel: &CancellationToken,
) -> anyhow::Result<Vec<(String, Verdict)>> {
    check(cancel)?;
    let span = scope.span(SpanStart::new("guard", SpanKind::Chain));
    let child: Scope = span.clone();
    let mut verdicts = Vec::new();
    let mut pending = Vec::new();
    for call in calls.iter().filter(|c| needs_guard(engine, c)) {
        if let Some(verdict) = deterministic(context, call) {
            verdicts.push((key(call), verdict));
        } else {
            pending.push(call);
        }
    }
    if !pending.is_empty() {
        let options: Map<String, Value> =
            MODEL_REASONS.iter().map(|r| ((*r).into(), json!(POLICY.reasons[*r].when))).collect();
        let questions = pending
            .iter()
            .enumerate()
            .map(|(i, _)| choice(&format!("{INSTRUCTIONS}\nReview state.calls[{i}]."), options.clone()))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let mut request = DecisionRequest::new(
            json!({
                "task": context.task, "root_request": context.root_request,
                "calls": pending.iter().map(|c| json!({"id":c.id,"name":c.name,"arguments":c.arguments})).collect::<Vec<_>>(),
                "prior_writes": writes,
            }),
            child,
            cancel.clone(),
        );
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
                Ok(decision) => Verdict::new(&decision.answer(&format!("call_{i}"), &questions[i])?.choice),
                Err(error) => {
                    let mut verdict =
                        Verdict::new(if current_reply(context, call) { "allow" } else { "guard_unavailable" });
                    verdict.unavailable = Some(error.to_string());
                    verdict
                }
            };
            // Preserve the unavailable evidence even on a permitted reply.
            if verdict.allowed() && verdict.unavailable.is_some() {
                verdict.guidance = "Decision service unavailable; reply to the current conversation permitted.".into();
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
        let expected: Vec<_> = MODEL_REASONS
            .iter()
            .copied()
            .chain(["denied_tool", "unknown_chat", "secret_pattern", "guard_unavailable"])
            .collect();
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
        };
        for id in [json!(-100123), json!("42"), json!(99), Value::Null] {
            assert!(deterministic(&context, &call("send_telegram_message", json!({"chatId":id}))).is_none());
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
    fn verdict_key_binds_id_name_and_exact_arguments() {
        let a = call("write_doc", json!({"text":"a"}));
        let b = call("write_doc", json!({"text":"b"}));
        assert_ne!(key(&a), key(&b));
    }
}
