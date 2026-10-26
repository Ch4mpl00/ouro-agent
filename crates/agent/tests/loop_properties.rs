// Port of test/agent-loop.test.ts: the transient-failure retry policy over
// generated failure sequences, and the AgentLoop's conversation protocol over
// generated model turns (tool calls that succeed, fail, throw, carry broken
// arguments or name tools the loop may not use).
mod support;
use agent::{
    agent::LoopOptions,
    providers::{ApiError, Message, RetryInfo, RetryOpts, assistant, retry_on_transient, role, tool_calls},
};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use serde_json::{Map, Value, json};
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

fn runs() -> u32 {
    std::env::var("PROPERTY_RUNS").ok().and_then(|v| v.parse().ok()).unwrap_or(150)
}

// ── retry ───────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug)]
enum Outcome {
    Ok,
    // None: no HTTP status, a connection failure.
    Transient(Option<u16>),
    Permanent(u16),
    Plain,
}

fn outcome() -> impl Strategy<Value = Outcome> {
    prop_oneof![
        2 => Just(Outcome::Ok),
        4 => prop::sample::select(vec![Some(408), Some(429), Some(500), Some(502), Some(503), Some(529), None])
            .prop_map(Outcome::Transient),
        1 => prop::sample::select(vec![400u16, 401, 403, 404, 422]).prop_map(Outcome::Permanent),
        1 => Just(Outcome::Plain),
    ]
}

fn raise(o: Outcome) -> anyhow::Error {
    let api = |status: u16| ApiError::Status { status, message: format!("status {status}") }.into();
    match o {
        Outcome::Plain => anyhow::anyhow!("not an API error"),
        Outcome::Permanent(status) | Outcome::Transient(Some(status)) => api(status),
        Outcome::Transient(None) => ApiError::Connection("offline".into()).into(),
        Outcome::Ok => unreachable!(),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(runs()))]
    #[test]
    fn retries_exactly_the_transient_failures_within_the_budget_with_capped_exponential_backoff(
        max_retries in 0u32..=4,
        script in prop::collection::vec(outcome(), 6..=6),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let mut calls = 0;
        let mut retries: Vec<RetryInfo> = Vec::new();
        let settled = runtime.block_on(retry_on_transient(
            &RetryOpts { max_retries, base_delay_ms: 1, max_delay_ms: 4, jitter: false },
            &CancellationToken::new(),
            |info| retries.push(info.clone()),
            || {
                let i = calls;
                calls += 1;
                let o = script[i];
                async move { if let Outcome::Ok = o { Ok(format!("answer {i}")) } else { Err(raise(o)) } }
            },
        ));
        // From the script: stop at the first success or non-transient
        // failure, or when the retry budget is spent.
        let stop = script.iter().position(|o| !matches!(o, Outcome::Transient(_)));
        let want = stop.map_or(usize::MAX, |s| s + 1).min(max_retries as usize + 1);
        let at = format!("maxRetries={max_retries} script={script:?}");
        prop_assert_eq!(calls, want, "{}: attempts", at);
        match script[want - 1] {
            Outcome::Ok => prop_assert_eq!(settled.ok(), Some(format!("answer {}", want - 1)), "{}", at),
            last => prop_assert_eq!(
                settled.err().map(|e| e.to_string()),
                Some(raise(last).to_string()),
                "{}: the last failure itself is thrown",
                at
            ),
        }
        prop_assert_eq!(retries.iter().map(|r| r.attempt).collect::<Vec<_>>(), (1..want as u32).collect::<Vec<_>>(), "{}: one retry note per retried attempt", at);
        prop_assert_eq!(retries.iter().map(|r| r.delay_ms).collect::<Vec<_>>(), (0..retries.len()).map(|i| (1u64 << i).min(4)).collect::<Vec<_>>(), "{}: backoff", at);
        for (i, r) in retries.iter().enumerate() {
            let want_status = if let Outcome::Transient(status) = script[i] { status } else { None };
            prop_assert_eq!(r.status, want_status, "{}: status of attempt {}", at, i + 1);
        }
    }
}

// ── the AgentLoop's conversation protocol ───────────────────────────────────

// MCP has four tools; the loop may use three of them.
const MCP_TOOLS: [&str; 4] = ["t_ok", "t_err", "t_throw", "t_hidden"];
const ALLOWED: [&str; 3] = ["t_ok", "t_err", "t_throw"];

#[derive(Clone, Debug)]
enum Turn {
    Answer(String),
    // (tool name, raw `arguments` text as a model would send it)
    Calls(Vec<(String, String)>),
}

fn turn() -> impl Strategy<Value = Turn> {
    let name = prop::sample::select(vec!["t_ok", "t_err", "t_throw", "t_hidden", "t_ok", "t_ok", "no_such_tool"]);
    let args = prop_oneof![
        6 => ".{0,20}".prop_map(|q| json!({"q": q}).to_string()),
        1 => prop::sample::select(vec!["", "{}", "{broken", "null", "[1,2]", "\"text\""]).prop_map(str::to_owned),
    ];
    prop_oneof![
        3 => prop::collection::vec((name, args), 1..=4)
            .prop_map(|calls| Turn::Calls(calls.into_iter().map(|(n, a)| (n.to_owned(), a)).collect())),
        1 => ".{0,20}".prop_map(Turn::Answer),
    ]
}

fn message(turn: &Turn, at: usize) -> Message {
    match turn {
        Turn::Answer(text) => assistant(text),
        Turn::Calls(calls) => json!({
            "role": "assistant",
            "content": null,
            "tool_calls": calls.iter().enumerate().map(|(i, (name, args))| json!({
                "id": format!("call_{}_{i}", at + 1),
                "type": "function",
                "function": {"name": name, "arguments": args},
            })).collect::<Vec<_>>(),
        })
        .as_object()
        .unwrap()
        .clone(),
    }
}

// Arguments the loop must hand to a tool: a JSON object; empty means {}.
fn parsed_args(raw: &str) -> Option<Map<String, Value>> {
    serde_json::from_str::<Value>(if raw.is_empty() { "{}" } else { raw }).ok().and_then(|v| v.as_object().cloned())
}

async fn exercise(
    script: Vec<Turn>,
    max: usize,
    witnessed: &Mutex<BTreeSet<&'static str>>,
) -> Result<(), TestCaseError> {
    let replies: Vec<_> = script.iter().enumerate().map(|(i, t)| message(t, i)).collect();
    let model = Arc::new(support::Scripted::new(replies.clone()));
    let f = support::fixture(model.clone(), &MCP_TOOLS, &[("s", "---\ntools: *\n---\nskill\n")]);
    let mut options = LoopOptions::new("loop", support::context());
    options.skills = vec!["s".into()];
    options.tool_whitelist = Some(ALLOWED.into_iter().map(str::to_owned).collect());
    options.max_iterations = max;
    let loop_ = f.engine.start_loop(options).await.map_err(|e| TestCaseError::fail(e.to_string()))?;
    let settled = loop_.send("start".into()).await;
    f.engine.end_loop("loop").await;
    f.engine.shutdown().await;
    let requests: Vec<Vec<Message>> = model.seen.lock().unwrap().iter().map(|s| s.messages.clone()).collect();
    let dispatched = f.mcp.called.lock().unwrap().clone();
    let at = format!("maxIterations={max} script={script:?}");

    // Termination, from the script: the first answer within the budget.
    let answer_at = script.iter().position(|t| matches!(t, Turn::Answer(_)));
    let want = match answer_at {
        Some(a) if a < max => a + 1,
        _ => max.min(script.len() + 1),
    };
    prop_assert_eq!(requests.len(), want.min(script.len() + 1), "{}: model calls", at);
    if let Some(a) = answer_at.filter(|a| *a < max) {
        let Turn::Answer(text) = &script[a] else { unreachable!() };
        prop_assert_eq!(settled.as_ref().ok(), Some(text), "{}", at);
        witnessed.lock().unwrap().insert("answers");
    } else {
        prop_assert!(settled.is_err(), "{}: no answer in budget → the loop fails", at);
        witnessed.lock().unwrap().insert(if max <= script.len() { "runs out of iterations" } else { "model fails" });
    }

    // Every request is a valid conversation: each assistant tool_calls
    // message is followed by one tool reply per call, same ids, same order.
    for (k, msgs) in requests.iter().enumerate() {
        for (i, m) in msgs.iter().enumerate() {
            if role(m) == "tool" {
                let owner = msgs[..i].iter().rev().find(|x| role(x) != "tool");
                prop_assert_eq!(
                    owner.map(role),
                    Some("assistant"),
                    "{}: request {}: a tool reply follows an assistant call",
                    at,
                    k
                );
            }
            let calls = tool_calls(m);
            if role(m) != "assistant" || calls.is_empty() {
                continue;
            }
            let answered: Vec<String> = msgs[i + 1..(i + 1 + calls.len()).min(msgs.len())]
                .iter()
                .map(|x| {
                    if role(x) == "tool" {
                        x["tool_call_id"].as_str().unwrap().to_owned()
                    } else {
                        format!("<{}>", role(x))
                    }
                })
                .collect();
            prop_assert_eq!(
                answered,
                calls.iter().map(|c| c.id.clone()).collect::<Vec<_>>(),
                "{}: request {}: replies to message {}",
                at,
                k,
                i
            );
        }
        // Append-only: the next request is this one, the model's reply, and
        // the tool replies — nothing earlier rewritten.
        if let Some(next) = requests.get(k + 1) {
            prop_assert_eq!(&next[..msgs.len()], msgs.as_slice(), "{}: request {} rewrote history", at, k + 1);
            prop_assert_eq!(&next[msgs.len()], &replies[k], "{}: the model's own message comes back as sent", at);
        }
    }

    // MCP sees exactly the allowed calls with object arguments, with the
    // arguments the model sent, round after round. Every reply's calls ran:
    // the loop only stops after replying to them. Within a round the calls
    // run at once (each an engine call of its own), so their start order is
    // not the model's: a round is compared as a multiset.
    let sent: Vec<_> = replies.iter().take(requests.len()).flat_map(tool_calls).collect();
    let mut rest = dispatched.as_slice();
    for (k, reply) in replies.iter().take(requests.len()).enumerate() {
        let mut want: Vec<_> = tool_calls(reply)
            .iter()
            .filter(|c| ALLOWED.contains(&c.name.as_str()))
            .filter_map(|c| parsed_args(&c.arguments).map(|a| (c.name.clone(), a)))
            .collect();
        prop_assert!(rest.len() >= want.len(), "{}: round {}: dispatched {:?}", at, k, dispatched);
        let (round, later) = rest.split_at(want.len());
        let mut got = round.to_vec();
        let key = |c: &(String, Map<String, Value>)| format!("{}{}", c.0, Value::Object(c.1.clone()));
        got.sort_by_key(key);
        want.sort_by_key(key);
        prop_assert_eq!(got, want, "{}: round {}: dispatched calls", at, k);
        rest = later;
    }
    prop_assert!(rest.is_empty(), "{}: calls past the last round: {:?}", at, rest);
    if sent.iter().any(|c| !ALLOWED.contains(&c.name.as_str())) {
        witnessed.lock().unwrap().insert("refuses a tool outside the loop");
    }
    if sent.iter().any(|c| parsed_args(&c.arguments).is_none()) {
        witnessed.lock().unwrap().insert("refuses broken arguments");
    }
    Ok(())
}

#[test]
fn sends_a_valid_append_only_conversation_and_dispatches_exactly_the_allowed_calls() {
    let witnessed = Mutex::new(BTreeSet::new());
    let mut runner = TestRunner::new(Config { cases: runs(), ..Config::default() });
    runner
        .run(&(prop::collection::vec(turn(), 1..=6), 1usize..=5), |(script, max)| {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            runtime.block_on(exercise(script, max, &witnessed))
        })
        .unwrap();
    assert_eq!(
        witnessed.into_inner().unwrap().into_iter().collect::<Vec<_>>(),
        [
            "answers",
            "model fails",
            "refuses a tool outside the loop",
            "refuses broken arguments",
            "runs out of iterations"
        ]
    );
}
