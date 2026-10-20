// Port of src/decisions.test.ts and test/decisions-filter.test.ts, one test
// per case. The scripted JSON API stands where the TS tests put a scripted
// fetch under the OpenAI SDK: same path, body and error statuses.
use agent::{
    decisions::*,
    providers::{ApiError, JsonApi, RetryOpts},
    tracing::null_scope,
};
use async_trait::async_trait;
use proptest::prelude::*;
use serde_json::{Map, Value, json};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

type Reply = Result<Value, (u16, &'static str)>;

struct Scripted {
    replies: Mutex<VecDeque<Reply>>,
    requests: Mutex<Vec<(String, Value)>>,
}

#[async_trait]
impl JsonApi for Scripted {
    async fn post(&self, path: &str, body: Value, _: &CancellationToken) -> anyhow::Result<Value> {
        self.requests.lock().unwrap().push((path.into(), body));
        match self.replies.lock().unwrap().pop_front().expect("Unexpected decisions call") {
            Ok(value) => Ok(value),
            Err((status, message)) => Err(ApiError::Status { status, message: message.into() }.into()),
        }
    }
}

fn scripted(replies: Vec<Reply>) -> Arc<Scripted> {
    Arc::new(Scripted { replies: Mutex::new(replies.into()), requests: Mutex::default() })
}

fn client(api: Arc<Scripted>, model: &str) -> DecisionClient {
    DecisionClient::with_retry(model.into(), api, RetryOpts { base_delay_ms: 0, jitter: false, ..RetryOpts::default() })
}

fn obj(v: Value) -> Map<String, Value> {
    v.as_object().unwrap().clone()
}

struct Questions {
    intent: Question<ChoiceAnswer>,
    urgency: Question<ScoreAnswer>,
    is_question: Question<NoulAnswer>,
}

fn questions() -> Questions {
    Questions {
        intent: choice(
            "What does the user want?",
            obj(json!({ "digest": "A news digest", "reminder": "A reminder", "chit_chat": "Small talk" })),
        )
        .unwrap(),
        urgency: score("How urgent is it?", vec!["Low".into(), "Normal".into(), "High".into()]).unwrap(),
        is_question: noul("The message is a question.", Some(("Yes", "No"))),
    }
}

fn request(state: Value, q: &Questions) -> DecisionRequest {
    DecisionRequest::new(state, null_scope(), CancellationToken::new())
        .question("intent", &q.intent)
        .question("urgency", &q.urgency)
        .question("is_question", &q.is_question)
}

// Captured from the live API (typesafe/jev-1.13, 2026-10-12).
fn live_reply() -> Value {
    json!({
        "model": "typesafe/jev-1.13-20260917",
        "answers": {
            "intent": {
                "type": "choice",
                "choice": "digest",
                "probabilities": { "digest": 1, "reminder": 0, "chit_chat": 0 },
                "confidence": 1,
            },
            "urgency": {
                "type": "score",
                "score": 0.37,
                "legend": { "0": "Low", "1": "Normal", "2": "High" },
                "probabilities": { "0": 0.64, "1": 0.35, "2": 0.01 },
                "confidence": 0.44,
            },
            "is_question": { "type": "noul", "noul": 0.25 },
        },
        "usage": { "input_tokens": 544, "output_tokens": 82, "cost": 2.2848e-5 },
        "id": "gen-dec-1",
        "provider": "TypeSafe",
    })
}

// ── decision client ─────────────────────────────────────────────────────────

#[tokio::test]
async fn sends_the_native_questions_schema_to_the_decisions_endpoint() {
    let api = scripted(vec![Ok(live_reply())]);
    let q = questions();
    client(api.clone(), "typesafe/jev-1.13")
        .decide(request(json!({ "message": "send me the digest" }), &q))
        .await
        .unwrap();
    let (url, body) = api.requests.lock().unwrap()[0].clone();
    assert_eq!(url, OPENROUTER_DECISIONS_URL);
    assert_eq!(
        body,
        json!({
            "model": "typesafe/jev-1.13",
            "state": { "message": "send me the digest" },
            "questions": {
                "intent": {
                    "type": "choice",
                    "instructions": "What does the user want?",
                    "criteria": { "digest": "A news digest", "reminder": "A reminder", "chit_chat": "Small talk" },
                },
                "urgency": { "type": "score", "instructions": "How urgent is it?", "criteria": ["Low", "Normal", "High"] },
                "is_question": {
                    "type": "noul",
                    "instructions": "The message is a question.",
                    "criteria": { "true": "Yes", "false": "No" },
                },
            },
        })
    );
}

#[tokio::test]
async fn normalises_answers_score_levels_by_label_noul_as_a_probability() {
    let api = scripted(vec![Ok(live_reply())]);
    let q = questions();
    let decision = client(api, "typesafe/jev-1.13").decide(request(json!("send me the digest"), &q)).await.unwrap();
    let intent = decision.answer("intent", &q.intent).unwrap();
    assert_eq!(intent.choice, "digest");
    assert_eq!(intent.confidence, 1.0);
    assert_eq!(intent.probabilities["reminder"], 0);
    assert_eq!(
        decision.answer("urgency", &q.urgency).unwrap(),
        ScoreAnswer { level: "Low".into(), score: 0.37, confidence: 0.64, probabilities: vec![0.64, 0.35, 0.01] }
    );
    assert_eq!(decision.answer("is_question", &q.is_question).unwrap(), NoulAnswer { probability: 0.25 });
    assert_eq!(decision.model, "typesafe/jev-1.13-20260917");
    assert_eq!(decision.usage, Some(agent::tracing::TokenUsage { input: 544, output: 82, total: 626, cached: None }));
    assert_eq!(decision.cost_usd, Some(2.2848e-5));
}

#[tokio::test]
async fn rejects_a_choice_outside_the_declared_options_instead_of_passing_it_on() {
    let mut reply = live_reply();
    reply["answers"]["intent"]["choice"] = json!("weather");
    let q = questions();
    let err = client(scripted(vec![Ok(reply)]), "typesafe/jev-1.13").decide(request(json!("x"), &q)).await.unwrap_err();
    assert!(
        err.to_string().contains("answer \"intent\": choice \"weather\" is not one of the declared options"),
        "{err}"
    );
}

#[tokio::test]
async fn rejects_a_reply_that_is_missing_an_answer() {
    let mut reply = live_reply();
    reply["answers"] = json!({ "intent": reply["answers"]["intent"] });
    let q = questions();
    let err = client(scripted(vec![Ok(reply)]), "typesafe/jev-1.13").decide(request(json!("x"), &q)).await.unwrap_err();
    assert!(err.to_string().starts_with("answer \"urgency\""), "{err}");
}

#[tokio::test]
async fn retries_a_transient_failure_and_gives_up_at_once_on_a_4xx() {
    let q = questions();
    let transient = scripted(vec![Err((503, "overloaded")), Ok(live_reply())]);
    let decision = client(transient.clone(), "m").decide(request(json!("x"), &q)).await.unwrap();
    assert_eq!(transient.requests.lock().unwrap().len(), 2);
    assert_eq!(decision.answer("intent", &q.intent).unwrap().choice, "digest");

    let permanent = scripted(vec![Err((400, "bad schema"))]);
    let err = client(permanent.clone(), "m").decide(request(json!("x"), &q)).await.unwrap_err();
    assert!(err.to_string().contains("400"), "{err}");
    assert_eq!(permanent.requests.lock().unwrap().len(), 1);
}

#[test]
fn refuses_a_choice_question_the_api_would_reject() {
    let err = choice("one option", obj(json!({ "only": "x" }))).err().unwrap();
    assert!(err.to_string().contains("2–255 options"), "{err}");
}

// ── filter preset ───────────────────────────────────────────────────────────

// Answers each filter question from the item itself, through the real client
// and the question's own parser. Records every request's state; tracks how
// many batches are in flight at once.
struct FakeFilter {
    p_of: Box<dyn Fn(&Value) -> f64 + Send + Sync>,
    fail: Option<&'static str>,
    states: Mutex<Vec<Value>>,
}

#[async_trait]
impl JsonApi for FakeFilter {
    async fn post(&self, _: &str, body: Value, _: &CancellationToken) -> anyhow::Result<Value> {
        if let Some(fail) = self.fail {
            anyhow::bail!(fail);
        }
        let state = body["state"].clone();
        self.states.lock().unwrap().push(state.clone());
        let answers: Map<String, Value> = state["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| (format!("item_{}", i["n"]), json!({ "type": "noul", "noul": (self.p_of)(&i["item"]) })))
            .collect();
        Ok(json!({ "model": "fake", "answers": answers }))
    }
}

fn fake_filter(p_of: impl Fn(&Value) -> f64 + Send + Sync + 'static, fail: Option<&'static str>) -> Arc<FakeFilter> {
    Arc::new(FakeFilter { p_of: Box::new(p_of), fail, states: Mutex::default() })
}

async fn run_filter(api: Arc<FakeFilter>, items: &[Value], threshold: Option<f64>) -> anyhow::Result<FilterResult> {
    let client = DecisionClient::with_retry("fake".into(), api, RetryOpts { max_retries: 0, ..RetryOpts::default() });
    filter(&client, items, "rules", threshold, &null_scope(), &CancellationToken::new()).await
}

#[tokio::test]
async fn keeps_items_at_or_above_the_threshold_in_input_order_with_aligned_probabilities() {
    let api = fake_filter(|item| if item == "b" || item == "d" { 0.3 } else { 0.29 }, None);
    let items = [json!("a"), json!("b"), json!("c"), json!("d")];
    let r = run_filter(api.clone(), &items, None).await.unwrap();
    assert_eq!(r.kept, [json!("b"), json!("d")]);
    assert_eq!(r.probabilities, [0.29, 0.3, 0.29, 0.3]);
    assert_eq!(
        api.states.lock().unwrap()[0],
        json!({
            "rules": "rules",
            "items": [{ "n": 1, "item": "a" }, { "n": 2, "item": "b" }, { "n": 3, "item": "c" }, { "n": 4, "item": "d" }],
        })
    );
}

#[tokio::test]
async fn honours_a_custom_threshold() {
    let api = fake_filter(|item| if item == "hi" { 0.6 } else { 0.4 }, None);
    let r = run_filter(api, &[json!("hi"), json!("lo")], Some(0.5)).await.unwrap();
    assert_eq!(r.kept, [json!("hi")]);
}

#[tokio::test]
async fn packs_batches_by_item_count_and_by_size_keeping_order_across_batches() {
    let api = fake_filter(|_| 1.0, None);
    let items: Vec<Value> = (0..60).map(Value::from).collect();
    let r = run_filter(api.clone(), &items, None).await.unwrap();
    let mut sizes: Vec<usize> =
        api.states.lock().unwrap().iter().map(|s| s["items"].as_array().unwrap().len()).collect();
    sizes.sort_by(|a, b| b.cmp(a));
    assert_eq!(sizes, [FILTER_MAX_BATCH_ITEMS, FILTER_MAX_BATCH_ITEMS, 10]);
    assert_eq!(r.kept, items);

    let big = fake_filter(|_| 1.0, None);
    run_filter(big.clone(), &[json!("x".repeat(15_000)), json!("y".repeat(15_000)), json!("z")], None).await.unwrap();
    let mut sizes: Vec<usize> =
        big.states.lock().unwrap().iter().map(|s| s["items"].as_array().unwrap().len()).collect();
    sizes.sort();
    assert_eq!(sizes, [1, 2]);
}

#[tokio::test]
async fn asks_nothing_for_an_empty_list() {
    let api = fake_filter(|_| 1.0, None);
    let r = run_filter(api.clone(), &[], None).await.unwrap();
    assert!(r.kept.is_empty() && r.probabilities.is_empty());
    assert!(api.states.lock().unwrap().is_empty());
}

#[tokio::test]
async fn fails_when_the_decision_model_fails_no_pass_through() {
    let api = fake_filter(|_| 1.0, Some("openrouter 503"));
    let err = run_filter(api, &[json!("a")], None).await.unwrap_err();
    assert!(format!("{err:#}").contains("openrouter 503"), "{err:#}");
}

// ── filter through the decision client (test/decisions-filter.test.ts) ──────
//
// A stand-in for OpenRouter's decisions API answers every item from the item
// itself, sometimes badly; the rules say from the items alone what must be
// asked, how it is batched, and what comes back — or that the whole filter
// fails.

#[derive(Clone, Copy, Debug, PartialEq)]
enum Fault {
    None,
    Extra,
    Missing,
    OutOfRange,
    WrongType,
    Rejected,
}

struct SeenBatch {
    ids: Vec<i64>,
    ns: Vec<i64>,
    question_keys: Vec<String>,
    chars: usize,
}

struct FaultyApi {
    fault_for: Box<dyn Fn(i64) -> Fault + Send + Sync>,
    seen: Mutex<Vec<SeenBatch>>,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
}

#[async_trait]
impl JsonApi for FaultyApi {
    async fn post(&self, _: &str, body: Value, _: &CancellationToken) -> anyhow::Result<Value> {
        let entries = body["state"]["items"].as_array().expect("malformed request").clone();
        let questions = body["questions"].as_object().expect("malformed request");
        let ids: Vec<i64> = entries.iter().map(|e| e["item"]["id"].as_i64().unwrap()).collect();
        self.seen.lock().unwrap().push(SeenBatch {
            ids: ids.clone(),
            ns: entries.iter().map(|e| e["n"].as_i64().unwrap()).collect(),
            question_keys: questions.keys().cloned().collect(),
            chars: entries.iter().map(|e| e["item"].to_string().encode_utf16().count()).sum(),
        });
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_in_flight.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        let fault = (self.fault_for)(ids.first().copied().unwrap_or(-1));
        if fault == Fault::Rejected {
            return Err(ApiError::Status { status: 400, message: "bad schema".into() }.into());
        }
        let mut answers: Map<String, Value> = entries
            .iter()
            .map(|e| (format!("item_{}", e["n"]), json!({ "type": "noul", "noul": e["item"]["p"] })))
            .collect();
        let first_key = format!("item_{}", entries.first().map_or(1, |e| e["n"].as_i64().unwrap()));
        match fault {
            Fault::Extra => {
                answers.insert("item_999".into(), json!({ "type": "noul", "noul": 0.5 }));
            }
            Fault::Missing => {
                answers.remove(&first_key);
            }
            Fault::OutOfRange => {
                answers.insert(first_key, json!({ "type": "noul", "noul": 1.7 }));
            }
            Fault::WrongType => {
                answers.insert(first_key, json!({ "type": "choice", "choice": "yes" }));
            }
            Fault::None | Fault::Rejected => {}
        }
        let n = self.seen.lock().unwrap().len();
        Ok(json!({ "model": "typesafe/jev-test", "id": format!("dec-{n}"), "answers": answers }))
    }
}

fn filter_item() -> impl Strategy<Value = (f64, String)> {
    let p = prop::sample::select(vec![0.0, 0.1, 0.29, FILTER_THRESHOLD, 0.31, 0.8, 1.0]);
    // Mostly short, sometimes big enough to fill a batch by size.
    let body = prop_oneof![6 => ".{0,60}", 1 => (2_000usize..=30_000).prop_map(|n| "я".repeat(n))];
    (p, body)
}

fn fault() -> impl Strategy<Value = Fault> {
    prop::sample::select(vec![
        Fault::None,
        Fault::None,
        Fault::None,
        Fault::Extra,
        Fault::Missing,
        Fault::OutOfRange,
        Fault::WrongType,
        Fault::Rejected,
    ])
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(150))]
    #[test]
    fn asks_about_every_item_once_in_bounded_batches_and_keeps_exactly_the_items_at_or_over_the_threshold(
        raw in prop::collection::vec(filter_item(), 0..=80),
        threshold in prop::option::of(prop::sample::select(vec![0.1, 0.3, 0.5, 0.9])),
        fault in fault(),
        fault_pick in any::<usize>(),
    ) {
        let items: Vec<Value> =
            raw.iter().enumerate().map(|(id, (p, body))| json!({ "id": id, "p": p, "body": body })).collect();
        let fault_id = if items.is_empty() { None } else { Some((fault_pick % items.len()) as i64) };
        // The fault hits one batch: the one that starts at this item.
        let api = Arc::new(FaultyApi {
            fault_for: Box::new(move |first| if Some(first) == fault_id { fault } else { Fault::None }),
            seen: Mutex::default(),
            in_flight: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
        });
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let settled = rt.block_on(async {
            let client = DecisionClient::with_retry("typesafe/jev-test".into(), api.clone(), RetryOpts::default());
            filter(&client, &items, "Keep tech news.", threshold, &null_scope(), &CancellationToken::new()).await
        });
        let at = format!("{} items, fault {fault:?}", items.len());
        let seen = api.seen.lock().unwrap();

        // Every item asked about exactly once, in contiguous ascending
        // batches within both limits, numbered from 1 with one question each.
        let mut asked: Vec<i64> = seen.iter().flat_map(|s| s.ids.clone()).collect();
        asked.sort();
        let fault_hit = seen.iter().any(|s| s.ids.first().copied() == fault_id)
            && fault != Fault::None
            && fault != Fault::Extra;
        if !fault_hit {
            prop_assert_eq!(&asked, &(0..items.len() as i64).collect::<Vec<_>>(), "{}: every item once", at);
        }
        for s in seen.iter() {
            let first = s.ids[0];
            prop_assert_eq!(&s.ids, &(0..s.ids.len() as i64).map(|k| first + k).collect::<Vec<_>>(), "{}: a batch is a run of consecutive items", at);
            prop_assert_eq!(&s.ns, &(1..=s.ids.len() as i64).collect::<Vec<_>>(), "{}: numbered from 1", at);
            let mut keys = s.question_keys.clone();
            keys.sort();
            let mut want: Vec<String> = s.ns.iter().map(|n| format!("item_{n}")).collect();
            want.sort();
            prop_assert_eq!(keys, want, "{}: one question per item", at);
            prop_assert!(s.ids.len() <= FILTER_MAX_BATCH_ITEMS, "{}: at most {} items", at, FILTER_MAX_BATCH_ITEMS);
            if s.ids.len() > 1 {
                prop_assert!(s.chars <= FILTER_MAX_BATCH_CHARS, "{}: within the char budget", at);
            }
        }
        prop_assert!(api.max_in_flight.load(Ordering::SeqCst) <= FILTER_CONCURRENCY, "bounded concurrency");
        if items.is_empty() {
            prop_assert!(seen.is_empty(), "nothing to ask");
        }

        if fault_hit {
            // One bad batch fails the whole filter: nothing passes through.
            prop_assert!(settled.is_err(), "{}: a malformed or refused answer fails the filter", at);
            return Ok(());
        }
        let r = settled.unwrap();
        let t = threshold.unwrap_or(FILTER_THRESHOLD);
        prop_assert_eq!(&r.probabilities, &raw.iter().map(|(p, _)| *p).collect::<Vec<_>>(), "{}", at);
        let kept: Vec<Value> = r.kept.iter().map(|i| i["id"].clone()).collect();
        let want: Vec<Value> = items.iter().filter(|i| i["p"].as_f64().unwrap() >= t).map(|i| i["id"].clone()).collect();
        prop_assert_eq!(kept, want, "{}: kept, in input order", at);
    }
}

// ── Rust-only ───────────────────────────────────────────────────────────────

#[test]
fn question_parsers_reject_malformed_answers() {
    let q = questions();
    assert!(
        q.intent
            .parse(&json!({"type":"choice","choice":"digest","confidence":0.7,"probabilities":{"digest":1.2}}))
            .is_err()
    );
    assert!(q.urgency.parse(&json!({"type":"score","score":-1,"confidence":0.9,"probabilities":{}})).is_err());
    for p in [-0.1, 1.1] {
        assert!(q.is_question.parse(&json!({"type":"noul","noul":p})).is_err());
    }
    assert!(q.is_question.parse(&json!({"type":"choice","noul":0.5})).is_err());
    assert!(noul("x", None).wire.get("criteria").is_none());
}

#[test]
fn batching_counts_utf16_and_gives_oversized_items_their_own_batch() {
    let items = vec![json!("🦊".repeat(6000)), json!("x".repeat(12000)), json!("big".repeat(9000)), json!("end")];
    assert_eq!(pack_filter_batches(&items), [vec![0], vec![1], vec![2], vec![3]]);
    let ascii = vec![json!("x".repeat(11998)), json!("x".repeat(11998))];
    assert_eq!(pack_filter_batches(&ascii), [vec![0, 1]]);
}
