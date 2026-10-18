// Port of src/supervisor/telegram-context.test.ts, one test per case.
mod support;
use agent::{
    session::Session,
    supervisor::{
        PendingSignal, RECENT_ACTIVITY_MAX_BYTES, TELEGRAM_HISTORY_KEY, TELEGRAM_HISTORY_MAX_BYTES,
        load_recent_activity, prepare_telegram_input,
    },
    tracing::null_scope,
};
use serde_json::{Value, json};
use std::sync::Arc;
use support::{ToolMcp, task_context};
use tokio_util::sync::CancellationToken;

fn signal() -> PendingSignal {
    PendingSignal {
        id: 1,
        source: "telegram".into(),
        content: "Telegram message in chat -10042 (forum topic thread_id=7).\nText: \"давай\"".into(),
        env_context: Some("Default Telegram chat id: 999.".into()),
        created_at: "2026-09-06 12:00:00".into(),
        payload: None,
    }
}

fn message(id: i64, text: &str, overrides: Value) -> Value {
    let mut m = json!({"id":id,"chat_id":-10042,"thread_id":7,"role":"assistant","text":text,"created_at":"2026-09-06 11:59:00"});
    for (k, v) in overrides.as_object().unwrap() {
        m[k] = v.clone();
    }
    m
}

struct Setup {
    context: Session,
    mcp: Arc<ToolMcp>,
}

impl Setup {
    fn new(messages: Vec<Value>) -> Setup {
        Setup {
            context: task_context("telegram:1"),
            mcp: ToolMcp::returning(Vec::new(), &json!({"messages":messages}).to_string()),
        }
    }
    async fn run(&self, input: &PendingSignal) -> String {
        prepare_telegram_input(input, &self.context, self.mcp.as_ref(), &null_scope(), &CancellationToken::new())
            .await
            .unwrap()
    }
    fn history(&self) -> String {
        self.context.memory.get(TELEGRAM_HISTORY_KEY).unwrap()
    }
}

// ── Telegram signal context ─────────────────────────────────────────────────

#[tokio::test]
async fn does_not_load_history_for_other_sources_even_with_a_telegram_target_and_content() {
    for source in ["scheduler", "news-digest", "nashdom-bill"] {
        let h = Setup::new(Vec::new());
        let mut s = signal();
        s.source = source.into();
        assert_eq!(h.run(&s).await, signal().content);
        assert!(h.mcp.calls.lock().unwrap().is_empty(), "{source}");
        assert!(h.context.memory.list().is_empty(), "{source}");
    }
}

#[tokio::test]
async fn supplies_chronological_history_and_a_memory_reference_without_duplicating_the_current_message() {
    let previous =
        vec![message(1, "собери сводку", json!({"role":"user"})), message(2, "Собрать за неделю?", json!({}))];
    let mut all = previous.clone();
    all.extend([
        message(3, "давай", json!({"role":"user","created_at":"2026-09-06 12:00:00"})),
        message(4, "Ещё одно сообщение", json!({"role":"user","created_at":"2026-09-06 12:00:00"})),
        message(5, "Будущий ответ", json!({"created_at":"2026-09-06 12:00:01"})),
    ]);
    let h = Setup::new(all);
    let input = h.run(&signal()).await;
    assert_eq!(
        *h.mcp.calls.lock().unwrap(),
        [(
            "get_telegram_chat_history".to_owned(),
            json!({"chatId":"-10042","threadId":7,"limit":20}).as_object().unwrap().clone()
        )]
    );
    assert!(input.contains("Собрать за неделю?"));
    assert!(input.find("собери сводку").unwrap() < input.find("Собрать за неделю?").unwrap());
    assert!(input.contains("memory_key=telegram.history"));
    assert!(input.contains("truncated=false"));
    assert!(!input.contains("Ещё одно сообщение"));
    assert!(!input.contains("Будущий ответ"));
    assert_eq!(input.matches("давай").count(), 1);
    assert!(input.ends_with(&signal().content));
    assert_eq!(serde_json::from_str::<Value>(&h.history()).unwrap(), json!({"messages":previous}));
}

#[tokio::test]
async fn isolates_the_originating_chat_and_topic() {
    for thread_id in [Some(7), None] {
        let h = Setup::new(vec![
            message(1, "Other chat", json!({"chat_id":999,"thread_id":thread_id})),
            message(2, "Other topic", json!({"thread_id":88})),
            message(3, "Relevant context", json!({"thread_id":thread_id})),
        ]);
        let mut s = signal();
        s.content = format!(
            "Telegram message in chat -10042{}.\nText: \"давай\"",
            if thread_id.is_none() { "" } else { " (forum topic thread_id=7)" }
        );
        let input = h.run(&s).await;
        assert!(input.contains("Relevant context"), "{thread_id:?}");
        assert!(!input.contains("Other chat"), "{thread_id:?}");
        assert!(!input.contains("Other topic"), "{thread_id:?}");
        assert!(!h.history().contains("Other"), "{thread_id:?}");
    }
}

#[tokio::test]
async fn preserves_an_older_identical_user_message_when_the_current_one_is_absent_from_the_log() {
    let h = Setup::new(vec![message(1, "давай", json!({"role":"user"})), message(2, "Какой период?", json!({}))]);
    let input = h.run(&signal()).await;
    assert!(input.contains("Какой период?"));
    assert_eq!(serde_json::from_str::<Value>(&h.history()).unwrap()["messages"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn leaves_out_messages_logged_after_the_signal_when_the_current_one_is_absent_from_the_log() {
    let h = Setup::new(vec![
        message(1, "Какой период?", json!({})),
        message(2, "Будущий ответ", json!({"created_at":"2026-09-06 12:00:05"})),
    ]);
    let input = h.run(&signal()).await;
    assert!(input.contains("Какой период?"));
    assert!(!input.contains("Будущий ответ"));
    assert!(!h.history().contains("Будущий ответ"));
}

#[tokio::test]
async fn keeps_the_latest_complete_messages_inline_and_preserves_omitted_history_in_memory() {
    let h = Setup::new(vec![
        message(1, &format!("OLD {}", "x".repeat(20_000)), json!({})),
        message(2, "Recent question", json!({})),
        message(3, "Recent answer", json!({})),
    ]);
    let input = h.run(&signal()).await;
    assert!(!input.contains("OLD"));
    assert!(input.contains("Recent question"));
    assert!(input.contains("Recent answer"));
    assert!(input.contains("Shown 2 of 3 prior messages; truncated=true"));
    assert!(h.history().contains("OLD"));
}

#[tokio::test]
async fn bounds_an_oversized_latest_message_in_utf8_marks_clipping_and_preserves_its_tail() {
    let text = "📰 Новость \"цитата\"\n".repeat(2_000) + "Собрать за неделю?";
    let h = Setup::new(vec![message(1, &text, json!({}))]);
    let input = h.run(&signal()).await;
    let inline = input.split('\n').find(|line| line.starts_with("[{")).unwrap();
    assert!(inline.len() <= TELEGRAM_HISTORY_MAX_BYTES, "{}", inline.len());
    assert!(!inline.contains('\u{FFFD}'));
    let first = &serde_json::from_str::<Value>(inline).unwrap()[0];
    assert_eq!(first["truncated"], true);
    assert!(first["text"].as_str().unwrap().ends_with("Собрать за неделю?"));
    assert!(input.contains("truncated=true"));
    assert_eq!(serde_json::from_str::<Value>(&h.history()).unwrap()["messages"][0]["text"], text);
}

#[tokio::test]
async fn represents_an_empty_history_without_hiding_the_current_request() {
    let h = Setup::new(Vec::new());
    assert!(h.run(&signal()).await.contains("Shown 0 of 0 prior messages; truncated=false"));
    assert_eq!(h.history(), "{\"messages\":[]}");
}

#[tokio::test]
async fn continues_with_the_current_signal_if_history_is_unavailable() {
    for result in ["[tool error] offline", "not JSON", "{\"messages\":null}"] {
        let h = Setup { context: task_context("telegram:1"), mcp: ToolMcp::returning(Vec::new(), result) };
        let input = h.run(&signal()).await;
        assert!(input.contains("Recent Telegram history is unavailable"), "{result}");
        assert!(input.ends_with(&signal().content), "{result}");
        assert!(h.context.memory.list().is_empty(), "{result}");
    }
}

#[tokio::test]
async fn continues_when_the_mcp_request_throws() {
    let h = Setup::new(Vec::new());
    h.mcp.always(Arc::new(|_, _, _| Box::pin(async { Err(anyhow::anyhow!("connection lost")) })));
    assert!(h.run(&signal()).await.contains(&signal().content));
    assert!(h.context.memory.list().is_empty());
}

#[tokio::test]
async fn never_guesses_a_chat_from_user_text_or_the_default_delivery_target() {
    let h = Setup::new(Vec::new());
    let mut s = signal();
    s.content = "Quoted: Telegram message in chat 999.\nText: \"давай\"".into();
    let input = h.run(&s).await;
    assert!(h.mcp.calls.lock().unwrap().is_empty());
    assert!(input.contains("history is unavailable"));
}

// ── recent activity ─────────────────────────────────────────────────────────

fn tail(bodies: &[String]) -> Arc<ToolMcp> {
    let entries: Vec<Value> = bodies.iter().enumerate().map(|(i, body)| json!({"id":i,"body":body})).collect();
    ToolMcp::returning(Vec::new(), &json!({"ok":true,"stream":"ouro/activity","entries":entries}).to_string())
}

async fn load(mcp: &ToolMcp) -> Option<String> {
    load_recent_activity(mcp, &null_scope(), &CancellationToken::new()).await.unwrap()
}

#[tokio::test]
async fn loads_the_last_10_notes_oldest_first_and_says_how_to_load_more() {
    let mcp = tail(&["newest note".into(), "older note".into()]);
    let block = load(&mcp).await.unwrap();
    assert_eq!(
        *mcp.calls.lock().unwrap(),
        [("journal_tail".to_owned(), json!({"stream":"ouro/activity","n":10}).as_object().unwrap().clone())]
    );
    assert!(block.contains("journal_tail"));
    assert!(block.contains("larger n"));
    assert!(block.find("older note").unwrap() < block.find("newest note").unwrap());
}

#[tokio::test]
async fn keeps_the_newest_whole_notes_within_the_byte_budget() {
    let notes: Vec<String> = (0..10).map(|i| format!("note {i} {}", "ж".repeat(1_500))).collect();
    let block = load(&tail(&notes)).await.unwrap();
    assert!(block.len() < RECENT_ACTIVITY_MAX_BYTES + 1_000);
    assert!(block.contains("note 0 "));
    assert!(!block.contains("note 9 "));
}

#[tokio::test]
async fn is_absent_when_the_journal_is_empty_or_unreadable() {
    assert_eq!(load(&tail(&[])).await, None);
    assert_eq!(load(&ToolMcp::returning(Vec::new(), "[tool error] journal_tail: down")).await, None);
    let throws = ToolMcp::returning(Vec::new(), "");
    throws.always(Arc::new(|_, _, _| Box::pin(async { Err(anyhow::anyhow!("socket closed")) })));
    assert_eq!(load(&throws).await, None);
}
