// User confirmation: 1. pending cards and the SQLite store; 2. host-rendered
// requests and resolution. Tokens authorize one exact call in one durable run.
use crate::db::MemoryStore;
use crate::{agent::ConfirmTarget, db::AgentDb, mcp::Mcp, providers::ToolCall};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use rusqlite::{OptionalExtension, params};
use serde_json::{Value, json};
use std::collections::HashMap;
use tokio_util::sync::CancellationToken;

// What the user sees: the card, its buttons and the line a press leaves on
// it. The agent may reword it when the user asks (`set_memory` of CARD_KEY,
// a guarded write; fields it leaves out keep their defaults). Whatever the
// wording, the card shows the exact call: a text without `{name}` and
// `{arguments}` gets them appended. `{guidance}` and `{goal}` are optional.
pub const CARD_KEY: &str = "confirmation.card";
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(default)]
pub struct Card {
    pub text: String,
    pub confirm: String,
    pub decline: String,
    pub outcomes: HashMap<String, String>,
}
impl Default for Card {
    fn default() -> Self {
        Card {
            text: "Confirm action: {name}\n{arguments}\n\n{guidance}\n\nYour goal: {goal}".into(),
            confirm: "Confirm".into(),
            decline: "Decline".into(),
            outcomes: [
                ("confirmed", "Confirmed"),
                ("declined", "Declined"),
                ("expired", "Expired"),
                ("other", "Not confirmed"),
            ]
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect(),
        }
    }
}
impl Card {
    /// The stored wording over the defaults; an unreadable one is ignored.
    pub fn load(memory: &dyn MemoryStore) -> Card {
        let defaults = Card::default();
        let Some(mut card) =
            memory.get(CARD_KEY).ok().flatten().and_then(|raw| serde_json::from_str::<Card>(&raw).ok())
        else {
            return defaults;
        };
        if !card.text.contains("{name}") || !card.text.contains("{arguments}") {
            card.text = format!("{}\n\n{{name}}\n{{arguments}}", card.text.trim_end());
        }
        let label = |l: &str| !l.trim().is_empty() && l.chars().count() <= 32;
        if !label(&card.confirm) || !label(&card.decline) || card.confirm == card.decline {
            (card.confirm, card.decline) = (defaults.confirm, defaults.decline);
        }
        for (k, v) in defaults.outcomes {
            card.outcomes.entry(k).or_insert(v);
        }
        card
    }
    pub fn outcome(&self, outcome: &str) -> &str {
        self.outcomes.get(outcome).or_else(|| self.outcomes.get("other")).map_or("", String::as_str)
    }
    fn render(&self, name: &str, arguments: &str, guidance: &str, goal: &str) -> String {
        self.text
            .replace("{name}", name)
            .replace("{arguments}", arguments)
            .replace("{guidance}", guidance)
            .replace("{goal}", goal)
    }
}

// ── 1. store ───────────────────────────────────────────────────────────────
#[derive(Clone, Debug)]
pub struct Pending {
    // The guard run that asked, and the loop run whose call it is.
    pub run: String,
    pub loop_run: Option<String>,
    pub key: String,
    pub yes: String,
    pub no: String,
    pub target: ConfirmTarget,
    pub message: Option<i64>,
    pub card: String,
}
type ResolutionRow = (String, String, Option<i64>, String, Option<i64>, Option<String>);

/// A button press the guard run consumed: the token pressed and the event's
/// data (`Record::Event` in the run's journal).
pub struct Consumed {
    pub type_: String,
    pub data: Value,
}

pub trait Store: Send + Sync {
    fn outcome(&self, run: &str, key: &str) -> anyhow::Result<Option<String>>;
    fn get(&self, run: &str, key: &str) -> anyhow::Result<Option<Pending>>;
    /// The card holding token `token`, answered or not.
    fn by_token(&self, token: &str) -> anyhow::Result<Option<(Pending, Option<String>)>>;
    /// Cards waiting in `target`, oldest first (liveness is the caller's).
    fn waiting(&self, target: &ConfirmTarget) -> anyhow::Result<Vec<(String, Value)>>;
    fn insert(&self, pending: &Pending) -> anyhow::Result<()>;
    fn sent(&self, run: &str, key: &str, message: i64) -> anyhow::Result<()>;
    /// Resolves using an event the guard run consumed, never a YAML claim of
    /// approval; spent once.
    fn resolve(&self, run: &str, key: &str, claimed: &str, consumed: &[Consumed]) -> anyhow::Result<String>;
    /// Whether a card of a guard of `loop_run` still waits for `key`.
    fn pending_loop_confirmation(&self, loop_run: &str, key: &str) -> anyhow::Result<bool>;
    /// One reviewed call authorizes one host invocation (the engine call's
    /// idempotency key): a loop cannot spend a confirmed verdict twice.
    fn use_guard_verdict(&self, child: &str, call: &str, invocation: &str) -> anyhow::Result<bool>;
    /// Cards created before `before_ms` and not answered: the cut-over's.
    fn unanswered_before(&self, before_ms: i64) -> anyhow::Result<Vec<Pending>>;
    fn expire(&self, run: &str, key: &str) -> anyhow::Result<()>;
}
fn pending_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Pending> {
    Ok(Pending {
        run: r.get(0)?,
        key: r.get(1)?,
        yes: r.get(2)?,
        no: r.get(3)?,
        target: ConfirmTarget { chat_id: r.get(4)?, message_thread_id: r.get(5)? },
        message: r.get(6)?,
        card: r.get(7)?,
        loop_run: r.get(8)?,
    })
}
const PENDING_COLUMNS: &str = "run_id,call_key,yes_type,no_type,chat_id,thread_id,message_id,card,loop_run";
impl Store for AgentDb {
    fn outcome(&self, run: &str, key: &str) -> anyhow::Result<Option<String>> {
        Ok(self
            .conn()
            .query_row("SELECT outcome FROM confirmations WHERE run_id=?1 AND call_key=?2", params![run, key], |r| {
                r.get(0)
            })
            .optional()?
            .flatten())
    }

    fn waiting(&self, target: &ConfirmTarget) -> anyhow::Result<Vec<(String, Value)>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT run_id,card,chat_id,thread_id,message_id,created_at_ms FROM confirmations
            WHERE outcome IS NULL AND message_id IS NOT NULL AND chat_id = ?1 AND thread_id IS ?2
            ORDER BY created_at_ms",
        )?;
        let cards = stmt.query_map(params![target.chat_id,target.message_thread_id], |r| Ok((r.get::<_,String>(0)?, json!({
            "call":clip(&r.get::<_,String>(1)?,500),"chatId":r.get::<_,String>(2)?,"messageThreadId":r.get::<_,Option<i64>>(3)?,
            "messageId":r.get::<_,i64>(4)?,"since":r.get::<_,i64>(5)?
        }))))?.collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(cards)
    }

    fn get(&self, run: &str, key: &str) -> anyhow::Result<Option<Pending>> {
        Ok(self
            .conn()
            .query_row(
                &format!("SELECT {PENDING_COLUMNS} FROM confirmations WHERE run_id = ?1 AND call_key = ?2"),
                params![run, key],
                pending_row,
            )
            .optional()?)
    }
    fn by_token(&self, token: &str) -> anyhow::Result<Option<(Pending, Option<String>)>> {
        Ok(self
            .conn()
            .query_row(
                &format!("SELECT {PENDING_COLUMNS},outcome FROM confirmations WHERE yes_type = ?1 OR no_type = ?1"),
                [token],
                |r| Ok((pending_row(r)?, r.get(9)?)),
            )
            .optional()?)
    }
    fn insert(&self, p: &Pending) -> anyhow::Result<()> {
        self.conn().execute(
            "INSERT INTO confirmations (run_id,call_key,yes_type,no_type,chat_id,thread_id,card,created_at_ms,loop_run)
            VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![
                p.run,
                p.key,
                p.yes,
                p.no,
                p.target.chat_id,
                p.target.message_thread_id,
                p.card,
                chrono::Utc::now().timestamp_millis(),
                p.loop_run
            ],
        )?;
        Ok(())
    }
    fn sent(&self, run: &str, key: &str, message: i64) -> anyhow::Result<()> {
        self.conn().execute(
            "UPDATE confirmations SET message_id = ?3 WHERE run_id = ?1 AND call_key = ?2 AND message_id IS NULL",
            params![run, key, message],
        )?;
        Ok(())
    }
    fn resolve(&self, run: &str, key: &str, claimed: &str, consumed: &[Consumed]) -> anyhow::Result<String> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let row: Option<ResolutionRow> = tx.query_row(
            "SELECT yes_type,no_type,message_id,chat_id,thread_id,outcome FROM confirmations WHERE run_id = ?1 AND call_key = ?2",
            params![run,key], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?))).optional()?;
        let Some((yes, no, message, chat, thread, prior)) = row else { return Ok("unverified".into()) };
        if prior.is_some() {
            return Ok("unverified".into());
        }
        // The first press of this card's two tokens the run consumed, and only
        // when it comes from the card's own chat, message and thread.
        let verified = consumed.iter().find(|c| c.type_ == yes || c.type_ == no).and_then(|c| {
            let v = &c.data;
            let same_chat =
                v["chatId"].as_str().map(str::to_owned).or_else(|| v["chatId"].as_i64().map(|i| i.to_string()))
                    == Some(chat.clone());
            (same_chat
                && v["messageId"].as_i64() == message
                && message.is_some()
                && v["messageThreadId"].as_i64() == thread)
                .then(|| if c.type_ == yes { "confirmed".to_owned() } else { "declined".to_owned() })
        });
        // An early/forged expiry cannot close a live card. A real expiry is
        // determined by host time, independent of the workflow's outcome word.
        let old_enough: bool = tx.query_row(
            "SELECT created_at_ms + 86400000 <= ?3 FROM confirmations WHERE run_id = ?1 AND call_key = ?2",
            params![run, key, chrono::Utc::now().timestamp_millis()],
            |r| r.get(0),
        )?;
        let outcome = verified
            .unwrap_or_else(|| if claimed == "expired" && old_enough { "expired".into() } else { "unverified".into() });
        tx.execute(
            "UPDATE confirmations SET outcome = ?3 WHERE run_id = ?1 AND call_key = ?2",
            params![run, key, outcome],
        )?;
        tx.commit()?;
        Ok(outcome)
    }
    fn pending_loop_confirmation(&self, loop_run: &str, key: &str) -> anyhow::Result<bool> {
        Ok(self.conn().query_row(
            "SELECT EXISTS(SELECT 1 FROM confirmations WHERE loop_run = ?1 AND call_key = ?2 AND outcome IS NULL)",
            params![loop_run, key],
            |r| r.get(0),
        )?)
    }
    fn use_guard_verdict(&self, child: &str, call: &str, invocation: &str) -> anyhow::Result<bool> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT INTO guard_verdict_uses (child_run,call_key,invocation,created_ms) VALUES (?1,?2,?3,?4) ON CONFLICT DO NOTHING",
            params![child, call, invocation, chrono::Utc::now().timestamp_millis()],
        )?;
        let same = tx.query_row(
            "SELECT invocation=?3 FROM guard_verdict_uses WHERE child_run=?1 AND call_key=?2",
            params![child, call, invocation],
            |r| r.get(0),
        )?;
        tx.commit()?;
        Ok(same)
    }
    fn unanswered_before(&self, before_ms: i64) -> anyhow::Result<Vec<Pending>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(&format!(
            "SELECT {PENDING_COLUMNS} FROM confirmations WHERE outcome IS NULL AND created_at_ms < ?1 ORDER BY created_at_ms"
        ))?;
        Ok(stmt.query_map([before_ms], pending_row)?.collect::<rusqlite::Result<_>>()?)
    }
    fn expire(&self, run: &str, key: &str) -> anyhow::Result<()> {
        self.conn().execute(
            "UPDATE confirmations SET outcome = 'expired' WHERE run_id = ?1 AND call_key = ?2 AND outcome IS NULL",
            params![run, key],
        )?;
        Ok(())
    }
}

/// `pending_confirmations`' cards: the ones waiting in `target` whose guard
/// run is still live in the engine (at most ten).
pub async fn pending(engine: &crate::agent::Engine, target: &ConfirmTarget) -> anyhow::Result<Value> {
    let mut cards = Vec::new();
    for (run, card) in Store::waiting(&engine.deps.db, target)? {
        if engine.runtime.get(&run).await?.is_some_and(|r| r.ended_ms.is_none()) {
            cards.push(card);
        }
        if cards.len() == 10 {
            break;
        }
    }
    Ok(json!(cards))
}

// ── 2. requests and resolution ─────────────────────────────────────────────
fn clip(s: &str, limit: usize) -> String {
    let mut chars = s.chars();
    let head: String = chars.by_ref().take(limit).collect();
    if chars.next().is_some() { format!("{head}… [truncated]") } else { head }
}
fn token() -> String {
    format!("cf:{}", URL_SAFE_NO_PAD.encode(uuid::Uuid::new_v4().as_bytes()))
}

// A new card for `req`, not sent yet: fresh tokens, the call as the user reads it.
fn card(req: &Request<'_>, key: String) -> Pending {
    let pretty = serde_json::from_str::<Value>(&req.call.arguments)
        .ok()
        .and_then(|v| serde_json::to_string_pretty(&v).ok())
        .unwrap_or_else(|| req.call.arguments.clone());
    Pending {
        run: req.run.into(),
        loop_run: Some(req.loop_run.into()),
        key,
        yes: token(),
        no: token(),
        target: req.target.clone(),
        message: None,
        card: req.card.render(
            &clip(&req.call.name, 128),
            &clip(&pretty, 1000),
            &clip(req.guidance, 300),
            &clip(req.goal, 400),
        ),
    }
}

pub struct Request<'a> {
    // The guard run asking, and the loop whose call it is.
    pub run: &'a str,
    pub loop_run: &'a str,
    pub call: &'a ToolCall,
    pub target: &'a ConfirmTarget,
    pub guidance: &'a str,
    pub goal: &'a str,
    pub card: &'a Card,
}

/// Sends the card (once: `confirm_request` is at most once, and a stored
/// card is never sent again). A press reaches the guard run as a targeted
/// event (`deliver_event`), so nothing is registered here.
pub async fn request(
    db: &AgentDb,
    mcp: &dyn Mcp,
    req: Request<'_>,
    cancel: &CancellationToken,
) -> anyhow::Result<Value> {
    let key = crate::guard::key(req.call);
    // A write may wait for the engine's lock on agent.db (runs.rs §2).
    let (run, lookup) = (req.run.to_owned(), key.clone());
    let stored = db.write(move |db| Store::get(db, &run, &lookup)).await?;
    // A request with unknown send outcome is never dispatched a second time.
    let p = if let Some(p) = stored {
        p
    } else {
        let p = card(&req, key.clone());
        let row = p.clone();
        db.write(move |db| Store::insert(db, &row)).await?;
        let raw = mcp
            .call_tool_once(
                "send_telegram_message",
                json!({"chatId":p.target.chat_id,"messageThreadId":p.target.message_thread_id,
            "text":p.card,"buttons":[[{"text":req.card.confirm,"data":p.yes},{"text":req.card.decline,"data":p.no}]]})
                .as_object()
                .unwrap()
                .clone(),
                cancel,
            )
            .await
            .map_err(|e| anyhow::anyhow!("confirmation card send: {e:?}"))?;
        anyhow::ensure!(!crate::tool_results::is_tool_error(&raw), "{raw}");
        let result: Value = serde_json::from_str(&raw)?;
        let message =
            result["messageId"].as_i64().ok_or_else(|| anyhow::anyhow!("confirmation card returned no messageId"))?;
        let (run, sent) = (req.run.to_owned(), key.clone());
        db.write(move |db| Store::sent(db, &run, &sent, message)).await?;
        Pending { message: Some(message), ..p }
    };
    anyhow::ensure!(p.message.is_some(), "confirmation card outcome unknown; it will not be sent again");
    Ok(json!({"required":true,"types":[p.yes,p.no]}))
}

pub async fn close_card(
    store: &dyn Store,
    mcp: &dyn Mcp,
    run: &str,
    call: &ToolCall,
    outcome: &str,
    card: &Card,
    cancel: &CancellationToken,
) {
    if let Some(p) = store.get(run, &crate::guard::key(call)).ok().flatten() {
        edit_card(mcp, &p, outcome, card, cancel).await;
    }
}

async fn edit_card(mcp: &dyn Mcp, p: &Pending, outcome: &str, card: &Card, cancel: &CancellationToken) {
    let edit = async {
        if let Some(message) = p.message {
            let label = card.outcome(outcome);
            let raw = mcp
                .call_tool_once(
                    "edit_telegram_message",
                    json!({"chatId":p.target.chat_id,"messageId":message,"text":format!("{}\n\n{label}",p.card)})
                        .as_object()
                        .unwrap()
                        .clone(),
                    cancel,
                )
                .await
                .map_err(|e| anyhow::anyhow!("{e:?}"))?;
            anyhow::ensure!(!crate::tool_results::is_tool_error(&raw), "{raw}");
        }
        anyhow::Ok(())
    }
    .await;
    if let Err(error) = edit {
        eprintln!("[confirmation] card edit failed: {error:#}");
    }
}

/// The cut-over (design 03, R-L6): cards created before the first start on
/// the engine belong to runs that are gone; each is edited to "expired" once.
pub async fn expire_cut_over(
    store: &dyn Store,
    mcp: &dyn Mcp,
    before_ms: i64,
    card: &Card,
    cancel: &CancellationToken,
) -> anyhow::Result<usize> {
    let cards = store.unanswered_before(before_ms)?;
    for p in &cards {
        store.expire(&p.run, &p.key)?;
        edit_card(mcp, p, "expired", card, cancel).await;
    }
    Ok(cards.len())
}

/// Why a press found no card to answer (design 03, decision 8): the agent
/// is told and decides what to say.
pub fn undelivered(store: &dyn Store, token: &str) -> anyhow::Result<Option<&'static str>> {
    Ok(match store.by_token(token)? {
        None => Some("unknown_card"),
        Some((_, Some(_))) => Some("already_answered"),
        Some(_) => None,
    })
}

// ── 3. tests ───────────────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn a_stored_card_rewords_but_always_shows_the_call() {
        let db = AgentDb::open(":memory:").unwrap();
        assert_eq!(Card::load(&db).confirm, "Confirm");
        db.set(CARD_KEY, r#"{"text":"Апрувни","confirm":"Да","decline":"Да","outcomes":{"declined":"Нет"}}"#).unwrap();
        let card = Card::load(&db);
        assert_eq!(card.render("update_fact", "{\"id\":19}", "g", "x"), "Апрувни\n\nupdate_fact\n{\"id\":19}");
        // Two identical buttons can't tell yes from no: both fall back.
        assert_eq!((card.confirm.as_str(), card.decline.as_str()), ("Confirm", "Decline"));
        assert_eq!((card.outcome("declined"), card.outcome("expired")), ("Нет", "Expired"));
        db.set(CARD_KEY, "not json").unwrap();
        assert_eq!(Card::load(&db).text, Card::default().text);
    }
    #[test]
    fn approval_is_bound_to_run_exact_call_and_card_and_spent_once() {
        for foreign in [false, true] {
            let db = AgentDb::open(":memory:").unwrap();
            let p = Pending {
                run: "r".into(),
                loop_run: Some("loop".into()),
                key: "exact-key".into(),
                yes: token(),
                no: token(),
                target: ConfirmTarget { chat_id: "42".into(), message_thread_id: Some(7) },
                message: None,
                card: "card".into(),
            };
            db.insert(&p).unwrap();
            db.sent("r", "exact-key", 100).unwrap();
            assert!(db.pending_loop_confirmation("loop", "exact-key").unwrap());
            let consumed = [Consumed {
                type_: p.yes.clone(),
                data: json!({"chatId":if foreign {43} else {42},"messageThreadId":7,"messageId":100}),
            }];
            assert_eq!(db.resolve("r", "different-call", "confirmed", &consumed).unwrap(), "unverified");
            assert_eq!(db.resolve("other-run", "exact-key", "confirmed", &consumed).unwrap(), "unverified");
            assert_eq!(
                db.resolve("r", "exact-key", "confirmed", &consumed).unwrap(),
                if foreign { "unverified" } else { "confirmed" }
            );
            assert_eq!(db.resolve("r", "exact-key", "confirmed", &consumed).unwrap(), "unverified");
            assert!(!db.pending_loop_confirmation("loop", "exact-key").unwrap());
        }
    }
    #[test]
    fn tokens_have_random_entropy_and_cards_clip_unicode_visibly() {
        let a = token();
        assert_ne!(a, token());
        assert!(a.len() <= 64);
        assert_eq!(clip("🧾🧾🧾", 2), "🧾🧾… [truncated]");
    }
}
