// User confirmation: 1. pending cards and the SQLite store; 2. host-rendered
// requests and resolution. Tokens authorize one exact call in one durable run.
use crate::{agent::ConfirmTarget, db::AgentDb, mcp::Mcp, providers::ToolCall};
use rusqlite::{OptionalExtension, params};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

// ── 1. store ───────────────────────────────────────────────────────────────
#[derive(Clone, Debug)]
pub struct Pending {
    pub run: String,
    pub key: String,
    pub yes: String,
    pub no: String,
    pub target: ConfirmTarget,
    pub message: Option<i64>,
    pub card: String,
}
pub trait Store: Send + Sync {
    fn get(&self, run: &str, key: &str) -> anyhow::Result<Option<Pending>>;
    fn pending(&self, target: &ConfirmTarget) -> anyhow::Result<Value>;
    fn insert(&self, pending: &Pending) -> anyhow::Result<()>;
    fn sent(&self, run: &str, key: &str, message: i64) -> anyhow::Result<()>;
    /// Resolves using a consumed event, never a YAML claim of approval.
    fn resolve(&self, run: &str, key: &str, claimed: &str) -> anyhow::Result<String>;
}
impl Store for AgentDb {
    fn pending(&self, target: &ConfirmTarget) -> anyhow::Result<Value> {
        let conn = self.conn();
        let mut stmt = conn.prepare("SELECT c.card,c.chat_id,c.thread_id,c.message_id,c.created_at_ms
            FROM confirmations c JOIN workflow_runs r ON r.id = c.run_id
            WHERE c.outcome IS NULL AND c.message_id IS NOT NULL AND r.ended_ms IS NULL
            AND c.chat_id = ?1 AND c.thread_id IS ?2 ORDER BY c.created_at_ms LIMIT 10")?;
        let cards = stmt.query_map(params![target.chat_id,target.message_thread_id], |r| Ok(json!({
            "call":clip(&r.get::<_,String>(0)?,500),"chatId":r.get::<_,String>(1)?,"messageThreadId":r.get::<_,Option<i64>>(2)?,
            "messageId":r.get::<_,i64>(3)?,"since":r.get::<_,i64>(4)?
        })))?.collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(json!(cards))
    }

    fn get(&self, run: &str, key: &str) -> anyhow::Result<Option<Pending>> {
        Ok(self.conn().query_row("SELECT yes_type,no_type,chat_id,thread_id,message_id,card FROM confirmations WHERE run_id = ?1 AND call_key = ?2",
            params![run,key], |r| Ok(Pending { run:run.into(),key:key.into(),yes:r.get(0)?,no:r.get(1)?,target:ConfirmTarget { chat_id:r.get(2)?, message_thread_id:r.get(3)? },message:r.get(4)?,card:r.get(5)? })).optional()?)
    }
    fn insert(&self, p: &Pending) -> anyhow::Result<()> {
        self.conn().execute("INSERT INTO confirmations (run_id,call_key,yes_type,no_type,chat_id,thread_id,card,created_at_ms)
            VALUES (?1,?2,?3,?4,?5,?6,?7,?8)", params![p.run,p.key,p.yes,p.no,p.target.chat_id,p.target.message_thread_id,p.card,chrono::Utc::now().timestamp_millis()])?;
        Ok(())
    }
    fn sent(&self, run: &str, key: &str, message: i64) -> anyhow::Result<()> {
        self.conn().execute("UPDATE confirmations SET message_id = ?3 WHERE run_id = ?1 AND call_key = ?2 AND message_id IS NULL", params![run,key,message])?;
        Ok(())
    }
    fn resolve(&self, run: &str, key: &str, claimed: &str) -> anyhow::Result<String> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let row: Option<(String,String,Option<i64>,String,Option<i64>,Option<String>)> = tx.query_row(
            "SELECT yes_type,no_type,message_id,chat_id,thread_id,outcome FROM confirmations WHERE run_id = ?1 AND call_key = ?2",
            params![run,key], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?))).optional()?;
        let Some((yes,no,message,chat,thread,prior)) = row else { return Ok("unverified".into()) };
        if prior.is_some() { return Ok("unverified".into()) }
        let consumed: Option<(String,String)> = tx.query_row("SELECT type,data FROM workflow_events WHERE run_id = ?1 AND type IN (?2,?3)
            AND consumed_at_ms IS NOT NULL ORDER BY consumed_at_ms,id LIMIT 1", params![run,yes,no], |r| Ok((r.get(0)?,r.get(1)?))).optional()?;
        let verified = consumed.map(|(t,data)| -> anyhow::Result<Option<String>> {
            let v: Value = serde_json::from_str(&data)?;
            let same_chat = v["chatId"].as_str().map(str::to_owned).or_else(|| v["chatId"].as_i64().map(|i| i.to_string())) == Some(chat);
            Ok((same_chat && v["messageId"].as_i64() == message && message.is_some() && v["messageThreadId"].as_i64() == thread)
                .then(|| if t == yes { "confirmed".into() } else { "declined".into() }))
        }).transpose()?.flatten();
        // An early/forged expiry cannot close a live card. A real expiry is
        // determined by host time, independent of the workflow's outcome word.
        let old_enough: bool = tx.query_row("SELECT created_at_ms + 86400000 <= ?3 FROM confirmations WHERE run_id = ?1 AND call_key = ?2",
            params![run,key,chrono::Utc::now().timestamp_millis()], |r| r.get(0))?;
        let outcome = verified.unwrap_or_else(|| if claimed == "expired" && old_enough { "expired".into() } else { "unverified".into() });
        tx.execute("UPDATE confirmations SET outcome = ?3 WHERE run_id = ?1 AND call_key = ?2", params![run,key,outcome])?;
        // Close both tokens, including the decline token if yes won.
        tx.execute("DELETE FROM workflow_awaits WHERE run_id = ?1 AND type IN (?2,?3)", params![run,yes,no])?;
        tx.commit()?;
        Ok(outcome)
    }
}

// ── 2. requests and resolution ─────────────────────────────────────────────
fn clip(s: &str, limit: usize) -> String {
    let mut chars = s.chars();
    let head: String = chars.by_ref().take(limit).collect();
    if chars.next().is_some() { format!("{head}… [truncated]") } else { head }
}
fn token() -> String { format!("cf:{}", uuid::Uuid::new_v4().simple()) }

pub struct Request<'a> {
    pub run: &'a str,
    pub call: &'a ToolCall,
    pub target: &'a ConfirmTarget,
    pub guidance: &'a str,
    pub goal: &'a str,
}
pub async fn request(store: &dyn Store, events: &dyn owf::run::Events, mcp: &dyn Mcp, req: Request<'_>, cancel: &CancellationToken) -> anyhow::Result<Value> {
    let key = crate::guard::key(req.call);
    // A request with unknown send outcome is never dispatched a second time.
    let p = if let Some(p) = store.get(req.run, &key)? { p } else {
        let pretty = serde_json::from_str::<Value>(&req.call.arguments).ok()
            .and_then(|v| serde_json::to_string_pretty(&v).ok()).unwrap_or_else(|| req.call.arguments.clone());
        let p = Pending { run:req.run.into(),key:key.clone(),yes:token(),no:token(),target:req.target.clone(),message:None,
            card:format!("Confirm action: {}\n{}\n\n{}\n\nYour goal: {}",clip(&req.call.name,128),clip(&pretty,1000),clip(req.guidance,300),clip(req.goal,400)) };
        store.insert(&p)?;
        events.register(req.run, &[p.yes.clone(),p.no.clone()])?;
        let raw = mcp.call_tool_once("send_telegram_message", json!({"chatId":p.target.chat_id,"messageThreadId":p.target.message_thread_id,
            "text":p.card,"buttons":[[{"text":"Confirm","data":p.yes},{"text":"Decline","data":p.no}]]}).as_object().unwrap().clone(),cancel).await
            .map_err(|e| anyhow::anyhow!("confirmation card send: {e:?}"))?;
        anyhow::ensure!(!crate::tool_results::is_tool_error(&raw), "{raw}");
        let result: Value = serde_json::from_str(&raw)?;
        let message = result["messageId"].as_i64().ok_or_else(|| anyhow::anyhow!("confirmation card returned no messageId"))?;
        store.sent(req.run, &key, message)?;
        Pending { message:Some(message), ..p }
    };
    anyhow::ensure!(p.message.is_some(), "confirmation card outcome unknown; it will not be sent again");
    Ok(json!({"required":true,"types":[p.yes,p.no]}))
}

pub async fn close_card(store: &dyn Store, mcp: &dyn Mcp, run: &str, call: &ToolCall, outcome: &str, cancel: &CancellationToken) {
    let edit = async {
        if let Some(p) = store.get(run, &crate::guard::key(call))? && let Some(message) = p.message {
            let label = match outcome { "confirmed" => "Confirmed", "declined" => "Declined", "expired" => "Expired", _ => "Not confirmed" };
            let raw = mcp.call_tool_once("edit_telegram_message", json!({"chatId":p.target.chat_id,"messageId":message,"text":format!("{}\n\n{label}",p.card)}).as_object().unwrap().clone(),cancel).await
                .map_err(|e| anyhow::anyhow!("{e:?}"))?;
            anyhow::ensure!(!crate::tool_results::is_tool_error(&raw), "{raw}");
        }
        anyhow::Ok(())
    }.await;
    if let Err(error) = edit { eprintln!("[confirmation] card edit failed: {error:#}"); }
}

// ── 3. tests ───────────────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;
    use owf::run::Events;
    #[test]
    fn approval_is_bound_to_run_exact_call_and_card_and_spent_once() {
        for foreign in [false,true] {
            let db = AgentDb::open(":memory:").unwrap();
            let runs = crate::durable::RunStore::new(db.clone());
            let lease = runs.start("r","agent-loop","x",&json!({})).unwrap().unwrap();
            let p = Pending { run:"r".into(),key:"exact-key".into(),yes:token(),no:token(),target:ConfirmTarget { chat_id:"42".into(),message_thread_id:Some(7) },message:None,card:"card".into() };
            db.insert(&p).unwrap();
            db.sent("r","exact-key",100).unwrap();
            lease.register("r", &[p.yes.clone(),p.no.clone()]).unwrap();
            assert_eq!(db.resolve("r","different-call","confirmed").unwrap(), "unverified");
            assert!(runs.deliver(&p.yes,&json!({"chatId":if foreign {43} else {42},"messageThreadId":7,"messageId":100})).unwrap());
            assert!(lease.take("r","listen#event",&[p.yes,p.no]).unwrap().is_some());
            assert_eq!(db.resolve("other-run","exact-key","confirmed").unwrap(), "unverified");
            assert_eq!(db.resolve("r","exact-key","confirmed").unwrap(), if foreign { "unverified" } else { "confirmed" });
            assert_eq!(db.resolve("r","exact-key","confirmed").unwrap(), "unverified");
        }
    }
    #[test]
    fn tokens_have_random_entropy_and_cards_clip_unicode_visibly() {
        let a = token();
        assert_ne!(a,token());
        assert!(a.len() <= 64);
        assert_eq!(clip("🧾🧾🧾",2),"🧾🧾… [truncated]");
    }
}
