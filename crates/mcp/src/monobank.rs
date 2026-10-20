// Monobank Personal API: a statement on demand. No poller — reactive only.
// Auth is the personal token from MONOBANK_API_KEY in the X-Token header;
// the rate limit is one statement request per 60s per account.
//
// Sections:
//   1. client — statement fetch + normalisation to major units
//   2. tools  — `monobank` toolset: list_monobank_transactions

use chrono::{DateTime, Duration, Utc};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::{schemars, tool, tool_router};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::json;

use crate::server::{McpTools, ToolResult, invalid_params, respond};
use crate::time::{iso, iso_from_unix};

// ── 1. client ────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct Monobank {
    http: reqwest::Client,
    api_key: Option<String>,
    // Where the API lives; tests point it at a stand-in.
    api_root: String,
}

// A statement holds at most this many transactions, newest first; when it is
// full, older ones in the range need another request with an earlier `to`.
pub const STATEMENT_PAGE_LIMIT: usize = 500;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawItem {
    id: String,
    time: i64,
    description: String,
    mcc: i64,
    amount: i64,
    operation_amount: i64,
    currency_code: i64,
    cashback_amount: i64,
    comment: Option<String>,
    receipt_id: Option<String>,
    invoice_id: Option<String>,
    counter_edrpou: Option<String>,
    counter_iban: Option<String>,
    counter_name: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    id: String,
    time: Option<String>,
    description: String,
    comment: Option<String>,
    mcc: i64,
    #[serde(serialize_with = "js_number")]
    amount: f64,
    #[serde(serialize_with = "js_number")]
    operation_amount: f64,
    currency: String,
    #[serde(serialize_with = "js_number")]
    cashback_amount: f64,
    receipt_id: Option<String>,
    invoice_id: Option<String>,
    counter_iban: Option<String>,
    counter_name: Option<String>,
    counter_edrpou: Option<String>,
}

// JSON.stringify writes 100, not 100.0 — keep amounts looking the same.
fn js_number<S: Serializer>(value: &f64, s: S) -> Result<S::Ok, S::Error> {
    if value.fract() == 0.0 && value.abs() < 9_007_199_254_740_992.0 {
        s.serialize_i64(*value as i64)
    } else {
        s.serialize_f64(*value)
    }
}

pub fn iso_currency(code: i64) -> String {
    match code {
        980 => "UAH",
        840 => "USD",
        978 => "EUR",
        826 => "GBP",
        985 => "PLN",
        124 => "CAD",
        756 => "CHF",
        392 => "JPY",
        156 => "CNY",
        643 => "RUB",
        _ => return code.to_string(),
    }
    .to_owned()
}

// Minor units (kopecks/cents). Every currency we expect uses 100, so no
// per-currency exponent table.
const MINOR_UNIT: f64 = 100.0;
const MAX_RANGE_SECS: i64 = 31 * 24 * 60 * 60;

impl From<RawItem> for Transaction {
    fn from(raw: RawItem) -> Self {
        Self {
            id: raw.id,
            time: iso_from_unix(raw.time),
            description: raw.description,
            comment: raw.comment,
            mcc: raw.mcc,
            amount: raw.amount as f64 / MINOR_UNIT,
            operation_amount: raw.operation_amount as f64 / MINOR_UNIT,
            currency: iso_currency(raw.currency_code),
            cashback_amount: raw.cashback_amount as f64 / MINOR_UNIT,
            receipt_id: raw.receipt_id,
            invoice_id: raw.invoice_id,
            counter_iban: raw.counter_iban,
            counter_name: raw.counter_name,
            counter_edrpou: raw.counter_edrpou,
        }
    }
}

impl Monobank {
    pub fn new(http: reqwest::Client) -> Self {
        let api_key = std::env::var("MONOBANK_API_KEY").ok().filter(|k| !k.is_empty());
        Self::with_api(http, api_key, "https://api.monobank.ua")
    }

    pub fn with_api(http: reqwest::Client, api_key: Option<String>, api_root: &str) -> Self {
        Self { http, api_key, api_root: api_root.trim_end_matches('/').to_owned() }
    }

    pub async fn statement(
        &self,
        account: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> anyhow::Result<Vec<Transaction>> {
        let key = self.api_key.as_deref().ok_or_else(|| anyhow::anyhow!("MONOBANK_API_KEY is not set in .env"))?;
        let (from_s, to_s) = (from.timestamp(), to.timestamp());
        if to_s - from_s > MAX_RANGE_SECS {
            anyhow::bail!(
                "Monobank statement range cannot exceed 31 days (got {:.1}d)",
                (to_s - from_s) as f64 / 86400.0
            );
        }
        let path = format!("/personal/statement/{}/{from_s}/{to_s}", urlencode(account));
        let res = self.http.get(format!("{}{path}", self.api_root)).header("X-Token", key).send().await?;
        if res.status().as_u16() == 429 {
            anyhow::bail!("Monobank rate limit hit (1 request per 60s per account). Try again later.");
        }
        if !res.status().is_success() {
            let status = res.status();
            let detail = res.text().await.unwrap_or_default();
            let detail =
                if detail.is_empty() { status.canonical_reason().unwrap_or_default().to_owned() } else { detail };
            anyhow::bail!("Monobank {path} failed ({}): {detail}", status.as_u16());
        }
        let raw: Vec<RawItem> = res.json().await?;
        Ok(raw.into_iter().map(Transaction::from).collect())
    }

    pub async fn recent(&self, account: &str, days: i64) -> anyhow::Result<serde_json::Value> {
        let to = Utc::now();
        let from = to - Duration::days(days);
        let transactions = self.statement(account, from, to).await?;
        // A full page means Monobank cut the list: there may be older ones.
        let complete = transactions.len() < STATEMENT_PAGE_LIMIT;
        let mut out = json!({
            "accountId": account,
            "from": iso(from),
            "to": iso(to),
            "days": days,
            "count": transactions.len(),
            "complete": complete,
        });
        if !complete {
            out["oldestReturned"] = json!(transactions.last().and_then(|t| t.time.clone()));
        }
        out["transactions"] = json!(transactions);
        Ok(out)
    }
}

fn urlencode(segment: &str) -> String {
    url::form_urlencoded::byte_serialize(segment.as_bytes()).collect::<String>().replace('+', "%20")
}

// ── 2. tools ─────────────────────────────────────────────────────────────────

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ListParams {
    /// Monobank account id, or '0' for default UAH. Defaults to '0'.
    account_id: Option<String>,
    /// Lookback window in days (default 7, max 31).
    #[schemars(range(min = 1, max = 31))]
    days: Option<i64>,
}

#[tool_router(router = monobank_tools, vis = "pub(crate)")]
impl McpTools {
    #[tool(
        name = "list_monobank_transactions",
        annotations(read_only_hint = true),
        title = "List Monobank transactions",
        description = "Fetch recent transactions for a Monobank account. accountId can be a specific \
            account.id or '0' for the default UAH account. days is the lookback window \
            (default 7, max 31). Rate limit: 1 request per 60s per account — surface 429s \
            rather than retrying. Monobank returns at most 500 transactions, newest first: \
            `complete: false` means older ones in the window were left out (the list stops at \
            `oldestReturned`), so don't total the window from it."
    )]
    async fn list_monobank_transactions(&self, Parameters(p): Parameters<ListParams>) -> ToolResult {
        if p.days.is_some_and(|d| !(1..=31).contains(&d)) {
            return Err(invalid_params("days must be between 1 and 31"));
        }
        let account = p.account_id.unwrap_or_else(|| "0".into());
        respond(self.deps.monobank.recent(&account, p.days.unwrap_or(7)).await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalises_minor_units_and_currency() {
        let raw: RawItem = serde_json::from_value(json!({
            "id": "x", "time": 1780000000, "description": "Silpo", "mcc": 5411, "originalMcc": 5411,
            "hold": false, "amount": -12550, "operationAmount": -10000, "currencyCode": 980,
            "commissionRate": 0, "cashbackAmount": 0, "balance": 100000
        }))
        .unwrap();
        let tx = serde_json::to_value(Transaction::from(raw)).unwrap();
        assert_eq!(tx["amount"], json!(-125.5));
        // 100.0 must print as 100, the way JSON.stringify did.
        assert_eq!(serde_json::to_string(&tx["operationAmount"]).unwrap(), "-100");
        assert_eq!(tx["currency"], "UAH");
        assert_eq!(tx["comment"], serde_json::Value::Null);
        assert_eq!(iso_currency(999), "999");
    }
}
