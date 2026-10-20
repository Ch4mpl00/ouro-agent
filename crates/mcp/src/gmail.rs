// Gmail, read-only: NashDom bills arrive as email with a PDF attached.
//
// Sections:
//   1. oauth         — consent URL, code exchange, token refresh + storage
//   2. api           — the few REST calls used (list, get, attachments)
//   3. subscriptions — what to poll and how a hit reads as a signal
//   4. poller        — per-subscription watermark loop → signals
//   5. tools         — `gmail` toolset: list_nashdom_mails, download_gmail_attachment
//
// Plain REST instead of a generated client: four endpoints and a token
// refresh don't justify the dependency. Tokens live in `integration_account`
// exactly as googleapis stored them (expires_at as an ISO string), so the
// account the TS server authorised still works.

use std::path::{Path, PathBuf};
use std::time::Duration;

use base64::Engine;
use base64::engine::DecodePaddingMode;
use base64::engine::general_purpose::{GeneralPurpose, GeneralPurposeConfig};
use chrono::Utc;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::{schemars, tool, tool_router};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio_util::sync::CancellationToken;

use crate::db::Db;
use crate::server::{McpTools, ToolResult, invalid_params, respond};
use crate::signals::SignalSink;
use crate::signals::Signals;
use crate::time::{iso_from_unix_ms, parse_js_date};

const PROVIDER: &str = "gmail";
const SCOPES: &[&str] =
    &["https://www.googleapis.com/auth/gmail.readonly", "https://www.googleapis.com/auth/userinfo.email"];
// googleapis' default; tests point a module at a local fake.
const API_ROOT: &str = "https://gmail.googleapis.com/";
// google-auth-library refreshes this long before expiry.
const EAGER_REFRESH_MS: i64 = 5 * 60 * 1000;

// ── 1. oauth ─────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct GmailModule {
    db: Db,
    http: reqwest::Client,
    // Where the Gmail API lives, with a trailing slash.
    api_root: String,
}

struct OAuthClient {
    client_id: String,
    client_secret: String,
    redirect_uri: String,
}

fn require_env(name: &str) -> anyhow::Result<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty()).ok_or_else(|| anyhow::anyhow!("Missing env var: {name}"))
}

impl OAuthClient {
    fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            client_id: require_env("GOOGLE_CLIENT_ID")?,
            client_secret: require_env("GOOGLE_CLIENT_SECRET")?,
            redirect_uri: require_env("GOOGLE_REDIRECT_URI")?,
        })
    }
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: Option<String>,
    refresh_token: Option<String>,
    expires_in: Option<i64>,
}

struct StoredTokens {
    access_token: Option<String>,
    refresh_token: Option<String>,
    expires_at_ms: Option<i64>,
}

impl GmailModule {
    pub fn new(db: Db, http: reqwest::Client) -> Self {
        Self::with_api_root(db, http, API_ROOT)
    }

    pub fn with_api_root(db: Db, http: reqwest::Client, api_root: &str) -> Self {
        let api_root = if api_root.ends_with('/') { api_root.to_owned() } else { format!("{api_root}/") };
        Self { db, http, api_root }
    }

    pub fn auth_url(&self) -> anyhow::Result<String> {
        let oauth = OAuthClient::from_env()?;
        let mut url = url::Url::parse("https://accounts.google.com/o/oauth2/v2/auth")?;
        url.query_pairs_mut()
            .append_pair("access_type", "offline")
            .append_pair("prompt", "consent")
            .append_pair("scope", &SCOPES.join(" "))
            .append_pair("response_type", "code")
            .append_pair("client_id", &oauth.client_id)
            .append_pair("redirect_uri", &oauth.redirect_uri);
        Ok(url.into())
    }

    pub async fn exchange_code_and_persist(&self, code: &str) -> anyhow::Result<String> {
        let oauth = OAuthClient::from_env()?;
        let tokens = self
            .token_request(&[
                ("grant_type", "authorization_code"),
                ("code", code),
                ("client_id", &oauth.client_id),
                ("client_secret", &oauth.client_secret),
                ("redirect_uri", &oauth.redirect_uri),
            ])
            .await?;
        let access =
            tokens.access_token.clone().ok_or_else(|| anyhow::anyhow!("token response had no access_token"))?;

        #[derive(Deserialize)]
        struct UserInfo {
            email: Option<String>,
        }
        let info: UserInfo = self
            .http
            .get("https://www.googleapis.com/oauth2/v2/userinfo")
            .bearer_auth(&access)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let account = info
            .email
            .ok_or_else(|| anyhow::anyhow!("Could not resolve account email from Google userinfo response"))?;
        self.persist_tokens(&account, &tokens).await?;
        Ok(account)
    }

    async fn token_request(&self, form: &[(&str, &str)]) -> anyhow::Result<TokenResponse> {
        let res = self.http.post("https://oauth2.googleapis.com/token").form(form).send().await?;
        if !res.status().is_success() {
            let status = res.status();
            anyhow::bail!("Google token request failed ({status}): {}", res.text().await.unwrap_or_default());
        }
        Ok(res.json().await?)
    }

    // Overwrites a stored value only with a fresh non-null one: refresh
    // tokens are not always re-issued, and losing one means re-consenting.
    async fn persist_tokens(&self, account: &str, tokens: &TokenResponse) -> anyhow::Result<()> {
        let existing = self.stored_tokens(account).await?;
        let access = tokens.access_token.clone().or(existing.as_ref().and_then(|e| e.access_token.clone()));
        let refresh = tokens.refresh_token.clone().or(existing.as_ref().and_then(|e| e.refresh_token.clone()));
        let expires_at = match tokens.expires_in {
            Some(secs) => iso_from_unix_ms(Utc::now().timestamp_millis() + secs * 1000),
            None => existing.and_then(|e| e.expires_at_ms).and_then(iso_from_unix_ms),
        };
        self.db
            .client()
            .await?
            .execute(
                "INSERT INTO integration_account (provider, account_key, access_token, refresh_token, expires_at)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT (provider, account_key) DO UPDATE SET
                   access_token = EXCLUDED.access_token,
                   refresh_token = EXCLUDED.refresh_token,
                   expires_at = EXCLUDED.expires_at,
                   updated_at = now()",
                &[&PROVIDER, &account, &access, &refresh, &expires_at],
            )
            .await?;
        Ok(())
    }

    async fn stored_tokens(&self, account: &str) -> anyhow::Result<Option<StoredTokens>> {
        let row = self
            .db
            .client()
            .await?
            .query_opt(
                "SELECT access_token, refresh_token, expires_at FROM integration_account
                 WHERE provider = $1 AND account_key = $2",
                &[&PROVIDER, &account],
            )
            .await?;
        Ok(row.map(|r| StoredTokens {
            access_token: r.get(0),
            refresh_token: r.get(1),
            expires_at_ms: r
                .get::<_, Option<String>>(2)
                .as_deref()
                .and_then(parse_js_date)
                .map(|t| t.timestamp_millis()),
        }))
    }

    // GMAIL_ACCOUNT_KEY wins; otherwise the most recently authorised account.
    pub async fn resolve_account_key(&self) -> anyhow::Result<Option<String>> {
        if let Some(key) = std::env::var("GMAIL_ACCOUNT_KEY").ok().filter(|k| !k.is_empty()) {
            return Ok(Some(key));
        }
        let row = self
            .db
            .client()
            .await?
            .query_opt(
                "SELECT account_key FROM integration_account WHERE provider = 'gmail' ORDER BY created_at DESC LIMIT 1",
                &[],
            )
            .await?;
        Ok(row.map(|r| r.get(0)))
    }

    async fn require_account_key(&self) -> anyhow::Result<String> {
        self.resolve_account_key()
            .await?
            .ok_or_else(|| anyhow::anyhow!("No authorized Gmail account; run `gmail-auth`."))
    }

    async fn access_token(&self, account: &str, force_refresh: bool) -> anyhow::Result<String> {
        let stored = self
            .stored_tokens(account)
            .await?
            .ok_or_else(|| anyhow::anyhow!("No Gmail account \"{account}\". Run `gmail-auth` to authorize."))?;
        let refresh = stored
            .refresh_token
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Gmail account \"{account}\" has no refresh token. Re-authorize."))?;
        let fresh = stored.expires_at_ms.is_some_and(|exp| exp - EAGER_REFRESH_MS > Utc::now().timestamp_millis());
        if !force_refresh
            && fresh
            && let Some(access) = stored.access_token
        {
            return Ok(access);
        }
        let oauth = OAuthClient::from_env()?;
        let tokens = self
            .token_request(&[
                ("grant_type", "refresh_token"),
                ("refresh_token", &refresh),
                ("client_id", &oauth.client_id),
                ("client_secret", &oauth.client_secret),
            ])
            .await?;
        self.persist_tokens(account, &tokens).await?;
        tokens.access_token.ok_or_else(|| anyhow::anyhow!("token refresh returned no access_token"))
    }

    // ── 2. api ───────────────────────────────────────────────────────────────

    async fn get<T: serde::de::DeserializeOwned>(
        &self,
        account: &str,
        path: &str,
        query: &[(&str, &str)],
    ) -> anyhow::Result<T> {
        let mut force_refresh = false;
        loop {
            let token = self.access_token(account, force_refresh).await?;
            let res = self
                .http
                .get(format!("{}gmail/v1/users/me{path}", self.api_root))
                .bearer_auth(token)
                .query(query)
                .timeout(Duration::from_secs(60))
                .send()
                .await?;
            // A revoked/rotated access token: refresh once and retry.
            if res.status() == reqwest::StatusCode::UNAUTHORIZED && !force_refresh {
                force_refresh = true;
                continue;
            }
            if !res.status().is_success() {
                let status = res.status();
                anyhow::bail!("Gmail {path} failed ({status}): {}", res.text().await.unwrap_or_default());
            }
            return Ok(res.json().await?);
        }
    }

    pub async fn list_messages(
        &self,
        account: &str,
        query: &str,
        max_results: u32,
        page_token: Option<&str>,
    ) -> anyhow::Result<Page> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct ListResponse {
            messages: Option<Vec<IdOnly>>,
            next_page_token: Option<String>,
        }
        #[derive(Deserialize)]
        struct IdOnly {
            id: Option<String>,
        }
        let max = max_results.to_string();
        let mut q = vec![("q", query), ("maxResults", max.as_str())];
        if let Some(token) = page_token {
            q.push(("pageToken", token));
        }
        let list: ListResponse = self.get(account, "/messages", &q).await?;
        let ids: Vec<String> = list.messages.unwrap_or_default().into_iter().filter_map(|m| m.id).collect();
        let summaries = futures::future::try_join_all(ids.iter().map(|id| self.summary(account, id))).await?;
        Ok(Page { messages: summaries, next_page_token: list.next_page_token })
    }

    async fn summary(&self, account: &str, id: &str) -> anyhow::Result<MessageSummary> {
        let query = [
            ("format", "metadata"),
            ("metadataHeaders", "From"),
            ("metadataHeaders", "To"),
            ("metadataHeaders", "Subject"),
            ("metadataHeaders", "Date"),
        ];
        let raw: RawMessage = self.get(account, &format!("/messages/{id}"), &query).await?;
        raw.summary()
    }

    // The full MIME tree — what attachment discovery walks.
    pub async fn raw_message(&self, account: &str, id: &str) -> anyhow::Result<RawMessage> {
        self.get(account, &format!("/messages/{id}"), &[("format", "full")]).await
    }

    pub async fn attachment_data(
        &self,
        account: &str,
        message_id: &str,
        attachment_id: &str,
    ) -> anyhow::Result<Vec<u8>> {
        #[derive(Deserialize)]
        struct Body {
            data: Option<String>,
        }
        let body: Body = self.get(account, &format!("/messages/{message_id}/attachments/{attachment_id}"), &[]).await?;
        let data = body.data.ok_or_else(|| anyhow::anyhow!("Empty attachment data: {attachment_id}"))?;
        Ok(BASE64URL.decode(data.trim())?)
    }
}

// Gmail emits base64url, sometimes padded and sometimes not.
const BASE64URL: GeneralPurpose = GeneralPurpose::new(
    &base64::alphabet::URL_SAFE,
    GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawMessage {
    pub id: Option<String>,
    pub thread_id: Option<String>,
    pub snippet: Option<String>,
    pub internal_date: Option<String>,
    pub label_ids: Option<Vec<String>>,
    pub payload: Option<MessagePart>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessagePart {
    pub mime_type: Option<String>,
    pub filename: Option<String>,
    pub headers: Option<Vec<Header>>,
    pub body: Option<PartBody>,
    pub parts: Option<Vec<MessagePart>>,
}

#[derive(Debug, Deserialize)]
pub struct Header {
    pub name: Option<String>,
    pub value: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartBody {
    pub attachment_id: Option<String>,
    pub size: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageSummary {
    pub id: String,
    pub thread_id: String,
    pub snippet: String,
    pub from: Option<String>,
    pub to: Option<String>,
    pub subject: Option<String>,
    pub date: Option<String>,
    pub internal_date: Option<String>,
    pub label_ids: Vec<String>,
}

pub struct Page {
    pub messages: Vec<MessageSummary>,
    pub next_page_token: Option<String>,
}

impl RawMessage {
    fn header(&self, name: &str) -> Option<String> {
        let headers = self.payload.as_ref()?.headers.as_ref()?;
        headers.iter().find(|h| h.name.as_deref().is_some_and(|n| n.eq_ignore_ascii_case(name)))?.value.clone()
    }

    fn summary(&self) -> anyhow::Result<MessageSummary> {
        let (Some(id), Some(thread_id)) = (self.id.clone(), self.thread_id.clone()) else {
            anyhow::bail!("Gmail returned a message without id/threadId");
        };
        Ok(MessageSummary {
            id,
            thread_id,
            snippet: self.snippet.clone().unwrap_or_default(),
            from: self.header("From"),
            to: self.header("To"),
            subject: self.header("Subject"),
            date: self.header("Date"),
            internal_date: self.internal_date.clone(),
            label_ids: self.label_ids.clone().unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AttachmentRef {
    pub attachment_id: String,
    pub filename: String,
    pub mime_type: String,
    pub size_bytes: i64,
}

// Every MIME part carrying an attachmentId; inline body parts are skipped.
pub fn find_attachments(message: &RawMessage) -> Vec<AttachmentRef> {
    fn walk(part: &MessagePart, out: &mut Vec<AttachmentRef>) {
        if let Some(body) = &part.body
            && let Some(id) = &body.attachment_id
        {
            out.push(AttachmentRef {
                attachment_id: id.clone(),
                filename: part.filename.clone().filter(|f| !f.is_empty()).unwrap_or_else(|| "untitled".into()),
                mime_type: part
                    .mime_type
                    .clone()
                    .filter(|m| !m.is_empty())
                    .unwrap_or_else(|| "application/octet-stream".into()),
                size_bytes: body.size.unwrap_or(0),
            });
        }
        for child in part.parts.iter().flatten() {
            walk(child, out);
        }
    }
    let mut out = Vec::new();
    if let Some(payload) = &message.payload {
        walk(payload, &mut out);
    }
    out
}

fn is_pdf(att: &AttachmentRef) -> bool {
    att.mime_type == "application/pdf" || att.filename.to_lowercase().ends_with(".pdf")
}

// ── 3. subscriptions ─────────────────────────────────────────────────────────

// Adding an email-driven signal type = one entry here + a matching
// `skills/<signal_source>.md`. The poller is generic.
pub struct Subscription {
    // Internal id; keys the watermark.
    pub name: &'static str,
    pub query: &'static str,
    // signal.source → skills/<signal_source>.md
    pub signal_source: &'static str,
    pub interval: Duration,
    pub build_content: fn(&MessageSummary, &[AttachmentRef]) -> String,
}

// All NashDom mail, by sender or subject. Real bills come from
// nashdom*@gmail.com with a Cyrillic subject and a PDF; other NashDom mail
// (announcements, replies) is surfaced too.
const NASHDOM_QUERY: &str = "from:nashdom OR subject:nashdom";

pub const SUBSCRIPTIONS: &[Subscription] = &[Subscription {
    name: "nashdom-bill",
    query: NASHDOM_QUERY,
    signal_source: "nashdom-bill",
    interval: Duration::from_secs(60),
    build_content: nashdom_content,
}];

fn format_attachment(att: &AttachmentRef) -> String {
    format!(
        "  - attachmentId: {}\n    filename: {}\n    mimeType: {}\n    sizeBytes: {}",
        att.attachment_id, att.filename, att.mime_type, att.size_bytes
    )
}

fn nashdom_content(m: &MessageSummary, attachments: &[AttachmentRef]) -> String {
    let meta = [
        format!("Subject: {}", m.subject.as_deref().unwrap_or("(no subject)")),
        format!("From: {}", m.from.as_deref().unwrap_or("(unknown)")),
        format!("Date: {}", m.date.as_deref().or(m.internal_date.as_deref()).unwrap_or("(unknown)")),
        format!("messageId: {}", m.id),
    ];
    let pdfs: Vec<&AttachmentRef> = attachments.iter().filter(|a| is_pdf(a)).collect();
    let mut lines: Vec<String> = Vec::new();
    if pdfs.is_empty() {
        lines.push("A new NashDom email arrived without attachments: forward its subject and a short summary to the user in Telegram.".into());
        lines.push(String::new());
        lines.extend(meta);
        lines.push(format!("Snippet: {}", m.snippet));
        lines.push(String::new());
        lines.push("Step: send_telegram_message(text).".into());
    } else {
        lines.push("A new NashDom bill arrived. Download the PDF, read it and send the user a short summary in Telegram (bill type, period, 2–5 key line items, total).".into());
        lines.push(String::new());
        lines.extend(meta);
        lines.push("Attachments:".into());
        lines.extend(pdfs.iter().map(|a| format_attachment(a)));
        lines.push(String::new());
        lines.push(
            "Steps: download_gmail_attachment(messageId, attachmentId) → read_pdf(filePath) → send_telegram_message(text)."
                .into(),
        );
    }
    lines.join("\n")
}

// ── 4. poller ────────────────────────────────────────────────────────────────

impl GmailModule {
    async fn watermark(&self, sub: &Subscription) -> anyhow::Result<Option<String>> {
        let row = self
            .db
            .client()
            .await?
            .query_opt("SELECT value FROM gmail_kv WHERE key = $1", &[&watermark_key(sub)])
            .await?;
        Ok(row.map(|r| r.get(0)))
    }

    async fn set_watermark(&self, sub: &Subscription, value: i64) -> anyhow::Result<()> {
        self.db
            .client()
            .await?
            .execute(
                "INSERT INTO gmail_kv (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                &[&watermark_key(sub), &value.to_string()],
            )
            .await?;
        Ok(())
    }

    // First run on a fresh install sets the watermark to now and emits
    // nothing, rather than flooding the queue with years of old mail.
    // Otherwise: query with `after:` (every page), take rows newer than the
    // watermark, oldest first, and enqueue one signal per row, advancing the
    // watermark past each one as it goes.
    async fn poll(&self, sub: &Subscription, account: &str, signals: &dyn SignalSink) -> anyhow::Result<()> {
        let Some(watermark) = self.watermark(sub).await? else {
            self.set_watermark(sub, Utc::now().timestamp_millis()).await?;
            tracing::info!(subscription = sub.name, "bootstrapping gmail watermark, no emit");
            return Ok(());
        };
        let watermark_ms: i64 = watermark.parse().unwrap_or(0);
        let query = format!("{} after:{}", sub.query, watermark_ms / 1000);
        // Every page: Gmail lists newest first, so after an outage with more
        // new mail than one page, stopping at the first page kept the newest,
        // moved the watermark past them and lost the older ones for good.
        let mut messages = Vec::new();
        let mut page_token: Option<String> = None;
        loop {
            let page = self.list_messages(account, &query, 50, page_token.as_deref()).await?;
            messages.extend(page.messages);
            match page.next_page_token {
                Some(token) => page_token = Some(token),
                None => break,
            }
        }
        let internal = |m: &MessageSummary| m.internal_date.as_deref().and_then(|d| d.parse::<i64>().ok()).unwrap_or(0);
        // Chronological, so signals arrive in order and the watermark only advances.
        messages.sort_by_key(internal);

        let mut emitted = 0;
        for m in &messages {
            let at = internal(m);
            // `after:` is second-granular and non-strict: dedupe here.
            if at <= watermark_ms {
                continue;
            }
            // Attachment refs inline, so the signal is self-contained.
            let attachments = find_attachments(&self.raw_message(account, &m.id).await?);
            signals.record(sub.signal_source, &(sub.build_content)(m, &attachments)).await?;
            // Past each mail as soon as it is signalled: a failure further down
            // the batch resumes after it next tick instead of signalling it again.
            self.set_watermark(sub, at).await?;
            emitted += 1;
        }
        tracing::info!(subscription = sub.name, emitted, "gmail poll done");
        Ok(())
    }

    // One poll of one subscription, as the interval runs it. Errors are the
    // poll's; the interval logs them and tries again next time.
    pub async fn poll_subscription_once(&self, sub: &Subscription, signals: &dyn SignalSink) -> anyhow::Result<()> {
        let Some(account) = self.resolve_account_key().await? else {
            tracing::warn!(subscription = sub.name, "no Gmail account authorized — skipping");
            return Ok(());
        };
        self.poll(sub, &account, signals).await
    }

    pub async fn run_poller(self, signals: Signals, cancel: CancellationToken) {
        let tasks = SUBSCRIPTIONS.iter().map(|sub| {
            let (gmail, signals, cancel) = (self.clone(), signals.clone(), cancel.clone());
            async move {
                tracing::info!(subscription = sub.name, every = ?sub.interval, source = sub.signal_source, "gmail poller started");
                let mut interval = tokio::time::interval(sub.interval);
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => return,
                        _ = interval.tick() => {}
                    }
                    if let Err(err) = gmail.poll_subscription_once(sub, &signals).await {
                        tracing::error!(subscription = sub.name, error = %format!("{err:#}"), "gmail poll failed");
                    }
                }
            }
        });
        futures::future::join_all(tasks).await;
    }
}

fn watermark_key(sub: &Subscription) -> String {
    format!("subscription.{}.last_internal_date_ms", sub.name)
}

// ── 5. tools ─────────────────────────────────────────────────────────────────

// Bills dated before this are already settled — tracking began here. Said in
// the tool description so the model never suggests paying an old invoice.
const PAYMENT_TRACKING_SINCE: &str = "2026-05";

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ListParams {
    #[schemars(range(min = 1, max = 100))]
    limit: Option<u32>,
    /// Continuation token from a previous call's nextPageToken.
    page_token: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct DownloadParams {
    message_id: String,
    attachment_id: String,
    /// Suggested filename for the saved file. Defaults to 'attachment.pdf'.
    filename: Option<String>,
}

fn sanitize(name: &str) -> String {
    let re = regex::Regex::new(r"[/\\\x00\n\r]+").expect("valid regex");
    let cleaned = re.replace_all(name, "_").trim().to_owned();
    if cleaned.is_empty() { "untitled".into() } else { cleaned }
}

pub fn attachment_path(
    storage: &Path,
    account: &str,
    message_id: &str,
    attachment_id: &str,
    filename: Option<&str>,
) -> PathBuf {
    let prefix: String = attachment_id.chars().filter(char::is_ascii_alphanumeric).take(12).collect();
    let name = sanitize(filename.unwrap_or("attachment.pdf"));
    storage.join("gmail").join(sanitize(account)).join(message_id).join(format!("{prefix}_{name}"))
}

#[tool_router(router = gmail_tools, vis = "pub(crate)")]
impl McpTools {
    #[tool(
        name = "list_nashdom_mails",
        annotations(read_only_hint = true, open_world_hint = false),
        title = "List NashDom mails",
        description = "List ALL NashDom-related emails (sender or subject match), newest \
            first. Returns message metadata (subject, from, date, snippet) and \
            PDF attachment refs if any. Most utility bills will have a PDF \
            attachment, but non-bill mail (announcements, replies) is also \
            returned with an empty `attachments` array. The billing period is \
            in the subject (Ukrainian) and `date` field; deduce from there \
            which bill is which. No side effects — call download_gmail_attachment \
            to fetch a specific PDF. \
            IMPORTANT: payment tracking started 2026-05; bills \
            with an earlier billing period are considered already settled — do \
            NOT suggest the user pay them. \
            Pagination: pass `pageToken` from a previous response's \
            `nextPageToken` to get the next page."
    )]
    async fn list_nashdom_mails(&self, Parameters(p): Parameters<ListParams>) -> ToolResult {
        if p.limit.is_some_and(|l| !(1..=100).contains(&l)) {
            return Err(invalid_params("limit must be between 1 and 100"));
        }
        let gmail = &self.deps.gmail;
        respond(
            async {
                let account = gmail.require_account_key().await?;
                let page = gmail
                    .list_messages(&account, NASHDOM_QUERY, p.limit.unwrap_or(25), p.page_token.as_deref())
                    .await?;
                let messages = futures::future::try_join_all(page.messages.iter().map(|m| async {
                    let pdfs: Vec<AttachmentRef> = find_attachments(&gmail.raw_message(&account, &m.id).await?)
                        .into_iter()
                        .filter(is_pdf)
                        .collect();
                    anyhow::Ok(json!({
                        "messageId": m.id,
                        "subject": m.subject,
                        "from": m.from,
                        "date": m.date,
                        "snippet": m.snippet,
                        "attachments": pdfs,
                    }))
                }))
                .await?;
                Ok(json!({
                    "accountKey": account,
                    "query": NASHDOM_QUERY,
                    "paymentTrackingSince": PAYMENT_TRACKING_SINCE,
                    "messages": messages,
                    "nextPageToken": page.next_page_token,
                }))
            }
            .await,
        )
    }

    #[tool(
        name = "download_gmail_attachment",
        // Writes only a cache of what it reads: repeating it changes nothing.
        annotations(read_only_hint = true, open_world_hint = false),
        title = "Download a Gmail attachment",
        description = "Save a Gmail attachment to local storage and return the absolute filePath. Use \
            after list_nashdom_mails to fetch a specific PDF, then read it with the Read tool \
            to extract bill fields."
    )]
    async fn download_gmail_attachment(&self, Parameters(p): Parameters<DownloadParams>) -> ToolResult {
        // Gmail ids are base64url. Anything else is refused before Gmail is
        // asked: the message id names a storage directory, so where a file
        // lands must not depend on the API rejecting a malformed id.
        let gmail_id = regex::Regex::new("^[A-Za-z0-9_-]+$").expect("valid regex");
        if !gmail_id.is_match(&p.message_id) {
            return Err(invalid_params("messageId: not a Gmail message id"));
        }
        if !gmail_id.is_match(&p.attachment_id) {
            return Err(invalid_params("attachmentId: not a Gmail attachment id"));
        }
        let gmail = &self.deps.gmail;
        let storage = &self.deps.storage_dir;
        respond(
            async {
                let account = gmail.require_account_key().await?;
                let bytes = gmail.attachment_data(&account, &p.message_id, &p.attachment_id).await?;
                let path = attachment_path(storage, &account, &p.message_id, &p.attachment_id, p.filename.as_deref());
                if let Some(dir) = path.parent() {
                    tokio::fs::create_dir_all(dir).await?;
                }
                tokio::fs::write(&path, &bytes).await?;
                let file_path = std::path::absolute(&path)?;
                Ok(json!({ "filePath": file_path, "sizeBytes": bytes.len() }))
            }
            .await,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message() -> RawMessage {
        serde_json::from_value(json!({
            "id": "m1",
            "threadId": "t1",
            "snippet": "Bill",
            "internalDate": "1780000000000",
            "payload": {
                "mimeType": "multipart/mixed",
                "headers": [{ "name": "subject", "value": "Bill for April" }, { "name": "From", "value": "nashdom@x" }],
                "parts": [
                    { "mimeType": "text/plain", "body": { "size": 10 } },
                    { "mimeType": "application/pdf", "filename": "bill.pdf", "body": { "attachmentId": "ANGjdJ_9-x", "size": 2048 } },
                    { "mimeType": "multipart/related", "parts": [
                        { "mimeType": "image/png", "filename": "", "body": { "attachmentId": "img", "size": 5 } }
                    ] }
                ]
            }
        }))
        .unwrap()
    }

    #[test]
    fn walks_the_mime_tree_for_attachments() {
        let found = find_attachments(&message());
        assert_eq!(found.len(), 2);
        assert_eq!(found[0].filename, "bill.pdf");
        assert_eq!(found[1].filename, "untitled");
        assert_eq!(found.iter().filter(|a| is_pdf(a)).count(), 1);
    }

    #[test]
    fn headers_match_case_insensitively() {
        let summary = message().summary().unwrap();
        assert_eq!(summary.subject.as_deref(), Some("Bill for April"));
        assert_eq!(summary.to, None);
    }

    #[test]
    fn bill_signal_lists_the_pdf_and_the_steps() {
        let msg = message();
        let content = nashdom_content(&msg.summary().unwrap(), &find_attachments(&msg));
        assert!(content.starts_with("A new NashDom bill arrived."));
        assert!(content.contains(
            "  - attachmentId: ANGjdJ_9-x\n    filename: bill.pdf\n    mimeType: application/pdf\n    sizeBytes: 2048"
        ));
        assert!(content.contains("Date: 1780000000000"));
        assert!(!content.contains("img"));
    }

    #[test]
    fn attachment_paths_are_sanitised_and_prefixed() {
        let path = attachment_path(Path::new("storage"), "me@x.com", "m1", "ANGjdJ_9-x/more", Some("../evil\n.pdf"));
        assert_eq!(path, Path::new("storage/gmail/me@x.com/m1/ANGjdJ9xmore_.._evil_.pdf"));
    }

    #[test]
    fn decodes_padded_and_unpadded_base64url() {
        assert_eq!(BASE64URL.decode("aGk").unwrap(), b"hi");
        assert_eq!(BASE64URL.decode("aGk=").unwrap(), b"hi");
    }
}
