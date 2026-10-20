// The personal Telegram account, read-only, over MTProto (grammers). It reads
// the channels the user is subscribed to; the news poller harvests them.
//
// Sections:
//   1. session — the stored credential, in gramjs StringSession format
//   2. client  — lazy connection, dialogs, channel history
//   3. login   — the one-time interactive flow behind `userbot-auth`
//   4. tools   — `userbot` toolset: list_userbot_dialogs
//
// The session string is the long-lived credential: whoever has it has the
// account. It lives only in the `mcp_state` database. It is kept in the exact
// format gramjs wrote ("1" + base64(dc, address, port, 256-byte key)), so the
// session the TS server logged in with still works, no re-login.

use std::net::{Ipv4Addr, SocketAddrV4, SocketAddrV6};
use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use chrono::{DateTime, Utc};
use grammers_client::peer::Peer;
use grammers_client::session::storages::MemorySession;
use grammers_client::session::types::{DcOption, PeerRef};
use grammers_client::session::{Session, SessionData};
use grammers_client::{Client, SenderPool, SignInError, tl};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::{schemars, tool, tool_router};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::Mutex;

use crate::db::Db;
use crate::server::{McpTools, ToolResult, invalid_params, respond};

const PROVIDER: &str = "telegram_userbot";

// ── 1. session ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct StringSession {
    pub dc_id: i32,
    pub address: String,
    pub port: u16,
    pub auth_key: [u8; 256],
}

impl StringSession {
    pub fn decode(raw: &str) -> anyhow::Result<Self> {
        let body = raw.strip_prefix('1').ok_or_else(|| anyhow::anyhow!("unsupported session string version"))?;
        let bytes = STANDARD.decode(body.trim())?;
        anyhow::ensure!(bytes.len() > 3 + 2 + 256, "session string too short");
        let dc_id = i32::from(bytes[0]);
        let len = usize::from(u16::from_be_bytes([bytes[1], bytes[2]]));
        let address =
            std::str::from_utf8(bytes.get(3..3 + len).ok_or_else(|| anyhow::anyhow!("truncated address"))?)?.to_owned();
        let port_at = 3 + len;
        let port = u16::from_be_bytes([bytes[port_at], bytes[port_at + 1]]);
        let key: [u8; 256] =
            bytes[port_at + 2..].try_into().map_err(|_| anyhow::anyhow!("auth key must be 256 bytes"))?;
        Ok(Self { dc_id, address, port, auth_key: key })
    }

    pub fn encode(&self) -> String {
        let mut bytes = Vec::with_capacity(1 + 2 + self.address.len() + 2 + 256);
        bytes.push(u8::try_from(self.dc_id).unwrap_or_default());
        bytes.extend_from_slice(&(self.address.len() as u16).to_be_bytes());
        bytes.extend_from_slice(self.address.as_bytes());
        bytes.extend_from_slice(&self.port.to_be_bytes());
        bytes.extend_from_slice(&self.auth_key);
        format!("1{}", STANDARD.encode(bytes))
    }

    // A grammers session whose home DC carries this key; the other DCs keep
    // grammers' built-in addresses and get keys on demand.
    fn into_session(self) -> anyhow::Result<MemorySession> {
        let mut data = SessionData { home_dc: self.dc_id, ..SessionData::default() };
        let ipv4: Ipv4Addr =
            self.address.parse().map_err(|_| anyhow::anyhow!("session DC address is not IPv4: {}", self.address))?;
        let ipv6 = data
            .dc_options
            .get(&self.dc_id)
            .map(|o| o.ipv6)
            .unwrap_or_else(|| SocketAddrV6::new(ipv4.to_ipv6_mapped(), self.port, 0, 0));
        data.dc_options.insert(
            self.dc_id,
            DcOption { id: self.dc_id, ipv4: SocketAddrV4::new(ipv4, self.port), ipv6, auth_key: Some(self.auth_key) },
        );
        Ok(MemorySession::from(data))
    }

    fn from_session(session: &MemorySession) -> anyhow::Result<Self> {
        let dc_id = session.home_dc_id()?;
        let option = session.dc_option(dc_id)?.ok_or_else(|| anyhow::anyhow!("no DC option for home DC {dc_id}"))?;
        let auth_key =
            option.auth_key.ok_or_else(|| anyhow::anyhow!("home DC has no auth key — login did not complete"))?;
        Ok(Self { dc_id, address: option.ipv4.ip().to_string(), port: option.ipv4.port(), auth_key })
    }
}

pub struct ApiCredentials {
    pub api_id: i32,
    pub api_hash: String,
}

pub fn api_credentials() -> anyhow::Result<ApiCredentials> {
    let var = |name: &str| {
        std::env::var(name).ok().filter(|v| !v.is_empty()).ok_or_else(|| anyhow::anyhow!("Missing env var: {name}"))
    };
    let raw_id = var("TELEGRAM_APP_ID")?;
    let api_id = raw_id.parse().map_err(|_| anyhow::anyhow!("TELEGRAM_APP_ID must be numeric, got {raw_id}"))?;
    Ok(ApiCredentials { api_id, api_hash: var("TELEGRAM_APP_API_HASH")? })
}

// ── 2. client ────────────────────────────────────────────────────────────────

// Connected on first use, so the server boots before the userbot is ever
// authorised (auth is a one-time interactive step).
#[derive(Clone)]
pub struct Userbot {
    db: Db,
    client: Arc<Mutex<Option<Client>>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Dialog {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    pub unread_count: i32,
}

// What a dialog is. Telegram (and gramjs) report a supergroup as a channel
// too; it is a group conversation, so it is typed and harvested as one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DialogKind {
    User,
    BasicGroup,
    Supergroup,
    Channel,
}

impl DialogKind {
    pub fn type_label(self) -> &'static str {
        match self {
            DialogKind::User => "user",
            DialogKind::BasicGroup | DialogKind::Supergroup => "group",
            DialogKind::Channel => "channel",
        }
    }
}

// One dialog as the account lists it.
#[derive(Debug, Clone)]
pub struct DialogInfo {
    // The id list_userbot_dialogs shows (Bot API form for chats).
    pub id: String,
    // Bare chat id, as gramjs's `entity.id` — the key every stored channel
    // post's metadata.chat_id and watermark uses.
    pub bare_id: Option<String>,
    pub kind: DialogKind,
    pub title: Option<String>,
    pub username: Option<String>,
    pub unread_count: i32,
    // How the real account addresses it; None for a stand-in.
    pub peer: Option<PeerRef>,
}

#[derive(Debug, Clone)]
pub struct ChannelHandle {
    pub chat_id: String,
    pub title: Option<String>,
    pub username: Option<String>,
    pub peer: Option<PeerRef>,
}

#[derive(Debug, Clone)]
pub struct ChannelMessage {
    pub id: i64,
    pub date: DateTime<Utc>,
    pub text: String,
    pub views: Option<i64>,
    pub forwards: Option<i64>,
}

// What the harvester and the dialog tool need from the account — two calls,
// the way the TS server asks gramjs for them, so a test can play Telegram.
#[async_trait::async_trait]
pub trait TelegramAccount: Send + Sync {
    async fn has_session(&self) -> bool;
    // Most recent first; every dialog when `limit` is None.
    async fn dialogs(&self, limit: Option<usize>) -> anyhow::Result<Vec<DialogInfo>>;
    // A channel's history as Telegram serves it: newest first; with
    // `reverse`, oldest first from just after `min_id`; never more than
    // `limit`. Media-only posts come back with empty text.
    async fn messages(
        &self,
        channel: &ChannelHandle,
        limit: usize,
        min_id: Option<i64>,
        reverse: bool,
    ) -> anyhow::Result<Vec<ChannelMessage>>;
}

pub type SharedAccount = Arc<dyn TelegramAccount>;

fn dialog_kind(peer: &Peer) -> DialogKind {
    match peer {
        Peer::Channel(_) => DialogKind::Channel,
        Peer::Group(g) if g.is_megagroup() => DialogKind::Supergroup,
        Peer::Group(_) => DialogKind::BasicGroup,
        Peer::User(_) => DialogKind::User,
    }
}

// Broadcast channels only: a supergroup's messages are a group
// conversation, not posts to put in the news store.
pub async fn list_channels(account: &dyn TelegramAccount) -> anyhow::Result<Vec<ChannelHandle>> {
    Ok(account
        .dialogs(Some(500))
        .await?
        .into_iter()
        .filter(|d| d.kind == DialogKind::Channel)
        .filter_map(|d| Some(ChannelHandle { chat_id: d.bare_id?, title: d.title, username: d.username, peer: d.peer }))
        .collect())
}

// With `since` (exclusive): the OLDEST `limit` posts after it, so a channel
// that posted more than a tick's worth is caught up over the next ticks.
// Telegram's default order is newest first — that stored the newest `limit`,
// moved the watermark past them, and the older ones in between were never
// fetched. Without: the newest `limit` (the bootstrap). Text posts only.
pub async fn fetch_messages(
    account: &dyn TelegramAccount,
    channel: &ChannelHandle,
    since: Option<i64>,
    limit: usize,
) -> anyhow::Result<Vec<ChannelMessage>> {
    let raw = match since {
        None => account.messages(channel, limit, None, false).await?,
        Some(since) => account.messages(channel, limit, Some(since), true).await?,
    };
    Ok(raw
        .into_iter()
        .filter_map(|m| {
            let text = m.text.trim().to_owned();
            (!text.is_empty()).then_some(ChannelMessage { text, ..m })
        })
        .collect())
}

// The account's dialogs in Telegram's order (most recent first). With a
// type, the first `limit` dialogs OF THAT TYPE — filtering after the limit
// would hand back fewer than exist.
pub async fn list_dialogs(
    account: &dyn TelegramAccount,
    kind: Option<&str>,
    limit: usize,
) -> anyhow::Result<Vec<Dialog>> {
    let dialogs = account.dialogs(if kind.is_some() { None } else { Some(limit) }).await?;
    Ok(dialogs
        .into_iter()
        .filter(|d| kind.is_none_or(|k| d.kind.type_label() == k))
        .take(limit)
        .map(|d| Dialog {
            id: d.id,
            kind: d.kind.type_label(),
            title: d.title.unwrap_or_else(|| "(untitled)".into()),
            username: d.username,
            unread_count: d.unread_count,
        })
        .collect())
}

impl Userbot {
    pub fn new(db: Db) -> Self {
        Self { db, client: Arc::default() }
    }

    pub async fn saved_session(&self) -> anyhow::Result<Option<(String, String)>> {
        let row = self
            .db
            .client()
            .await?
            .query_opt(
                "SELECT account_key, access_token FROM integration_account WHERE provider = $1 ORDER BY created_at DESC LIMIT 1",
                &[&PROVIDER],
            )
            .await?;
        Ok(row.map(|r| (r.get(0), r.get(1))))
    }

    pub async fn has_session(&self) -> bool {
        matches!(self.saved_session().await, Ok(Some(_)))
    }

    pub async fn save_session(
        &self,
        account_key: &str,
        session: &str,
        metadata: &serde_json::Value,
    ) -> anyhow::Result<()> {
        self.db
            .client()
            .await?
            .execute(
                "INSERT INTO integration_account (provider, account_key, access_token, metadata) VALUES ($1, $2, $3, $4)
                 ON CONFLICT (provider, account_key) DO UPDATE SET
                   access_token = EXCLUDED.access_token,
                   metadata = EXCLUDED.metadata,
                   updated_at = now()",
                &[&PROVIDER, &account_key, &session, &metadata.to_string()],
            )
            .await?;
        Ok(())
    }

    async fn client(&self) -> anyhow::Result<Client> {
        let mut slot = self.client.lock().await;
        if let Some(client) = slot.as_ref() {
            return Ok(client.clone());
        }
        let (_, raw) = self
            .saved_session()
            .await?
            .ok_or_else(|| anyhow::anyhow!("Telegram userbot is not authorized. Run `userbot-auth` once to log in."))?;
        let creds = api_credentials()?;
        let session = Arc::new(StringSession::decode(&raw)?.into_session()?);
        let client = connect(session, creds.api_id);
        *slot = Some(client.clone());
        Ok(client)
    }
}

#[async_trait::async_trait]
impl TelegramAccount for Userbot {
    async fn has_session(&self) -> bool {
        Userbot::has_session(self).await
    }

    async fn dialogs(&self, limit: Option<usize>) -> anyhow::Result<Vec<DialogInfo>> {
        let client = self.client().await?;
        let mut iter = client.iter_dialogs();
        let mut out = Vec::new();
        while limit.is_none_or(|l| out.len() < l)
            && let Some(dialog) = iter.next().await?
        {
            let peer = dialog.peer();
            let unread_count = match &dialog.raw {
                tl::enums::Dialog::Dialog(d) => d.unread_count,
                tl::enums::Dialog::Folder(_) => 0,
            };
            out.push(DialogInfo {
                id: peer.id().bot_api_dialog_id().map_or_else(|| peer.id().to_string(), |id| id.to_string()),
                bare_id: peer.id().bare_id().map(|id| id.to_string()),
                kind: dialog_kind(peer),
                title: peer.name().map(str::to_owned),
                username: peer.username().map(str::to_owned),
                unread_count,
                peer: Some(dialog.peer_ref()),
            });
        }
        Ok(out)
    }

    async fn messages(
        &self,
        channel: &ChannelHandle,
        limit: usize,
        min_id: Option<i64>,
        reverse: bool,
    ) -> anyhow::Result<Vec<ChannelMessage>> {
        let peer = channel.peer.ok_or_else(|| anyhow::anyhow!("channel {} has no peer", channel.chat_id))?;
        let client = self.client().await?;
        let mut iter = client.iter_messages(peer).limit(limit);
        if reverse {
            iter = iter.offset_id(i32::try_from(min_id.unwrap_or(0))?).reverse(true);
        }
        let mut out = Vec::new();
        while let Some(message) = iter.next().await? {
            let id = i64::from(message.id());
            if !reverse && min_id.is_some_and(|min| id <= min) {
                break;
            }
            out.push(ChannelMessage {
                id,
                date: message.date(),
                text: message.text().to_owned(),
                views: message.view_count().map(i64::from),
                forwards: message.forward_count().map(i64::from),
            });
        }
        Ok(out)
    }
}

fn connect(session: Arc<MemorySession>, api_id: i32) -> Client {
    let SenderPool { runner, handle, .. } = SenderPool::new(session, api_id);
    tokio::spawn(runner.run());
    Client::new(handle)
}

// ── 3. login ─────────────────────────────────────────────────────────────────

pub struct Prompts<'a> {
    pub phone: &'a dyn Fn() -> anyhow::Result<String>,
    pub code: &'a dyn Fn() -> anyhow::Result<String>,
    pub password: &'a dyn Fn(Option<&str>) -> anyhow::Result<String>,
}

// Phone → code → (2FA password) → the session saved under the account id.
pub async fn login(userbot: &Userbot, prompts: Prompts<'_>) -> anyhow::Result<(String, Option<String>)> {
    let creds = api_credentials()?;
    let session = Arc::new(MemorySession::default());
    let client = connect(session.clone(), creds.api_id);

    let phone = (prompts.phone)()?;
    let token = client.request_login_code(phone.trim(), &creds.api_hash).await?;
    let code = (prompts.code)()?;
    let user = match client.sign_in(&token, code.trim()).await {
        Ok(user) => user,
        Err(SignInError::PasswordRequired(password_token)) => {
            let password = (prompts.password)(password_token.hint())?;
            client.check_password(password_token, password.trim()).await?
        }
        Err(err) => return Err(err.into()),
    };

    let account_key = user.id().bare_id().map_or_else(|| user.id().to_string(), |id| id.to_string());
    let encoded = StringSession::from_session(&session)?.encode();
    let metadata = json!({ "username": user.username(), "firstName": user.first_name(), "phone": user.phone() });
    userbot.save_session(&account_key, &encoded, &metadata).await?;
    client.disconnect();
    Ok((account_key, user.username().map(str::to_owned)))
}

// ── 4. tools ─────────────────────────────────────────────────────────────────

#[derive(Deserialize, schemars::JsonSchema, PartialEq)]
#[serde(rename_all = "lowercase")]
enum DialogFilter {
    Channel,
    Group,
    User,
    All,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct ListDialogsParams {
    /// Filter dialogs by type. 'channel' is the right choice for the news digest (broadcast channels); a supergroup is a 'group'. Default 'all'.
    #[serde(rename = "type")]
    kind: Option<DialogFilter>,
    /// Max dialogs to return. Default 100.
    #[schemars(range(min = 1, max = 500))]
    limit: Option<usize>,
}

#[tool_router(router = userbot_tools, vis = "pub(crate)")]
impl McpTools {
    #[tool(
        name = "list_userbot_dialogs",
        annotations(read_only_hint = true, open_world_hint = false),
        title = "List userbot dialogs (chats / channels)",
        description = "List the personal Telegram account's dialogs (channels, groups, \
            private chats) the userbot is subscribed to. Mostly useful for \
            discovery / debugging — to read channel posts use `list_news` \
            with source='channel'; the news poller harvests every subscribed \
            channel in the background. Requires `userbot-auth` to have \
            been run once."
    )]
    async fn list_userbot_dialogs(&self, Parameters(p): Parameters<ListDialogsParams>) -> ToolResult {
        if p.limit.is_some_and(|l| !(1..=500).contains(&l)) {
            return Err(invalid_params("limit must be between 1 and 500"));
        }
        respond(
            async {
                let kind = match p.kind {
                    None | Some(DialogFilter::All) => None,
                    Some(DialogFilter::Channel) => Some("channel"),
                    Some(DialogFilter::Group) => Some("group"),
                    Some(DialogFilter::User) => Some("user"),
                };
                let dialogs = list_dialogs(&*self.deps.userbot, kind, p.limit.unwrap_or(100)).await?;
                Ok(json!({ "count": dialogs.len(), "dialogs": dialogs }))
            }
            .await,
        )
    }
}

// Accepts "tginsider", "@tginsider", "https://t.me/tginsider/".
pub fn normalize_handle(handle: &str) -> String {
    let h = handle.trim();
    let h = h.strip_prefix("https://t.me/").or_else(|| h.strip_prefix("http://t.me/")).unwrap_or(h);
    h.trim_start_matches('@').trim_end_matches('/').to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_session_round_trips_the_gramjs_layout() {
        let mut key = [0u8; 256];
        key.iter_mut().enumerate().for_each(|(i, b)| *b = i as u8);
        let session = StringSession { dc_id: 2, address: "149.154.167.41".into(), port: 443, auth_key: key };
        let encoded = session.encode();
        assert!(encoded.starts_with('1'));
        // 1 + 2 + len("149.154.167.41") + 2 + 256 bytes, as gramjs wrote it.
        assert_eq!(STANDARD.decode(&encoded[1..]).unwrap().len(), 275);
        assert_eq!(StringSession::decode(&encoded).unwrap(), session);
    }

    #[test]
    fn imports_into_a_grammers_session_on_the_home_dc() {
        let session = StringSession { dc_id: 2, address: "149.154.167.41".into(), port: 443, auth_key: [7; 256] };
        let grammers = session.clone().into_session().unwrap();
        assert_eq!(grammers.home_dc_id().unwrap(), 2);
        assert_eq!(StringSession::from_session(&grammers).unwrap(), session);
    }

    #[test]
    fn rejects_foreign_session_strings() {
        assert!(StringSession::decode("2abc").is_err());
        assert!(StringSession::decode("1AAAA").is_err());
    }

    #[test]
    fn normalizes_channel_handles() {
        for raw in ["tginsider", "@tginsider", "https://t.me/tginsider/", " @@tginsider "] {
            assert_eq!(normalize_handle(raw), "tginsider");
        }
    }
}
