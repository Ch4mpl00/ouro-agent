// The MCP endpoint: the tool-serving handler every domain file adds its
// `#[tool_router]` block to, and the two transports it is served over.
//
// Sections:
//   1. deps      — everything a tool handler may reach, built once in main
//   2. handler   — `McpTools`: own tools + gateway upstreams behind one list
//   3. results   — the JSON-text result shape and how failures surface
//   4. sessions  — "newest wins" policy for the single-session instance
//   5. transport — stdio and Streamable HTTP

use std::path::PathBuf;
use std::sync::Arc;

use futures::Stream;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::tool::ToolCallContext;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ClientJsonRpcMessage, ContentBlock, ListToolsResult,
    PaginatedRequestParams, ServerJsonRpcMessage, Tool,
};
use rmcp::service::RequestContext;
use rmcp::transport::streamable_http_server::session::local::{LocalSessionManager, LocalSessionManagerError};
use rmcp::transport::streamable_http_server::session::{EventStore, ServerSseMessage};
use rmcp::transport::streamable_http_server::{RestoreOutcome, SessionId, SessionManager};
use rmcp::transport::{StreamableHttpServerConfig, StreamableHttpService};
use rmcp::{ErrorData, RoleServer, ServerHandler, ServiceExt, tool_handler};
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use crate::gateway::Gateway;
use crate::gmail::GmailModule;
use crate::knowledge::KnowledgeRepository;
use crate::memory::MemoryService;
use crate::monobank::Monobank;
use crate::news::NewsRepository;
use crate::peer::Peer;
use crate::scheduler::Scheduler;
use crate::settings::Settings;
use crate::signals::Signals;
use crate::skills::SkillCatalog;
use crate::telegram::TelegramModule;
use crate::userbot::SharedAccount;

// ── 1. deps ──────────────────────────────────────────────────────────────────

// Built once in the composition root and shared by every session. A
// handler's reach is exactly this struct — no globals, no service locators.
pub struct Deps {
    pub settings: Settings,
    pub signals: Signals,
    pub peer: Peer,
    pub scheduler: Scheduler,
    pub telegram: TelegramModule,
    pub gmail: GmailModule,
    pub monobank: Monobank,
    pub userbot: SharedAccount,
    pub skills: SkillCatalog,
    pub skill_scorer: Option<Arc<dyn crate::skills::SkillScorer>>,
    // The SSRF-guarded client behind fetch_url (fetch.rs).
    pub fetcher: crate::fetch::SharedFetch,
    // Which local files read_file / read_pdf may open.
    pub files: crate::fs::FileAccess,
    // Where downloaded attachments land (STORAGE_DIR, default ./storage).
    pub storage_dir: PathBuf,
    pub news: NewsRepository,
    pub knowledge: KnowledgeRepository,
    pub memory: MemoryService,
    // Stamped onto every memory write: who the instance writes as. Audit
    // metadata, never access control (one shared space).
    pub memory_actor: String,
    // Third-party MCP upstreams, re-exposed namespaced. Only on an
    // unrestricted instance with upstreams configured.
    pub gateway: Option<Arc<Gateway>>,
}

// ── 2. handler ───────────────────────────────────────────────────────────────

// One per MCP session. Cheap: the deps are shared and the router is a clone
// of the one composed from the selected toolsets.
#[derive(Clone)]
pub struct McpTools {
    pub(crate) deps: Arc<Deps>,
    tool_router: ToolRouter<Self>,
}

impl McpTools {
    pub fn new(deps: Arc<Deps>, tool_router: ToolRouter<Self>) -> Self {
        Self { deps, tool_router }
    }
}

// Own tools come first and win any name collision; gateway tools follow,
// already namespaced (`tavily__tavily_search`). The agent sees one list.
#[tool_handler(router = self.tool_router.clone(), name = "mcp-tools", version = "0.1.0")]
impl ServerHandler for McpTools {
    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let mut tools = self.tool_router.list_all();
        if let Some(gateway) = &self.deps.gateway {
            tools.extend(gateway.tools().iter().cloned());
        }
        Ok(ListToolsResult::with_all_items(tools))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        if !self.tool_router.has_route(&request.name)
            && let Some(gateway) = &self.deps.gateway
            && gateway.has_tool(&request.name)
        {
            return Ok(gateway.call(request).await.into());
        }
        let tcc = ToolCallContext::new(self, request, context);
        self.tool_router.call(tcc).await
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.tool_router.get(name).cloned().or_else(|| self.deps.gateway.as_ref()?.tool(name))
    }
}

// ── 3. results ───────────────────────────────────────────────────────────────

pub type ToolResult = Result<CallToolResult, ErrorData>;

// A single text block of pretty JSON — what every tool has always returned,
// and what clients without structured-content support can still read.
pub fn json_result(value: &impl Serialize) -> ToolResult {
    let text = serde_json::to_string_pretty(value).map_err(|err| ErrorData::internal_error(err.to_string(), None))?;
    Ok(CallToolResult::success(vec![ContentBlock::text(text)]))
}

// A failure inside a handler — an API error, a missing file — comes back as
// an `isError` result carrying the message, which is what the TS SDK did
// with a thrown error. The model reads it and adapts; a JSON-RPC error would
// be invisible to it. Protocol errors (bad params) stay `ErrorData`.
pub fn tool_failed(err: impl std::fmt::Display) -> ToolResult {
    let text = err.to_string();
    tracing::warn!(error = %text, "tool failed");
    Ok(CallToolResult::error(vec![ContentBlock::text(text)]))
}

pub fn respond<T: Serialize>(result: anyhow::Result<T>) -> ToolResult {
    match result {
        Ok(value) => json_result(&value),
        Err(err) => tool_failed(format!("{err:#}")),
    }
}

// `?` for handlers: an Err becomes an isError result via `tool_failed`.
#[macro_export]
macro_rules! try_tool {
    ($e:expr) => {
        match $e {
            Ok(value) => value,
            Err(err) => return $crate::server::tool_failed(err),
        }
    };
}

pub fn invalid_params(message: &'static str) -> ErrorData {
    ErrorData::invalid_params(message, None)
}

// ── 4. sessions ──────────────────────────────────────────────────────────────

// The full instance serves exactly one client — the supervisor — and a fresh
// `initialize` means the previous one is gone (it restarted after an unclean
// death and never closed its session). So the newest session evicts the rest
// instead of being refused: refusing is what crash-looped the TS server in
// production (2026-06-15, 2026-08-23 — 78 restarts).
//
// A restricted instance (MCP_TOOLSETS set) has no signal delivery to race on
// and must hold several sessions at once — tunnel-client keeps a probe session
// of its own, so a ChatGPT client is always the second connection.
pub struct SessionPolicy {
    inner: LocalSessionManager,
    newest_wins: bool,
}

impl SessionPolicy {
    pub fn new(newest_wins: bool) -> Self {
        Self { inner: LocalSessionManager::default(), newest_wins }
    }
}

impl SessionManager for SessionPolicy {
    type Error = LocalSessionManagerError;
    type Transport = <LocalSessionManager as SessionManager>::Transport;

    async fn create_session(&self) -> Result<(SessionId, Self::Transport), Self::Error> {
        if self.newest_wins {
            let stale: Vec<SessionId> = self.inner.sessions.read().await.keys().cloned().collect();
            for id in stale {
                tracing::info!(session = %id, "evicting previous session: newest wins");
                if let Err(err) = self.inner.close_session(&id).await {
                    tracing::warn!(session = %id, %err, "evicting stale session failed");
                }
            }
        }
        self.inner.create_session().await
    }

    async fn initialize_session(
        &self,
        id: &SessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<ServerJsonRpcMessage, Self::Error> {
        self.inner.initialize_session(id, message).await
    }

    async fn has_session(&self, id: &SessionId) -> Result<bool, Self::Error> {
        self.inner.has_session(id).await
    }

    async fn close_session(&self, id: &SessionId) -> Result<(), Self::Error> {
        self.inner.close_session(id).await
    }

    async fn create_stream(
        &self,
        id: &SessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + Sync + 'static, Self::Error> {
        self.inner.create_stream(id, message).await
    }

    async fn accept_message(&self, id: &SessionId, message: ClientJsonRpcMessage) -> Result<(), Self::Error> {
        self.inner.accept_message(id, message).await
    }

    async fn create_standalone_stream(
        &self,
        id: &SessionId,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + Sync + 'static, Self::Error> {
        self.inner.create_standalone_stream(id).await
    }

    async fn resume(
        &self,
        id: &SessionId,
        last_event_id: String,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + Sync + 'static, Self::Error> {
        self.inner.resume(id, last_event_id).await
    }

    async fn restore_session(&self, id: SessionId) -> Result<RestoreOutcome<Self::Transport>, Self::Error> {
        self.inner.restore_session(id).await
    }

    fn event_store(&self) -> Option<Arc<dyn EventStore>> {
        self.inner.event_store()
    }
}

// ── 5. transport ─────────────────────────────────────────────────────────────

pub async fn serve_stdio(tools: McpTools, cancel: CancellationToken) -> anyhow::Result<()> {
    let running = tools.serve_with_ct(rmcp::transport::stdio(), cancel).await?;
    running.waiting().await?;
    Ok(())
}

pub struct HttpOptions {
    pub bind_address: std::net::IpAddr,
    pub port: u16,
    // Hosts (`host` or `host:port`) accepted in the Host header. rmcp only
    // accepts loopback by default, which would reject the compose service
    // names (`mcp:3000`, `mcp-tunnel:3001`) the agent and tunnel dial.
    // Empty = no validation.
    pub allowed_hosts: Vec<String>,
    pub multi_session: bool,
}

// Generic over the handler so the transport's session rules can be tested on
// their own (tests/http_transport.rs); production serves McpTools.
pub async fn serve_http<S>(
    make_tools: impl Fn() -> S + Send + Sync + 'static,
    options: HttpOptions,
    cancel: CancellationToken,
) -> anyhow::Result<()>
where
    S: ServerHandler + Send + 'static,
{
    let mut config = if options.allowed_hosts.is_empty() {
        StreamableHttpServerConfig::default().disable_allowed_hosts()
    } else {
        StreamableHttpServerConfig::default().with_allowed_hosts(options.allowed_hosts)
    };
    config.cancellation_token = cancel.child_token();
    let service = StreamableHttpService::new(
        move || Ok(make_tools()),
        Arc::new(SessionPolicy::new(!options.multi_session)),
        config,
    );
    let app = axum::Router::new().nest_service("/mcp", service);
    let listener = tokio::net::TcpListener::bind((options.bind_address, options.port)).await?;
    tracing::info!(addr = %listener.local_addr()?, multi_session = options.multi_session, "mcp http listening");
    axum::serve(listener, app).with_graceful_shutdown(cancel.cancelled_owned()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn http_listener_uses_the_configured_address() {
        #[derive(Clone)]
        struct Empty;
        impl ServerHandler for Empty {}

        // IPv6 loopback distinguishes the requested address from the old
        // IPv4 wildcard binding; an ignored bind_address cannot pass.
        let port = std::net::TcpListener::bind("[::1]:0").unwrap().local_addr().unwrap().port();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(serve_http(
            || Empty,
            HttpOptions { bind_address: "::1".parse().unwrap(), port, allowed_hosts: Vec::new(), multi_session: true },
            cancel.clone(),
        ));
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let mut answered = false;
        for _ in 0..100 {
            if client.post(format!("http://[::1]:{port}/mcp")).send().await.is_ok() {
                answered = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        cancel.cancel();
        task.await.unwrap().unwrap();
        assert!(answered, "MCP must listen on the configured IPv6 loopback address");
    }

    #[tokio::test]
    async fn newest_session_evicts_the_previous_one() {
        let policy = SessionPolicy::new(true);
        let (first, _t1) = policy.create_session().await.unwrap();
        let (second, _t2) = policy.create_session().await.unwrap();
        assert!(!policy.has_session(&first).await.unwrap());
        assert!(policy.has_session(&second).await.unwrap());
    }

    #[tokio::test]
    async fn multi_session_keeps_every_session() {
        let policy = SessionPolicy::new(false);
        let (first, _t1) = policy.create_session().await.unwrap();
        let (second, _t2) = policy.create_session().await.unwrap();
        assert!(policy.has_session(&first).await.unwrap());
        assert!(policy.has_session(&second).await.unwrap());
    }

    #[test]
    fn handler_failures_become_is_error_results_not_protocol_errors() {
        let result = respond::<()>(Err(anyhow::anyhow!("Telegram sendMessage failed (400): chat not found"))).unwrap();
        assert_eq!(result.is_error, Some(true));
    }
}
