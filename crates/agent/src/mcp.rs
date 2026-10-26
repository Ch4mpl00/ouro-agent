// The connection to the MCP server — the agent's only way to the outside
// world. Two transports: Streamable HTTP (MCP_TRANSPORT=http, MCP_URL; the
// containers) and stdio (a child process; local dev).
//
// Connection lifecycle (.claude/tasks/done/mcp-connection-lifecycle.md):
//   - startup retries inside the process, with capped backoff, instead of
//     exiting and letting Docker restart it (the crash-loops of 2026-06-15
//     and 2026-08-23);
//   - a 4xx other than 408/429 is a misconfiguration and stays fatal;
//   - a session the server lost (it restarted, or another client took the
//     single session) is renewed: the client re-initializes on a 404 and replays
//     the request that met it — it never ran — and when that recovery itself
//     fails, the next call reconnects; a call that may have reached a tool is
//     never replayed (a Telegram message must not go out twice).

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::future::BoxFuture;
use rmcp::model::{
    CallToolRequest, CallToolRequestParams, CancelledNotification, CancelledNotificationParam, ClientConfig,
    ClientRequest, Implementation, ServerResult,
};
use rmcp::service::{PeerRequestOptions, RunningService};
use rmcp::{RoleClient, ServiceExt};
use serde_json::{Map, Value};
use tokio_util::sync::CancellationToken;

use crate::errors::{aborted, check, or_cancel};
use crate::providers::ToolDef;
use std::sync::atomic::{AtomicBool, Ordering};

// What the agent needs from MCP: the tool list (fixed at boot — mcp and the
// agent deploy together) and calls. A tool's failure is a "[tool error] …"
// reply, not an Err; Err is a transport or protocol failure.
#[async_trait]
pub trait Mcp: Send + Sync {
    fn tools(&self) -> Vec<ToolDef>;
    // Whether the server marks `name` read-only (`readOnlyHint`). Anything
    // not marked may change the world.
    fn read_only(&self, _name: &str) -> bool {
        false
    }
    // MCP defaults openWorldHint to true; only an explicit false is closed.
    fn open_world(&self, _name: &str) -> bool {
        true
    }
    async fn call_tool(
        &self,
        name: &str,
        args: Map<String, Value>,
        cancel: &CancellationToken,
    ) -> anyhow::Result<String>;
    /// Durable calls must not hide ambiguous request retries. Adapters that
    /// cannot prove non-dispatch conservatively return Unknown.
    async fn call_tool_once(
        &self,
        name: &str,
        args: Map<String, Value>,
        cancel: &CancellationToken,
    ) -> Result<String, ToolCallFailure> {
        self.call_tool(name, args, cancel).await.map_err(ToolCallFailure::Unknown)
    }
    async fn close(&self) {}
}

#[derive(Debug)]
pub enum ToolCallFailure {
    NotExecuted(anyhow::Error),
    Unknown(anyhow::Error),
}

pub type SharedMcp = Arc<dyn Mcp>;

// ── connect retry ───────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConnectRetryPolicy {
    // Retries after the first attempt; None = never give up.
    pub max_retries: Option<u32>,
    pub base_delay_ms: u64,
    // Ceiling, so an hours-long outage settles into a steady poll.
    pub max_delay_ms: u64,
}

// Supervisor startup: never give up — giving up only moves the retry back
// into Docker, with no backoff and no log line explaining the wait.
pub const RETRY_UNTIL_UP: ConnectRetryPolicy =
    ConnectRetryPolicy { max_retries: None, base_delay_ms: 1_000, max_delay_ms: 30_000 };

// Scripts and the mid-flight reconnect: bounded, because a tool call that
// hangs forever is worse than one that fails.
pub const CONNECT_RETRY: ConnectRetryPolicy =
    ConnectRetryPolicy { max_retries: Some(4), base_delay_ms: 500, max_delay_ms: 8_000 };

#[derive(Clone, Debug)]
pub struct ConnectRetryInfo {
    pub attempt: u32,
    pub delay_ms: u64,
    pub error: String,
}

// The HTTP status an MCP failure carries, when it carries one.
pub fn http_status(err: &anyhow::Error) -> Option<u16> {
    let text = format!("{err:#}");
    let at = text.find("HTTP ")?;
    text[at + 5..].get(..3)?.parse().ok()
}

// A 4xx means the server is up and rejecting us on the merits — wrong path,
// wrong auth, protocol mismatch — so it stays fatal; 408 and 429 mean "come
// back later". No status (connection refused, DNS, reset) and 5xx (the
// "Already connected to a transport" of the incident) are transient.
pub fn is_fatal_connect_error(err: &anyhow::Error) -> bool {
    let text = format!("{err:#}");
    if text.contains("Auth required") || text.contains("Insufficient scope") {
        return true;
    }
    match http_status(err) {
        Some(408 | 429) | None => false,
        Some(status) => (400..500).contains(&status),
    }
}

pub type Sleep = Arc<dyn Fn(u64) -> BoxFuture<'static, ()> + Send + Sync>;

pub fn real_sleep() -> Sleep {
    Arc::new(|ms| Box::pin(tokio::time::sleep(Duration::from_millis(ms))))
}

pub async fn connect_with_retry<T, F, Fut>(
    mut open: F,
    policy: ConnectRetryPolicy,
    sleep: &Sleep,
    mut on_retry: impl FnMut(&ConnectRetryInfo),
) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    let mut attempt: u32 = 1;
    loop {
        match open().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                let exhausted = policy.max_retries.is_some_and(|max| attempt > max);
                if exhausted || is_fatal_connect_error(&err) {
                    return Err(err);
                }
                let delay_ms =
                    policy.base_delay_ms.saturating_mul(1u64 << (attempt - 1).min(40)).min(policy.max_delay_ms);
                on_retry(&ConnectRetryInfo { attempt, delay_ms, error: format!("{err:#}") });
                sleep(delay_ms).await;
                attempt += 1;
            }
        }
    }
}

// ── the client ──────────────────────────────────────────────────────────────

pub type Client = RunningService<RoleClient, ClientConfig>;

// Opens one session: a fresh transport per attempt.
pub type Open = Arc<dyn Fn() -> BoxFuture<'static, anyhow::Result<Client>> + Send + Sync>;

// MCP tools only the supervisor calls, never offered to a model: an agent
// loop or a workflow plan blocking on the signal queue would stall its run.
// `list_scheduled_tasks` is MCP's old schedule, no longer imported (design
// 06); kept from every model, whose `list_scheduled_tasks` is the agent's own
// (schedule.rs).
pub const SUPERVISOR_ONLY_TOOLS: &[&str] = &["wait_for_signal", "list_scheduled_tasks"];

// The tools a model may see and call.
pub fn model_tools(mcp: &dyn Mcp) -> Vec<ToolDef> {
    mcp.tools().into_iter().filter(|t| !SUPERVISOR_ONLY_TOOLS.contains(&t.name.as_str())).collect()
}

// The SDK's default per-request deadline the TS agent ran with.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

fn client_info() -> ClientConfig {
    let mut info = ClientConfig::default();
    info.client_info = Implementation::new("agent-loop", "0.1.0");
    info
}

// From MCP_TRANSPORT / MCP_URL (http) or MCP_STDIO_COMMAND (stdio, default
// `mcp-tools`). A malformed MCP_URL fails here, outside any retry.
pub fn open_from_env() -> anyhow::Result<Open> {
    let mode = std::env::var("MCP_TRANSPORT").unwrap_or_else(|_| "stdio".into()).to_lowercase();
    if mode == "http" {
        let url = std::env::var("MCP_URL").unwrap_or_else(|_| "http://localhost:3000/mcp".into());
        url::Url::parse(&url).map_err(|e| anyhow::anyhow!("MCP_URL {url:?}: {e}"))?;
        return Ok(open_http(url));
    }
    let command = std::env::var("MCP_STDIO_COMMAND").unwrap_or_else(|_| "mcp-tools".into());
    Ok(Arc::new(move || {
        let command = command.clone();
        Box::pin(async move {
            let mut parts = command.split_whitespace();
            let program = parts.next().unwrap_or("mcp-tools");
            let mut cmd = tokio::process::Command::new(program);
            cmd.args(parts);
            let transport = rmcp::transport::TokioChildProcess::new(cmd)?;
            Ok(client_info().serve(transport).await?)
        })
    }))
}

pub fn open_http(url: String) -> Open {
    Arc::new(move || {
        let url = url.clone();
        Box::pin(async move {
            // The supervisor owns recovery so concurrent callers share one
            // replacement and close it deterministically at shutdown.
            let config = rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig::with_uri(url)
                .reinit_on_expired_session(false);
            let transport = rmcp::transport::StreamableHttpClientTransport::with_client(reqwest::Client::new(), config);
            Ok(client_info().serve(transport).await?)
        })
    })
}

fn tool_defs(tools: Vec<rmcp::model::Tool>) -> Vec<ToolDef> {
    tools
        .into_iter()
        .map(|t| ToolDef {
            name: t.name.to_string(),
            description: Some(t.description.map(|d| d.to_string()).unwrap_or_default()),
            parameters: Value::Object((*t.input_schema).clone()),
        })
        .collect()
}

fn log_retry(what: &str) -> impl FnMut(&ConnectRetryInfo) + '_ {
    move |info| {
        eprintln!(
            "[mcp-client] {what} attempt {} failed ({}), retrying in {}ms",
            info.attempt, info.error, info.delay_ms
        )
    }
}

// A session definitively rejected by the HTTP transport.
fn is_session_lost(err: &anyhow::Error) -> bool {
    // Only a protocol-level 404 with a session attached proves the request
    // was rejected before tool execution. Text in a 5xx or JSON-RPC failure
    // may have come from a tool that already performed a side effect.
    let Some(rmcp::ServiceError::TransportSend(error)) = err.downcast_ref::<rmcp::ServiceError>() else { return false };
    matches!(
        error.error.downcast_ref::<rmcp::transport::streamable_http_client::StreamableHttpError<reqwest::Error>>(),
        Some(rmcp::transport::streamable_http_client::StreamableHttpError::SessionExpired)
    )
}

struct Connection {
    peer: rmcp::Peer<RoleClient>,
    service: tokio::sync::Mutex<Option<Client>>,
}

impl Connection {
    fn new(client: Client) -> Arc<Self> {
        Arc::new(Self { peer: client.peer().clone(), service: tokio::sync::Mutex::new(Some(client)) })
    }
    async fn close(&self) {
        if let Some(client) = self.service.lock().await.take() {
            let _ = client.cancel().await;
        }
    }
}

pub struct McpClient {
    tools: Vec<ToolDef>,
    read_only: HashSet<String>,
    closed_world: HashSet<String>,
    open: Open,
    sleep: Sleep,
    // The current session. A replaced one is dropped when the last call
    // holding it finishes — closing it under them would fail them with
    // "connection closed", which tells them nothing about whether the tool
    // ran.
    client: tokio::sync::RwLock<Arc<Connection>>,
    closed: AtomicBool,
    // One reconnect at a time, shared by concurrent callers.
    reconnecting: tokio::sync::Mutex<()>,
}

pub struct ConnectOptions {
    pub startup_retry: ConnectRetryPolicy,
    pub sleep: Sleep,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        ConnectOptions { startup_retry: CONNECT_RETRY, sleep: real_sleep() }
    }
}

impl McpClient {
    // Connects and lists the tools, retrying both as one unit: a session that
    // can't list tools is not a usable session.
    pub async fn connect(open: Open, options: ConnectOptions) -> anyhow::Result<McpClient> {
        let (client, tools) = connect_with_retry(
            || {
                let open = open.clone();
                async move {
                    let client = open().await?;
                    match client.list_all_tools().await {
                        Ok(tools) => Ok((client, tools)),
                        Err(err) => {
                            let _ = client.cancel().await;
                            Err(err.into())
                        }
                    }
                }
            },
            options.startup_retry,
            &options.sleep,
            log_retry("connect"),
        )
        .await?;
        let read_only = tools
            .iter()
            .filter(|t| t.annotations.as_ref().and_then(|a| a.read_only_hint) == Some(true))
            .map(|t| t.name.to_string())
            .collect();
        let closed_world = tools
            .iter()
            .filter(|t| t.annotations.as_ref().and_then(|a| a.open_world_hint) == Some(false))
            .map(|t| t.name.to_string())
            .collect();
        Ok(McpClient {
            read_only,
            closed_world,
            tools: tool_defs(tools),
            open,
            sleep: options.sleep,
            client: tokio::sync::RwLock::new(Connection::new(client)),
            closed: AtomicBool::new(false),
            reconnecting: tokio::sync::Mutex::new(()),
        })
    }

    async fn current(&self) -> Arc<Connection> {
        self.client.read().await.clone()
    }

    // Replaces `failed` with a fresh session, unless someone already did.
    async fn reconnect(&self, failed: &Arc<Connection>) -> anyhow::Result<Arc<Connection>> {
        let _one = self.reconnecting.lock().await;
        if self.closed.load(Ordering::SeqCst) {
            return Err(aborted());
        }
        let current = self.current().await;
        if !Arc::ptr_eq(&current, failed) {
            return Ok(current);
        }
        let fresh = Connection::new(
            connect_with_retry(|| (self.open)(), CONNECT_RETRY, &self.sleep, log_retry("reconnect")).await?,
        );
        *self.client.write().await = fresh.clone();
        Ok(fresh)
    }

    async fn call_on(
        client: &Arc<Connection>,
        name: &str,
        args: &Map<String, Value>,
        cancel: &CancellationToken,
    ) -> anyhow::Result<rmcp::model::CallToolResult> {
        let request = ClientRequest::CallToolRequest(CallToolRequest::new(
            CallToolRequestParams::new(name.to_owned()).with_arguments(args.clone()),
        ));
        let handle =
            client.peer.send_cancellable_request(request, PeerRequestOptions::with_timeout(REQUEST_TIMEOUT)).await?;
        let (id, peer) = (handle.id.clone(), handle.peer.clone());
        let response = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                // Tell the server, so the tool stops too.
                let notification = CancelledNotification::new(CancelledNotificationParam::new(Some(id), None));
                let _ = peer.send_notification(notification.into()).await;
                return Err(aborted());
            }
            response = handle.await_response() => response?,
        };
        match response {
            ServerResult::CallToolResult(result) => Ok(result),
            other => anyhow::bail!("unexpected reply to tools/call: {other:?}"),
        }
    }
}

// Text parts joined; an error result becomes "[tool error] …".
pub fn result_text(result: &rmcp::model::CallToolResult) -> String {
    let text: Vec<&str> = result.content.iter().filter_map(|c| c.as_text().map(|t| t.text.as_str())).collect();
    let text = text.join("\n");
    if result.is_error == Some(true) {
        return format!("[tool error] {}", if text.is_empty() { "(no error text)" } else { &text });
    }
    if text.is_empty() { "(empty)".into() } else { text }
}

#[async_trait]
impl Mcp for McpClient {
    async fn call_tool_once(
        &self,
        name: &str,
        args: Map<String, Value>,
        cancel: &CancellationToken,
    ) -> Result<String, ToolCallFailure> {
        check(cancel).map_err(ToolCallFailure::NotExecuted)?;
        if self.closed.load(Ordering::SeqCst) {
            return Err(ToolCallFailure::NotExecuted(aborted()));
        }
        let mut client = self.current().await;
        if client.peer.is_transport_closed() {
            client = or_cancel(cancel, self.reconnect(&client)).await.map_err(ToolCallFailure::NotExecuted)?;
        }
        match Self::call_on(&client, name, &args, cancel).await {
            Ok(result) => Ok(result_text(&result)),
            Err(error) if is_session_lost(&error) => {
                // The session rejected the request before dispatch. Refresh it
                // for the next attempt, but never reissue tools/call here.
                let _ = or_cancel(cancel, self.reconnect(&client)).await;
                Err(ToolCallFailure::NotExecuted(error))
            }
            Err(error) => Err(ToolCallFailure::Unknown(error)),
        }
    }
    fn tools(&self) -> Vec<ToolDef> {
        self.tools.clone()
    }

    fn read_only(&self, name: &str) -> bool {
        self.read_only.contains(name)
    }

    fn open_world(&self, name: &str) -> bool {
        !self.closed_world.contains(name)
    }

    async fn call_tool(
        &self,
        name: &str,
        args: Map<String, Value>,
        cancel: &CancellationToken,
    ) -> anyhow::Result<String> {
        check(cancel)?;
        if self.closed.load(Ordering::SeqCst) {
            return Err(aborted());
        }
        let mut client = self.current().await;
        // A session whose transport is gone can't have taken this call.
        if client.peer.is_transport_closed() {
            client = or_cancel(cancel, self.reconnect(&client)).await?;
        }
        let result = match Self::call_on(&client, name, &args, cancel).await {
            Ok(result) => result,
            Err(err) => {
                check(cancel)?;
                if !is_session_lost(&err) {
                    return Err(err);
                }
                eprintln!("[mcp-client] session lost, reconnecting…");
                let fresh = or_cancel(cancel, self.reconnect(&client)).await?;
                check(cancel)?;
                Self::call_on(&fresh, name, &args, cancel).await?
            }
        };
        Ok(result_text(&result))
    }

    async fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        let _one = self.reconnecting.lock().await;
        self.current().await.close().await;
    }
}
