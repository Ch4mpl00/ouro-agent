// Port of src/mcp-client.test.ts, one test per case; `connectMcp` runs over
// real Streamable HTTP instead of the SDK's in-memory pair. The eviction,
// lost-reply and tool-error tests at the end are Rust-only.
// This crate never imports MCP server internals: the SDK fixture speaks only
// the public protocol, including cancellation and a lost response after a tool.
use agent::mcp::*;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, ListToolsResult, PaginatedRequestParams,
    ServerCapabilities, ServerConfig, Tool,
};
use rmcp::service::RequestContext;
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService,
    session::{SessionManager, local::LocalSessionManager},
};
use rmcp::{ErrorData, RoleServer, ServerHandler};
use serde_json::{Map, Value, json};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

fn recording_sleep() -> (Sleep, Arc<Mutex<Vec<u64>>>) {
    let delays = Arc::new(Mutex::new(Vec::new()));
    let log = delays.clone();
    (
        Arc::new(move |delay| {
            log.lock().unwrap().push(delay);
            Box::pin(async {})
        }),
        delays,
    )
}
// The 500 the reconnecting agent actually saw on the droplet: the MCP still
// held the dead session of the agent it just replaced.
fn already_connected() -> anyhow::Error {
    anyhow::anyhow!("HTTP 500: Error POSTing to endpoint: Already connected to a transport")
}

// An `open` that fails while `n` is below `succeed_at`, counting attempts.
async fn run_connect(
    policy: ConnectRetryPolicy,
    succeed_at: usize,
    error: fn() -> anyhow::Error,
) -> (anyhow::Result<&'static str>, usize, Vec<u64>, Vec<u32>) {
    let (sleep, slept) = recording_sleep();
    let mut attempts = 0;
    let mut seen = Vec::new();
    let result = connect_with_retry(
        || {
            attempts += 1;
            let n = attempts;
            async move { if n < succeed_at { Err(error()) } else { Ok("connected") } }
        },
        policy,
        &sleep,
        |r| seen.push(r.attempt),
    )
    .await;
    let slept = slept.lock().unwrap().clone();
    (result, attempts, slept, seen)
}

// ── isFatalConnectError ─────────────────────────────────────────────────────

#[test]
fn treats_connectivity_failures_and_5xx_as_retryable() {
    // No HTTP status at all: ECONNREFUSED / DNS / socket reset.
    assert!(!is_fatal_connect_error(&anyhow::anyhow!("fetch failed")));
    assert!(!is_fatal_connect_error(&already_connected()));
    assert!(!is_fatal_connect_error(&anyhow::anyhow!("HTTP 503: unavailable")));
    // The two 4xx that mean "come back later".
    assert!(!is_fatal_connect_error(&anyhow::anyhow!("HTTP 408: timeout")));
    assert!(!is_fatal_connect_error(&anyhow::anyhow!("HTTP 429: slow down")));
}

#[test]
fn treats_a_deterministic_rejection_as_fatal() {
    // Wrong MCP_URL path — the server is up and says so.
    assert!(is_fatal_connect_error(&anyhow::anyhow!("HTTP 404: not found")));
    assert!(is_fatal_connect_error(&anyhow::anyhow!("HTTP 403: forbidden")));
    assert!(is_fatal_connect_error(&anyhow::anyhow!("Auth required")));
}

// ── connectWithRetry ────────────────────────────────────────────────────────

#[tokio::test]
async fn retries_a_transient_failure_and_returns_the_eventual_success() {
    let (result, attempts, slept, _) = run_connect(CONNECT_RETRY, 3, already_connected).await;
    assert_eq!(result.unwrap(), "connected");
    assert_eq!(attempts, 3);
    assert_eq!(slept, [500, 1000]);
}

#[tokio::test]
async fn caps_the_backoff_instead_of_drifting_into_hour_long_sleeps() {
    let (result, _, slept, _) = run_connect(RETRY_UNTIL_UP, 12, || anyhow::anyhow!("fetch failed")).await;
    assert_eq!(result.unwrap(), "connected");
    // 1s doubling to the 30s ceiling, then flat.
    assert_eq!(slept, [1000, 2000, 4000, 8000, 16000, 30000, 30000, 30000, 30000, 30000, 30000]);
}

#[tokio::test]
async fn never_gives_up_when_max_retries_is_none() {
    // Far past any bounded policy's budget — RETRY_UNTIL_UP must still get there.
    let (result, attempts, _, _) =
        run_connect(RETRY_UNTIL_UP, 250, || anyhow::anyhow!("connect ECONNREFUSED 172.18.0.3:3000")).await;
    assert_eq!(result.unwrap(), "connected");
    assert_eq!(attempts, 250);
}

#[tokio::test]
async fn gives_up_after_max_retries_on_a_bounded_policy() {
    let bounded = ConnectRetryPolicy { max_retries: Some(3), base_delay_ms: 10, max_delay_ms: 100 };
    let (result, attempts, slept, _) = run_connect(bounded, usize::MAX, || anyhow::anyhow!("fetch failed")).await;
    assert_eq!(result.unwrap_err().to_string(), "fetch failed");
    // 1 initial attempt + maxRetries.
    assert_eq!(attempts, 4);
    assert_eq!(slept, [10, 20, 40]);
}

#[test]
fn keeps_the_shipped_mid_flight_policy_bounded() {
    // A tool call the workflow executor is awaiting must not hang forever.
    assert!(CONNECT_RETRY.max_retries.is_some());
    assert!(RETRY_UNTIL_UP.max_retries.is_none());
}

#[tokio::test]
async fn rethrows_a_fatal_error_immediately_without_sleeping() {
    let (result, attempts, slept, _) =
        run_connect(RETRY_UNTIL_UP, usize::MAX, || anyhow::anyhow!("HTTP 404: not found")).await;
    assert!(result.unwrap_err().to_string().contains("not found"));
    assert_eq!(attempts, 1);
    assert!(slept.is_empty());
}

#[tokio::test]
async fn emits_one_on_retry_per_failed_attempt_so_the_droplet_logs_show_the_wait() {
    let (_, _, _, seen) = run_connect(CONNECT_RETRY, 3, already_connected).await;
    assert_eq!(seen, [1, 2]);
}

#[derive(Default)]
struct State {
    ledger: Mutex<Vec<String>>,
    began: tokio::sync::Notify,
    cancelled: AtomicUsize,
    lose_reply: AtomicBool,
    offline: AtomicBool,
    startup_failures: AtomicUsize,
}
#[derive(Clone)]
struct Probe(Arc<State>);
impl ServerHandler for Probe {
    fn get_info(&self) -> ServerConfig {
        let mut info = ServerConfig::default();
        info.capabilities = ServerCapabilities::builder().enable_tools().build();
        info
    }
    async fn list_tools(
        &self,
        _: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        Ok(ListToolsResult::with_all_items(vec![Tool::new(
            "effect",
            "records a side effect",
            Arc::new(json!({"type":"object"}).as_object().unwrap().clone()),
        )]))
    }
    async fn call_tool(
        &self,
        p: CallToolRequestParams,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let args = p.arguments.unwrap_or_default();
        let id = args.get("id").and_then(Value::as_str).unwrap_or("unknown");
        self.0.ledger.lock().unwrap().push(format!("start {id}"));
        self.0.began.notify_one();
        if args.get("wait") == Some(&Value::Bool(true)) {
            ctx.ct.cancelled().await;
            self.0.cancelled.fetch_add(1, Ordering::SeqCst);
            return Ok(CallToolResult::success(Vec::new()).into());
        }
        self.0.ledger.lock().unwrap().push(format!("done {id}"));
        if args.get("error") == Some(&Value::Bool(true)) {
            return Ok(CallToolResult::error(vec![ContentBlock::text("bad action")]).into());
        }
        Ok(CallToolResult::success(vec![ContentBlock::text("ok")]).into())
    }
}
struct Server {
    url: String,
    state: Arc<State>,
    manager: Arc<LocalSessionManager>,
    task: tokio::task::JoinHandle<()>,
    stop: CancellationToken,
}
impl Server {
    async fn new() -> Self {
        let state = Arc::new(State::default());
        let manager = Arc::new(LocalSessionManager::default());
        let stop = CancellationToken::new();
        let probe = Probe(state.clone());
        let mut config = StreamableHttpServerConfig::default().disable_allowed_hosts();
        config.cancellation_token = stop.clone();
        let service = StreamableHttpService::new(move || Ok(probe.clone()), manager.clone(), config);
        let filter_state = state.clone();
        let app = axum::Router::new().nest_service("/mcp", service).layer(axum::middleware::from_fn(
            move |req: axum::extract::Request, next: axum::middleware::Next| {
                let state = filter_state.clone();
                async move {
                    let startup = state
                        .startup_failures
                        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                        .is_ok();
                    if startup || state.offline.load(Ordering::SeqCst) {
                        return axum::response::Response::builder()
                            .status(503)
                            .body(axum::body::Body::from("offline"))
                            .unwrap();
                    }
                    let is_post = req.method() == axum::http::Method::POST;
                    let response = next.run(req).await;
                    if is_post && state.lose_reply.swap(false, Ordering::SeqCst) {
                        // The action completed, but its reply got lost. This text
                        // must not be mistaken for a definitive session rejection.
                        return axum::response::Response::builder()
                            .status(503)
                            .body(axum::body::Body::from("Session not found upstream after effect"))
                            .unwrap();
                    }
                    response
                }
            },
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self { url: format!("http://{addr}/mcp"), state, manager, task, stop }
    }
    async fn evict(&self) {
        let ids: Vec<_> = self.manager.sessions.read().await.keys().cloned().collect();
        for id in ids {
            self.manager.close_session(&id).await.unwrap();
        }
    }
    async fn connect(&self) -> (Arc<McpClient>, Arc<AtomicUsize>) {
        let open_http = open_http(self.url.clone());
        let connections = Arc::new(AtomicUsize::new(0));
        let count = connections.clone();
        let open: Open = Arc::new(move || {
            count.fetch_add(1, Ordering::SeqCst);
            open_http()
        });
        let (sleep, _) = recording_sleep();
        (
            Arc::new(
                McpClient::connect(
                    open,
                    ConnectOptions {
                        startup_retry: ConnectRetryPolicy { base_delay_ms: 0, max_delay_ms: 0, ..CONNECT_RETRY },
                        sleep,
                    },
                )
                .await
                .unwrap(),
            ),
            connections,
        )
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.stop.cancel();
        self.task.abort();
    }
}
fn args(id: &str) -> Map<String, Value> {
    json!({"id":id}).as_object().unwrap().clone()
}
// ── connectMcp ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn survives_an_mcp_that_500s_the_first_initializes_and_yields_a_working_handle() {
    let server = Server::new().await;
    server.state.startup_failures.store(3, Ordering::SeqCst);
    let (client, connections) = server.connect().await;
    assert!(connections.load(Ordering::SeqCst) > 1);
    assert_eq!(client.tools().iter().map(|t| t.name.as_str()).collect::<Vec<_>>(), ["effect"]);
    assert_eq!(client.call_tool("effect", args("one"), &CancellationToken::new()).await.unwrap(), "ok");
    client.close().await;
}

#[tokio::test]
async fn survives_an_mcp_that_is_not_listening_yet() {
    let server = Server::new().await;
    let open_http = open_http(server.url.clone());
    let attempts = Arc::new(AtomicUsize::new(0));
    let count = attempts.clone();
    let open: Open = Arc::new(move || {
        if count.fetch_add(1, Ordering::SeqCst) < 5 {
            return Box::pin(async { Err(anyhow::anyhow!("fetch failed")) });
        }
        open_http()
    });
    let (sleep, _) = recording_sleep();
    let startup_retry = ConnectRetryPolicy { max_retries: None, base_delay_ms: 0, max_delay_ms: 0 };
    let client = McpClient::connect(open, ConnectOptions { startup_retry, sleep }).await.unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 6);
    assert_eq!(client.call_tool("effect", args("one"), &CancellationToken::new()).await.unwrap(), "ok");
    client.close().await;
}

#[tokio::test]
async fn still_fails_fast_on_a_deterministic_rejection() {
    // A wrong MCP_URL must not be retried into invisibility — the supervisor
    // should die on it so the misconfiguration is obvious.
    let server = Server::new().await;
    let open = open_http(server.url.replace("/mcp", "/wrong"));
    let (sleep, delays) = recording_sleep();
    let result = McpClient::connect(open, ConnectOptions { startup_retry: RETRY_UNTIL_UP, sleep }).await;
    assert!(result.is_err());
    assert!(delays.lock().unwrap().is_empty());
}

// ── Rust-only ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn a_tool_failure_is_text_and_a_closed_client_refuses_calls() {
    let server = Server::new().await;
    let (client, _) = server.connect().await;
    let mut a = args("error");
    a.insert("error".into(), true.into());
    assert_eq!(client.call_tool("effect", a, &CancellationToken::new()).await.unwrap(), "[tool error] bad action");
    client.close().await;
    assert!(client.call_tool("effect", args("closed"), &CancellationToken::new()).await.is_err());
}
#[tokio::test]
async fn session_evictions_share_one_reconnect_and_never_duplicate_effects() {
    let server = Server::new().await;
    let (client, connections) = server.connect().await;
    for round in 0..4 {
        server.evict().await;
        let calls = (0..3).map(|i| {
            let client = client.clone();
            async move { client.call_tool("effect", args(&format!("{round}-{i}")), &CancellationToken::new()).await }
        });
        for result in futures::future::join_all(calls).await {
            assert_eq!(result.unwrap(), "ok");
        }
        assert_eq!(connections.load(Ordering::SeqCst), round + 2);
    }
    let ledger = server.state.ledger.lock().unwrap().clone();
    assert_eq!(ledger.len(), 24);
    assert_eq!(ledger.iter().collect::<std::collections::HashSet<_>>().len(), 24);
    client.close().await;
}
#[tokio::test]
async fn forwards_cancellation_to_an_in_flight_mcp_request_without_reconnecting() {
    let server = Server::new().await;
    let (client, connections) = server.connect().await;
    let cancel = CancellationToken::new();
    let c = client.clone();
    let signal = cancel.clone();
    let mut a = args("wait");
    a.insert("wait".into(), true.into());
    let task = tokio::spawn(async move { c.call_tool("effect", a, &signal).await });
    tokio::time::timeout(Duration::from_secs(2), server.state.began.notified()).await.unwrap();
    cancel.cancel();
    assert!(task.await.unwrap().is_err());
    tokio::time::timeout(Duration::from_secs(2), async {
        while server.state.cancelled.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(connections.load(Ordering::SeqCst), 1);
    client.close().await;
}
#[tokio::test]
async fn a_lost_reply_after_a_side_effect_is_never_replayed() {
    let server = Server::new().await;
    let (client, connections) = server.connect().await;
    server.state.lose_reply.store(true, Ordering::SeqCst);
    assert!(client.call_tool("effect", args("once"), &CancellationToken::new()).await.is_err());
    assert_eq!(*server.state.ledger.lock().unwrap(), ["start once", "done once"]);
    assert_eq!(connections.load(Ordering::SeqCst), 1);
    assert_eq!(client.call_tool("effect", args("next"), &CancellationToken::new()).await.unwrap(), "ok");
    client.close().await;
}

#[tokio::test]
async fn durable_transport_reports_ambiguous_effect_without_reissuing_it() {
    use agent::mcp::ToolCallFailure;
    let server = Server::new().await;
    let (client, connections) = server.connect().await;
    server.state.lose_reply.store(true, Ordering::SeqCst);
    assert!(matches!(
        client.call_tool_once("effect", args("durable"), &CancellationToken::new()).await,
        Err(ToolCallFailure::Unknown(_))
    ));
    assert_eq!(*server.state.ledger.lock().unwrap(), ["start durable", "done durable"]);
    assert_eq!(connections.load(Ordering::SeqCst), 1);
    client.close().await;
    assert!(matches!(
        client.call_tool_once("effect", args("closed"), &CancellationToken::new()).await,
        Err(ToolCallFailure::NotExecuted(_))
    ));
}

#[tokio::test]
async fn durable_transport_can_prove_session_rejection_before_execution() {
    use agent::mcp::ToolCallFailure;
    let server = Server::new().await;
    let (client, _) = server.connect().await;
    server.evict().await;
    let cancel = CancellationToken::new();
    match client.call_tool_once("effect", args("fresh"), &cancel).await {
        Ok(text) => assert_eq!(text, "ok"), // Transport closed before dispatch; safely reconnected.
        Err(ToolCallFailure::NotExecuted(_)) => {
            assert!(server.state.ledger.lock().unwrap().is_empty());
            assert_eq!(client.call_tool_once("effect", args("fresh"), &cancel).await.unwrap(), "ok");
        }
        Err(ToolCallFailure::Unknown(error)) => panic!("session rejection was ambiguous: {error}"),
    }
    assert_eq!(*server.state.ledger.lock().unwrap(), ["start fresh", "done fresh"]);
    client.close().await;
}
