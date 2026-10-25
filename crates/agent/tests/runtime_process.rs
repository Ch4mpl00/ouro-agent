// Full composition-root smoke: compiled binary, real MCP + LLM HTTP, real
// SQLite and real OTLP exporter. Every service is a local fixture; env_clear
// prevents production credentials/configuration from entering the child.
mod support;
use agent::db::{AgentDb, TraceStore};
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, ListToolsResult, PaginatedRequestParams,
    ServerCapabilities, ServerConfig, Tool,
};
use rmcp::service::RequestContext;
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use rmcp::{ErrorData, RoleServer, ServerHandler};
use serde_json::{Value, json};
use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
#[derive(Default)]
struct State {
    source: String,
    fail_send: bool,
    wait_model: bool,
    polls: AtomicUsize,
    sends: AtomicUsize,
    requests: Mutex<Vec<Value>>,
    decisions: Mutex<Vec<Value>>,
    exports: Mutex<Vec<Value>>,
    journal_entries: Mutex<Vec<Value>>,
    model_started: tokio::sync::Notify,
}
#[derive(Clone)]
struct Server(Arc<State>);
impl ServerHandler for Server {
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
        Ok(ListToolsResult::with_all_items(
            [
                "get_next_signal",
                "get_timezone",
                "get_telegram_chat_history",
                "journal_tail",
                "journal_append",
                "send_telegram_message",
                "telegram_send_status",
            ]
            .iter()
            .map(|n| Tool::new(*n, *n, Arc::new(json!({"type":"object"}).as_object().unwrap().clone())))
            .collect(),
        ))
    }
    async fn call_tool(
        &self,
        p: CallToolRequestParams,
        _: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let result = match p.name.as_ref() {
            "get_next_signal" => {
                if self.0.polls.fetch_add(1, Ordering::SeqCst) == 0 {
                    json!({"signal":{"id":42,"source":self.0.source,"content":if self.0.source=="telegram"{"Telegram message in chat 42.\nText: \"hello\""}else{"Task: reply in Telegram."},"envContext":"Send to chat 42","created_at":"2026-10-01T12:00:00Z"},"pendingAfter":0})
                } else {
                    json!({"signal":null,"pendingAfter":0})
                }
            }
            "get_timezone" => json!({"timezone":"Europe/Chisinau"}),
            "get_telegram_chat_history" => json!({"messages":[]}),
            "journal_tail" => json!({"entries":[]}),
            "journal_append" => {
                self.0.journal_entries.lock().unwrap().push(json!(p.arguments));
                json!({"entry_id":7})
            }
            "send_telegram_message" => {
                self.0.sends.fetch_add(1, Ordering::SeqCst);
                if self.0.fail_send {
                    return Ok(CallToolResult::error(vec![ContentBlock::text("partial delivery")]).into());
                }
                json!({"delivered":true,"chatId":"42","messageThreadId":null,"messageId":1})
            }
            _ => json!({"ok":true}),
        };
        Ok(CallToolResult::success(vec![ContentBlock::text(result.to_string())]).into())
    }
}
struct Fixture {
    _dir: tempfile::TempDir,
    root: std::path::PathBuf,
    state: Arc<State>,
    url: String,
    stop: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.stop.cancel();
        self.task.abort();
    }
}
impl Fixture {
    async fn new(source: &str, fail_send: bool, wait_model: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_owned();
        // These fixtures exercise one supplied signal, not system maintenance.
        AgentDb::open(root.join("agent.db"))
            .unwrap()
            .conn()
            .execute("DELETE FROM scheduled_tasks WHERE workflow = 'skill-index'", [])
            .unwrap();
        std::fs::create_dir(root.join("skills.default")).unwrap();
        for (name, tools, body) in [
            ("orchestrator", "*", "ORCHESTRATOR"),
            ("routing", "[]", "ROUTING"),
            ("recovery", "[]", "RECOVERY"),
            ("telegram", "*", "TELEGRAM"),
            ("scheduler", "*", "SCHEDULER"),
            ("worker", "*", "WORKER"),
            ("composer", "[]", "COMPOSER"),
        ] {
            std::fs::write(
                root.join("skills.default").join(format!("{name}.md")),
                format!("---\ntools: {tools}\n---\n{body}"),
            )
            .unwrap();
        }
        let state = Arc::new(State { source: source.into(), fail_send, wait_model, ..Default::default() });
        let server = Server(state.clone());
        let stop = CancellationToken::new();
        let mut config = StreamableHttpServerConfig::default().disable_allowed_hosts();
        config.cancellation_token = stop.clone();
        let mcp =
            StreamableHttpService::new(move || Ok(server.clone()), Arc::new(LocalSessionManager::default()), config);
        let llm_state = state.clone();
        let export_state = state.clone();
        let decision_state = state.clone();
        let app=axum::Router::new().nest_service("/mcp",mcp).route("/chat/completions",axum::routing::post(move|headers:axum::http::HeaderMap,axum::Json(body):axum::Json<Value>|{let state=llm_state.clone();async move{
   assert_eq!(headers["authorization"],"Bearer fixture-key");state.requests.lock().unwrap().push(body.clone());state.model_started.notify_one();if state.wait_model{std::future::pending::<()>().await;}
   let system=body["messages"][0]["content"].as_str().unwrap_or_default();let message=if system.starts_with("You write one entry of the activity journal"){json!({"role":"assistant","content":"Request received.\nSent a message to Telegram."})}else if system.contains("RECOVERY")||body["messages"].as_array().unwrap().iter().any(|m|m["role"]=="tool"){json!({"role":"assistant","content":"done"})}else{json!({"role":"assistant","content":null,"tool_calls":[{"id":"send","type":"function","function":{"name":"send_telegram_message","arguments":"{\"chatId\":\"42\",\"text\":\"agent\"}"}}]})};axum::Json(json!({"model":"gpt-fixture","choices":[{"message":message,"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12}}))
  }})).route("/decisions", axum::routing::post(move |headers: axum::http::HeaderMap, axum::Json(body): axum::Json<Value>| {
            let state = decision_state.clone();
            async move {
                assert_eq!(headers["authorization"], "Bearer fixture-key");
                state.decisions.lock().unwrap().push(body.clone());
                let answers: serde_json::Map<String, Value> = body["questions"].as_object().unwrap().iter().map(|(key, q)| {
                    let answer = if q["type"] == "choice" {
                        json!({"type":"choice","choice":"allow","confidence":1.0,"probabilities":{"allow":1.0}})
                    } else { json!({"type":"noul","noul":0.0}) };
                    (key.clone(), answer)
                }).collect();
                axum::Json(json!({"model":"guard-fixture","answers":answers}))
            }
        })).route("/api/public/otel/v1/traces",axum::routing::post(move|headers:axum::http::HeaderMap,axum::Json(body):axum::Json<Value>|{let state=export_state.clone();async move{assert_eq!(headers["authorization"],"Basic cHVibGljOnNlY3JldA==");state.exports.lock().unwrap().push(body);axum::http::StatusCode::OK}}));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self { _dir: dir, root, state, url: format!("http://{addr}"), stop, task }
    }
    fn child(&self, once: bool) -> tokio::process::Command {
        let mut cmd = tokio::process::Command::new(env!("CARGO_BIN_EXE_agent-supervisor"));
        cmd.current_dir(&self.root)
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("AGENT_ROOT", &self.root)
            .env("AGENT_DB_PATH", self.root.join("agent.db"));
        for name in ["OPENAI_API_KEY", "DEEPSEEK_API_KEY", "GEMINI_API_KEY", "OPEN_ROUTER_API_KEY"] {
            cmd.env(name, "fixture-key");
        }
        for name in ["AGENT_BASE_MODEL", "AGENT_SMART_MODEL", "AGENT_SMARTEST_MODEL", "AGENT_COMPILER_MODEL"] {
            cmd.env(name, "gpt-fixture");
        }
        cmd.env("MCP_TRANSPORT", "http")
            .env("MCP_URL", format!("{}/mcp", self.url))
            .env("OPENAI_BASE_URL", &self.url)
            .env("TELEGRAM_DEFAULT_CHAT_ID", "42")
            .env("OPENROUTER_DECISIONS_URL", format!("{}/decisions", self.url))
            .env("ACTIVITY_JOURNAL", "off")
            .env("LANGFUSE_BASE_URL", &self.url)
            .env("LANGFUSE_PUBLIC_KEY", "public")
            .env("LANGFUSE_SECRET_KEY", "secret")
            .kill_on_drop(true)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if once {
            cmd.arg("--once");
        }
        cmd
    }
    fn trace(&self) -> (agent::trace_model::Trace, Vec<agent::trace_model::Observation>) {
        let db = AgentDb::open(self.root.join("agent.db")).unwrap();
        let recent = db.list_recent(10, None).unwrap();
        assert_eq!(recent.len(), 1);
        db.get_trace(&recent[0].id).unwrap().unwrap()
    }
}
#[tokio::test]
async fn binary_runs_scheduler_and_agent_sources_and_flushes_matching_otel_ids() {
    for source in ["scheduler", "telegram", "domain"] {
        let f = Fixture::new(source, false, false).await;
        let output = tokio::time::timeout(Duration::from_secs(10), f.child(true).output()).await.unwrap().unwrap();
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        assert_eq!(f.state.sends.load(Ordering::SeqCst), 1);
        // A plain reply to the conversation the request came from skips Jev;
        // a send to the default chat on a scheduler's behalf is judged.
        assert_eq!(
            f.state.decisions.lock().unwrap().iter().any(|r| r["state"]["calls"][0]["name"] == "send_telegram_message"),
            source != "telegram",
            "{source}"
        );
        let (trace, observations) = f.trace();
        assert_eq!(trace.metadata.as_ref().unwrap()["activity"], "run");
        assert!(observations.iter().any(|o| o.meta("tool") == Some(&json!("send_telegram_message"))));
        let exports = f.state.exports.lock().unwrap();
        assert!(!exports.is_empty());
        let spans: Vec<_> = exports
            .iter()
            .flat_map(|body| body["resourceSpans"][0]["scopeSpans"][0]["spans"].as_array().unwrap())
            .collect();
        assert!(spans.iter().all(|s| s["traceId"] == trace.id));
        for observation in observations.iter().filter(|o| o.parent_observation_id.is_some()) {
            assert!(spans.iter().any(|s| s["spanId"] == observation.id), "{}", observation.name);
        }
    }
}
#[tokio::test]
async fn binary_tool_failure_returns_to_the_scheduler_loop_without_recovery() {
    let f = Fixture::new("scheduler", true, false).await;
    let output = tokio::time::timeout(Duration::from_secs(10), f.child(true).output()).await.unwrap().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(f.state.sends.load(Ordering::SeqCst), 1);
    let (trace, obs) = f.trace();
    assert_eq!(trace.tags, ["scheduler", "agent-loop"]);
    assert!(!obs.iter().any(|o| o.name == "recovery"));
    assert!(obs.iter().any(|o| o.meta("tool") == Some(&json!("send_telegram_message")) && o.level == "ERROR"));
    assert_eq!(f.state.requests.lock().unwrap().len(), 2);
}
#[cfg(unix)]
#[tokio::test]
async fn sigterm_cancels_an_active_signal_and_flushes_trace_before_process_exit() {
    let f = Fixture::new("telegram", false, true).await;
    let child = f.child(false).spawn().unwrap();
    tokio::time::timeout(Duration::from_secs(5), f.state.model_started.notified()).await.unwrap();
    let pid = child.id().unwrap();
    assert!(std::process::Command::new("kill").args(["-TERM", &pid.to_string()]).status().unwrap().success());
    let output = tokio::time::timeout(Duration::from_secs(5), child.wait_with_output()).await.unwrap().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let (trace, obs) = f.trace();
    assert_eq!(trace.metadata.as_ref().unwrap()["error"], true);
    assert!(obs.iter().any(|o| o.name == "iter-0" && o.level == "ERROR"));
    assert!(!obs.iter().any(|o| o.name == "recovery"));
    assert!(!f.state.exports.lock().unwrap().is_empty());
}
#[test]
fn binary_configuration_failures_are_deterministic_and_do_not_connect() {
    let dir = tempfile::tempdir().unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_agent-supervisor"))
        .arg("--check-config")
        .env_clear()
        .current_dir(dir.path())
        .output()
        .unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("DEEPSEEK_API_KEY"));
    assert!(!dir.path().join("packages/agent/data/agent.db").exists());
}

#[cfg(unix)]
#[tokio::test]
async fn binary_journal_records_one_completed_run_without_creating_recursive_traces() {
    let f = Fixture::new("telegram", false, false).await;
    let child =
        f.child(false).env("ACTIVITY_JOURNAL", "on").env("ACTIVITY_JOURNAL_POLL_INTERVAL_MS", "20").spawn().unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if !f.state.journal_entries.lock().unwrap().is_empty() {
                let db = AgentDb::open(f.root.join("agent.db")).unwrap();
                let traces = db.list_recent(10, None).unwrap();
                if traces.len() == 1 && db.activity_progress(&traces[0].id).unwrap().is_some_and(|p| p.status == "done")
                {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        std::process::Command::new("kill")
            .args(["-TERM", &child.id().unwrap().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let output = tokio::time::timeout(Duration::from_secs(5), child.wait_with_output()).await.unwrap().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(f.state.sends.load(Ordering::SeqCst), 1);
    let (trace, _) = f.trace();
    let entries = f.state.journal_entries.lock().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["metadata"]["trace_id"], trace.id);
    assert_eq!(entries[0]["metadata"]["source"], "telegram");
    assert!(entries[0]["body"].as_str().unwrap().contains("Sent a message"));
    // The default path takes a closing turn after the reply (the Telegram
    // loop variant that ended on it returns with the versions step).
    assert_eq!(f.state.requests.lock().unwrap().len(), 3, "a reply turn, a closing turn and one journal composition");
    for export in f.state.exports.lock().unwrap().iter() {
        for span in export["resourceSpans"][0]["scopeSpans"][0]["spans"].as_array().unwrap() {
            assert_eq!(span["traceId"], trace.id);
        }
    }
}
