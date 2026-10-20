// The real server process — the mcp-tools binary, main.rs as the composition
// root — booted over HTTP the way the droplet runs `mcp` and `mcp-tunnel`,
// with a gateway config naming one paid upstream. A port of
// packages/mcp/test/server-process.test.ts. Whatever MCP_TOOLSETS says: a
// restricted instance never reaches the upstream (no session, no call,
// nothing of it in tools/list), so a memory or tunnel client cannot spend
// third-party credits; an out-of-scope tool can't be called even by its exact
// name; and the full instance does serve the upstream through the gateway.
//
// The process runs from an empty directory with a minimal environment, so no
// .env with real credentials is ever read, and with MCP_NO_POLLERS=1.

mod support;

use std::collections::BTreeSet;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use mcp_tools::server::{HttpOptions, serve_http};
use mcp_tools::toolsets::parse_toolsets;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, ListToolsResult, PaginatedRequestParams,
    ServerCapabilities, ServerConfig, Tool,
};
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, ServerHandler};
use serde_json::json;
use support::{Outcome, TestDatabase, call, connect, property_runs, runtime};
use tokio_util::sync::CancellationToken;

const GROUPS: [&str; 7] = ["memory", "news-read", "telegram-send", "skills", "signals", "scheduler", "fs"];

// What the paid upstream has seen.
#[derive(Default)]
struct Seen {
    sessions: AtomicUsize,
    charges: AtomicUsize,
}

#[derive(Clone)]
struct Paid(Arc<Seen>);

impl ServerHandler for Paid {
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
        let schema = json!({ "type": "object", "properties": { "q": { "type": "string" } } })
            .as_object()
            .cloned()
            .unwrap_or_default();
        Ok(ListToolsResult::with_all_items(vec![Tool::new("charge", "Costs money per call.", Arc::new(schema))]))
    }

    async fn call_tool(
        &self,
        _: CallToolRequestParams,
        _: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        self.0.charges.fetch_add(1, Ordering::SeqCst);
        Ok(CallToolResult::success(vec![ContentBlock::text(json!({ "charged": true }).to_string())]).into())
    }
}

struct Env {
    db: TestDatabase,
    dir: std::path::PathBuf,
    seen: Arc<Seen>,
    _upstream: CancellationToken,
}

fn env() -> &'static Env {
    static ENV: OnceLock<Env> = OnceLock::new();
    ENV.get_or_init(|| {
        runtime().block_on(async {
            let db = support::test_database().await;
            let dir = std::env::temp_dir().join(format!("server-process-{:016x}", rand::random::<u64>()));
            std::fs::create_dir_all(&dir).unwrap();
            let seen = Arc::new(Seen::default());
            let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
            let cancel = CancellationToken::new();
            let handler_seen = seen.clone();
            tokio::spawn(serve_http(
                move || {
                    handler_seen.sessions.fetch_add(1, Ordering::SeqCst);
                    Paid(handler_seen.clone())
                },
                HttpOptions { bind_address: "127.0.0.1".parse().unwrap(), port, allowed_hosts: Vec::new(), multi_session: true },
                cancel.clone(),
            ));
            std::fs::write(
                dir.join("gateway.config.json"),
                json!({ "upstreams": [{ "name": "paid", "prefix": "paid", "url": format!("http://127.0.0.1:{port}/mcp") }] }).to_string(),
            )
            .unwrap();
            Env { db, dir, seen, _upstream: cancel }
        })
    })
}

// Boots the binary and returns once it answers on its port.
async fn boot(env: &Env, toolsets: Option<&str>) -> (String, Child) {
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_mcp-tools"));
    cmd.current_dir(&env.dir)
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("HOME", std::env::var("HOME").unwrap_or_default())
        .env("MCP_TRANSPORT", "http")
        .env("MCP_HOST", "127.0.0.1")
        .env("MCP_PORT", port.to_string())
        .env("MCP_NO_POLLERS", "1")
        .env("DATABASE_URL", &env.db.url)
        .env("STATE_DATABASE_URL", &env.db.state_url)
        // Required at boot; nothing here embeds, so it is never used.
        .env("OPENAI_API_KEY", "sk-test-unused")
        .env("GATEWAY_CONFIG", env.dir.join("gateway.config.json"))
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(t) = toolsets {
        cmd.env("MCP_TOOLSETS", t);
    }
    let mut child = cmd.spawn().expect("the binary starts");
    let url = format!("http://127.0.0.1:{port}/mcp");
    for _ in 0..600 {
        if reqwest::Client::new().post(&url).send().await.is_ok() {
            return (url, child);
        }
        if let Ok(Some(status)) = child.try_wait() {
            panic!("server exited ({status}) before listening");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let _ = child.kill();
    panic!("server did not start");
}

fn stop(mut child: Child) {
    // SIGTERM, as compose stops a container; the binary shuts down cleanly.
    let _ = Command::new("kill").arg("-TERM").arg(child.id().to_string()).status();
    for _ in 0..100 {
        if let Ok(Some(_)) = child.try_wait() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    let _ = child.kill();
    let _ = child.wait();
}

#[test]
fn only_the_full_instance_reaches_the_paid_upstream_and_out_of_scope_tools_stay_uncallable() {
    let env = env();
    let witnessed = Mutex::new(BTreeSet::new());
    let toolsets = prop_oneof![
        1 => Just(None::<String>),
        1 => Just(Some(String::new())),
        4 => proptest::sample::subsequence(GROUPS.to_vec(), 1..=GROUPS.len()).prop_map(|g| Some(g.join(","))),
    ];
    let run = |toolsets: Option<String>| {
        runtime().block_on(async {
            let restricted = parse_toolsets(toolsets.as_deref()).unwrap().restricted;
            env.seen.sessions.store(0, Ordering::SeqCst);
            env.seen.charges.store(0, Ordering::SeqCst);
            let (url, child) = boot(env, toolsets.as_deref()).await;
            let at = format!("MCP_TOOLSETS={toolsets:?}");
            let client = connect(&url).await;
            let tools = client.list_all_tools().await.unwrap();
            for tool in &tools {
                if tool.name.starts_with("paid__") {
                    assert!(tool.annotations.is_none(), "gateway retains absent upstream annotations");
                } else {
                    assert!(
                        tool.annotations.as_ref().and_then(|a| a.open_world_hint).is_some(),
                        "{} must explicitly decide openWorldHint",
                        tool.name
                    );
                }
            }
            let names: Vec<String> = tools.into_iter().map(|t| t.name.to_string()).collect();
            let charge = call(&client, "paid__charge", json!({ "q": "x" })).await;
            if restricted {
                witnessed.lock().unwrap().insert("restricted");
                assert!(!names.iter().any(|n| n.starts_with("paid__")), "{at}: no upstream tool listed");
                assert!(!matches!(charge, Outcome::Ok(_)), "{at}: the upstream tool can't be called by name");
                assert_eq!(
                    (env.seen.sessions.load(Ordering::SeqCst), env.seen.charges.load(Ordering::SeqCst)),
                    (0, 0),
                    "{at}: the upstream is never contacted"
                );
                if !names.iter().any(|n| n == "get_next_signal") {
                    witnessed.lock().unwrap().insert("signals out of scope");
                    let next = call(&client, "get_next_signal", json!({})).await;
                    assert!(!matches!(next, Outcome::Ok(_)), "{at}: get_next_signal can't be called by name");
                }
            } else {
                witnessed.lock().unwrap().insert("full");
                assert!(names.iter().any(|n| n == "paid__charge"), "{at}: the full instance serves the upstream");
                let Outcome::Ok(value) = charge else { panic!("{at}: and the call reaches it: {charge:?}") };
                assert_eq!(value["charged"], true, "{at}");
                assert_eq!(env.seen.charges.load(Ordering::SeqCst), 1, "{at}");
            }
            let _ = client.cancel().await;
            stop(child);
        });
    };
    // Every boot costs seconds, so the instances prod runs are always among
    // them: `mcp` (everything) and `mcp-tunnel`.
    run(None);
    run(Some("news-read,telegram-send,skills,memory".into()));
    TestRunner::new(Config::with_cases(property_runs(8)))
        .run(&toolsets, |t| {
            run(t);
            Ok(())
        })
        .unwrap();
    assert_eq!(
        witnessed.into_inner().unwrap().into_iter().collect::<Vec<_>>(),
        ["full", "restricted", "signals out of scope"]
    );
}
