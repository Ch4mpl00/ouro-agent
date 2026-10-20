// Third-party MCP upstreams, re-exposed through this server: the agent sees
// one merged tool list, upstream tools namespaced as `${prefix}__${tool}`
// (`tavily__tavily_search`). Onboarding is config + secret + skill
// frontmatter, no code (.claude/tasks/done/mcp-gateway.md).
//
// Sections:
//   1. config  — gateway.config.json, ${VAR} secrets resolved from the env
//   2. clients — one Streamable HTTP client per upstream, reconnect-once
//   3. gateway — merged tool list + routing, failures isolated per upstream
//
// Unlike the TS gateway, there is no wrapper server around own-MCP: the
// handler in server.rs asks this module only for the tools it doesn't own,
// so own tools keep their names and always win a collision.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use rmcp::model::{CallToolRequestParams, CallToolResult, ContentBlock, Tool};
use rmcp::service::RunningService;
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::{RoleClient, ServiceExt};
use serde::Deserialize;
use tokio::sync::RwLock;

// ── 1. config ────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct ConfigFile {
    #[serde(default)]
    upstreams: Vec<UpstreamConfig>,
}

#[derive(Deserialize)]
struct UpstreamConfig {
    name: String,
    // Only "http": a stdio upstream would spawn a child inside the poller
    // process, out of scope until the gateway has its own container.
    #[serde(default = "http")]
    transport: String,
    url: String,
    prefix: Option<String>,
    #[serde(default)]
    headers: HashMap<String, String>,
    #[serde(default = "enabled")]
    enabled: bool,
}

fn http() -> String {
    "http".into()
}

fn enabled() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq)]
pub struct Upstream {
    pub name: String,
    pub url: String,
    pub prefix: String,
    pub headers: Vec<(String, String)>,
}

fn ident_ok(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

// ${VAR} → env. A missing variable is recorded so the caller can skip the
// upstream rather than connect with an empty credential.
fn interpolate(value: &str, env: &dyn Fn(&str) -> Option<String>, missing: &mut Vec<String>) -> String {
    let re = regex::Regex::new(r"\$\{([A-Z0-9_]+)\}").expect("valid regex");
    re.replace_all(value, |caps: &regex::Captures<'_>| match env(&caps[1]).filter(|v| !v.is_empty()) {
        Some(v) => v,
        None => {
            missing.push(caps[1].to_owned());
            String::new()
        }
    })
    .into_owned()
}

// The enabled, fully resolvable upstreams. A missing file means none — the
// gateway is a no-op. One misconfigured upstream is logged and skipped,
// never fatal.
pub fn load_config(path: &Path, env: &dyn Fn(&str) -> Option<String>) -> anyhow::Result<Vec<Upstream>> {
    let raw = match std::fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err.into()),
    };
    let file: ConfigFile = serde_json::from_str(&raw)?;
    let mut out = Vec::new();
    for u in file.upstreams {
        anyhow::ensure!(ident_ok(&u.name), "gateway: name must be [a-zA-Z0-9_-], got {:?}", u.name);
        anyhow::ensure!(u.prefix.as_deref().is_none_or(ident_ok), "gateway: prefix must be [a-zA-Z0-9_-]");
        anyhow::ensure!(
            u.transport == "http",
            "gateway: upstream {} has unsupported transport {:?}",
            u.name,
            u.transport
        );
        if !u.enabled {
            continue;
        }
        let mut missing = Vec::new();
        let url = interpolate(&u.url, env, &mut missing);
        let headers = u.headers.iter().map(|(k, v)| (k.clone(), interpolate(v, env, &mut missing))).collect();
        if !missing.is_empty() {
            missing.sort();
            missing.dedup();
            tracing::warn!(
                upstream = u.name,
                vars = missing.join(", "),
                "skipping gateway upstream: unresolved env var(s)"
            );
            continue;
        }
        out.push(Upstream { prefix: u.prefix.unwrap_or_else(|| u.name.clone()), name: u.name, url, headers });
    }
    Ok(out)
}

// ── 2. clients ───────────────────────────────────────────────────────────────

const CALL_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const SEPARATOR: &str = "__";

type Session = RunningService<RoleClient, ()>;

// A remote MCP session. Sessions are stateful upstream: a restart there
// invalidates ours, so a lost connection is reopened once and the call
// retried.
struct RemoteClient {
    upstream: Upstream,
    session: RwLock<Arc<Session>>,
}

async fn open(upstream: &Upstream) -> anyhow::Result<Session> {
    let mut headers = reqwest::header::HeaderMap::new();
    for (k, v) in &upstream.headers {
        headers.insert(reqwest::header::HeaderName::from_bytes(k.as_bytes())?, v.parse()?);
    }
    let http = reqwest::Client::builder().default_headers(headers).build()?;
    let transport = StreamableHttpClientTransport::with_client(
        http,
        StreamableHttpClientTransportConfig::with_uri(upstream.url.as_str()),
    );
    Ok(().serve(transport).await?)
}

fn connection_lost(err: &str) -> bool {
    [
        "No valid session id",
        "Session not found",
        "Not connected",
        "terminated",
        "connection refused",
        "Connection refused",
        "closed",
    ]
    .iter()
    .any(|needle| err.contains(needle))
}

impl RemoteClient {
    async fn connect(upstream: Upstream) -> anyhow::Result<Self> {
        let session = tokio::time::timeout(CONNECT_TIMEOUT, open(&upstream)).await.map_err(|_| {
            anyhow::anyhow!("upstream '{}' connect timed out after {CONNECT_TIMEOUT:?}", upstream.name)
        })??;
        Ok(Self { upstream, session: RwLock::new(Arc::new(session)) })
    }

    async fn list_tools(&self) -> anyhow::Result<Vec<Tool>> {
        let session = self.session.read().await.clone();
        Ok(tokio::time::timeout(CALL_TIMEOUT, session.list_all_tools())
            .await
            .map_err(|_| anyhow::anyhow!("listTools timed out"))??)
    }

    async fn call_once(session: &Session, request: CallToolRequestParams) -> anyhow::Result<CallToolResult> {
        Ok(tokio::time::timeout(CALL_TIMEOUT, session.call_tool(request))
            .await
            .map_err(|_| anyhow::anyhow!("call timed out after {}ms", CALL_TIMEOUT.as_millis()))??)
    }

    async fn call(&self, request: CallToolRequestParams) -> anyhow::Result<CallToolResult> {
        let used = self.session.read().await.clone();
        match Self::call_once(&used, request.clone()).await {
            Ok(result) => Ok(result),
            Err(err) if connection_lost(&format!("{err:#}")) => {
                tracing::warn!(upstream = self.upstream.name, "gateway upstream connection lost, reconnecting");
                let fresh = {
                    let mut slot = self.session.write().await;
                    // Single flight: another call may have reconnected already.
                    if Arc::ptr_eq(&slot, &used) {
                        *slot = Arc::new(open(&self.upstream).await?);
                    }
                    slot.clone()
                };
                Self::call_once(&fresh, request).await
            }
            Err(err) => Err(err),
        }
    }
}

// ── 3. gateway ───────────────────────────────────────────────────────────────

// What OpenAI-compatible clients accept as a tool name; a namespaced name
// that breaks it is dropped, not surfaced and then rejected mid-session.
fn exposed_name_ok(name: &str) -> bool {
    (1..=64).contains(&name.len()) && ident_ok(name)
}

pub struct Gateway {
    clients: Vec<RemoteClient>,
    tools: Vec<Tool>,
    // exposed name → (client index, original name)
    routes: HashMap<String, (usize, String)>,
}

impl Gateway {
    // Connects every upstream concurrently and lists its tools once (the
    // agent lists tools once; deploys are lockstep). Unreachable upstreams
    // and colliding names are skipped with a log.
    pub async fn connect(upstreams: Vec<Upstream>, own_tools: &[String]) -> Self {
        let connected = futures::future::join_all(upstreams.into_iter().map(|u| async move {
            let name = u.name.clone();
            match RemoteClient::connect(u).await {
                Ok(client) => Some(client),
                Err(err) => {
                    tracing::warn!(upstream = name, error = %format!("{err:#}"), "gateway upstream unavailable, skipping");
                    None
                }
            }
        }))
        .await;
        let mut gateway = Gateway { clients: Vec::new(), tools: Vec::new(), routes: HashMap::new() };
        for client in connected.into_iter().flatten() {
            let tools = match client.list_tools().await {
                Ok(tools) => tools,
                Err(err) => {
                    tracing::warn!(upstream = client.upstream.name, error = %format!("{err:#}"), "gateway listTools failed, skipping its tools");
                    continue;
                }
            };
            let index = gateway.clients.len();
            for mut tool in tools {
                let exposed = format!("{}{SEPARATOR}{}", client.upstream.prefix, tool.name);
                if !exposed_name_ok(&exposed) {
                    tracing::warn!(tool = %tool.name, exposed, "dropping gateway tool: invalid exposed name");
                    continue;
                }
                if gateway.routes.contains_key(&exposed) || own_tools.contains(&exposed) {
                    tracing::warn!(exposed, "dropping gateway tool: name already taken");
                    continue;
                }
                gateway.routes.insert(exposed.clone(), (index, tool.name.to_string()));
                tool.name = exposed.into();
                gateway.tools.push(tool);
            }
            gateway.clients.push(client);
        }
        let sources: Vec<&str> = gateway.clients.iter().map(|c| c.upstream.name.as_str()).collect();
        tracing::info!(
            tools = gateway.tools.len(),
            upstreams = sources.join(", "),
            "gateway aggregated upstream tools"
        );
        gateway
    }

    pub fn tools(&self) -> &[Tool] {
        &self.tools
    }

    pub fn has_tool(&self, name: &str) -> bool {
        self.routes.contains_key(name)
    }

    pub fn tool(&self, name: &str) -> Option<Tool> {
        self.tools.iter().find(|t| t.name == name).cloned()
    }

    // An upstream failure is a tool error for that call; the gateway stays up.
    pub async fn call(&self, mut request: CallToolRequestParams) -> CallToolResult {
        let Some((index, original)) = self.routes.get(request.name.as_ref()).cloned() else {
            return CallToolResult::error(vec![ContentBlock::text(format!(
                "[gateway] unknown tool: {}",
                request.name
            ))]);
        };
        let client = &self.clients[index];
        request.name = original.into();
        match client.call(request).await {
            Ok(result) => result,
            Err(err) => CallToolResult::error(vec![ContentBlock::text(format!(
                "[gateway] upstream '{}' failed: {err:#}",
                client.upstream.name
            ))]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_config(body: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!("gateway-{}.json", rand::random::<u64>()));
        std::fs::write(&path, body).unwrap();
        path
    }

    #[test]
    fn resolves_secrets_and_defaults_the_prefix() {
        let path = write_config(
            r#"{ "upstreams": [
                { "name": "tavily", "url": "https://mcp.tavily.com/mcp/?tavilyApiKey=${TAVILY_API_KEY}" },
                { "name": "off", "url": "https://x", "enabled": false },
                { "name": "nokey", "url": "https://y", "headers": { "Authorization": "Bearer ${MISSING}" } }
            ] }"#,
        );
        let env = |name: &str| (name == "TAVILY_API_KEY").then(|| "secret".to_owned());
        let upstreams = load_config(&path, &env).unwrap();
        assert_eq!(
            upstreams,
            vec![Upstream {
                name: "tavily".into(),
                url: "https://mcp.tavily.com/mcp/?tavilyApiKey=secret".into(),
                prefix: "tavily".into(),
                headers: vec![]
            }]
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn a_missing_file_means_no_upstreams_and_bad_names_fail() {
        assert!(load_config(Path::new("/nonexistent/gateway.json"), &|_| None).unwrap().is_empty());
        let path = write_config(r#"{ "upstreams": [{ "name": "bad name", "url": "https://x" }] }"#);
        assert!(load_config(&path, &|_| None).is_err());
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn exposed_names_follow_the_openai_tool_name_rule() {
        assert!(exposed_name_ok("tavily__tavily_search"));
        assert!(!exposed_name_ok("tavily__search.v2"));
        assert!(!exposed_name_ok(&"x".repeat(65)));
    }
}
