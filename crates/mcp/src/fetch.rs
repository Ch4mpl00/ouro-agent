// fetch_url: GET a page and return its RAW body (HTML / JSON / text) — for
// parsing a <table> in a code_agent step, or a URL the search provider can't
// retrieve. Complements tavily_extract, which returns cleaned prose.
//
// Sections:
//   1. ssrf guard — only public addresses, enforced at DNS resolution
//   2. client     — the guarded HTTP client
//   3. tools      — `fetch` toolset (never handed to third-party clients:
//                   it would make this server their proxy)
//
// The URL is checked before the request, and the check also lives in the
// client's resolver and redirect policy, so every connection — each redirect
// hop and a DNS answer that changed since validation — is checked, not just
// the URL the model typed.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::{schemars, tool, tool_router};
use serde::Deserialize;
use serde_json::json;

use crate::server::{McpTools, ToolResult, invalid_params, respond};

// ── 1. ssrf guard ────────────────────────────────────────────────────────────

// Everything that isn't the public internet: this network, RFC1918,
// carrier-grade NAT (Tailscale), loopback, link-local / cloud metadata, the
// same through NAT64, and IPv6 unspecified / loopback / IPv4-compatible,
// unique-local, link-local, site-local and local-use NAT64. IPv4 rules hold
// for the IPv4-mapped spelling too (::ffff:7f00:1 is 127.0.0.1), as Node's
// BlockList applies them in the TS server.
const NON_PUBLIC_V4: [([u8; 4], u32); 7] = [
    ([0, 0, 0, 0], 8),
    ([10, 0, 0, 0], 8),
    ([100, 64, 0, 0], 10),
    ([127, 0, 0, 0], 8),
    ([169, 254, 0, 0], 16),
    ([172, 16, 0, 0], 12),
    ([192, 168, 0, 0], 16),
];
const NON_PUBLIC_V6: [(u128, u32); 5] =
    [(0, 96), (0xfc00 << 112, 7), (0xfe80 << 112, 10), (0xfec0 << 112, 10), ((0x0064_ff9b_0001u128) << 80, 48)];
const V4_MAPPED: u128 = 0xffff << 32;
const NAT64: u128 = 0x0064_ff9b << 96;

fn in_prefix(addr: u128, base: u128, prefix: u32) -> bool {
    prefix == 0 || (addr >> (128 - prefix)) == (base >> (128 - prefix))
}

pub fn is_private_ip(ip: IpAddr) -> bool {
    let addr = match ip {
        IpAddr::V4(v4) => V4_MAPPED | u128::from(u32::from(v4)),
        IpAddr::V6(v6) => u128::from(v6),
    };
    NON_PUBLIC_V4.iter().any(|(octets, len)| {
        let bits = u128::from(u32::from_be_bytes(*octets));
        in_prefix(addr, V4_MAPPED | bits, 96 + len) || in_prefix(addr, NAT64 | bits, 96 + len)
    }) || NON_PUBLIC_V6.iter().any(|(base, prefix)| in_prefix(addr, *base, *prefix))
}

pub type IsPrivate = Arc<dyn Fn(IpAddr) -> bool + Send + Sync>;

fn host_ip(url: &reqwest::Url) -> Option<IpAddr> {
    match url.host()? {
        url::Host::Ipv4(ip) => Some(IpAddr::V4(ip)),
        url::Host::Ipv6(ip) => Some(IpAddr::V6(ip)),
        url::Host::Domain(_) => None,
    }
}

struct PublicOnlyResolver {
    is_private: IsPrivate,
}

impl Resolve for PublicOnlyResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let is_private = self.is_private.clone();
        Box::pin(async move {
            let host = name.as_str().to_owned();
            let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), 0)).await?.collect();
            if let Some(bad) = addrs.iter().find(|a| is_private(a.ip())) {
                let err = format!("refusing to fetch private/loopback address ({host} → {})", bad.ip());
                return Err(err.into());
            }
            let iter: Addrs = Box::new(addrs.into_iter());
            Ok(iter)
        })
    }
}

// ── 2. client ────────────────────────────────────────────────────────────────

const DEFAULT_MAX_BYTES: usize = 2_000_000;
const HARD_MAX_BYTES: usize = 8_000_000;
const TIMEOUT: Duration = Duration::from_secs(20);
// Some sites 403 a missing/unknown User-Agent.
pub const BROWSER_UA: &str =
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

// Every fetch of a URL that came from a model or a feed goes through this:
// the URL is checked before the request (a literal IP never reaches the
// resolver), each DNS answer at connect time, and each redirect target.
// `is_private` is the rule; tests narrow it so a local server can play the
// public internet, as the TS server's createGuardedFetch({ isPrivate }).
#[derive(Clone)]
pub struct GuardedFetch {
    client: reqwest::Client,
    is_private: IsPrivate,
}

impl Default for GuardedFetch {
    fn default() -> Self {
        Self::with_rule(Arc::new(is_private_ip))
    }
}

impl GuardedFetch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_rule(is_private: IsPrivate) -> Self {
        let on_redirect = is_private.clone();
        let client = reqwest::Client::builder()
            .dns_resolver(Arc::new(PublicOnlyResolver { is_private: is_private.clone() }))
            // Literal-IP redirect targets skip the resolver; vet them here.
            .redirect(reqwest::redirect::Policy::custom(move |attempt| {
                let url = attempt.url();
                if url.scheme() != "http" && url.scheme() != "https" {
                    return attempt.error("redirect to a non-http(s) URL");
                }
                if host_ip(url).is_some_and(|ip| on_redirect(ip)) {
                    return attempt.error("redirect to a private/loopback address");
                }
                if attempt.previous().len() >= 10 {
                    return attempt.error("too many redirects");
                }
                attempt.follow()
            }))
            .user_agent(BROWSER_UA)
            .timeout(TIMEOUT)
            .build()
            .expect("static client config is valid");
        Self { client, is_private }
    }

    pub async fn assert_public_url(&self, raw: &str) -> anyhow::Result<reqwest::Url> {
        let url = reqwest::Url::parse(raw).map_err(|_| anyhow::anyhow!("invalid URL: {raw}"))?;
        if url.scheme() != "http" && url.scheme() != "https" {
            anyhow::bail!("unsupported scheme \"{}:\" — only http/https", url.scheme());
        }
        let host = url.host_str().unwrap_or_default().to_owned();
        let addrs: Vec<IpAddr> = match host_ip(&url) {
            Some(ip) => vec![ip],
            None => tokio::net::lookup_host((host.as_str(), 0))
                .await
                .map_err(|_| anyhow::anyhow!("could not resolve host \"{host}\""))?
                .map(|a| a.ip())
                .collect(),
        };
        if let Some(ip) = addrs.into_iter().find(|ip| (self.is_private)(*ip)) {
            anyhow::bail!("refusing to fetch private/loopback address ({host} → {ip})");
        }
        Ok(url)
    }
}

// A response as the tools and the article reader see it: where the fetch
// ended up, its status and type, and the body as a stream the reader pulls
// only as far as it wants.
pub struct Fetched {
    pub url: String,
    pub status: u16,
    pub content_type: Option<String>,
    pub body: futures::stream::BoxStream<'static, anyhow::Result<Vec<u8>>>,
}

// GET a URL that came from outside. The production implementation is
// GuardedFetch; a test can hand in a stand-in web, as the TS server's
// GuardedFetch function type allows.
#[async_trait::async_trait]
pub trait Fetch: Send + Sync {
    async fn get(&self, url: &str, timeout: Duration) -> anyhow::Result<Fetched>;
}

pub type SharedFetch = Arc<dyn Fetch>;

#[async_trait::async_trait]
impl Fetch for GuardedFetch {
    // Checked first, then sent through the guarded client.
    async fn get(&self, raw: &str, timeout: Duration) -> anyhow::Result<Fetched> {
        let url = self.assert_public_url(raw).await?;
        let res = self.client.get(url).header("accept", "*/*").timeout(timeout).send().await.map_err(|err| {
            if err.is_timeout() {
                anyhow::anyhow!("timed out after {}ms", timeout.as_millis())
            } else {
                anyhow::anyhow!("{err:#}")
            }
        })?;
        Ok(Fetched {
            url: res.url().to_string(),
            status: res.status().as_u16(),
            content_type: res
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned),
            body: Box::pin(res.bytes_stream().map(|chunk| chunk.map(|b| b.to_vec()).map_err(anyhow::Error::from))),
        })
    }
}

// The cap is on bytes. Reads past it by at most one chunk, so a body of
// exactly `max_bytes` isn't reported as truncated, and cuts on a character
// boundary: a multi-byte sequence the cap split in two is held back, as a
// streaming decoder does. Returns the text, the bytes read and whether
// anything was cut.
pub async fn read_capped(
    mut stream: futures::stream::BoxStream<'static, anyhow::Result<Vec<u8>>>,
    max_bytes: usize,
) -> anyhow::Result<(String, usize, bool)> {
    let mut body = Vec::new();
    while let Some(chunk) = stream.next().await {
        body.extend_from_slice(&chunk?);
        if body.len() > max_bytes {
            break;
        }
    }
    let total = body.len();
    if total <= max_bytes {
        return Ok((String::from_utf8_lossy(&body).into_owned(), total, false));
    }
    let cut = &body[..max_bytes];
    Ok((String::from_utf8_lossy(&cut[..cut.len() - incomplete_tail(cut)]).into_owned(), total, true))
}

// How many trailing bytes are the start of a UTF-8 sequence that continues
// past the end.
fn incomplete_tail(bytes: &[u8]) -> usize {
    let n = bytes.len();
    for back in 1..=n.min(4) {
        let b = bytes[n - back];
        if b & 0xc0 == 0x80 {
            continue;
        }
        let needed = match b {
            0xc0..=0xdf => 2,
            0xe0..=0xef => 3,
            0xf0..=0xf7 => 4,
            _ => 1,
        };
        return if needed > back { back } else { 0 };
    }
    0
}

fn is_binary(content_type: &str) -> bool {
    let re =
        regex::Regex::new(r"(?i)^(image|audio|video)/|application/(pdf|zip|octet-stream|x-)").expect("valid regex");
    re.is_match(content_type)
}

// ── 3. tools ─────────────────────────────────────────────────────────────────

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct FetchParams {
    /// Absolute http(s) URL.
    url: String,
    /// Cap on bytes returned (default 2000000).
    #[schemars(range(min = 1, max = 8_000_000))]
    max_bytes: Option<usize>,
}

#[tool_router(router = fetch_tools, vis = "pub(crate)")]
impl McpTools {
    #[tool(
        name = "fetch_url",
        annotations(read_only_hint = true),
        title = "Fetch a URL's raw content",
        description = "GET a URL and return its RAW body (HTML / JSON / text). Use when you \
            already have a link and need the raw content — e.g. the raw HTML to \
            parse a <table> in a code_agent step, or a page the search/extract \
            provider can't retrieve. Complements tavily_extract (which returns \
            cleaned prose). Follows redirects; binary content (PDF/image/zip) is \
            not returned — use read_pdf for PDFs."
    )]
    async fn fetch_url(&self, Parameters(p): Parameters<FetchParams>) -> ToolResult {
        if p.max_bytes.is_some_and(|m| m == 0 || m > HARD_MAX_BYTES) {
            return Err(invalid_params("maxBytes must be between 1 and 8000000"));
        }
        let cap = p.max_bytes.unwrap_or(DEFAULT_MAX_BYTES).min(HARD_MAX_BYTES);
        let fetcher = self.deps.fetcher.clone();
        respond(
            async {
                let res = fetcher.get(&p.url, TIMEOUT).await.map_err(|err| anyhow::anyhow!("fetch failed: {err:#}"))?;
                let content_type = res.content_type.unwrap_or_default();
                if is_binary(&content_type) {
                    return Ok(json!({
                        "url": res.url,
                        "status": res.status,
                        "contentType": content_type,
                        "binary": true,
                        "note": "binary content not returned; for a PDF use read_pdf",
                    }));
                }
                let (content, bytes, truncated) = read_capped(res.body, cap).await?;
                Ok(json!({
                    "url": res.url,
                    "status": res.status,
                    "contentType": content_type,
                    "bytes": bytes,
                    "truncated": truncated,
                    "content": content,
                }))
            }
            .await,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn private(ip: &str) -> bool {
        is_private_ip(ip.parse().unwrap())
    }

    #[test]
    fn flags_loopback_rfc1918_link_local_and_metadata() {
        for ip in ["127.0.0.1", "10.0.0.5", "192.168.1.1", "172.16.0.1", "172.31.255.255", "169.254.169.254", "0.0.0.0"]
        {
            assert!(private(ip), "{ip}");
        }
        for ip in [
            "::1",
            "fe80::1",
            "fc00::1",
            "fd12:3456::1",
            "::ffff:10.0.0.1",
            "::ffff:7f00:1",
            "100.64.0.1",
            "64:ff9b::7f00:1",
        ] {
            assert!(private(ip), "{ip}");
        }
    }

    #[test]
    fn allows_public_addresses() {
        for ip in ["8.8.8.8", "1.1.1.1", "172.15.0.1", "172.32.0.1", "2606:4700::1"] {
            assert!(!private(ip), "{ip}");
        }
    }

    #[tokio::test]
    async fn rejects_bad_schemes_private_hosts_and_garbage() {
        let guard = GuardedFetch::new();
        let msg = |r: anyhow::Result<reqwest::Url>| r.unwrap_err().to_string();
        assert!(msg(guard.assert_public_url("file:///etc/passwd").await).contains("scheme"));
        assert!(msg(guard.assert_public_url("ftp://example.com").await).contains("scheme"));
        assert!(
            msg(guard.assert_public_url("http://169.254.169.254/latest/meta-data/").await).contains("private/loopback")
        );
        assert!(msg(guard.assert_public_url("http://127.0.0.1:8080/").await).contains("private/loopback"));
        assert!(msg(guard.assert_public_url("http://localhost/admin").await).contains("private/loopback"));
        assert!(msg(guard.assert_public_url("not a url").await).contains("invalid URL"));
    }

    #[test]
    fn a_cut_holds_back_a_split_character() {
        assert_eq!(incomplete_tail("aé".as_bytes()), 0);
        assert_eq!(incomplete_tail(&"aé".as_bytes()[..2]), 1);
        assert_eq!(incomplete_tail(&"a€".as_bytes()[..3]), 2);
        assert_eq!(incomplete_tail(&"😀".as_bytes()[..3]), 3);
        assert_eq!(incomplete_tail("😀".as_bytes()), 0);
    }

    #[test]
    fn binary_content_types_match_like_the_ts_regex() {
        assert!(is_binary("application/pdf"));
        assert!(is_binary("IMAGE/png"));
        assert!(is_binary("application/x-tar"));
        assert!(!is_binary("text/html; charset=utf-8"));
        assert!(!is_binary("application/json"));
    }
}
