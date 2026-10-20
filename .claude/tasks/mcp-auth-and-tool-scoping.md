# MCP auth and per-client tool scoping

**Status:** in-progress. Scoping per *instance* (A2 mechanism) and the Secure
**Open work:** #45
MCP Tunnel (A6) are shipped; token auth (A1/A3), the `mcp_tokens` store and
per-token roles are not started.
**Priority:** P2 (nothing is public: both MCP endpoints are loopback-only; this
becomes P1 before any public exposure or a second human)
**Area:** mcp / transport, security
**Created:** 2026-08-21

## Context

Several agents (the droplet supervisor, Claude Code on the laptop, ChatGPT)
talk to MCP. Concerns:

- **Authentication.** The HTTP endpoint is unauthenticated. It is safe only
  because it is not reachable from outside: native MCP binds loopback
  (`MCP_HOST=127.0.0.1`), clients come in over `ssh -L` or the tunnel daemon.
- **Every client sees every tool** of its instance: Gmail, Telegram send,
  Monobank, fs read, fetch, the signals queue and whatever the gateway
  re-exposes from third-party upstreams (`tavily__*`, which costs money per
  call). A third-party agent must not get `send_telegram_message` because it
  wanted to read a memory document.
- Multi-connect itself is solved: a restricted instance is multi-session
  (`done/mcp-connection-lifecycle.md`, `session policy` in
  `crates/mcp/src/server.rs`).

## What the clients support (verified 2026-08-21)

| Client | Static API key | OAuth | Notes |
|---|---|---|---|
| ChatGPT custom connector (developer mode) | No | Yes | Cannot present custom API keys, client-credentials, JWT assertions or mTLS. OAuth or none; public HTTPS required. |
| OpenAI Responses API MCP tool | Yes | Yes | Arbitrary `headers` forwarded on every request. |
| Claude Code | Yes | Yes | `.mcp.json` `{"type":"http","url":...,"headers":{"Authorization":"Bearer ${ENV}"}}`. |
| Our own agents | Yes | n/a | We control the client. |

A static bearer token covers everything except the ChatGPT UI. For that client
the answer is not OAuth but the Secure MCP Tunnel (A6), which sidesteps the
credential question: ChatGPT authenticates to OpenAI and a daemon of ours does
the rest.

OAuth in brief (MCP spec 2025-06-18 to 2025-11-25): the MCP server is an OAuth
2.1 *resource server only* (the authorization server is separate: Auth0,
Stytch, Keycloak, WorkOS, Logto; writing one is not advisable). Unauthenticated
request, 401 with `WWW-Authenticate: Bearer resource_metadata=...`; client reads
the Protected Resource Metadata (RFC 9728), discovers the AS, registers (CIMD is
the 2025-11-25 default), runs Authorization Code + PKCE S256, then sends
`Authorization: Bearer` and the server validates token and audience (RFC 8707).

## Decisions

**A1 - two credential carriers, static-token based in v1.** The server accepts
`Authorization: Bearer <token>` (Claude Code, our agents, the Responses API) or
`?api-key=<token>` in the MCP URL (clients that cannot send headers). Same token
store, same scoping. No valid token means 401.

**A2 - the token carries the role and the toolset; clients never declare their
own** (self-declared roles are forgeable). Planned table:
`mcp_tokens(id, name, token_hash, role, toolsets[], created_at, last_used_at,
revoked_at)`. `supervisor` is the only role that gets the signals tools;
everything else is deny-by-default and opts into named toolsets. Unauthorised
tools are *invisible* (not registered), which also keeps prompts small.
*Shipped so far:* scoping per instance, not per token: `MCP_TOOLSETS` picks the
groups a process registers (`crates/mcp/src/toolsets.rs`), and each audience is
a separate process; restricted instances are multi-session. The actor stamped on
writes comes from `MCP_MEMORY_ACTOR` on the instance.

**A3 - prefer not publishing the endpoint at all.** Supervisor: loopback.
ChatGPT/Codex/Responses API: the Secure MCP Tunnel. Claude Code: `ssh -N -L
3001:127.0.0.1:3001` to the droplet (or Tailscale). If a public endpoint is ever
needed, TLS through a reverse proxy (Caddy) is a hard prerequisite.

**A4 - `?api-key=` is fine once the secret stays inside our infrastructure.**
With A6 the URL carrying the key is configured in tunnel-client on our host;
ChatGPT only knows a `tunnel_id`. Query strings land in access logs, so redact
them. Tokens stay per-client, narrowly scoped and independently revocable.

**A5 - OAuth is deferred.** It buys per-user identity and delegation, which a
one-person system lacks, and costs an external authorization server plus
PRM/CIMD/PKCE plumbing. Revisit when a second human needs access or the ChatGPT
UI becomes a daily driver. The design doesn't block it: the token check is one
middleware and OAuth adds a second validator behind the same interface.

**A6 - use OpenAI's Secure MCP Tunnel for ChatGPT instead of exposing anything.**
`tunnel-client` is a customer-run daemon making an outbound-only HTTPS long-poll
to an OpenAI-hosted endpoint, forwarding queued MCP requests to a local MCP
server. Credentials: a `tunnel_id` and a *restricted* runtime API key with
`Tunnels Read + Use` (not an admin key). Caveats: private/developer-mode use
only; OpenAI-only (Claude Code and our agents still need A1/A3); the tunnel
grants whatever MCP serves it, so A2 scoping matters more here; an OAuth AS, if
ever added, is not tunnelled; permission changes take up to about 30 minutes.

## Shipped (current state)

- Restricted endpoint: native `ouro-mcp-tunnel` with `MCP_NO_POLLERS=1`,
  `MCP_TOOLSETS=news-read,telegram-send,skills,memory,peer`, actor `chatgpt`,
  multi-session, loopback only; `tunnel-client` runs in Docker (host network)
  and reaches it at `http://127.0.0.1:3001/mcp`. Nothing is published
  (`docs/operations.md`, Deploy).
- A restricted instance never attaches the gateway, so `tavily__*` cannot leak
  to the tunnel: upstream tools cannot be named in the allow-list, they are
  excluded wholesale.
- `?api-key=` is not needed yet (the endpoint is loopback-only).

## Open

- Token store, 401 middleware, both carriers, per-token toolsets, revocation
  without restart (acceptance below).
- Hygiene: `tunnel-client` takes `env_file: .env.mcp`, so a third-party binary's
  environment holds every integration secret; the tunnel key is a project key
  rather than a restricted `Tunnels Read + Use` one. Split the key into its own
  env file.
- `fetch_article` is in `news-read`, which the tunnel gets: ChatGPT can fetch
  arbitrary pages (through the SSRF guard) and store them in `news_items`, a
  persistent prompt-injection path into the corpus the digests read. Accept it
  deliberately or move `fetch_article` to its own toolset (the `fetch` group's
  comment says a page fetcher "turns our server into their proxy").
- Abandoned multi-session sessions are never released (a session ends only on
  client DELETE); an idle TTL would bound the leak.

## Acceptance

- No MCP port is published. ChatGPT reaches MCP through tunnel-client, Claude
  Code over `ssh -L`/Tailscale; a public endpoint, if ever added, is HTTPS
  behind a proxy.
- A request with no, unknown or revoked token gets 401 (and, with OAuth, a
  `WWW-Authenticate` pointing at PRM).
- Both carriers work: `Authorization: Bearer ...` and `?api-key=...`.
- Two clients with different tokens are connected at once; each `tools/list`
  returns only that token's toolsets (diff the two lists).
- Only the supervisor's token sees `get_next_signal`; another token cannot call
  it even by exact name.
- Gateway tools (`tavily__*`) are scoped like any toolset: a memory client
  cannot spend upstream credits.
- Proxy logs contain no `api-key` value.
- Revoking a token drops its next request without restarting MCP.

## Notes

- Related: `done/unified-memory.md` (the driver; A2 answers its Q9),
  `done/mcp-connection-lifecycle.md`, `done/mcp-gateway.md`,
  `done/peer-messaging.md` (a peer is trusted as the user because the endpoints
  are loopback-only; per-token identity is a precondition for public exposure).
- Claude Code has open issues about headers not being sent during session
  establishment on HTTP transport (anthropics/claude-code #29562, #17069);
  verify our own client path early.
- Tool-count pressure is a real second motive for scoping: own tools plus
  gateway upstreams already put 25+ tools in front of every client.
