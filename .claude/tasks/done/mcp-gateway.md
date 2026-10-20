# MCP gateway: aggregate own and third-party MCP servers

**Status:** done (Tavily wired, pilot passed 2026-06-19; ported to Rust with the
MCP server, `crates/mcp/src/gateway.rs`). Extract-to-container is deferred.
**Priority:** P2
**Area:** infra / mcp / agent
**Created:** 2026-06-19

## Context

The agent connects to exactly one MCP upstream and lists its tools once at boot.
We want to keep our own MCP (first-party integrations, pollers, signal queue)
and also connect a growing set of third-party MCP servers (Tavily first, for web
search/extract), with the agent still seeing one endpoint.

## Decision

**A thin in-process aggregator inside the MCP server.** Own MCP is both an MCP
server (to the agent) and an MCP client (to N third-party upstreams) and
re-exposes their tools under a namespace. The agent talks to one endpoint and
its code is untouched; the tool list just grows.

Why not an off-the-shelf proxy (mcpproxy-go in its own container was the
original plan): reading the sources disproved the "thin and stateless" premise.
The hard requirement is that the agent lists all tools from one StreamableHTTP
endpoint, so the gateway must **merge** upstreams. TBXark/mcp-proxy doesn't merge
(a path per upstream); mcpproxy-go merges (`routing_mode: direct`, `srv__tool`)
but is a heavy security control plane (BoltDB quarantine, per-tool approval,
Docker isolation, PII detection, web UI); MetaMCP needs Postgres + Next.js. The
"merges AND thin" quadrant is empty off the shelf, and for a few trusted
upstreams and one user a small module fits better (user decision, 2026-06-19).

Why in-process rather than a separate container: failure isolation matters most
for opaque heavy binaries. Our own thin code calling remote HTTP upstreams has
the risk class of the existing outbound calls (Gmail, OpenAI embeddings,
Monobank); mitigated by connect and per-call timeouts and error isolation, so a
hung upstream can't block a tool call or the pollers. It is container-ready by
construction (swap the in-memory client for a StreamableHTTP client to the MCP
endpoint).

## How it works (Rust)

- `gateway.config.json` (`crates/mcp/gateway.config.json`, git-tracked,
  secret-free): `{ name, transport: "http", url with ${ENV_VAR} refs, prefix,
  headers?, enabled }`. A missing file means no upstreams; a missing secret
  skips that upstream (logged), never fatal; a non-`http` transport is rejected
  (a stdio upstream would spawn a child inside the poller process, which is the
  trigger to extract the gateway to its own container).
- One Streamable HTTP client per upstream, reconnect once; failures isolated per
  upstream. Tools are listed once at boot and merged: own tools un-prefixed,
  upstream tools as `${prefix}__${tool}` (`__` because OpenAI function names
  forbid `.`); names validated against `^[a-zA-Z0-9_-]{1,64}$`; collisions and
  illegal names dropped and logged; **own tools always win a collision**.
- Tool schemas pass through verbatim; the handler in `server.rs` asks the
  gateway only for the tools it does not own. With no upstream configured the
  server behaves exactly as without a gateway.
- A restricted instance (`MCP_TOOLSETS` set) skips the gateway entirely, so
  upstream tools can never reach the tunnel (see
  `mcp-auth-and-tool-scoping.md`).
- Static tool list built at boot (the agent caches it at boot and the two
  deploy in lockstep); no `tools/list_changed` handling.

Pilot (live, 2026-06-19): connected to Tavily (5 tools), namespaced passthrough
with verbatim schemas, and a deliberately broken upstream was skipped while the
rest served.

## Onboarding contract: adding a new MCP

Connect (config, no code) plus enable (skill):

1. Find the server's StreamableHTTP endpoint and auth.
2. Add an entry to `crates/mcp/gateway.config.json` (`name`, `url` with
   `${SECRET}` refs, optional `prefix`/`headers`).
3. Add the secret to the MCP service's environment (production:
   `/etc/ouro/mcp.env`, root-only; locally `.env.mcp`).
4. Restart MCP and check the boot log for the aggregated, namespaced tool names.
5. Add the namespaced name (for example `tavily__tavily_search`) to the
   `tools:` frontmatter of the skill that should use it; without this the tool
   is connected but invisible to the model (curation is agent-side).
6. Deploy with `scripts/deploy.sh`; the supervisor revalidates every skill's
   `tools:` against MCP's list at boot.

Steady state: three small edits (config, secret, skill) and a deploy.

## Follow-ups

- Extract to a container when a stdio upstream is needed or a remote upstream is
  seen destabilising the pollers.
- OAuth upstreams need a persisted auth flow; out of scope.
- An upstream down at MCP boot is skipped until MCP restarts, and a skill that
  names its tools then fails the agent's boot validation
  (#42).
- Tavily is enabled only in the `gaia` skill, which serves a dropped benchmark;
  pick the skill(s) that should search the web deliberately.
