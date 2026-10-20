# MCP HTTP connection lifecycle

**Status:** done. The resilience fix (P1) shipped on both sides in the
TypeScript code (2026-08-23) and survives in the Rust port; multi-connect
exists for restricted instances only.
**Priority:** P1
**Area:** mcp / transport
**Created:** 2026-06-15

## Context

The full MCP instance serves one agent session on purpose: the signals queue has
no delivery coordination, so two connected agents would compete for the same
signals. The problem was resilience, not concurrency. When the agent restarted
ungracefully the old session's transport never closed, the server stayed bound
to the dead session, and every `initialize` from the reconnecting agent failed
(`Already connected` then HTTP 500, agent exit 1, Docker restart, again): a
self-sustaining crash loop only an MCP restart broke. Incidents: 2026-06-15 and
again 2026-08-23 (78 restarts).

## Resolution

- **Server: newest wins.** A new `initialize` evicts the bound session before
  connecting, so a reconnecting agent always gets in. Rust:
  `SessionPolicy::new(newest_wins)` in `crates/mcp/src/server.rs`.
- **Client: retry at startup and mid-flight.** The agent retries its initial
  connect with capped backoff (1 s to 30 s, unbounded, one log line per attempt)
  instead of exiting and letting the process manager be the retry loop;
  a 4xx other than 408/429 is a misconfiguration and stays fatal. A session the
  server lost (restart, or another client took it) is renewed: the client
  re-initializes on a 404 and replays the request that met it (it never ran);
  a call that may have reached a tool is never replayed (a Telegram message must
  not go out twice). Rust: `crates/agent/src/mcp.rs`.
- Either half alone leaves a hole: the eviction stops the 500, the client retry
  stops any startup blip (a slow-booting MCP) from becoming exit-and-restart.
- **Restricted instances are multi-session** (`MCP_TOOLSETS` set): one
  `McpServer` per session, and they have no signals tools, so the delivery race
  cannot occur; the constraint is sidestepped, not solved. `tunnel-client` holds
  a probe session of its own, which is why the tunnel instance must be
  multi-session.

## Not done

- Real multi-connect on the full instance needs the signals queue to coordinate
  delivery (atomic claim, visibility timeout, ack/nack, redelivery on agent
  death); do not start without that. Until then the server should reject a
  second concurrent connection rather than silently corrupt delivery (today:
  newest wins). The delivery gap is tracked in `../durable-workflows.md`.
- The live check (kill the agent ungracefully, confirm it reconnects without an
  MCP restart) was never run against a real killed process.
