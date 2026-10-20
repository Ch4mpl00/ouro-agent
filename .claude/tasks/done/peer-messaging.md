# Peer messaging: other agents talk to ouro over MCP

**Status:** done (implemented in `crates/mcp/src/peer.rs`, `skills.default/peer.md`, `crates/mcp/specs/peer_messages.feature`; live on the tunnel endpoint)
**Priority:** P2
**Area:** mcp / peer, agent / supervisor
**Created:** 2026-10-19

## Context

Other agents (Claude Code on the laptop, ChatGPT through the tunnel) have no
way to ask ouro something and get an answer. They share memory, but not a
conversation. The shape already exists for Telegram: an incoming message is
a signal, the reply is a tool call. Peer messaging is the same with an MCP
table as the transport.

## Decisions (2026-10-19)

- **P1 — trust: a peer is the user.** Ouro runs a peer's request with the
  same tools and authority as a Telegram message from the user. Safe only
  because both endpoints are loopback-only (SSH / the tunnel daemon). Real
  per-token identity (`../mcp-auth-and-tool-scoping.md` A1/A2) is a precondition
  for any public exposure.
- **P2 — sender.** The trusted identity is the instance's actor
  (`MCP_MEMORY_ACTOR`: `chatgpt` on the tunnel). The peer's own `from` is a
  display label, never a basis for trust.
- **P3 — v1 scope.** Peer → ouro → reply only. Ouro never opens a thread.
- **Async, not a blocking ask.** `ask_ouro` returns the thread at once; the
  peer long-polls `read_replies` (≤ 50 s per call, like `wait_for_signal`). A
  session with sub-agents outlives a single call's deadline.

## Design

- `crates/mcp/src/peer.rs` — `peer_threads` / `peer_messages` in `mcp_state`
  (direction in/out, sender, from label, text, created_at).
  - toolset `peer` (the peer endpoint, not in the default surface):
    `ask_ouro(text, from?, threadId?)`, `read_replies(threadId, afterId?, timeoutMs?)`.
  - toolset `peer-reply` (default surface, ouro's side):
    `reply_to_peer(threadId, text)` (a write), `get_peer_thread(threadId)` (read-only).
- Signal `source=peer`, content
  `Message from peer agent "<from>" (via <actor>), thread <id>:\n\n<text>`.
- **Cross-instance wake-up.** `Signals` wakes waiters with an in-process
  `Notify`; the tunnel is another process, so a peer's signal would wait for
  the supervisor's 25 s re-check, and a reply for the peer's deadline.
  Postgres `LISTEN/NOTIFY` on both (`pg_notify` beside the insert, one
  listener connection per instance feeding the existing `Notify`s).
- Agent: `peer` joins `telegram`/`scheduler` as a transport source
  (`supervisor.rs`), with a preload of the thread (`get_peer_thread`) like the
  Telegram history preload. Skill `skills.default/peer.md`: exactly one
  `reply_to_peer` per signal, the peer is another agent (answer in its
  language, data over prose).
- Deploy: add `peer` to the tunnel's `MCP_TOOLSETS`. Claude Code reaches it
  with `ssh -N -L 3001:127.0.0.1:3001` and an http entry in `.mcp.json`.

## Acceptance

- `crates/mcp/specs/peer_messages.feature` and the `tool_surface.feature`
  update pass.
- `crates/mcp/tests/shipped_skills.rs` accepts `peer.md`.
- A real round trip from Claude Code: ask → ouro answers in the thread.

## Notes

- Ouro-initiated messages (notifying a peer) — later, needs a peer inbox
  that something actually reads.
