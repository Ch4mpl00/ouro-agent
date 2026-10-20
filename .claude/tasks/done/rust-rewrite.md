# Rewrite in Rust — the MCP server first, then the agent

**Status:** done — Rust MCP on prod since 2026-10-14; `packages/mcp` deleted 2026-10-16 (rust-agent.md, last section)
**Priority:** P2
**Area:** whole repo; `crates/mcp`, `crates/agent`, `crates/codex`, `crates/evals`
**Created:** 2026-10-02

## Context

The whole system moves to Rust. The MCP server went first: it talks to the
agent only over the MCP protocol, so the Rust `mcp` container replaces the
TS one without the agent noticing. `packages/mcp` is deleted; `crates/mcp`
is the server.

**One domain, one file** carries over: `scheduler.rs` holds storage, cron,
the poller and the tools; `memory.rs` the types, patch engine, projection,
store, indexer, service and tools; and so on. Each domain adds its tools as
a `#[tool_router(router = <x>_tools)]` block on `McpTools`; `toolsets.rs`
maps a toolset name to that router.

## Compatibility kept on purpose

So the switch is reversible by redeploying the previous image:

- **State moved out of sqlite** into a Postgres database `mcp_state` beside
  the news store (decided 2026-10-02). `import-sqlite-state` copies the
  live tables once, keeping ids; the `mcp-data` volume with the old file is
  kept, unmounted, for rollback. Consequence: rolling back to the TS server
  after the switch means it resumes from the sqlite state as of the switch
  (Telegram may replay up to 24 h of updates, Gmail may re-signal recent
  mail).
- **Postgres** — `pg.rs` re-implements drizzle's migrator: same journal
  table, same hashes, same `when` ordering. Verified both ways on a
  throwaway pgvector DB: the Rust migrator applies nothing after the real
  drizzle migrator, and a Rust-migrated DB `pg_dump`s to the same schema.
  Migrations now take an advisory lock (fixes the `mcp` / `mcp-tunnel`
  concurrent-boot race the TS server also had).
- **Userbot** — the session stays in gramjs `StringSession` format and is
  imported into grammers; no re-login. Confirmed against the real local
  session (dialog listing works).
- **Gmail** — tokens in `integration_account` exactly as googleapis stored
  them; refresh 5 min early, retry once on 401.
- **Wire shapes** — tool names, input field names, JSON result shapes,
  signal content, ISO timestamps (`…000Z`) and the env-context text are
  unchanged; `toolsets.rs` tests pin both the default and the tunnel
  surface. A handler failure is an `isError` result, as with the TS SDK.
- **Sessions** — newest-wins for the full instance, multi-session for a
  restricted one; an unknown session id answers 404 "Session not found",
  which the agent's reconnect logic already matches.

## Deliberate differences

- `fetch_url`'s SSRF guard runs in the HTTP client's DNS resolver, so every
  redirect hop and a DNS answer that changed after validation are checked
  too (the TS guard only checked the first URL).
- `fetch_article` goes through the same guarded client.
- rmcp validates the `Host` header: compose sets `MCP_ALLOWED_HOSTS` (`*` on
  the tunnel, whose forwarded Host isn't predictable).
- `.env` is read from the working directory only (dotenv/config semantics).

## Acceptance

- [x] Every toolset, poller and CLI ported; TS package removed.
- [x] Unit tests + Postgres integration tests (`TEST_DATABASE_URL`).
- [x] End-to-end HTTP smoke of the full surface against a copy of a real
      `tokens.db` and a test Postgres.
- [x] Release image built locally for linux/amd64 and deployed from a clean
      archive of `577ecb5`; binary SHA-256 values matched on the droplet.
- [x] Deployed on the droplet; MCP protocol checks passed on both endpoints,
      agent and tunnel reconnected, first HN/Habr/channel harvests succeeded.
- [ ] One day of signals processed normally (Telegram replies, a NashDom bill,
      the 08:00/09:00 digests, dreaming); Gmail currently requires OAuth renewal.
- [ ] Agent port: `packages/agent` → `crates/agent`.

## Deploy notes

- **Local dev**: the sqlite file moves —
  `mkdir -p crates/mcp/data && mv packages/mcp/data/tokens.db* crates/mcp/data/`.
- **Droplet**: a release build of the crate is heavy (hundreds of crates,
  aws-lc). If the droplet is small, build the image elsewhere or add swap
  before `docker compose up -d --build`.
- **First deploy** (switch from TS): back up the sqlite file and Postgres,
  stop `mcp` + `mcp-tunnel`, run
  `docker compose run --rm -v agent-helper_mcp-data:/legacy:ro mcp import-sqlite-state --sqlite /legacy/tokens.db`,
  then start the Rust services. Later deploys are plain `pnpm deploy`.
- **Rollback**: retag the saved TS image as `mcp-tools-image:latest`, check
  out the previous commit on the droplet, `docker compose up -d --no-build`.
  The Postgres journal and the userbot session are shared; state written to
  `mcp_state` after the switch is not visible to the TS server.
- `glass_pumpkin` is pinned to `2.0.0-rc0` in Cargo.lock: `grammers-crypto`
  0.10 does not compile against rc1. Don't `cargo update` it away.


## Production switch — 2026-10-02

- Droplet checkout: detached `577ecb5`; Rust replaced `mcp` and `mcp-tunnel`.
  The existing TS agent and codex containers continue to run.
- State imported into `mcp_state`. Verified counts before enabling pollers:
  2 integration accounts, 769 Telegram messages, 1 Telegram cursor, 1 Gmail
  watermark, 2 settings, 13 scheduled tasks, 635 signals; 0 pending signals.
- Backups and measurements: `/root/backups/rust-mcp-577ecb5-caGhks6K/`.
  Includes a fully read-verified Postgres dump, final tokens/agent SQLite
  snapshots, previous compose and commit, import report and MCP checks.
  Old images retained as `mcp-tools-image:ts-backup-<service>-577ecb5`.
- SQLite snapshot procedure: stop every process sharing the state volume,
  copy the DB and WAL to a writable staging directory, use SQLite's backup
  API, then set the snapshot to `PRAGMA journal_mode=DELETE`. Mount that
  portable snapshot read-only for `import-sqlite-state`; a WAL-mode file
  without writable sidecar files can fail to open on a read-only mount.
- The auxiliary, no-poller TS `bench-mcp` also mounted the retired SQLite
  volume; it was stopped during cutover and remains stopped.
- MCP checks: 42 tools including 5 Tavily gateway tools on the full endpoint;
  18 tools and no gateway on the restricted endpoint. State/news/memory reads
  passed, supervisor skill validation passed, tunnel initialized. No restarts.
- Early Docker memory samples (MiB): primary MCP 296.3 → 171.2,
  tunnel MCP 137.4 → 6.1; combined 433.7 → 177.3 (about 59% less).
  Postgres rose from 134.3 to about 300; agent after restart about 206 versus
  95.9 before. The final sample settled to primary 119.2, tunnel 5.7, combined
  124.9 MiB (about 71% below the original combined sample); Postgres 204.7.
  These are early resident-memory samples, not a controlled load comparison.
  Host available RAM rose from 576 to about 780 MiB.
- Gmail returns `invalid_grant`. Migrated credentials byte-match the original
  snapshot, and the original TS Google SDK also fails to refresh that token
  with `invalid_grant`: renew Google authorization. No credentials were printed.
- `list_news(limit=1)` took 25–28 seconds; resolved by migration 0004; see
  `done/news-list-index-order.md` (MCP response now 28 ms).
  Habr extraction also emits many html5ever namespace warnings.


## Back to TS — 2026-10-12, and Rust again — 2026-10-14

On 2026-10-12 prod returned to the TS server (`main` 29ac01d): the
Rust-era fixes were backported, state moved to `mcp_state` in Postgres on
both sides, and the specs + a fast-check reference model became the shared
contract. Two days later the memory pressure on the 2 GB droplet (search
reading 1.2 GB of TOASTed vectors, dockerd at 540 MB) made the Rust server
worth having again.

`feat/rust-mcp` (from `main` ad4ffbe) brings `crates/mcp` over from
`feat/rust-rewrite` and catches it up with everything `packages/mcp` gained
since:

- **Ported features**: the memory journal (streams, capacity, metadata,
  `journal_append` / `journal_tail`); HNSW `search_news` with `k` up to 150
  (migration 0007); the index following the last write (`FOR UPDATE`) and
  an isolating embed backfill.
- **Bugs that existed only in Rust**, found by porting the property tests:
  `fetch_article` and the HN / Habr fetches had no SSRF guard; `read_file` /
  `read_pdf` were not confined to readable roots; the Gmail poller read one
  page and moved the watermark only at the end; the scheduler could fire a
  slot twice (no compare-and-set) and placed DST wall times differently from
  cron-parser; userbot harvested supergroups and lost posts after a burst;
  Telegram failed long texts instead of splitting them; Monobank never said
  a statement was cut at 500; a racing `mcp_state` create failed boot; a
  NUL in a post, mail subject or message stalled a poller (also fixed in TS
  on `test/coverage-loop`).
- **Tests, one to one**: every TS property test has a Rust twin under
  `crates/mcp/tests/` (proptest, same generators and witnessed-case
  checks), each mutation-checked; fakes for the Bot API, Gmail, Monobank,
  the userbot and fetch. The Lean model is replaced by `tests/model/`, the
  Rust copy of `packages/mcp/model` (properties, differential and
  linearizability checks against the server). The specs runner reads
  `packages/mcp/specs` directly — one text for both servers.
- **Deploy**: the Dockerfile cross-compiles the crate on the build platform
  (`mcp-build` stage, cargo caches in BuildKit mounts); compose runs
  `mcp-tools` for `mcp` and `mcp-tunnel`. `scripts/deploy.sh` builds on the
  laptop and ships the image.

### Switching and rolling back

No state import this time: both servers already use `mcp_state` and the
same Postgres migrations, so either one boots on what the other left.

- **Switch**: `scripts/deploy.sh mcp mcp-tunnel` (then `agent` if its code
  changed). Check `docker compose ps`, both endpoints' `tools/list`, and the
  tunnel-client log for `mcp session initialized` (restart it if it came up
  first).
- **Rollback without a rebuild**: the TS server is in the same image. On the
  droplet, set `command: pnpm mcp:serve` for `mcp` and `mcp-tunnel` (or
  check out the previous commit) and `docker compose up -d mcp mcp-tunnel`.
  The previous TS-only image is also kept, tagged, for a full rollback.

### Production switch — 2026-10-14

- `main` 4a8ceab deployed with `scripts/deploy.sh mcp mcp-tunnel`; the agent
  and codex kept running and reconnected on their own. `tunnel-client` was
  restarted so it holds a session on the new server.
- Before: TS image tagged `mcp-tools-image:ts-backup-ad4ffbe`; `mcp_state`
  and the main database (without `news_items` data) dumped to
  `/root/backups/pre-rust-mcp-20261014-1658/` with the previous compose file.
- Checks through `mcp-tunnel`: 20 tools; `search_news` 0.8–1.1 s (HNSW),
  `list_news` 27 ms, `recall` 1.8 s, `journal_tail` and `list_skills` fine.
- Memory (docker stats, MiB): `mcp` 242 → 34, `mcp-tunnel` 220 → 5; host
  available RAM 673 → 1174.
- Gmail still answers `invalid_grant`: the refresh token dates from
  2026-05-20 and the watermark stopped on 2026-05-13 — not the switch. Renew
  with `docker compose run --rm mcp gmail-auth`.


### Agent continuation — 2026-10-14

`feat/rust-agent` in `.claude/worktrees/rust-agent` now contains a tested Rust
runtime (providers, tracing, SQLite, MCP client, engine / ReAct loop,
synthetic tools, Codex, decisions, workflow, supervisor and activity journal).
The `agent-supervisor` binary and compose switch are prepared and tested locally;
Linux image verification and deployment remain. Production still runs the TS agent. Detailed progress, library
mapping and the exact continuation point: [rust-agent.md](rust-agent.md).
