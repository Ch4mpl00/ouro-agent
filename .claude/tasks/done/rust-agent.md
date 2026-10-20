# The agent in Rust — crates/agent

**Status:** done — Rust agent on prod since 2026-10-15; the TS code, tooling included, deleted 2026-10-16 (see the last section)
**Priority:** P2
**Area:** packages/agent → crates/agent (runtime), crates/evals (tooling); packages/codex → crates/codex
**Created:** 2026-10-14

## Context

The MCP server runs as Rust since 2026-10-14 (`crates/mcp`, see
`rust-rewrite.md`). The agent supervisor is next, for the same reason: the
droplet has 2 GB. Scope, decided with the user: the **runtime** — everything
the `agent` container runs — moves; the offline tooling (`judging/`,
`scripts/`, `eval/`) stays in TypeScript and keeps reading the same
`agent.db` and Langfuse traces, whose shapes the Rust agent reproduces.

Like the MCP port: the TS agent stays in the image as the rollback, its
tests are ported one to one, and a fix in one goes into the other while both
exist.

## Libraries

| TS | Used for | Rust |
|---|---|---|
| `openai` SDK (chat completions) | four OpenAI-compatible providers: OpenAI, DeepSeek, Gemini, OpenRouter | `reqwest` + serde types of our own. The providers send fields the SDK types don't model (DeepSeek `thinking` + `reasoning_content`, OpenRouter `reasoning`, the decisions API); `async-openai`'s typed requests would fight that. |
| `@modelcontextprotocol/sdk` client | MCP over Streamable HTTP (and stdio for dev) | `rmcp` client (`transport-streamable-http-client-reqwest`, `transport-child-process`) — the crate the MCP server already uses |
| `effect` 4 | retries with backoff, timeouts, bounded concurrency, cancellation | `tokio` (`time::timeout`, `select!`), `futures` (`stream::iter().buffered(n)` keeps order), `tokio_util::sync::CancellationToken` for `AbortSignal` |
| `zod` | validating LLM-provided tool args, the workflow DSL, MCP replies | `serde` + hand-written checks that produce the same `path: message` errors the model is shown |
| `zod-to-json-schema` | inspection-only JSON schema of the DSL | dropped — nothing in the runtime reads it |
| `better-sqlite3` + `drizzle-orm` | `agent.db` (memory KV, trace mirror, journal progress) | `rusqlite` (bundled), SQL by hand; a drizzle-compatible migrator reads the same `migrations/*.sql` + `_journal.json` and the same `__drizzle_migrations` table, so TS tooling and either agent run on one file |
| `drizzle-kit` | generating migrations | stays in TS: `pnpm db:generate:agent` keeps generating the SQL both agents apply |
| `@langfuse/otel`, `@langfuse/tracing`, `@opentelemetry/*` | traces to Langfuse | explicit OTLP/HTTP JSON exporter (`reqwest` + `serde_json`, no OTel dependency) to `${LANGFUSE_BASE_URL}/api/public/otel/v1/traces`, Basic auth, the same `langfuse.*` span attributes the JS SDK sets (`langfuse.observation.type/input/output/metadata.<k>/level/status_message/model.name/model.parameters/usage_details`, `langfuse.trace.name`, `session.id`, `langfuse.trace.tags`) |
| `dotenv` | `.env` | `dotenvy` |
| `fast-check` | property tests | `proptest` |
| Node `fetch` | Codex service client | `reqwest` |
| `Intl.DateTimeFormat` | local time in the context block | `chrono-tz` |

## Compatibility kept on purpose

- `agent.db`: same schema, same migrations, same JSON column contents
  (`traces.observations` in the canonical Langfuse read shape) — the TS
  judge, improver and scripts read what the Rust agent writes.
- Langfuse: same observation names, types, metadata keys
  (`judge_node`, `activity`, `tool`, `skill`, `agent_id`, …) and ids (the
  local mirror keys on the OTel trace / span ids).
- Prompts and tool definitions are copied verbatim (see
  `feedback_verbatim_protocol_prompts`): working-memory instructions,
  synthetic tool descriptions, compiler prompt pieces, recovery and note
  instructions.
- Strings the model sees that are built with `JSON.stringify` keep its
  formatting (`serde_json` with `preserve_order`; pretty = two spaces).
  Lengths that bound text (clip, budgets) count UTF-16 units like JS.

## Plan

1. Crate skeleton, providers, retry, generation, tracing (null, local
   recorder, tee, Langfuse), agent.db + migrator.
2. Skills, session context, tool results, synthetic tools, MCP client,
   Codex client, decisions, engine, agent loop.
3. Workflow: DSL + checks, variables, compiler, executor, runner.
4. Supervisor: module, Telegram context, poll loop, activity journal, main.
5. Tests: every runtime test file of `packages/agent` (not `judging/`,
   `eval/`) as a Rust twin, each mutation-checked.
6. Image + compose (`command:` switch, TS kept for rollback), deploy.


## Acceptance

- [x] Rust workspace member and injected runtime library (`crates/agent`).
- [x] OpenAI / DeepSeek / Gemini / OpenRouter providers, retry, deadlines,
      cancellation, usage and tracing interfaces.
- [x] SQLite migration bytes/journal match TS; concurrent boot, memory KV,
      trace mirror and judge read/filter contracts tested.
- [x] Skills overlay, working memory, synthetic tools, Codex adapter,
      typed decisions + filter, engine and ReAct loop.
- [x] HTTP MCP reconnect / cancellation and no replay after an ambiguous
      failure; checked against SDK HTTP and the actual TS HTTP test process.
- [x] Workflow DSL, variables, compiler, executor, patch / replan, runner.
- [x] Supervisor, Telegram context, signal polling, activity journal, main.
- [x] Complete per-test TS parity matrix and mutation checks for all runtime
      tests (see "Test parity — 2026-10-15").
- [x] Image / compose wiring with TS rollback and local runtime process smoke.
- [x] Linux release image verification and production deployment
      (2026-10-15, see "Production switch").
- [x] Live Langfuse ingestion verified on the first production run.

## Continuation checkpoint — 2026-10-14 (Codex)

Resumed Claude session `fa4f10c4-586f-45bf-ac41-4423405ac925`, which stopped
at `mcp.rs` at 17:57 UTC on the session limit. Worktree remains
`.claude/worktrees/rust-agent`, branch `feat/rust-agent`, based on `4fc041f`.
This first checkpoint covered the runtime library. The continuation below adds
the binary and compose wiring; the production supervisor is still TS.

Completed Claude's disconnected module wiring and the unfinished MCP SDK
constructors, then ported the engine / loop / synthetic tools as one domain
in `src/agent.rs`, Codex and decisions. Synthetic tool definitions in
`src/synthetic_tools.json` are serialized from the TS registry (verbatim
prompts / JSON schemas); update that snapshot when TS definitions change.
All long-lived clients / stores are injected; loop registrations hold weak
engine references, and shutdown cancels loops, joins them, then closes clients.

Important differences/fixes discovered during the port:

- Session recovery belongs to our MCP client, not rmcp's automatic re-init:
  concurrent callers share one replacement. Only the typed `SessionExpired`
  transport failure proves a call never reached a tool. A matching string
  inside a 5xx body / tool failure cannot justify replay. Closing MCP awaits
  the service's transport finalizer and prevents later reconnects.
- `packages/mcp/src/http-transport.ts` now answers **404** for an unknown
  attached session id, matching Rust / Streamable HTTP; absent session id
  remains 400. The TS agent already recognizes `Session not found`. Added
  raw-wire tests, and checked the Rust client against the actual TS test
  process across eviction, a SIGKILL during a call and restart in both modes.
- A generation deadline cancels the operation's own token (including retry
  backoff / the actual provider) without cancelling its parent session.
- Environment gathering propagates SQLite read errors; an unreadable
  watermark is not silently treated as a first run. Invalid zones fail as
  `Intl.DateTimeFormat` does instead of rendering UTC under a wrong label.

### Current test mapping

| TS tests / contract | Rust |
|---|---|
| `providers/providers.test.ts`, `providers/retry.test.ts` | `providers.rs`, `retry.rs`, `provider_http.rs` |
| `session-context.test.ts`, `tool-results.test.ts` | `session_context.rs`, `tool_results.rs` |
| `tracing/tee.test.ts`, canonical trace mirror | `tracing.rs` |
| `test/agent-db.test.ts` | `agent_db.rs` (KV property sequences, trace/judge filters, migration bytes, concurrent boot) |
| `mcp-client.test.ts`, `test/mcp-reconnect.test.ts` | `mcp_client.rs`, `mcp_typescript.rs` (cross-language cases, randomized process histories still pending) |
| `agent-loop.test.ts`, `test/agent-loop.test.ts`, `test/write-skill.test.ts` | `agent_loop.rs`, `loop_properties.rs`, `skills.rs` (not yet every source test case) |
| `decisions.test.ts`, `test/decisions-filter.test.ts` | `decisions.rs` (builders, validation, usage, batching/order/threshold; remaining cases pending) |

Validation commands from the worktree:

```sh
cargo test -p agent --offline
cargo test -p agent --offline --test mcp_typescript -- --ignored
cargo clippy -p agent --all-targets --offline -- -D warnings
cargo fmt -p agent -- --check
pnpm exec tsc --noEmit
pnpm exec vitest run packages/mcp/src/http-transport.test.ts packages/mcp/test/http-transport.test.ts packages/agent/src/mcp-client.test.ts
```

The cross-language tests are ignored by default because they need Node / pnpm
dependencies. They launch only `packages/mcp/test/support/mcp-child.ts`, whose
sole tool writes a temporary ledger; they never start production pollers.
Local node_modules directories link dependencies from the main checkout inside
the ignored directories; no package lifecycle/build scripts were run. On a new
checkout, install pnpm dependencies before the TypeScript / cross-language checks.

Seven compiling mutations were caught: skill whitelist intersection changed
to union, workers allowed to delegate, child-created skill-write authorization,
unordered tool replies, deadline leaving provider cancellation live, every
result labeled text, and replay based on text inside an ambiguous HTTP failure.
Sources restored after each mutation. This is a targeted check, not a complete
mutation audit of the TS runtime test suite.

### Verified result at this checkpoint

- `cargo test -p agent --offline -- --include-ignored`: **52 passed**, including
  both cross-language process tests; no production integrations were called.
- `pnpm exec vitest run …` for the two HTTP transport suites and agent MCP
  client suite: **23 passed** (3 files).
- `cargo clippy -p agent --all-targets --offline -- -D warnings`,
  `cargo fmt -p agent -- --check`, `pnpm exec tsc --noEmit`, `git diff --check`:
  passed.
- The TS transport fix is isolated in local commit `b686e49`.


## Workflow and supervisor checkpoint — 2026-10-14 (Codex)

All production-agent runtime domains are now in Rust. `agent-supervisor` is
registered as a Cargo binary; compose uses it for `agent`, and the existing
Docker Rust stage includes it when building workspace binaries. The TS
runtime remains in the image for rollback:
`command: sh -c "pnpm setup:agent && pnpm agent:start"`.
The judge, improver, GAIA harness and CLI scripts remain TS by design.
Neither live databases nor deployed containers were changed.

- `workflow.rs`: strict eight-kind DSL, own-property variable resolution,
  forward/sibling/duplicate binding checks, compiler validation retries,
  planner/composer patches, execution, bounded parallel cancellation with
  finalizer draining, context-carrying replans and distinct failure stages.
- `supervisor.rs`: scheduler-to-workflow routing, domain delegation hints,
  scoped Telegram history, recent activity in system context only, serial
  queue consumption, recovery and coordinated shutdown. An execution failure
  goes to recovery without re-executing the signal's side effects.
- `activity.rs`: bounded trace projection, validated note generation,
  code-owned provenance, persistent eligibility cutoff, draft reuse across
  append retries/restarts, generation backoff and cancellation. The journal
  shares the supervisor's MCP client and creates no recursive activity traces.
- `main.rs`: cwd dotenv, injected clients/stores, startup retry, migrations,
  skill validation, OS signal cancellation, journal lifecycle and trace flush.
  `--check-config` opens no databases/services; `--once` handles at most one
  signal and disables the background journal.

### Additional test mapping

| TS contracts | Rust |
|---|---|
| workflow DSL / variables | `workflow_dsl.rs`, committed TS reference cases |
| workflow compile / execute / runner / patches | `workflow_runtime.rs` |
| supervisor module / Telegram context / loop | `supervisor.rs` |
| activity projection / notes / progress / worker | `activity_projection.rs`, `activity_worker.rs` |
| composition root / cleanup / OTLP flush | `runtime_process.rs` (real binary with isolated local MCP, LLM and exporter) |

Reference snapshots are generated by
`node --import tsx crates/agent/tests/support/make-parity-fixtures.ts` using
actual TS implementations. Projection tests compare complete JSON outputs,
including UTF-16 budgets and omission behavior. Runtime tests combine source
cases into scenario tests; this is **not** a claim that every TS test or
property generator has a one-to-one Rust twin. The full parity/mutation audit
remains an explicit acceptance item above.

Validation: 90 Rust tests including the normally ignored cross-language
process tests; clippy with warnings denied; formatting; TS typecheck; compose
configuration validation using empty fixture env files. Process tests cover
scheduler, Telegram and domain signals, execution-failure recovery, SIGTERM,
matching SQLite/OTLP ids and one journal entry without recursive traces.
No production integration credentials enter those child processes.

Additional targeted compiling mutations: tag invalid plans as judge nodes,
leave parallel siblings running, regenerate a saved journal draft. Each is
expected to fail its corresponding assertion, and sources are restored.

**Remaining rollout work:** verify the Linux release image and real Langfuse
ingestion, then deploy and observe real signals. Build/dev scripts and deployment
were not run, per the session instructions. Local tests do not establish live
provider behavior or resource usage on the droplet.


## Test parity — 2026-10-15

Every runtime test of `packages/agent` (not `judging/`, `eval/`, `scripts/`)
has a Rust twin: one `#[test]` per TS `it`, named after it (an `it.each`
table loops inside one test), and every fast-check property as a proptest
with the same generators, rules, run counts (`PROPERTY_RUNS`) and
witnessed-path checks. Where a TS test spies on `startAgentLoop` /
`endAgentLoop`, the Rust one reads the trace the loops leave and
`Engine::loop_ids`. The Codex scenario tests stay, marked Rust-only.

| TS | cases | Rust |
|---|---|---|
| `src/agent-loop.test.ts` | 32 | `agent_loop.rs` (+9 Rust-only on real skills / agent.db) |
| `test/agent-loop.test.ts` | 2 properties | `loop_properties.rs` |
| `test/write-skill.test.ts` | 4 + property | `write_skill.rs` |
| `src/providers/providers.test.ts` | 21 | `providers.rs` |
| `src/providers/retry.test.ts` | 5 (+3 per provider) | `retry.rs` (paused tokio clock for TestClock) |
| `src/session-context.test.ts` | 13 | `session_context.rs` |
| `src/tool-results.test.ts` | 4 | `tool_results.rs` |
| `src/tracing/tee.test.ts` | 1 | `tracing.rs` |
| `test/agent-db.test.ts` | 2 properties + 2 | `agent_db.rs` (three real processes via self-exec) |
| `src/mcp-client.test.ts` | 13 | `mcp_client.rs` (real Streamable HTTP for the SDK's in-memory pair) |
| `test/mcp-reconnect.test.ts` | property | `mcp_typescript.rs` (same TS child process; `--ignored`, needs node) |
| `src/decisions.test.ts` | 11 | `decisions.rs` |
| `test/decisions-filter.test.ts` | property | `decisions.rs` |
| `src/supervisor/telegram-context.test.ts` | 13 | `telegram_context.rs` |
| `test/supervisor.test.ts` | property | `supervisor_e2e.rs` (in-process rmcp server over a duplex pipe) |
| `test/activity-journal.test.ts` | 13 | `activity_journal.rs` |
| `src/workflow/dsl.test.ts` | 40 | `workflow_dsl.rs` (`workflowToJsonSchema` has no Rust twin: inspection-only, dropped) |
| `src/workflow/variables.test.ts` | 26 | `workflow_variables.rs` |
| `src/workflow/compile.test.ts` | 12 | `workflow_compile.rs` |
| `src/workflow/execute.test.ts` | 52 | `workflow_execute.rs` |
| `test/workflow.test.ts` | 3 properties + 1 | `workflow_properties.rs` |
| `test/workflow-replan.test.ts`, `-patch`, `-execute` | properties | `workflow_properties.rs` |

The ports found two behaviour differences, both fixed in Rust:

- Tool calls (`maxConcurrentTools`) and filter batches ran through
  `futures::buffered`, which keeps a finished call's slot until every
  earlier one finishes: with a slow first call the third never started.
  `Effect.forEach({ concurrency })` frees the slot at once. Now
  `buffer_unordered`, with the results put back in call order.
- A decide step whose items aren't an array says what they were
  (`got object`), like the TS message.

Run: `cargo test -p agent` (343 tests, ~40 s); `-- --include-ignored`
adds the two cross-language tests and the reconnect property.

### Mutation check

31 compiling source mutations, one at a time, each run against the test files
that cover it (script kept out of the repo; the list is the record):
off-by-one inline / preview limits, working-memory duplicate keys and byte
sizes, DeepSeek stamping without thinking, OpenRouter `max`, 408 not
transient, the retry budget, MCP 408 fatal and an uncapped connect backoff,
the filter threshold and batch size, `buffered` tool calls, workers allowed
to delegate, `write_skill` without the signal's authority, a whitelist
union, replies out of order, Telegram history after the signal, recent
activity newest first, the history byte cap, the note line limit, a done
journal entry keeping its draft, duplicate binds accepted, whole-string
placeholders turned to text, a failed parallel child not cancelling its
siblings, failed compile attempts tagged as planner nodes, the last-pass
flag, array `length` paths, judge listing order, tee ids not forced, the
patch put first.

30 were caught as first written. The survivor — Telegram history keeping
messages logged after the signal — was masked by the cut at the current
message; the TS suite had the same gap. A case for it went into both
(`leaves out messages logged after the signal…`), and the mutation now
fails both.

## Production switch — 2026-10-15

Deployed `1832b91` with `scripts/deploy.sh agent` (compose also recreated
`mcp` and `codex`, same code, new image). Before: `agent.db` copied with the
agent stopped to `/root/backups/pre-rust-agent-20261015-1740/`. The TS
agent's own image had already been pruned (only its running container held
the layers), so the rollback is the TS agent inside the current image —
`command: sh -c "pnpm setup:agent && pnpm agent:start"` — or the image
`mcp-tools-image:ts-backup-ad4ffbe`; the TS agent code is the same in both.

Observed:
- boot: MCP connected on the second attempt (mcp was restarting), skill
  validation passed (44 tools), activity journal resumed from the TS cutoff;
- memory: agent 114 MiB (TS) → 5.7 MiB idle, 34 MiB during a run;
- `agent.db`: the TS driver still reads it; the migration table is
  unchanged (3), the Rust migrator applied nothing;
- the first real signal, `scheduler:685` (the evening digest): compile,
  parallel fetch, 20 filter batches, compose, send, watermark — all DEFAULT,
  38 s, the same shape as the TS run of the same task the day before;
- journal: its note appended 5 s after the run (entry 23);
- Langfuse: the trace reads back with all 36 observations, types, model,
  usage and `judge_node` metadata.

Still to see on prod: a Telegram message (the agent-loop path, history
preload, recovery).

## TypeScript retired — 2026-10-16

The user asked to drop every TS file that has a Rust twin, then to port the
tooling too ("перепиши тулинг на раст и всё TS удаляй") and to drop the GAIA
benchmark outright ("gaia удаляй просто"). Branch `feat/rust-only`, stacked on
`feat/rust-agent` (PR #28).

What moved where:

| TS | Rust |
|---|---|
| `packages/agent/src/judging/*` (schema, backend, node-judge, materials, noise, gate, gate-runtime, improver, improve-cycle, improve-worker, monitor, langfuse-scores, trace-source, print, worker, sigma-baseline) | `crates/evals/src/*` — prompts byte-identical (checked against the deleted files) |
| `scripts/{judge,judge-worker,judge-noise,judge-gate,improve,improve-worker,judge-replay,langfuse-trace,news-filter-eval}.ts` | `crates/evals/src/bin/*` with the same names, flags and output |
| `db/improver-store.ts` | `crates/evals/src/improver_store.rs` (same `improver_state` rows) |
| `skills.ts` `readSkillRaw` / `savePatch` / `deletePatch` | `FsSkillStore` inherent methods in `crates/agent/src/skills.rs` |
| `judging/noise-baseline.json` | `crates/evals/noise-baseline.json` |
| `packages/codex` (runner + HTTP front) | `crates/codex` (`codex-service`) |
| `test/skills-contract.test.ts`, `test/shipped-skills.test.ts` | `crates/mcp/tests/skills_contract.rs`, `shipped_skills.rs` (mcp dev-depends on agent) |
| `crates/agent/tests/mcp_typescript.rs` (against the TS test server) | `crates/mcp/tests/agent_mcp_client.rs` — the same three tests against `serve_http`, the server a self-exec of the test binary; no longer `#[ignore]` |
| `packages/mcp/specs/*.feature` | `crates/mcp/specs/` (moved unchanged) |
| `eval/benchmarks/gaia/*` | deleted, not ported |

Tests: one Rust test per TS case — judging 57 (gate 12, improver 12,
materials 13, monitor 10, node-judge 2, noise 8), codex runner 5 (the
property with 25 cases, failure, the deadline killing the process group, HTTP
health/404, well-formed vs nine refused bodies), the skills contract property
(150 cases) and the shipped-skills boot check. The migration byte-for-byte
tests against the TS copies became hash pins (an applied migration never
changes); new migrations are written by hand now — there is no drizzle-kit.

Compose: `codex` runs `codex-service`, `judge-worker` / `improve-worker` run
their binaries (opening agent.db migrates, so no setup step). The image has no
node_modules any more; it stays a node image only for the npm-packaged Codex
CLI (and the tunnel healthcheck's `node -e fetch`).

Rollback to TS is no longer a compose edit: it is the image
`mcp-tools-image:ts-backup-ad4ffbe` on the droplet, or a checkout before this
branch.
