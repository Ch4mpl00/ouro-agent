# mcp-tools

A personal-agent system structured as independent processes in one Cargo
workspace:

- **`crates/mcp`** (`mcp-tools`) — stateless MCP server. Wraps Gmail /
  Telegram / Monobank as primitive tools and runs the pollers that turn
  external events into signals on a queue. Knows nothing about the agent.
- **`crates/agent`** (`agent-supervisor`) — agent supervisor. Pulls one
  signal at a time from MCP, loads the matching skill, runs one session, and
  stops. Knows nothing about MCP internals — only the actions exposed by the
  MCP protocol.
- **`crates/codex`** (`codex-service`) — the sandbox the agent's `code_agent`
  tool and the codex judge run `codex exec` in, behind a small HTTP front.
- **`crates/evals`** — the evaluation tooling around the agent: the per-node
  judge (CLI + `judge-worker`), judge-noise calibration, the improver gate and
  the closed-loop improver (`improve-worker`), trace replay, the news-filter
  golden-set eval. Reads the agent's trace mirror and Langfuse; never in the
  signal path.
- **`crates/owf`** — an interpreter for the Open Workflow Specification DSL
  (the subset saved workflows use): parse + the official JSON Schema,
  validation, and the run behind `Functions`/`Observer`/`Clock` seams. No
  knowledge of MCP or the agent; `crates/agent/src/flows.rs` adapts it.

The server and the agent communicate strictly through the MCP protocol;
neither depends on the other's crate (only tests do: `crates/mcp/tests` drive
the agent's client and skill loader against the real server). Deployed as
three native systemd services; Postgres, Codex and tunnel-client stay in
Docker (`docker-compose.yml`). See `docs/operations.md`, Deploy, for the current layout.

## The main principle: the agent's freedom and generality

**Strive for the greatest freedom and generality of the agent.** Weigh every
design decision against it first.

- **The runtime informs; the agent decides and acts.** The supervisor may
  gather context before the first turn: history, the activity journal, the
  skills Jev preselects. It never takes an action on the agent's behalf. A
  typing indicator, a reply or a write is a tool call the agent chooses to
  make.
- **Behaviour lives in skills, not in Rust.** How to talk in Telegram, when to
  delegate, what a digest looks like: all of it is skill text, which the agent
  can find, read and rewrite (`write_skill`). The code may gather context
  differently per channel (Telegram history, a peer thread), but it never
  hard-codes how the agent behaves in a channel, a source or a task.
- **General mechanisms over special cases.** Prefer one general mechanism
  (skill search, memory, workflows, sub-agents) to a branch for one situation,
  even when the branch would save a model turn. An optimisation that takes a
  decision away from the agent is the wrong one: make the agent better
  informed instead.

Example: Telegram's `start_typing` costs a model turn, yet the supervisor does
not call it automatically. Whether and when to show typing is the agent's
choice.

**Everything was TypeScript until 2026-10.** The MCP server went to Rust on
2026-10-14, the agent on 2026-10-15, and the TS packages (`packages/mcp`,
`packages/agent`, `packages/codex`, the tooling) were deleted afterwards.
Their tests were ported one to one before that; a test or comment that says
"a port of …test.ts" names where it came from. The databases, migrations,
trace shapes and Langfuse attributes are still the ones the TS code wrote, so
nothing on the volumes was converted. History: `.claude/tasks/rust-rewrite.md`,
`.claude/tasks/rust-agent.md`. Rolling back to TS means an old image
(`mcp-tools-image:ts-backup-ad4ffbe` on the droplet) or a commit before the
deletion.

## How a signal turns into action

1. A poller inside `crates/mcp` fires on its own cadence — Gmail (1 min),
   Telegram bot getUpdates (long-poll), userbot channels (30 min).
2. When it sees something new, it records a signal (`Signals::record`),
   which inserts a row into the `signals` queue in the `mcp_state` Postgres
   database. Scheduled tasks are the agent's own (`crates/agent/src/schedule.rs`):
   cron rows in agent.db, a 30 s tick that claims each slot once and hands
   the fire straight to the supervisor as a `scheduler` (or the task's own
   source) signal, with the `envContext` a queued signal carries (the agent
   builds it from the same `TELEGRAM_DEFAULT_CHAT_ID` / `TELEGRAM_TOPICS_JSON`
   in its own env). Fire ids start above 10⁹, so `scheduler:<id>` never lands
   on an older MCP signal's trace. The first start imports the tasks MCP held
   (`list_scheduled_tasks`, kept read-only for that) or seeds the system one
   (the nightly `dreaming` tick). Digests are user-specific: they live in
   agent.db once the user asks for them, never in the seed.
3. The supervisor (`crates/agent/src/supervisor.rs`, composed in `main.rs`)
   loops on `get_next_signal`. With an empty queue it blocks on
   `wait_for_signal`, which MCP answers the moment a poller records a signal
   (25 s rounds), so a signal starts at once instead of on the next poll. The
   wait never takes a signal: a wait abandoned by a dead supervisor can't
   lose one. `wait_for_signal` is supervisor-only — never offered to a model
   (`agent::mcp::SUPERVISOR_ONLY_TOOLS`). Each signal runs the OWF workflow
   `signal` (`workflows/signal.yaml`, its functions in `supervisor.rs` §2b; the agent
   may activate its own version, with the same breaker as `agent-loop`), which
   holds the routing and the first turn's context below. Routing is by source: a `scheduler`
   signal whose payload names a saved workflow (`{"workflow": …}`, set by
   `schedule_task(workflow: …)`) runs its active version (`flows.rs` over `crates/owf`, no
   model deciding the steps); other `scheduler` signals run through the
   workflow module (`workflow.rs`: compile → execute); every other source
   runs the primary agent loop. Every agent loop (primary, worker, recovery)
   is a run of the OWF workflow `agent-loop` (`workflows/agent-loop.yaml`, its host
   functions in `agent.rs` §4): the primary one durable, so a restart resumes
   it from its last round. The agent may activate its own version of
   `agent-loop`; three faults of it in a row fall back to the built-in one
   (`flows.rs` §4b). A worker of the durable primary loop is a durable run of
   its own, which a resumed parent joins. A compile failure, or a saved
   workflow that faulted before any write call, degrades to the agent loop in
   the same trace; an execute failure, or a saved workflow that faulted after
   a write, goes to `recovery` instead of being retried. Writes are the MCP
   tools without `readOnlyHint` (plus `set_memory`). The agent-loop path loads:
   - `skills/orchestrator.md` + `skills/routing.md` — always; how to use
     working memory and when to delegate to a domain skill.
   - `skills/<signal.source>.md` (with `skills.default/` fallback) — only for
     transport sources (`telegram`, `peer`, `scheduler`); domain work is delegated to
     a sub-agent with that source's skill.

   The built-in `signal` workflow's `try`/`catch` requests a recovery report
   through `recovery_loop`, then raises the signal failure again. Its prompt,
   skills, preset, iteration limit and system context live in YAML;
   `signal_transcript` reads the primary loop's bounded transcript tail.
   Rust guards cancellation, aborts and missing sessions, traces the report
   under `recovery`, and cleans up its loop. Reporting errors cannot replace
   the signal error. A `signal` override without a catch chooses no report.
4. Plus a session-context block (local time, tz, watermarks) and the signal's
   `envContext` (per-source env addendum, e.g. default Telegram chat id).
   For `telegram` signals the supervisor also preloads recent chat/topic
   history (`supervisor.rs`), for `peer` signals the thread's earlier turns,
   and for both the latest activity-journal notes before the
   first LLM turn.
5. The signal's `content` is pushed as the first user message. The loop runs;
   every side effect (Telegram reply, DB write) is a tool call.

To add a new domain: drop a `skills.default/<name>.md` + emit signals with
`source=<name>` from a new poller. No supervisor change.

## Layout

```
mcp-tools/
├── Cargo.toml              the workspace
├── .mcp.json               registers prod's restricted MCP with Claude Code (http, via ssh -L)
├── .env.mcp                MCP container env (integration creds)
├── .env.agent              agent container env (model keys, presets)
├── .env.example, .env.mcp.example, .env.agent.example
├── docker-compose.yml      postgres, codex, tunnel-client (native app: systemd)
├── Dockerfile              cached cross-builder + explicit Codex runtime image
├── scripts/deploy.sh       changed binaries via rsync; tests/docs skip deploy
├── deploy/systemd/         native services and container startup ordering
├── skills.default/         shipped skills (git-tracked, read-only fallback)
├── skills/                 live overlay, gitignored; dreaming writes here
├── storage/                downloaded Gmail attachments (gitignored)
├── eval-fixtures/          golden sets for news-filter-eval (gitignored)
└── crates/
    ├── mcp/                one domain per file
    │   ├── src/main.rs                     composition root: deps, pollers, HTTP/stdio transport
    │   ├── src/server.rs                   McpTools, the HTTP transport, session policy
    │   ├── src/db.rs                       the `mcp_state` database (create, migrate, seed)
    │   ├── src/pg.rs                       the news/memory Postgres pool + drizzle-compatible migrator
    │   ├── src/toolsets.rs                 named tool groups (MCP_TOOLSETS scoping)
    │   ├── src/{gmail,telegram,userbot,monobank,scheduler,news,memory,pdf,fs,signals,
    │   │        settings,skills,knowledge,gateway,embeddings,fetch,eval,peer}.rs
    │   ├── src/bin/                        CLIs: gmail-auth, userbot-auth, mcp-setup, embed-backfill, …
    │   ├── migrations/pg/                  the Postgres migrations (applied files never change)
    │   ├── gateway.config.json             third-party MCP upstreams (git-tracked, secret-free)
    │   ├── specs/*.feature                 the executable specification (the user's)
    │   └── tests/                          black-box tests; tests/model/ the reference model
    ├── agent/
    │   ├── src/main.rs                     composition root (agent-supervisor)
    │   ├── src/supervisor.rs               poll loop, per-signal routing, tg history preload
    │   ├── src/workflow.rs                 dynamic workflows: DSL, variables, compile, execute
    │   ├── src/flows.rs                    saved workflows: store, function catalog, tracing,
    │   │                                   scheduled runs, the authoring tools
    │   ├── src/durable.rs                  saved workflows' run state: admission, leases, checkpoints
    │   ├── src/schedule.rs                 scheduled tasks: cron rows, the 30 s tick, the schedule
    │   │                                   tools, the first start's import
    │   ├── src/agent.rs                    the engine, the loop's turn/tool/settle, synthetic tools
    │   ├── workflows/                      the built-in OWF workflows in git: agent-loop.yaml
    │   │                                   (every loop), signal.yaml (every signal)
    │   ├── src/activity.rs                 traces → ouro/activity journal notes (background
    │   │                                   task inside the supervisor)
    │   ├── src/providers.rs                ChatProviders: openai, gemini, deepseek, openrouter
    │   ├── src/decisions.rs                typed classification (Jev via OpenRouter's decisions
    │   │                                   API) + presets (filter); the workflow `decide` step
    │   ├── src/mcp.rs                      StreamableHTTP / stdio MCP client with reconnect
    │   ├── src/session.rs                  session context + working memory
    │   ├── src/skills.rs                   two-layer loader (live → default) + patch overlay
    │   ├── src/{tracing,langfuse,local_recorder,trace_model}.rs  Tracer, Langfuse, mirror
    │   ├── src/db.rs                       agent.db: memory KV, trace mirror, judgements
    │   └── migrations/                     agent.db migrations (drizzle journal, unchanged)
    ├── codex/                              codex-service (POST /run, GET /health)
    ├── owf/                                Open Workflow Specification interpreter (+ schema/)
    └── evals/
        ├── src/                            judge, noise, gate, improver, scores, Langfuse API
        ├── src/bin/                        judge, judge-worker, judge-noise, judge-gate, improve,
        │                                   improve-worker, judge-replay, langfuse-trace,
        │                                   news-filter-eval
        └── noise-baseline.json             committed judge σ per (model | prompt version)
```

`agent.db` lives at `packages/agent/data/agent.db` (`agent::db::DEFAULT_DB_PATH`)
— the TS-era path the `agent-data` volume has always been mounted at; only that
data directory remains under `packages/`, gitignored.

## Stack

- Rust 2024, tokio; `anyhow` errors, `serde_json` values on the wire
- `rmcp` (MCP server + client, stdio and Streamable HTTP), `axum`
- `tokio-postgres` + `deadpool-postgres` + pgvector text literals (Postgres)
- `rusqlite` (agent.db); `sqlite3` CLI from Bash for ad-hoc queries
- `reqwest` for every HTTP API (Gmail REST, Telegram Bot API, OpenAI-compatible
  model APIs, Langfuse); `grammers` for the MTProto userbot
- `croner` for scheduled tasks; `proptest` for properties; `cucumber` for the specs

## Three databases — split by ownership

- **`mcp_state`** (Postgres database next to the news store;
  `crates/mcp/src/db.rs`) — MCP's private state: OAuth tokens and the userbot
  session, Gmail watermarks, the Telegram cursor and chat log, the `signals`
  queue, peer threads (`peer_threads` / `peer_messages`), settings (the user's time zone), and the `scheduled_tasks` MCP
  fired before the agent owned the schedule, read once by the agent's first
  start. The server derives it from
  `DATABASE_URL` (same server, database name `mcp_state`;
  `STATE_DATABASE_URL` overrides) and creates + migrates it on boot (versioned
  migrations inline in `db.rs`). It replaced the sqlite `tokens.db`
  (`import-sqlite-state` copies one over). Don't read or write it from agent
  code — go through MCP tools.
- **`packages/agent/data/agent.db`** (sqlite) — agent's domain state:
  `memory` (freeform KV, e.g. `news_digest.last_read_at`), the local trace
  mirror (`traces` + per-node `judgements`), `improver_state`, the
  activity journal's progress, the schedule (`scheduled_tasks`,
  `scheduled_fires`), saved workflows (`workflows`) and their durable runs
  (`workflow_runs`, `workflow_records`). Migrations live in `crates/agent/migrations`
  (drizzle's sqlite format and journal, applied on boot by `AgentDb::open`
  under the same race-tolerant boot the TS client had); a new one is a new
  `.sql` file plus a journal entry, and an applied one never changes (a test
  pins the hashes).
- **Postgres + pgvector** (containerized, `postgres` service in
  docker-compose) — the news / RAG store **and unified memory**. One table
  `news_items` unifies HN/Habr articles and harvested Telegram channel posts;
  rows have a 1536-dim `embedding` column (text-embedding-3-small). Unified
  memory adds `memory_projects` / `memory_project_docs` / `memory_doc_patches`
  / `memory_facts` (the read models) plus `memory_index` (the search
  projection — the only memory table with vectors). Owned by MCP; the agent
  reaches it only through MCP tools (`search_news`, `recall`, `read_doc`, …).
  Migrations are `crates/mcp/migrations/pg/*.sql` listed in `pg.rs`
  (`MIGRATIONS`, with drizzle's `when` stamps), applied on server boot under
  an advisory lock (`mcp` and `mcp-tunnel` boot at once). A new migration is
  a new file and entry; an applied file never changes (a test pins the
  hashes drizzle recorded).

Everything applies automatically on boot; `mcp-setup` does the `mcp_state`
part without starting the server.

For ad-hoc queries during development:

```bash
sqlite3 -json packages/agent/data/agent.db "SELECT * FROM memory"
sqlite3      packages/agent/data/agent.db "UPDATE memory SET value=? WHERE key=?"
docker compose exec postgres psql -U mcp -d mcp_state -c "SELECT * FROM signals ORDER BY id DESC LIMIT 5"
```

For multi-line / quote-heavy SQL, use a heredoc. Always single-quote
string literals; double single quotes inside (`'O''Brien'`).

## Code structure: modules + DI

Domain code is organised as **modules** with explicit **dependency
injection**. Every long-lived piece of state (DB pool, HTTP clients, the
embedder, stores) is built once in a composition root (`main.rs` of a crate,
or a binary's `main`) and passed down. No global singletons, no service
locators, no `static` handles in business code.

### Rules

1. **One domain, one file.** A domain is `src/<domain>.rs`: its types, its
   store, its service and its `#[tool_router]` together, split into numbered
   sections in a header comment. A file grows sections, not siblings.

2. **Seams are traits.** Anything a test must replace (a store, an embedder,
   the web, Telegram, a model provider, the MCP client, the clock) is a trait,
   and the domain takes `Arc<dyn Trait>` (or a generic) — never the concrete
   type. Tests supply fakes; `InMemoryStore` is the memory contract's.

3. **Generic infrastructure is dependency-free.** A reusable layer
   (`embeddings.rs`, `fetch.rs`, `providers.rs`) declares its traits and never
   names a concrete table or domain type. Domains supply the implementations.

4. **Repos live with the table they own.** A store for `news_items` lives in
   `news.rs`, not in `embeddings.rs`.

5. **DB handles are injected.** `pg::connect` builds a pool, `Db` wraps
   `mcp_state`, `AgentDb::open` opens agent.db — and anything that talks to a
   database takes one of those as a parameter or sits behind a store that
   does.

6. **Tools and pollers take their deps explicitly.** Tool handlers read
   `McpTools`'s `Deps`; pollers are spawned with what they use. Nothing
   reaches for a global inside a handler.

7. **The composition root is the only place that knows the full graph.**
   `main.rs` builds the deps in order, hands them to the server and the
   pollers, and owns shutdown. Binaries in `src/bin` do the same for their
   narrower scope.

8. **Add a new domain → add a new file.** `src/<domain>.rs` with its own
   types and router, wired in `main.rs`. Do not grow `news.rs` with unrelated
   concerns just because Postgres is already there.

9. **Async orchestration is tokio, bounded and cancellable.** Concurrency has
   an explicit limit (`buffer_unordered(n)` with results put back in order,
   not `buffered`, which holds a finished slot); child tasks stay within
   their parent's lifetime; a `CancellationToken` reaches every HTTP/SDK call
   (`errors::or_cancel`); finalizers run before a trace or client closes.

   Reuse `agent::generation` for LLM generation/tracing and the retry layer
   in `providers.rs` (`with_retry`, `retry_on_transient`) for transient
   failures. Keep automatic retries in one layer, and never automatically
   replay a tool side effect (the MCP client reconnects only on a session the
   transport proves was rejected before the tool ran).

### Anti-patterns (don't)

- `static` / `OnceLock` handles to a pool or client that business code reaches
  for. They turn every consumer into a hidden coupling on global state and
  make tests painful. (A `OnceLock` for a compiled regex is fine.)
- A struct with only `new` and one method — that's a function.
- A generic module that imports a concrete table or schema. Move the trait
  up, the impl down.
- Handlers that build their own clients or read env vars mid-request. The
  handler's inputs must be its contract.
- `unsafe` (transmutes above all) to get past the borrow checker.

## Task tracking

Planned work and tech debt live under `.claude/tasks/` — one markdown
file per task with frontmatter-style fields (`Status`, `Priority`,
`Area`, `Created`). Format is documented in `.claude/tasks/README.md`.

When picking work, scan that directory first — there's usually a
written-up task with context, rather than starting fresh from a half-
remembered Slack thread. When agreeing on new tech debt during a
design discussion, add a file there before moving on, so the decision
doesn't evaporate.

## MCP tools (signal-emitting + agent-callable)

Defined next to their domain in `crates/mcp/src/<domain>.rs` (each file
ends with its `#[tool_router]`). The agent calls these via MCP; you
also see them when running `claude` locally with `.mcp.json` registered.

- **Gmail** — `list_nashdom_mails`, `download_gmail_attachment`
- **Telegram bot** — `send_telegram_message`, `edit_telegram_message`,
  `send_telegram_chat_action`, `get_telegram_chat_history`
- **Telegram userbot (read-only MTProto)** — `list_userbot_dialogs`,
  `list_channel_posts`
- **Monobank** — `list_monobank_transactions` (no poller; reactive only)
- **News** — `list_news`, `fetch_article` (HN, Habr — both
  upsert into news_items and embed inline), `search_news` (semantic
  search across the unified store: HN, Habr, channel posts)
- **PDF** — `read_pdf`
- **Files** — `read_file`
- **Skills** — `list_skills`, `read_skill` (exact active `.md` filenames +
  contents, plus the improver's `.patch.md` overlay and the composed
  `effectiveInstructions` the agent actually runs)
- **Unified memory** — shared by every agent, see the section below.
  `recall`, `remember`, `get_fact`, `update_fact`, `list_memory`,
  `create_project`, `read_doc`, `append_doc`, `patch_doc`, `write_doc`,
  `doc_history`, `revert_patch`, `journal_append`, `journal_tail`
- **Signals queue** — `get_next_signal`, `wait_for_signal` (supervisor
  only: blocks until a signal is pending, takes nothing), `list_signals`
- **Peer messaging** (`peer.rs`) — other agents talk to ouro. `ask_ouro`
  and `read_replies` (toolset `peer`, the tunnel only) open or continue a
  thread and long-poll ouro's answer; the message is a `peer` signal, and
  ouro answers with `reply_to_peer` / reads `get_peer_thread` (`peer-reply`,
  default surface). A peer is trusted as the user; instances wake each
  other's waiters through Postgres `LISTEN/NOTIFY` (`db.rs`, wake-ups).
  Decisions: `.claude/tasks/peer-messaging.md`.
- **Scheduler** — `list_scheduled_tasks` (supervisor only: the tasks MCP
  held, read by the agent's first start). `schedule_task`,
  `list_scheduled_tasks` and `cancel_scheduled_task` as a model sees them
  are the agent's synthetic tools (`schedule.rs`).
- **Env** — `get_timezone`, `set_timezone`
- **Third-party (via gateway)** — when `gateway.config.json` lists upstreams,
  `gateway.rs` makes own-MCP an MCP *client* to them too and re-exposes
  their tools namespaced as `${prefix}__${tool}` (e.g. `tavily__tavily_search`).
  The agent still sees one merged endpoint. Onboarding (config + secret + skill
  frontmatter, no code) is in `.claude/tasks/mcp-gateway.md`.

### Tool scoping (`MCP_TOOLSETS`)

One MCP process serves one audience. `crates/mcp/src/toolsets.rs` holds the
named toolset → router map (`gmail`, `telegram`, `telegram-send`,
`monobank`, `pdf`, `fs`, `signals`, `news-read`, `knowledge`, `dreaming`,
`userbot`, `scheduler`, `skills`, `memory`, `peer`, `peer-reply`);
`MCP_TOOLSETS=news-read,telegram-send,skills` makes an instance register only
those groups. Unset/empty = every group, i.e. the behaviour before scoping
existed. An unknown name is a boot error.

A **restricted instance is also multi-session** (`serve_http`'s
`multi_session`), which is what lets several agents hold a memory connection at
once: an instance scoped to `memory` has no signals tools, so nothing races
for signal delivery.

Scoping works by **not registering**, so an out-of-scope tool never appears in
`tools/list` — invisible, not merely rejected. A restricted instance also skips
the gateway entirely, because upstream tools are namespaced at runtime
(`tavily__*`) and can't be expressed in the allow-list. Rationale and the wider
auth design: `.claude/tasks/mcp-auth-and-tool-scoping.md`.

## Unified memory

One memory every agent shares — the droplet supervisor, Claude Code sessions,
anything on the far side of the ChatGPT tunnel. Lives in
`crates/mcp/src/memory.rs`; design and rationale in
`.claude/tasks/unified-memory.md`.

Three read models, one search projection:

- **Projects** are folders of markdown documents (`passport.md`, `roadmap.md`,
  `progress.md`, …), not a fixed schema. The agent reads and patches each
  document individually, which is the interface LLMs are best at.
- **Facts** are flat freeform statements — what `knowledge_base_notes` held,
  plus a lifecycle state and an update path.
- **The journal** is the third read model: operational records a workflow
  appends and reads back by recency (`journal_tail`), e.g. the `news-digest`
  stream the digest dedups against. Streams are named categories
  (`digest/news`) with a stored capacity (default 100, oldest dropped
  first). Entries are immutable, never indexed and keep no patch history —
  records, not knowledge. Invariants: the journal tests in `memory.rs`
  (proptest against a reference model; the Postgres race tests run on the
  test container or `TEST_DATABASE_URL`).
- **`memory_index`** is the only table with embeddings. Documents and facts are
  chunked into it with their subject denormalised into the indexed text, so a
  fragment matches even though the stored document stays clean.

The agent's flow is two-step, the same search→read shape that fixed the
planner's snippet failures on the GAIA benchmark: `recall` returns **refs**
(`doc:leetcode-graphs/roadmap.md#2`, `fact:88`), then `read_doc` / `get_fact`
loads the whole thing.

Rules worth knowing before touching the code:

- **Writes are search/replace on quoted literals, never line numbers.** An
  off-by-one line number corrupts silently; a quote that doesn't match fails
  loudly and changes nothing. Each `old` must be unique, all edits apply or
  none do, and a miss comes back with the literal from the document that
  nearly matched (the model normalised an em-dash, a «quote», ё→е).
- **`append_doc` is the safe default** — it cannot destroy text and needs no
  version. `patch_doc` needs `expected_version`. `write_doc` is the only op
  that can lose content.
- **Every write is versioned with a compare-and-swap**, which is the whole
  concurrency story for several agents on one document.
- **Every write records a patch** with the previous body, an actor and the
  rationale (the utterance that caused it). `revert_patch` undoes one; an
  older patch whose text later writes touched fails cleanly and names the
  version a `rollback: true` would restore. History is never rewritten.
- **Indexing failure never fails a write.** Documents read and patch with the
  embedder down; `embed-backfill` drains the NULL-vector backlog.

The rules live in `MemoryService` and the store underneath (`MemoryStore`) is
dumb CRUD, so the whole contract is tested against `InMemoryStore` without a
Postgres.

**Who writes:** `MCP_MEMORY_ACTOR` names the instance (`supervisor` for `mcp`,
`chatgpt` for `mcp-tunnel`) and is stamped onto every patch. Audit metadata,
never access control — one shared space (D6). It is a property of the
instance, not something a client declares, because a self-declared actor is
forgeable; real per-token identity is `.claude/tasks/mcp-auth-and-tool-scoping.md` A2.

## Agent skills

Live under `skills.default/` (git-tracked, shipped in image) with an
optional live overlay in `skills/` (gitignored, mounted as a Docker
volume — written by the `dreaming` skill when it self-revises).

`read_skill(name)` reads `skills/<name>.md` first, falls back to
`skills.default/<name>.md`. Naming is signal-source-based:

- `nashdom-bill`, `tech-digest`, `dreaming`, `scheduler`,
  `telegram` — primary domain skills, loaded per signal.source.
- `routing`, `orchestrator`, `worker` — always loaded on the agent-loop path
  (`worker` is the default skill for a sub-agent with no domain skill).
- `planner` — the workflow compiler's system prompt (scheduler signals).
- `workflow-author` — a worker skill (on `smart` at least): drafts a saved
  workflow with `save_workflow`, dry-runs it with `try_workflow` (real reads
  and model steps, writes only recorded) and describes it in words with a
  sample; the parent activates it once the user confirms.
- `recovery` — requested by the built-in `signal` catch to phrase a failed
  signal to the user; an activated workflow controls the report policy.

The improver (`crates/evals`) writes an append-only overlay at
`skills/<name>.patch.md`; the runtime glues it onto the end of the body with
`append_patch`, so the instructions in force are body + patch.
**`crates/mcp/src/skills.rs` re-implements that composition** for the
`read_skill` export — the server and the agent don't share code, so if you
change `append_patch` (marker, spacing), change the MCP copy too or the export
starts describing a skill nobody runs. `crates/mcp/tests/skills_contract.rs`
runs both on generated skill directories and demands they agree;
`crates/mcp/tests/shipped_skills.rs` validates `skills.default` against the
real server's tool list the way the agent does at boot.

## Running

Binaries run with `cargo run -p <crate> --bin <name> -- …` locally; in the
image they are on `PATH`.

- `mcp-tools` — the MCP server. **Do not run locally if the droplet is also
  running it** — Telegram getUpdates is exclusive and the second poller
  causes 409 Conflict. `.mcp.json` points Claude Code at the droplet's
  restricted endpoint instead: `ssh -N -L 3001:127.0.0.1:3001 root@165.22.199.65`
  before the session starts (memory, news, skills, `ask_ouro`).
- `agent-supervisor` — the supervisor (long-running loop);
  `agent-supervisor --check-config` validates env and exits.
- `codex-service` — the codex sandbox service.
- `mcp-setup` — create/migrate `mcp_state` without starting the server.
- `gmail-auth` — one-time OAuth bootstrap. Writes to `mcp_state` (needs
  `DATABASE_URL`). `gmail-list-unread` — debug helper.
- `telegram-get-chat-id` — discover your chat id (after sending any message
  to your bot).
- `userbot-auth` — one-time MTProto login (phone + code).
- `monobank-txns` — debug helper.
- `migrate-channel-posts` — one-shot copy of the legacy sqlite
  `channel_posts` table (the old `tokens.db`) into PG `news_items` +
  inline-embed. Idempotent. `import-sqlite-state` moves the rest of
  `tokens.db` into `mcp_state`.
- `memory-import-notes` — one-shot copy of `knowledge_base_notes` into
  memory facts (idempotent; the source table is left untouched).
- `embed-backfill` — re-attempt embeddings for any `news_items` rows where
  `embedding IS NULL` (typically left behind by an OpenAI outage during
  inline embed).
- `eval-snapshot`, `eval-rag`, `eval-inspect` — the RAG eval harness over
  `crates/mcp/eval/fixtures`.
- `judge <traceId>` / `judge --recent N` (`--provider openai|codex`,
  `--dump` writes the materials for the judge-trace skill) — judge a run per
  node. `judge-noise` measures the judge's σ into `noise-baseline.json`;
  `judge-gate` A/Bs a candidate patch; `improve` runs one improver cycle;
  `judge-replay` replays a planner/compose generation under another model;
  `langfuse-trace` prints a trace; `news-filter-eval` runs the filter golden
  set (`eval-fixtures/news-filter/`).

Tests:

- `scripts/test.py` — the whole suite with the test binaries side by side
  (6 at a time, `TEST_JOBS`; ~2 min instead of ~7 for `cargo test`). Builds
  once, logs each binary to `target/test-logs/`, prints a failing one's log;
  extra args go to cargo (`scripts/test.py -p agent`). Each test file uses its
  own database in the shared test Postgres, so binaries don't collide.
- `cargo test` — every crate: unit, property, the Postgres tests (an
  auto-started pgvector testcontainer unless `TEST_DATABASE_URL` is set —
  point it at a throwaway server, never prod), the specs, the reference
  model. `PROPERTY_RUNS` raises property case counts. `cargo clippy
  --all-targets -- -D warnings`, `cargo fmt`.
- `cargo test -p mcp-tools --test specs` — the executable specification:
  `crates/mcp/specs/*.feature`, plain-English scenarios run by cucumber
  against the real server over MCP with a Testcontainers Postgres. **The
  `.feature` files are the user's**: never edit one to make a failing run
  pass — fix the code, or ask. New behaviour gets a scenario there first.
  See `crates/mcp/specs/README.md`.
- `cargo test -p mcp-tools --test model` — the reference model of memory
  documents: `crates/mcp/tests/model/model.rs` says what
  write/append/patch/revert mean (no database) and `properties.rs` checks its
  properties; `main.rs` sends random call sequences, then overlapping calls
  from several sessions, to both the model and the real server; answers must
  match and every concurrent history must be linearizable. `MODEL_CASES`
  raises the case count. **Same ownership as the `.feature` files**: when
  the server and the model disagree, fix the code or ask — a rule change
  starts in `model.rs`, and its properties must still pass.

## Production deployment

`docs/operations.md` → Deploy is the deployment reference. `scripts/deploy.sh`
selects runtime changes, cross-compiles affected binaries locally, skips
unchanged binaries by hash and sends them via rsync. Test/spec/doc-only
changes do not build, upload or restart. `scripts/test_deploy.py` checks this.

Native units: `ouro-mcp`, `ouro-mcp-tunnel`, `ouro-agent`, running as the
`ouro` user. Application files are `/opt/ouro/{bin,app}`; mutable state is
`/var/lib/ouro/{agent,skills,storage}`. Native credentials live in root-only
`/etc/ouro/{agent,mcp,mcp-tunnel}.env`, imported from the effective server
container environment at migration. They are authoritative afterwards;
local env files are never uploaded. `AGENT_DB_PATH` points at the migrated
sqlite database; its TS-era default path is still used locally.

Postgres and Codex remain containerized, with the existing `pg-data` and
`codex-home` volumes. Codex uses a stable runtime image and the deployed
`codex-service` executable through a read-only bind mount. Use
`scripts/deploy.sh --runtime-image` only for runtime dependencies; ordinary
Rust deployments never send the whole image. Evaluation workers remain off;
evaluation binaries can still be run explicitly as CLIs.

`ouro-containers` starts Postgres and Codex on boot. `ouro-tunnel-client`
waits for the restricted native endpoint before starting the tunnel
container, and retries if the daemon exits. The daemon still uses the
server checkout's `.env.mcp`; it uses host networking to reach
`http://127.0.0.1:3001/mcp`. Credentials are mapped inside its entrypoint
as before. Native MCP binds loopback with `MCP_HOST=127.0.0.1`; Postgres
and Codex publish only on loopback. Neither MCP endpoint is public.

The restricted endpoint keeps `MCP_NO_POLLERS=1`,
`MCP_TOOLSETS=news-read,telegram-send,skills,memory,peer`, actor `chatgpt`,
and multi-session transport. The full MCP is actor `supervisor` and owns
the only Telegram poller. Both share the same PG-backed memory; the
restricted endpoint reads the same live skills through the state symlink.

The first migration stops/removes legacy MCP/agent containers, copies the
closed sqlite database, live skills and attachments to native state, and
retains the existing database/sandbox volumes. No previous application
release or rollback copy is kept (explicit user preference). Binaries are
atomically replaced. Deploys require a clean pushed commit, reject stale
or concurrent applies, and record the current revision only after endpoint
health and the supervisor's skill validation pass.
