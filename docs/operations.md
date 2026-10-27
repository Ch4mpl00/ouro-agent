# Ouro: operations

Running locally, tests and deployment. The design is in
[architecture.md](architecture.md).

## Running locally

> ⚠️ **Don't run a polling MCP server while production is up.** Telegram
> `getUpdates` allows one consumer per bot. A second poller causes 409
> Conflict and silently swallows updates. Locally, use `MCP_NO_POLLERS=1` (or
> a separate bot token), or just use prod's restricted endpoint over the SSH
> tunnel above.

Environment files:

| File | For | Notes |
| --- | --- | --- |
| `.env.mcp` (from `.env.mcp.example`) | `mcp-tools` and the CLIs | Integration credentials, `OPENAI_API_KEY` (embeddings, required), `DATABASE_URL`, `TAVILY_API_KEY`, tunnel keys |
| `.env.agent` (from `.env.agent.example`) | `agent-supervisor` | Required: `DEEPSEEK_API_KEY`, `OPENAI_API_KEY`, `GEMINI_API_KEY`, `OPEN_ROUTER_API_KEY`. Optional: `AGENT_*_MODEL`, `AGENT_DECISION_MODEL`, `LANGFUSE_*`, `TELEGRAM_DEFAULT_CHAT_ID` / `TELEGRAM_TOPICS_JSON` |

The server and the supervisor load only `./.env` from the working directory
(they never search parent directories), plus the process environment. The
CLIs in `crates/mcp/src/bin` also load `./.env.mcp`. Docker Compose fills
the Postgres user, password and database from the shell or `.env`; all three
default to `mcp`.

```bash
docker compose up -d postgres          # pgvector on 127.0.0.1:5432

# MCP server over HTTP, tools only
(set -a; . ./.env.mcp; set +a
 DATABASE_URL=postgres://mcp:mcp@127.0.0.1:5432/mcp \
 MCP_TRANSPORT=http MCP_HOST=127.0.0.1 MCP_NO_POLLERS=1 \
 cargo run -p mcp-tools --bin mcp-tools)

# the supervisor, in a second terminal
(set -a; . ./.env.agent; set +a
 MCP_TRANSPORT=http MCP_URL=http://127.0.0.1:3000/mcp \
 cargo run -p agent --bin agent-supervisor)
```

- `agent-supervisor --check-config` validates the environment and exits.
  `--once` handles at most one signal.
- Without `MCP_TRANSPORT=http`, both sides default to stdio, and the
  supervisor spawns `mcp-tools` itself (`MCP_STDIO_COMMAND`).
- `MCP_PORT` defaults to 3000, and `MCP_ALLOWED_HOSTS` to
  `localhost,127.0.0.1,::1`.

### One-time credentials and CLIs

Run these with `cargo run -p mcp-tools --bin <name>`. They write to the
`mcp_state` database that `DATABASE_URL` points at.

| CLI | What it does |
| --- | --- |
| `gmail-auth` | Gmail OAuth: open the consent URL, then paste the `code`. Tokens go to `mcp_state`. |
| `userbot-auth` | MTProto login (phone + code) for the read-only userbot; needs `TELEGRAM_APP_ID` / `TELEGRAM_APP_API_HASH` |
| `telegram-get-chat-id` | Finds your chat id, after you send any message to the bot |
| `mcp-setup` | Creates and migrates `mcp_state` without starting the server |
| `embed-backfill` | Re-embeds rows left without a vector by an embeddings outage |
| `eval-snapshot` · `eval-rag` · `eval-inspect` | The RAG evaluation harness over `crates/mcp/eval/fixtures` |
| `gmail-list-unread`, `monobank-txns` | Debug helpers |
| `import-sqlite-state`, `migrate-channel-posts`, `memory-import-notes` | One-shot, idempotent migrations from the pre-Postgres sqlite era |

The evaluation binaries run with `cargo run -p evals --bin <name>`:

- `judge <traceId>` and `judge --recent N` judge a run per node.
- `judge-noise` measures the judge's σ into `noise-baseline.json`.
- `judge-gate` A/Bs a candidate patch.
- `improve` runs one improver cycle.
- `judge-replay` replays a compose generation under another model.
- `langfuse-trace` prints a trace.
- `optimize <trace-id>...` iteratively proposes and tests a task-specific
  standalone workflow using the normal agent loop with local workbench tools.
  The optimizer uses `smartest` at maximum reasoning effort; candidate compose
  calls use the configured presets. `--one-shot` retains proposal-only behavior;
  `--dump` prints the prompt without a model call. No live MCP transport,
  production workflow stores, activation or scheduling are available. See
  [optimizer workbench](optimizer.md) for scenarios, permissions, artifacts and
  the explicit standalone-context limitation (#60).
- `news-filter-eval` runs the filter golden set.

`judge-worker` and `improve-worker` are the long-running variants, and they
are off in production.

For ad-hoc queries:

```bash
sqlite3 -json packages/agent/data/agent.db "SELECT * FROM memory"
docker compose exec postgres psql -U mcp -d mcp_state -c "SELECT * FROM signals ORDER BY id DESC LIMIT 5"
```

## Workflow runs on the engine

Every workflow run of the agent (a signal, its primary loop, workers, guard
rounds, a recovery report) is a run of the ouro engine, stored in
`engine.db` beside agent.db (the same directory, so the same volume:
`/var/lib/ouro/agent/engine.db`), `ouro_*` tables the engine creates and
migrates (`crates/agent/src/runs.rs`; docs/designs/03-runs-on-the-engine.md).
Back up both files (`sqlite3 <file> ".backup <copy>"`). Not inside agent.db,
as design 01 first had it: the engine's adapter takes SQLite's write lock for
every operation, and agent.db's writes starved behind it (docs/designs/LOG.md). The built-ins `signal`,
`agent-loop` and `guard` are saved and activated at boot, with
`metadata.ouro.breaker: off`.

**Workflow versions** (docs/designs/04-versions-and-breaker.md): the engine
is also the version store. Every version the agent saves, its text and
activation history, the active version and its breaker live in `engine.db`;
agent.db's `workflow_meta` keeps only the agent's notes on them by content
(the note, whether a dry run tried it, whether it is a built-in text). Nothing
copies the versions back: **back up `engine.db` together with `agent.db`**
(both with `sqlite3 <file> ".backup <copy>"`, one after the other while the
agent is stopped, or accept that the two copies are a few seconds apart).
An engine store-version bump migrates `engine.db` in place.

The boot activates a built-in's new text only while the active version is a
built-in text: a version the agent activated (an override of `agent-loop` or
`signal`) stays across restarts and upgrades. A name an operator paused is
left alone at boot, and the agent cannot activate a version of it either,
since an activation would lift the pause; while `agent-loop` is paused no
loop and no failure report can start (the log names the pause). Inspect and
operate with the engine's control API (`ouro_runtime::control::Control`):
`definitions`/`definition` show versions, the trip and the pause; `pause`
and `resume` act on a name.

An agent version's breaker is the runtime's (3 faults in a row); a tripped
version stays active and is set aside for each run until a version is
activated again: the built-in runs in its place (the run's trace carries a
`fallback` record). A name keeps at most 20 never-activated versions; pruning
them belongs to the engine (Ch4mpl00/ouro#36).

**Capacity** (`runs::config`): one run tree is a primary loop, its guard
round and up to 16 workers with theirs, 34 drives; the signal holds none
while it waits. Two trees can be live at once (a resumed one and the next
signal's), so the worker drives up to **68 runs** (`max_concurrency`) with
**136 handler slots** (`max_function_concurrency`). A child starts at the
worker's next tick (`poll_interval`, 50 ms).

**Jump budget** (`limits.max_jumps`, per run tree): a backstop, not the
binding limit, which is each loop's `max_iterations`. One agent-loop round
takes two jumps plus one per guarded call; the dispatcher takes at most
three, the Telegram route a handful more (design 08): Σ loops `max_iterations × (2 + guarded calls per round) + 3`. With
the primary's 100 iterations and 16 workers of 50, each with up to 16
guarded calls a round, that is 16 203; the ceiling is **20 000**. The
recovery report runs as a root of its own (`recovery:<signal id>`), so an
exhausted tree budget never stops it. `max_tasks` per run is 50 000.

**Record sizes:** every task record of a loop carries its context, the
transcript included, so a loop's records grow with rounds × transcript
size; a call's checkpointed reply is clipped to 64 Ki UTF-16 units. The
engine sweeps the records of runs that ended more than 7 days ago; the
reconciler (every minute) sweeps the agent's own rows of those runs
(`run_trace`, `confirmations`, `guard_verdict_uses`, `run_evidence`, and
`evidence` no run links). Host data is at most a few KiB: large evidence
(skill text, earlier messages) is content-addressed in `evidence`.

**Latency:** a round's guard child, a worker and a signal's primary each
start at the worker's next tick, and every commit goes through the store, so
a signal costs a few hundred milliseconds before its first turn. A round of
many workers is CPU-bound in the engine (its listens poll the store while
they hold, and every task record carries the loop's context): in a debug
build 16 one-turn workers take about 30 s; measure a release build before
relying on wide fan-out.

**Signal loss window:** `get_next_signal` takes a signal before the engine
admits its run; a crash in between loses that signal, as before (design 03,
decision 5).

## Tests

| Command | What it runs |
| --- | --- |
| `scripts/test.py` | The whole suite, building once and running the test binaries side by side (`TEST_JOBS`, default 6). Logs go to `target/test-logs/`, and a failing binary's log is printed. Extra args go to cargo (`scripts/test.py -p agent`). Cleans `target/` above `TARGET_LIMIT_GB` (default 25). |
| `cargo test` | Every crate: unit, property, Postgres, specs and reference-model tests, one binary at a time |
| `cargo test -p mcp-tools --test specs` | **The executable specification**: plain-English `crates/mcp/specs/*.feature` scenarios run by cucumber against the real server over MCP |
| `cargo test -p mcp-tools --test model` | The reference model of memory documents (`tests/model/model.rs`), checked against the real server with random and concurrent call sequences, including linearizability |
| `python3 scripts/test_deploy.py` | Deployment safety checks; needs no Docker, SSH or production data |
| `cargo clippy --all-targets -- -D warnings` · `cargo fmt` | Lint and format |

Postgres tests use one shared pgvector testcontainer, with a fresh database
per test file, unless `TEST_DATABASE_URL` is set. Point that variable only at
a throwaway server, never at prod. `PROPERTY_RUNS` and `MODEL_CASES` raise
the number of property and model cases.

The `.feature` files and the reference model belong to the project owner.
When they disagree with the server, fix the code (or ask); never edit the
spec to make a run pass.

## Deploy

Production is one droplet (1 vCPU / 2 GB), reached as the SSH alias `ouro`:
its address lives only in each machine's `~/.ssh/config` (`Host ouro`,
`HostName <droplet ip>`, `User root`), never in the repo. It never
compiles Rust. The laptop cross-compiles only the affected Linux binaries in
a cached Docker builder and sends the changed files with rsync.

```bash
scripts/deploy.sh                    # deploy whatever the runtime changes since the server's revision need
scripts/deploy.sh agent              # also force a service: mcp, mcp-tunnel, agent, tunnel-client
scripts/deploy.sh mcp                # both MCP instances (one shared binary) + reconnect the agent
python3 scripts/test_deploy.py       # deployment regression checks, no Docker/SSH
```

`DEPLOY_HOST` (or `--host`) and `DEPLOY_DIR` (or `--repository`) override the
SSH host and the server checkout. They default to `ouro` and
`/root/agent-helper`. The commit must be **clean and pushed**, because the
server checks out exactly that commit.

### What a change deploys

`scripts/deploy.py` diffs the server's recorded revision against `HEAD`:

| Changed path | Builds | Restarts |
| --- | --- | --- |
| `Cargo.toml`, `Cargo.lock` | both service binaries | everything |
| `crates/mcp/{src,migrations,Cargo.toml}` | `mcp-tools` | `mcp`, `mcp-tunnel`, `agent` |
| `crates/agent/{src,migrations,Cargo.toml}` | `agent-supervisor` | `agent` |
| `skills.default/` | — | the native services |
| `crates/mcp/gateway.config.json` | — | `mcp`, `agent` |
| `docker-compose.yml`, `deploy/systemd/` | — | containers + every service |
| `deploy/probe.py` | — | the native services + tunnel client |
| tests, specs, docs, anything else | — | nothing: no build, upload or restart |

Two cases worth knowing:

- **`crates/evals` is not selected automatically**: its binaries are run
  as CLIs and never deployed. A change of the engine's pinned tag changes
  `Cargo.lock` and so rebuilds every service binary.
- **Unchanged binaries are omitted by hash**, so a rebuild that produces the
  same binary uploads nothing.

### How an apply works

On the server, `deploy.py --apply`:

1. Takes `/opt/ouro/deploy.lock`, which rejects concurrent applies.
2. Refuses a stale deployment (the server's revision changed since planning)
   and any payload whose checksum doesn't match.
3. Checks that the server checkout is clean and that the commit is on
   `origin`.
4. Stops the affected units, agent first.
5. Atomically replaces the binaries in `/opt/ouro/bin` and refreshes the
   shipped skills, the gateway config, `probe.py` and the systemd units.
   Then it runs `systemd-analyze verify` and `daemon-reload`.
6. Checks out the revision (detached) in the server checkout.
7. Runs `agent-supervisor --check-config` as `ouro` with the real
   `/etc/ouro/agent.env`.
8. Starts or restarts the containers and units.
9. Probes MCP (`:3000`) and restricted MCP (`:3001`). The
   MCP probe is a sessionless `ping` answered with HTTP 422, so it can never
   evict the agent's session.
10. Waits for the supervisor to log `mcp connected; skill validation passed`.
11. Writes `/opt/ouro/revision` and removes the staging directory.

**No previous release or rollback directory is kept.** This is an explicit
choice. To roll back, deploy an older commit.

### Production layout

| What | Where |
| --- | --- |
| Native units (user `ouro`) | `ouro-mcp` (full MCP + the only Telegram poller), `ouro-mcp-tunnel` (restricted MCP), `ouro-agent` (supervisor) |
| Container units | `ouro-containers` starts Postgres on boot. `ouro-tunnel-client` waits for the restricted endpoint, then runs the tunnel container and owns its retries. |
| Executables | `/opt/ouro/bin` (`mcp-tools`, `agent-supervisor`) |
| App files | `/opt/ouro/app`: `skills.default`, `crates/mcp/gateway.config.json`, `probe.py`, and the symlinks `skills` → `/var/lib/ouro/skills` and `storage` → `/var/lib/ouro/storage` |
| State | `/var/lib/ouro/agent` (agent.db, `AGENT_DB_PATH`, and the engine's `engine.db` beside it), `/var/lib/ouro/skills` (live skills), `/var/lib/ouro/storage` (attachments) |
| Native env (root-only) | `/etc/ouro/agent.env`, `/etc/ouro/mcp.env`, `/etc/ouro/mcp-tunnel.env` |
| Container env | The server checkout's `.env.mcp` (tunnel keys: `OPENAI_TUNNEL_ID`, `OPENAI_TUNNEL_API_KEY`) and `.env`/Compose interpolation (Postgres credentials) |
| Docker volumes | `agent-helper_pg-data`. Keep the compose project directory stable. |

**Native env files are authoritative.** They were imported from the
containers' effective environment at migration. Edit them on the server and
restart the affected unit. Local `.env.*` files are never uploaded.

**Endpoints.** None is public; everything binds loopback.

| Port | Service |
| --- | --- |
| `127.0.0.1:3000` | MCP (`MCP_TOOLSETS` unset, actor `supervisor`, pollers on) |
| `127.0.0.1:3001` | Restricted MCP (`MCP_NO_POLLERS=1`, `MCP_TOOLSETS=news-read,telegram-send,skills,memory,peer`, actor `chatgpt`, multi-session) |
| `127.0.0.1:5432` | Postgres |

The tunnel container uses host networking to reach
`http://127.0.0.1:3001/mcp` for the ChatGPT connector (OpenAI Secure MCP
Tunnel). Claude Code reaches the same endpoint over `ssh -L 3001`. Both MCP
instances share the PG-backed memory, and the restricted one reads the same
live skills through the state symlink.

### Credentials and one-time setup

- **Integration keys** (Google OAuth client, bot token, Monobank, OpenAI,
  Tavily) go in `/etc/ouro/mcp.env`. Model keys go in
  `/etc/ouro/agent.env`. Restart the unit after editing.
- **Gmail OAuth and the userbot session** live in `mcp_state`, not in env.
  `gmail-auth` and `userbot-auth` are not installed on the server (a deploy
  ships only the two service binaries). Run them with a `DATABASE_URL`
  that reaches production Postgres, which listens on loopback only. One way
  is from the laptop through an SSH forward:
  `ssh -N -L 15432:127.0.0.1:5432 ouro`, then
  `DATABASE_URL=postgres://<user>:<password>@127.0.0.1:15432/<db> cargo run -p mcp-tools --bin gmail-auth`.
  The credentials are the ones in `/etc/ouro/mcp.env`.
- **The tunnel** needs `OPENAI_TUNNEL_ID` and a restricted
  `OPENAI_TUNNEL_API_KEY` (Tunnels Read + Use) in the server checkout's
  `.env.mcp`.

### First deployment (bootstrap)

When `/opt/ouro/revision` doesn't exist, the deploy builds every binary and
migrates from the old all-Docker layout:

- It creates the `ouro` user and writes `/etc/ouro/*.env` from the legacy
  containers' effective environment. The tunnel's file gets
  `MCP_TOOLSETS=news-read,telegram-send,skills,memory`; `peer` was added to
  it by hand later.
- It stops the legacy agent and MCP containers gracefully (closing sqlite
  and stopping the second poller). It copies `agent-data`, `agent-skills` and
  `mcp-storage` into `/var/lib/ouro`, checks agent.db with `PRAGMA
  quick_check`, and removes the old containers.

Postgres keeps its existing data volume.

### On the server

```bash
systemctl status ouro-agent ouro-mcp ouro-mcp-tunnel ouro-tunnel-client
journalctl -u ouro-agent -f
systemctl restart ouro-agent
cat /opt/ouro/revision                 # what is deployed
```
