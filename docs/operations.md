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
| `.env.agent` (from `.env.agent.example`) | `agent-supervisor` | Required: `DEEPSEEK_API_KEY`, `OPENAI_API_KEY`, `GEMINI_API_KEY`, `OPEN_ROUTER_API_KEY`. Optional: `AGENT_*_MODEL`, `AGENT_DECISION_MODEL`, `LANGFUSE_*`, `CODEX_URL`, `TELEGRAM_DEFAULT_CHAT_ID` / `TELEGRAM_TOPICS_JSON` |

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
- `code_agent` needs a `codex-service` at `CODEX_URL` (default
  `http://localhost:3010`).

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
- `judge-replay` replays a planner or compose generation under another model.
- `langfuse-trace` prints a trace.
- `news-filter-eval` runs the filter golden set.

`judge-worker` and `improve-worker` are the long-running variants, and they
are off in production.

For ad-hoc queries:

```bash
sqlite3 -json packages/agent/data/agent.db "SELECT * FROM memory"
docker compose exec postgres psql -U mcp -d mcp_state -c "SELECT * FROM signals ORDER BY id DESC LIMIT 5"
```

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

Production is one droplet (1 vCPU / 2 GB, `root@165.22.199.65`). It never
compiles Rust. The laptop cross-compiles only the affected Linux binaries in
a cached Docker builder and sends the changed files with rsync.

```bash
scripts/deploy.sh                    # deploy whatever the runtime changes since the server's revision need
scripts/deploy.sh agent              # also force a service: mcp, mcp-tunnel, agent, codex, tunnel-client
scripts/deploy.sh mcp                # both MCP instances (one shared binary) + reconnect the agent
scripts/deploy.sh --runtime-image    # rebuild and ship the Codex runtime image (Node/Python/Codex CLI)
python3 scripts/test_deploy.py       # deployment regression checks, no Docker/SSH
```

`DEPLOY_HOST` (or `--host`) and `DEPLOY_DIR` (or `--repository`) override the
SSH host and the server checkout. They default to `root@165.22.199.65` and
`/root/agent-helper`. The commit must be **clean and pushed**, because the
server checks out exactly that commit.

### What a change deploys

`scripts/deploy.py` diffs the server's recorded revision against `HEAD`:

| Changed path | Builds | Restarts |
| --- | --- | --- |
| `Cargo.toml`, `Cargo.lock` | all three service binaries | everything |
| `crates/mcp/{src,migrations,Cargo.toml}` | `mcp-tools` | `mcp`, `mcp-tunnel`, `agent` |
| `crates/agent/{src,migrations,Cargo.toml}` | `agent-supervisor` | `agent` |
| `crates/codex/…` | `codex-service` | `codex` |
| `skills.default/` | — | the native services |
| `crates/mcp/gateway.config.json` | — | `mcp`, `agent` |
| `docker-compose.yml`, `deploy/systemd/` | — | containers + every service |
| `deploy/probe.py` | — | the native services + tunnel client |
| tests, specs, docs, anything else | — | nothing: no build, upload or restart |

Two cases need a manual step:

- **`crates/owf` and `crates/evals` are not selected automatically.** After
  a change that only touches `crates/owf`, run `scripts/deploy.sh agent`.
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
9. Probes MCP (`:3000`), restricted MCP (`:3001`) and Codex (`:3010`). The
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
| Container units | `ouro-containers` starts Postgres and Codex on boot. `ouro-tunnel-client` waits for the restricted endpoint, then runs the tunnel container and owns its retries. |
| Executables | `/opt/ouro/bin` (`mcp-tools`, `agent-supervisor`, `codex-service`) |
| App files | `/opt/ouro/app`: `skills.default`, `crates/mcp/gateway.config.json`, `probe.py`, and the symlinks `skills` → `/var/lib/ouro/skills` and `storage` → `/var/lib/ouro/storage` |
| State | `/var/lib/ouro/agent` (agent.db, `AGENT_DB_PATH`), `/var/lib/ouro/skills` (live skills), `/var/lib/ouro/storage` (attachments) |
| Native env (root-only) | `/etc/ouro/agent.env`, `/etc/ouro/mcp.env`, `/etc/ouro/mcp-tunnel.env` |
| Container env | The server checkout's `.env.mcp` (tunnel keys: `OPENAI_TUNNEL_ID`, `OPENAI_TUNNEL_API_KEY`) and `.env`/Compose interpolation (Postgres credentials) |
| Docker volumes | `agent-helper_pg-data`, `agent-helper_codex-home`. Keep the compose project directory stable. |

**Native env files are authoritative.** They were imported from the
containers' effective environment at migration. Edit them on the server and
restart the affected unit. Local `.env.*` files are never uploaded.

**Endpoints.** None is public; everything binds loopback.

| Port | Service |
| --- | --- |
| `127.0.0.1:3000` | MCP (`MCP_TOOLSETS` unset, actor `supervisor`, pollers on) |
| `127.0.0.1:3001` | Restricted MCP (`MCP_NO_POLLERS=1`, `MCP_TOOLSETS=news-read,telegram-send,skills,memory,peer`, actor `chatgpt`, multi-session) |
| `127.0.0.1:3010` | Codex service |
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
  ships only the three service binaries). Run them with a `DATABASE_URL`
  that reaches production Postgres, which listens on loopback only. One way
  is from the laptop through an SSH forward:
  `ssh -N -L 15432:127.0.0.1:5432 root@165.22.199.65`, then
  `DATABASE_URL=postgres://<user>:<password>@127.0.0.1:15432/<db> cargo run -p mcp-tools --bin gmail-auth`.
  The credentials are the ones in `/etc/ouro/mcp.env`.
- **Codex** keeps its login in the `codex-home` volume. Bootstrap it once
  from the server checkout:
  `docker compose run --rm codex codex login --device-auth`.
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
- It tags the installed image as `ouro-codex-runtime:current`, so no image
  is transferred.
- It stops the legacy agent and MCP containers gracefully (closing sqlite
  and stopping the second poller). It copies `agent-data`, `agent-skills` and
  `mcp-storage` into `/var/lib/ouro`, checks agent.db with `PRAGMA
  quick_check`, and removes the old containers.

Postgres and Codex keep their existing data volumes.

### On the server

```bash
systemctl status ouro-agent ouro-mcp ouro-mcp-tunnel ouro-tunnel-client
journalctl -u ouro-agent -f
systemctl restart ouro-agent
cat /opt/ouro/revision                 # what is deployed
```
