# 01. Run the agent on the ouro engine

Status: approved (overview) · Epic: #62 · Engine: Ch4mpl00/ouro (pinned rev on main until v0.2.0)
Review: goldfish round 1 (2026-10-25), findings resolved below; decisions log: [LOG.md](LOG.md)

This is the overview: target shape, decisions, engine prerequisites, the order
of steps and the agent invariants every step must keep. Each step gets its own
document (02…) before it is built.

## Problem

The agent owns a whole workflow stack: a forked interpreter (`crates/owf`,
sync journal traits), its own SQLite run store (`agent/src/durable.rs`,
`workflow_runs`/`workflow_records`/…), a function catalog that classifies calls
as read/write from MCP's `readOnlyHint`, an in-process child driver, a
memory-KV breaker with per-run fallback to built-in text, a cron table with a
30 s tick (`schedule.rs`), and a sandbox that builds a second engine
(`sandbox.rs`). The same design now lives, generalised and verified (Quint
contract, conformance on three stores, crash injection), in the ouro engine.
Two copies means every fix lands twice and the agent's copy is the weaker one.

## Decisions

Maintainer (2026-10-24/25):

1. **Dependency:** `ows`, `ouro-durable`, `ouro-runtime` as git dependencies
   pinned to a revision of engine `main` until `v0.2.0` is tagged, then the tag.
   Engine changes the agent needs go into the engine, never patched here.
2. **No migration of runs or saved workflows.** Current runs are dropped;
   agent-authored saved workflows are deleted and recreated by the agent;
   scheduled tasks are recreated by hand. No compatibility code. Old tables are
   dropped by a new migration (applied migrations never change).
3. **Store:** the engine's SQLite adapter inside `agent.db` (`ouro_*` tables,
   `ouro_migrations`), its own connection: no shared transactions.
4. **Schedules on engine triggers:** one definition per task,
   `metadata.ouro.timezone`, one-shot `at`, `$run.schedule`.
5. **`signal` is a durable engine run.**
6. **Tracing:** Langfuse stays in the agent; the engine knows only OTel.
7. **Breaker fallback is agent logic:** on a trip of a *user* version the agent
   runs its built-in version for that run with `Runtime::start_at`; the user's
   version stays active (digest rule).
8. **Delivery chosen at registration** per MCP tool from `readOnlyHint` (writes
   at-most-once); YAML may tighten with `metadata.ouro.delivery`; ambiguous
   write failures are unknown outcomes, never resent.
9. **Host-vouched run parameters live in the engine admission** (engine design
   03, host data): the agent puts preset, confirmation target, conversation and
   session context in the run's host data; host functions read
   `FunctionContext::host()`; host functions start vouched children with
   `FunctionContext::start_child`. Workflow-started children get none. No agent
   side table. *(Revised from "agent table" after review H2.)*
10. **Built-in definitions opt out of the breaker** (engine design 04,
    `metadata.ouro.breaker: off` on `signal`, `agent-loop`, `guard`,
    `recovery`): a provider outage must not stop the agent. *(Review H3.)*
11. **Evals optimizer** keeps the engine interpreter in memory (`ows::run::run`).
12. **Worker failure** restarts the engine worker in place with a backoff.

Elephant (this session, logged in [LOG.md](LOG.md), reversible):

13. **One integration branch `feat/engine`;** each step lands on it with its
    suites green; **no prod deploy** until the maintainer decides (deploy is
    deferred, not part of the night run).
14. **Tracing split** (review H4): `agent_turn`/`tool` handlers keep writing
    their own generation/tool spans with full payloads through the agent's
    Tracer, parented to the engine's call span (`FunctionContext::span`);
    engine observers carry run/task lifecycle only. Durable derived state
    (activity journal projection, mirror run-end, judge readiness) is
    reconciled from the store — `Control::history`/`list` with an end-time
    watermark — not from observer events (at-most-once).
15. **Saved workflow sources live in an agent table too** (`workflow_meta`:
    name, version, YAML, note, created, tried, builtin, activation history), so
    an engine store-version bump can be re-seeded from it (review M7).
16. **Gap workarounds** (review Q1): cooperative cancellation — accept, close
    spans from `Drop` guards; dry runs — a second in-memory `Runtime` with
    drafts activated, a dry registry and its own observer; run ends — store
    reconciliation (14); missing function on resume — a persisted last-known
    tool snapshot as a stable superset with permissive schemas, absent tools
    answer `not_executed`, `check()` runs on save.
17. **`emit` is refused in agent-saved workflows** for now (app-side check on
    save), and the reserved built-in names (`signal`, `agent-loop`, `guard`,
    `recovery`) can't be saved by the agent.

## Target shape

```
supervisor ─ signal ─► Runtime::start_with("signal:<id>", "signal", input, host)
                         │ (engine store in agent.db; engine worker drives)
                         ▼
             engine interpreter ── calls ──► Registry (agent-built):
                                              MCP tools (snapshot superset,
                                                delivery by readOnlyHint),
                                              agent_turn / classify / tool_once /
                                                tool_repeat / guard / confirm …,
                                              decisions, compose, memory, skills
             host functions ── FunctionContext::start_child ──► vouched children
             handlers ──► Tracer (generations, tool spans, full payloads)
             observers ──► lifecycle spans
             store reconciliation ──► activity journal · mirror · judge readiness
             trip notice (user versions) ──► start_at(built-in) for that run
agent tables: workflow_meta, run_trace, confirmations, guard_verdict_uses,
              memory, traces, judgements, activity …
```

## Engine prerequisites

| Engine change | Design | Needed by |
|---|---|---|
| Host data at admission, host-started children | ouro 03 (#34) | step 3 |
| Breaker threshold per definition (`off`) | ouro 04 (#35) | step 3 |
| Tag `v0.2.0` after 03/04 and the pre-tag drift review | — | step 3 pins it |

## Agent invariants every step keeps (each step document names its tests)

1. A confirmation is honoured only from a consumed event bound to the run, the
   call key, the chat, the message and the thread — read from the guard run's
   recorded event (`Runtime::records`), never from a YAML output.
2. Every model write is reviewed by the guard round.
3. Model parameters come only from the host (host data), never from YAML or a
   workflow-started child.
4. An at-most-once tool call is never resent after a crash, shutdown, hold or
   trip; an unknown outcome reaches the model as "may have happened".
5. Workers inherit the confirmation target.
6. A card whose send outcome is unknown is never resent.

## Steps (on `feat/engine`; suites green after each)

1. **Dependency and dialect.** Pin the engine; parse every built-in YAML with
   `ows` in tests asserting the effective extensions; prepare `metadata.ouro`
   versions of the built-ins and the DSL-teaching skills text
   (`workflow-author`, tool descriptions) — switched over in step 3, since the
   running `owf` reads the flat keys.
2. **Runs on the engine** (the core; one step, several commits): `Runtime`
   over the SQLite adapter in `agent.db`; registry from the tool snapshot plus
   host functions with typed outcomes, swapped on reconnect; the loop's `tool`
   split into `classify → tool_once | tool_repeat` with a `catch` for
   `call.unknown`; host functions gated by `FunctionContext::definition`
   (`confirm_request`, `resolve_review`, dispatcher functions); `signal` as a
   durable run started with host data; `primary_loop`/`recovery_loop`/worker/
   guard starts as host-started children (`FunctionContext::start_child`);
   supervisor waits on the run's end (one signal at a time kept); targeted
   button delivery with Telegram callback ids; built-ins saved and activated at
   boot with `breaker: off`; capacity config (`max_concurrency`,
   `max_function_concurrency`, `limits.max_jumps`) sized and documented;
   tracing split (decision 14) including `run_trace` and store reconciliation;
   migration dropping the old run tables.
3. **Versions and breaker:** `workflow_meta` (decision 15); save/activate/try
   tools over `Runtime::save/activate`; trip notice handling for user versions
   with `start_at`; delete the memory-KV breaker and built-in text fallbacks.
4. **Sandbox:** the second in-memory `Runtime` (decision 16); `try_workflow`
   and the activation gate on it; remove run-time `engine.sandbox()` branches
   in favour of a dry registry. (Until this step, activation of agent-authored
   versions is refused on the branch.)
5. **Schedules:** one definition per task, `fire_scheduled` host function,
   synthetic tools rewritten; per-task overlap/catch-up chosen; a tripped
   scheduled definition's notice becomes a signal the agent can act on
   ("the runtime informs, the agent decides"); time-zone change re-saves task
   definitions; `schedule.rs` tables and tick removed.
6. **Remove `crates/owf`;** evals on `ows::run::run`; check `crates/evals` for
   `workflow_runs` dependencies; docs (`CLAUDE.md`, `docs/architecture.md`,
   `docs/operations.md`).

## Failure modes and risks

- **Delivery binding:** writes must be at-most-once; changing a tool's
  `readOnlyHint` changes its binding (in-flight runs interrupt, then fault).
- **Schema binding:** permissive schemas + `check()` on save (decision 16).
- **Capacity:** a parent waiting on guard/worker children can starve them at
  the defaults (4 drives, 16 function calls); a 15-minute `hold` keeps a drive
  permit.
- **Jump budget:** one unit per loop round across the tree; default 100 is too
  low; set and document.
- **Storage growth:** each loop record stores the transcript; bounded by the
  jump budget and the 7-day sweep — state the numbers in step 2.
- **One file, two connections:** formerly transactional pairs become
  eventually consistent; each gets a test.
- **Store upgrades:** an engine store-version bump re-creates the store;
  workflows re-seeded from `workflow_meta`, schedules re-generated; back up with
  `sqlite3 .backup` first.
- **Pending confirmations at cut-over** are dropped (decision 2); their cards
  are edited to "expired" once at first start.

## Rollout

Nothing deploys during the night run. The maintainer deploys `feat/engine`
after review; before it: back up `agent.db`, expect old runs and saved
workflows gone, recreate scheduled tasks after step 5.

## Open questions

None for the overview. Step documents may raise their own; any decision
needed while the maintainer is away is taken reversibly and logged in LOG.md.
