# 01. Run the agent on the ouro engine

Status: draft · Epic: #62 · Engine: Ch4mpl00/ouro (target tag v0.2.0)

This is the overview: target shape, decisions, the remaining engine gaps and
the order of steps. Each step gets its own document (02…) before it is built.

## Problem

The agent owns a whole workflow stack: a forked interpreter (`crates/owf`,
sync journal traits), its own SQLite run store (`agent/src/durable.rs`,
`workflow_runs`/`workflow_records`/…), a function catalog that classifies calls
as read/write from MCP's `readOnlyHint`, an in-process child driver, a
memory-KV breaker with per-run fallback to built-in text, a cron table with a
30 s tick (`schedule.rs`), and a sandbox that builds a second engine
(`sandbox.rs`). About 9.5k lines in those files plus `owf`. The same design
now lives, generalised, verified (Quint contract, conformance on three stores,
crash injection) and maintained, in the ouro engine. Keeping two copies means
every fix lands twice and the agent's copy is the weaker one.

## Decisions (maintainer, 2026-10-24/25)

1. **Dependency:** `ows`, `ouro-durable`, `ouro-runtime` as git dependencies
   pinned to a tag (v0.2.0 or later). Engine changes needed by the agent go
   into the engine (its own issues/designs), never patched in the agent.
2. **No migration of runs or saved workflows.** Current runs are dropped;
   agent-authored saved workflows are deleted and recreated by the agent;
   scheduled tasks are recreated by hand. No compatibility code. Old tables
   are dropped by a new migration (applied migrations never change).
3. **Store:** the engine's SQLite adapter inside `agent.db` (its own
   `ouro_*` tables, schema tracked in `ouro_migrations`, engine#27). Separate
   connection: no shared transactions with the agent's tables.
4. **Schedules move onto engine triggers** ("use the engine to the maximum"):
   one definition per task, `metadata.ouro.timezone`, one-shot `at`,
   `$run.schedule` (engine#24). `schedule.rs` rows and the tick go away.
5. **`signal` becomes a durable engine run** (one kind of workflow, one launch path).
6. **Tracing via the engine's observer seam** (engine#26): Langfuse, the local
   trace mirror and the activity journal are observers; replayed history is
   never re-emitted. Langfuse stays in the agent; the engine only knows OTel.
7. **Breaker fallback is agent logic:** on a trip the agent runs its built-in
   version for that run with `Runtime::start_at` (engine#23); the user's
   version stays active. A trip refuses only the tripped content (digest rule).
8. **Delivery is chosen by the agent at registration** per MCP tool (from
   `readOnlyHint`: writes at-most-once); a YAML call may tighten with
   `metadata.ouro.delivery`. Ambiguous write failures are answered as unknown
   outcomes (engine#25), so the engine never resends them.
9. **Admission trust stays agent-side:** model parameters given by the host
   (preset, confirmation target, conversation, session context) live in an
   agent table keyed by run id; host functions read them by
   `FunctionContext::run_id`. No engine seam.
10. **Evals optimizer** keeps using the engine interpreter in memory
    (`ows::run::run`).
11. **Worker failure** restarts the engine worker in place with a backoff;
    the process does not exit.

## Target shape

```
supervisor ─ signal ─► Runtime::start("signal:<id>", "signal", input)
                         │  (engine store in agent.db, engine worker drives)
                         ▼
             engine interpreter ── calls ──► Registry (agent-built):
                                              MCP tools (delivery by readOnlyHint),
                                              agent_turn / tool / guard host functions,
                                              decisions, compose, memory, skills …
             engine observers ──► Langfuse · trace mirror · activity journal
             engine triggers  ──► scheduled task definitions
             trip notice      ──► agent: start_at(built-in) for that run
agent tables: admitted_runs, run_trace, workflow_meta, confirmations,
              guard_verdict_uses, memory, traces, judgements, activity …
```

## What maps directly (no engine work)

Interpreter and validation (`ows::model`; the built-in `agent-loop`, `guard`,
`signal` YAML parse and validate unchanged — checked), durable runs, leases,
children (`run: workflow` / `start_child`), event waits and targeted delivery
(CloudEvents with ids: Telegram callback id as `id`), versions/activation,
breaker reports, dry run with host mocks, control API (hold/cancel/pause for
the console), annotations (token usage), schedules with time zones and
one-shots, typed call outcomes, observers.

## Remaining engine gaps (to settle before the step that needs them)

| Gap | Needed by | Options |
|---|---|---|
| Cooperative cancellation: the runtime drops a cancelled handler without waiting; agent turns end their spans and child loops first today | step 3 | engine `Function::cooperative` (wait for the handler on stop, bounded), or accept: the observer's `run_interrupted` closes spans |
| Dry runs are silent to host observers and return no per-task path/outputs; the sandbox report needs them | step 6 | engine: dry run accepts an observer and returns records; or the agent runs drafts on a second in-memory `Runtime` with its own observer |
| No events for runs cancelled while no execution holds them (cascade-cancelled children); the journal waits for "all runs of a trace ended" | step 4 | engine run-ended notice to the event sink; or the agent polls `Runtime::get` for the runs in `run_trace` |
| Built-in definitions can be tripped by a provider outage (three faulted loops in a row) | step 5 | engine per-definition breaker opt-out/threshold in `metadata.ouro`; or the agent re-runs built-ins pinned after every trip |
| A function missing on resume faults the run (counts toward the breaker); MCP tools come and go with the connection | step 2 | agent registers a stable superset with permissive schemas and answers `not_executed` when a tool is gone; optional engine option to interrupt instead of fault |

## Steps (each leaves the suite green and deployable)

1. **Interpreter and model types.** Depend on the engine; replace `owf::model`
   uses with `ows::model`; move the read/write classification out of the
   interpreter into an agent table. Ephemeral paths on `ows::run::run`.
2. **Registry.** Build the engine `Registry` from MCP `tools/list` (stable
   superset, permissive schemas, delivery by `readOnlyHint`) plus all host
   functions; handlers answer typed outcomes; swap with `set_registry` on
   reconnect. Split the loop's `tool` step so delivery is static
   (`classify` → `tool_once` (AMO) | `tool_repeat` (ALO)).
3. **Runs on the engine** (largest). `Runtime` over the SQLite adapter in
   `agent.db`; replace `durable.rs`, `execute_saved`, the agent's worker,
   resume, child driver with `Runtime::start/drive/worker`; `signal` as a
   durable run; children via the engine; button presses via
   `Runtime::deliver` with ids; `admitted_runs` table; capacity config
   (`max_concurrency`, `max_function_concurrency`, `limits.max_jumps`) sized
   for a primary loop with guard and workers; migration dropping the old run
   tables.
4. **Observers.** Langfuse, mirror and activity journal as engine observers;
   `run_trace` table replaces the anchor columns; journal/judge readiness
   without SQL joins to runs.
5. **Versions and breaker.** `workflow_meta` (note, created, tried, builtin);
   save/activate/try tools over `Runtime::save/activate`; built-ins saved at
   boot; trip handling with `start_at`; delete the memory-KV breaker and
   built-in text fallbacks.
6. **Sandbox.** Signal-chain dry run on a second in-memory `Runtime` (or engine
   dry run with observer); remove run-time `engine.sandbox()` branches in
   favour of a dry registry.
7. **Schedules.** One definition per task; `fire_scheduled` host function;
   synthetic tools rewritten; `schedule.rs` tables and tick removed.
8. **Remove `crates/owf`**; evals optimizer on `ows::run::run`; docs
   (`CLAUDE.md`, `docs/architecture.md`, `docs/operations.md`).

## Contract and model

The engine's contract covers runs, calls, children, triggers and the breaker.
The agent keeps its own invariants (confirmation verification only by a
consumed event, guard review of model calls, the admitted-input rule) and their
tests; each step's document lists which agent tests pin them.

## Failure modes and risks

- **Delivery binding:** a tool registered at-least-once is resent after a
  crash; writes must be at-most-once. Changing a tool's `readOnlyHint` changes
  its binding: in-flight runs see a binding mismatch (interrupt, then fault).
- **Schema binding:** exact schema comparison on resume → register permissive
  schemas, validate arguments in the agent's `check()` and in MCP.
- **Capacity:** a parent waiting on its guard/worker children can starve them
  at the default 4 drives / 16 function calls.
- **Jump budget:** one unit per loop round across the whole tree; default 100
  is too low for a long primary loop with workers.
- **One file, two connections:** formerly transactional pairs (trace mirror vs
  run end, card vs run, sweep) become eventually consistent; each needs a test.
- **Child latency:** a child starts on the next worker tick, not inside the
  parent's call.

## Tests

Per step, in its document. The existing agent suites that cover each layer
today: `flows.rs`, `workflow_kinds.rs`, `agent_loop.rs`, `loop_variants.rs`,
`durable*.rs`, `loop_durability.rs`, `supervisor*.rs`, `sandbox.rs`,
`schedule.rs`, `tracing.rs`, `activity_*.rs`, `confirmation_properties.rs`,
`tool_results.rs`. A step is done when its suites pass on the engine and the
replaced code is deleted.

## Rollout

Deploy after each step that changes runtime behaviour (3, 4, 5, 7). Before
step 3: back up `agent.db`; old runs and saved workflows are dropped by design.
After step 7: recreate scheduled tasks by hand.

## Open questions

1. Each engine gap in the table: engine change or agent workaround?
2. Is the step order right, or should observers (4) come together with runs
   (3) so tracing never regresses on prod between deploys?
