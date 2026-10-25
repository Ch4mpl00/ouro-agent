# 03. Step 2: runs on the engine

Status: draft · Overview: [01](01-engine-migration.md) step 2 · Branch: `feat/engine`
Engine prerequisites: ouro designs 03 (host data, host-started children) and
04 (breaker policy) merged; engine pinned to that revision.

The core step: every workflow run of the agent (signal, loops, workers, guard
rounds, recovery, saved workflows, routes) becomes an engine run. Built as
several commits (phases A–E below), each leaving the suite green; the step is
done when the agent's own run store, interpreter use and child driver are
gone from the running path.

## Problem

See the overview. Concretely this step replaces: `agent/src/durable.rs` (run
store), `flows.rs` `execute_saved`/`resume`/`worker`/`recover_once`/
`run_loop_durable`/`run_child`/`loop_child`, the `start_child` host function,
the `owf::run::Functions` implementations (`Functions`, `SignalFunctions`),
the anchor columns of the trace mirror, and the SQL joins from agent tables to
`workflow_runs`/`workflow_events`.

## Decisions

From the overview (not reopened): engine store in `agent.db`; `signal` durable;
host data carries model parameters; built-ins `breaker: off`; delivery by
`readOnlyHint` at registration; tracing split with store reconciliation;
`emit` refused and reserved names unsavable in agent-authored workflows; no
migration of runs/saved workflows.

This document adds:

- **The supervisor still handles one signal at a time** (today's semaphore):
  it starts `signal:<signal id>` with host data, then waits for that run's end
  (polling `Runtime::get` with backoff, woken by the observer's `run_ended` as
  a hint). Signal-level concurrency is a later, separate decision.
- **The signal id is the idempotency key**: the run id is
  `signal:<mcp signal id>`; a crash between `get_next_signal` and the committed
  start re-starts the same id on the next take (MCP re-offers an unacknowledged
  signal) or, if already admitted, joins it.
- **Host functions that run loops become child starters**: `primary_loop`,
  `recovery_loop`, `run_saved_workflow`, the loop's worker start and guard start
  call `FunctionContext::start_child` and return a child ref; the YAML waits with
  `listen` on `done:<id>` and reads the result with `child_outcome`. No host
  function blocks for a whole loop (that would hold drive and function permits).
- **Who may call what** is checked in handlers against
  `FunctionContext::definition` and host data (one registry serves all
  definitions): dispatcher functions only from `signal` and activated routes;
  `confirm_request`/`resolve_review` only from `guard` runs started by a loop;
  loop host functions (`agent_turn`, `tool_*`, `agent_settle`, `loop_setup`)
  only from runs with loop host data.

## Design

### A. Registry

- `Registry` built from: the MCP tool snapshot (decision 16 of the overview:
  persisted last-known `tools/list` in `agent.db`, permissive `{"type":"object"}`
  schemas, delivery `AtMostOnce` unless `readOnlyHint`; `WITHHELD` and
  supervisor-only tools excluded), plus every host function with its schema and
  delivery:
  - **loop:** `agent_turn` (ALO; a model call may be repeated, tokens only),
    `classify` (ALO, pure read of tool effect + guard state), `tool_once` (AMO),
    `tool_repeat` (ALO), `agent_settle` (ALO), `loop_setup`, `signal_input`,
    `recent_activity`, `pending_confirmations`, `preselect_skills` (ALO reads);
  - **children (ALO, design 03 rule):** `start_guard`, `start_worker`,
    `primary_loop`, `recovery_loop`, `run_saved_workflow`, `route_signal`
    (returns a child ref or `{routed:false}`);
  - **guard:** `review` (ALO), `confirm_request` (AMO; card send unknown is never
    resent — invariant 6), `resolve_review` (ALO);
  - **dispatcher:** `deliver_event` (ALO; idempotent by event id),
    `signal_session`, `signal_transcript`;
  - **workflow steps:** `filter_relevant`, `choose`, `llm_compose`, `set_memory`
    (as today, delivery as today's `check` allows).
- Handlers answer typed outcomes: MCP `ToolCallFailure::NotExecuted` →
  `not_executed`, ambiguous transport failure after send → `unknown_outcome`,
  cancellation → `interrupted`; everything else a definite answer.
- On MCP reconnect, rebuild and `Runtime::set_registry`; the snapshot is
  updated, never shrunk while runs reference a tool (superset).

### B. YAML (built-ins move to the engine dialect)

- Built-ins converted with step 1's transform; then hand edits:
  - `agent-loop.yaml`: `call: start_child {workflow: guard, …}` →
    `call: start_guard {calls, …}`; `call: start_child {workflow: agent-loop, …}`
    → `call: start_worker {call, i, index, memory}`; the `tool` call →
    `classify` then `switch` → `tool_once` | `tool_repeat`, with
    `catch: { errors: { with: { type: urn:ouro:errors:call.unknown } } }`
    around `tool_once` that records "the write may have happened" in the
    transcript (invariant 4); `metadata.ouro.breaker: off`.
  - `signal.yaml`: `primary_loop`, `recovery_loop`, `run_saved_workflow`,
    `route_signal` return child refs; each is followed by `listen` on
    `done:<id>` and `child_outcome`; `metadata.ouro.breaker: off`.
  - `guard.yaml`: unchanged calls; `metadata.ouro.{concurrency,hold,breaker}`.
  - `recovery` loop variant: `breaker: off`.
- Skills and tool descriptions that teach the DSL (`workflow-author`,
  `flows.rs` authoring tool texts) switch to `metadata.ouro`.

### C. Runtime wiring

- `main.rs` builds `Runtime::new(sqlite adapter on agent.db, registry,
  Config { max_concurrency, max_function_concurrency, max_concurrent_runs,
  limits.max_jumps, breaker_threshold: 3, metadata }, clock, tracer)` with
  documented formulas: a primary loop with up to 16 parallel workers and a
  guard per round must never starve (e.g. drives ≥ 2 + 16 + 16, function slots
  ≥ drives × 2, max_jumps ≥ 100 rounds × (1 + workers)); the numbers and the
  record-size estimate (transcript per loop record, 7-day sweep) go into
  `docs/operations.md`.
- Built-ins saved and activated at boot (idempotent: save returns the same
  version for the same digest; activate only when the active digest differs).
- The engine worker runs in a restart loop with backoff (overview decision 12).
- Host data per run: `{ preset, confirm, conversation, session, loop, skills,
  allow_skill_writes, system }` for loops; `{ signal }` for `signal`; guard and
  worker children inherit what the starting function gives them (workers get
  the parent's `confirm` — invariant 5).

### D. Confirmations and events

- Button presses: the supervisor's `telegram_button` signal becomes
  `Runtime::deliver(event{source:"telegram", id:<callback query id>,
  type:<token>, data}, Some(<guard run id>))` — targeted (the token → guard run
  mapping is in `confirmations`), so a press before the guard listens is
  mailboxed, not lost. A press of the other token of the same card after the
  first was consumed is answered to the user as "already answered" (the handler
  checks the card row) instead of silently mailboxed.
- Verification (invariant 1): `resolve_review` reads the consumed event from
  `Runtime::records(guard_run)` at the listen task's key and checks run, call
  key, chat, message and thread against the card row; then the existing
  "spent once" update. No SQL against engine tables.
- `pending_confirmations` and the dispatcher's card cleanup read
  `confirmations` plus `Runtime::get(run)` liveness instead of the
  `workflow_runs` join.

### E. Tracing split and reconciliation

- `agent_turn`, `tool_*`, `confirm_request` and decision/compose handlers
  write their generation/tool spans with full payloads through the agent
  Tracer, parented under the engine call span (`FunctionContext::span`).
- An agent observer maps lifecycle events (run started/resumed/ended, task
  entered/exited, switch, retry, jump) to Langfuse spans and mirror rows.
- `run_trace(run_id, trace_id, session_id, parent_span_id, recorded)` replaces
  `trace_anchor`/`trace_recorded`; the signal run's trace is the root, children
  nest under their parent's span.
- A reconciler (boot + every minute) walks `Control::history` by end-time
  watermark (stored in `memory`) and marks `run_trace.recorded`, then the
  activity journal and judge readiness read `run_trace` only.
- Migration `00NN_engine_runs.sql`: create `run_trace`, `tool_snapshot`; drop
  `workflow_runs`, `workflow_records`, `workflow_events`, `workflow_awaits`,
  `workflow_listeners` and the anchor columns' dependents (applied migrations
  untouched). Pending confirmation cards are edited to "expired" once at first
  start (dispatcher maintenance function).

## Contract and model

Engine contract unchanged. Agent invariants 1–6 (overview) each get a test that
runs on the engine (listed under Tests).

## Failure modes

- Crash between `get_next_signal` and start: re-offer/join by signal id.
- Crash inside a loop: the loop child resumes on the engine; an AMO tool call in
  flight becomes unknown and is reported to the model, never resent.
- MCP down at boot: registry from the snapshot; tools answer `not_executed`
  until reconnect; runs that need them fail their call, not the run (the loop
  sees an error result).
- Provider outage: loops fault; built-ins never trip (`breaker: off`); the
  signal's recovery report still runs.
- A parent waiting on children with too few permits: prevented by the capacity
  formulas; a test runs a primary with 16 workers and a guard concurrently.
- Engine event loss (observer): only spans; durable state comes from the
  reconciler.

## Tests

Existing suites keep passing on the engine (`flows.rs`, `workflow_kinds.rs`,
`agent_loop.rs`, `loop_variants.rs`, `loop_durability.rs`, `durable*.rs`
rewritten against the engine, `supervisor*.rs`, `confirmation_properties.rs`,
`tool_results.rs`, `tracing.rs`, `activity_*.rs`). New, each failing first
where it can:

1. Invariant 1: a press with a wrong chat/message/thread, a press for another
   run, and a forged YAML output are not honoured; a press before the guard
   listens is honoured once.
2. Invariant 2: a model write without a guard round is refused.
3. Invariant 3: a saved workflow's `run: workflow agent-loop` (or built-in
   `start_child`) child has no host data → `agent_turn` refuses; a dispatcher
   function called from a non-dispatcher definition refuses.
4. Invariant 4: crash during an AMO tool call → the call is unknown, the model
   is told, the tool is not called again (fake MCP counts calls).
5. Invariant 5: a worker's confirmation target equals its parent's.
6. Invariant 6: a card send with an unknown outcome is not resent.
7. Capacity: primary + 16 workers + guard complete with the configured limits.
8. Built-in breaker off: three faulted signals in a row, the fourth is admitted.
9. Signal idempotency: the same signal id started twice is one run.
10. Registry superset: boot with MCP down, a run calling a known tool gets
    `not_executed`, not a fault.
11. Reconciler: a run whose `run_ended` event was dropped is still marked
    recorded.

## Rollout

On `feat/engine` only; no deploy (overview decision 13). `docs/operations.md`
capacity numbers; `CLAUDE.md` "How a signal turns into action" updated.

## Open questions

None for the maintainer; decisions taken here are logged in LOG.md.
