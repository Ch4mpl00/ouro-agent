# Decisions log

Decisions taken without the maintainer (e.g. during an overnight run), each
reversible, with the reason. The maintainer reviews this file and either keeps
or reverses each entry.

## 2026-10-25 (night run: engine migration)

- **Integration branch, no deploy.** The migration lands on `feat/engine`
  step by step with green suites; prod is not deployed until the maintainer
  decides. Reason: a runtime swap of the live agent is hard to roll back
  unattended. (Design 01, decision 13.)
- **Pin the engine to a revision of `main`** until `v0.2.0` is tagged. Reason:
  `v0.1.0` predates everything the migration needs.
- **Tracing split:** handlers keep their own generation/tool spans with full
  payloads; observers carry lifecycle only; derived state reconciled from the
  store. Reason: observer events are capped (16 KiB) and at-most-once.
  (Design 01, decision 14; review H4.)
- **Saved workflow sources kept in `workflow_meta`.** Reason: engine
  store-version bumps re-create the store. (Decision 15; review M7.)
- **Gap workarounds instead of engine changes** for cooperative cancellation,
  dry runs, run-end events and missing functions on resume. (Decision 16.)
- **`emit` refused in agent-saved workflows; reserved built-in names can't be
  saved.** (Decision 17; review M3.)
- **Built-ins with `metadata.ouro.breaker: off`.** Maintainer approved the
  engine feature; applying it to all four built-ins is this session's choice.
- **Step order restructured** per review H5: dependency + dialect, then one
  core step (runs + registry + observers + built-ins), then versions/breaker,
  sandbox, schedules, `owf` removal.
- **Engine design 03 (host data), resolved review findings without the
  maintainer:** host data stored as `admission.runtime["host"]` (no struct
  change, no state bump); the control API redacts it except in `inspect`;
  host-started children use a `hostchild:` id prefix keyed by the call's
  idempotency key; child-starting functions must be at-least-once; refusals
  map to `not_executed`; mismatch checked in the shared store engine; the
  "workflow child has no host data" rule is pinned by conformance tests, not
  Quint; scheduled runs carry no host data.
- **Engine design 04 (breaker policy):** three-state `Breaker` with a serde
  default (no definitions layout bump); queued refusals stop pinning a
  threshold (resolved at promotion); `off` adds no new reporting.
- **Agent design 03 (runs on the engine):** one signal at a time kept; run id
  `signal:<mcp signal id>` as the idempotency key; loop-running host functions
  become child starters joined by `listen`/`child_outcome`; function access
  checked in handlers by definition and host data; a second press of a card is
  answered "already answered"; capacity formulas documented in operations.
- **Agent design 03, round-1 review resolved without the maintainer:**
  supervisor waits until the signal's tree is quiescent (not until it ends);
  host data small + content-addressed `evidence` table; "first commit wins"
  joins for host children (engine design 03 amended); dispatcher state from
  the run's own journal; jump budget as a wide backstop, `max_iterations`
  binding; `settle_child` host function for outcome mapping; button events
  keyed `telegram_button:<signal id>`, undelivered presses fall through to the
  agent with a reason (no runtime-authored reply); saved-workflow features
  disabled during step 2; engine worker starts after MCP.
- **Deferred to the maintainer:** the signal-loss window between
  `get_next_signal` and admission stays as today; an MCP lease/ack protocol is
  filed as 63 (needs a `.feature` scenario first).
- **Agent design 03, round-2 review resolved:** evidence linked per run
  (`run_evidence`), swept by links not age; the recovery report runs as an
  independent root outside the signal tree (budget can't block it); loop
  routing uses `if:` (no jump); model sees short worker handles; capacity
  sized for two concurrent trees; `engine.` memory-key prefix reserved. Engine
  design 03 gains `FunctionContext::child_id(key)` (asked of its implementer).

## 2026-10-26 (night run: step 2, runs on the engine)

Decisions taken while implementing design 03 that the design does not
cover, each reversible:

- **AgentDb writes from engine handlers go to the blocking pool**
  (`AgentDb::write`). The engine's SQLite adapter shares agent.db and holds
  its write lock across the awaits of a commit, and the worker drives every
  run in one task: a synchronous write that waited for that lock inside a
  handler (or on a current-thread runtime) blocked the very task that would
  release it ("database is locked" after 5 s). Engine tests run on the
  multi-thread runtime, as the binary does. Alternative kept for the
  maintainer: the engine store in a file of its own (overview decision 3
  says agent.db).
- **The signal takes its primary's `waiting:` event** before listening for
  `done:` (`signal.yaml`): an owned child's waiting notice that the parent
  never consumes keeps the parent due, and it busy-loops. Built-in listens
  that switch on an event's type read envelopes (`read: envelope`); the
  engine's default is the event data.
- **`run_saved_workflow` and `route_signal` start nothing in step 2**
  (decision 9): they answer handover / not routed directly, without the
  listen / child_outcome / settle_child sequence a starter gets. That
  sequence is used for the primary.
- **Recovery waits:** the supervisor's quiescence predicate covers the
  signal's tree and its `recovery:<id>` root, so a report is finished (or
  waiting) before the next signal starts.
- **Loop host data also carries `guard_request`** (a digest) for every loop
  kind, not only workers: the primary's guard task is the signal's content.
- **Live loops** are ended on `run_ended` only for loops the engine built
  for a run (`loop_for`); a loop a caller started (`start_loop`, tests and
  evals) is the caller's. A run's live trace scope ends when its execution
  stops; a later execution opens a continuation segment.
- **`AgentLoop::send` is kept** (tests, evals) and runs the loop as an
  engine run of kind `loop`; repeated sends use run ids `<id>#n`.
- **The evals workbench keeps an `owf` adapter** (`flows::standalone_host`)
  over the engine's workflow steps until step 6; it is not the agent's
  running path.
- **The worker's poll interval is 50 ms** (engine default 100 ms): a child
  starts at the next tick and a signal is a chain of three or four starts.
- **Suites that need saved workflows, loop variants, signal overrides or
  the sandbox are parked** in `crates/*/tests/pending/` (not compiled) with
  a pointer to the step that restores them; the old durability and
  confirmation property suites (`durable_loop`, `loop_durability`,
  `confirmation_properties`) are parked too and covered by the design's
  tests in `tests/engine_runs.rs`, to be rewritten on the engine.
- **MCP's tool list is read once at boot** (`Engine::refresh_tools` after
  the first successful `tools/list`); the registry is not swapped on a
  reconnect, as MCP's tools are fixed at boot (mcp.rs).
- **Engine pinned at 9aca1de** (design 04 merged): the built-ins carry
  `metadata.ouro.breaker: off`; the interim lifted threshold is gone.
