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

## 2026-10-26 (step 2b: the suite green on the engine)

Decisions and findings while finishing step 2, each reversible:

- **Engine pinned at the `v0.2.1` tag** (was rev 9aca1de): v0.2.0 hashes
  child ids per attempt (`hostchild:<32hex>`; the agent only ever uses
  `FunctionContext::child_id`), v0.2.1 wakes a parent joined on `done:`
  only for an exact current listen.
- **The engine's store is `engine.db` beside agent.db, not inside it**
  (deviates from design 01 decision 3, which the maintainer took; flagged
  for review). Evidence: the SQLite adapter opens `BEGIN IMMEDIATE` for
  every operation, reads included, back to back on its one connection, and a
  write of the agent's on another connection to the same file starved behind
  it: 5–30 s waits and "database is locked" faults of `start_guard` and
  `tool_once` under the suite's load; with two files the same test runs in
  4 s with no slow write. One directory, one volume; two files to back up.
  Reversible in one place (`runs::engine_store_path`). Kept besides, as
  hygiene: AgentDb reads go to a reader connection of their own, handler
  writes to the blocking pool, and the lifecycle drain and the reconciler
  run in tasks of their own (their synchronous writes never sit in the
  worker's task, which polls every run's calls).
- **A handler's panic is its call's failure** (`internal error: …`), not the
  end of the engine worker (it polls all runs in one task; a panic there
  stopped the engine for good).
- **Closing a loop the host drives (`AgentLoop::close`) cancels its run and
  tree, joins its handlers, then ends its trace**; a shutdown cancels
  nothing (runs resume at the next start). A cancelled run's span is an
  aborted (error) observation. A child's span ends, and its loop closes,
  when its parent reads its outcome (`Engine::ended_child`), not later in
  the lifecycle drain.
- **The reconciler writes a root's outcome into its trace only for a run
  whose creator may be gone** (a signal that suspended): not for a recovery
  report (its span is in the signal's trace) nor a loop the host runs itself.
- **Quiescence from two agreeing reads of the run tree** (double collect,
  the engine's clock): a parent that started a child and suspended between
  the tree's listing and its own read looked quiescent with the child
  unlisted, and the supervisor answered "resumes" for a running signal.
- **Live trace scopes end at a shutdown** (and when the supervisor stops),
  so a call cut in flight is written as stopped before the tracer closes.
- **The guard round is pinned to the built-in definition**: `start_guard`
  starts the version whose digest is the built-in's, and a round's verdicts
  count only from a guard child running that text, whatever version of
  `guard` an operator activated.
- **`create_engine_on(deps, config, clock)`**: the runtime's clock as a seam
  (tests move it past a 24-hour confirmation).
- **Within a round, calls start in any order** (each is an engine call):
  the round properties compare a round's dispatches as a multiset; reply
  order stays the round's.
- **Parked suites, triaged**: `confirmation_properties`, `loop_durability`,
  most of `durable_loop`, the supervisor's confirmation scenarios and a
  workflow's own steps (from `flows`) are ported to the engine
  (`tests/confirmations.rs`, `loop_durability.rs`, `durable_loop.rs`,
  `supervisor.rs`, `workflow_steps.rs`); what needs saved or activated
  versions, routes or loop variants (step 3) or the sandbox (step 4) stays in
  `tests/pending/` with a header naming the step. Dropped as tests of the old
  store or of behaviour design 03 changed: in-process run history,
  `a_restart_after_guard_rechecks_*` (a round's verdict is its guard
  child's stored outcome, durable across a restart, not reviewed again),
  pinned-definition reopen and fast-child timing (engine conformance).
- **Performance finding for the engine**: a round of many workers is
  CPU-bound in the engine (each holding listen polls the store; every task
  record carries the loop's context); 16 one-turn workers take ~30 s in a
  debug build. Recorded in docs/operations.md; not changed here.

## 2026-10-26 (step 3: design 04, versions and breaker)

- **Maintainer:** `engine.db` stays beside agent.db for now; the engine's
  SQLite adapter gets read transactions (`BEGIN DEFERRED`) separately (ouro
  `fix/sqlite-read-tx`).
- **Design 04 decisions 1–8** (elephant, reversible), after goldfish round 1
  (3 high, 10 medium, 9 low, all resolved in the document):
  only `guard` reserved, `agent-loop`/`signal` overridable again (#55,
  narrowing design 01 decision 17); the engine is the version store and
  `workflow_meta` keeps only note/tried/builtin by `(name, digest)`; no YAML
  copy, no automatic re-seeding (revises design 01 decision 15); agent
  versions can't declare a breaker, built-in digests can always be
  re-activated; one refusal table for fallbacks; an operator pause also stops
  the built-in fallback (for the maintainer to confirm); the fallback record
  and a line in the run's system parts; same-text saves reuse the version,
  20 agent versions per name at most.
- **Goldfish round 2** (2 high, 5 medium, 8 low): resolved in the document
  (R2-n). The three maintainer calls taken reversibly: an operator pause is
  never lifted by the agent and falls back only to another name; the
  20-version cap counts only never-activated versions; `call: start_child`
  stays allowed, `run: workflow` and any `schedule` are refused in agent
  versions until step 5.
