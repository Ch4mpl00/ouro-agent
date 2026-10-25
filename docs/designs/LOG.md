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
