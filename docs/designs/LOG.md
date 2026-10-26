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
- **Engine design 04 implementation deviation accepted:** queued refusals keep
  the host default threshold (needed to resolve `Inherit` at promotion); the
  active version's policy still decides; no `Queued` layout bump. Alternative
  (default in the store's `Policy`) left for later.
- **Engine design 03 implementation choices accepted:** first-commit-wins for
  every child join at the store level (roots keep the full comparison);
  `dry_run_with` added beside `dry_run`; no state version bump.
- **Engine pre-tag drift review (2026-10-26):** child ids become per-attempt
  and hashed (`child:`/`hostchild:` + 32 hex) — a DSL retry starts its own
  child, a crash re-dispatch joins; link conflicts are typed; host-data size
  checked after the join; CHANGELOG re-cut as 0.2.0 (the #22/#20/#7 entries
  moved under 0.1.0); crate versions 0.2.0.

## Status at the end of the night run (usage limit reached)

- Engine: designs 01–04 merged on ouro main (9aca1de). Pre-tag fixes (hashed
  per-attempt child ids, CHANGELOG 0.1.0/0.2.0, crates 0.2.0) are on branch
  `fix/child-ids` (worktree ouro.worktrees/childids), green per its agent
  (315 tests, Postgres on a native server) but NOT yet verified by the elephant
  nor merged; `v0.2.0` not tagged.
- Open engine decision: a parent waiting on a waiting child is re-driven on
  every worker pass (pending_wanted keeps waking it); proposed fix: ignore
  `waiting:` notices no explicit listen asked for.
- Docker Desktop's VM filesystem went read-only (disk was 97% full); restart
  attempted, daemon not answering yet — needs a manual restart/check.
- Agent migration: step 1 done; step 2 executor stopped at the limit — see
  `feat/engine` commits for how far phases A–F got; continue from the design
  `03-runs-on-the-engine.md` and the executor report if present
  (scratchpad agent-step2-report.md).

## 2026-10-26 (continued)

- **ouro v0.2.0 tagged** (137767a) after verifying `fix/child-ids` on a working
  Docker (315 tests). Docker Desktop needed a full restart (VM filesystem had
  gone read-only); `mcp-tools-test-pg` started again by hand.
- **Engine design 05 (waiting parents stay asleep):** one `wakes()` rule for
  all wake sites; lifecycle notices wake only an exact current listen (not `*`,
  not internal `child:*` listeners); `notify` gated; pinned by conformance only
  (Quint models no wake condition). Target release 0.2.1.
- **Step 2 continuation** started with a fresh executor (the previous one was
  stopped at 68 passed / 31 failed).
