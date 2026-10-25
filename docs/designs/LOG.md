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
