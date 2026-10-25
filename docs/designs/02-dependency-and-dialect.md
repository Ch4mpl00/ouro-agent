# 02. Step 1: engine dependency and the `metadata.ouro` dialect

Status: approved (small step, no goldfish round) · Overview: [01](01-engine-migration.md) step 1

## Problem

Before runs move to the engine, the workspace must build with the engine
crates beside `owf`, and the built-in workflows must be shown to mean the same
thing under the engine's dialect: the engine reads extensions only under
`metadata.ouro` and silently ignores flat keys (review H1), while the running
`owf` reads the flat keys.

## Design

- Add `ows`, `ouro-durable`, `ouro-runtime` to `crates/agent` (and the
  workspace) as git dependencies on `https://github.com/Ch4mpl00/ouro`, pinned
  with `rev = "<engine main at the time>"`. They must coexist with `owf`
  (different crate names; shared deps such as `rusqlite 0.40` must resolve to
  one version — check `cargo tree -d`).
- A test module `crates/agent/tests/engine_dialect.rs`:
  - a function `to_engine_dialect(yaml) -> String` that moves the flat task
    keys `concurrency`, `hold`, `delivery` and document keys `limits`, `at`,
    `timezone`, `catchup`, `overlap` under `ouro` (a YAML value transform, not
    text replacement);
  - for each built-in (`agent-loop.yaml`, `signal.yaml`, `guard.yaml` and the
    test fixture loops): parse the original with `owf` and the converted text
    with `ows`, validate both, and assert the effective extensions match task
    by task (concurrency, hold, delivery) — and that the converted text parsed
    *without* conversion by `ows` would lose them (documents the hazard);
  - parse the converted built-ins with an accept-all `ows` catalog.
- No runtime behaviour changes; nothing switches to the engine yet. The real
  file conversion and the skills text (`workflow-author`, tool descriptions)
  move to `metadata.ouro` in step 2, when `owf` stops running them.

## Tests

The module above; the full agent suite stays green (`scripts/test.py`).

## Rollout

Nothing deploys. `Cargo.lock` changes.
