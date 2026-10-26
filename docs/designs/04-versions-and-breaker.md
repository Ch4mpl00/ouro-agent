# 04. Step 3: the agent's workflow versions and the breaker on the engine

Status: draft · Overview: [01](01-engine-migration.md) step 3 · Epic: #62
Decisions log: [LOG.md](LOG.md)

## Problem

After step 2 every run is an engine run, but the agent cannot author
workflows any more: `save_workflow`/`activate_workflow` answer "unavailable
until the versions step", `route_signal` never routes, `run_saved_workflow`
always hands over, the loop catalog lists only built-ins, and the suites for
routes, loop variants, workflow kinds and the breaker are parked
(`crates/agent/tests/pending/{loop_variants,workflow_kinds,flows,durable_loop}.rs`).

On `main` these features exist on the agent's own store: a `workflows`
table (draft → active → retired, `tried` gate), a memory-KV breaker per name
(three faults in a row → the built-in text, or `agent-loop` for a name
without one), active versions that replace built-ins by name (#55: the agent
may override `agent-loop` and `signal`), routes in `metadata.route`, loop
variants in `metadata.loop`, and scheduled tasks naming a saved workflow.
The engine now owns versions (`save`, `activate_as`, `definitions`), the
breaker (per definition, by content digest, `DefinitionTripped` on start),
and pinned starts (`start_with { version }`). This step puts the agent's
authoring on them and deletes the agent's copies.

Scenario: the agent saves `tg-loop` (a copy of `agent-loop` with a route on
`source: telegram`), a dry run marks it tried (step 4), the user confirms,
it is activated. Telegram signals now run `tg-loop`. It faults three times in
a row: the engine trips it; the next Telegram signal is refused at the start
of `tg-loop` and runs the built-in `agent-loop` instead, in the same trace,
saying so; `list_workflows` shows `tg-loop` tripped; activating a new version
clears it.

## Decisions

Maintainer (earlier, binding here):

- #55: built-in workflows are overridable by the agent ("the agent's freedom
  and generality"); behaviour lives in YAML, not Rust.
- Design 01, 7: on a trip of an agent version the agent runs the built-in
  version for that run (`start_with` pinned); the tripped version stays
  active (digest rule). The engine never picks a version; the agent does.
- Design 01, 10: built-ins carry `metadata.ouro.breaker: off`.
- Design 01, 15: saved sources also live in an agent table (`workflow_meta`)
  so an engine store re-creation can be re-seeded.
- The engine's store is `engine.db` beside agent.db (step 2b, LOG.md;
  pending the maintainer's review): `workflow_meta` lives in agent.db, so
  agent rows and engine rows are never in one transaction.

Elephant (this document, reversible, logged in LOG.md):

1. **Decision 17 of design 01 is narrowed:** only `guard` is reserved (its
   round is pinned to the built-in digest, step 2b). The agent may save and
   activate versions of `agent-loop` and `signal` again, as on `main` (#55).
   `recovery` is not a definition (a host kind that runs `agent-loop`,
   `recovery_loop` in functions.rs), so it needs no rule. `emit` stays
   refused in agent-saved workflows.
2. **The engine is the version store.** A version's number, YAML, digest,
   activation history, active version, breaker state and trip are the
   engine's. `workflow_meta` holds only what the engine lacks — note,
   `tried`, `builtin` (which digests are built-in texts), created time — and
   a copy of the YAML for re-seeding. No status column: status is derived.
3. **The breaker of an agent version is the runtime's.** An agent-saved
   version may not declare `metadata.ouro.breaker` (refused at save and at
   activation): its threshold is the engine's default (`Config::breaker_threshold`,
   3 — the old `LOOP_BREAKER`). Otherwise a broken override of `agent-loop`
   with `breaker: off` would fault every signal forever.
4. **One fallback rule, at every start of a name the agent may have
   overridden:** if the engine refuses the start of the active version
   (`DefinitionTripped`, or the start-time registry check — a
   `MissingFunction` refusal, `execution.rs` `refusal()`: a tool the version
   calls went away), the same run id is started pinned at the
   built-in version of that name; a name without a built-in falls back to
   `agent-loop`'s built-in version (routes, loop variants, workers), or hands
   over to the agent loop (a scheduled task's workflow, as `main` did). A
   fault *during* a run is not a refusal: it ends as a fault (the signal's
   catch reports it) and counts toward the breaker.
5. **The fallback informs.** The run's span records
   `fallback: { from: {name, version}, to: {name, version}, reason }`; the
   fallback run's first-turn context gets one line (`signal_session` adds
   `fallback` to the session context: "workflow X v3 was set aside after 3
   faults in a row; the built-in one runs") so the agent can tell the user
   or fix its version. No automatic notice signal in this step (step 5
   decides trip notices for scheduled definitions and may generalise it).
6. **Activation needs `tried`**, as on `main`. Only `try_workflow` (step 4)
   sets it; until step 4 lands, agent activation stays refused in practice.
   Tests set it through a test seam (`WorkflowMeta::mark_tried`).

## Non-goals

- The sandbox and `try_workflow` (step 4). Schedules and trip notices for
  scheduled definitions (step 5). Removing `crates/owf` (step 6).
- Migrating saved workflows from `main` (design 01, decision 2: none).
- Engine changes. Everything here uses the engine API at `v0.2.1`.

## Design

### Store: `workflow_meta` (agent.db, migration `0016_workflow_meta.sql`)

```sql
CREATE TABLE workflow_meta (
    name TEXT NOT NULL,
    version INTEGER NOT NULL,      -- the engine's version number
    digest TEXT NOT NULL,          -- ouro_durable::digest_of(yaml)
    definition TEXT NOT NULL,      -- the YAML, for re-seeding
    note TEXT,
    builtin INTEGER NOT NULL DEFAULT 0,  -- this digest is (was) a built-in text
    created_ms INTEGER NOT NULL,
    tried_ms INTEGER,
    PRIMARY KEY (name, version)
);
```

- `flows.rs` §1 gets a small store: `WorkflowMeta` trait (`record`,
  `mark_tried`, `get`, `list_names`) with `AgentDb` and in-memory impls, the
  way `main` had `WorkflowStore`.
- `install_builtins` (runs.rs §7) moves its "known built-in digests" from
  the memory KV `engine.builtin_digests.<name>` to `workflow_meta.builtin`;
  the KV keys are no longer read or written. Its rule is unchanged: an active
  version that is not a known built-in digest (the agent's or an operator's)
  is never replaced at boot.
- Re-seeding: when `runtime.definitions(name)` has no versions but
  `workflow_meta` has rows (a re-created engine store), boot saves them back
  in version order and activates the newest one that was active, recording the
  new numbers. Only the active choice is restored, not trips.

Status is derived: `active` = engine's `active`; `retired` = in the engine's
`activations` but not active; `draft` = never activated. Plus `tripped`
(`Definitions::tripped` with `breaker == true`) and `paused` (a manual trip).

### Saving (`save_workflow { name, definition, note? }`)

As on `main` (`flows.rs` §5 there: `check`, `document.name == name`,
`require_guarded_turns` — for the engine dialect: `agent_turn` requires a
`call: start_guard` round —, `require_metadata`, warnings, the reply's
`calls` and `next`), plus: `name != "guard"`; no `emit`; no
`metadata.ouro.breaker`. Then `runtime.save(name, yaml)` and
`workflow_meta.record(name, version, digest, yaml, note)`. Saving the same
text twice gives two engine versions with one digest; both are recorded.

A crash between the engine save and the meta row leaves an engine version
with no meta row: it is a draft with no note that is not tried, so it cannot
be activated; the agent saves again. `list_workflows` shows it (from the
engine).

### Activating (`activate_workflow { name, version }`)

Checks on `main` (re-`check` against the current tools, guarded turns,
metadata, route priority conflicts) plus: `name != "guard"`, no
`metadata.ouro.breaker`, `tried`. Then
`runtime.activate_as(name, version, "agent", note)`. The engine clears a
trip on activation; activating the tripped version again is allowed (new
epoch). Reply texts as on `main`, with "three faults in a row put the
built-in one back" (or "hand the task to the agent loop").

### Reading (`list_workflows`, `read_workflow`)

Engine names come from `Control::definitions` (paged; names in
`workflow_meta` ∪ the built-ins are enough and avoid paging: use them).
`list_workflows`: per name the latest version, its derived status, the
active version, `tripped`/`paused`, note, created time; built-ins included
with `builtin: true` for their built-in version. `read_workflow { name,
version? }`: the version's YAML from the engine, its status and note; `guard`
reads its built-in text.

### Which version a start runs (one helper)

`runs::start_named(engine, StartNamed { id, name, input, host, kind })`
replaces the direct `start_with`/`start_child` calls for every name the agent
may override: the signal (`supervisor.rs`), `primary_loop`, workers and
`recovery_loop` (`functions.rs`), `run_saved_workflow`, routed loops. It:

1. starts the active version (`version: None`);
2. on refusal — `DefinitionTripped`, `ChildRefusal::Tripped`, or the
   start-time registry/validation refusal — starts the same id pinned at the
   fallback: the built-in version of `name` (digest lookup as `start_guard`
   does), else for `kind = Loop | Route` the built-in `agent-loop`, else
   (`kind = Scheduled`) returns `Handover { reason }`;
3. returns which version ran and the fallback record (decision 5).

An id that already exists joins as today (`AdmissionMismatch` stays fatal).
`guard` keeps its own pinned start and never falls back.

### Routes, loop variants, scheduled workflows

- `route_signal` (functions.rs) gets `main`'s `signal_route` over the active
  versions of the names in `workflow_meta` (built-ins declare no routes):
  `metadata.route.when` on `source` and top-level payload fields, most
  specific wins, ties refused at activation (`require_route_priority`). A
  route whose start falls back runs the built-in `agent-loop` with the
  route's admitted parameters; the routed path's own context gathering is
  the fallback loop's (it is the default path).
- The loop catalog (`flows::loop_catalog`) lists built-ins plus active agent
  versions with `metadata.loop.description`; a worker names one, started
  through `start_named(kind: Loop)`.
- `run_saved_workflow { name }`: `start_named(kind: Scheduled)`; missing
  active version or a refusal → `{handover: true}` (the signal continues to
  the agent loop, as `main`). A run of it that faulted after any at-most-once
  call was dispatched goes to recovery instead of the handover (the run's
  call journal, `Runtime::calls`, has an `AtMostOnce` entry), as `main`.
- `metadata.agent`/`metadata.route` preset and confirm: admitted as host
  data by the launch (`agent_start` on `main`), never read from YAML by a
  host function (agent invariant 3).

### Deleted

The memory-KV breaker (`<name>.breaker` keys are not read; old rows are
left alone), `engine.builtin_digests.*` KV use, `unavailable(.., "versions")`
for the four tools, the `route_signal` stub.

## Contract and model

No engine contract change. Agent invariants of design 01 hold; this step
adds:

- **A7.** A start of an overridable name never fails because of the agent's
  version: a refusal of the active version runs the fallback (or hands over),
  in the same run id and trace.
- **A8.** No agent version can opt out of the breaker.
- **A9.** `guard` is never saved, activated or started at a version other
  than the built-in digest by the agent.

## Failure modes

- **Crash between engine save and meta row:** see Saving.
- **Crash between activation and reply:** the engine activated; the reply is
  lost; `list_workflows` shows it active. No meta write is needed.
- **Fallback start crash:** the refused start stored nothing; the retried
  signal starts again, is refused again, falls back again (idempotent by id).
- **Tool removed after activation:** start refused by the registry check →
  fallback (decision 4), with reason in the fallback record.
- **Concurrent activations** (two workers): the engine serialises them; the
  later one wins, as on `main`.
- **Breaker counting:** only runs admitted at the active version in the
  current epoch count (engine). Fallback runs are of another digest and never
  count against the agent's version.
- **Re-seeding** after a store loss restores versions and the active choice,
  not running runs (decision 2 of design 01).

## Tests

Restore from `tests/pending/` (moving each back into `tests/`, adapted to
the engine API, nothing weakened): `loop_variants.rs`, `workflow_kinds.rs`,
the versions part of `flows.rs` and of `durable_loop.rs`. New, each failing
before its code:

1. Save → draft; activate refused until tried; tried → active; previous
   active → retired (derived from the engine).
2. `guard` refused for save/activate; `emit` refused; `metadata.ouro.breaker`
   refused at save and at activation.
3. An agent version of `agent-loop` that faults three times in a row trips;
   the next signal runs the built-in `agent-loop` pinned, in the same run id
   and trace, with the `fallback` record on the span and the line in the
   session context; the tripped version stays active; activating a new
   version clears the trip and runs it.
4. Same for `signal` and for a route (route trip → default path with the
   built-in loop) and for a worker's loop variant (→ built-in `agent-loop`).
5. A scheduled saved workflow: missing / tripped → handover to the agent
   loop; faulted after an at-most-once call → recovery, not handover.
6. A version calling a tool that the snapshot no longer has: start refused →
   fallback with reason.
7. A saved copy with identical text to a tripped version is still refused
   (digest) and falls back.
8. `install_builtins` keeps an agent-activated `agent-loop` across a restart
   and replaces a known built-in digest with a new built-in text; known
   digests come from `workflow_meta`, not the memory KV.
9. Re-seeding: an empty engine store with `workflow_meta` rows restores the
   versions and the active one.
10. Route priority conflict refused at activation; most specific route wins.
11. `list_workflows`/`read_workflow` show derived status, tripped/paused and
    built-ins.

Full suite green twice (`scripts/test.py`), fmt, clippy.

## Rollout

Migration `0016_workflow_meta.sql`. `CLAUDE.md` text about overrides and the
breaker stays true; update its "three faults of a name in a row fall back"
sentence to name the engine's breaker and the fallback record; skills
`workflow-author` mention of `metadata.ouro.breaker` (agent versions can't
set it). LOG.md entries for decisions 1–6. Nothing deploys.

## Open questions

None blocking. For the maintainer, reversible: decision 1 (overrides of
`agent-loop`/`signal` restored per #55, narrowing design 01's
decision 17).
