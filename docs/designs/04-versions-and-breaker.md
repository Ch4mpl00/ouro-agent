# 04. Step 3: the agent's workflow versions and the breaker on the engine

Status: reviewed (goldfish round 1, findings resolved below as R-n) ·
Overview: [01](01-engine-migration.md) step 3 · Epic: #62 · Decisions log: [LOG.md](LOG.md)

## Problem

After step 2 every run is an engine run, but the agent cannot author
workflows: `save_workflow`/`activate_workflow` answer "unavailable until the
versions step", `route_signal` never routes, `run_saved_workflow` always
hands over, loop selection is ignored, the loop catalog lists only
built-ins, and the suites for routes, loop variants, workflow kinds, signal
overrides and the breaker are parked (`crates/agent/tests/pending/`).

On `main` these features run on the agent's own store: a `workflows` table
(draft → active → retired, `tried` gate), a memory-KV breaker per name (three
faults in a row → the built-in text, or `agent-loop` for a name without
one), active versions that replace built-ins by name (#55), routes
(`metadata.route`), loop variants (`metadata.loop`) chosen four ways, and
scheduled tasks naming a saved workflow, all launched with host-vouched
parameters (`supervisor.rs` `admission(.., Launch)`). The engine now owns
versions (`save`, `activate_as`, `definitions`), the breaker (per definition,
by content digest, refusal at start) and pinned starts. This step puts the
agent's authoring on them and deletes the agent's copies.

Scenario: the agent saves `tg-loop` (a copy of `agent-loop` routed on
`source: telegram`), a dry run marks it tried (step 4), the user confirms,
it is activated; Telegram signals run it. It faults three times in a row; the
engine trips it. The next Telegram signal's route start is refused, so
`route_signal` answers "not routed" with a fallback record and the signal
takes the default path, whose first turn says `tg-loop` v3 was set aside.
`list_workflows` shows it tripped; activating a new version clears it.

## Decisions

Maintainer (binding):

- #55: built-in workflows are overridable by the agent; behaviour lives in
  YAML, not Rust.
- Design 01, 7: on a trip of an agent version the agent runs the built-in
  version for that run (pinned start); the tripped version stays active. The
  engine never picks a version.
- Design 01, 10: built-ins carry `metadata.ouro.breaker: off`.
- Engine store in `engine.db` beside agent.db (step 2b; the maintainer
  accepted it and asked for the engine's SQLite read fix separately).

Elephant (reversible, logged in LOG.md):

1. **Only `guard` is reserved** (narrows design 01 decision 17, restores
   #55): the agent may save and activate versions of `agent-loop` and
   `signal`. `recovery` is not a definition (`recovery_loop` runs
   `agent-loop`), so no rule. `signal` may not declare a route (as `main`).
2. **The engine is the version store.** Number, YAML, digest, activation
   history, active version, breaker state and trip are the engine's. The
   agent table `workflow_meta` holds only what the engine lacks, keyed by
   content: `(name, digest) → note, tried_ms, builtin, created_ms`. Status is
   derived. **No YAML copy and no automatic re-seeding** (R-M5; design 01
   decision 15 was the elephant's and is revised): the engine store migrates
   in place; operations back up `engine.db` with `agent.db`.
3. **The breaker of an agent version is the runtime's**: an agent-saved
   version may not declare `metadata.ouro.breaker` (`Workflow::breaker !=
   Inherit`), so it trips at the engine default (3). Activating a version
   whose digest is a known built-in is always allowed, without `tried` —
   that is how the agent undoes an override (R-M7).
4. **One fallback rule at every start of a name the agent may have
   overridden**, by an explicit refusal table (R-M1, below): a breaker trip or
   a registry refusal of the active version → the same start pinned at the
   name's built-in version, or the per-site outcome for names without one.
   A fault *during* a run is not a refusal.
5. **An operator pause stops the fallback too** (R-M2): a paused definition
   refuses every start, built-in included; the start fails with the pause's
   note. Deferred to the maintainer to confirm; reversible in the refusal
   table.
6. **The fallback informs, the agent decides**: a `fallback` record on the
   span and one line in the run's own system parts (R-M4). No automatic
   notice signal in this step (step 5 decides trip notices).
7. **Activation needs `tried`** (set only by `try_workflow`, step 4; tests
   use `WorkflowMeta::mark_tried`). Until step 4, agent activation is refused
   in practice except for built-in digests.
8. **Saving the same text twice reuses its version** (R-M6), and a name holds
   at most 20 agent versions: every version's YAML is in the engine's one
   `definitions` row per name, read by every admission and finish of that
   name. Pruning never-activated versions is an engine issue to file.

## Non-goals

- The sandbox and `try_workflow` (step 4); schedules, trip notices for
  scheduled definitions (step 5); removing `crates/owf` (step 6).
- Migrating saved workflows from `main` (design 01, decision 2).
- Engine changes: everything uses ouro `v0.2.1` (v0.2.2 only if the SQLite
  read fix lands first; no API change).

## Design

### Store: `workflow_meta` (agent.db, migration `0016_workflow_meta.sql`)

```sql
CREATE TABLE workflow_meta (
    name TEXT NOT NULL,
    digest TEXT NOT NULL,          -- ouro_durable::digest_of(yaml)
    note TEXT,
    builtin INTEGER NOT NULL DEFAULT 0,
    created_ms INTEGER NOT NULL,
    tried_ms INTEGER,
    PRIMARY KEY (name, digest)
);
```

- Written **before** `runtime.save` (R-L2): an orphan row (crash before the
  engine save) is harmless; `tried` and the note attach to content, which is
  what a dry run proves and what a trip refuses.
- `flows.rs` §1: `WorkflowMeta` trait (`record`, `mark_tried`, `get`,
  `names`) with `AgentDb` and in-memory impls.
- `install_builtins` (runs.rs §7) records built-in digests with
  `builtin = 1` instead of the memory KV `engine.builtin_digests.<name>`;
  migration 0016 imports those KV entries once (R-L5) and the KV keys are
  then unused. Its rule is unchanged: an active version that is not a known
  built-in digest is never replaced at boot.
- Status: `active` = the engine's active version; `retired` = in the engine's
  `activations`, not active; `draft` = never activated; plus `tripped`
  (`Trip.breaker`) and `paused` (`Trip.manual`).

### The save/activate gate (R-H1)

One function `flows::gate(yaml, name)` used by save and activation:

1. `runtime.validate(yaml)` — the engine dialect and the current registry
   (the old `owf` `check` is no longer used for saving; it stays only for
   the evals workbench until step 6).
2. The agent's argument check (`with` names against each function's schema)
   ported to `ows` call sites; warnings (`main` §5 `warnings`) ported.
3. `document.name == name`; `name != "guard"`; `signal` declares no route.
4. Deny-list for agent versions: `emit`; `run: workflow` (the skill already
   says it is rejected); `schedule` and `metadata.ouro.{at, timezone,
   catchup, overlap}` until step 5 (an activation would arm engine triggers
   that fire outside the supervisor with no host data);
   `metadata.ouro.breaker` (decision 3) — the error says "remove
   `metadata.ouro.breaker`: your versions use the runtime's breaker".
5. `require_guarded_turns`: `agent_turn` requires a `call: start_guard`
   round; the message points at `read_workflow agent-loop`'s round.
6. `require_metadata` (`main`): `metadata.loop.description`, `metadata.agent`
   / `metadata.route` preset and confirm (`agent_start`), route shape; a
   workflow that calls dispatcher functions (`primary_loop`, …) declares no
   route and no `metadata.agent` (routes that call `primary_loop` were
   superseded on `main`; they are refused now).

A verbatim copy of built-in `agent-loop` or `signal` minus the breaker key
passes the gate (test).

### Tools

- **`save_workflow { name, definition, note? }`:** gate; if the name already
  has a version with this digest, reuse it (reply says so); else refuse
  past 20 agent versions; `workflow_meta.record`, then `runtime.save`. Reply
  as `main` (`calls` with their delivery instead of read/write, `next`,
  `warnings`).
- **`activate_workflow { name, version }`:** if the version's digest is a
  known built-in → `activate_as(.., "agent", note)` (no gate, no `tried`).
  Else gate (tools may have changed), route priority (`main`
  `require_route_priority`; two concurrent activations can both pass, which
  is harmless since `signal_route` breaks ties by name, R-L7), `tried` →
  `activate_as`. Reply as `main`, plus: activation clears a trip and starts
  a fresh count, also for the version that tripped (a change from `main`,
  R-L1); a version's faults count all its runs, workers and recovery runs of
  an `agent-loop` override and re-raised faults of a `signal` override
  included (R-L6).
- **`list_workflows`:** names from `workflow_meta` ∪ built-ins; per name the
  latest version, derived status, active version, tripped/paused, note,
  created time; built-in versions marked `builtin: true`.
- **`read_workflow { name, version? }`:** the YAML from the engine, status,
  note; `guard` its built-in text.

### Starting a name the agent may have overridden (R-M1)

Two entry points over one decision function:

- `runs::start_named_root(runtime, id, name, input_fn, host, site)` for the
  signal (`supervisor.rs`) and `recovery_loop` and `AgentLoop::send`
  (agent.rs, used by evals);
- `runs::start_named_child(ctx, key, name, input_fn, host, site)` for
  `primary_loop`, `start_worker`, the route and saved launches.

`input_fn(fallback: Option<&Fallback>) -> Value` builds the input, so a
fallback rebuilds it with the line (R-M4). Steps: start the active version;
on a refusal in the table, rebuild input with the fallback line and start
the same id/key pinned at the fallback version; return what ran, read from
the joined admission (`version`, `runtime["pinned"]`), not assumed — a
re-dispatched starter joins its first commit (R-M1.1). `LoopHost.workflow`
names the definition that runs (R-M1.3).

Refusal table:

| Engine answer | Meaning | Outcome |
|---|---|---|
| `DefinitionTripped { paused: false }` / `ChildRefusal::Tripped` (breaker) | the agent's version tripped | fallback |
| `MissingFunction`, `InvalidDefinition` / `ChildRefusal::Inadmissible` | the version no longer fits the registry | fallback |
| no active version (checked with `definitions(name).active == None` before starting; `UnknownDefinition` is crate-private) | nothing activated | fallback |
| `DefinitionTripped { paused: true }` | operator pause | no fallback: the start fails with the note (decision 5) |
| `Depth`, `HostData`, `Unavailable`, `Mismatch`, `ResourceUnavailable` | not the version's fault | no fallback, error as today |
| the fallback itself refused | pause or broken deploy | the original error, logged; no second fallback (R-L9) |

Fallback version per site: a name with a built-in → its built-in version
(digest lookup, as `start_guard`); `site = Loop` (worker, primary) → built-in
`agent-loop`; `site = Route` → no start, `route_signal` answers `{routed:
false, fallback}` and the default path runs (R-M3, as `main` and the skill);
`site = Saved` → `{handover: "<text>"}` (R-L3) and the agent loop takes the
task, as `main`.

**Loops only for loop sites (R-H2):** at `site = Loop` a name is accepted
only if it is `agent-loop` or its active version declares `metadata.loop`;
anything else (a scheduled workflow, `signal`, a route) runs built-in
`agent-loop` with fallback reason `not_a_loop`. `invoke_sub_agent` is not
guarded, so this is what keeps a worker from firing an unreviewed write
workflow.

### Loop selection (R-M9)

Restored as `main`, each through `start_named_child(site = Loop)`, precedence
highest first: the YAML's `start_worker { loop }`; `invoke_sub_agent`'s
`loop:`; the worker skill's frontmatter `loop:`; `agent-loop`. The primary's
`primary_loop(loop: …)` likewise. `tests/pending/loop_variants.rs` (:346,
:370) pins the precedence.

### Route and saved launches (R-H3)

`route_signal` and `run_saved_workflow` start the route's or task's
workflow as a host child of the signal (`site = Route | Saved`) with the
host data `main`'s `admission(.., Launch)` built, read from the activated
version's document metadata (vetted by the gate at save and activation —
invariant 3 forbids run input and expressions as a source, not the vetted
document):

| | Routed | Scheduled |
|---|---|---|
| preset default | `smart` | `base` |
| skills | the workflow sets up its own (`loop_setup`) | the source's skill if it exists (`scheduler`) |
| confirm | `metadata.agent`/`metadata.route` `confirm`, default the conversation, else the default chat | same |
| guard request | the signal content | same |
| system parts | context, `Signal source: …`, env addendum, fallback line | same + handover note |

`signal.yaml` gains, for both paths, the wait the primary path has: `listen`
for the child's `done:`/`waiting:` → `child_outcome` → `settle_child { kind:
route | saved }` (`settle_child` maps `route` like `primary`, `saved` as
today: faulted before any at-most-once call → handover; after → recovery).
Without it the signal ends and its owned child is cancelled.

### Deleted

The memory-KV breaker (old `<name>.breaker` rows are left alone),
`engine.builtin_digests.*` use after the import, the `unavailable(..,
"versions")` replies, the `route_signal` stub, `RESERVED` beyond `guard`.

## Contract and model

No engine contract change. Design 01's agent invariants hold; added:

- **A7.** A breaker trip or registry refusal of an agent version never fails
  a start: the fallback runs (or the route/saved path falls back to the
  default path), same id, same trace. A pause is not covered (decision 5).
- **A8.** No agent version declares a breaker policy.
- **A9.** The agent never saves or activates `guard`; the guard round runs
  only the built-in digest.
- **A10.** A loop site runs only a loop.
- Accepted boundary of invariant 2 (as `main`, R-L8): calls written into an
  activated workflow's YAML are trusted as the user confirmed them at
  activation; only the model's calls go through the guard.

## Failure modes

- **Crash after the meta row, before the engine save:** an orphan meta row;
  the next save of that text records nothing new and saves.
- **Crash between activation and reply:** activated; `list_workflows` shows it.
- **Crash during a fallback start:** the refused start stored nothing; the
  retried starter is refused again and falls back again (same id/key); a
  starter that already committed joins and reads what ran from the
  admission.
- **Tool removed after activation:** registry refusal → fallback, reason in
  the record.
- **Hot rows:** bounded by decision 8.
- **Breaker counting:** fallback runs have another digest and never count
  against the agent's version.

## Tests

Restore from `tests/pending/` into `tests/`, adapted, nothing weakened:
`loop_variants.rs`, `workflow_kinds.rs`, the versions part of `flows.rs`
(incl. `the_authoring_skills_reference_definition_is_valid` against the new
gate) and `durable_loop.rs`, and the versions-step tests of
`pending/supervisor.rs`: `a_saved_workflow_runs_and_faults_hand_over_by_effect`,
`an_interrupted_saved_workflow_is_left_to_durable_recovery`,
`an_activated_signal_workflow_handles_signals_until_its_breaker_opens`,
`an_activated_signal_override_changes_the_recovery_prompt`,
`a_signal_override_without_a_catch_gets_no_automatic_recovery`,
`a_catch_before_signal_session_cannot_start_recovery`,
`activated_signal_routes_*`, `a_signal_route_has_its_own_breaker_*`,
`a_telegram_signal_runs_the_loop_that_declares_its_route`,
`a_rebuilt_routed_loop_keeps_what_its_setup_added`. New, each failing before
its code:

1. Save → draft; activation refused until tried; tried → active; the
   previous active → retired (derived).
2. Gate: a verbatim copy of built-in `agent-loop`/`signal` minus the breaker
   saves; `guard`, `emit`, `run: workflow`, `schedule`, `metadata.ouro.at`,
   `metadata.ouro.breaker` refused at save and at activation, and no engine
   trigger exists after a refused activation.
3. An `agent-loop` override that faults three times trips; the next primary
   runs the built-in pinned, same run id and trace, `fallback` record on the
   span, the line in its system parts; the override stays active; activating
   a new version clears the trip.
4. Same for a `signal` override, and for a worker's loop variant (→ built-in
   `agent-loop`, line in the worker's system parts).
5. A tripped route → `routed: false` with the fallback record; the default
   path runs with its context gathering and the line.
6. A scheduled saved workflow: missing or tripped → handover with text;
   faulted after an at-most-once call → recovery.
7. A version calling a tool the snapshot no longer has → fallback, reason.
8. `start_named_*` never pins an agent version (only built-in digests).
9. A worker naming a scheduled write workflow or `signal` runs `agent-loop`;
   the fake MCP sees no write.
10. Loop selection precedence (four paths).
11. A paused `agent-loop` refuses the built-in fallback too: the primary
    start fails with the pause's note and the signal reports it.
12. A re-dispatched starter after a fallback joins and reports the version
    that ran.
13. Saving the same text twice reuses the version; the 21st version is
    refused.
14. Activating the built-in digest back needs no `tried`; the next boot
    upgrades it to a new built-in text.
15. `install_builtins` keeps an agent-activated override across a restart;
    0016 imports the KV digests.
16. Route priority conflict refused at activation; most specific route wins.
17. `list_workflows`/`read_workflow` show derived status, tripped/paused,
    built-ins.
18. Route and saved launches: preset/skills/confirm defaults and system parts
    as the table; the signal waits for the child and settles it.

Full suite green twice (`scripts/test.py`), fmt, clippy.

## Rollout

Migration `0016_workflow_meta.sql`. `skills.default/workflow-author.md`: its
DSL part rewritten for the engine dialect (`metadata.ouro.*`, `start_guard`,
`classify`/`tool_once`/`tool_repeat`, `start_worker { loop }`, no
`hold` under flat metadata, the deny-list and the breaker rule) (R-M8).
`CLAUDE.md`: overrides, the engine's breaker and the fallback record.
`docs/operations.md`: back up `engine.db` with `agent.db`. LOG.md: decisions
1–8. An ouro issue: pruning never-activated versions. Nothing deploys.

## Open questions

For the maintainer, reversible, not blocking: decision 5 (a pause also stops
the built-in fallback) and decision 2 (no re-seeding, no YAML copy).
