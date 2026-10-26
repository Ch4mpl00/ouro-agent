# 04. Step 3: the agent's workflow versions and the breaker on the engine

Status: approved after goldfish rounds 1 and 2 (resolutions marked R-n, R2-n) ·
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
   Inherit`), so it trips at the engine default (3). Activating the version
   whose digest is the *current compiled* built-in text of `agent-loop` or
   `signal` is always allowed, without the gate or `tried` — that is how the
   agent undoes an override (R-M7; only the current text, R2-L1).
4. **One fallback rule at every start of a name the agent may have
   overridden**, by an explicit refusal table (R-M1, below): a breaker trip or
   a registry refusal of the active version → the same start pinned at the
   name's built-in version, or the per-site outcome for names without one.
   A fault *during* a run is not a refusal.
5. **An operator pause is respected, never undone by the agent** (R-M2,
   R2-M1, R2-M2): a paused name falls back like a tripped one (reason
   `paused`) when the fallback is another name (route → default path, saved →
   handover, loop variant → built-in `agent-loop`); when the fallback is the
   same name (`agent-loop`, `signal`) the engine refuses it too and the start
   fails with the pause's note — and while `agent-loop` is paused no recovery
   report can run (the operator sees the log). `activate_workflow` refuses a
   paused name ("paused by an operator: ask them to resume it"), since an
   activation would lift the pause; `install_builtins` skips a paused name
   and logs it. For the maintainer to confirm; reversible.
6. **The fallback informs, the agent decides**: a `fallback` record on the
   span and one line in the run's own system parts (R-M4). No automatic
   notice signal in this step (step 5 decides trip notices).
7. **Activation needs `tried`** (set only by `try_workflow`, step 4; tests
   use `WorkflowMeta::mark_tried`). Until step 4, agent activation is refused
   in practice except for built-in digests.
8. **Saving the same text twice reuses its version** (R-M6), and a name holds
   at most 20 **never-activated** agent versions (R2-M3: activated ones are
   bounded by the user's confirmations, so overrides of `agent-loop` and
   `signal` never end): every version's YAML is in the engine's one
   `definitions` row per name, read by every admission and finish of that
   name. Pruning never-activated versions is an ouro issue to file; the cap's
   error names it.

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
- `record(name, digest, note, builtin)` is an upsert (R2-L6): the first
  write sets `created_ms`; `tried_ms` is never cleared; a given note replaces
  the stored one; `builtin` only goes 0 → 1.
- `flows.rs` §1: `WorkflowMeta` trait (`record`, `mark_tried`, `get`,
  `names`) with `AgentDb` and in-memory impls.
- `install_builtins` (runs.rs §7) records built-in digests with
  `builtin = 1` instead of the memory KV `engine.builtin_digests.<name>`;
  migration 0016 imports those KV entries once (`json_each` over `memory`,
  R-L5) and the KV keys are then unused. Its rule is unchanged (an active
  version that is not a known built-in digest is never replaced at boot),
  plus: a paused name is skipped and logged (decision 5).
- Status: `active` = the engine's active version; `retired` = in the engine's
  `activations`, not active; `draft` = never activated; plus `tripped`
  (`Trip.breaker`), `paused` (`Trip.manual`) and `tried`.

### The save/activate gate (R-H1)

One function `flows::gate(yaml, name)` used by save and activation:

1. `runtime.validate(yaml)`: the engine dialect, the current registry and
   each call's arguments against the function's schema (the engine does
   this, so the old `owf` `check` is not used for saving; it stays only for
   the evals workbench until step 6). `main`'s `warnings` are ported to
   `ows` and returned, never refused (R2-L4).
2. `document.name == name`; `name != "guard"` (checked first, before any
   shortcut, R2-L1); `signal` declares neither a route nor `metadata.loop`
   (R2-L3).
3. Deny-list for agent versions, on the parsed model (R2-M4):
   - `wf.schedule.is_some()` (every/cron/after/at/on — what activation arms
     as engine triggers that would fire outside the supervisor with no host
     data; step 5 brings schedules);
   - `Workflow::breaker != Inherit` (decision 3; error: "remove
     `metadata.ouro.breaker`: your versions use the runtime's breaker");
   - `emit` (design 01, 17);
   - `run: workflow` (its namespace/version resolution picks versions the
     agent never named). The engine's `call: start_child { name, input }`
     stays allowed, as on `main`: it starts the active version of a name the
     YAML names, with no host data, so its host functions refuse and only
     its own trusted calls run.
4. `require_guarded_turns`: `agent_turn` requires a `call: start_guard`
   round; the message points at `read_workflow agent-loop`'s round.
5. `require_metadata` (`main`): `metadata.loop.description`, `metadata.agent`
   / `metadata.route` preset and confirm (`agent_start`), route shape; a
   workflow that calls dispatcher functions (`primary_loop`, …) declares no
   route and no `metadata.agent`.

A verbatim copy of built-in `agent-loop` or `signal` minus the breaker key
passes the gate (test).

### Tools

- **`save_workflow { name, definition, note? }`:** gate; if the name has a
  version with this digest, reuse it (reply says so); else refuse past 20
  never-activated agent versions (decision 8); `workflow_meta.record`, then
  `runtime.save`. Reply as `main` (`calls` with their delivery instead of
  read/write, `next`, `warnings`).
- **`activate_workflow { name, version, note? }`** (R2-L8: the note is the
  activation's, else the save's): refuse `guard`; refuse a paused name
  (decision 5). The current compiled built-in digest → `activate_as(..,
  "agent", note)` with no gate and no `tried`. Else gate (tools may have
  changed), route priority (`main` `require_route_priority`; two concurrent
  activations can both pass, harmless since `signal_route` breaks ties by
  name, R-L7), `tried` → `activate_as`. Reply as `main`, plus: activation
  clears a trip and starts a fresh count, also for the version that tripped
  (a change from `main`, R-L1); a version's faults count all its runs —
  workers and recovery runs of an `agent-loop` override and re-raised
  faults of a `signal` override included (R-L6).
- **`list_workflows`:** names from `workflow_meta` that have engine versions
  (orphans skipped) ∪ built-ins; per name the latest version, derived
  status, `tried`, active version, tripped/paused, note, created time;
  built-in versions marked `builtin: true`.
- **`read_workflow { name, version? }`:** the YAML from the engine, status,
  note; `guard` its built-in text.
- **Loop catalog** (`flows::loop_catalog`, R2-L7): `name: description` lines
  for the built-ins and every agent name whose active version declares
  `metadata.loop.description` (an active override of `agent-loop` replaces
  its line); a tripped or paused one is marked `(set aside)`; a draft never
  hides an active version.

### Starting a name the agent may have overridden (R-M1, R2-H1)

Two entry points over one decision function:

- `runs::start_named_root(runtime, id, name, build, site)` for the signal
  (`supervisor.rs`), `recovery_loop` and `AgentLoop::send` (agent.rs, used by
  evals);
- `runs::start_named_child(ctx, key, name, build, site)` for `primary_loop`,
  `start_worker`, and the route and saved launches.

`build(fallback: Option<&Fallback>) -> (input, host)`: the starter's own
builder, so a fallback rebuilds both — the line goes into the input or the
host's system parts, and `LoopHost.workflow` names the definition that runs
(R-M1.3, R-M4).

Steps:

0. **Already admitted?** Root: `runtime.get(id)`; child:
   `runtime.get(ctx.child_id(key))`. If it exists, join it under its
   **stored** name and version (a child: `start_child` with the stored name;
   a root: return it) and report what ran from its admission (`version`,
   `runtime["pinned"]`, the host's fallback). A re-dispatched starter after a
   cross-name fallback (`tg-loop` → `agent-loop`) or a host change
   (`recovery_loop`) therefore joins instead of failing with `Mismatch`.
1. **Choose.** `definitions(name)`: no active version → fallback
   (`missing`); `site = Loop` and the active version is neither `agent-loop`
   nor declares `metadata.loop` → fallback (`not_a_loop`, R-H2). Otherwise
   start the active version **pinned at the version just read** (so a
   concurrent activation can't slip a non-loop in, R2-L3; the active
   version's runs count toward its breaker either way).
2. **On a refusal in the table**, `build(Some(fallback))` and start the
   same id/key pinned at the fallback version; nothing was stored by the
   refused start, so the id is the same.

Refusal table:

| Engine answer | Reason | Outcome |
|---|---|---|
| `DefinitionTripped { paused: false }` / `ChildRefusal::Tripped` | `tripped` | fallback |
| `MissingFunction`, `InvalidDefinition` / `ChildRefusal::Inadmissible` | `invalid` | fallback |
| no active version (pre-check; `UnknownDefinition` is crate-private) | `missing` | fallback |
| not a loop at a loop site (pre-check) | `not_a_loop` | fallback |
| `DefinitionTripped { paused: true }` | `paused` | fallback if it is another name; the same name → the start fails with the note (decision 5) |
| `Depth`, `HostData`, `Unavailable`, `Mismatch`, `ResourceUnavailable`, an interruption | — | no fallback, error as today |
| the fallback itself refused | — | the original error, logged; no second fallback (R-L9) |

Fallback per site: a name with a built-in (`agent-loop`, `signal`) → its
built-in version (digest lookup, as `start_guard`); `site = Loop` (worker,
primary) for another name → built-in `agent-loop`; `site = Route` → no
start: `route_signal` skips the route and tries the next match (R2-L2, as
`main`), answering `{routed: false, fallback}` if none starts, so the
default path runs (R-M3); `site = Saved` → `{handover}` and the default
path takes the task, as `main`.

```rust
pub struct Fallback {
    pub name: String, pub version: Option<u64>, pub digest: Option<String>,
    pub reason: FallbackReason, // Tripped | Invalid | Missing | NotALoop | Paused
    pub error: Option<String>,
    pub ran: Option<(String, u64)>, // what runs instead; None for route/saved
    pub line: String,
}
```

The line (R2-L5): ``Workflow `{name}` v{version} was set aside ({reason
text}: {error}); the built-in `{ran}` runs this time. Tell the user if it
matters; fix or re-activate it if you own it.`` — `{reason text}`: "its
breaker tripped after {n} faults in a row", "it no longer fits the tools",
"it has no active version", "it is not a loop", "an operator paused it".
The span gets the record as `fallback` metadata.

### Loop selection (R-M9)

Restored as `main`, each through `start_named_child(site = Loop)`, precedence
highest first: the YAML's `start_worker { loop }`; `invoke_sub_agent`'s
`loop:`; the worker skill's frontmatter `loop:`; `agent-loop`. The primary's
`primary_loop(loop: …)` likewise. `tests/pending/loop_variants.rs` (:346,
:370) pins the precedence.

### Route and saved launches (R-H3, R2-M5)

`route_signal { context }` and `run_saved_workflow { name, context }` start
the route's or task's workflow as a host child of the signal with host data
read from the activated version's document metadata (vetted by the gate —
invariant 3 forbids run input and expressions as a source, not the vetted
document), as `main`'s `admission(.., Launch)`:

| | Route | Saved (scheduled) |
|---|---|---|
| child key | `route` | `saved` |
| `LoopHost.kind` | `primary` (it is the signal's primary run) | `primary` |
| `signal_id` | the signal's | the signal's |
| input | built like `primary_loop`'s (`agent::prepare`): the signal content as the first user message, plus `env` | same |
| preset default | `smart` | `base` |
| skills | the workflow sets up its own (`loop_setup`) | the source's skill if it exists (`scheduler`) |
| confirm | `metadata.agent`/`metadata.route` `confirm`; default the conversation, else the default chat | same |
| guard request | the signal content | same |
| system parts | context, `Signal source: …`, env addendum, `$context.handover` (a saved workflow's handover, R2-M5), the signal's own fallback line | context, source, env addendum, the signal's own fallback line |

Outputs: `run_saved_workflow` → `{started: ChildRef}` or `{handover: text}`
(no active version, or every start refused); `route_signal` →
`{started: ChildRef}` or `{routed: false, fallback?: Fallback}`. `ChildRef`
is `primary_loop`'s (`{id, key, done, waiting}`).

### `signal.yaml` (R2-H2)

The three launches share one wait. Sketch of the changed steps (the
executor writes the YAML; names and data flow are binding):

```yaml
- saved:      # if signal.workflow != null and no handover yet
    call: run_saved_workflow
    with: { name: '${ $context.signal.workflow }', context: '${ $context }' }
    export: { as: '${ $context + { primary: .started, kind: "saved", handover: .handover } }' }
- after_saved: { switch: [ { started: { when: '${ $context.primary != null }', then: wait } }, { on: { then: route } } ] }
- route:
    call: route_signal
    with: { context: '${ $context }' }
    export: { as: '${ $context + { primary: .started, kind: "route", route_fallback: .fallback.line } }' }
- after_route: { switch: [ { started: { when: '${ $context.primary != null }', then: wait } }, { default: { then: confirm_target } } ] }
# … default path unchanged; primary_loop's `system` adds
#   $context.route_fallback and $context.signal.fallback;
#   its export becomes { primary: ., kind: "primary" }
- wait:        # the current primary_wait / primary_done / primary_outcome, on $context.primary
- settled:
    call: settle_child
    with: { ref: '${ $context.primary }', kind: '${ $context.kind }' }
- after_settle:
    switch:
      - handover: { when: '${ .action == "handover" }', then: handed_over }
      - raise:    { when: '${ .action == "raise" }', then: primary_failed }
      - done:     { then: answer }
- handed_over:
    set: …       # export { handover: .handover, primary: null, kind: null }
    then: route  # a saved workflow is tried once: `saved` runs only while handover == null
```

- The signal's own fallback line: `start_named_root` puts it in the signal
  input as `signal.fallback` (the `SignalHost` is unchanged, so a re-start
  joins); the built-in `signal.yaml` (what runs on that fallback) passes it
  to whichever primary runs.
- `settle_child { kind: route }` behaves as `primary` for the outcome and
  the trace output; its `skills` metadata is empty (no `primary_loop`
  record). The catch's transcript keeps reading `$context.primary`, which is
  now the route's or saved run's too, so a faulted route's report has its
  transcript (as `main`).
- The handover backward jump runs at most once per signal (bounded jumps).

### Deleted

The memory-KV breaker (old `<name>.breaker` rows are left alone),
`engine.builtin_digests.*` use after the import, the `unavailable(..,
"versions")` replies, the `route_signal` and `run_saved_workflow` stubs,
`RESERVED` beyond `guard`.

## Contract and model

No engine contract change. Design 01's agent invariants hold; added:

- **A7.** A breaker trip or registry refusal of an agent version never fails
  a start: the fallback runs (or the route/saved path falls back to the
  default path), same id, same trace. A pause is covered only when the
  fallback is another name (decision 5).
- **A8.** No agent version declares a breaker policy.
- **A9.** The agent never saves or activates `guard`; the guard round runs
  only the built-in digest.
- **A11.** The agent never lifts an operator's pause (activation refused
  while paused; boot skips a paused built-in).
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
  starter re-dispatched after its start committed finds the run in step 0
  and joins it under its stored name (R2-H1).
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
   saves; `guard`, `emit`, `run: workflow`, a `schedule` (every/cron/at/on),
   `metadata.ouro.breaker`, a route or `metadata.loop` on `signal` refused at
   save and at activation, and no engine trigger exists after a refused
   activation; `call: start_child` is accepted.
3. An `agent-loop` override that faults three times trips; the next primary
   runs the built-in pinned, same run id and trace, `fallback` record on the
   span, the line in its system parts; the override stays active; activating
   a new version clears the trip.
4. Same for a `signal` override (the line reaches the primary through
   `signal.fallback`), and for a worker's loop variant (→ built-in
   `agent-loop`, line in the worker's system parts).
5. A tripped route with a less specific matching route → the next route
   runs; with none → `routed: false` with the fallback record; the default
   path runs with its context gathering and the line.
6. A scheduled saved workflow: missing or tripped → handover with text;
   faulted after an at-most-once call → recovery.
7. A version calling a tool the snapshot no longer has → fallback, reason.
8. `start_named_*` pins only the active version it just read or a built-in
   digest; an activation between the read and the start can't put a non-loop
   at a loop site.
9. A worker naming a scheduled write workflow or `signal` runs `agent-loop`;
   the fake MCP sees no write.
10. Loop selection precedence (four paths).
11. Pause: a paused route → default path, a paused loop variant → built-in
    `agent-loop`, a paused saved workflow → handover (reason `paused`); a
    paused `agent-loop` → the primary start fails with the note and no
    recovery report runs (logged); `activate_workflow` refuses a paused name;
    `install_builtins` skips it.
12. A re-dispatched starter joins after a fallback: a cross-name child
    (`tg-loop` → `agent-loop`) and `recovery_loop` (host changed by the line)
    both report the version that ran instead of `Mismatch`.
13. Saving the same text twice reuses the version; the 21st never-activated
    version is refused; activated versions don't count.
14. Activating the current built-in digest back needs no `tried`; an old
    built-in digest goes through the gate; the next boot upgrades the
    built-in to a new text.
15. `install_builtins` keeps an agent-activated override across a restart;
    0016 imports the KV digests.
16. Route priority conflict refused at activation; most specific route wins.
17. `list_workflows`/`read_workflow` show derived status, tripped/paused,
    built-ins.
18. Route and saved launches: key, kind, input, preset/skills/confirm
    defaults and system parts as the table (the handover note reaches the
    route); the signal waits for the child and settles it; a saved
    workflow's handover continues to routing once.
19. A faulted route's recovery report has the route's transcript.
20. `workflow_meta.record` upsert rules; `list_workflows` skips orphans and
    shows `tried`; the loop catalog marks set-aside loops.

Full suite green twice (`scripts/test.py`), fmt, clippy.

## Rollout

Migration `0016_workflow_meta.sql`. `skills.default/workflow-author.md`: its
DSL part rewritten for the engine dialect (`metadata.ouro.*`, `start_guard`,
`classify`/`tool_once`/`tool_repeat`, `start_worker { loop }`, no
`hold` under flat metadata, the deny-list and the breaker rule) (R-M8).
`CLAUDE.md`: overrides, the engine's breaker and the fallback record.
`docs/operations.md`: back up `engine.db` with `agent.db`. LOG.md: decisions
1–8. An ouro issue: pruning never-activated versions (and caching parsed
routes per (name, active, epoch) if `route_signal` shows up in profiles).
Nothing deploys.

## Open questions

For the maintainer, reversible, not blocking: decision 5 (pause semantics),
decision 8 (the cap counts only never-activated versions) and decision 2 (no
re-seeding, no YAML copy).
