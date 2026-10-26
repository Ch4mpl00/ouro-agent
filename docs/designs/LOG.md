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

## 2026-10-26 (step 3 (executor))

Choices taken while implementing design 04 where the document is silent or
ambiguous, each reversible:

- **Argument names are checked by the agent at the gate**, against MCP's
  current schemas and the host functions' own lists: the engine's bindings
  of MCP tools are permissive (design 01, decision 16), so its validation
  does not see them. The `owf` `check` stays for the evals workbench only.
  The deny-list also refuses the guard round's private functions
  (`review`, `confirm_request`, `resolve_review`), as `main` did.
- **A loop's input tells its own steps `conversation` and `confirm`**
  (`agent::loop_input`), and a route's or saved run's input also carries
  `signal` and `env`: informational for the YAML (the reference Telegram
  loop decides delivery and status on them); host functions read host data
  only. Workers have no conversation. The fixture reads
  `$context.conversation` where `main`'s read `$context.loop.conversation`.
- **Step 1 also sets aside a version whose trip stands** before building
  anything (the same outcome the engine's refusal would give); the
  refusal mapping stays for a start that races an activation or a trip.
- **The fallback record:** in a loop's host data (`LoopHost.fallback`) and
  its span's metadata, with a `fallback` event; for `signal`,
  `signal.fallback` is the line and the record is `input.fallback` and the
  trace's metadata. A start refused after its anchor was written takes the
  anchor back (`AgentDb::forget_run_trace`).
- **Traces of route and saved launches** nest the primary run's
  `agent_loop` span in a `route:<name>` or `runner` span (ended with it,
  `Scopes::own_wrapped`), as `main`'s traces had.
- **A saved workflow's completed answer** is `{ok, workflow, output}` as an
  object (the signal's output and trace output), as `main`; its handover
  text is "This task names the saved workflow `x`. <the line> Do the task
  yourself."; a primary run that took no turn has no transcript for the
  report, as `main`.
- **`read_workflow` without a version** reads the active one, else the
  newest (`main`: the newest); statuses are `active`/`retired`/`draft` plus
  `builtin` (`main` said `built-in`).
- **The draft cap** counts the versions never activated that are not a
  built-in text.
- **`runs::ChildStarts`**, a seam over `FunctionContext::start_child`, lets
  a test stand in for a re-dispatched starter (design test 12).
- **Small behaviour kept from `main`** while restoring its tests: the
  recovery report's span opens before its setup (an unknown preset is an
  ERROR observation), `primary_loop` checks its `input` before the session,
  and a loop the host runs writes its run's answer into its trace (a variant
  answering from a step).
- **Restored tests, adapted:** a signal route calling `primary_loop` is
  refused by the gate now, so `activated_signal_routes_*` routes to a loop
  and asserts the refusal; with no `FlowRun` on the engine,
  `an_interrupted_saved_workflow_*` leaves a saved workflow waiting on an
  event; the `owf` task spans (`call:<task>`) are gone by design 01,
  decision 14, so those assertions read the call spans; the memory-KV
  breaker's assertions read the engine's trip; `schedule_task(workflow:)`
  stays unavailable until step 5, so tests fire saved workflows through the
  schedule's sink. The dry-run halves stay parked for step 4
  (`tests/pending/{flows,workflow_kinds,sandbox,skill_index}.rs`), and the
  `owf`-era Telegram loop is kept as `tests/fixtures/telegram-loop.flat.yaml`
  for the dialect test.
- **Step 2's tests that waited for this step:** the process test's journal
  run takes the reference Telegram loop again (activated through the gate
  into the files the binary opens) and ends on its reply, with no closing
  turn, as on `main`; the Telegram history test gains its routed variant
  beside the default path's. A routed signal's trace carries no `skills`
  metadata (design 04: `settle_child { kind: route }`), where `main`
  recorded the routed loop's skills, so the activity journal's packet of a
  routed signal lists none: left open for the maintainer.
- **Test-first, by mutants:** this run wrote the step's code before the
  design's tests; each test was then seen failing on a scratch copy of the
  commit with the code behind it removed (one mutant per piece: fallback,
  tools, launches, built-ins, loop selection, meta, gate, pinning,
  handover, re-dispatch join, cap and reuse, catalog, route priority,
  transcript). The executor's report lists which mutant fails which test.

## 2026-10-26 (step 4: design 05, the sandbox)

- **The engine first:** the sandbox needs the engine to give handlers the
  store that drives them (a dry run's runs live in a private store), to
  report a dry run's whole tree, to take an observer of its own, to cut
  holds in a dry run and to let the host name the root. That is ouro design
  07 (ouro#39), approved after two goldfish rounds; it is a breaking release
  (0.3.0). Run access reads the call's lineage (a loop reads the signal run
  above it) and delivers only to the call's run and its descendants,
  lease-checked, never with the engine's reserved event types (whether
  `Runtime::deliver` should refuse them too: ouro#40).
- **Design 05 revises design 01, decision 16** (decision 12): drafts are not
  activated in a second runtime but overlaid as "active" inside the sandbox's
  guard store, which both the agent and the engine read; and a small named
  set of `Engine::sandbox` branches stays (confirmation press, card edit,
  `tool`'s write answer, turn/call records, recovery) instead of a parallel
  dry registry, because the handlers are shared and the differences few.
- **Reversible choices:** recovery in a try is a second dry run after the
  signal's (decision 13); a draft of a tripped name is tried unless the name
  is paused or the draft is the tripped content (decision 4, so a fix can
  become tried); `tried` needs every run of the draft to complete
  (decision 10); the no-signal mode's input follows step 5's fire input.

## 2026-10-26 (step 4 (executor))

Choices taken while implementing design 05 where the document is silent,
each reversible (the two places the code made the document wrong are its
Deviations 1 and 2):

- **Engine pinned at the `v0.3.0` tag** (design 07); its breaking fields
  needed no change in the agent (no struct literal of them).
- **One seam for a handler's reads, `runs::RunReads`** (`run`, `records`,
  `children`): implemented for `FunctionContext` (its run access) and for
  `Engine` (the host's own store: the supervisor, a loop the host drives,
  tests). `ChildStarts` is `RunReads` + the start, which is how it "gains
  `run(id)`". `loop_tool` takes a `CallSite { runs, run, task, invocation }`;
  tests call it through `support::loop_tool` on the host's store. `Records`
  became `pub` (the seam names it).
- **Handler call sites (R2-M2), classified:** moved to run access:
  `loop_setup`, `context` (the signal run's records; a loop's read of the
  signal run, an ancestor), `unanchor`, `start_guard`'s joined child,
  `primary_loop`, `recovery_loop`, `signal_fallback_line`, `launch`,
  `route_signal`'s joined route, `settle_child` (child and records),
  `effects_started` (each run's `call_journal` of its records and its
  children), `review`, `reviewed`, `signal_of`, `signal_transcript`,
  `loop_of`/`loop_for`/`guard_context_of`, `guard_child_verdict`,
  `loop_tool`'s worker read, `worker_runs`, `read_run`, and
  `start_named_child`'s step 0. Stay on the engine by design: `deliver_event`
  (mocked in a try), `confirmation::pending`, `builtin_version` and `decide`
  (definitions, served by the guard store), `start_named_root` (live roots
  only: a try records its recovery start instead), and host-side code (the
  supervisor, `AgentLoop::drive`/`close` and `end_tree_loops`, the drain,
  the reconciler, boot).
- **The report:** runs also carry `key` (the last segment of their link
  key: `route`, `primary`, `guard`, `worker:i.j`), since run ids are the
  engine's hashes; `writes[]` are `{run, task, function, arguments, via}`;
  the signal's writes are `writes` and the recovery's are
  `recovery.writes` (split, not repeated); `drafts[].outcome` is
  `completed`, `faulted: <detail>`, `cancelled`, `unfinished`, `did not
  run` or `set aside: <line>`; `ended` is the first loop among the root's
  children; a step path lists tasks entered (not `do`), skipped tasks,
  switch cases, faults where they happened and retries, not suspensions
  (with `max_hold = 0` every join suspends).
- **Set-aside versions are recorded** by `runs::start_named_child` on
  `Engine::sandbox` (`Sandbox::set_aside`, `fallback`): one more record of
  decision 12's set; the root's comes from its `Choice`.
- **`runs::choose`** (decide, then set aside) chooses the dry root and the
  recorded recovery start; `start_named_root` keeps its own refusal retry.
  **`supervisor::signal_admission`** (anchor, evidence, the builder) is
  shared by `run_active` and the sandbox. **`functions::mcp_functions`** is
  the registry's MCP set, whose non-read-only members the mock marks.
- **A model's write is mocked in `AgentLoop::invoke`**, after the
  availability checks (an unavailable tool still answers unavailable) and
  after the verdict; its span gets `mock: true`.
- **The press** delivers `dry_confirm:<token>` to the guard run with
  `cx.deliver`; a card already stored (a re-dispatch) answers without a
  second delivery; an expiry backdates the card 24 h + 1 ms
  (`confirmation::Store::backdate`).
- **One budget per try:** the signal's dry run and its recovery's share the
  `TryOptions::timeout` deadline; `TryOptions::stray_read` is the test-only
  hook of test 19 (a read of the run in `signal_session` through the
  engine's handle). The guard store refuses and counts every method but
  `definitions`, `query_definitions`, `definition_control` (overlaid) and
  `policy`.
- **Without a signal** a try also takes `drafts` (test 18), runs as
  `try:<name>` under the builder's `dry-run` span, maps statuses as R-L2
  (`main`'s `waiting` is `suspended`) and lists `drafts` when it named more
  than the root.
- **Restored tests, adapted:** paths are the engine's (`next[1] →
  replied`), `child_outcome` answers a `Completed` status object, and the
  skill-index text is read from migration 0007's file (a later migration
  dropped the `workflows` table); only its dry-run test is restored, the
  rest of `pending/skill_index.rs` needs scheduled fires (step 5). Test 16's
  handler write is a tool whose read-only mark changes mid-run (no handler
  writes to MCP on its own in a try otherwise).
- **`workflow-author`**'s try section names the new report fields (`via`,
  `run`, `task`, `key`, `fallback`, `recovery`, `outcome`,
  `sandbox_refusals`) and that only drafts whose runs all completed become
  tried.
- **Hand mutants** (once, on a scratch copy): every one the design names is
  caught; "`mark_tried` without the completed check" was first taken two
  ways, and the one that drops the dry run's own status survived until test
  6 gained a try whose routed draft completes while a `signal` draft faults
  after it (nothing is tried).

## 2026-10-26 (step 4 verified)

- Deviations 1–3 of design 05 accepted: no-signal `steps` from the observer
  (the engine's call journal carries no results); the no-signal write answer
  stays `main`'s `{"ok":true,"dryRun":true}` (skill-index checks `ok`); the
  wall-clock bounds of tests 1 and 10 are relative to the try's own cost.
- `try_workflow`'s description now names the no-signal write answer, so it
  no longer tells the model something untrue.
- The engine's per-start/resume cost in a debug build (100–200 ms) goes to
  ouro as an issue; it does not block the migration.
- The full-suite failure (`an_empty_queue_is_waited_on…`) is the known
  load-sensitive timing test; supervisor 34/34 and sandbox 24/24 alone.

## 2026-10-26 (step 5: design 06, schedules)

- Design 06 approved after goldfish rounds 1 (4 high) and 2 (1 high).
  Round 1 moved the fire out of the engine: `fire_scheduled` only records a
  row in `scheduled_fires` and the supervisor's drain runs it as a signal,
  so no handler waits on the supervisor (no permit, drive or cycle
  deadlock).
- Revises the overview's step 5: no trip notice signal. A task's run only
  records a fire, so a signal's failure never faults it (its catch reports
  to the user); task definitions run with `breaker: off`. Reversible.
- No overlap knob on `schedule_task` (runs end at once).
- Revises the overview's risk line "schedules re-generated": the engine's
  store is the only copy of the tasks; export them before any store
  re-creation (design 06, Rollout).
- A cap of 50 live tasks. Reversible.
- Design 05's open question on the no-signal try's input closes unchanged.

## 2026-10-26 (step 5 (executor))

Choices taken while implementing design 06 where the document is silent,
each reversible:

- **Cap raised to 100 by the maintainer** (decision 15 and test 13 say 100
  now; `schedule::MAX_LIVE_TASKS`). Paused tasks count as live.
- **The seam** is `schedule::Schedules { fires: Arc<dyn FireStore>, the
  write mutex, the wake-up }`; `FireStore` (agent.db, `MemoryFires`, the
  sandbox's read-through `DryFires`) also hands out task ids (`task_ids`)
  and the seed's id (`seed_id`, one transaction: read the key, else
  allocate and store it). Test fixtures on a fixture's agent.db use its
  table; the others and the evals optimizer use `Schedules::in_memory()`.
- **Replies:** `schedule_task` answers `{ok, task: {id, cron_expr,
  recurring, prompt, workflow, timezone}, next_fire}` (R2-L9's shape;
  `next_fire` is the engine summary's due time, ISO UTC like every time in
  the list). `cancel_scheduled_task` adds `error` to `ok: false`. The list
  adds `consumed: true` for a fired one-shot whose fire is pending,
  `faults` and `last_error` (latest failed run of the definition, none in
  a dry run, whose store reads no runs) when the count is above zero, and
  `paused` (the operator's note) for a manual pause: with `breaker: off`
  that is the only trip a task can meet.
- **The task block** keeps the zone for one-shots too (`timezone`: the
  zone the instant was computed on); only recurring tasks carry
  `metadata.ouro.timezone`. Every task activation is attributed to actor
  `agent` with a note (`scheduled`, `cancelled`, `time zone A → B`,
  `seeded`); an identical text reuses its version (digest).
- **The drain** stops a round at the first fire left pending (oldest first
  stays oldest first) and waits out the backoff without listening to the
  wake-up; a round errors into the same backoff. `user_zone` reads
  `get_timezone` for the seed (UTC when unreadable) and the watch (skip).
- **`start_named_child` refuses a `task-` name** (so `launch` does); the
  gate refuses `fire_scheduled` by the reserved contexts
  (`functions::RESERVED_CONTEXTS`), and a `start_child` whose `name` is a
  literal `task-…` string.
- **Removed with the tick:** `tests/schedule.rs` (the tick's property suite,
  a port of MCP's scheduler tests) and `tests/support/wall_cron.rs`: the
  cron rule is the engine's now, pinned by ouro's tests and test 14.
- **Test 6** moves the clock a minute while the held signal's run is in a
  model call; a jump past the engine's 30 s lease stalls the engine's
  worker, so the test walks the clock in 10 s steps with a 50 ms
  heartbeat (`support::fixture_configured`).
- **Test 2** also schedules a prompt that is a whole expression
  (`${ .x }`), which the engine would evaluate if it reached `with`.
- **Restored skill-index tests** (test 9) run on fires recorded into the
  table and drained; `main`'s "lost write reply, retried and verified"
  case now pins the engine's at-most-once answer (the run faults with
  `call.unknown`, the next fire writes nothing), and the reopen case lists
  the task instead of counting `scheduled_tasks` rows. The MCP sandbox test
  includes the agent's test support, so `mcp-tools` has `ouro-runtime` as a
  dev-dependency at the agent's tag; its server serves `memory,scheduler`
  (the signal session reads `get_timezone`).
- **Hand mutants** (once each, scratch worktree): all seven the design
  names are caught; "the prompt written into `with`" is caught at save (the
  engine refuses `${` embedded in text, so the first prompt never fires).

## 2026-10-26 (maintainer: design 04 calls; step 5 verified)

- **Maintainer confirmed** design 04 decisions 2 (the engine is the only
  version store, no YAML copy, no re-seeding), 5 (an operator pause is never
  lifted by the agent) and 8 (at most 20 never-activated versions per name).
- **Maintainer:** a routed signal's trace carries the routed loop's skills
  again (the activity journal lists them, as on `main`); the load-sensitive
  wall-clock tests are rewritten to assert what they mean, not elapsed time.
- **Step 5 verified** (design 06): the drain's one rule and `fire_scheduled`
  read against the design; 106/107 binaries in the executor's full run, the
  one failure the sandbox confirmation test's wall-clock bound under load
  (24/24 alone), covered by the item above.

## 2026-10-26 (fixes: journal skills, clock tests)

- **A routed signal's trace carries the routed loop's skills again.**
  `settle_child { kind: route }` now writes `skills` (host skills, the
  engine's unless excluded, then `loop_setup`'s added and preselected, read
  from the route's run: `agent::loop_skill_names`) instead of an empty list;
  the telegram-loop test pins the trace metadata and the journal's packet.
- **Two wall-clock tests assert what they mean.** The empty-queue test waits
  for `get_next_signal` right after the wait ends (60 s, far below the
  hour), the signal's handling has only a loose bound. The sandbox
  confirmation test drops the baseline comparison for a 60 s backstop (a
  round's PT60S join or a worker's PT15M still fail it; the engine's dry run
  exposes no record of a hold's timeout, so the PT5S answer wait is the
  engine's own test). Mutant once (`max_hold` left unset in the engine's dry
  run): the test failed at 122 s.
## 2026-10-26 (step 6: executor)

Design 07 (remove `crates/owf`), written and implemented by the step's
executor; choices where the document is silent, each reversible:

- **No goldfish round** for 07: a removal with no new contract (the
  workbench keeps its rules, the agent's running path is untouched).
- **`flows::check` reads the engine's dialect**, so
  `tests/flows.rs`'s at-most-once refusal of `choose` writes
  `metadata: { ouro: { delivery: at_most_once } }` (the flat key is no
  longer read, by `check` or the engine).
- **`Standalone` keeps `ows`'s default `execute`** (`call` with
  `CallFailure::answer`): the workbench's real calls keep `owf`'s catchable
  error contract, and only typed call-outcome errors stop a run.
- **`engine_dialect.rs`** gains `converted_flat_keys_reach_the_engine` (the
  converter's reading of each extension on the synthetic definition) in place
  of the `owf`-vs-`ows` comparison it loses.
- **`docs/optimizer.md`** (the workbench's own doc) names the engine's
  dialect; the two `.claude/tasks` records that describe `crates/owf` as live
  code carry a superseded note; their open issues (#43, #44) are left to the
  maintainer to re-triage.
- **Hand mutants** (once each, in place on the committed tree, restored by
  `git checkout`): both caught (deviation 1 of design 07 on which test).

## 2026-10-26 (maintainer: no seeded dreaming)

- **Maintainer: "dreaming isn't needed for now."** The first start seeds no
  task (design 06 decision 14 and test 12 removed; the design's Deviations
  say what went). The `dreaming` skill stays; the agent schedules it when
  the user asks. Such a task fires as a `scheduler` signal, so only its
  primary loop may write skills, not a delegated `dreaming` worker. No
  migration: a `schedule.engine_seeded` key already written stays, unread.

## 2026-10-26 (maintainer: one database — tried, reverted)

The maintainer asked to move the engine's store into agent.db
(`runs::engine_store_path` returning agent.db itself) now that ouro v0.2.2
made reads `BEGIN DEFERRED`. Tried on the branch with the no-seed change
above, measured with one full `scripts/test.py` run, and **reverted**:
`engine.db` stays beside agent.db, and the step 2b entry stands.

- Checked first: both sides open the file in WAL with a busy timeout
  (AgentDb 5 s, the engine 30 s); the drizzle migrator and its hash pin read
  only `__drizzle_migrations`, the engine only `ouro_migrations`/`ouro_*`;
  nothing in scripts/deploy backs up or copies `engine.db` by name.
- **The run**: 99/101 binaries, 394 s (37 s build) against 101/101 and
  459 s (134 s build) for the last two-file run: tests 357 s against 325 s.
  Slower: `engine_runs` 59 → 95 s, `loop_properties` 325 → 358 s,
  `confirmations` 120 → 136 s.
- **Failure 1, a lock fault, deterministic**: `mcp-tools`'s
  `skill_index_workflow` fails every time, alone and unloaded, with
  "database is locked" from `WorkflowMeta::mark_tried` (`flows.rs`, the
  agent's synchronous `UPDATE workflow_meta` after a dry run,
  `sandbox::mark_tried`), after the 5 s busy timeout. The test runs on a
  current-thread runtime: the engine opened an IMMEDIATE transaction on its
  connection, and the agent's write then blocked the only runtime thread in
  SQLite's busy wait, so the engine task that would commit never ran. The
  same test on a 4-thread runtime passes (5 s). On one file every
  synchronous agent write on a runtime thread can wait on an engine
  transaction that needs a runtime thread to finish; with two files the two
  never share a lock.
- **Failure 2**: `confirmations`'
  `confirmation_resolution_crashes_preserve_authority_and_at_most_once_execution`
  (the crash-around-resolution property, 4-thread runtimes): the next
  process completed the run without the confirmed write (`writes == []`).
  Not seen in the two-file runs; no lock line in its log, cause not
  isolated.
- **After the revert** (no-seed change only): `skill_index_workflow`
  passes (4.9 s) and the crash property passes alone (28 s).
- **What one file would need**: every agent.db write off the runtime
  threads (the blocking pool), or the agent's tables written through the
  engine's connection. Left to the maintainer.
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

## 2026-10-26 (design 08: executor)

Design 08 (the Telegram wrapper), implemented with the goldfish review's
corrections and the maintainer's decisions (08, Deviations). Choices the
executor took alone, each reversible:

- **A built-in that does not fit the registry is not installed.**
  `runs::install_builtins` validates each built-in first and skips (with a
  log line) one the engine refuses, instead of failing the boot: an MCP
  without `telegram_send_status` gets no Telegram route and its Telegram
  signals take the default path. On the full MCP nothing changes.
- **Default-path tests keep the default path by their MCP, not a seam.**
  The test MCPs that drive Telegram signals down the default path no
  longer offer `telegram_send_status` (`Harness::new`, `supervisor.rs`'s
  `ContextMcp` unless a test opts in, `supervisor_e2e`), so the built-in
  route is not installed there; the tests that activate the reference
  `telegram-loop` opt in (`Harness::with_status`, `ContextMcp.status`).
  `versions.rs`' set-aside-route tests moved to a source with no built-in
  route (`chat`); the route-priority tests now expect the built-in
  `telegram` last; the sandbox's narrowed and paused drafts now fall to the
  built-in route (its MCP offers the four Telegram tools). The e2e property
  therefore covers the default path only; the wrapper's own suite is
  `tests/telegram_loop.rs`.
- **`telegram_send_status` is journal noise** (`activity::NOISE`): the
  bubble is progress, never what the user got; observers' spans are left
  out by their `activity: observer` tag.
- **`start_loop` inherits more than the review listed**, all narrowings:
  the caller's tool whitelist, deny set, `parent_id` (a worker's loop stays
  a worker), iteration limit and engine-skill choice; `allow_skill_writes`
  only when the caller has it and asks.
- **The observer also gets `reasoning`** when the provider returned
  `reasoning_content` beside the calls (agent-loop passes
  `.message.reasoning_content`), clipped to 400 like `content`.
- **The status lines' unknown outcomes are caught by name** (a nested `try`
  around each bubble call: ouro's catch-all never takes
  `call.unknown`), then any other error by the outer catch.
- **Test 11's failing run** was taken with the new skill moved aside; the
  sleep case of G-M11 (the observer's PT20S bound) has no wall-clock test.
- **Hand mutants** (once each, on a scratch worktree of the committed tree):
  the observer joined (a `listen` on its `done:` after `observe`): caught
  by `a_hanging_status_model_delays_nothing` (24 s against its 15 s bound);
  the tombstone removed: caught by the MCP seq test (a late line dropped as
  `stale`, not `cleared`: with the clear at seq 1 000 000 the stale rule
  alone already keeps late lines out, so the tombstone guards a clear sent
  without a seq); `exclude_tools` ignored: caught by four `telegram_loop`
  tests. The scratch worktree shared the target directory, and one full
  run picked up the mutants' artifacts; the reported run is a clean one
  after a rebuild.
