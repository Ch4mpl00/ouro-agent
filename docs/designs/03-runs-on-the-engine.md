# 03. Step 2: runs on the engine

Status: reviewed (round 1 resolved; round 2 pending) · Overview: [01](01-engine-migration.md) step 2 · Branch: `feat/engine`
Engine prerequisites: ouro designs 03 (host data, host-started children, with
"first commit wins" joins for host children) and 04 (breaker policy) merged;
engine pinned to that revision. Review findings are referenced as (R-Hn/Mn/Ln).

The core step: every workflow run of the agent (signal, loops, workers, guard
rounds, recovery) becomes an engine run. Built as phases A–F, each a series of
commits; the step is done when the agent's own run store, interpreter use and
child driver are gone from the running path.

## Problem

See the overview. This step replaces: `agent/src/durable.rs`; `flows.rs`
`execute_saved`/`resume`/`worker`/`recover_once`/`run_loop_durable`/
`run_child`/`loop_child`; the `start_child` host function; the
`owf::run::Functions` implementations (`Functions`, `SignalFunctions`); the
trace anchor columns; every SQL join from agent tables to `workflow_runs` /
`workflow_events` / `workflow_awaits`; and every reader of `owf` records.

## Decisions

From the overview (not reopened): engine store in `agent.db`; `signal` durable;
host data carries vouched facts; built-ins `breaker: off`; delivery by
`readOnlyHint`; tracing split with store reconciliation; `emit` refused and
reserved names unsavable; no migration of runs or saved workflows.

Added by this document (logged in LOG.md):

1. **Supervisor waiting rule (R-H1).** The supervisor starts `signal:<id>`
   and waits until the signal's run tree is *quiescent*: the signal ended, or
   no run of its tree is runnable or leased and every live run is waiting on an
   external event or a timer (walk the tree with `Control::list` by `parent`,
   bounded). Then it takes the next signal; a suspended tree is resumed by the
   engine worker when its event arrives (e.g. a button press delivered by a
   later signal). Signals still start one at a time.
2. **Host data is small; large evidence is content-addressed (R-H2).** Host
   data holds only vouched facts and digests (sizes ≤ 4 KiB by construction);
   large evidence (loaded skill text, earlier user messages, the preselect
   block, signal content) goes into an agent table
   `evidence(digest PRIMARY KEY, body, created_ms)`, immutable; only host data
   names a digest, so YAML can't substitute evidence.
3. **Child starters rely on "first commit wins" (R-H3).** A re-dispatched
   child-starting handler joins the child its first dispatch committed, whatever
   it recomputes now (engine design 03). Handlers still derive host data only
   from their arguments, their run's host data and journal — never the clock or
   live memory — so the first dispatch is reproducible in tests.
4. **Dispatcher state comes from the run, not memory (R-H4).**
   `signal_session` records its env snapshot in its output; `signal_input`
   records the earlier user messages (as an evidence digest) in its output;
   later handlers read them from this run's journal (`Runtime::records` of
   `ctx.run_id`); `signal_transcript` takes the primary's child ref, checks it
   is this run's own child, and reads that child's last loop record.
5. **Signal loss window stays (R-H5).** `get_next_signal` consumes atomically;
   a crash between it and the committed start loses that signal — as today.
   Documented, not claimed otherwise. An MCP lease/ack protocol is deferred to
   the maintainer (needs a `.feature` scenario first; issue filed).
6. **Outcome mapping lives in a host function (R-H6).**
   `settle_child {ref, kind}` reads `Runtime::get(child)` and the calls of the
   child and its descendants and answers `done`, `handover` or `raise`:
   saved workflow — completed/waiting/interrupted → `done`; missing or faulted
   with no at-most-once call `completed`/`unknown`/`failed` → `handover`;
   faulted after a write → `raise`; primary/route — faulted → `raise`,
   cancelled → `done` without a report. YAML only switches on the answer.
7. **Jump budget as a backstop (R-H7).** `max_iterations` per loop stays the
   binding limit; `limits.max_jumps` is set with a wide margin over
   `Σ loops max_iterations × (2 + max guarded calls per round) + 3`
   (documented in operations), so the recovery loop always has budget left. A
   test exhausts `max_iterations` in the primary and still gets the report.
8. **Button presses (R-M1).** Delivered inside `signal` by `deliver_event`
   (durable, traced): `Runtime::deliver(event{source:"telegram",
   id:"telegram_button:<signal id>", type:<token>, data}, Some(<guard run>))`.
   An undelivered press (card answered, run gone) falls through to the primary
   loop with `delivered:false` and a `reason` (`already_answered`, `expired`,
   `unknown_card`) — the agent decides what to say.
9. **No saved-workflow features in step 2 (R-M4).** `try_workflow`,
   `save_workflow`, `activate_workflow` answer "unavailable until the engine
   migration's versions step" (step 3/4); sandbox tests are `#[ignore]` with a
   pointer to step 4; `run_saved_workflow`/`route_signal` see no saved
   workflows (always hand over / not routed); superseded signal routes are
   removed. Agent-authored workflows are gone anyway (overview decision 2).
10. **Engine worker starts after MCP (R-M12)**: after the first successful
    `tools/list` (the snapshot is the registry until then; nothing resumes
    before it).

## Design

### A. Registry and host functions

- MCP tools from the persisted snapshot (`tool_snapshot` table; never shrunk —
  R-L9), permissive schemas, `AtMostOnce` unless `readOnlyHint`; `WITHHELD`
  and supervisor-only tools excluded. Direct MCP calls from workflows answer
  typed outcomes (`not_executed`, `unknown_outcome`, `interrupted`).
- Loop functions (who may call: runs whose host data is loop data):
  - `agent_turn` ALO; `agent_settle` ALO; `loop_setup` ALO.
  - `classify` ALO: `tool_once` for writes (MCP tool without `readOnlyHint`,
    `SYNTHETIC_WRITES`, unknown names — `agent.rs:1000-1025`) and for a
    confirmed open-world read (a card exists for this guard run and call key);
    `tool_repeat` for reads and for `invoke_sub_agent` with a durable worker
    (its outcome is re-readable) (R-M9).
  - `tool_once` AMO, `tool_repeat` ALO: both answer **definite** results for
    tool errors and MCP `NotExecuted` — `{presented:"[tool error] …",
    failed:true}` as today (`flows.rs:409-417`) — so a failing tool never
    faults the loop (R-M8). Only an ambiguous failure of `tool_once` becomes
    `unknown_outcome`, caught in YAML and turned into exactly today's
    `unknown_tool_outcome` shape for `agent_settle` (R-M9).
  - Both check the guard verdict: `guard_run` must be this run's own host child
    named `guard` with host data `round == i` (`Runtime::get`), and
    `pending_child_confirmation` is re-implemented over `confirmations` with a
    new `loop_run` column (R-M2).
  - `read_run`: `Runtime::get/records`, children via `Control::list(parent)`;
    only runs in this loop's tree (R-M3). The sub-agent reply keeps
    `worker_run` (now a `hostchild:` id).
- Context functions (callable by `signal` and loops — R-M11):
  `signal_input`, `recent_activity`, `pending_confirmations`,
  `preselect_skills` ALO.
- Child starters, ALO (engine design 03 rule), return
  `ChildRef {id, key, done, waiting}` unless noted:
  - `start_guard {calls, …}` → `{run, key, guarded}`, `{run:null}` for rounds
    with only local reads (`flows.rs:1883-1885`); guard host data
    `{kind:"guard", parent_loop, round, confirm, evidence: digest}`; guard
    history goes into the child's `input`, not host data.
  - `start_worker {call, i, index, memory}` honours the loop-variant selection
    (`worker_loop`/`loop`, `flows.rs:1859-1867`); the worker inherits `confirm`
    (invariant 5).
  - `primary_loop`, `recovery_loop`, `run_saved_workflow`, `route_signal`
    (`route_signal` refuses being called from a route).
  - `settle_child` (Decision 6) — not a starter.
- Guard functions: `review` ALO; `confirm_request` AMO (invariant 6);
  `resolve_review` ALO — callable only by `guard` runs whose host data names a
  parent loop.
- Dispatcher functions (callable only by `signal`): `deliver_event` ALO
  (idempotent by event id), `signal_session`, `signal_transcript`.
- Workflow-step functions with explicit delivery (R-L3): `set_memory` AMO;
  `llm_compose`, `filter_relevant`, `choose`, `hash_value`,
  `skill_catalog_snapshot` ALO.
- Host data per kind (the trust boundary; exact fields, R-M10):
  - `signal`: `{kind, signal_id, source, created_ms, evidence: {content}}`.
  - loop (`primary`, `worker`, `recovery`): `{kind, loop, preset, confirm,
    conversation, session_id, skills: [names], allow_skill_writes,
    max_iterations, max_concurrent_tools, tool_whitelist,
    include_engine_skills, parent_id, root_request_digest, signal_id,
    evidence: {skills, earlier, preselect, system}}`.
  - `recovery` is `agent-loop` started with loop host data `confirm: null`,
    `include_engine_skills: false`, `max_iterations` from the dispatcher
    (today 5), the prompt as the first user message — no new YAML (R-M10).
- Invariant 3 restated (R-M11): the `signal` definition (built-in, or an
  activated override in later steps) is the trusted source of
  `primary_loop`'s arguments; every other run gets host data only from the
  host's derivation (the parent loop for workers and guards).

### B. YAML

- Built-ins converted with step 1's transform, then:
  - `agent-loop.yaml`: `start_child {workflow: guard}` → `start_guard`;
    `start_child {workflow: agent-loop}` → `start_worker`; `tool` →
    `classify` + `switch` → `tool_once` (inside a `try` whose `catch` names
    `urn:ouro:errors:call.unknown`) | `tool_repeat`; `metadata.ouro.breaker: off`.
  - `signal.yaml`: `deliver_event` with the new event shape; each starter
    followed by `listen` on `done:<id>` (no hold — the signal suspends),
    `child_outcome`, then `settle_child`; `breaker: off`.
  - `guard.yaml`: `metadata.ouro.{concurrency: 16, hold, breaker: off}`; a
    `catch` naming `call.unknown` around `confirm_request` resolves that item
    `unverified` so the round's other verdicts stand (R-L4).
  - Holds (R-L7): signal → primary none; loop → worker 15 min; loop → guard
    60 s; guard → card 5 s.
- Skills/tool texts teaching the DSL: `metadata.ouro`, engine built-in names
  `start_child {name, input}` / `child_outcome {id, key}` (R-M9).

### C. Runtime wiring

- `Runtime::new(sqlite adapter on agent.db, registry, Config {…}, clock,
  tracer)`; capacity: drives ≥ 34 (primary + guard + 16 workers + 16 worker
  guards; the signal holds none), function slots ≥ 2 × drives, `max_jumps` per
  Decision 7; numbers and record-size estimate in `docs/operations.md`.
- Built-ins at boot (R-M7): read `definitions(name)`; reuse the version whose
  digest equals the built-in's, else save; activate only when nothing is
  active or the active digest is one of the known built-in digests (kept in
  memory KV `builtin_digests.<name>` until step 3's `workflow_meta`).
- Engine worker in a restart loop with backoff, started after MCP (Decision 10).
- `--once` mode drives the engine until the supervisor's predicate holds (R-L8).
- `signal:<id>` ids: MCP ids and scheduled fire ids (> 10⁹) don't collide
  (R-L10).
- Live loop cache (R-M6): ended on the observer's `run_ended` (hint) and by
  the reconciler for terminal runs; a test asserts the map is empty after a
  signal and its workers end.

### D. Confirmations

- Evidence (invariant 1): `resolve_review` scans the guard run's
  `Record::Event`s for the card's two token types (unique per card — R-L5) and
  checks run, call key, chat, message and thread against the card row, then
  the "spent once" update. The engine closes the subscription when the listen
  ends; the other token's later press stays mailboxed until it expires
  (R-M2); its signal falls through per Decision 8.
- `pending_confirmations`, card cleanup and `pending_child_confirmation` use
  `confirmations` (+ `loop_run`) and `Runtime::get` liveness.
- Cut-over (R-L6): a boot task edits cards created before the migration
  marker (`memory` KV `engine_migrated_at`) to "expired" once.

### E. Journal readers

`guard::restore_chain` matches `tool_once`/`tool_repeat` (R-M2);
`setup_record`/`turned`, the guard's trusted `review` evidence and
`history_messages` read engine `Record`s.

### F. Tracing, reconciliation, sweep

- Handlers write generation/tool spans with payloads (agent Tracer, parented
  under `FunctionContext::span`); an agent observer maps lifecycle events,
  wrapped in `observer::buffered` drained by a task (R-L7).
- `run_trace(run_id, trace_id, session_id, parent_span_id, recorded, created_ms)`
  inserted **before** the engine start (R-M5).
- Reconciler (boot + every minute): windows over `Control::history` with
  `before = now − 30 s` and overlap, idempotent; for an ended run it writes
  the trace's final output and terminal marker from the stored outcome, then
  sets `recorded`; a `run_trace` row with no engine run after 10 minutes is
  closed as never started; it ends stale live loops; it sweeps
  `run_trace`, `confirmations`, `guard_verdict_uses`, `evidence` older than
  the engine's retention for ended runs (R-M2).
- Migration `00NN_engine_runs.sql`: create `run_trace`, `tool_snapshot`,
  `evidence`, `confirmations.loop_run`; drop `workflow_runs`,
  `workflow_records`, `workflow_events`, `workflow_awaits`,
  `workflow_listeners` (applied migrations untouched).

## Contract and model

Engine contract unchanged. Agent invariants 1–6 (overview; 3 restated above)
each get an engine-backed test.

## Failure modes

- Crash between `get_next_signal` and start: the signal is lost (Decision 5).
- Crash inside a child starter after its commit: the re-dispatch joins the same
  child (Decision 3).
- Crash inside a loop: resumes; an AMO tool call in flight is unknown, reported
  to the model, not resent.
- MCP down at boot: the worker waits for MCP (Decision 10); a direct MCP call
  from a workflow later recorded `not_executed` faults that workflow unless it
  catches (R-M8) — loops never fault on tool errors.
- Provider outage: loops fault; built-ins never trip; the recovery report
  still runs (Decision 7).
- Observer event loss: only spans; durable state from the reconciler.

## Tests

`durable.rs`, `durable_properties.rs` (the deleted store) are removed — the
engine's conformance covers it; agent-level behaviours in `durable_loop.rs`,
`loop_durability.rs` are rewritten on the engine (R-L1). Existing suites keep
passing: `flows.rs` (saved-workflow parts per Decision 9), `workflow_kinds.rs`,
`agent_loop.rs`, `loop_variants.rs`, `supervisor*.rs`,
`confirmation_properties.rs`, `tool_results.rs`, `tracing.rs`, `activity_*.rs`;
`sandbox.rs` `#[ignore]`d until step 4. New, failing first where possible:

1. Invariant 1: wrong chat/message/thread, another run's press, and a forged
   YAML output are not honoured; a press before the guard listens is honoured
   once; a press arriving as the *next signal* while the primary waits is
   honoured within seconds (R-H1).
2. Invariant 2: a model write is never executed unreviewed (inline review
   fallback kept — R-L2); a `guard_run` that isn't the caller's own child for
   that round is refused; a pending card can't be bypassed.
3. Invariant 3: a saved workflow's `run: workflow agent-loop` / built-in
   `start_child` child has no host data → `agent_turn` refuses; dispatcher and
   guard functions refuse callers of the wrong kind.
4. Invariant 4: crash during `tool_once` → unknown, the model is told, the fake
   MCP sees one call.
5. Invariant 5: a worker's `confirm` equals its parent's.
6. Invariant 6: a card send with an unknown outcome is not resent; the round's
   other verdicts stand.
7. Capacity: primary + 16 workers + guards complete.
8. Breaker off: three faulted signals, the fourth is admitted.
9. Signal idempotency: the same signal id started twice is one run.
10. Host data size: a 40 KiB skill set starts fine (evidence table) (R-H2).
11. Re-dispatch: crash after a child starter's commit, recover with a changed
    skill file and a later clock → the same child continues (R-H3).
12. Restart between `signal_input` and `primary_loop`: the primary's guard
    context equals the run without restart (R-H4).
13. `settle_child`: each branch (done, handover, raise, cancelled) (R-H6).
14. Budget: the primary exhausts `max_iterations`, the recovery report is sent
    (R-H7).
15. Registry superset: boot with MCP down, the worker does not resume runs
    until MCP answers; a direct MCP call answers `not_executed` (R-M8, R-M12).
16. Reconciler: dropped `run_ended` → still recorded with the final output;
    orphan `run_trace` closed; overlapping windows idempotent (R-M5).
17. Live loops: the map is empty after a signal and its workers end (R-M6).
18. Boot: an active non-built-in version is not replaced; the built-in version
    is reused by digest (R-M7).

## Rollout

On `feat/engine` only; no deploy (overview decision 13). `docs/operations.md`
(capacity, budget, record sizes), `CLAUDE.md` "How a signal turns into action".

## Open questions

None blocking. Deferred to the maintainer (LOG.md): an MCP lease/ack protocol
to close the signal-loss window (Decision 5).
