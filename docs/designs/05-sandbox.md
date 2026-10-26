# 05. Step 4: dry runs on the engine (`try_workflow`)

Status: approved after goldfish rounds 1 and 2 (resolutions marked R-n, R2-n) ·
Overview: [01](01-engine-migration.md) step 4, decision 16 (revised here,
decision 12) · Epic: #62 · Engine prerequisite: ouro design 07 (ouro#39,
release 0.3.0) · Decisions log: [LOG.md](LOG.md)

## Problem

After step 3 the agent can save, gate and activate its own workflow versions
on the engine, but activation needs `tried` and nothing sets it:
`try_workflow` answers "unavailable until the sandbox step"
(`crates/agent/src/sandbox.rs`). So the agent can only activate built-in
versions. The dry-run suites are parked (`crates/agent/tests/pending/sandbox.rs`,
the dry-run parts of `pending/flows.rs`, `pending/workflow_kinds.rs`,
`pending/skill_index.rs`).

On `main`, `try_workflow` has two modes (`main:crates/agent/src/sandbox.rs`,
`main:crates/agent/src/flows.rs` `try_draft`):

- **with `signal`:** a second agent `Engine` whose stores read through to
  the real ones and keep or refuse every write (`DryMcp`, `DryMemory`,
  `DraftWorkflows`, `DrySkills`, `DrySchedule`), a run store in an in-memory
  agent.db, a tracer nested under the builder's call. It runs the signal
  through the whole chain (dispatcher → route → loop → guard → workers) with
  the named drafts in place of the active versions everywhere (routing, the
  loop catalog, starts). Model turns, reads and the guard are real; writes are
  recorded and answered with `MOCK_REPLY`; confirmations are answered on the
  spot through the real guard (`confirmation::press`: a stored card, a
  Telegram-shaped press, or a card a day old for an expiry); waits do not
  sleep. It reports a tree of runs (path of steps, the case each switch took,
  turns with reasoning/text/calls, each call's verdict, card answer and reply,
  how each loop ended) and marks every draft that ran as tried when the run
  completed.
- **without `signal`:** a workflow that takes no agent turn runs once on a
  scheduled fire's input, same mocking; reports `version`, `path`, `steps`
  (each step's output), `writes` with their task, `warnings`; marked tried
  when it completed.

Scenario: the workflow-author worker saves `tg-loop` v2 (routed on
`source: telegram`; never activated). It calls `try_workflow(name: "tg-loop",
signal: {source: "telegram", content: "remind me at 9"}, confirm: "accept")`.
The signal runs through the active `signal`; routing sees the draft's
`metadata.route.when` and routes to `tg-loop` v2, which gathers its first-turn
context from the dry signal and takes real model turns; its
`send_telegram_message` is recorded, not sent; the guard asks for a
confirmation of `schedule_task`, answered "accept" at once. The report shows
`tg-loop` draft v2 ran and completed; v2 is now tried; the parent shows the
user what it will do and activates it after they confirm.

## Decisions

Maintainer (earlier sessions, not reopened): the sandbox from #56 (reads real,
writes mocked with an explicit marker, auto-confirm, drafts, the builder
judges, nothing kept); "the runtime informs, the agent decides"; no channel
knowledge in Rust; behaviour lives in YAML and skills; the engine only executes
and knows no read/write (ouro#13: the host's mock rule).

Elephant (logged in LOG.md, reversible):

1. **The sandbox is a second agent `Engine`, built per try and never
   started** (no worker, no drain, no reconciler). Its deps are the dry ones
   from `main` (`DryMcp`, `DryMemory`, `DrySkills`, `DrySchedule`, a nested
   tracer), an in-memory `AgentDb`, a `Drafts` overlay (decision 4), and as
   its run store a **guard store** (decision 3). Its registry is built over
   itself (`functions::registry`), so every handler in a dry run uses the dry
   deps. Two tries may run at once: separate engines, separate private
   stores (R-L8).
2. **It runs on the engine's dry run** (`Runtime::dry_run_opts`, ouro 07):
   the private memory store holds every run, children included; durable time
   advances without sleeping, and `wait`s, timed `listen`s and holds are cut
   (`max_hold = 0`, ouro 07 decision 8), so a loop joining its guard or a
   worker suspends at once and the child is driven (R-H2). The root is
   admitted under the live id and name (`DryRunOptions::{id, name}`, ouro 07
   decision 9): `signal:0` named `signal` with a signal; `try:<name>` named
   after the draft without one (R-H3, R2-L). The report takes each run's
   version from the sandbox's own choice (`Choice`), not from the root's
   `DryRunRun::version`, which is the private store's save (R2-L).
3. **Handlers reach their run tree through `FunctionContext` run access**
   (ouro 07: `cx.run`, `records`, `children`, `cx.deliver`), never through
   `engine.runtime`/`engine.control`, wherever they read their own run, its
   records, a descendant or its children, or deliver to one (R-H1). The
   complete list of call sites is below (Handler refactor). Run access
   reads the call's lineage (ouro 07 R2-H1), so a loop reads the signal run
   that started it, an ancestor. The sandbox
   engine's run store is a **guard store**: a `RunStore` that answers the
   definition reads a dry run needs (from a read-only view of the real store)
   and refuses every run read and every write with "sandbox: use the call's
   run access", so a missed call site fails a try loudly instead of reading
   the wrong store (R-M3). Several sites swallow store errors
   (`effects_started`, `read_run`, `unanchor`, `signal_transcript`,
   `end_tree_loops`), so the guard store also **counts** its refusals; the
   report carries `sandbox_refusals` when nonzero, and every sandbox test
   asserts zero (R2-M2). If `RunStore`'s surface makes the wrapper
   impractical, the executor uses the read-only view directly and records
   the deviation; the restored suites then carry the burden.
4. **Drafts are an overlay of "active", not activations (R-H4, R-H5,
   R2-M3).** A `Drafts` map (name → {version, digest, yaml}) lives **in the
   guard store**: its `definitions(name)` and its definition queries answer a
   listed draft as the active version, so the agent (`flows::active_versions`
   → `signal_routes` matches a draft's own `metadata.route.when`,
   `loop_catalog` lists draft loops; `runs::decide`) and the engine itself
   (`call: start_child {name}` in a draft's YAML, `run: workflow` children)
   all see the same thing, with no agent-side branch. Nothing is saved or
   activated anywhere. For a listed draft the overlay reports the name's trip
   only when the name is paused by an operator (`trip.manual`, design 04
   decision 5) or the draft's digest is the tripped one; any other trip is
   hidden for the draft (the engine admits other content, and the try is how
   a fix becomes tried).
5. **The root goes through the live start rule (R-M5).** The sandbox
   chooses the root's YAML with the same `decide` (drafts included) and builds
   its input with the same `build(Choice)` the supervisor uses, so a tripped
   or paused active `signal` override, not listed as a draft, runs the
   built-in with `signal.fallback`, as live.
6. **The mock rule (R-M1).** The engine `Mock` marks a registered function
   when it is an MCP tool of the registry snapshot not marked read-only,
   `set_memory`, or `deliver_event` (it delivers into a real run, R2-L);
   every other registered function dispatches (host functions
   included). The answer is `MOCK_REPLY`, and `Mock::response` appends to the
   sandbox's ordered write log with its run id and task path (`via: yaml`)
   (R-M6). Separately, in a sandbox, `tool` answers a model's call that
   `agent::tool_effect` classifies as `Write`, except `invoke_sub_agent`
   (workers run inside a try, as on `main`), with `Sandbox::mock`
   (`via: tool`) — **after** the verdict, so a blocked write is reported
   blocked, not mocked. `DryMcp` and the dry stores stay as a second layer
   for any write a handler makes on its own (`via: handler`). One log, in the
   order the writes happened.
7. **Confirmations through the real guard, as `main` did (R-H6).** The
   sandbox's `confirm_request` is `main`'s `confirmation::press` ported: it
   stores the `Pending` card in the sandbox db, marks it sent with the
   synthetic message id `DRY_CARD_MESSAGE` (0), and for `accept`/`decline`
   delivers a Telegram-shaped press (`{source: "telegram", id:
   "dry_confirm:<token>", type: <yes|no token>, data: {chatId,
   messageThreadId, messageId: 0, data: <token>, text: <label>}}`) to its own
   run with `cx.deliver(event, &cx.run_id)`; for `expire` it backdates the row
   by more than 24 h (`Store::backdate`, as `main`) and delivers nothing (the
   listen's timeout fires at once). It answers `{required: true, types:
   [yes, no]}` as live. `resolve_review` then verifies the press against the
   row exactly as live. In a sandbox, `resolve_review` does not call
   `close_card` (no card exists to edit). `confirm` defaults to `accept`
   (R-L6).
8. **Steps and records.** Steps come from the dry run's own observer
   (`DryRunOptions::observer`, ouro 07): `main`'s `Path` (task entered or
   skipped, switch case, fault where it happened, retry), per run id, capped
   (200 steps, the rest counted). The run tree, versions and outcomes come
   from `DryRun::runs`. Turns (reasoning, text, calls), verdicts and card
   answers come from the sandbox's records (`main`'s `Sandbox::turn/call/press`
   hooks).
9. **The sandbox's observer also does the lifecycle work (R-M7, R2-M1).** A
   never started engine has no drain. `Observer` is synchronous, so the
   sandbox's observer composes `Path` with a buffer: it pushes each
   `RunEnded` (with its outcome) onto a channel, and a sandbox task, running
   for the length of the dry run, ends that run's scope with the event's
   outcome and closes its live loop, with no store read. `RunSuspended` is
   ignored (with `max_hold = 0` every join suspends; a loop stays open
   across it). The sandbox borrows the root span under the root id
   (`scopes.borrow`), writes the root's evidence and `run_trace` anchor under
   it in the sandbox db, and after the dry run closes any loop and scope still
   open with "dry run ended" before it ends the root span. It never calls
   `Engine::shutdown` (whose "resumes at the next start" text is wrong here).
10. **`tried` (R-M4).** A listed draft is marked tried
    (`WorkflowMeta::mark_tried(name, digest)`) when the dry run's status is
    completed and every `DryRunRun` of that (name, digest) completed. A saved
    draft that handed over, a worker loop draft that faulted, a draft no
    route reached: not tried, and the report says why (`ran`, `outcome`).
    This is the only write a try makes outside the sandbox.
11. **No signal → the draft alone (R-M9).** `try_workflow(name)` without
    `signal` dry-runs the draft as the root (its own id and name), on the
    input a scheduled fire admits (`signal_input` with source `scheduler`,
    empty content), with no host data, refused with `main`'s two messages for
    a workflow that handles signals or takes agent turns
    (`is_runtime_workflow`, `takes_turns`, ported from `main:flows.rs`). Its
    report keeps `main`'s fields: `version`, `path`, `steps` (each call's
    clipped output from the root's `DryRunRun::calls`), `writes` with their
    `task`, `warnings`.
12. **Revises design 01 decision 16 and step 4 (R-M8).** 01 asked for "drafts
    activated" and "a dry registry instead of run-time `sandbox()` branches".
    This design keeps drafts unactivated (an overlay) and keeps a small,
    named set of branches on `Engine::sandbox`: `confirm_request`,
    `resolve_review`'s card edit, `tool`'s write answer, the turn/call
    records, `recovery_loop` (decision 13). `Engine::sandbox` is added by
    this step (the branch has none; R2-L); the overlay is in the guard store,
    not a branch. Reason: the
    handlers are shared, the differences are few, and a parallel registry
    would duplicate the handlers it wraps. Logged in LOG.md.
13. **Recovery runs as a second dry run (R-M2, R2-H2).** Live,
    `recovery_loop` starts the root `recovery:<id>` and returns; the signal's
    catch then raises, and the signal's terminal commit would cancel any
    owned child, so a host child cannot carry it. In a sandbox
    `recovery_loop` records the start it would make (the chosen definition,
    its input and host data from the same builder) and returns as live.
    After the signal's dry run ends, the sandbox runs that recorded start as
    a second `dry_run_opts` on the same sandbox engine (id `recovery:0`),
    under the same timeout budget; its tree is reported under `recovery`
    with its writes (mocked). A try whose signal did not fault runs none.
14. **Bounds.** `DRY_RUN_TIMEOUT` (300 s, ported from `main:flows.rs`)
    cancels the dry run's stop token; the engine's `max_tasks`, `max_depth`
    and the jump budget bound it too.
15. **Inside a try (R-L5).** `code_agent` runs for real (a read, as on
    `main`; it uses the codex sandbox's workspace). `try_workflow` inside a
    sandbox answers "not available inside a dry run".
16. **The dry signal's env (R-L4)** is the builder session's (its chat and
    email), as on `main`: the sandbox engine's `env` takes them.

## Non-goals

- No change to `guard.yaml`, `signal.yaml`, `agent-loop.yaml`.
- No replay-based tries (ouro `replay_definition`); the optimizer (#60) is
  unchanged.
- Schedules (step 5): `schedule_task(workflow:)` is still unavailable; a try
  of a scheduled pipeline uses the no-signal mode or a scheduler signal.
- No deterministic model turns: a try spends real model tokens, as on `main`.
- `pending_confirmations` shows nothing in a dry run (cards live in the
  sandbox db); the report's `note` says so (R-L7).

## Design

### The sandbox engine (`sandbox.rs` §1–2)

```rust
pub struct Sandbox {                     // what one try records
    answer: Answer,                      // accept (default) | decline | expire
    drafts: BTreeMap<String, Draft>,     // name → {version, digest, yaml}
    record: Mutex<Record>,               // turns, calls, answers, writes, steps
}

fn sandbox_engine(real: &Arc<Engine>, sandbox: Arc<Sandbox>, root: Arc<dyn Span>,
    env: EnvConfig) -> anyhow::Result<Arc<Engine>>;
```

- A constructor of its own (not `create_engine_on`): it builds the registry
  over the sandbox engine and sets `Engine::sandbox` (R-L6).
- deps: `providers`, `presets`, `skills`, `codex`, `decisions` shared; `env`
  from the builder's session; `mcp` = `DryMcp`, `memory` = `DryMemory`,
  `skill_store` = `DrySkills`, `schedule` = `DrySchedule`, `tracer` =
  `Nested(root)`; `drafts` = the overlay; `db` = `AgentDb::open(":memory:")`
  with the MCP tool snapshot and `workflow_meta` copied in; `store` = the
  guard store over `real.deps.dry_store`.
- `EngineDeps` gains `dry_store: Arc<dyn RunStore>`: a read-only view of
  `store`, opened by the composition root right after `runs::open_store`
  (which migrates), with `sqlite::open_read_only` on `engine.db`; tests get it
  from `runs::memory_store_with_view()`, which returns the memory store and
  its `read_only_view` (R-L3).
- Tracing: every scope a handler asks for is a span under the try's root span
  (`Nested`), so the dry run shows in the builder's own trace and never as a
  signal of its own (the activity journal ignores it, as `activity.rs`
  already says).

### Choosing drafts (R-L1)

A draft is `name` at `version`, or each name's **latest version whose
`workflow_meta` row has `builtin = 0`**. A built-in name with no such version
is not a draft: it runs its active version (trying `signal` or `agent-loop`
as it is, with drafts below it, is allowed for `name`). `guard` is refused.
Each draft passes the gate (`flows::gate`).

### `try_workflow` with a signal (`sandbox.rs` §5)

Arguments as today (`synthetic_tools.json`): `name`, `version?`, `signal`,
`drafts?`, `confirm?`.

1. `signal == {}` → `signal_types` (unchanged from `main`), no run, no draft.
2. Resolve drafts (above).
3. Check `signal.source` against the types (unchanged).
4. Build the root as the supervisor does (`run_active`'s `decide` +
   `build(Choice)`, factored into one function both call; decision 5), with
   signal id 0: id `signal:0`; evidence and the `run_trace` anchor written
   under it in the sandbox db; the root span borrowed under it.
5. `sandbox.runtime.dry_run_opts(root_yaml, input, mock, {id, name, host,
   observer}, stop)` under `DRY_RUN_TIMEOUT`.
6. Close what is open (decision 9); mark tried (decision 10); build the
   report; end the root span.

### The report (`sandbox.rs` §6)

`main`'s shape, with the run tree from `DryRun::runs`:

```json
{ "name", "status": "completed|faulted|suspended|timed_out", "output", "error",
  "ended", "writes": [{run, task, function, arguments, via: "yaml|tool|handler"}],
  "drafts": [{name, version, ran, outcome}], "confirm", "signal",
  "runs": [{run, workflow, version: "draft vN|vN|built-in", path, omitted_steps?,
            turns?, ended?, children?, fallback?}],
  "note", "next" }
```

Status mapping (R-L2): root `Completed` → `completed`; `Waiting` →
`suspended`; `Faulted`, `Interrupted` without a timeout and `Cancelled` →
`faulted` with the error; the timeout → `timed_out`; the builder's own
cancellation → an `Err` (as `main`); an engine `Err` (no `DryRun`) →
`faulted` with an empty tree.

### Handler refactor (decision 3; R-H1)

Every call site below moves to the call's run access. An executor greps
`rg "\b(self|engine)\.(runtime|control)\." crates/agent/src/{functions,agent,confirmation,runs}.rs`
and classifies each hit (R2-M2); the known ones:

- `functions.rs`: `loop_setup` (records), `context` on the signal run (get,
  records), `primary_loop` (records), `recovery_loop` (records; start per
  decision 13), `start_guard`'s joined-child read, `launch` (records),
  `settle_child` (records, child get), `reviewed` (records: consumed
  events), `confirm_request`/`resolve_review`, `effects_started` (children
  list, and each child's call records from `records`: run access has no
  call journal, the `Record::Call` entries are the journal; R2-L),
  `signal_transcript` (records), `unanchor`, the child checks by parent,
  `guard_context_of` (R2-L).
- `agent.rs`: `loop_of` (records: the guard chain), the guard's own-run check,
  the worker reads, `worker_runs` and `read_run` (records, children list),
  and the `self.runtime`/`self.control` reads among them (R2-M2).
- `runs.rs`: `start_named_child` step 0 (child get: the `ChildStarts` seam
  gains `run(id)`, R2-L), `decide` (definitions: stays on the engine's
  runtime, served by the guard store from the real store, with the overlay).
- Stay on `engine.runtime`, by design: `deliver_event` (delivers into another
  signal's run; mocked in a sandbox, decision 6) and `confirmation::pending`
  (other signals' cards; in a sandbox it reads only the sandbox db's cards,
  so it makes no run read) (R2-L).
- `flows::builtin_version(&Runtime, ..)` used by `start_guard` and fallbacks:
  definitions only, unchanged.

Not handler code, unchanged: boot, the reconciler, the drain, the supervisor's
quiescence wait, `start_named_root` live.

## Contract and model

Invariants (agent side; tests below):

- **D1 Nothing persists.** After any try: the real run store, agent.db (all
  tables except `workflow_meta.tried`), MCP (no non-read-only call reached
  the real client), memory, skills, schedule and the trace mirror's signal
  traces are unchanged. Exception stated: `code_agent`'s workspace (decision
  15).
- **D2 Tried only by a completed try, only for drafts whose runs all
  completed.**
- **D3 A try never saves or activates** (sandbox `save_workflow` /
  `activate_workflow` are mocked writes).
- **D4 Confirmations** follow `confirm`: accept → the call runs (a write:
  mocked), decline → declined, expire → expired; each through the real guard
  and `resolve_review`; no card edit reported.
- **D5 Drafts only in the try:** a live signal handled during a try runs the
  active versions.

No Quint change (the engine's X1 covers the engine side; ouro 07 extends its
check).

## Failure modes

- A provider outage during a try: the try faults; nothing marked tried.
- Timeout: `timed_out`; nothing marked.
- A draft set aside in the try (paused name, or its digest is the tripped
  one): the named start falls back as live; the report shows the fallback
  line; not tried.
- A missed handler call site: the guard store refuses it, the try faults with
  "sandbox: use the call's run access" (decision 3).
- Cost: real model turns; one try ≈ one signal's tokens.

## Tests

Restore, failing first where the step adds behaviour:

1. `pending/sandbox.rs` (7 tests) → `tests/sandbox.rs`, adapted to the engine:
   the draft chain with real reads and mocked writes (its routed loop's path
   includes `input`, `activity`, `pending_confirmations`, `preselect`: R-H3);
   confirmations answered through the real guard (accept, decline, expire,
   under 4 s); a worker inside runs dry and nests in the trace; empty signal
   discovers types; signal type check; a jq error names the expression and
   value; a `signal` override draft runs in place of the built-in dispatcher.
2. `pending/flows.rs` `a_dry_run_records_the_writes_and_changes_nothing`
   (no-signal mode: `steps`, `version`, `writes[].task`).
3. `pending/workflow_kinds.rs` `a_semi_deterministic_workflow_saves_tries_and_runs_on_a_scheduled_signal`
   (the builder session's chat: R-L4).
4. `pending/skill_index.rs` `dry_run_generates_without_writing_memory` (and
   the file, if only its dry-run test kept it parked).

New:

5. D1: snapshot the real run store, agent.db tables and the fake MCP's write
   log before and after a try that writes, confirms and starts a worker;
   equal except `workflow_meta.tried`.
6. D2/R-M4: a faulted try and a timed-out try mark nothing; a completed try
   marks only drafts whose runs all completed; a handed-over saved draft and
   a faulted worker-loop draft are not tried; an unreached draft is
   `ran: false`.
7. D3: a draft whose model calls `save_workflow`/`activate_workflow` changes
   no version and no activation.
8. D5: a live signal processed while a try is in flight (a held model turn)
   runs the active version.
9. Activation end to end: save → try → `activate_workflow` succeeds; without
   the try it is refused.
10. R-H2: a try whose loop has two guarded rounds and a worker completes in
    under 5 s.
11. R-H4: a route draft of a name never activated runs and is tried; a draft
    that narrows `when` is not matched by a signal only the active version's
    `when` matched.
12. R-H5: after v3 of a name trips, draft v4 runs in a try, is tried and
    activates; a draft of a paused name is set aside with the reason.
13. R-H6/D4: the verdict per answer (the call ran; declined; expired) and no
    `edit_telegram_message` in the writes.
14. R-M2/R2-H2: a draft chain whose loop faults: the report's `recovery`
    tree shows the recovery loop's run and its writes (mocked); a try whose
    signal completes has no `recovery`.
15. R-M5: an active tripped `signal` override not listed as a draft: the try
    runs the built-in with the fallback line.
16. R-M6: writes in order across YAML, model and handler sources.
17. R-L5: `try_workflow` called by a model inside a try answers "not
    available inside a dry run".
18. R2-M3: a draft whose YAML has `call: start_child {name: <other draft>}`
    starts the other draft, not its active version.
19. R2-M2: every sandbox test asserts `sandbox_refusals` is absent (zero);
    one test leaves a deliberate site on `engine.runtime` behind a test-only
    hook and sees the count.

Mutants the suite must catch (by hand, once, on a scratch copy; only these
design rules, no blanket mutation run): the overlay
ignored by `active_versions` or `decide`; `decide` ignores the digest rule;
`mark_tried` without the completed check; `confirm_request` writes no card
row; `resolve_review` edits the card in a sandbox; `DryMcp` dispatches a
write; a handler site left on `engine.runtime`.

## Rollout

1. ouro design 07 implemented and released (0.3.0); the agent pins it and
   adapts to its breaking fields.
2. This step on `feat/engine`; `workflow-author` skill: the try section
   checked against the report shape (field names), no behaviour change.
3. LOG.md entry (decision 12's revision of 01); CLAUDE.md "dry runs"
   paragraph points at the engine's dry run.

## Open questions

- None blocking. Reversible: decision 11's input for the no-signal mode may
  change with step 5's fire input; decision 13 (recovery as a host child only
  in a try).
