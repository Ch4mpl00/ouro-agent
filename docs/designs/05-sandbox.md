# 05. Step 4: dry runs on the engine (`try_workflow`)

Status: draft · Overview: [01](01-engine-migration.md) step 4, decision 16 ·
Epic: #62 · Engine prerequisite: ouro design 07 (release 0.2.3) ·
Decisions log: [LOG.md](LOG.md)

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
  the named drafts in place of the active versions. Model turns, reads and the
  guard are real; writes are recorded and answered with `MOCK_REPLY`;
  confirmations are answered on the spot (`confirm`: accept, decline,
  expire); waits do not sleep. It reports a tree of runs (path of steps, the
  case each switch took, turns with reasoning/text/calls, each call's verdict,
  card answer and reply, how each loop ended) and marks every draft that ran
  as tried when the run completed.
- **without `signal`:** a workflow that takes no agent turn runs once on a
  scheduled fire's input, same mocking, path and writes reported, marked tried
  when it completed.

Scenario: the workflow-author worker saves `tg-loop` v2 (routed on
`source: telegram`). It calls `try_workflow(name: "tg-loop", signal: {source:
"telegram", content: "remind me at 9"}, confirm: "accept")`. The signal runs
through the active `signal`, routes to the draft `tg-loop` v2, takes real
model turns; its `send_telegram_message` is recorded, not sent; the guard
asks for a confirmation of `schedule_task`, answered "accept" at once. The
report shows `tg-loop` draft v2 ran and completed; v2 is now tried; the
parent shows the user what it will do and activates it after they confirm.

## Decisions

Maintainer (earlier sessions, not reopened): the sandbox from #56 (reads real,
writes mocked with an explicit marker, auto-confirm, drafts, the builder
judges, nothing kept); "the runtime informs, the agent decides"; no channel
knowledge in Rust; behaviour lives in YAML and skills; the engine only executes
and knows no read/write (ouro#13: the host's mock rule); design 01 decision 16
(a dry run is the agent's sandbox engine on the engine's dry run, with a dry
registry, not run-time `sandbox()` branches).

Elephant (logged in LOG.md, reversible):

1. **The sandbox is a second agent `Engine`, built per try and never
   started** (no worker, no drain, no reconciler). Its deps are the dry ones
   from `main` (`DryMcp`, `DryMemory`, `DrySkills`, `DrySchedule`, a nested
   tracer), an in-memory `AgentDb` and, as its run store, a **read-only view
   of the real run store**. Its registry is built over itself
   (`functions::registry`), so every handler in a dry run uses the dry deps.
2. **It runs on the engine's dry run** (`Runtime::dry_run_opts`, ouro 07) of
   the sandbox engine's runtime: the private memory store holds every run,
   children included; durable time advances without sleeping; the host's
   observers stay silent. The real run store is only read (definitions);
   the read-only view makes any stray write fail loudly.
3. **Handlers reach their run tree through `FunctionContext::runs`** (ouro
   07), never through `engine.runtime`/`engine.control`, wherever they read
   their own run, a child or deliver an event. This is a refactor of every
   such call site (live behaviour unchanged: live `cx.runs` is the same
   runtime). Calls that are about the whole store and not the run's tree
   (the reconciler, the drain, the supervisor's quiescence wait, boot) stay
   on `engine.runtime`/`engine.control`: they never run inside a dry run.
4. **Drafts are chosen at the named starts, not activated.** A
   `SandboxDrafts` map (name → version) on the sandbox engine; `decide`
   (runs.rs) returns the draft's version for a listed name instead of the
   active one, the same pinned start follows. Nothing is saved or activated
   anywhere. The root (`signal`, or the draft itself in the no-signal mode)
   is the YAML of the draft when it is listed, else the active version's.
5. **The mock rule (the engine's `Mock`)** marks a call whose function
   `agent::tool_effect` classifies as `Write` (an MCP tool the server does
   not mark read-only, an unknown name, or one of `SYNTHETIC_WRITES`), except
   `invoke_sub_agent`, which starts a dry child (workers run inside a try, as
   on `main`). Host functions (`agent_turn`, `tool`, `start_guard`, …) are
   never marked. One classification, the guard's; no new list. Its response
   is `MOCK_REPLY`. Writes made inside handlers (`tool` dispatching a model's
   call) never reach the engine as calls. In a sandbox, `tool` answers a
   model's call that the same rule marks with `Sandbox::mock` (recorded,
   `MOCK_REPLY`) before dispatching it; `DryMcp` and the dry stores (which
   refuse or keep in memory) stay as a second layer for any write a handler
   makes on its own, as on `main`.
6. **Confirmations through the real guard.** The sandbox's `confirm_request`
   handler sends no card. For `accept`/`decline` it delivers the
   corresponding button event to its own run (`cx.runs.deliver`, targeted)
   before it returns, so the guard's `listen` takes it like a real press; for
   `expire` it delivers nothing and the listen's 24 h timeout passes at once
   (time advances). `guard.yaml` is unchanged.
7. **Steps come from an observer of the dry run's own**
   (`DryRunOptions::observer`, ouro 07): the agent's `Path` observer from
   `main` (task entered/skipped, switch case, fault where it happened,
   retry), per run id, capped (200 steps, the rest counted).
8. **The run tree, versions and outcomes come from `DryRun::runs`** (ouro
   07); turns, verdicts, card answers and the replies of calls made inside
   `tool` come from the sandbox's records (`main`'s `Sandbox::turn/call/press`
   hooks, called by the handlers when their engine is a sandbox). A write is
   reported from both sources: `DryRun::mocked` (YAML calls) and `DryMcp`'s
   records (calls inside handlers), each with its run.
9. **`tried` is marked** (`WorkflowMeta::mark_tried(name, digest)`) for every
   listed draft whose (name, digest) appears in `DryRun::runs`, only when the
   dry run's status is completed. The root counts as the root draft's name
   (the engine names the root `dry-run`; the sandbox maps it). This is the
   only write a try makes outside the sandbox.
10. **No signal → the draft alone.** `try_workflow(name)` without `signal`
    dry-runs the draft itself as the root, on the input a scheduled fire
    admits (`signal_input` with source `scheduler`, empty content), refused
    with `main`'s two messages for a workflow that handles signals or takes
    agent turns. Step 5 may change the fire's input; this mode follows it.
11. **Bounds:** `DRY_RUN_TIMEOUT` (300 s, as `main`) cancels the dry run's
    stop token; the engine's `max_tasks` and the jump budget bound it too.
    One try at a time per sandbox; tries are not queued (a second concurrent
    try builds its own sandbox).

## Non-goals

- No change to `guard.yaml`, `signal.yaml`, `agent-loop.yaml`.
- No replay-based tries (ouro `replay_definition`); the optimizer (#60) is
  unchanged.
- Schedules (step 5): `schedule_task(workflow:)` is still unavailable; a try
  of a scheduled pipeline uses the no-signal mode or a scheduler signal.
- No deterministic model turns: a try spends real model tokens, as on `main`.

## Design

### The sandbox engine (`sandbox.rs` §1–2)

```rust
pub struct Sandbox {             // what one try records
    answer: Answer,              // accept | decline | expire
    drafts: BTreeMap<String, Draft>,   // name → {version, digest, yaml}
    record: Mutex<Record>,       // turns, calls, answers, writes, steps
}

fn sandbox_engine(real: &Arc<Engine>, sandbox: Arc<Sandbox>, root: Arc<dyn Span>)
    -> anyhow::Result<Arc<Engine>>;
```

- deps: `providers`, `presets`, `skills`, `codex`, `decisions`, `env` shared;
  `mcp` = `DryMcp`, `memory` = `DryMemory`, `skill_store` = `DrySkills`,
  `schedule` = `DrySchedule`, `tracer` = `Nested(root)`;
  `db` = `AgentDb::open(":memory:")` with the rows a dry run reads copied in:
  the MCP tool snapshot and `workflow_meta` (read by the named starts and
  fallbacks); `store` = `real.deps.dry_store` (below).
- `EngineDeps` gains `dry_store: Arc<dyn RunStore>`: a read-only view of
  `store`, built once by the composition root (`sqlite::open_read_only` on
  `engine.db`; `memory::read_only_view` in tests).
- `Engine` gains `sandbox: Option<Arc<Sandbox>>`; the handlers that differ in
  a dry run ask it: `confirm_request` (decision 6), `tool`'s write answer
  (decision 5), the loop's turn/tool/settle records (decision 8), `decide`
  (decision 4). These are the only branches.
- Tracing: every scope a handler asks for (`scope_for`) is a span under the
  try's root span (`Nested`), so the dry run shows in the builder's own trace
  and never as a signal of its own (the activity journal ignores it, as
  `activity.rs` already says).

### `try_workflow` with a signal (`sandbox.rs` §5)

Arguments as today (`synthetic_tools.json`): `name`, `version?`, `signal`,
`drafts?`, `confirm?`. Steps:

1. `signal == {}` → `signal_types` (unchanged from `main`, `list_signals` +
   `scheduler`), no run, no draft needed.
2. Resolve drafts: `name` at `version` (latest saved by default) and each of
   `drafts` at its latest saved version, from the engine's `definitions`
   (versions with YAML) and `workflow_meta`; `guard` refused; a name with no
   saved version is refused except `name` itself when it is a built-in tried
   as it is (`main`'s rule). Each draft passes the gate (`flows::gate`).
3. Check `signal.source` against the types (unchanged).
4. Build the signal root: the same input and host data the supervisor builds
   (`run_active`'s `build`, factored into one function both call), with
   signal id 0 and its evidence put into the sandbox's in-memory db under the
   dry root's run id.
5. `sandbox.runtime.dry_run_opts(root_yaml, input, mock, {host, observer:
   Path}, stop)` under `DRY_RUN_TIMEOUT`.
6. Mark tried (decision 9); build the report; end the root span.

### `try_workflow` without a signal

Decision 10: the draft is the root; same sandbox, same mock, same report
(one run unless the draft starts children).

### The report (`sandbox.rs` §6)

`main`'s shape, with the run tree from `DryRun::runs`:

```json
{ "name", "status": "completed|faulted|suspended|timed_out", "output", "error",
  "ended", "writes": [{run, function, arguments, via: "yaml|tool"}],
  "drafts": [{name, version, ran}], "confirm", "signal",
  "runs": [{run, workflow, version: "draft vN|vN|built-in", path, omitted_steps?,
            turns?, ended?, children?}],
  "note", "next" }
```

`suspended` = the root's status is `Waiting` (a run waited for something
that never came). Field texts (`note`, `next`) as on `main`.

### Handler refactor (decision 3)

Every handler call site listed by `rg "engine.runtime.(get|deliver|calls|definitions)"`
in `functions.rs`, `agent.rs`, `confirmation.rs` and the named starts in
`runs.rs` (`start_named_child` step 0, `decide`) uses `cx.runs`. The ones that
are not handler code (boot, reconciler, drain, supervisor waits,
`start_named_root`) stay. A test-only guard: the sandbox engine's
`engine.runtime` is over the read-only store, so a missed call site fails a
dry run loudly instead of reading the wrong store silently (a write) — a
missed *read* returns `None`; the restored suites cover each path.

## Contract and model

Invariants (agent side; tests below):

- **D1 Nothing persists.** After any try: the real run store, agent.db (all
  tables except `workflow_meta.tried`), MCP (no non-read-only call reached
  the real client), memory, skills, schedule and the trace mirror's signal
  traces are unchanged.
- **D2 Tried only by a completed try, only for drafts that ran.**
- **D3 A try never saves or activates** (sandbox `save_workflow` /
  `activate_workflow` are mocked writes).
- **D4 Confirmations** follow `confirm`: accept → the call runs (a write:
  mocked), decline → refused, expire → expired; each through the real guard.
- **D5 Drafts only in the try:** a live signal handled during a try runs the
  active versions.

No Quint change (the engine's X1 covers the engine side; ouro 07 extends its
check).

## Failure modes

- A provider outage during a try: the try faults; nothing marked tried.
- Timeout: `timed_out`, the stop token cancels the dry run; nothing marked.
- A draft set aside in the try (its digest is the tripped one): the named
  start falls back as live and the report shows the fallback line; the draft
  did not run, so it is not tried.
- `dry_store` cannot be opened read-only (a fresh engine.db with no schema):
  the agent boots the real engine first (`ready()`), which creates it; the
  sandbox opens after.
- Cost: real model turns; one try ≈ one signal's tokens.

## Tests

Restore, failing first where the step adds behaviour:

1. `pending/sandbox.rs` (7 tests) → `tests/sandbox.rs`, adapted to the engine:
   the draft chain with real reads and mocked writes; confirmations answered
   through the real guard (accept, decline, expire); a worker inside runs dry
   and nests in the trace; empty signal discovers types; signal type check;
   a jq error names the expression and value; a `signal` override draft runs
   in place of the built-in dispatcher.
2. `pending/flows.rs` `a_dry_run_records_the_writes_and_changes_nothing`
   (no-signal mode).
3. `pending/workflow_kinds.rs` `a_semi_deterministic_workflow_saves_tries_and_runs_on_a_scheduled_signal`.
4. `pending/skill_index.rs` `dry_run_generates_without_writing_memory` (and
   the file, if only its dry-run test kept it parked).

New:

5. D1: snapshot the real run store, agent.db tables and the fake MCP's write
   log before and after a try that writes, confirms and starts a worker;
   equal except `workflow_meta.tried`.
6. D2: a faulted try and a timed-out try mark nothing; a completed try marks
   only the drafts in `DryRun::runs`; a listed draft no route reached is
   reported `ran: false` and not marked.
7. D3: a draft whose model calls `save_workflow`/`activate_workflow` changes
   no version and no activation.
8. D5: a live signal processed while a try is in flight (a held model turn)
   runs the active version.
9. Activation end to end: save → try → `activate_workflow` succeeds; without
   the try it is refused (step 3's refusal now reachable to pass).
10. Handler refactor: a dry run whose loop starts a worker and a guard child
    completes (fails before: `unknown run`).

Mutants the suite must catch (run by the executor once): `decide` ignores
drafts; `mark_tried` without the completed check; `confirm_request` delivers
nothing for accept; `DryMcp` dispatches a write; the sandbox engine uses the
real store.

## Rollout

1. ouro design 07 implemented, released as v0.2.3; the agent pins v0.2.3.
2. This step on `feat/engine`; `workflow-author` skill: the try section
   checked against the report shape (field names), no behaviour change.
3. LOG.md entry; CLAUDE.md "dry runs" paragraph points at the engine's dry
   run.

## Open questions

- None blocking. Reversible: decision 10's input for the no-signal mode may
  change with step 5's fire input.
