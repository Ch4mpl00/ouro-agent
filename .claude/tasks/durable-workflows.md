# Durable workflows: run state, waits and user input

**Status:** in-progress. The durable core is built and used by saved workflows
**Open work:** #44
and the agent loop (see "Done"). Open: external events and user replies as
waits, a durable inbox with MCP lease/ack, atomic admission of scheduler fires,
and a user-facing inspect/cancel/resolve-unknown API.
**Priority:** P3
**Area:** agent / workflow / supervisor / persistence
**Created:** 2026-10-16

## Context

Agreed 2026-10-16: the process builder grows on the agent side, on top of the
saved OWF workflows (`crates/agent/src/flows.rs`, `durable.rs` over
`crates/owf`). MCP provides tools, data and delivery of external events; it
does not run workflow DSL or know a run's position.

Needed: long tasks, continuation after restart, waiting for time, an external
event or a user's reply. An ordinary LLM step is a direct await in the
executor; no signal/callback per model step.

Contract and boundaries: `crates/owf/README.md`. Invariants, including the
requirements for the future event integration: `crates/owf/src/durable.rs`.

## Ownership

- **Agent workflow runtime** owns definitions and versions, runs, step
  results, conditions/loops, waits, retries, reply correlation and
  continuation, in the agent's own DB (agent.db).
- **MCP** owns integrations, datasets and their invariants, guarded HTTP, the
  signals queue and external pollers. It never stores execution position and
  does not understand a workflow's bindings, skills or checkpoints.
- **A signal** is an event with a stable id, type, payload and origin. An
  optional correlation/run id is opaque to MCP; the runtime links an event to a
  stored wait. A shared event contract is needed, a shared workflow format
  between processes is not.
- **Cron** is the agent's (moved 2026-10-18, `crates/agent/src/schedule.rs`):
  rows in agent.db, a 30 s tick hands the fire straight to the supervisor,
  a task with `workflow` runs the saved process. A durable time wait belongs to
  the runtime and is restored from the stored deadline (recovery worker), not
  from a sent signal.

## Done

- `crates/owf`: replay of completed steps and contexts, separate checkpoints of
  call replies, stable paths for iterations/then/retry/functions/fork, absolute
  deadlines, stored retry decisions and the `compete` winner.
- Delivery via `metadata.delivery: at_least_once | at_most_once`. An unknown
  outcome of an at-most-once call stops the run with `outcome_unknown`; proof
  that it did not execute allows a retry. The MCP durable adapter never
  retries `tools/call` silently. LLM adapters are at-least-once only (their
  internal retry has no single-attempt API yet).
- agent.db: `workflow_runs` and `workflow_records` (WAL/FULL), pinned
  definition/input, lease with heartbeat and fencing on every commit. The
  supervisor uses the durable runner; the recovery worker resumes due waits and
  expired leases (up to 4 runs in parallel). Shutdown leaves a run resumable;
  an explicit cancel is terminal. Interrupted runs restart with backoff from 2 s
  and fail after `MAX_INTERRUPTIONS` (10) in a row; a new run is admitted
  already claimed (`RunStore::start`); `Interrupted` in the supervisor does not
  start `recovery`.
- Write-call retries (2026-10-18): the key of a protected call (`Write` or any
  `at_most_once`) is run id + path with every enclosing `try` attempt replaced by
  the first, so a `try` retry is not a new logical call; `for` iterations,
  `then` entries, calling functions and `fork` branches stay distinct. The key
  is built from the attempt prefixes the interpreter itself executes and stored
  in the call record (`Record::Call::logical`). An early attempt's success is
  returned without sending; a tool error or lost reply is sent again per the
  delivery policy; different arguments under the same key are a fatal
  `conflict`; an at-most-once call with unknown outcome or a tool error still
  blocks the retry.
- Tests: crash tests before/after commits, real SIGKILL of a child worker at
  eight points per delivery policy (16 scenarios, independent fsync journal of
  effects, rollback and `integrity_check` verified), durable property tests
  (`PROPERTY_RUNS=256 cargo test -p owf --test durable`), `cargo test -p agent
  --test durable`.
- The agent loop and the `signal` workflow run on this machinery
  (`agent-loop-on-owf.md`).

## Open: acceptance

### Waits and user input

- Support waiting for a deadline, an external event and a user's reply
  (the interpreter rejects `listen` today). A wait is stored and frees the
  worker.
- A sent question is linked to run/step and chat/thread/message ids. An
  incoming reply is accepted only in that context and checked against the
  expected format; an ambiguous reply asks for clarification.
- A repeated event never continues a step twice. A fast reply that arrives
  before the question's message id is saved is not lost (the event stays
  available for re-matching). Early, late and unrelated messages have defined
  behaviour.
- Timeout, cancel and reply compete for one wait; exactly one transition ends
  it. A cancelled run is never resumed by a late reply.
- Accepting an incoming event is persisted on the agent side before its final
  acknowledgement in MCP. The current destructive dequeue (`get_next_signal`)
  does not guarantee that: add a lease/ack or an equivalent delivery protocol.
  The guarantee starts after admission today; the dequeue before it has a gap
  (a crash after the pop loses the signal).
- On continuation the LLM gets the needed stored data; no live session,
  unbounded history or busy worker for the whole wait.

### Builder and harness independence

- Evolve validate, preview, version save, run, inspect and cancel; design the
  exact API before implementing (partly there: `save_workflow`,
  `try_workflow`, `read_run`).
- The executor depends on injected interfaces (tools, model calls, agent
  delegation), not directly on `Engine`; the author (compiler/agent) stays
  separate from the runtime. An ordinary LLM/Jev step is a direct await through
  a provider adapter; a full agent step delegates through the agent executor
  and stores its result. Replacing the authoring agent needs no rewrite of
  processes; replacing the harness inside an agent step needs an adapter.
- Managing processes should be reachable by a future external agent, if
  needed through a thin MCP interface to the agent-owned service; state
  ownership and DSL interpretation stay in the agent.

### Verification

- A run survives restart before/after a step, during a user wait and a
  deadline; stored results are not recomputed.
- A reply continues the right run with several tasks in one chat; duplicate,
  fast/late reply and cancel are covered by tests.
- Other signals are processed while one task waits.
- A model error retries by step policy; an ambiguous external side effect never
  causes a blind repeat.
- Use fake clock/providers/tools and a test DB.

## Notes

### Call policy decisions (2026-10-18)

Call accounting belongs to the agent's durable layer (agent.db) so it works
with third-party MCP servers unchanged; a mandatory effect journal in MCP was
rejected. This chooses retry semantics, it is not universal idempotency.

- A call has a stable id `runId + reference + iteration path`
  (`run-1:/do/1/loop[3]/do/0/send`). The argument fingerprint is checked
  separately; changed arguments must not turn recovery of an old call into a new
  external effect.
- `at_least_once` (default): the reply becomes a checkpoint only after durable
  save; without a saved reply recovery may repeat the call (duplicates
  possible). A tool error is a reply handled by DSL error/retry policy.
- `at_most_once`: with an unknown outcome the call is never repeated (recovery,
  DSL retries, transport retries). Retry is allowed only with proof of no
  execution (request not sent, or rejected by the server before the tool ran);
  timeout, a disconnect after send, or a tool error are no such proof, so the
  transport adapter must tell `not_executed` from `outcome_unknown`.
- Plain MCP `tools/call` has no separate acceptance acknowledgement, so the
  agent persists the intent before sending. On recovery without a reply the
  outcome stays unknown. A crash between the intent write and the send can lose
  an execution: the accepted price of at-most-once.
- Pending/unknown is not success: dependent steps get no invented output; an
  explicit error/inspection path is needed. SSE recovery to read a reply differs
  from replaying `tools/call`.
- Grounds: MCP spec 2025-11-25, "Calling tools" and "Transports / Sending
  messages to the server".

### Design choices from the 2026-10-18 estimate

- Replaying the interpreter with stored results of finished steps is enough;
  no serialisable stack and no jump to a position.
- Store current durable state, not a history of snapshots: step records needed
  for replay, context, waits and unfinished operations (one latest call result
  is not enough). Several tables/rows updated transactionally; big results
  stored apart from the hot state; waiting tasks hold no live session. Deleting
  old snapshots must not remove data still needed for replay or effect dedup.
- Version the state format and executor semantics together with the immutable
  definition version.
- Hard parts: atomic transitions, unfinished external calls, wait/timeout/cancel
  races, and the fate of unfinished runs after an interpreter update.
- Telegram reply correlation, reliable signal delivery and MCP-write
  idempotency need our integration even with an off-the-shelf runtime.

First application scenarios: #36, #33,
#37.
