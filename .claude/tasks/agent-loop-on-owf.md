# The agent loop as an OWF workflow

> **Superseded (2026-10-26).** The runtime this record describes (`crates/owf`,
> `crates/agent/src/durable.rs` and their tables) was replaced by the ouro
> engine and removed (`docs/designs/01-engine-migration.md` … `07-remove-owf.md`).
> Kept as history; its open work is to be re-triaged against the engine.

**Status:** in-progress. The loop, the `signal` workflow, durable sub-agents,
**Open work:** #43
cleanup and `read_run` are done (2026-10-20); two fault-handling gaps remain
(see "Still open").
**Priority:** P1
**Area:** agent / owf / supervisor / durability
**Created:** 2026-10-20

## Context

The ReAct loop used to live only in process memory: `get_next_signal` consumes
the signal before the session starts, so a deploy or a crash in the middle of a
session lost it silently, and a provider outage (429) failed it at once. Saved
workflows already have everything a durable session needs (`durable.rs` +
`owf::durable`): pinned definitions, fenced leases, the recovery worker,
backoff, intent/result call records, durable waits.

Decided 2026-10-20: the loop itself becomes an OWF workflow on that machinery,
not a second durability layer in Rust. It also makes the loop data the agent
can later rewrite (Harness^2, arXiv:2610.14224) and gives skill procedures
(#34) a loop to embed.

## Design (as built)

- **No new tables.** The transcript and working memory travel in the workflow
  context; the step records the interpreter already writes are the checkpoints
  ("the last successful turn" is the last task record). Cost: the context is
  copied into every task record of a run.
- **Host functions** (`crates/agent/src/agent.rs` §4, catalogued in
  `flows.rs`): `agent_turn` (one generation from the transcript), `tool` (one
  tool call by name), `agent_settle` (`finish`, the round's errors, results
  stored in working memory become the tool replies). Control (the loop, rounds,
  limits, ending) is YAML: `crates/agent/workflows/agent-loop.yaml`.
- **Interpreter additions** (`crates/owf`): per-invocation effect
  classification (at-most-once for `tool` on a write); an unknown at-most-once
  outcome may become a host-defined value instead of a fatal fault (the model
  is told the call may have run; it is never sent again); `for` with
  `metadata: { concurrency: n }` runs iterations at once and outputs results in
  order (a round's calls run in parallel).
- **Supervisor**: the primary loop of a signal is admitted as a durable run of
  `agent-loop`, so a restart resumes it. Workers and recovery run the same
  definition in process (`flows::run_loop_here`).
- **Override + breaker**: the version the agent activates under `agent-loop`
  drives every loop (no dry run needed); three faults in a row of that version
  put the built-in one back (`agent_loop.breaker` in the memory KV).
- **Sub-agents as child runs.** In a durable loop, `invoke_sub_agent` starts the
  worker as a durable run of its own, `<parent loop id>__sub<i>.<index>/loop`,
  keyed by the parent's turn and the call's place in the round. That call is no
  longer at-most-once: a resumed parent calls it again and joins the child's
  run (or reads its outcome) instead of starting a second worker; the child's
  own writes stay at-most-once. The recovery worker may resume the child first;
  the parent then waits (`flows::run_child`). A lease dropped before its attempt
  ended is released at once.
- **The `signal` workflow** (`workflows/signal.yaml`, host functions in
  `supervisor.rs` §2b): routing, context gathering and the primary loop's
  options are YAML. The run is in process and untraced itself. The override +
  breaker is general (`flows::built_in`, `definition`, `record_outcome`;
  `signal.breaker`). The failure report is its `try`/`catch`: YAML owns the
  prompt and loop options; `signal_transcript` and `recovery_loop` provide the
  mechanisms, with cancellation/abort/session guards and the original failure
  preserved.
- **Cleanup.** `workflow_runs.ended_ms` (migration 0008) stamps a run's end; the
  recovery worker deletes the records of runs that ended more than
  `RECORDS_KEPT_MS` (7 days) ago, hourly, and keeps the row with its outcome so
  a swept run is never resumed.
- **Run history.** `read_run` lets every loop inspect durable `agent-loop`
  evidence: without an id it lists its workers with tasks and outcomes; with an
  id it shows a bounded transcript paged over assistant turns. Missing
  in-process runs and swept transcripts are explicit; other workflow names are
  refused.

## Still open

- A provider outage (429) faults the run into recovery; it should back off as an
  interruption (the way an interrupted saved-workflow run does).
- A resumed run that faults is only logged; it should get a recovery report.
- The `signal` workflow can call only its own functions; letting it call the
  general catalog (a model step to classify, an MCP read) needs task spans that
  don't change the signal trace's shape.

## Acceptance

- A peer message survives a restart in the middle of a round: no repeated
  generation, reads rerun, a write that was in flight comes back to the model as
  "may have run", never resent.
- Tool calls of one round run concurrently.
- Same traces as before for the judge and the activity journal (`agent_loop`,
  `iter-N`).
- Finished runs' records are swept.
- A restart during a sub-agent resumes the sub-agent's run; the parent joins it.
- (open) A 429 backs off instead of faulting; a resumed faulted run is reported.
