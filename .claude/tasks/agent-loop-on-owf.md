# The agent loop as an OWF workflow

**Status:** in-progress
**Priority:** P1
**Area:** agent / owf / supervisor / durability
**Created:** 2026-10-20

## Context

The ReAct loop (`agent.rs` `run_until_settled`) lives only in process
memory: `get_next_signal` consumes the signal before the session starts, so a
deploy or a crash in the middle of a session loses it silently, and a provider
outage (429) fails it at once. Saved workflows already have everything a
durable session needs (`durable.rs` + `owf::durable`): pinned definitions,
fenced leases, the recovery worker, backoff, intent/result call records,
durable waits.

Decided 2026-10-20: the loop itself becomes an OWF workflow on that
machinery, rather than a second durability layer in Rust. It also makes the
loop data the agent can later rewrite (Harness², arXiv:2610.14224) and gives
skill procedures (`skill-procedures.md`) a loop to embed.

## Design

- **No new tables.** The transcript and working memory travel in the
  workflow context; the step records the interpreter already writes are the
  checkpoints ("the last successful turn" is the last task record). Cost:
  the context is copied into every task record of a run.
- **Host functions** (in `flows.rs`): `agent_turn` (one generation from the
  transcript), `tool` (one tool call by name), `agent_settle` (`finish`, the
  round's errors, results stored in working memory → the tool replies).
  Control (the loop, rounds, limits, ending) is YAML.
- **Interpreter additions** (`crates/owf`):
  1. per-invocation classification: the host may give one invocation of a
     function its own effect and require at-most-once (`tool` on a write);
  2. an unknown at-most-once outcome may become a host-defined value instead
     of a fatal fault: the model is told the call may have run; it is still
     never sent again;
  3. `for` with `metadata: { concurrency: n }` runs its iterations at once,
     each on the `for`'s input and a copy of the context, and outputs their
     results in order (a round's calls run in parallel).
- **Supervisor**: the primary loop of a signal is admitted as a durable run
  of `agent-loop`, so a restart resumes it instead of losing it. Workers and
  recovery run the same definition in process (`flows::run_loop_here`). The
  in-process ReAct loop is gone (2026-10-20).
- **Override + breaker**: the version the agent activates under
  `agent-loop` drives every loop (no dry run needed); three faults in a row
  of that version put the built-in one back (`agent_loop.breaker` in the
  memory KV).

## Still open

- A provider outage (429) faults the run into recovery; it should back off
  as an interruption.
- A resumed run that faults is only logged; it should get a recovery report.

## Sub-agents as child runs (done 2026-10-20)

In a durable loop, `invoke_sub_agent` starts the worker as a durable run of
its own, `<parent loop id>__sub<i>.<index>/loop`, keyed by the parent's turn
and the call's place in the round (the definition passes `i` and `index` to
`tool`). That call is no longer at-most-once: a resumed parent calls it again
and joins the child's run (or reads its outcome) instead of starting a second
worker; the child's own writes are at-most-once in its run. The recovery
worker may resume the child first; the parent then waits for its outcome
(`flows::run_child`). A lease dropped before its attempt ended (a parent
stopping with its worker in flight) is released at once, so the child is due
on the next start rather than after the lease expires.

## The signal workflow (done 2026-10-20)

`Supervisor::run_active`'s body is the built-in workflow `signal`
(`workflows/signal.yaml`): routing, context gathering and the primary loop's options
are YAML over seven host functions (`supervisor.rs` §2b) that act on the
signal the run serves and trace as before. The run is in process and
untraced itself (the primary loop inside it is the durable part). The
override + breaker of `agent-loop` is now general (`flows::built_in`,
`definition`, `record_outcome`; `signal.breaker` in the memory KV).
Open: the signal workflow can call only its own functions; letting it call
the general catalog (a model step to classify, an MCP read) needs task spans
that don't change the signal trace's shape.

## Cleanup of finished runs (done 2026-10-20)

`workflow_records` of a terminal run are only evidence once it has ended,
and with the transcript in the context they are large. `workflow_runs.ended_ms`
(migration 0008) stamps the end; the recovery worker deletes the records of
runs that ended more than `RECORDS_KEPT_MS` (7 days) ago, hourly, and keeps
the row with its outcome, so a swept run still reports it and is never
resumed.

## Run history (done 2026-10-20)

`read_run` lets every loop inspect durable `agent-loop` evidence on demand:
without an id it lists its workers with tasks and outcomes; with an id it
shows a bounded transcript, paged over assistant turns, including calls,
arguments and replies. The store exposes generic lease-free record and
prefix reads. Missing in-process runs and swept transcripts are explicit;
other workflow names are refused. Worker reporting rules live in the
orchestrator skill, and delegation still returns only the worker's answer.

## Acceptance

- A peer message survives a restart in the middle of a round: no repeated
  generation, reads rerun, a write that was in flight comes back to the model
  as "may have run", never resent.
- Tool calls of one round run concurrently.
- Same traces as before for the judge and the activity journal
  (`agent_loop`, `iter-N`).
- Finished runs' records are swept.
- A restart during a sub-agent resumes the sub-agent's run; the parent joins it.
