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
- **Supervisor**: the primary loop of a signal is admitted as a run of the
  built-in `agent-loop` definition (behind a flag first: `peer`), so a
  restart resumes it instead of losing it.

## Cleanup of finished runs

`workflow_records` of a terminal run (completed, failed, cancelled) are only
evidence once the run has ended, and with the transcript in the context they
are large. Plan: a sweep in the recovery worker deletes the records of runs
that reached a terminal state more than 7 days ago (keeping the
`workflow_runs` row with its outcome), plus a test that a terminal run
without records still reports its outcome and is never resumed.

## Acceptance

- A peer message survives a restart in the middle of a round: no repeated
  generation, reads rerun, a write that was in flight comes back to the model
  as "may have run", never resent.
- Tool calls of one round run concurrently.
- Same traces as before for the judge and the activity journal
  (`agent_loop`, `iter-N`).
- Finished runs' records are swept.
