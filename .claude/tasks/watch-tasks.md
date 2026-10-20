# Watch tasks: long-running monitoring with carried state

**Status:** design agreed 2026-09-10, re-based on saved workflows 2026-10-20;
not started. The generic half shipped as saved workflows (compile once,
dry-run, execute many); what is missing is the monitoring discipline around a
recurring task (below).
**Priority:** P2
**Area:** agent / scheduler / workflows
**Created:** 2026-09-10

## Context

The agent can be told "do X at time T" (`schedule_task`, cron, agent-owned in
`crates/agent/src/schedule.rs`). It cannot yet be told "watch X until it
changes or ends, and tell me when something happens". Motivating case: *track
a flight*; "check every couple of minutes and send me updates". That needs:

1. **Carried state.** Tick N must know what tick N-1 saw, or the agent
   re-reports the same status forever. A scheduled task has no state column and
   every fire starts from a blank session.
2. **A stop condition.** A flight lands, a cron doesn't. Nothing expires a task
   but the user remembering to say stop. A forgotten watch is a money leak.
3. **A cost model that survives a 1-minute cadence.** A watch is mostly "nothing
   changed" (about 99% of ticks for a flight). Those ticks should cost nothing
   and never cost an LLM plan or a skill preamble.

## What already exists

- **Saved workflows run on a schedule without a model deciding the steps**
  (`schedule_task(workflow: ...)`, `flows.rs`): the planner/compile-per-fire
  problem of the original design is gone. Authoring is the `workflow-author`
  worker: `save_workflow`, `probe_function`, `try_workflow` (a dry run: reads and
  model steps run for real, writes are only recorded), activation only after the
  user agrees.
- **Effect annotation:** every workflow function is `Read` or `Write` (an MCP
  tool without `readOnlyHint` is a write), so dry runs are safe.
- **Durable runs** with at-most-once writes and recovery (`durable-workflows.md`).
- A workflow can end without sending anything, so a silent tick is expressible
  (a `switch` on "changed?"); state can be kept in unified memory (a document or
  the journal) or the agent's memory KV (`set_memory`).

## Decisions kept from the design (2026-09-10)

- Keep **adaptive interval**, but as a mutable interval on the task, not a
  self-rescheduling chain: a chain is not self-healing (one forgotten
  `schedule_task` kills the watch silently and permanently; cron re-fires
  regardless). This needs an `update_scheduled_task` tool (there is only
  schedule/list/cancel).
- **Deterministic probe, LLM only on delta.** The tick runs the saved
  workflow, normalises the observation and diffs it against the carried state
  *in code*. Unchanged: write state, send nothing, no signal and no session.
  Changed: one human sentence (a model step) and a send.
- **Probe contract:** a probe workflow may use only read functions and must bind
  a result; delivery belongs outside the probe, so a silent tick is provably
  silent. Enforce at save/activation time (today any workflow may contain
  writes).
- Rejected: one long-lived agent session sleeping between checks (context grows
  without bound, nothing survives a restart, a sleeping session parks the whole
  signal queue). Kept as a later idea: **push instead of poll** (webhook-fed
  probes) should be a new probe kind, not a second system.
- A saved workflow's tools can disappear (an upstream dropped, a toolset
  rescoped): validate again on load and degrade to a human-visible failure, not
  silence.

## Acceptance

- The user can say "watch flight X" in Telegram and a watch is created, with the
  interval and the stop condition echoed back once.
- State survives restarts; a tick sees the previous observation without an extra
  tool call from a model.
- An unchanged tick sends no message and runs no LLM session.
- A changed tick produces exactly one short message in the user's language.
- The watch stops on its own: stop condition met, `expires_at` reached or
  `max_ticks` exhausted (the last two enforced in code, not by the model).
- A task's interval is mutable (`update_scheduled_task`), with a **floor** set
  by configuration, not by a number the model picks (a 1-minute cadence against
  a third-party API gets rate-limited or blocked).
- The failure mode is loud: a probe that errors N times in a row reports once and
  marks the watch failed instead of dying silently.
- "What are you watching now?" is answerable (a list of active watches) and
  stopping one is a normal request.
- A probe workflow containing a write function is rejected at activation.

## Notes

- Delta-only vs heartbeat: a message every 2 minutes saying "still airborne" is
  noise. Report on change, plus an optional low-rate heartbeat (for example
  hourly) and always on the terminal event.
- Queue pressure: the supervisor handles one signal at a time; a silent tick
  that never reaches the queue is what keeps a 1-minute watch from delaying
  Telegram replies.
- Report dedup belongs on a stored `last_report`, not on chat history.
- Design question left open: store watch fields (`expires_at`, `max_ticks`,
  `tick_count`, `state`, `last_report`, failures) as columns on the
  agent-owned `scheduled_tasks` row, or in a separate `watches` table pointing
  at a task. Prefer extending the existing task; both are agent.db (agent
  domain state; MCP is not involved, per `durable-workflows.md`).
- Prototype without schema changes: a recurring task whose prompt names a
  unified-memory document (`watch/flight-ps101.md`): `read_doc`, compare,
  `append_doc`, report on delta, `cancel_scheduled_task` at the end. Useful to
  learn the report cadence, but it pays an agent turn per tick.
- Related: `durable-workflows.md` (runtime), `agent-jit-workflows.md`,
  `agent-working-memory.md`, `done/unified-memory.md` (where state documents
  live).
