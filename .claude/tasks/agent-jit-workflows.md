# JIT for agents: compile recurring ReAct runs into workflows

**Status:** pending (idea, not designed)
**Priority:** P3
**Area:** agent / workflow / supervisor
**Created:** 2026-10-03

## Context

A JIT compiler for the agent. The ReAct loop is the interpreter: flexible and
expensive, and every run of a familiar task re-derives the same tool sequence
from scratch. Run history already records what the loop did (Langfuse and the
local `traces` mirror in agent.db). Once a kind of task has been solved the
same way enough times, that path could be compiled into a saved OWF workflow
(`crates/agent/src/flows.rs`, authored through `workflow-author`) and replayed
without an LLM in the hot path.

Rough shape:

- **Profile.** Mine run history for task kinds whose ReAct traces keep taking
  the same tool path, possibly with a few variants.
- **Compile.** Turn the common path into a saved workflow. Where traces
  diverge, the workflow gets a branch point instead of a fixed step.
- **Route branches with Jev.** A branch point is a typed question over program
  state ("which of these 3 paths?"), the kind of decision `jev-system-one.md`
  answers in ~100 ms with a calibrated probability (the `choose` function),
  instead of a full LLM turn.
- **Deoptimise.** When Jev's confidence is low, a precondition fails, or a
  step returns something unexpected, fall back to the agent loop for this run,
  as a JIT falls back to the interpreter. The run still completes, just slower.

## Acceptance

Not defined yet; nothing is decided. Answer the questions below first.

## Notes

Open questions:

- What counts as "the same task kind": signal source, a classifier over the
  request, or clustering of traces?
- How many consistent runs before a path is worth compiling, and how to tell a
  stable path from luck?
- Where a compiled workflow comes from: generalised from traces, or authored
  by the `workflow-author` worker with traces as examples?
- Mid-run deopt: can the agent loop resume from the workflow's context, or does
  it restart the task? Side effects already fired must not be replayed
  (CLAUDE.md: never replay tool side effects automatically). The existing
  handover rule (before any write: agent loop; after a write: `recovery`)
  is the starting point.
- How a compiled workflow is invalidated when tools, skills or the world
  change (the same drift problem as in `watch-tasks.md`).
- How to measure that it helps: cost and latency per run vs. answer quality
  against the ReAct baseline (golden sets, `per-node-judge-and-improver.md`).

Related: `watch-tasks.md` (compile once, execute many, the manual version of
this), `jev-system-one.md` (plan-cache validity is the routing primitive),
`durable-workflows.md`.
