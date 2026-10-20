# Per-node judge and closed-loop skill improver

**Status:** PAUSED (since 2026-08-31). Everything below is built in
`crates/evals` (ported from TypeScript). Both workers (`judge-worker`,
`improve-worker`) are off in production: judging live prod runs didn't earn its
codex quota, and the direction is golden sets instead. Nothing was deleted: the
code, the schema and the judged corpus in agent.db are intact and the local
trace mirror keeps recording every run. The loop was never validated end to end
on prod (shadow mode only). Revisit, if at all, against golden sets.
**Priority:** P3
**Area:** evals / agent / self-improvement
**Created:** 2026-06-14

## Context

Goal: a closed-loop self-improvement agent. The agent runs; every run's trace is
mirrored into agent.db; a **per-node judge** scores each generative LLM node
against its owner's contract; an **improver** aggregates scores per skill,
authors a patch, gates it by replay, ships it, monitors the live trend and
auto-reverts.

```
agent --trace--> agent.db (local mirror, tee'd with Langfuse)
per-node judge:  per generative node -> score vs that node's owner contract
improver (cron): per-skill scores -> patch -> GATE (replay) -> ship -> monitor -> revert
```

## What exists (Rust)

`crates/evals/src`: `node_judge.rs` + `materials.rs` + `schema.rs` (the
per-node judge), `worker.rs` (judge-worker), `noise.rs` / `sigma.rs` (judge
repeatability), `gate.rs` + `gate_runtime.rs` (replay gate), `improver.rs` +
`improve_cycle.rs` + `improve_worker.rs` + `improver_store.rs` + `monitor.rs`
(the improver), `scores.rs` / `langfuse_api.rs`. Binaries: `judge`,
`judge-worker`, `judge-noise`, `judge-gate`, `improve`, `improve-worker`,
`judge-replay`, `langfuse-trace`. Committed noise baseline:
`crates/evals/noise-baseline.json`. The runtime applies patches through
`append_patch` in `crates/agent/src/skills.rs` (the MCP `read_skill` export
re-implements it; a contract test keeps them equal, see CLAUDE.md).

## Decisions (locked)

- **Per-node is the only judge.** A node is `(skill, input, output)`; judging it
  against the skill's contract is generic (new scenario = new skill, judge
  unchanged) and pins failure to one skill. Whole-run scores were too blurry to
  drive patching (the old whole-run trajectory judge is gone).
- **Node owner = a named skill.** Prompt-only composes had the planner as owner;
  the planner was removed (2026-10-20) but the judge keeps its Planner
  node/rubric for historical traces. Node role lives in `metadata.judge_node`
  (never in the display name, so renames can't break judging; classification is
  metadata-only).
- **`llm_agent`-style nodes: judge yes, patch no.** Replay is unsafe (inner loop
  calls real tools). The improver targets compose-style skills.
- **Axes:** composer / agent nodes: `composition`, `coverage`, `faithfulness`
  (claim decomposition, per-claim supported/partial/unsupported, n/a on a quiet
  day); planner (historical): `query_formulation`, `process`.
  `composition` was de-tasted (prompt n3 to n4): dock only a clear, citable
  contradiction of a rule the contract states; pooled sigma fell 0.19 to 0.022.
- **Judge model != generator family**; the judge yardstick is always the
  ORIGINAL contract (the patch nudges the generator, not the goalposts).
  Pairwise judging needs a swap (both orders; a verdict that flips is a tie):
  the judge has strong position bias.
- **Closed-loop, but gated.** No propose-only stage; every patch passes the
  gate. `dreaming` was declared deprecated for this purpose and is not
  integrated (its "only path to self-revision" text is stale; verify before
  relying on either).
- **Patch = `skills/<skill>.patch.md`, append-only**, owned by the improver; the
  body stays human-owned. Injection is ONE shared function used by the runtime
  and by the gate replay, so the gate measures what prod runs. Revert =
  surgically remove the lesson (delete the file when none remain).
- **Sigma is measured per judge model**; the committed baseline is keyed by
  (model | prompt version). `codex|n4`: composition 0.030, coverage 0.050,
  process 0.020, faithfulness 0.018, query_formulation 0.029. Accept a gain only
  when delta > k * sigma (k about 2) on the target axis and no holdout regression.

## Improver design (locked 2026-06-18)

Per skill, per axis:

1. **Select candidates: absolute + sigma, never a percentile.** Failure =
   `score < 0.6 AND score < 0.75 - k*sigma`. A percentile never converges
   (always a bottom X%), ignores sigma and absolute quality. The absolute floor
   makes the loop shut off when the skill is uniformly good. Holdout = all-time
   nodes with `score >= 0.85`; the cluster comes from a recent window.
2. **Failure-mode taxonomy** (improve-time, not judge-time): distill verdicts
   into atomic, instance-free failure phrases and ask one LLM call
   ("open-coding") for 3-7 named modes with frequencies and member nodes; patch
   the dominant mode (Pareto). Do not embed raw verdicts (instance specifics
   dominate the vector). Embedding + clustering is the scale-up path.
3. **Author ONE append-only lesson** for that mode, shown the current
   `.patch.md` so it doesn't repeat itself (dedup on input).
4. **Cheap gate.** Regeneration is the irreducible cost (re-run the generator
   on the frozen input under the recorded model with the patch appended; the
   OLD output re-judged under a new contract is meaningless). "Before" = the
   stored judgement (free), threshold `k*sigma*sqrt(1+1/S)`; cluster S=2,
   holdout S=1; judge only the target axis (skip faithfulness unless it is the
   target); informed retry off by default (opt-in); worker round-robin with a
   per-tick cap (`IMPROVE_MAX_CYCLES_PER_TICK=1`, least-recently-attempted
   first; monitoring is free and runs for all pairs). The first prod shadow run
   blew the shared 5 h codex quota (faithfulness re-judged on every sample, the
   retry re-gated, a runaway second run); these cuts made the reject path about
   4x cheaper.
5. **Decide** (`decide_ship`): ship iff the target axis net-improves on the
   cluster and nothing regresses anywhere (conservative, shipping is automatic).
6. **Monitor and auto-revert** (`monitor.rs`): after a ship, compare the target
   axis on traces after the ship with the pre-ship baseline (two-sample band
   `k*sigma*sqrt(1/postN+1/baseN)`); revert only on a confident drop. While a
   ship is pending the worker authors nothing (one change at a time). Per-skill
   lesson cap (default 8) blocks growth.
7. Shadow by default (`IMPROVE_APPLY=false`), an audit line per cycle in
   `improver-audit.jsonl` on the agent-data volume.

What the loop is: **floor-lifting** (fixes systematic failure modes, raises the
median, shrinks variance) with a hard ceiling set by generator model, contract
and judge; it does not raise the ceiling. Standing risks: **Goodhart** (we
optimise the judge's score; mitigated by objective axes, sigma-aware gating and
the live monitor) and **patch bloat**.

## Open

- Prod validation of the whole loop (the corpus lives in the droplet's agent.db);
  run `improve` there first without `--apply`.
- Author == judge == codex today (self-consistency risk); move the author to
  another model if patches overfit the judge.
- Retire-by-correlation (drop lessons that stop correlating with low scores)
  needs per-lesson attribution; only the lesson cap exists.
- Known gate failure: regeneration of huge map-stage news outputs dropped the
  stream (`Premature close`); options: broaden the retry, gate only the
  reduce/terminal node, lower samples. Re-check in the Rust HTTP client.
- Cost on map-stage nodes (large chunk inputs): sample, or restrict faithfulness.
- Gate replay sends model + messages but not the recorded temperature/reasoning
  preset; revisit if it skews delta.
- The sigma baseline still wants an authoritative `--runs 5` over more traces.
