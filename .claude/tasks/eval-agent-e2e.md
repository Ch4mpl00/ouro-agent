# End-to-end agent regression on frozen scenarios

**Status:** pending (nothing built; the pieces below exist)
**Priority:** P3
**Area:** evals / agent
**Created:** 2026-06-02 (rewritten for the Rust code 2026-10-20)

## Context

There is offline retrieval eval (`eval-rag`, `eval-snapshot`, `eval-inspect`
over `crates/mcp/eval/fixtures`), a per-node judge and a single-generation
replay (`judge-replay`) in `crates/evals`, and the news-filter golden set
(`news-filter-eval`). What is still missing is **regression testing of a full
agent session**: when a skill prompt, a model or the supervisor loop changes,
nothing answers "did this break the news-digest flow?" short of waiting for the
next cron fire in prod and reading Telegram.

Telemetry already exists: every signal run is a trace (Langfuse and the local
`traces` mirror in agent.db) with a generation per iteration, a span per tool
call and nested sub-agent traces. This task is about replaying frozen scenarios
and diffing trajectories across model/prompt changes, not about new telemetry.

## Acceptance

### Sandbox engine

- The agent runs against an alternative `Mcp` implementation (the trait in
  `crates/agent/src/mcp.rs`; verify the seam holds for a mock).
- **Replay mode:** deterministic responses recorded from a captured trace, to
  replay a historical session against a new model/prompt.
- **Corpus mode:** `search_news`/`list_news` read the frozen
  `crates/mcp/eval/fixtures/corpus.jsonl` instead of live Postgres; side-effect
  tools (Telegram send, `schedule_task`, memory writes) record the call and
  no-op. For scenario tests with the real retriever stack and no prod writes.
- The sandbox returns recorded responses bit-for-bit identical to what the live
  MCP returned for the same input.

### Datasets and runs

- Select traces by tag and age from the local mirror or Langfuse, drop failed
  runs (errors, timeouts, escalation-loop trips), and freeze each as
  `{input: signal, expected: trajectory}` fixtures in the repo (scenarios are
  version-controlled; Langfuse is not the source of truth). Idempotent by trace
  id.
- A runner replays each item through the sandbox with a chosen model/preset,
  uploads the new trajectory as a Langfuse Dataset Run (run-vs-run diff UI for
  free), with a `--max-items` knob for cheap smoke tests.

### Scorers

- **Structural invariants** (cheap, deterministic), written as Langfuse scores:
  `search_news` called at least once for news skills; Telegram sends exactly 1
  for digests (0..1 for queries); iteration count bounded (< 30); output length
  within per-skill bounds.
- Component metrics from the offline RAG eval (reformulation P@5/MRR).
- LLM-as-judge for the final output through the existing per-node judge
  (`per-node-judge-and-improver.md`).

## Notes

- Replay mode catches "the model reads identical inputs differently"
  (model/prompt regression); corpus mode catches "the agent still finds and
  synthesises correctly on a known corpus" (skill regression). Orthogonal.
- It does not replace the offline RAG eval: run that on PRs touching retrieval,
  this one on PRs touching skills, the supervisor or the model.
- Use Langfuse Datasets rather than rebuilding trace storage, run grouping, diff
  views and score aggregation.
- First step, before any code: spend 30 minutes in the Langfuse UI on last
  week's production traces; obvious patterns (tool overuse, escalation loops,
  format drift) may be cheaper to fix than to detect.
