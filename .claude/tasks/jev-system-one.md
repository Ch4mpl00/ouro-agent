# Jev (TypeSafe System One): typed decisions where we spend an LLM turn

**Status:** in-progress. The client (`decisions.rs`), the `filter_relevant` and
**Open work:** #46
`choose` workflow functions and the news-filter golden set are shipped (the
digest filter moved to Jev 2026-10-12). Open: the other candidate uses, the
dedup/`covered` measurement, calibration on Russian/Ukrainian text.
**Priority:** P2
**Area:** agent / models
**Created:** 2026-09-20

## Context

TypeSafe AI released **Jev** on 2026-09-15, the first public "System One"
model. It is not a chat model: you give it program state plus a schema of typed
questions defined in advance, and it answers all of them in one parallel pass
with typed values and calibrated probabilities. It generates no strings.

- 70-500 ms end to end, claimed 40-200x faster than frontier LLMs; $0.042 / M
  input tokens, output free; type errors structurally impossible; at most 255
  choices per decision; the schema must be known up front; about 32k context
  per request (secondary sources).
- Several of our LLM turns are classification and scoring wearing a chat
  interface.

## Where it fits, best first

1. **The news-digest significance filter** (shipped, see below).
2. **Routing in the orchestrator.** `routing.md` spends a full turn of a big
   model deciding digest / topical query / reminder / chit-chat. A calibrated
   probability gives a principled escalation threshold. (The `choose` workflow
   function is the same primitive inside saved workflows.)
3. **Dedup against chat history**: "was this event already covered?" is the
   failure the digest skill worries about most; a yes/no with a confidence.
4. **Judge pre-screen.** The judge burns codex quota per node. Jev could cover
   cheap axes and flag which nodes deserve the expensive judge. Measurable today:
   a judged corpus and the sigma baseline exist.
5. **Plan-cache validity** ("does this signal match saved workflow X?" in
   ~100 ms), see #37.

Not a candidate wherever text is produced: digest composition, recovery
messages.

## Wire format (settled 2026-10-12)

Our OpenRouter account has access. `typesafe/jev-1.13` is rejected on
`/chat/completions` ("is a decisions model"), so it does not ride
`ChatProvider`; it lives in `crates/agent/src/decisions.rs` (built in the
supervisor when `OPEN_ROUTER_API_KEY` is set; model `AGENT_DECISION_MODEL`,
default `typesafe/jev-1.13`).

```
POST https://openrouter.ai/api/alpha/decisions
{ "model": "typesafe/jev-1.13",
  "state": string | object | array,
  "questions": {
    "<key>": { "type": "choice", "instructions": "...", "criteria": { "<option>": "<description>", ... } },
    "<key>": { "type": "score",  "instructions": "...", "criteria": ["Low", "Normal", "High"] },
    "<key>": { "type": "noul",   "instructions": "<a statement>", "criteria"?: { "true": "...", "false": "..." } } } }
-> { "model": "...", "id": "gen-dec-...", "provider": "TypeSafe",
     "answers": {
       "<choice>": { "type": "choice", "choice": "<option>", "probabilities": { "<option>": p }, "confidence": p },
       "<score>":  { "type": "score", "score": 0.37, "legend": { "0": "Low", ... }, "probabilities": { "0": p, ... }, "confidence": p },
       "<noul>":   { "type": "noul", "noul": p } },
     "usage": { "input_tokens": 544, "output_tokens": 82, "cost": 0.0000228 } }
```

Traps the client normalises away: a score is the probability-weighted mean level
*index* (0..n-1, not 0..1) and its probabilities are keyed by index; a noul has
no `confidence` field. Measured: about 0.7 s end to end, about $0.00002 per
three-question call on a short Russian message, 32k max prompt tokens.

## Results so far

Golden set: `eval-fixtures/news-filter/` (gitignored, see CLAUDE.md): the 100
latest prod channel posts (a 2.7 h window), a rubric distilled from the digest
skill (`interests.md`), reference labels by an Opus agent (`opus.json`: 33 keep /
67 skip, 33 low-confidence), Jev's answers, `report.md`. Run with
`cargo run -p evals --bin news-filter-eval -- jev|compare|filter|pipeline|compare-dedup`.

- One `noul` per post with the rubric in the instructions: 5.2 s wall at
  concurrency 8, $0.0096 per 100 posts; ROC AUC 0.91 against all labels, 0.994
  against Opus's high-confidence ones (disagreements are borderline calls).
- Probabilities are compressed (nothing above about 0.8), so 0.5 is the wrong
  cut: p >= 0.5 gives precision 83% / recall 58%. As a **pre-filter** in front
  of the LLM composer, p >= 0.2-0.3 drops 40-46% of posts and keeps 97% recall.
- **Batching works** (rubric once in `state`, one `noul` per post, 25 per call):
  4 calls, 0.5 s wall, $0.0020 per 100 posts (5x cheaper), no quality loss.
  Order/cross-talk effects not tested.
- **Filter + dedup in one pass** (`keep_n`, `covered_n` vs the previous digest,
  `dup_n` over embedding neighbours): $0.0026 per 100 posts; filter quality
  unchanged; 100 posts become 39 for the composer, event recall 92%. Dedup
  clustering is **not better than a plain cosine threshold** (pairwise F1 0.57
  vs 0.56), candidate generation caps recall at 60%. **`covered` is unmeasured**:
  the window started 3.7 h after the digest and Opus found no covered posts.
- Shipped: the digest's map phase (3 x gpt-5.4-mini, each carrying about 25k
  tokens of ignored chat history) was replaced by `list_news` then the filter
  then one compose then send. Rules go in `state` once per batch, one `noul` per
  item, batches packed by size (at most 25 items / 24k chars), all batches
  concurrently, default threshold 0.3. No fallback: a Jev failure fails the step
  (`decision_error`). Chat history was dropped from the digest until `covered_n`
  lands, so there is no dedup against previous digests yet.
- Caveats: one reference labeller, one short window, no posts on the user's
  priority topics in it.

## Acceptance (evaluation, not adoption)

- [x] Access obtained and the real request/response shape written down.
- [x] A golden set and a filter measurement (above).
- [ ] Offline replay of a large real digest window (a 367-post Langfuse trace;
      pull it with `cargo run -p evals --bin langfuse-trace`) through a Jev
      schema: recall of digest events, precision at equal budget, cost, wall
      time with realistic concurrency.
- [ ] A window that overlaps a previous digest's sources, to measure `covered`.
- [ ] Calibration sanity check on Russian/Ukrainian text (vendor benchmarks are
      almost certainly English).
- [ ] Decide whether routing / judge pre-screen / dedup move to Jev.

## Notes

- Two-week-old vendor: nothing on the delivery path without a fallback to the
  current LLM step (the filter currently has none; deliberate, revisit).
- It is another processor for private Telegram content: a separate decision
  from the technical one.
- Input is about 1000x cheaper than the old map phase, so the interesting
  failure mode is silent quality loss (a filter that drops the one item the user
  cared about), not cost.
