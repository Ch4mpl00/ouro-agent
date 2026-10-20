# Complete trace coverage as the source for Ouro's activity memory

**Status:** pending. The activity journal built on traces is implemented (see
"Done"); the coverage gaps below are an unfixed source-inspection audit, ported
to the Rust code on 2026-10-20 (the audit was done on the TypeScript code and
the Rust port kept the behaviour; items marked "confirmed" were re-checked).
Also open: check journal-note quality on real prod runs.
**Priority:** P2
**Area:** agent / tracing / activity memory
**Created:** 2026-10-13

## Context

Telegram should know what Ouro has done across sessions. The chosen path:
complete trace recording, then a deterministic bounded projection of each
finished trace, then a background LLM writes a short note into the shared
journal stream `ouro/activity`. The raw traces are also the judge/improver
corpus, so their read interface must stay compatible.

Scope decisions from the user: crash durability is deferred to durable
execution (now `durable-workflows.md`; the original trace persistence contract,
record on trace end, is unchanged); judge/improver activity is inactive;
additional `code_agent` tracing is deferred; detailed session preparation is
unnecessary for activity memory; MCP reconnects stay in technical logs. A
structured partial-send outcome is accepted. Everything else below is a
proposal, user-visible operations first.

## Done: the activity journal

Code: `crates/agent/src/activity.rs` (projection, note generation and
validation, SQLite progress, background worker), journal metadata in
`crates/mcp/src/memory.rs` (migration `0006_memory_journal_metadata.sql`).

- A background loop inside the supervisor (not a separate process: a separate
  one would need a third, memory-scoped MCP instance) over the supervisor's own
  MCP session; journal actor `supervisor`, provenance in
  `metadata.trace_id` (jsonb, at most 2000 chars). `ACTIVITY_JOURNAL=off`
  disables it; `ACTIVITY_JOURNAL_MODEL` overrides the base preset model.
- Eligibility is an allowlist: the supervisor stamps `metadata.activity = "run"`
  on signal traces; benchmarks, scripts and the worker itself never do. No
  backfill: the cutoff is recorded once in the memory KV
  (`activity_journal.cutoff`). Capacity of `ouro/activity`: 200 notes (oldest
  evicted); raw trace retention is unaffected.
- Projection packet: trace id, run time, source, original input, skills,
  context actually supplied for context-dependent Telegram requests, a
  chronological list of actions with ids/parents (nested workers not counted
  twice; tool identity from `metadata.tool`), selected arguments and compact
  results with confirmed outcomes, errors, partial-delivery evidence and
  references, the final result, explicit truncation markers. Per-field budgets
  plus one hard total (target 3-4k input tokens, a 5-10 line note). Large results
  have extraction rules (search/list news: queries and counts, no bodies;
  Telegram sends: destination, delivery outcome, message ids; and so on). Do not
  use `trace.output` blindly: on scheduler workflows it is an execution marker.
- The model has no tools; its input is historical data. The note preserves
  "proposed", "awaiting an answer", "rejected", "partial" as such; side effects
  count only from action evidence. Code adds trace id/source/time.
- Progress in agent.db by projection records (not a timestamp watermark):
  attempts / next attempt / error bookkeeping; a generated note is saved as a
  draft before the MCP append, so an append failure retries without another LLM
  call; generation gives up after 5 attempts. An in-process in-flight set guards
  the single worker. No journal-append idempotency by decision (a successful
  append followed by a failed done-mark can duplicate; exactly-once is out of
  scope).
- Reading it back: for every Telegram signal the supervisor loads the last 10
  notes (at most 8 KB, whole notes, oldest first) into the **system prompt**
  ("Recent Ouro activity"), telling the agent it can call `journal_tail` for
  more; the system prompt, not the user message, so notes are not re-read as a
  run's conversation. An unreadable journal just omits the block.

## Open

- **Note quality on real prod runs is unchecked.** Validate on representative
  scenarios: an ordinary Telegram exchange, a context-dependent confirmation, a
  large news digest, a refusal, a partial send, confirmed delivery followed by a
  later failure, nested workers, a scheduler root with a generic output. Tests
  can pin the invariants; a bounded sample run judges the notes.
- The coverage gaps below.

## Coverage gaps (audit 2026-10-13, Rust status 2026-10-20)

Already covered: LLM message history and returned assistant message with usage;
MCP/synthetic tool calls with arguments and full results/failures (working
memory previews do not replace the raw text in the span); sub-agent, workflow
and recovery sessions as nested scopes; workflow step outputs; classification
batches with items, answers and probabilities; transient LLM retries as events;
the automatic Telegram history load; Telegram send message ids (a chat-log
write failure comes back as `logError`); `search_news`/`list_news` results.

Gaps, by priority:

1. **Partial Telegram send is textual only (accepted).** `PartialSendError`
   (`crates/mcp/src/telegram.rs`) puts delivered/total counts and message ids
   into the error text, but no structured partial outcome or per-part delivery
   objects reach the trace. Add a structured outcome; keep the error behaviour
   and the delivered-message evidence so consumers never repeat delivered parts.
   Confirmed.
2. **`fetch_article` hides its persistence outcome.** The upsert's
   `{saved, embedded, failed}` is ignored and a save error is only logged
   (`tracing::error!` in `news.rs`) while the article is returned normally; the
   trace can't tell persisted, embedding-failed or save-failed. Return a
   warning. Confirmed.
3. **Saved memory can have an unreported indexing failure.** A fact or document
   write returns after quiet indexing; a failed embedding leaves NULL vectors
   that search excludes, but the trace shows only the successful source write.
   Expose "indexing pending/degraded" as a warning without turning the write
   into a failure or inviting a duplicate write.
4. **Domain refusals have no explicit outcome.** Memory and scheduler tools
   return `{ok:false,error}` as ordinary results; the agent recognises bracketed
   tool-error strings, so refusals get normal span severity. No payload is lost,
   but a journal projection must not equate normal severity with a completed
   write; give refusals an explicit outcome.
5. **MCP background work and non-supervisor clients have no producer traces.**
   Pollers (news, Gmail, Telegram), scheduler firing and calls from other
   clients record state and logs but no trace. The useful coverage is aggregated
   ingestion outcomes and background failures, not empty ticks. Needs
   producer-side tracing and protocol correlation without sharing private DBs.
6. **The MCP result envelope is flattened.** The agent's client keeps only text
   content blocks (`crates/agent/src/mcp.rs`); images/audio/resources and
   `structuredContent` are dropped before the span records them (a
   structured-only reply becomes empty). Matters for gateway/upstream tools;
   conditional on an upstream returning useful non-text data. Confirmed.
7. **Lower priority:** `finish_reason` is parsed (`providers.rs`) but not
   recorded on generations; provider wire metadata and the actual tools/response
   format of a request are not snapshotted (replay/debug concern); the local
   root observation is not synchronised on update/end and node latency is
   always 0 (`local_recorder.rs`; confirmed).
8. **Deferred by decision:** `code_agent` detail (events, thread id, usage,
   stderr; the workflow path also omits the resolved input), judge/improver
   execution traces, session-preparation diagnostics (timezone fallback to UTC,
   skill provenance), MCP reconnect tracing.

## Acceptance

- Coverage of all observable agent activity is defined and audited: signal and
  supplied context, LLM requests/responses, tool arguments/results, delegated
  sessions, workflow execution, decisions, delivery confirmations, errors,
  retries, cancellation, recovery.
- Recorded outcomes distinguish attempted, confirmed success, partial success
  and expected domain refusal.
- Complete relevant LLM/tool/delegation inputs and outputs stay available
  through the existing store and canonical evaluation read interface; recording
  needs no extra LLM call and no schema replacement; no secrets (API keys, auth
  headers) enter trace payloads.
- Each fix has tests for the specific missing field or path.

## Notes

Open decisions: whether "everything" includes MCP background activity in the
first phase (agent-run completeness is the initial scope); whether judge/
improver activity joins the same activity view; whether completeness means the
normalised application request/result or the full provider wire exchange
(minus credentials); raw trace retention and archive budgets (separate from
journal compaction). Journal compaction is a later phase; full retention does
not mean loading full traces into an LLM context. Keep the canonical `getTrace`
read shape for evaluation consumers and add bounded section/payload reads for
activity-memory consumers.
