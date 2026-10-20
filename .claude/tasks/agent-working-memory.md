# Working memory: work with data by reference, read it selectively

**Status:** in-progress. Stage 1 (a string KV, results stored by reference,
`input_refs` for sub-agents) is built; selective reads, derived-data
provenance and text probes are not.
**Priority:** P3
**Area:** agent / session-context
**Created:** 2026-09-06

## Context

The agent should work with data by reference: store it, hand it to a
sub-agent, use the result, without pushing the whole content through its own
LLM context. When it needs the data it can read all of it or a part.

## What is built (stage 1)

Code: `crates/agent/src/session.rs` (the KV), `tool_results.rs` (storing
results), `agent.rs` (the synthetic tools, `invoke_sub_agent`).

- **Session KV.** One instance per signal/task, shared by the primary loop,
  recovery and every `invoke_sub_agent`. Values are strings kept verbatim
  (JSON, `${...}` and whitespace are never interpreted); keys are exact
  non-empty strings. Writing an existing key is an error, reading a missing
  key is an error (an empty string is a real value); `list` returns metadata
  (key, format `text|json`, UTF-8 size) in insertion order; deleting frees the
  key; instances are independent and die with the task. `set_memory` still
  writes persistent state to agent.db.
- **Synthetic tools:** `working_memory_put/get/list/delete`. `get` loads the
  full value into context on purpose. These calls are not stored again.
- **Every MCP or synthetic tool result is stored** unchanged under
  `tool.<uuid>` when the call finishes. The model sees JSON with `memory_key`,
  `format`, `size_bytes`, `truncated`; up to 8 000 UTF-8 bytes the full string
  is inlined, above that a 512-byte `preview` (never splitting a character).
  The full result stays in the trace. The byte threshold is a heuristic.
- **`invoke_sub_agent({ skills, prompt, input_refs? })`** puts the referenced
  values into the child's first user message as data; the parent neither reads
  nor rewrites the payload. A missing key fails before the child starts.
  Without `skills` the `worker` skill is used; sub-agents do not delegate.
- For Telegram signals the supervisor preloads recent history and stores the
  full selected history as `telegram.history` for `input_refs`.

## Open

Requirements not yet met:

1. **Selective reads.** `working_memory_get` returns all or nothing. Needed:
   a part of a value (object field, list items, text slice by offset), with an
   explicit "this is partial" marker and a way to continue.
2. **Probes without reading.** Cheap approximate facts about a stored text, so
   the agent can choose a strategy before paying to read it: size (chars,
   estimated tokens), lines, "looks tabular?", language (`text_stats`);
   `keyword_search(ref, terms)` returning matching lines/spans;
   `vector_search(ref, query, k)` (chunk + embed + rank, reusing the embeddings
   stack); `read_span(ref, range)`. These run in-process on the stored value
   so the text never crosses MCP and never enters a prompt. (Carried over from
   the retired text-processing-toolkit task, whose GAIA/planner framing is
   gone. Decided there: do not build an opaque `text_process` ReAct tool;
   retrieval primitives are deterministic tools, map/reduce is a pattern the
   agent composes from sub-agents, tables are `code_agent`'s job. For a map
   step, return `{found, quote, confidence}` and verify the quote is a real
   substring of the chunk, with overlapping windows.)
3. **Provenance.** A result produced from stored data gets a new ref and
   records which refs it came from.
4. **Direct workflow results** are not tied to the shared memory yet.

Decide later: whether entries can be changed or appended (stage 1 only creates
and deletes), whether content search is needed, whether data should outlive a
task or a restart (the durable `agent-loop` carries working memory in its run
context, so a restart already keeps it for a resumed loop).

## Acceptance

- The agent reads a field, a list slice or a text range of a stored value and
  is told when the result is partial.
- A 1 MB fetched page can be probed (`text_stats`, `keyword_search`) and
  searched without entering any prompt.
- A derived result names its source refs.
- A missing ref or range returns a clear error, distinct from an empty value.

Related: `unified-memory` (done, `done/unified-memory.md`) is the persistent
cross-session store; this is per-task scratch space.
