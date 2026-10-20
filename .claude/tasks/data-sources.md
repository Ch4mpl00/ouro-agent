# Data sources: the agent builds data collection from workflows and MCP tools

**Status:** pending (not started; depends on `durable-workflows.md`)
**Priority:** P3
**Area:** agent / workflow / mcp datasets and tools
**Created:** 2026-10-14 (updated 2026-10-16, ported to the Rust code 2026-10-20)

## Context

The user gives a link or site and a collection goal. The agent explores the
resource, finds a way to get the data, creates a dataset, builds and verifies a
multi-step process, then schedules it.

**Agreed direction:** the general construction and execution logic lives in the
agent's saved OWF workflows (`crates/agent/src/flows.rs` over `crates/owf`).
MCP provides tools, data storage and external events. No separate pipeline
executor inside MCP. Basic dependency: `durable-workflows.md`.

A scheduled run executes the saved version without a planner. An LLM or Jev
step may process data but collection does not require one. Waiting, config
repair and clarifying questions to the user come from the shared agent
workflow runtime. Earlier research proposed an executor inside MCP, fixed
readers and a mandatory news projection queue; that was dropped (the research
document was removed, it is in git history).

Exact APIs/DSL and migration stages are still to be designed.

## Acceptance

### Scenario and components

- The agent explores a link with tools, finds RSS/Atom, a public JSON API or a
  usable static HTML structure, and picks an extraction method.
- A process may paginate, go list to detail, do a bounded crawl, transform
  fields, and fall back to article extraction. These belong to the agent
  workflow; MCP operations perform single actions.
- Enough operations: HTTP, parse feed/JSON/HTML, extract article, dataset
  read/write. Where pure transformations live is decided at implementation;
  general control flow is not moved into MCP tools.
- The agent builds and checks the process on representative examples, saves a
  version and sets up a cron signal carrying the process id and parameters
  (`schedule_task(workflow: …)`). MCP stores/delivers the event but does not
  interpret the definition.
- Preview shows intermediate data, page/branch coverage and the records it
  would write, without changing production datasets or the schedule (the
  `try_workflow` dry-run is the base).
- A connected resource is visible through list/status: active process, dataset,
  schedule, last run, errors. Pause/resume work.
- On a persistent format break, a bounded agent diagnosis loop runs and the new
  version is verified before activation. A network retry alone needs no LLM.
  An ambiguous goal can be resolved through user input.

### Data and ownership

- MCP provides datasets with free-form JSONB, a description and an optional
  schema. The agent runtime never reads MCP's database directly.
- Append mode keeps the first record with a key; upsert updates a changed
  value. JSON equality ignores object key order.
- Record identity is explicit; the working choice is
  `(datasetId, sourceId, externalId)`. Changing the key rule or dataset needs an
  explicit migration.
- MCP owns data invariants: uniqueness, revision/conditional update, atomic
  batches, idempotent operations. Run/step state, waits and action order live
  in the agent DB.
- The runtime checkpoint advances after the data is durably written through
  MCP. A crash between MCP commit and agent checkpoint is safe because the
  write repeats under a stable operation key (there is no shared transaction
  between the databases).
- Child jobs are persisted before the list advances: a detail not fetched in
  time is not lost when its record leaves the next top-N slice.
- Source data is available independently of derived views. News is the first
  consumer; its specialised write/indexing stays an MCP tool. No general
  process queue in MCP for it.

### Limits and quality

- Guarded HTTP applies to source URLs, redirects, next links and detail URLs.
  Response sizes, request/page counts, fan-out and time are bounded. A
  truncated RSS/JSON is an error, not a partially successful parse.
- A snapshot/top-N does not promise completeness after downtime; incremental
  pagination has explicit cursor, resume and limit semantics. A record missing
  from an incomplete snapshot does not mean deletion.
- One failing record is visible and retryable; successful neighbours are kept.
  Zero new records is not proof of schema drift.
- Browser-only sites, new auth/session automation and arbitrary executable code
  are out of the first slice; the agent reports an unsupported method.
- Control tools/skill are separate from the `news-read` toolset. Thin MCP
  access to managing agent-owned processes, if ever needed, delegates to the
  agent service; no second runtime.

### Compatibility with current news

- Move Habr/HN gradually onto saved workflows built from the shared
  operations. Keep the Telegram provider/watermarks on the current path.
- Keep source ids `habr`/`hackernews`, URL-based news keys and the 30/15 minute
  intervals. Seeding must not resurrect disabled/deleted configurations.
- Never run the old provider and the new process for one source at once. Old
  `news_items` are neither lost nor duplicated; moving all history into
  datasets is not required.
- `list_news`/`search_news` support dynamic source ids; `fetch_article` looks
  up new sources by URL.
- Keep the review findings: a news upsert must update URL/date correctly;
  embeddings are recomputed when the indexed text changes; a late inline/
  backfill result must not attach to a newer revision of the text.

### Verification

- End-to-end cases RSS to dataset to news, JSON list to pagination to detail,
  HTML list to article, all on one agent workflow runtime.
- Rerun and restart do not duplicate records; detail errors, a lost MCP reply,
  a partial batch and a schema change have tested recovery.
- A test shows waiting for a user clarification and resuming without blocking
  other tasks.
- Existing search, digests and Telegram keep working.
- Rust tests with fake HTTP/model/clock and test databases.

## Notes

Order: durable workflow state/waits, then missing data tools and
transformations, then authoring/preview/scheduled run, then Habr/HN migration.
The first useful process can be verified before the news migration is complete.
Jev is an optional operation; a general Jev solver is not a prerequisite.
