# Unified memory shared by all agents

**Status:** done. v1 implemented 2026-08-23 in TypeScript and ported to Rust
(`crates/mcp/src/memory.rs`; the D-numbers below are cited from that file).
Not built: the phase-2 memory agent (Q10) and the removal of the legacy
`add_note` / `find_notes` toolset (`../open-findings-from-tests.md`).
**Priority:** P1
**Area:** mcp / memory
**Created:** 2026-08-21

## Context

Today "memory" is scattered and none of it is shared:

- `knowledge_base_notes` (PG) — freeform facts, `add_note` / `find_notes`.
  Flat, append-only, no update or delete path.
- `agent.db` `memory` — opaque internal KV (watermarks like
  `news_digest.last_read_at`). Agent-private, not a knowledge store.
- Skills — static domain instructions, hand-written.

What's missing is a **single memory any of my agents can read, search and
write** — the supervisor on the droplet, Claude Code sessions, future
agents — so a fresh session picks up state instead of restarting from
zero.

The representation must stay **soft**: the right shape for a "project" or
a "fact" isn't known yet and will change. The design achieves that by
keeping the stored form freeform (markdown documents, verbatim fact
text) rather than by replaying an event log — see
[Why not event sourcing](#why-not-event-sourcing-deferred).

### Use cases driving v1

1. **Projects with progress.** Preparing for a coding interview on a
   topic: a passport (goal, constraints), a roadmap, current progress. A
   new session asks MCP for the project and gets all of it, then appends
   progress as it works.
2. **Facts.** "запомни то", "посмотри что там у нас по X было" — what
   `add_note` / `find_notes` do today, plus an update path.
3. **Short-lived lists.** Wishlists, shopping lists — cheap to write,
   expected to go stale.

### Constraints already on the table

- **MCP was single-connect, on purpose** (at design time). One
  process-wide server; a second `initialize` threw `Already connected`.
  Per `mcp-connection-lifecycle.md` that protects the **signals queue**
  (two agents would race for the same signal), not the tool surface.
  Unified memory needs many clients — see D1.
- PG + pgvector, `EmbeddingService` and the chunker are already there and
  reusable (`text-embedding-3-small`, 1536-dim).
- Module rules from CLAUDE.md apply: one domain, one file, repos next to the
  tables they own, no singletons.

## v1 shape

```
projects       id, slug, title, created_at, updated_at
project_docs   project_id, name, summary, body_md, version, updated_at
doc_patches    doc_id, pid, old, new, actor, rationale, created_at
facts          id, body, tags[], source, state, created_at, updated_at
memory_index   ref, text, embedding, tags[], actor, ts, state
```

```
write:  agent → patch_doc / append_doc / remember → document (versioned)
                                    ↓
        indexer (background) → chunks + embeddings → memory_index
                                    ↓
read:   recall(query) → refs → read_doc(project, name) / get_fact(id)
```

## Decisions

**D1 — multi-client: per-session `McpServer` + client roles.**
`runHttpTransport` stops sharing one process-wide server: each
`initialize` builds its own `McpServer` via a factory, so N concurrent
sessions are legal. The client declares a **role** on connect and the
tool surface is filtered by it:

```
initialize(role=supervisor) → full surface incl. get_next_signal
initialize(role=memory)     → memory tools (+ read-only tools)
```

Signal delivery stays safe without a claim/lease: a non-supervisor client
never sees `get_next_signal`, so it cannot compete for signals. This
supersedes the "multi-connect is P3, don't start" stance in
`mcp-connection-lifecycle.md` — and subsumes that task's P1 fix, since
per-session servers can't get stuck bound to a dead transport. Keep at
most one **supervisor** session, newest wins.

**D2 — `knowledge_base_notes` is absorbed.** Existing notes migrate into
`facts` (verbatim body + tags). `add_note` / `find_notes` become thin
aliases over `remember` / `recall`, or are dropped once the skills are
updated. One store, one place to write, one place to search.

**D3 — no event log in v1; documents are the truth.** Writes mutate the
document directly and bump its `version`; the applied patch is kept in
`doc_patches` for history and revert. What a general event log would have
added over this is nothing, because the stored form is already the raw
form (see the section below). One cheap piece of D3's original idea
survives: every patch carries **`rationale`** — the utterance that caused
it ("отметь, что Dijkstra пройден"). That's the only information a
document genuinely doesn't contain.

**D4 — the only background worker in v1 is the indexer.** It picks up
changed documents and facts, chunks them, embeds them, and refreshes
`memory_index` — the same inline-embed-with-backfill pattern
`knowledge`/`news` already use. Consequences:

- Reads of documents are immediate and never blocked by OpenAI being
  down; only searchability lags.
- *Phase 2 (later, not v1)* — a **memory agent**: an LLM worker that
  consolidates duplicate documents, summarises long feeds, promotes a
  recurring note into a fact. It is the first thing that makes memory
  **lossy**, and therefore the trigger for introducing a raw log.

**D5 — a project is a folder of markdown documents.** Not one blob, not
a fixed schema of passport/roadmap columns: N named `.md` documents the
agent reads and patches **individually** (`passport.md`, `roadmap.md`,
`progress.md`, `mistakes.md`, whatever it needs).

- **The interface is the one LLMs are best at** — read a file, patch a
  file; the same shape as Read/Edit.
- **Form grows bottom-up.** No schema decides a project has a "roadmap".
  Restructuring memory is writing, not migrating.
- **Reading is granular.** `read_doc(project, "roadmap.md")` pulls one
  small document instead of a month of history.
- **Concurrency mostly disappears.** Two agents in different documents
  never conflict; a single-blob design would collide on every pair.
- **No project status in v1** (explicitly dropped).
- **A document registry, or they multiply.** `list_docs(project)` returns
  name + one-line `summary` + `updated_at`, and the agent must consult it
  before creating anything — otherwise a model happily makes `notes.md`,
  `notes2.md`, `progress-new.md` and loses track of which is current.
  Consolidating duplicates is an explicit job for the phase-2 agent.

Knowingly given up: SQL over roadmap steps ("all open steps across
projects") needs markdown parsing or a later extraction pass.

**D6 — one shared space; `actor` is metadata.** Every agent reads and
writes the same memory. Each patch records who made it (`supervisor`,
`claude-code@laptop`, …) for audit and filtering, never for access
control.

**D7 — decay via explicit state + recency.** Things with a lifecycle
(list items, facts) carry `active / done / archived`; `recall` searches
active by default with an opt-in for archived. Ranking mixes cosine
distance with a recency boost. Nothing is deleted.

**D8 — two independent projections: one for search, one for reading.**
Searching all knowledge and reading a project are different queries with
different optimal shapes:

```
     search projection              read models
     memory_index                   projects / project_docs / facts
     (flat, chunked, embedded)      (documents, verbatim)
              ↓                              ↓
        recall(query) ───ref──────▶  read_doc(...) / get_fact(id)
```

- **Search projection** — one flat `memory_index`: `text`, `embedding`,
  `tags`, `actor`, `ts`, `state`, plus a **`ref`** back to the source
  (`doc:leetcode-graphs/roadmap.md#2`, `fact:88`). Everything lands here
  in comparable-sized chunks whatever its domain shape.
- **The agent's flow is two-step:** `recall` finds *where* the answer
  lives and returns refs; `read_doc` / `get_fact` loads the whole thing.
  Same search→read pattern that fixed the planner's snippet-only failures
  on GAIA.

Why the split earns its keep:

1. **Chunking is a search concern.** A month-long `progress.md` must not
   be one vector; the index decides granularity without the document
   store knowing.
2. **Chunks must be self-contained; documents must not.** The indexer
   denormalises context into the indexed text ("Проект «Графы для
   интервью», progress.md — застрял на Dijkstra"), so recall matches a
   fragment that names its own subject. The document stays clean.
3. **Re-chunk without touching content.** Changing chunking or embedding
   strategy rebuilds `memory_index` only.
4. **Embeddings live in exactly one place.** The embed + backfill +
   `embedding IS NULL` retry dance is currently duplicated across
   `news_items` and `knowledge_base_notes`; a third copy would be a
   systemic mistake. One index table, one implementation, one backfill.

**D9 — addressability is at the document + patch level.** There are no
per-line statement ids. The addressable things are a **document**
(`leetcode-graphs/roadmap.md`) and a **patch** (`pa:4f2a`, plus a version
number). So: "revert patch pa:4f2a", "roll roadmap.md back to v7", "what
changed in progress.md this week". Removing a roadmap bullet is an
ordinary patch, not a retraction.

`read_doc` returns body + current `version`; `history(doc)` lists patches
with ids, timestamps, actors and rationales. Patch ids appear in tool
results and history, never inline in the prose — the document the agent
reads stays clean markdown.

Pitfalls:

1. **Reverting a mid-stack patch can conflict.** If later patches touched
   the same lines, the inverse won't apply — same as `git revert`.
   Attempt it; on failure return a clear conflict and offer a
   whole-document rollback. Never apply a partial revert.
2. **Progress is history — correct it, don't erase it.** "Прошёл BFS в
   среду" is a fact about the past. Feed documents are append-only: fix a
   mistake by appending a correction. Free-form patching is for
   declarative documents (passport, roadmap). Otherwise an agent will
   tidy up its own history.
3. **The LLM will hallucinate ids.** Patch ids come back from `history`
   and write results, are short but random (sequences invite guessing
   neighbours), and an unknown id is a hard error listing near matches —
   never a silent fuzzy match.
4. **Patching needs a fresh read.** A stale quote won't match; the
   version check turns that into an explicit "re-read and retry", cheap
   because documents are small — which is itself why D5 splits a project
   into several files.
5. **Phase-2 rewrites break revertability.** Consolidated patches must
   still resolve — `superseded by …`, never a 404 — and ids are never
   reused.

**D10 — patch format: search/replace, never line numbers.**

```jsonc
patch_doc({
  project: "leetcode-graphs",
  doc: "roadmap.md",
  expected_version: 7,
  edits: [
    { old: "- [ ] Dijkstra", new: "- [x] Dijkstra" },
    { old: "- [ ] A*",       new: "- [ ] A*\n- [ ] Bellman-Ford" }
  ],
  rationale: "отметь, что Dijkstra пройден"
})
```

- **Exact literal match, uniqueness required.** More than one occurrence
  → the call fails and reports the count; the agent retries with more
  context. Never "take the first match".
- **Atomic.** Every edit applies or none do.
- **`new: ""` is a deletion.** No separate delete op.
- **Read before patch.** `expected_version` mismatch → conflict error,
  which is also what makes concurrent agents safe.
- **A miss returns near-matches in the error** (the model normalised a
  quote, an em-dash, ё→е). Suggest, never silently fuzzy-apply.

Why not line numbers (`delete 30–44, insert at 30`): an off-by-one is a
**silent** corruption — the wrong lines vanish and the call reports
success. A quoted string that doesn't match fails loudly and changes
nothing. Line numbers also shift under any concurrent edit above them,
and D1 gives us genuinely concurrent writers. Unified diff has the same
problems plus partially-applied hunks. Every serious coding agent
(Claude Code's `Edit`, Aider's search/replace blocks) landed on quoted
strings for this reason.

Three ops, by escalating risk:

1. **`append_doc(doc, text, under_heading?)`** — the safe default;
   physically cannot destroy existing text, needs no prior read, and
   markdown headings are stable anchors. Feeds use only this.
2. **`patch_doc(edits[])`** — targeted changes, as above.
3. **`write_doc(body)`** — full replacement. Requires `expected_version`,
   reserved for small documents or an explicit "rewrite from scratch".
   The only op that can lose content, so its description must steer away
   from it.

The **user** speaks natural language; the patch format is the *tool call
schema* the agent compiles that into. It is never written by a human,
which is exactly why it must tolerate an imprecise model rather than be
convenient to parse.

## Why not event sourcing (deferred)

The original plan was: append every action to a log, project it into a
read model in the background, and rebuild the read model in a new shape
whenever the structure needs to change. That was dropped after the design
converged on documents. The reasoning, kept so it isn't re-litigated:

> Event sourcing earns its place exactly when the projection **loses
> information** relative to the input.

- A **document** is lossless — the text the agent wrote *is* the stored
  text. It is its own log; there is nothing else to rebuild from.
- The **search index** does get rebuilt, but from the documents, not from
  a log.
- **Facts** store the original phrasing, so a future move from a flat
  list to, say, an entity graph is an LLM pass over text that is still
  there.

A log of `{old, new}` patches is **git, not event sourcing**: patches are
semantically empty, so replaying them can only ever reproduce the same
text. The flexibility we wanted comes from the stored form being freeform
in the first place.

**When to introduce a log:** with D4's phase-2 memory agent. The moment
an LLM starts summarising feeds and merging duplicates, the projection
becomes lossy and the raw input needs somewhere to live. Nothing is lost
by waiting — until then, all the raw material is in the documents.

## Acceptance

- Two clients (droplet supervisor + Claude Code) are connected to MCP at
  once and both use memory. Only the supervisor session sees
  `get_next_signal`. `docker kill agent` → it reconnects without an MCP
  restart.
- A project round-trips: create it, add documents, `list_docs` shows them
  with summaries, `read_doc` returns body + version, `append_doc` adds
  progress without a prior read.
- `patch_doc` with a stale `expected_version` fails with a conflict and
  changes nothing; with an ambiguous `old` it fails and reports the
  occurrence count; with a near-miss it returns candidate matches.
- `history` lists patches with ids, actors and rationales; reverting the
  last patch works; reverting a conflicting mid-stack patch fails
  cleanly and offers a version rollback.
- `recall` over a query returns refs spanning both projects and facts;
  following a ref with `read_doc` / `get_fact` yields the full content.
- Existing `knowledge_base_notes` are migrated into `facts` and findable
  through `recall`; `add_note` / `find_notes` either still work as
  aliases or are removed together with their skill references.
- With the embedding provider down, documents still read and patch;
  the index catches up on backfill.
- End-to-end: one session records progress on a project, a *different*
  agent in a new session reads the project and continues from it.

## What shipped (2026-08-23)

`crates/mcp/src/memory.rs` (originally `packages/mcp/src/services/memory/`) —
the module and its twelve MCP tools, registered as the `memory` toolset and part
of the default surface. PG tables per the v1 shape above (`memory_*`), migration
`0003_lazy_puppet_master.sql`, applied on mcp boot (later `0005`/`0006` added the
journal and its metadata).

The one structural choice not in the design: **the store is a port**
(`MemoryStore`) with a Postgres implementation and an in-memory one
(`InMemoryStore`). Every rule lives in `MemoryService`; the store is dumb CRUD.
That is what makes the acceptance list below testable without a Postgres.

Delivered against the acceptance list:

- Project round-trip, `list_memory` registry with summaries, `read_doc` with
  version, version-free `append_doc` (also under a named heading) — done.
- `patch_doc`: stale version → conflict carrying the current version, nothing
  written; ambiguous `old` → occurrence count; a normalised quote (em-dash,
  «», ё→е, reflowed whitespace) → the literal from the document that would
  have matched. Atomic across edits.
- `doc_history` with ids, actors, rationales; `revert_patch` exact on the
  newest patch, in place on an older one whose text later writes left alone,
  and a clean `revert_conflict` naming the rollback version otherwise.
  `rollback: true` performs it.
- `recall` spans documents and facts and returns refs that resolve.
- `knowledge_base_notes` import (`memory-import-notes`), idempotent via
  recorded provenance. The old table and `add_note` / `find_notes` are left in
  place — removing them is a follow-up once the skills stop naming them.
- Embedder down: documents still write, read and patch; recall says
  `search_unavailable` rather than "nothing found"; `embed-backfill`
  drains the NULL-vector backlog.
- Hand-off between two sessions over one store — covered end to end.

Not delivered, deliberately:

- **The `docker kill agent` half of the first acceptance bullet.** Multi-client
  works by construction (a `memory`-scoped instance is restricted, therefore
  multi-session, and has no signals tools), but nothing was deployed or killed
  to prove it.
- **Per-token roles.** The actor stamped on a write comes from
  `MCP_MEMORY_ACTOR` on the instance, not from the client — a self-declared
  actor would be forgeable. Real per-token identity is
  `../mcp-auth-and-tool-scoping.md` A2.
- **No compose service and no tunnel exposure.** Adding `memory` to
  `mcp-tunnel`'s `MCP_TOOLSETS` would hand ChatGPT write access to shared
  memory; that is a decision to take deliberately, not a side effect of this
  PR.

### Q8 — answered: twelve tools, one fold

Folded `list_projects` + `list_docs` into `list_memory(project?)`. Nothing else
was folded: `write_doc` into `patch_doc` would have merged the safe op with the
only destructive one, and `history` + `revert` behind a flag is how an agent
discards a document while thinking it is reading. Both trades buy prompt tokens
with the chance of a silent data loss, which is the wrong direction for a store
whose entire value is that it does not lose things.

The short-lived lists from use case 3 are **facts with a state**, not
documents: they have no internal structure worth patching, and `done` /
`archived` is exactly their lifecycle. The public MCP `recall` surface is
active-only: lifecycle selection remains an internal service capability until
there is a separately named history/admin tool. Letting a general-purpose
agent pass `states: ["archived"]` defeated archive as a discovery boundary.

## Open questions

- ~~**Q8 — tool surface.**~~ Answered above: twelve tools, one fold.
- ~~**Q9 — role handshake.**~~ Answered in
  `../mcp-auth-and-tool-scoping.md` A2: the role is a property of the
  client's token, not something the client declares. Self-declared roles
  are forgeable and don't bound anything.
- **Q10 — where the phase-2 memory agent runs.** Never built (the nightly
  `dreaming` skill is the nearest thing). Which worker, which model, what cost
  ceiling?
- ~~**Q11 — how Claude Code reaches the droplet's memory.**~~ Covered by
  `../mcp-auth-and-tool-scoping.md`: TLS proxy + a scoped bearer token in
  `.mcp.json` (`{"type":"http","headers":{"Authorization":"Bearer
  ${MCP_TOKEN}"}}`). That task is now a prerequisite for this one.

## Notes

- Related: `mcp-connection-lifecycle.md` (superseded in part by D1),
  `../multi-chunk-rag.md` (chunking quality feeds D8).
