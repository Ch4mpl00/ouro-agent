# Multi-chunk embeddings for long news items (sliding window)

**Status:** pending (deliberately deferred until measured)
**Priority:** P3
**Area:** news / embeddings
**Created:** 2026-06-01 (rewritten for the Rust code 2026-10-20)

## Context

`news_items` holds exactly one embedding per row, computed from the head of the
text (`crates/mcp/src/embeddings.rs`; roughly 8000 characters, about 4 chars per
token). For long HN threads or Habr articles the tail is silently dropped, and
retrieval suffers when the relevant point sits past the cutoff. Memory documents
are already chunked, in their own domain (`memory.rs`, `memory_index`); news is
not.

Multi-chunk needs a sibling table that owns `(item, chunk_idx, body,
embedding)` and a search that joins against it.

## Acceptance

- Migration adds `news_item_chunks (id, news_item_id FK, chunk_idx, body,
  embedding vector(1536))` (a new file in `crates/mcp/migrations/pg/` plus its
  entry in `pg.rs` `MIGRATIONS`; applied files never change).
- The news repository saves the chunks of an item instead of one vector; the
  embedder interface handles several chunks per item (it assumes one today).
- `search_news` joins chunks to items and returns the top-k items by best-chunk
  distance (or aggregated per item), keeping the dedup step.
- A sliding-window chunker (`size`, `overlap`) in the embeddings module.
- `embed-backfill` and the re-embed-on-text-change path handle chunks.

## Notes

Only do this when retrieval quality demonstrably suffers: find a real query
whose relevant chunk lives past the cutoff (the long-form queries q-020..q-034
in `crates/mcp/eval/fixtures/queries.jsonl` are the test bed; run `eval-rag`
before and after). Do not complicate the schema pre-emptively.
