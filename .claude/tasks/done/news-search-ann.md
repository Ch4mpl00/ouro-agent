# HNSW index for news search

**Status:** done
**Priority:** P1
**Area:** mcp / news search (`crates/mcp/src/news.rs`)
**Created:** 2026-10-14

## Context

`search_news` took 8–16 s on prod (`recall` over memory: ~0.2 s). The store
had grown to 117 561 rows (92k Telegram channel posts) with no ANN index —
the schema comment said to add one past ~50–100k rows. Every search was an
exact sequential scan: `EXPLAIN ANALYZE` 9.5 s, reading ~1.1 GB of TOASTed
1536-dim vectors from disk on a 2 GB droplet (`shared_buffers` 128 MB, swap
in use). A batch of 3 queries took 15–16 s.

## What was done

- `news_items_embedding_hnsw`: HNSW over `(embedding::halfvec(1536))
  halfvec_cosine_ops` (pgvector 0.8.2). halfvec halves the graph: 444 MB.
  Migration `0007_news_embedding_hnsw.sql` uses `IF NOT EXISTS`; on prod the
  index was built beforehand with `CREATE INDEX CONCURRENTLY`
  (`maintenance_work_mem=400MB`, no parallel workers — Docker's 64 MB
  `/dev/shm`): 7 min 10 s, writes not blocked. A plain build in the boot
  migration would have held news inserts for that long.
- Search orders by the indexed expression; the returned `distance` is still
  the exact one on the full vector. Per query, in a transaction:
  `hnsw.ef_search = min(600, max(200, 4 × pool))`,
  `hnsw.iterative_scan = relaxed_order`, `enable_seqscan = off` (at ef ≈ 1000
  the planner priced the index above a seq scan and fell back to 8–9 s).
- **A `source` or `channel` filter keeps the exact ordering.** The walk
  filters what it finds, and source is a topic and a language (Habr = Russian
  tech, channels = Russian news), so for a query far from that
  sub-population it ran out of budget: habr/hackernews top-10 recall 0.72–0.9,
  sometimes fewer rows than asked, at any ef_search or iterative mode. Time
  filters cut across topics and stay on the index (or the planner takes the
  `posted_at` btree and sorts exactly).

## Measurements (prod, 2026-10-14)

Exact top-10 vs the ANN pool; 20 real text queries (ru/en) + 10 random rows
as queries (self excluded). "Tie-aware" counts an ANN hit whose exact
distance is within the exact 10th — the corpus holds cross-posted duplicates
at equal distance, which the dedup step collapses anyway.

| scenario | k | top-10 recall (text queries) | tie-aware (all) | ms p50 / p95 (cold) |
|---|---|---|---|---|
| no filter | 10 | 0.99 | 0.993 | 860 / 1180 |
| no filter | 50 | 0.99 | 0.993 | 610 / 940 |
| no filter | 150 | 0.99 | 0.993 | 680 / 940 |
| since 30 d | 10 / 50 / 150 | 0.98 / 1.0 / 1.0 | 0.983 / 1.0 / 1.0 | 12–930 |
| since 3 d | any | 1.0 | 1.0 | 60–80 |

Before: 9 500 ms per query, recall 1.0 by construction. With warm pages the
unfiltered walk is 20–50 ms; the ~0.7 s above is the droplet reading index
pages after the exact scans of the benchmark evicted them.

Source/channel filters (exact ordering, unchanged recall): hackernews
0.4–4 s, habr up to ~9 s cold, a small channel 0.3 s, the biggest channel
(ejdailyru, 20k posts) ~8 s — the planner misestimates the jsonb filter and
scans the heap. Not a regression; see Notes.

## Notes

- Partial HNSW indexes per source would fix filtered recall but double the
  index memory on a 2 GB box. Revisit if source-filtered search becomes
  common (planner.md tells the digests to search with NO source).
- An expression index on `metadata->>'chat_username'` would make per-channel
  search fast; not needed yet.
- Retention for channel posts (92k of 117k rows) would shrink both the
  table and the graph.
