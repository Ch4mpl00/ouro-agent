-- The agent's workflow versions live in the engine's catalog (engine.db);
-- the agent keeps only what the engine lacks, keyed by content: the note,
-- whether a dry run tried it, whether it is a built-in text (design 04).
-- The built-in digests the first engine boots kept in the memory KV
-- (`engine.builtin_digests.<name>`, a JSON array) are imported once.
CREATE TABLE workflow_meta (
    name TEXT NOT NULL,
    digest TEXT NOT NULL,
    note TEXT,
    builtin INTEGER NOT NULL DEFAULT 0,
    created_ms INTEGER NOT NULL,
    tried_ms INTEGER,
    PRIMARY KEY (name, digest)
);
--> statement-breakpoint
INSERT OR IGNORE INTO workflow_meta (name, digest, builtin, created_ms)
SELECT substr(m.key, length('engine.builtin_digests.') + 1), d.value, 1, CAST(strftime('%s', 'now') AS INTEGER) * 1000
FROM memory m, json_each(m.value) d
WHERE m.key LIKE 'engine.builtin_digests.%' AND json_valid(m.value) AND d.type = 'text';
