-- Runs move to the ouro engine's store (`ouro_*` tables in this file, design
-- 03 step 2). The agent keeps only what the engine does not: trace anchors,
-- content-addressed evidence, the MCP tool snapshot and the confirmation cards.
-- Pending cards are kept (without the foreign key to the dropped runs) so the
-- first start can edit them to "expired".
CREATE TABLE confirmations_engine (
    run_id TEXT NOT NULL,
    call_key TEXT NOT NULL,
    yes_type TEXT NOT NULL UNIQUE,
    no_type TEXT NOT NULL UNIQUE,
    chat_id TEXT NOT NULL,
    thread_id INTEGER,
    message_id INTEGER,
    card TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    outcome TEXT,
    loop_run TEXT,
    PRIMARY KEY (run_id, call_key)
);
--> statement-breakpoint
INSERT INTO confirmations_engine (run_id, call_key, yes_type, no_type, chat_id, thread_id, message_id, card, created_at_ms, outcome)
SELECT run_id, call_key, yes_type, no_type, chat_id, thread_id, message_id, card, created_at_ms, outcome FROM confirmations;
--> statement-breakpoint
DROP TABLE confirmations;
--> statement-breakpoint
ALTER TABLE confirmations_engine RENAME TO confirmations;
--> statement-breakpoint
CREATE INDEX confirmations_loop ON confirmations(loop_run, call_key);
--> statement-breakpoint
DROP TABLE guard_verdict_uses;
--> statement-breakpoint
CREATE TABLE guard_verdict_uses (
    child_run TEXT NOT NULL,
    call_key TEXT NOT NULL,
    invocation TEXT NOT NULL,
    created_ms INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (child_run, call_key)
);
--> statement-breakpoint
DROP TABLE workflow_listeners;
--> statement-breakpoint
DROP TABLE workflow_awaits;
--> statement-breakpoint
DROP TABLE workflow_events;
--> statement-breakpoint
DROP TABLE workflow_records;
--> statement-breakpoint
DROP TABLE workflow_runs;
--> statement-breakpoint
DROP TABLE workflows;
--> statement-breakpoint
CREATE TABLE run_trace (
    run_id TEXT PRIMARY KEY NOT NULL,
    trace_id TEXT NOT NULL,
    session_id TEXT,
    parent_span_id TEXT,
    recorded INTEGER NOT NULL DEFAULT 0,
    created_ms INTEGER NOT NULL
);
--> statement-breakpoint
CREATE INDEX run_trace_trace ON run_trace(trace_id, recorded);
--> statement-breakpoint
CREATE INDEX run_trace_open ON run_trace(recorded, created_ms);
--> statement-breakpoint
CREATE TABLE evidence (
    digest TEXT PRIMARY KEY NOT NULL,
    body TEXT NOT NULL,
    created_ms INTEGER NOT NULL
);
--> statement-breakpoint
CREATE TABLE run_evidence (
    run_id TEXT NOT NULL,
    digest TEXT NOT NULL,
    PRIMARY KEY (run_id, digest)
);
--> statement-breakpoint
CREATE INDEX run_evidence_digest ON run_evidence(digest);
--> statement-breakpoint
CREATE TABLE tool_snapshot (
    name TEXT PRIMARY KEY NOT NULL,
    read_only INTEGER NOT NULL,
    seen_ms INTEGER NOT NULL
);
