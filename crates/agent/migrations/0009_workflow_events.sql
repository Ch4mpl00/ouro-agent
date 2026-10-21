CREATE TABLE workflow_awaits (
    run_id TEXT NOT NULL REFERENCES workflow_runs(id),
    type TEXT NOT NULL,
    registered_at_ms INTEGER NOT NULL,
    PRIMARY KEY (run_id, type)
);
--> statement-breakpoint
CREATE INDEX workflow_awaits_type ON workflow_awaits(type);
--> statement-breakpoint
CREATE TABLE workflow_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES workflow_runs(id),
    type TEXT NOT NULL,
    data TEXT NOT NULL,
    delivered_at_ms INTEGER NOT NULL,
    consumed_at_ms INTEGER,
    consumed_key TEXT,
    UNIQUE (run_id, consumed_key)
);
--> statement-breakpoint
CREATE INDEX workflow_events_pending ON workflow_events(run_id, consumed_at_ms, id);
