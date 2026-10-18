CREATE TABLE workflow_runs (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    definition TEXT NOT NULL,
    input TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'ready' CHECK (state IN ('ready', 'running', 'waiting', 'completed', 'failed', 'cancelled')),
    wake_at_ms INTEGER NOT NULL DEFAULT 0,
    owner TEXT,
    lease_until_ms INTEGER,
    interruptions INTEGER NOT NULL DEFAULT 0,
    outcome TEXT
);
--> statement-breakpoint
CREATE INDEX workflow_runs_due ON workflow_runs (state, wake_at_ms, lease_until_ms);
--> statement-breakpoint
CREATE TABLE workflow_records (
    run_id TEXT NOT NULL REFERENCES workflow_runs(id),
    key TEXT NOT NULL,
    record TEXT NOT NULL,
    PRIMARY KEY (run_id, key)
);
