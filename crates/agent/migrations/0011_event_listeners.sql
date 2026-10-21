CREATE TABLE workflow_listeners (
    run_id TEXT NOT NULL REFERENCES workflow_runs(id),
    key TEXT NOT NULL,
    type TEXT NOT NULL,
    PRIMARY KEY (run_id, key, type)
);
--> statement-breakpoint
CREATE INDEX workflow_listeners_type ON workflow_listeners(run_id, type);
