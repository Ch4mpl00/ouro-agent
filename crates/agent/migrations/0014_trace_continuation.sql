ALTER TABLE workflow_runs ADD COLUMN trace_anchor TEXT;
--> statement-breakpoint
ALTER TABLE workflow_runs ADD COLUMN trace_recorded INTEGER NOT NULL DEFAULT 0;
--> statement-breakpoint
CREATE INDEX workflow_runs_trace ON workflow_runs(json_extract(trace_anchor, '$.trace_id'));
--> statement-breakpoint
ALTER TABLE traces ADD COLUMN continued_output INTEGER NOT NULL DEFAULT 0;
