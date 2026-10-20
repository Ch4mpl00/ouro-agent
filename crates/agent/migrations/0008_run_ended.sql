ALTER TABLE workflow_runs ADD COLUMN ended_ms INTEGER;
--> statement-breakpoint
UPDATE workflow_runs SET ended_ms = CAST(strftime('%s', 'now') AS INTEGER) * 1000
WHERE state IN ('completed', 'failed', 'cancelled');
--> statement-breakpoint
CREATE INDEX workflow_runs_ended ON workflow_runs (ended_ms);
