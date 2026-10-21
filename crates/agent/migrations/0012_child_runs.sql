ALTER TABLE workflow_runs ADD COLUMN parent_run TEXT REFERENCES workflow_runs(id);
--> statement-breakpoint
ALTER TABLE workflow_runs ADD COLUMN child_key TEXT;
--> statement-breakpoint
ALTER TABLE workflow_runs ADD COLUMN child_reference TEXT;
--> statement-breakpoint
CREATE INDEX workflow_runs_parent ON workflow_runs(parent_run);
