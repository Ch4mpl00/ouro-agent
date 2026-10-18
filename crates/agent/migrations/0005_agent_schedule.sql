CREATE TABLE `scheduled_tasks` (
	`id` integer PRIMARY KEY AUTOINCREMENT NOT NULL,
	`cron_expr` text NOT NULL,
	`recurring` integer NOT NULL,
	`prompt` text NOT NULL,
	`source` text,
	`workflow` text,
	`last_run_at` integer,
	`created_ms` integer NOT NULL
);
--> statement-breakpoint
CREATE TABLE `scheduled_fires` (
	`id` integer PRIMARY KEY AUTOINCREMENT NOT NULL,
	`task_id` integer NOT NULL,
	`slot` integer NOT NULL,
	`fired_at` text DEFAULT (datetime('now')) NOT NULL
);
--> statement-breakpoint
-- Fire ids continue above any signal id MCP handed out (schedule::FIRST_FIRE_ID),
-- so a fire's trace and durable run (`scheduler:<id>`) never land on an older signal's.
INSERT INTO sqlite_sequence (name, seq) VALUES ('scheduled_fires', 1000000000);
