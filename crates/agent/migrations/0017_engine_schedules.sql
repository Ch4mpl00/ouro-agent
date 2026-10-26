-- Scheduled tasks moved onto the engine's triggers (docs/designs/06-schedules.md):
-- a task is the definition `task-<id>` in engine.db, its id from `task_ids`,
-- which continues the old `scheduled_tasks` sequence (ids are never reused).
CREATE TABLE `task_ids` (
	`id` integer PRIMARY KEY AUTOINCREMENT NOT NULL
);
--> statement-breakpoint
INSERT INTO sqlite_sequence (name, seq)
SELECT 'task_ids', COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'scheduled_tasks'), 0);
--> statement-breakpoint
-- A fire is recorded by its trigger's run (`run_id`, at most one row each)
-- with the task's parts, and run as a signal by the supervisor's drain
-- (`pending` until its signal run exists, then `done`). New rows keep slots
-- in milliseconds; the old rows' are seconds.
ALTER TABLE `scheduled_fires` ADD COLUMN `run_id` text;
--> statement-breakpoint
ALTER TABLE `scheduled_fires` ADD COLUMN `previous` integer;
--> statement-breakpoint
ALTER TABLE `scheduled_fires` ADD COLUMN `cron` text;
--> statement-breakpoint
ALTER TABLE `scheduled_fires` ADD COLUMN `recurring` integer;
--> statement-breakpoint
ALTER TABLE `scheduled_fires` ADD COLUMN `prompt` text;
--> statement-breakpoint
ALTER TABLE `scheduled_fires` ADD COLUMN `source` text;
--> statement-breakpoint
ALTER TABLE `scheduled_fires` ADD COLUMN `workflow` text;
--> statement-breakpoint
ALTER TABLE `scheduled_fires` ADD COLUMN `state` text DEFAULT 'pending' NOT NULL;
--> statement-breakpoint
-- The old fires ran under the tick: their history never reaches the drain.
UPDATE `scheduled_fires` SET `state` = 'done';
--> statement-breakpoint
CREATE UNIQUE INDEX `scheduled_fires_run` ON `scheduled_fires` (`run_id`);
--> statement-breakpoint
CREATE INDEX `scheduled_fires_state` ON `scheduled_fires` (`state`, `id`);
--> statement-breakpoint
CREATE INDEX `scheduled_fires_task` ON `scheduled_fires` (`task_id`, `slot`);
--> statement-breakpoint
DROP TABLE `scheduled_tasks`;
