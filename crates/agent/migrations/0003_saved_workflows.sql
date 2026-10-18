CREATE TABLE `workflows` (
	`name` text NOT NULL,
	`version` integer NOT NULL,
	`definition` text NOT NULL,
	`status` text DEFAULT 'draft' NOT NULL,
	`note` text,
	`created_at` text DEFAULT (datetime('now')) NOT NULL,
	`activated_at` text,
	PRIMARY KEY(`name`, `version`)
);
--> statement-breakpoint
CREATE UNIQUE INDEX `workflows_one_active` ON `workflows` (`name`) WHERE `status` = 'active';
