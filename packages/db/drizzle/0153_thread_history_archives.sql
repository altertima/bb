CREATE TABLE IF NOT EXISTS `thread_history_archives` (
	`thread_id` text PRIMARY KEY NOT NULL,
	`state` text NOT NULL,
	`format_version` integer NOT NULL,
	`cold_before_sequence` integer NOT NULL,
	`cursor_sequence` integer NOT NULL,
	`row_count` integer NOT NULL,
	`raw_bytes` integer NOT NULL,
	`packed_bytes` integer NOT NULL,
	`cold_at` integer,
	`restored_at` integer,
	`updated_at` integer NOT NULL,
	FOREIGN KEY (`thread_id`) REFERENCES `threads`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `thread_history_archives_state_idx` ON `thread_history_archives` (`state`);