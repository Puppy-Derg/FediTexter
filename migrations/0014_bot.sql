-- In-app bot user + verification/2FA reminder tracking + patch-note tracking.
ALTER TABLE users ADD COLUMN is_bot BOOL NOT NULL DEFAULT FALSE;
ALTER TABLE users ADD COLUMN last_reminder_at DATETIME NULL;
ALTER TABLE users ADD COLUMN last_patch_tag VARCHAR(32) NULL;
