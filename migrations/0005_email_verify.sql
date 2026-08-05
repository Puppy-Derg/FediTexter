-- Email verification plumbing. Enforced only when the server runs with
-- REQUIRE_EMAIL_VERIFICATION=1; dev runs auto-verify instead.
ALTER TABLE users
    ADD COLUMN email_verified BOOL NOT NULL DEFAULT FALSE AFTER server_id,
    ADD COLUMN verification_code VARCHAR(64) NULL AFTER email_verified;

-- Existing local accounts stay usable until verification is enforced.
UPDATE users SET email_verified = TRUE WHERE is_remote = FALSE;
