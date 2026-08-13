-- Per-user privacy overrides on top of the default profile visibility
-- (users.profile_visible). Each row means: user_id has set an explicit
-- SHOW/HIDE override for what target_id can see of user_id's profile.
-- When no override row exists, the default (profile_visible) applies.

CREATE TABLE IF NOT EXISTS privacy_overrides (
    user_id    BIGINT UNSIGNED NOT NULL,
    target_id  BIGINT UNSIGNED NOT NULL,
    visible    TINYINT(1) NOT NULL,
    PRIMARY KEY (user_id, target_id),
    FOREIGN KEY (user_id)   REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (target_id) REFERENCES users(id) ON DELETE CASCADE
);