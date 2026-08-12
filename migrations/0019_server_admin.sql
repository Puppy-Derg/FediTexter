-- Server (guild) administration: bans, and user profile privacy columns.

CREATE TABLE guild_bans (
    guild_id BIGINT UNSIGNED NOT NULL,
    user_id BIGINT UNSIGNED NOT NULL,
    banned_by BIGINT UNSIGNED NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (guild_id, user_id),
    FOREIGN KEY (guild_id) REFERENCES guilds(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (banned_by) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Profile info: a short bio, and whether other users may see rich profile
-- details (bio/avatar). When hidden, profile views return a bare-bones record.
ALTER TABLE users
    ADD COLUMN bio VARCHAR(500) NOT NULL DEFAULT '',
    ADD COLUMN profile_visible BOOL NOT NULL DEFAULT TRUE;
