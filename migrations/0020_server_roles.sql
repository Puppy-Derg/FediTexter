-- Server (guild) roles and admin-role membership. The creator is granted the
-- admin role at guild creation; everyone else joins without roles and must be
-- promoted. Invites now expire 7 days after creation.

ALTER TABLE guild_invites
    ADD COLUMN expires_at DATETIME NULL;

CREATE TABLE guild_roles (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    guild_id BIGINT UNSIGNED NOT NULL,
    name VARCHAR(50) NOT NULL,
    is_admin BOOL NOT NULL DEFAULT FALSE,
    FOREIGN KEY (guild_id) REFERENCES guilds(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE guild_member_roles (
    guild_id BIGINT UNSIGNED NOT NULL,
    user_id BIGINT UNSIGNED NOT NULL,
    role_id BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (guild_id, user_id, role_id),
    FOREIGN KEY (guild_id) REFERENCES guilds(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (role_id) REFERENCES guild_roles(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
