CREATE TABLE servers (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    domain VARCHAR(255) NOT NULL UNIQUE,
    public_key VARBINARY(32) NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE instance_meta (
    id TINYINT PRIMARY KEY,
    domain VARCHAR(255) NOT NULL,
    public_key VARBINARY(32) NOT NULL,
    private_key VARBINARY(32) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- remote mirror users have no email / password; server_id 0 = local account
ALTER TABLE users
    MODIFY email VARCHAR(255) NULL,
    ADD COLUMN is_remote BOOL NOT NULL DEFAULT FALSE AFTER display_name,
    ADD COLUMN server_id BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER is_remote,
    ADD COLUMN remote_id BIGINT UNSIGNED NULL AFTER server_id;

-- usernames are only unique within an instance (remote mirrors can share names)
ALTER TABLE users DROP INDEX username;
ALTER TABLE users ADD UNIQUE INDEX uq_users_username_server (username, server_id);
ALTER TABLE users ADD UNIQUE INDEX uq_users_remote (server_id, remote_id);
