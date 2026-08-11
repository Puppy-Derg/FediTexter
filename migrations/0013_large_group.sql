-- Add 'large_group' to the conversations kind enum (channels)
ALTER TABLE conversations
    MODIFY COLUMN kind ENUM('direct', 'group', 'large_group') NOT NULL DEFAULT 'direct';
