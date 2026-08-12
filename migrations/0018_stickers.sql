-- User-generated sticker packs. Stickers are stored server-side as compressed
-- images (JPEG/WebP, up to 1024x1024) so they can be searched by pack name and
-- sticker name and shared across users.

CREATE TABLE sticker_packs (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    owner_id BIGINT UNSIGNED NOT NULL,
    name VARCHAR(100) NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_sticker_packs_owner (owner_id),
    FOREIGN KEY (owner_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE stickers (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    pack_id BIGINT UNSIGNED NOT NULL,
    name VARCHAR(100) NOT NULL,
    data MEDIUMBLOB NOT NULL,
    mime VARCHAR(50) NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_stickers_pack (pack_id),
    INDEX idx_stickers_name (name),
    FOREIGN KEY (pack_id) REFERENCES sticker_packs(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
