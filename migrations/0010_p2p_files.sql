-- P2P file transfer: the server only stores a small thumbnail plus metadata.
-- The full file bytes are exchanged directly between clients over WebRTC.
ALTER TABLE messages
  ADD COLUMN file_id VARCHAR(64) NULL,
  ADD COLUMN file_size BIGINT NULL,
  ADD COLUMN thumbnail_data MEDIUMTEXT NULL;
