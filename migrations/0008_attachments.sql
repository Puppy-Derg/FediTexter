ALTER TABLE messages
  ADD COLUMN attachment_mime TEXT NULL,
  ADD COLUMN attachment_name TEXT NULL,
  ADD COLUMN attachment_data MEDIUMTEXT NULL;
