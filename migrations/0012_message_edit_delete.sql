-- Add edit / soft-delete support to messages.
-- edited_at:       when the message was last edited (NULL = never edited).
-- original_body:   the body before the most recent edit (NULL if never edited).
-- deleted_at:      soft-delete timestamp (NULL = not deleted).

ALTER TABLE messages
  ADD COLUMN edited_at DATETIME NULL AFTER thumbnail_data,
  ADD COLUMN original_body TEXT NULL AFTER edited_at,
  ADD COLUMN deleted_at DATETIME NULL AFTER original_body;
