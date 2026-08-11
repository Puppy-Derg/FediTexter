-- Track the sender's original message id for federation cross-referencing
-- (edits and deletes arrive with the remote message id, not the local one).
ALTER TABLE messages ADD COLUMN remote_message_id BIGINT UNSIGNED NULL;
