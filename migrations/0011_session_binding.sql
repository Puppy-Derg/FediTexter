-- Bind sessions to the device that logged in: record the client's device UUID
-- and its IP so a stolen token cannot be replayed from elsewhere.
ALTER TABLE sessions
  ADD COLUMN device_id VARCHAR(64) NULL,
  ADD COLUMN login_ip VARCHAR(45) NULL;
