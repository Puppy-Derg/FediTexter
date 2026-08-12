-- Voice channels: a channel conversation may be a text channel (the default)
-- or a voice channel. Voice channels carry no messages; members "join" them
-- and talk over a WebRTC P2P mesh relayed through the WS signaling hub.

ALTER TABLE conversations
    ADD COLUMN channel_type VARCHAR(20) NOT NULL DEFAULT 'text';
