use serde::Serialize;
use sqlx::FromRow;
use tokio::sync::broadcast;

#[derive(FromRow, Serialize, Debug, Clone)]
pub struct Message {
    pub id: u64,
    pub conversation_id: u64,
    pub sender_id: u64,
    pub body: String,
    pub created_at: chrono::NaiveDateTime,
    #[serde(default)]
    pub attachment_mime: Option<String>,
    #[serde(default)]
    pub attachment_name: Option<String>,
    #[serde(default)]
    pub attachment_data: Option<String>,
    #[serde(default)]
    pub file_id: Option<String>,
    #[serde(default)]
    pub file_size: Option<i64>,
    #[serde(default)]
    pub thumbnail_data: Option<String>,
    #[serde(default)]
    pub edited_at: Option<chrono::NaiveDateTime>,
    #[serde(default)]
    pub original_body: Option<String>,
    #[serde(default)]
    pub deleted_at: Option<chrono::NaiveDateTime>,
    /// The sender's original message id, stored when the message arrived via
    /// federation. Used to cross-reference edits/deletes (which arrive with the
    /// remote id, not the local one).
    #[serde(default)]
    #[sqlx(default)]
    pub remote_message_id: Option<u64>,
}

#[derive(FromRow, Debug, Clone)]
pub struct ConversationMember {
    pub conversation_id: u64,
    pub user_id: u64,
}

/// Events the server pushes to connected clients over WebSocket.
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum HubEvent {
    Message { message: Message },
    #[serde(rename = "message_edited")]
    MessageEdited { message: Message },
    #[serde(rename = "message_deleted")]
    MessageDeleted { conversation_id: u64, message_id: u64 },
    Signal { signal: SignalEvent },
    /// Someone started (re)typing in a conversation.
    Typing {
        conversation_id: u64,
        from_user_id: u64,
        from_username: String,
    },
    /// A user's online status changed.
    Presence { user_id: u64, online: bool },
    /// A user joined/left a voice channel.
    VoicePresence {
        channel_id: u64,
        user_id: u64,
        username: String,
        joined: bool,
    },
    /// The current occupants of a voice channel, delivered to a user right after
    /// they join so they can build the P2P mesh.
    VoiceState {
        channel_id: u64,
        users: Vec<(u64, String)>,
        #[serde(skip_serializing)]
        target_user_id: u64,
    },
}

/// WebRTC signaling relayed between clients. `target_user_id` is used by the
/// server hub to route the event to the right client and is stripped from the
/// JSON that is actually sent down the socket.
#[derive(Clone, Debug, Serialize)]
pub struct SignalEvent {
    pub file_id: String,
    #[serde(rename = "type")]
    pub kind: SignalKind,
    #[serde(default)]
    pub data: Option<String>,
    #[serde(default)]
    pub from_username: Option<String>,
    #[serde(default)]
    pub from_user_id: Option<u64>,
    #[serde(skip_serializing)]
    pub target_user_id: u64,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SignalKind {
    /// Recipient asks the sender to start serving the file.
    Fetch,
    /// SDP offer (sender side).
    Offer,
    /// SDP answer (recipient side).
    Answer,
    /// ICE candidate (either side).
    Ice,
    /// Sender no longer has the file (e.g. restarted).
    Cancel,
    /// Voice chat: SDP offer between two members of a voice channel.
    VoiceOffer,
    /// Voice chat: SDP answer.
    VoiceAnswer,
    /// Voice chat: ICE candidate.
    VoiceIce,
    /// Voice chat: tear down the peer connection.
    VoiceHangup,
}

impl SignalKind {
    pub fn from_str(s: &str) -> Option<SignalKind> {
        Some(match s {
            "fetch" => SignalKind::Fetch,
            "offer" => SignalKind::Offer,
            "answer" => SignalKind::Answer,
            "ice" => SignalKind::Ice,
            "cancel" => SignalKind::Cancel,
            "voice_offer" => SignalKind::VoiceOffer,
            "voice_answer" => SignalKind::VoiceAnswer,
            "voice_ice" => SignalKind::VoiceIce,
            "voice_hangup" => SignalKind::VoiceHangup,
            _ => return None,
        })
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            SignalKind::Fetch => "fetch",
            SignalKind::Offer => "offer",
            SignalKind::Answer => "answer",
            SignalKind::Ice => "ice",
            SignalKind::Cancel => "cancel",
            SignalKind::VoiceOffer => "voice_offer",
            SignalKind::VoiceAnswer => "voice_answer",
            SignalKind::VoiceIce => "voice_ice",
            SignalKind::VoiceHangup => "voice_hangup",
        }
    }
}

#[derive(Clone)]
pub struct ChatHub {
    tx: broadcast::Sender<HubEvent>,
}

impl Default for ChatHub {
    fn default() -> Self {
        Self::new()
    }
}

impl ChatHub {
    pub fn new() -> Self {
        // Generous buffer so a slow client doesn't lag out and get disconnected
        // during bursts (e.g. presence flushes, large conversation loads).
        let (tx, _) = broadcast::channel(1024);
        Self { tx }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<HubEvent> {
        self.tx.subscribe()
    }

    pub fn publish_message(&self, message: Message) {
        let _ = self.tx.send(HubEvent::Message { message });
    }

    pub fn publish_message_edited(&self, message: Message) {
        let _ = self.tx.send(HubEvent::MessageEdited { message });
    }

    pub fn publish_message_deleted(&self, conversation_id: u64, message_id: u64) {
        let _ = self.tx.send(HubEvent::MessageDeleted { conversation_id, message_id });
    }

    pub fn publish_signal(&self, signal: SignalEvent) {
        let _ = self.tx.send(HubEvent::Signal { signal });
    }

    pub fn publish_typing(&self, conversation_id: u64, from_user_id: u64, from_username: String) {
        let _ = self.tx.send(HubEvent::Typing {
            conversation_id,
            from_user_id,
            from_username,
        });
    }

    pub fn publish_presence(&self, user_id: u64, online: bool) {
        let _ = self.tx.send(HubEvent::Presence { user_id, online });
    }

    pub fn publish_voice_presence(
        &self,
        channel_id: u64,
        user_id: u64,
        username: String,
        joined: bool,
    ) {
        let _ = self.tx.send(HubEvent::VoicePresence {
            channel_id,
            user_id,
            username,
            joined,
        });
    }

    pub fn publish_voice_state(&self, channel_id: u64, users: Vec<(u64, String)>, target_user_id: u64) {
        let _ = self.tx.send(HubEvent::VoiceState {
            channel_id,
            users,
            target_user_id,
        });
    }
}
