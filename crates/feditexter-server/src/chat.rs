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
    Signal { signal: SignalEvent },
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
}

impl SignalKind {
    pub fn from_str(s: &str) -> Option<SignalKind> {
        Some(match s {
            "fetch" => SignalKind::Fetch,
            "offer" => SignalKind::Offer,
            "answer" => SignalKind::Answer,
            "ice" => SignalKind::Ice,
            "cancel" => SignalKind::Cancel,
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
        let (tx, _) = broadcast::channel(256);
        Self { tx }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<HubEvent> {
        self.tx.subscribe()
    }

    pub fn publish_message(&self, message: Message) {
        let _ = self.tx.send(HubEvent::Message { message });
    }

    pub fn publish_signal(&self, signal: SignalEvent) {
        let _ = self.tx.send(HubEvent::Signal { signal });
    }
}
