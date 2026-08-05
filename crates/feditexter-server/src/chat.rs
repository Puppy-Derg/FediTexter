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
}

#[derive(FromRow, Debug, Clone)]
pub struct ConversationMember {
    pub conversation_id: u64,
    pub user_id: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ChatEvent {
    pub kind: &'static str,
    pub message: Message,
}

#[derive(Clone)]
pub struct ChatHub {
    tx: broadcast::Sender<ChatEvent>,
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

    pub fn subscribe(&self) -> broadcast::Receiver<ChatEvent> {
        self.tx.subscribe()
    }

    pub fn publish(&self, message: Message) {
        let _ = self.tx.send(ChatEvent { kind: "message", message });
    }
}
