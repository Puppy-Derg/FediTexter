use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::Response;

use crate::auth::AuthUser;
use crate::db::AppState;
use crate::chat::ChatEvent;

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    auth: AuthUser,
) -> Response {
    ws.on_upgrade(move |socket| ws_loop(socket, state, auth))
}

async fn ws_loop(mut socket: WebSocket, state: AppState, auth: AuthUser) {
    let member_conversations: Vec<(u64,)> = match sqlx::query_as(
        "SELECT conversation_id FROM conversation_members WHERE user_id = ?",
    )
    .bind(auth.user.id)
    .fetch_all(&state.pool)
    .await
    {
        Ok(v) => v,
        Err(_) => return,
    };

    let mut rx = state.hub.subscribe();

    while let Ok(event) = rx.recv().await {
        if event_belongs_to(&event, &member_conversations) {
            let payload = match serde_json::to_string(&event) {
                Ok(p) => p,
                Err(_) => continue,
            };
            if socket.send(WsMessage::Text(payload.into())).await.is_err() {
                break;
            }
        }
    }
}

fn event_belongs_to(event: &ChatEvent, member_conversations: &[(u64,)]) -> bool {
    member_conversations.iter().any(|(id,)| *id == event.message.conversation_id)
}
