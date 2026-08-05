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

async fn load_members(state: &AppState, user_id: u64) -> Vec<(u64,)> {
    sqlx::query_as("SELECT conversation_id FROM conversation_members WHERE user_id = ?")
        .bind(user_id)
        .fetch_all(&state.pool)
        .await
        .unwrap_or_default()
}

async fn ws_loop(mut socket: WebSocket, state: AppState, auth: AuthUser) {
    let mut member_conversations = load_members(&state, auth.user.id).await;
    let mut rx = state.hub.subscribe();

    // Refresh the membership snapshot so conversations created after the
    // connection opened (e.g. the user starting a chat with the bot) still
    // get pushed.
    let mut refresh = tokio::time::interval(std::time::Duration::from_secs(5));
    refresh.tick().await;

    loop {
        tokio::select! {
            _ = refresh.tick() => {
                member_conversations = load_members(&state, auth.user.id).await;
            }
            event = rx.recv() => {
                match event {
                    Ok(ev) => {
                        if event_belongs_to(&ev, &member_conversations) {
                            let payload = match serde_json::to_string(&ev) {
                                Ok(p) => p,
                                Err(_) => continue,
                            };
                            if socket.send(WsMessage::Text(payload.into())).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }
}

fn event_belongs_to(event: &ChatEvent, member_conversations: &[(u64,)]) -> bool {
    member_conversations.iter().any(|(id,)| *id == event.message.conversation_id)
}
