use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::Response;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;

use crate::auth::AuthUser;
use crate::chat::{HubEvent, SignalEvent, SignalKind};
use crate::db::AppState;
use crate::federation;

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

/// A signaling message a client sends up over the WebSocket.
#[derive(Deserialize)]
struct ClientSignal {
    #[serde(rename = "type")]
    kind: String,
    to_user_id: u64,
    file_id: String,
    #[serde(default)]
    data: Option<String>,
}

/// A voice-channel presence message (`voice_join` / `voice_leave`).
#[derive(Deserialize)]
struct VoiceSignal {
    #[serde(rename = "type")]
    kind: String,
    guild_id: u64,
    channel_id: u64,
}

async fn ws_loop(socket: WebSocket, state: AppState, auth: AuthUser) {
    let mut member_conversations = load_members(&state, auth.user.id).await;
    let mut rx = state.hub.subscribe();
    // The voice channel this connection has joined, if any (for cleanup on drop).
    let mut voice_channel: Option<(u64, u64)> = None;

    // Refresh the membership snapshot so conversations created after the
    // connection opened (e.g. the user starting a chat with the bot) still
    // get pushed.
    let mut refresh = tokio::time::interval(std::time::Duration::from_secs(5));
    refresh.tick().await;

    // Mark the user online and announce it.
    {
        let mut online = state.presence.lock().unwrap();
        let was_online = !online.is_empty() && online.contains(&auth.user.id);
        online.insert(auth.user.id);
        if !was_online {
            drop(online);
            state.hub.publish_presence(auth.user.id, true);
        }
    }

    let (mut sink, mut stream) = socket.split();

    loop {
        tokio::select! {
            _ = refresh.tick() => {
                member_conversations = load_members(&state, auth.user.id).await;
            }
            event = rx.recv() => {
                match event {
                    Ok(HubEvent::Message { message }) => {
                        if event_belongs_to(&message.conversation_id, &member_conversations) {
                            let payload = match serde_json::to_string(&HubEvent::Message { message }) {
                                Ok(p) => p,
                                Err(_) => continue,
                            };
                            if sink.send(WsMessage::Text(payload.into())).await.is_err() {
                                break;
                            }
                        }
                    }
                    Ok(HubEvent::MessageEdited { message }) => {
                        if event_belongs_to(&message.conversation_id, &member_conversations) {
                            let payload = match serde_json::to_string(&HubEvent::MessageEdited { message }) {
                                Ok(p) => p,
                                Err(_) => continue,
                            };
                            if sink.send(WsMessage::Text(payload.into())).await.is_err() {
                                break;
                            }
                        }
                    }
                    Ok(HubEvent::MessageDeleted { conversation_id, message_id }) => {
                        if event_belongs_to(&conversation_id, &member_conversations) {
                            let payload = match serde_json::to_string(&HubEvent::MessageDeleted { conversation_id, message_id }) {
                                Ok(p) => p,
                                Err(_) => continue,
                            };
                            if sink.send(WsMessage::Text(payload.into())).await.is_err() {
                                break;
                            }
                        }
                    }
                    Ok(HubEvent::Signal { signal }) => {
                        if signal.target_user_id == auth.user.id {
                            let payload = match serde_json::to_string(&HubEvent::Signal { signal }) {
                                Ok(p) => p,
                                Err(_) => continue,
                            };
                            if sink.send(WsMessage::Text(payload.into())).await.is_err() {
                                break;
                            }
                        }
                    }
                    Ok(HubEvent::Typing { conversation_id, from_user_id, from_username }) => {
                        // Don't echo a user's own typing back to themselves.
                        if from_user_id != auth.user.id
                            && event_belongs_to(&conversation_id, &member_conversations)
                        {
                            let payload = match serde_json::to_string(&HubEvent::Typing {
                                conversation_id,
                                from_user_id,
                                from_username,
                            }) {
                                Ok(p) => p,
                                Err(_) => continue,
                            };
                            if sink.send(WsMessage::Text(payload.into())).await.is_err() {
                                break;
                            }
                        }
                    }
                    Ok(HubEvent::Presence { user_id, online }) => {
                        // Broadcast to every client; each one filters by the
                        // members of its own conversations.
                        let payload = match serde_json::to_string(&HubEvent::Presence { user_id, online }) {
                            Ok(p) => p,
                            Err(_) => continue,
                        };
                        if sink.send(WsMessage::Text(payload.into())).await.is_err() {
                            break;
                        }
                    }
                    Ok(HubEvent::VoicePresence { channel_id, user_id, username, joined }) => {
                        if event_belongs_to(&channel_id, &member_conversations) {
                            let payload = match serde_json::to_string(&HubEvent::VoicePresence {
                                channel_id,
                                user_id,
                                username,
                                joined,
                            }) {
                                Ok(p) => p,
                                Err(_) => continue,
                            };
                            if sink.send(WsMessage::Text(payload.into())).await.is_err() {
                                break;
                            }
                        }
                    }
                    Ok(HubEvent::VoiceState { channel_id, users, target_user_id }) => {
                        if target_user_id == auth.user.id {
                            let payload = match serde_json::to_string(&HubEvent::VoiceState {
                                channel_id,
                                users,
                                target_user_id,
                            }) {
                                Ok(p) => p,
                                Err(_) => continue,
                            };
                            if sink.send(WsMessage::Text(payload.into())).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
            incoming = stream.next() => {
                match incoming {
                    Some(Ok(WsMessage::Text(text))) => {
                        // Typing notifications carry only a type + conversation.
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text)
                            && v.get("type").and_then(|t| t.as_str()) == Some("typing")
                            && let Some(conversation_id) = v.get("conversation_id").and_then(|c| c.as_u64())
                        {
                            state.hub.publish_typing(
                                conversation_id,
                                auth.user.id,
                                auth.user.username.clone(),
                            );
                            continue;
                        }
                        // Voice channel join/leave presence messages.
                        if let Ok(voice) = serde_json::from_str::<VoiceSignal>(&text) {
                            match voice.kind.as_str() {
                                "voice_join" => {
                                    if voice_join(&state, &auth, voice.guild_id, voice.channel_id).await {
                                        voice_channel = Some((voice.guild_id, voice.channel_id));
                                    }
                                }
                                "voice_leave" => {
                                    if voice_channel == Some((voice.guild_id, voice.channel_id)) {
                                        voice_channel = None;
                                        voice_leave(&state, &auth, voice.guild_id, voice.channel_id).await;
                                    }
                                }
                                _ => {}
                            }
                            continue;
                        }
                        if let Ok(sig) = serde_json::from_str::<ClientSignal>(&text) {
                            route_signal(&state, &auth, &sig).await;
                        }
                    }
                    Some(Ok(_)) => {}
                    _ => break,
                }
            }
        }
    }

    // Mark the user offline and announce it.
    {
        let mut online = state.presence.lock().unwrap();
        online.remove(&auth.user.id);
        drop(online);
        state.hub.publish_presence(auth.user.id, false);
    }

    // Leaving the voice channel if the connection dropped while connected.
    if let Some((guild_id, channel_id)) = voice_channel {
        voice_leave(&state, &auth, guild_id, channel_id).await;
    }
}

fn event_belongs_to(conversation_id: &u64, member_conversations: &[(u64,)]) -> bool {
    member_conversations.iter().any(|(id,)| id == conversation_id)
}

/// Validate and record a `voice_join`: the channel must be a voice channel of a
/// guild the caller belongs to. On success, tells the joining client who is
/// already in the channel and announces the join to the other members.
async fn voice_join(state: &AppState, auth: &AuthUser, guild_id: u64, channel_id: u64) -> bool {
    let row: Option<(String, u64)> = sqlx::query_as(
        "SELECT channel_type, guild_id FROM conversations WHERE id = ?",
    )
    .bind(channel_id)
    .fetch_optional(&state.pool)
    .await
    .unwrap_or_default();
    let Some((channel_type, actual_guild)) = row else {
        return false;
    };
    if channel_type != "voice" || actual_guild != guild_id {
        return false;
    }
    let member: Option<(u64,)> = sqlx::query_as(
        "SELECT user_id FROM guild_members WHERE guild_id = ? AND user_id = ?",
    )
    .bind(guild_id)
    .bind(auth.user.id)
    .fetch_optional(&state.pool)
    .await
    .unwrap_or_default();
    if member.is_none() {
        return false;
    }

    let mut voice = state.voice.lock().unwrap();
    let occupants = voice.entry((guild_id, channel_id)).or_default();
    let was_present = occupants.contains_key(&auth.user.id);
    let users_before: Vec<(u64, String)> = occupants
        .iter()
        .filter(|(uid, _)| **uid != auth.user.id)
        .map(|(uid, name)| (*uid, name.clone()))
        .collect();
    occupants.insert(auth.user.id, auth.user.username.clone());
    drop(voice);

    if !was_present {
        state.hub.publish_voice_presence(
            channel_id,
            auth.user.id,
            auth.user.username.clone(),
            true,
        );
    }
    // The joiner connects out to the existing occupants.
    state
        .hub
        .publish_voice_state(channel_id, users_before, auth.user.id);
    true
}

/// Record a `voice_leave` and announce it to the channel's other members.
async fn voice_leave(state: &AppState, auth: &AuthUser, guild_id: u64, channel_id: u64) {
    let removed = {
        let mut voice = state.voice.lock().unwrap();
        let mut removed = false;
        if let Some(occupants) = voice.get_mut(&(guild_id, channel_id)) {
            if occupants.remove(&auth.user.id).is_some() {
                removed = true;
            }
            if occupants.is_empty() {
                voice.remove(&(guild_id, channel_id));
            }
        }
        removed
    };
    if removed {
        state.hub.publish_voice_presence(
            channel_id,
            auth.user.id,
            auth.user.username.clone(),
            false,
        );
    }
}

async fn route_signal(state: &AppState, auth: &AuthUser, sig: &ClientSignal) {
    let Some(kind) = SignalKind::from_str(&sig.kind) else {
        return;
    };
    let is_voice = matches!(
        kind,
        SignalKind::VoiceOffer | SignalKind::VoiceAnswer | SignalKind::VoiceIce | SignalKind::VoiceHangup
    );
    // Look up the target. If the target is a remote mirror, forward the
    // signaling to their server via the federation inbox. `remote_id` is NULL
    // for local users, so it must decode as an Option. Voice signaling is
    // guild-local only — never federated.
    let target: Option<(u64, Option<u64>, bool, String)> = sqlx::query_as(
        "SELECT u.server_id, u.remote_id, u.is_remote, COALESCE(s.domain, '')
         FROM users u LEFT JOIN servers s ON s.id = u.server_id
         WHERE u.id = ?",
    )
    .bind(sig.to_user_id)
    .fetch_optional(&state.pool)
    .await
    .unwrap_or_default();

    let Some((_server_id, remote_id, is_remote, domain)) = target else {
        return;
    };

    if is_remote && !is_voice {
        if let Some(remote_id) = remote_id {
            let _ = federation::deliver_signal_to_remote(
                state,
                remote_id,
                &domain,
                &auth.user,
                &sig.file_id,
                &kind,
                sig.data.as_deref(),
            )
            .await;
        }
        return;
    }

    state.hub.publish_signal(SignalEvent {
        file_id: sig.file_id.clone(),
        kind,
        data: sig.data.clone(),
        from_username: Some(auth.user.username.clone()),
        from_user_id: Some(auth.user.id),
        target_user_id: sig.to_user_id,
    });
}
