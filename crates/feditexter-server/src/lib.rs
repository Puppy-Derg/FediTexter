pub mod api;
pub mod auth;
pub mod chat;
pub mod db;
pub mod federation;
pub mod mail;

use axum::routing::{delete, get, post};
use axum::Router;
use api::auth_handlers::{login, logout, me, register, set_avatar, update_me, verify};
use api::chat_handlers::{
    create_conversation, delete_conversation, list_conversations, list_messages, send_message,
};
use api::federation_handlers::{inbox, user_lookup, well_known};
use api::moderation::{block_user, mute_user, unblock_user, unmute_user, user_profile};
use api::ws::ws_handler;
use db::AppState;

pub fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(crate::healthz))
        .route("/.well-known/feditexter", get(well_known))
        .route("/api/register", post(register))
        .route("/api/login", post(login))
        .route("/api/logout", post(logout))
        .route("/api/me", get(me).patch(update_me))
        .route("/api/me/avatar", post(set_avatar))
        .route("/api/verify", post(verify))
        .route("/api/users/{id}", get(user_profile))
        .route("/api/users/{id}/block", post(block_user))
        .route("/api/users/{id}/unblock", post(unblock_user))
        .route("/api/users/{id}/mute", post(mute_user))
        .route("/api/users/{id}/unmute", post(unmute_user))
        .route("/api/conversations", post(create_conversation))
        .route("/api/conversations", get(list_conversations))
        .route("/api/conversations/{id}", delete(delete_conversation))
        .route("/api/conversations/{id}/messages", get(list_messages))
        .route("/api/conversations/{id}/messages", post(send_message))
        .route("/api/federation/users/lookup", get(user_lookup))
        .route("/api/federation/inbox", post(inbox))
        .route("/api/ws", get(ws_handler))
        .with_state(state)
}

async fn healthz() -> &'static str {
    concat!("ok v", env!("CARGO_PKG_VERSION"))
}
