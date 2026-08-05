pub mod api;
pub mod auth;
pub mod chat;
pub mod db;
pub mod federation;

use axum::routing::{get, post};
use axum::Router;
use api::auth_handlers::{login, logout, me, register};
use api::chat_handlers::{create_conversation, list_conversations, list_messages, send_message};
use api::federation_handlers::{inbox, user_lookup, well_known};
use api::ws::ws_handler;
use db::AppState;

pub fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(crate::healthz))
        .route("/.well-known/feditexter", get(well_known))
        .route("/api/register", post(register))
        .route("/api/login", post(login))
        .route("/api/logout", post(logout))
        .route("/api/me", get(me))
        .route("/api/conversations", post(create_conversation))
        .route("/api/conversations", get(list_conversations))
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
