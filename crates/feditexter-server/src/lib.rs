pub mod api;
pub mod auth;
pub mod db;

use axum::{routing::{get, post}, Router};
use api::auth_handlers::{login, logout, me, register};
use db::AppState;

pub fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(crate::healthz))
        .route("/api/register", post(register))
        .route("/api/login", post(login))
        .route("/api/logout", post(logout))
        .route("/api/me", get(me))
        .with_state(state)
}

async fn healthz() -> &'static str {
    concat!("ok v", env!("CARGO_PKG_VERSION"))
}