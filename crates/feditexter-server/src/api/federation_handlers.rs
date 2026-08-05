use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, Uri};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::api::error::ApiError;
use crate::chat::Message;
use crate::db::AppState;
use crate::federation::{self, INBOX_PATH};

pub async fn well_known(State(state): State<AppState>) -> Json<Value> {
    Json(json!({
        "domain": state.federation.domain,
        "public_key": state.federation.public_key_hex(),
    }))
}

#[derive(Deserialize)]
pub struct LookupParams {
    pub username: String,
    pub domain: String,
}

pub async fn user_lookup(
    State(state): State<AppState>,
    headers: HeaderMap,
    uri: Uri,
    Query(params): Query<LookupParams>,
) -> Result<Json<Value>, ApiError> {
    let auth_header = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or(ApiError::Unauthorized("missing federation auth"))?;
    let path = uri.path_and_query().map(|p| p.as_str()).unwrap_or(uri.path());
    federation::verify_request(&state.pool, &state.federation.client, auth_header, "GET", path, b"").await?;

    let user: Option<(u64, String)> =
        sqlx::query_as("SELECT id, display_name FROM users WHERE username = ? AND server_id = 0")
            .bind(&params.username)
            .fetch_optional(&state.pool)
            .await
            .map_err(|_| ApiError::Internal("db error"))?;

    match user {
        Some((id, display_name)) => Ok(Json(json!({
            "id": id,
            "username": params.username,
            "display_name": display_name,
            "remote": false,
        }))),
        None => Err(ApiError::NotFound("user not found")),
    }
}

#[derive(Deserialize)]
pub struct InboxMessage {
    #[serde(rename = "type")]
    pub kind: String,
    pub from_server: String,
    pub from_username: String,
    pub from_id: u64,
    pub to_id: u64,
    pub body: String,
    #[serde(default)]
    pub sent_at: Option<String>,
}

pub async fn inbox(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, ApiError> {
    let auth_header = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or(ApiError::Unauthorized("missing federation auth"))?;

    let info =
        federation::verify_request(&state.pool, &state.federation.client, auth_header, "POST", INBOX_PATH, &body)
            .await?;

    let payload: InboxMessage =
        serde_json::from_slice(&body).map_err(|_| ApiError::BadRequest("invalid payload"))?;

    if payload.kind != "message" {
        return Err(ApiError::BadRequest("unsupported event type"));
    }
    if payload.from_server != info.domain {
        return Err(ApiError::Unauthorized("from_server mismatch"));
    }
    if payload.body.trim().is_empty() || payload.body.len() > 2000 {
        return Err(ApiError::BadRequest("invalid message body"));
    }

    let recipient: Option<(u64,)> =
        sqlx::query_as("SELECT id FROM users WHERE id = ? AND server_id = 0")
            .bind(payload.to_id)
            .fetch_optional(&state.pool)
            .await
            .map_err(|_| ApiError::Internal("db error"))?;
    let Some((recipient_id,)) = recipient else {
        return Err(ApiError::NotFound("recipient not found"));
    };

    let sender_id = federation::get_or_create_mirror(&state, info.id, payload.from_id, &payload.from_username).await?;

    let conversation_id = crate::api::chat_handlers::ensure_direct_conversation(&state, sender_id, recipient_id).await?;

    let inserted = sqlx::query("INSERT INTO messages (conversation_id, sender_id, body) VALUES (?, ?, ?)")
        .bind(conversation_id)
        .bind(sender_id)
        .bind(&payload.body)
        .execute(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;

    let message: Message = sqlx::query_as(
        "SELECT id, conversation_id, sender_id, body, created_at FROM messages WHERE id = ?",
    )
    .bind(inserted.last_insert_id())
    .fetch_one(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;

    state.hub.publish(message.clone());

    Ok(Json(json!({ "status": "ok", "local_id": message.id })))
}
