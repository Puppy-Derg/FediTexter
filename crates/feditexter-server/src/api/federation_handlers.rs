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
    #[serde(default)]
    pub file_id: Option<String>,
    #[serde(default)]
    pub file_size: Option<i64>,
    #[serde(default)]
    pub thumbnail_data: Option<String>,
    #[serde(default)]
    pub attachment_mime: Option<String>,
    #[serde(default)]
    pub attachment_name: Option<String>,
    #[serde(default)]
    pub signal_kind: Option<String>,
    #[serde(default)]
    pub signal_data: Option<String>,
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

    if payload.from_server != info.domain {
        return Err(ApiError::Unauthorized("from_server mismatch"));
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

    // WebRTC signaling for P2P files: relay to the local recipient.
    if payload.kind == "signal" {
        let file_id = payload.file_id.ok_or(ApiError::BadRequest("signal requires a file_id"))?;
        let sig_kind = payload.signal_kind.ok_or(ApiError::BadRequest("signal requires a signal_kind"))?;
        let kind = crate::chat::SignalKind::from_str(&sig_kind)
            .ok_or(ApiError::BadRequest("unsupported signal kind"))?;
        state.hub.publish_signal(crate::chat::SignalEvent {
            file_id,
            kind,
            data: payload.signal_data,
            from_username: Some(payload.from_username),
            from_user_id: Some(sender_id),
            target_user_id: recipient_id,
        });
        return Ok(Json(json!({ "status": "ok" })));
    }

    if payload.kind != "message" {
        return Err(ApiError::BadRequest("unsupported event type"));
    }
    if payload.body.trim().is_empty() || payload.body.len() > 2000 {
        return Err(ApiError::BadRequest("invalid message body"));
    }

    let conversation_id = crate::api::chat_handlers::ensure_direct_conversation(&state, sender_id, recipient_id).await?;

    let created_at = chrono::Utc::now().naive_utc();
    let inserted = sqlx::query(
        "INSERT INTO messages (conversation_id, sender_id, body, created_at, attachment_mime, attachment_name,
                               file_id, file_size, thumbnail_data)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(conversation_id)
    .bind(sender_id)
    .bind(&payload.body)
    .bind(created_at)
    .bind(&payload.attachment_mime)
    .bind(&payload.attachment_name)
    .bind(&payload.file_id)
    .bind(payload.file_size)
    .bind(&payload.thumbnail_data)
    .execute(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;

    let message: Message = sqlx::query_as(
        "SELECT id, conversation_id, sender_id, body, created_at, attachment_mime, attachment_name, attachment_data,
                file_id, file_size, thumbnail_data FROM messages WHERE id = ?",
    )
    .bind(inserted.last_insert_id())
    .fetch_one(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;

    state.hub.publish_message(message.clone());

    Ok(Json(json!({ "status": "ok", "local_id": message.id })))
}
