use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::api::error::ApiError;
use crate::auth::AuthUser;
use crate::chat::Message;
use crate::db::AppState;
use crate::federation;

#[derive(Deserialize)]
pub struct CreateConversationRequest {
    pub user_id: Option<u64>,
    pub handle: Option<String>,
    #[serde(default)]
    pub member_ids: Vec<u64>,
    #[serde(default)]
    pub handles: Vec<String>,
}

#[derive(Deserialize)]
pub struct SendMessageRequest {
    pub body: String,
    #[serde(default)]
    pub attachment_mime: Option<String>,
    #[serde(default)]
    pub attachment_name: Option<String>,
    #[serde(default)]
    pub attachment_data: Option<String>,
}

async fn is_member(state: &AppState, conversation_id: u64, user_id: u64) -> Result<bool, ApiError> {
    let row: Option<(u64,)> = sqlx::query_as(
        "SELECT user_id FROM conversation_members WHERE conversation_id = ? AND user_id = ?",
    )
    .bind(conversation_id)
    .bind(user_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;
    Ok(row.is_some())
}

pub(crate) async fn ensure_direct_conversation(
    state: &AppState,
    a: u64,
    b: u64,
) -> Result<u64, ApiError> {
    let existing: Option<(u64,)> = sqlx::query_as(
        "SELECT c.id
         FROM conversations c
         WHERE c.kind = 'direct'
           AND EXISTS (SELECT 1 FROM conversation_members WHERE conversation_id = c.id AND user_id = ?)
           AND EXISTS (SELECT 1 FROM conversation_members WHERE conversation_id = c.id AND user_id = ?)
           AND (SELECT COUNT(*) FROM conversation_members WHERE conversation_id = c.id) = 2
         LIMIT 1",
    )
    .bind(a)
    .bind(b)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;

    if let Some((id,)) = existing {
        return Ok(id);
    }

    let mut tx = state
        .pool
        .begin()
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    let inserted = sqlx::query("INSERT INTO conversations (kind) VALUES ('direct')")
        .execute(&mut *tx)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    let conversation_id = inserted.last_insert_id();
    sqlx::query("INSERT INTO conversation_members (conversation_id, user_id) VALUES (?, ?), (?, ?)")
        .bind(conversation_id)
        .bind(a)
        .bind(conversation_id)
        .bind(b)
        .execute(&mut *tx)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    tx.commit().await.map_err(|_| ApiError::Internal("db error"))?;
    Ok(conversation_id)
}

async fn create_group_conversation(state: &AppState, member_ids: &[u64]) -> Result<u64, ApiError> {
    let mut tx = state
        .pool
        .begin()
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    let inserted = sqlx::query("INSERT INTO conversations (kind) VALUES ('group')")
        .execute(&mut *tx)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    let conversation_id = inserted.last_insert_id();
    for uid in member_ids {
        sqlx::query("INSERT INTO conversation_members (conversation_id, user_id) VALUES (?, ?)")
            .bind(conversation_id)
            .bind(uid)
            .execute(&mut *tx)
            .await
            .map_err(|_| ApiError::Internal("db error"))?;
    }
    tx.commit().await.map_err(|_| ApiError::Internal("db error"))?;
    Ok(conversation_id)
}

async fn conversation_json(state: &AppState, conversation_id: u64) -> Result<Value, ApiError> {
    let kind: String = sqlx::query_scalar("SELECT kind FROM conversations WHERE id = ?")
        .bind(conversation_id)
        .fetch_one(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;

    let members: Vec<(u64, String, String, u64, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT m.user_id, u.username, u.display_name, u.server_id, s.domain, u.avatar_url
         FROM conversation_members m
         JOIN users u ON u.id = m.user_id
         LEFT JOIN servers s ON s.id = u.server_id
         WHERE m.conversation_id = ?
         ORDER BY m.user_id",
    )
    .bind(conversation_id)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;

    Ok(json!({
        "id": conversation_id,
        "kind": kind,
        "members": members
            .iter()
            .map(|(id, username, display_name, _server_id, domain, avatar_url)| {
                let domain = domain.clone().unwrap_or_else(|| state.federation.domain.clone());
                json!({ "id": id, "username": username, "display_name": display_name, "domain": domain, "avatar_url": avatar_url })
            })
            .collect::<Vec<_>>(),
    }))
}

fn parse_handle(handle: &str) -> Option<(String, String)> {
    let rest = handle.strip_prefix('@')?;
    let (username, domain) = rest.split_once('@')?;
    if username.is_empty() || username.len() > 32 || domain.is_empty() {
        return None;
    }
    if !username.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return None;
    }
    if !domain.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '.' || c == ':') {
        return None;
    }
    Some((username.to_string(), domain.to_string()))
}

async fn resolve_handle(state: &AppState, handle: &str) -> Result<u64, ApiError> {
    let (username, domain) = parse_handle(handle)
        .ok_or(ApiError::BadRequest("handle must look like @username@domain"))?;

    if domain == state.federation.domain {
        let user: Option<(u64,)> = sqlx::query_as("SELECT id FROM users WHERE username = ? AND server_id = 0")
            .bind(&username)
            .fetch_optional(&state.pool)
            .await
            .map_err(|_| ApiError::Internal("db error"))?;
        return user.map(|(id,)| id).ok_or(ApiError::NotFound("user not found"));
    }

    federation::resolve_remote_user(state, &username, &domain).await
}

pub async fn create_conversation(
    State(state): State<AppState>,
    auth: AuthUser,
    Json(body): Json<CreateConversationRequest>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let mut ids: Vec<u64> = Vec::new();

    if let Some(id) = body.user_id {
        ids.push(id);
    }
    if let Some(handle) = body.handle {
        ids.push(resolve_handle(&state, &handle).await?);
    }
    for id in body.member_ids {
        ids.push(id);
    }
    for handle in body.handles {
        ids.push(resolve_handle(&state, &handle).await?);
    }

    if ids.is_empty() {
        return Err(ApiError::BadRequest("provide user_id, handle, member_ids, or handles"));
    }

    let mut seen = std::collections::HashSet::new();
    ids.retain(|id| seen.insert(*id));
    let ids: Vec<u64> = ids.into_iter().filter(|id| *id != auth.user.id).collect();

    if ids.is_empty() {
        return Err(ApiError::BadRequest("cannot start a conversation with yourself"));
    }

    for id in &ids {
        let exists: Option<(u64,)> = sqlx::query_as("SELECT id FROM users WHERE id = ?")
            .bind(id)
            .fetch_optional(&state.pool)
            .await
            .map_err(|_| ApiError::Internal("db error"))?;
        if exists.is_none() {
            return Err(ApiError::NotFound("user not found"));
        }
    }

    let conversation_id = if ids.len() == 1 {
        ensure_direct_conversation(&state, auth.user.id, ids[0]).await?
    } else {
        let mut members = vec![auth.user.id];
        members.extend(ids);
        create_group_conversation(&state, &members).await?
    };

    Ok((StatusCode::CREATED, Json(conversation_json(&state, conversation_id).await?)))
}

pub async fn list_conversations(
    State(state): State<AppState>,
    auth: AuthUser,
) -> Result<Json<Value>, ApiError> {
    let ids: Vec<(u64,)> = sqlx::query_as(
        "SELECT conversation_id FROM conversation_members WHERE user_id = ? ORDER BY conversation_id",
    )
    .bind(auth.user.id)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;

    let mut convs = Vec::with_capacity(ids.len());
    for (id,) in ids {
        convs.push(conversation_json(&state, id).await?);
    }
    Ok(Json(json!({ "conversations": convs })))
}

pub async fn list_messages(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(conversation_id): Path<u64>,
) -> Result<Json<Value>, ApiError> {
    if !is_member(&state, conversation_id, auth.user.id).await? {
        return Err(ApiError::NotFound("conversation not found"));
    }

    let messages: Vec<Message> = sqlx::query_as(
        "SELECT id, conversation_id, sender_id, body, created_at, attachment_mime, attachment_name, attachment_data
         FROM messages WHERE conversation_id = ? ORDER BY id ASC",
    )
    .bind(conversation_id)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;

    Ok(Json(json!({ "messages": messages })))
}

pub async fn send_message(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(conversation_id): Path<u64>,
    Json(body): Json<SendMessageRequest>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    if !is_member(&state, conversation_id, auth.user.id).await? {
        return Err(ApiError::NotFound("conversation not found"));
    }
    let has_attachment = body.attachment_data.is_some();
    if body.body.trim().is_empty() && !has_attachment {
        return Err(ApiError::BadRequest("message body cannot be empty"));
    }
    if body.body.len() > 2000 {
        return Err(ApiError::BadRequest("message body too long (max 2000)"));
    }
    if let Some(data) = &body.attachment_data {
        if data.len() > 6_000_000 {
            return Err(ApiError::BadRequest("attachment too large (max ~6MB)"));
        }
        if !data.starts_with("data:") {
            return Err(ApiError::BadRequest("attachment must be a data: URL"));
        }
        if body.attachment_mime.is_none() {
            return Err(ApiError::BadRequest("attachment requires a mime type"));
        }
    }

    let created_at = chrono::Utc::now().naive_utc();
    let inserted = sqlx::query(
        "INSERT INTO messages (conversation_id, sender_id, body, created_at, attachment_mime, attachment_name, attachment_data)
         VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(conversation_id)
    .bind(auth.user.id)
    .bind(&body.body)
    .bind(created_at)
    .bind(&body.attachment_mime)
    .bind(&body.attachment_name)
    .bind(&body.attachment_data)
    .execute(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;
    let inserted_id = inserted.last_insert_id();

    let message: Message = sqlx::query_as(
        "SELECT id, conversation_id, sender_id, body, created_at, attachment_mime, attachment_name, attachment_data FROM messages WHERE id = ?",
    )
    .bind(inserted_id)
    .fetch_one(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;

    state.hub.publish(message.clone());
    federation::deliver_outbound(&state, &message, &auth.user);

    Ok((StatusCode::CREATED, Json(json!({ "message": message }))))
}

/// Remove the current user from a conversation. If no members remain the
/// conversation (and its messages) are deleted entirely.
pub async fn delete_conversation(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(conversation_id): Path<u64>,
) -> Result<StatusCode, ApiError> {
    if !is_member(&state, conversation_id, auth.user.id).await? {
        return Err(ApiError::NotFound("conversation not found"));
    }

    sqlx::query("DELETE FROM conversation_members WHERE conversation_id = ? AND user_id = ?")
        .bind(conversation_id)
        .bind(auth.user.id)
        .execute(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;

    let remaining: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM conversation_members WHERE conversation_id = ?",
    )
    .bind(conversation_id)
    .fetch_one(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;

    if remaining.0 == 0 {
        sqlx::query("DELETE FROM conversations WHERE id = ?")
            .bind(conversation_id)
            .execute(&state.pool)
            .await
            .map_err(|_| ApiError::Internal("db error"))?;
    }

    Ok(StatusCode::NO_CONTENT)
}
