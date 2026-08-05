use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::api::error::ApiError;
use crate::auth::AuthUser;
use crate::chat::Message;
use crate::db::AppState;

#[derive(Deserialize)]
pub struct CreateConversationRequest {
    pub user_id: u64,
}

#[derive(Deserialize)]
pub struct SendMessageRequest {
    pub body: String,
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

async fn conversation_json(state: &AppState, conversation_id: u64) -> Result<Value, ApiError> {
    let kind: String = sqlx::query_scalar("SELECT kind FROM conversations WHERE id = ?")
        .bind(conversation_id)
        .fetch_one(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;

    let members: Vec<(u64,)> = sqlx::query_as(
        "SELECT user_id FROM conversation_members WHERE conversation_id = ? ORDER BY user_id",
    )
    .bind(conversation_id)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;

    Ok(json!({
        "id": conversation_id,
        "kind": kind,
        "members": members.iter().map(|(id,)| json!({ "id": id })).collect::<Vec<_>>(),
    }))
}

pub async fn create_conversation(
    State(state): State<AppState>,
    auth: AuthUser,
    Json(body): Json<CreateConversationRequest>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    if body.user_id == auth.user.id {
        return Err(ApiError::BadRequest("cannot start a conversation with yourself"));
    }

    let exists: Option<(u64,)> = sqlx::query_as("SELECT id FROM users WHERE id = ?")
        .bind(body.user_id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    if exists.is_none() {
        return Err(ApiError::NotFound("user not found"));
    }

    let existing: Option<(u64,)> = sqlx::query_as(
        "SELECT c.id
         FROM conversations c
         WHERE c.kind = 'direct'
           AND EXISTS (SELECT 1 FROM conversation_members WHERE conversation_id = c.id AND user_id = ?)
           AND EXISTS (SELECT 1 FROM conversation_members WHERE conversation_id = c.id AND user_id = ?)
           AND (SELECT COUNT(*) FROM conversation_members WHERE conversation_id = c.id) = 2
         LIMIT 1",
    )
    .bind(auth.user.id)
    .bind(body.user_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;

    let conversation_id = match existing {
        Some((id,)) => id,
        None => {
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
                .bind(auth.user.id)
                .bind(conversation_id)
                .bind(body.user_id)
                .execute(&mut *tx)
                .await
                .map_err(|_| ApiError::Internal("db error"))?;
            tx.commit().await.map_err(|_| ApiError::Internal("db error"))?;
            conversation_id
        }
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
        "SELECT id, conversation_id, sender_id, body, created_at
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
    if body.body.trim().is_empty() {
        return Err(ApiError::BadRequest("message body cannot be empty"));
    }
    if body.body.len() > 2000 {
        return Err(ApiError::BadRequest("message body too long (max 2000)"));
    }

    let inserted = sqlx::query(
        "INSERT INTO messages (conversation_id, sender_id, body) VALUES (?, ?, ?)",
    )
    .bind(conversation_id)
    .bind(auth.user.id)
    .bind(&body.body)
    .execute(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;
    let inserted_id = inserted.last_insert_id();

    let message: Message = sqlx::query_as(
        "SELECT id, conversation_id, sender_id, body, created_at FROM messages WHERE id = ?",
    )
    .bind(inserted_id)
    .fetch_one(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;

    state.hub.publish(message.clone());

    Ok((StatusCode::CREATED, Json(json!({ "message": message }))))
}
