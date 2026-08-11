//! Discord-like servers ("guilds"). A guild owns many channel conversations;
//! joining a guild grants access to every channel it owns. We keep guilds
//! separate from the federation `servers` table (which tracks remote
//! instances).

use axum::extract::{Path, State};
use axum::Json;
use rand_core::RngCore;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::api::error::ApiError;
use crate::auth::AuthUser;
use crate::db::AppState;

#[derive(Deserialize)]
pub struct CreateGuildRequest {
    pub name: String,
}

#[derive(Deserialize)]
pub struct CreateChannelRequest {
    pub name: String,
}

#[derive(Deserialize)]
pub struct JoinGuildRequest {
    pub code: String,
}

async fn is_guild_member(state: &AppState, guild_id: u64, user_id: u64) -> Result<bool, ApiError> {
    let row: Option<(u64,)> = sqlx::query_as(
        "SELECT user_id FROM guild_members WHERE guild_id = ? AND user_id = ?",
    )
    .bind(guild_id)
    .bind(user_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;
    Ok(row.is_some())
}

/// Register `user_id` as a member of every channel conversation owned by the
/// guild (so the WS hub and message endpoints treat them as a member).
async fn sync_guild_member_channels(state: &AppState, guild_id: u64, user_id: u64) -> Result<(), ApiError> {
    let channel_ids: Vec<(u64,)> = sqlx::query_as(
        "SELECT id FROM conversations WHERE guild_id = ?",
    )
    .bind(guild_id)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;
    for (cid,) in channel_ids {
        sqlx::query("INSERT IGNORE INTO conversation_members (conversation_id, user_id) VALUES (?, ?)")
            .bind(cid)
            .bind(user_id)
            .execute(&state.pool)
            .await
            .map_err(|_| ApiError::Internal("db error"))?;
    }
    Ok(())
}

/// Remove `user_id` from every channel conversation owned by the guild.
async fn unsync_guild_member_channels(state: &AppState, guild_id: u64, user_id: u64) -> Result<(), ApiError> {
    let channel_ids: Vec<(u64,)> = sqlx::query_as(
        "SELECT id FROM conversations WHERE guild_id = ?",
    )
    .bind(guild_id)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;
    for (cid,) in channel_ids {
        sqlx::query("DELETE FROM conversation_members WHERE conversation_id = ? AND user_id = ?")
            .bind(cid)
            .bind(user_id)
            .execute(&state.pool)
            .await
            .map_err(|_| ApiError::Internal("db error"))?;
    }
    Ok(())
}

fn channel_json(_state: &AppState, conv_id: u64, name: &str) -> Value {
    json!({
        "id": conv_id,
        "name": name,
    })
}

/// List the guilds the caller is a member of, each with its channels.
pub async fn list_guilds(
    State(state): State<AppState>,
    auth: AuthUser,
) -> Result<Json<Value>, ApiError> {
    let guilds: Vec<(u64, String, u64)> = sqlx::query_as(
        "SELECT g.id, g.name, g.owner_id
         FROM guilds g
         JOIN guild_members gm ON gm.guild_id = g.id
         WHERE gm.user_id = ?
         ORDER BY g.name",
    )
    .bind(auth.user.id)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;

    let mut out = Vec::new();
    for (gid, gname, owner_id) in guilds {
        let channels: Vec<(u64, String)> = sqlx::query_as(
            "SELECT id, name FROM conversations WHERE guild_id = ? AND name IS NOT NULL ORDER BY name",
        )
        .bind(gid)
        .fetch_all(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
        let member_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM guild_members WHERE guild_id = ?",
        )
        .bind(gid)
        .fetch_one(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
        out.push(json!({
            "id": gid,
            "name": gname,
            "owner_id": owner_id,
            "member_count": member_count,
            "channels": channels.iter().map(|(cid, name)| channel_json(&state, *cid, name)).collect::<Vec<_>>(),
        }));
    }
    Ok(Json(json!({ "guilds": out })))
}

/// Full detail for one guild the caller belongs to.
pub async fn guild_detail(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(guild_id): Path<u64>,
) -> Result<Json<Value>, ApiError> {
    if !is_guild_member(&state, guild_id, auth.user.id).await? {
        return Err(ApiError::NotFound("guild not found"));
    }
    let row: Option<(String, u64)> = sqlx::query_as(
        "SELECT name, owner_id FROM guilds WHERE id = ?",
    )
    .bind(guild_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;
    let Some((name, owner_id)) = row else {
        return Err(ApiError::NotFound("guild not found"));
    };
    let channels: Vec<(u64, String)> = sqlx::query_as(
        "SELECT id, name FROM conversations WHERE guild_id = ? AND name IS NOT NULL ORDER BY name",
    )
    .bind(guild_id)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;
    let members: Vec<(u64, String, String)> = sqlx::query_as(
        "SELECT u.id, u.username, u.display_name
         FROM guild_members gm JOIN users u ON u.id = gm.user_id
         WHERE gm.guild_id = ? ORDER BY u.username",
    )
    .bind(guild_id)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;
    Ok(Json(json!({
        "id": guild_id,
        "name": name,
        "owner_id": owner_id,
        "channels": channels.iter().map(|(cid, name)| channel_json(&state, *cid, name)).collect::<Vec<_>>(),
        "members": members.iter().map(|(id, username, display_name)| json!({ "id": id, "username": username, "display_name": display_name })).collect::<Vec<_>>(),
    })))
}

/// Create a guild: becomes a member and gets a default `#general` channel.
pub async fn create_guild(
    State(state): State<AppState>,
    auth: AuthUser,
    Json(body): Json<CreateGuildRequest>,
) -> Result<(axum::http::StatusCode, Json<Value>), ApiError> {
    let name = body.name.trim().to_string();
    if name.is_empty() || name.len() > 100 {
        return Err(ApiError::BadRequest("guild name must be 1-100 characters"));
    }
    let mut tx = state
        .pool
        .begin()
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    let inserted = sqlx::query("INSERT INTO guilds (name, owner_id) VALUES (?, ?)")
        .bind(&name)
        .bind(auth.user.id)
        .execute(&mut *tx)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    let guild_id = inserted.last_insert_id();
    sqlx::query("INSERT INTO guild_members (guild_id, user_id) VALUES (?, ?)")
        .bind(guild_id)
        .bind(auth.user.id)
        .execute(&mut *tx)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    let conv = sqlx::query(
        "INSERT INTO conversations (kind, guild_id, name) VALUES ('group', ?, 'general')",
    )
    .bind(guild_id)
    .execute(&mut *tx)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;
    let conv_id = conv.last_insert_id();
    sqlx::query("INSERT INTO conversation_members (conversation_id, user_id) VALUES (?, ?)")
        .bind(conv_id)
        .bind(auth.user.id)
        .execute(&mut *tx)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    tx.commit().await.map_err(|_| ApiError::Internal("db error"))?;
    Ok((axum::http::StatusCode::CREATED, Json(json!({ "guild_id": guild_id }))))
}

/// Create a channel conversation inside a guild (owner only).
pub async fn create_channel(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(guild_id): Path<u64>,
    Json(body): Json<CreateChannelRequest>,
) -> Result<Json<Value>, ApiError> {
    let name = body.name.trim().to_string();
    if name.is_empty() || name.len() > 100 {
        return Err(ApiError::BadRequest("channel name must be 1-100 characters"));
    }
    let owner: Option<(u64,)> = sqlx::query_as(
        "SELECT owner_id FROM guilds WHERE id = ?",
    )
    .bind(guild_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;
    let Some((owner_id,)) = owner else {
        return Err(ApiError::NotFound("guild not found"));
    };
    if owner_id != auth.user.id {
        return Err(ApiError::Forbidden("only the guild owner can create channels"));
    }

    let mut tx = state.pool.begin().await.map_err(|_| ApiError::Internal("db error"))?;
    let inserted = sqlx::query("INSERT INTO conversations (kind, guild_id, name) VALUES ('group', ?, ?)")
        .bind(guild_id)
        .bind(&name)
        .execute(&mut *tx)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    let conv_id = inserted.last_insert_id();
    // Every current guild member can see the new channel.
    let members: Vec<(u64,)> = sqlx::query_as("SELECT user_id FROM guild_members WHERE guild_id = ?")
        .bind(guild_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    for (uid,) in members {
        sqlx::query("INSERT IGNORE INTO conversation_members (conversation_id, user_id) VALUES (?, ?)")
            .bind(conv_id)
            .bind(uid)
            .execute(&mut *tx)
            .await
            .map_err(|_| ApiError::Internal("db error"))?;
    }
    tx.commit().await.map_err(|_| ApiError::Internal("db error"))?;

    Ok(Json(json!({ "id": conv_id, "name": name })))
}

/// Generate an invite code for the guild (owner only).
pub async fn create_invite(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(guild_id): Path<u64>,
) -> Result<Json<Value>, ApiError> {
    let owner: Option<(u64,)> = sqlx::query_as(
        "SELECT owner_id FROM guilds WHERE id = ?",
    )
    .bind(guild_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;
    let Some((owner_id,)) = owner else {
        return Err(ApiError::NotFound("guild not found"));
    };
    if owner_id != auth.user.id {
        return Err(ApiError::Forbidden("only the guild owner can create invites"));
    }
    let mut bytes = [0u8; 16];
    rand_core::OsRng.fill_bytes(&mut bytes);
    let code = bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
    sqlx::query("INSERT INTO guild_invites (code, guild_id, created_by) VALUES (?, ?, ?)")
        .bind(&code)
        .bind(guild_id)
        .bind(auth.user.id)
        .execute(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    Ok(Json(json!({ "code": code })))
}

/// Join a guild by invite code, adding the caller as a member of every channel.
pub async fn join_guild(
    State(state): State<AppState>,
    auth: AuthUser,
    Json(body): Json<JoinGuildRequest>,
) -> Result<Json<Value>, ApiError> {
    let guild: Option<(u64,)> = sqlx::query_as(
        "SELECT guild_id FROM guild_invites WHERE code = ?",
    )
    .bind(&body.code)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;
    let Some((guild_id,)) = guild else {
        return Err(ApiError::NotFound("invite not found or expired"));
    };
    sqlx::query("INSERT IGNORE INTO guild_members (guild_id, user_id) VALUES (?, ?)")
        .bind(guild_id)
        .bind(auth.user.id)
        .execute(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    sync_guild_member_channels(&state, guild_id, auth.user.id).await?;
    Ok(Json(json!({ "guild_id": guild_id })))
}

/// Leave a guild (owner cannot leave without transferring/deleting).
pub async fn leave_guild(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(guild_id): Path<u64>,
) -> Result<Json<Value>, ApiError> {
    let owner: Option<(u64,)> = sqlx::query_as(
        "SELECT owner_id FROM guilds WHERE id = ?",
    )
    .bind(guild_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;
    if let Some((owner_id,)) = owner {
        if owner_id == auth.user.id {
            return Err(ApiError::BadRequest("the owner cannot leave; delete the guild instead"));
        }
    }
    sqlx::query("DELETE FROM guild_members WHERE guild_id = ? AND user_id = ?")
        .bind(guild_id)
        .bind(auth.user.id)
        .execute(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    unsync_guild_member_channels(&state, guild_id, auth.user.id).await?;
    Ok(Json(json!({ "status": "ok" })))
}
