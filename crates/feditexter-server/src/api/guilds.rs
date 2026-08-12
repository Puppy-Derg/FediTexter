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

#[derive(Deserialize)]
pub struct RevokeInviteParams {
    pub code: String,
}

#[derive(Deserialize)]
pub struct SetRoleRequest {
    pub user_id: u64,
    pub is_admin: bool,
}

#[derive(Deserialize)]
pub struct KickRequest {
    pub user_id: u64,
}

#[derive(Deserialize)]
pub struct TransferOwnerRequest {
    pub user_id: u64,
}

#[derive(Deserialize)]
pub struct RenameChannelRequest {
    pub name: String,
}

#[derive(Deserialize)]
pub struct CreateRoleRequest {
    pub name: String,
}

#[derive(Deserialize)]
pub struct AssignRoleRequest {
    pub user_id: u64,
    pub on: bool,
}

#[derive(Deserialize)]
pub struct BanRequest {
    pub user_id: u64,
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

/// Owner, or a member granted the guild's admin role.
async fn is_admin(state: &AppState, guild_id: u64, user_id: u64) -> Result<bool, ApiError> {
    let owner: Option<(u64,)> = sqlx::query_as("SELECT owner_id FROM guilds WHERE id = ?")
        .bind(guild_id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    if let Some((oid,)) = owner {
        if oid == user_id {
            return Ok(true);
        }
    }
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM guild_member_roles mr
         JOIN guild_roles r ON r.id = mr.role_id
         WHERE mr.guild_id = ? AND mr.user_id = ? AND r.is_admin = 1",
    )
    .bind(guild_id)
    .bind(user_id)
    .fetch_one(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;
    Ok(count > 0)
}

/// Ensure a guild has exactly one admin role, returning its id.
async fn ensure_admin_role(tx: &mut sqlx::Transaction<'_, sqlx::MySql>, guild_id: u64) -> Result<u64, ApiError> {
    let existing: Option<(u64,)> = sqlx::query_as(
        "SELECT id FROM guild_roles WHERE guild_id = ? AND is_admin = 1 LIMIT 1",
    )
    .bind(guild_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;
    if let Some((id,)) = existing {
        return Ok(id);
    }
    let inserted = sqlx::query("INSERT INTO guild_roles (guild_id, name, is_admin) VALUES (?, 'admin', 1)")
        .bind(guild_id)
        .execute(&mut **tx)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    Ok(inserted.last_insert_id())
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
        let members: Vec<(u64, String, String, u64)> = sqlx::query_as(
            "SELECT u.id, u.username, u.display_name, u.server_id
             FROM guild_members gm JOIN users u ON u.id = gm.user_id
             WHERE gm.guild_id = ? ORDER BY u.username",
        )
        .bind(gid)
        .fetch_all(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
        out.push(json!({
            "id": gid,
            "name": gname,
            "owner_id": owner_id,
            "member_count": member_count,
            "channels": channels.iter().map(|(cid, name)| channel_json(&state, *cid, name)).collect::<Vec<_>>(),
            "members": members.iter().map(|(id, username, display_name, _server_id)| json!({ "id": id, "username": username, "display_name": display_name })).collect::<Vec<_>>(),
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
    let is_admin_viewer = owner_id == auth.user.id || is_admin(&state, guild_id, auth.user.id).await?;

    // Roles (with assigned member ids) and bans — only meaningful to moderators.
    let roles: Vec<Value> = if is_admin_viewer {
        let role_rows: Vec<(u64, String, bool)> = sqlx::query_as(
            "SELECT id, name, is_admin FROM guild_roles WHERE guild_id = ? ORDER BY id",
        )
        .bind(guild_id)
        .fetch_all(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
        let mut out = Vec::with_capacity(role_rows.len());
        for (rid, rname, is_admin_flag) in role_rows {
            let member_ids: Vec<(u64,)> = sqlx::query_as(
                "SELECT user_id FROM guild_member_roles WHERE guild_id = ? AND role_id = ?",
            )
            .bind(guild_id)
            .bind(rid)
            .fetch_all(&state.pool)
            .await
            .map_err(|_| ApiError::Internal("db error"))?;
            out.push(json!({
                "id": rid,
                "name": rname,
                "is_admin": is_admin_flag,
                "member_ids": member_ids.iter().map(|(uid,)| *uid).collect::<Vec<_>>(),
            }));
        }
        out
    } else {
        Vec::new()
    };

    let bans: Vec<Value> = if is_admin_viewer {
        let banned: Vec<(u64, String, String)> = sqlx::query_as(
            "SELECT u.id, u.username, u.display_name
             FROM guild_bans gb JOIN users u ON u.id = gb.user_id
             WHERE gb.guild_id = ? ORDER BY u.username",
        )
        .bind(guild_id)
        .fetch_all(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
        banned.iter().map(|(id, username, display_name)| json!({ "id": id, "username": username, "display_name": display_name })).collect()
    } else {
        Vec::new()
    };

    Ok(Json(json!({
        "id": guild_id,
        "name": name,
        "owner_id": owner_id,
        "can_manage": is_admin_viewer,
        "channels": channels.iter().map(|(cid, name)| channel_json(&state, *cid, name)).collect::<Vec<_>>(),
        "members": members.iter().map(|(id, username, display_name)| json!({ "id": id, "username": username, "display_name": display_name })).collect::<Vec<_>>(),
        "roles": roles,
        "bans": bans,
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
    // The creator is the first admin: grant them the guild's admin role so
    // they can set up the server (channels, roles, bans) before anyone joins.
    let admin_role_id = ensure_admin_role(&mut tx, guild_id).await?;
    sqlx::query("INSERT IGNORE INTO guild_member_roles (guild_id, user_id, role_id) VALUES (?, ?, ?)")
        .bind(guild_id)
        .bind(auth.user.id)
        .bind(admin_role_id)
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
    let exists: Option<(u64,)> = sqlx::query_as("SELECT id FROM guilds WHERE id = ?")
        .bind(guild_id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    if exists.is_none() {
        return Err(ApiError::NotFound("guild not found"));
    }
    if !is_admin(&state, guild_id, auth.user.id).await? {
        return Err(ApiError::Forbidden("only the owner or an admin can create channels"));
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
    let expires_at = chrono::Utc::now().naive_utc() + chrono::Duration::days(7);
    sqlx::query("INSERT INTO guild_invites (code, guild_id, created_by, expires_at) VALUES (?, ?, ?, ?)")
        .bind(&code)
        .bind(guild_id)
        .bind(auth.user.id)
        .bind(expires_at)
        .execute(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    Ok(Json(json!({ "code": code })))
}

/// Revoke an invite code (owner only).
pub async fn revoke_invite(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(guild_id): Path<u64>,
    axum::extract::Query(params): axum::extract::Query<RevokeInviteParams>,
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
        return Err(ApiError::Forbidden("only the guild owner can revoke invites"));
    }
    sqlx::query("DELETE FROM guild_invites WHERE code = ? AND guild_id = ?")
        .bind(&params.code)
        .bind(guild_id)
        .execute(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    Ok(Json(json!({ "status": "ok" })))
}

/// Delete a guild (owner only). Channels, members and invites cascade.
pub async fn delete_guild(
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
        return Err(ApiError::Forbidden("only the guild owner can delete the guild"));
    }
    sqlx::query("DELETE FROM guilds WHERE id = ?")
        .bind(guild_id)
        .execute(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    Ok(Json(json!({ "status": "ok" })))
}

/// Join a guild by invite code, adding the caller as a member of every channel.
pub async fn join_guild(
    State(state): State<AppState>,
    auth: AuthUser,
    Json(body): Json<JoinGuildRequest>,
) -> Result<Json<Value>, ApiError> {
    let guild: Option<(u64,)> = sqlx::query_as(
        "SELECT guild_id FROM guild_invites
         WHERE code = ? AND (expires_at IS NULL OR expires_at > ?)",
    )
    .bind(&body.code)
    .bind(chrono::Utc::now().naive_utc())
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;
    let Some((guild_id,)) = guild else {
        return Err(ApiError::NotFound("invite not found or expired"));
    };
    let banned: Option<(u64,)> = sqlx::query_as(
        "SELECT user_id FROM guild_bans WHERE guild_id = ? AND user_id = ?",
    )
    .bind(guild_id)
    .bind(auth.user.id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;
    if banned.is_some() {
        return Err(ApiError::Forbidden("you are banned from this server"));
    }
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

/// Grant or revoke the admin role for a member (owner only).
pub async fn set_role(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(guild_id): Path<u64>,
    Json(body): Json<SetRoleRequest>,
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
        return Err(ApiError::Forbidden("only the owner can change roles"));
    }
    if body.user_id == owner_id {
        return Err(ApiError::BadRequest("cannot change the owner's role"));
    }
    if !is_guild_member(&state, guild_id, body.user_id).await? {
        return Err(ApiError::NotFound("member not found"));
    }

    let mut tx = state.pool.begin().await.map_err(|_| ApiError::Internal("db error"))?;
    if body.is_admin {
        let role_id = ensure_admin_role(&mut tx, guild_id).await?;
        sqlx::query("INSERT IGNORE INTO guild_member_roles (guild_id, user_id, role_id) VALUES (?, ?, ?)")
            .bind(guild_id)
            .bind(body.user_id)
            .bind(role_id)
            .execute(&mut *tx)
            .await
            .map_err(|_| ApiError::Internal("db error"))?;
    } else {
        sqlx::query(
            "DELETE mr FROM guild_member_roles mr
             JOIN guild_roles r ON r.id = mr.role_id
             WHERE mr.guild_id = ? AND mr.user_id = ? AND r.is_admin = 1",
        )
        .bind(guild_id)
        .bind(body.user_id)
        .execute(&mut *tx)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    }
    tx.commit().await.map_err(|_| ApiError::Internal("db error"))?;
    Ok(Json(json!({ "status": "ok" })))
}

/// Transfer guild ownership to another member (owner only). The new owner
/// becomes admin; the previous owner keeps the admin role they had at creation.
pub async fn transfer_owner(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(guild_id): Path<u64>,
    Json(body): Json<TransferOwnerRequest>,
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
        return Err(ApiError::Forbidden("only the owner can transfer ownership"));
    }
    if body.user_id == owner_id {
        return Err(ApiError::BadRequest("that user already owns the guild"));
    }
    if !is_guild_member(&state, guild_id, body.user_id).await? {
        return Err(ApiError::NotFound("member not found"));
    }

    let mut tx = state.pool.begin().await.map_err(|_| ApiError::Internal("db error"))?;
    sqlx::query("UPDATE guilds SET owner_id = ? WHERE id = ?")
        .bind(body.user_id)
        .bind(guild_id)
        .execute(&mut *tx)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    let role_id = ensure_admin_role(&mut tx, guild_id).await?;
    sqlx::query("INSERT IGNORE INTO guild_member_roles (guild_id, user_id, role_id) VALUES (?, ?, ?)")
        .bind(guild_id)
        .bind(body.user_id)
        .bind(role_id)
        .execute(&mut *tx)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    tx.commit().await.map_err(|_| ApiError::Internal("db error"))?;
    Ok(Json(json!({ "status": "ok", "owner_id": body.user_id })))
}

/// Kick a member from the guild (owner or admin).
pub async fn kick_member(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(guild_id): Path<u64>,
    Json(body): Json<KickRequest>,
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
    if body.user_id == owner_id {
        return Err(ApiError::BadRequest("cannot kick the owner"));
    }
    if auth.user.id != owner_id && !is_admin(&state, guild_id, auth.user.id).await? {
        return Err(ApiError::Forbidden("only the owner or an admin can kick members"));
    }
    if !is_guild_member(&state, guild_id, body.user_id).await? {
        return Err(ApiError::NotFound("member not found"));
    }
    sqlx::query("DELETE FROM guild_members WHERE guild_id = ? AND user_id = ?")
        .bind(guild_id)
        .bind(body.user_id)
        .execute(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    unsync_guild_member_channels(&state, guild_id, body.user_id).await?;
    Ok(Json(json!({ "status": "ok" })))
}

/// Rename a guild channel (owner or admin).
pub async fn rename_channel(
    State(state): State<AppState>,
    auth: AuthUser,
    Path((guild_id, channel_id)): Path<(u64, u64)>,
    Json(body): Json<RenameChannelRequest>,
) -> Result<Json<Value>, ApiError> {
    let name = body.name.trim().to_string();
    if name.is_empty() || name.len() > 100 {
        return Err(ApiError::BadRequest("channel name must be 1-100 characters"));
    }
    if !is_admin(&state, guild_id, auth.user.id).await? {
        return Err(ApiError::Forbidden("only the owner or an admin can manage channels"));
    }
    let row: Option<(u64,)> = sqlx::query_as(
        "SELECT id FROM conversations WHERE id = ? AND guild_id = ?",
    )
    .bind(channel_id)
    .bind(guild_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;
    if row.is_none() {
        return Err(ApiError::NotFound("channel not found"));
    }
    sqlx::query("UPDATE conversations SET name = ? WHERE id = ?")
        .bind(&name)
        .bind(channel_id)
        .execute(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    Ok(Json(json!({ "id": channel_id, "name": name })))
}

/// Delete a guild channel and its messages (owner or admin).
pub async fn delete_channel(
    State(state): State<AppState>,
    auth: AuthUser,
    Path((guild_id, channel_id)): Path<(u64, u64)>,
) -> Result<Json<Value>, ApiError> {
    if !is_admin(&state, guild_id, auth.user.id).await? {
        return Err(ApiError::Forbidden("only the owner or an admin can manage channels"));
    }
    sqlx::query("DELETE FROM conversations WHERE id = ? AND guild_id = ?")
        .bind(channel_id)
        .bind(guild_id)
        .execute(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    Ok(Json(json!({ "status": "ok" })))
}

/// Create a named role (owner only).
pub async fn create_role(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(guild_id): Path<u64>,
    Json(body): Json<CreateRoleRequest>,
) -> Result<Json<Value>, ApiError> {
    let name = body.name.trim().to_string();
    if name.is_empty() || name.len() > 50 {
        return Err(ApiError::BadRequest("role name must be 1-50 characters"));
    }
    let owner: Option<(u64,)> = sqlx::query_as("SELECT owner_id FROM guilds WHERE id = ?")
        .bind(guild_id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    let Some((owner_id,)) = owner else {
        return Err(ApiError::NotFound("guild not found"));
    };
    if owner_id != auth.user.id {
        return Err(ApiError::Forbidden("only the owner can manage roles"));
    }
    let inserted = sqlx::query("INSERT INTO guild_roles (guild_id, name) VALUES (?, ?)")
        .bind(guild_id)
        .bind(&name)
        .execute(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    Ok(Json(json!({ "id": inserted.last_insert_id(), "name": name })))
}

/// Delete a named role (owner only). Admin role is protected.
pub async fn delete_role(
    State(state): State<AppState>,
    auth: AuthUser,
    Path((guild_id, role_id)): Path<(u64, u64)>,
) -> Result<Json<Value>, ApiError> {
    let owner: Option<(u64,)> = sqlx::query_as("SELECT owner_id FROM guilds WHERE id = ?")
        .bind(guild_id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    let Some((owner_id,)) = owner else {
        return Err(ApiError::NotFound("guild not found"));
    };
    if owner_id != auth.user.id {
        return Err(ApiError::Forbidden("only the owner can manage roles"));
    }
    let role: Option<(bool,)> = sqlx::query_as("SELECT is_admin FROM guild_roles WHERE id = ? AND guild_id = ?")
        .bind(role_id)
        .bind(guild_id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    let Some((is_admin_role,)) = role else {
        return Err(ApiError::NotFound("role not found"));
    };
    if is_admin_role {
        return Err(ApiError::BadRequest("the admin role cannot be deleted"));
    }
    sqlx::query("DELETE FROM guild_roles WHERE id = ?")
        .bind(role_id)
        .execute(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    Ok(Json(json!({ "status": "ok" })))
}

/// Assign (`on=true`) or revoke (`on=false`) a named role for a member (owner only).
pub async fn assign_role(
    State(state): State<AppState>,
    auth: AuthUser,
    Path((guild_id, role_id)): Path<(u64, u64)>,
    Json(body): Json<AssignRoleRequest>,
) -> Result<Json<Value>, ApiError> {
    let owner: Option<(u64,)> = sqlx::query_as("SELECT owner_id FROM guilds WHERE id = ?")
        .bind(guild_id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    let Some((owner_id,)) = owner else {
        return Err(ApiError::NotFound("guild not found"));
    };
    if owner_id != auth.user.id {
        return Err(ApiError::Forbidden("only the owner can manage roles"));
    }
    if body.user_id == owner_id {
        return Err(ApiError::BadRequest("cannot change the owner's roles"));
    }
    let role: Option<(bool,)> = sqlx::query_as("SELECT is_admin FROM guild_roles WHERE id = ? AND guild_id = ?")
        .bind(role_id)
        .bind(guild_id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    let Some((is_admin_role,)) = role else {
        return Err(ApiError::NotFound("role not found"));
    };
    if is_admin_role {
        return Err(ApiError::BadRequest("use the admin toggle for the admin role"));
    }
    if !is_guild_member(&state, guild_id, body.user_id).await? {
        return Err(ApiError::NotFound("member not found"));
    }
    if body.on {
        sqlx::query("INSERT IGNORE INTO guild_member_roles (guild_id, user_id, role_id) VALUES (?, ?, ?)")
            .bind(guild_id)
            .bind(body.user_id)
            .bind(role_id)
            .execute(&state.pool)
            .await
            .map_err(|_| ApiError::Internal("db error"))?;
    } else {
        sqlx::query("DELETE FROM guild_member_roles WHERE guild_id = ? AND user_id = ? AND role_id = ?")
            .bind(guild_id)
            .bind(body.user_id)
            .bind(role_id)
            .execute(&state.pool)
            .await
            .map_err(|_| ApiError::Internal("db error"))?;
    }
    Ok(Json(json!({ "status": "ok" })))
}

/// Ban a member from the guild (owner or admin): removes membership and blocks
/// rejoin via invite until unbanned.
pub async fn ban_member(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(guild_id): Path<u64>,
    Json(body): Json<BanRequest>,
) -> Result<Json<Value>, ApiError> {
    let owner: Option<(u64,)> = sqlx::query_as("SELECT owner_id FROM guilds WHERE id = ?")
        .bind(guild_id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    let Some((owner_id,)) = owner else {
        return Err(ApiError::NotFound("guild not found"));
    };
    if body.user_id == owner_id {
        return Err(ApiError::BadRequest("cannot ban the owner"));
    }
    if auth.user.id != owner_id && !is_admin(&state, guild_id, auth.user.id).await? {
        return Err(ApiError::Forbidden("only the owner or an admin can ban members"));
    }
    sqlx::query("INSERT IGNORE INTO guild_bans (guild_id, user_id, banned_by) VALUES (?, ?, ?)")
        .bind(guild_id)
        .bind(body.user_id)
        .bind(auth.user.id)
        .execute(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    sqlx::query("DELETE FROM guild_members WHERE guild_id = ? AND user_id = ?")
        .bind(guild_id)
        .bind(body.user_id)
        .execute(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    unsync_guild_member_channels(&state, guild_id, body.user_id).await?;
    Ok(Json(json!({ "status": "ok" })))
}

/// Unban a user from the guild (owner or admin).
pub async fn unban_member(
    State(state): State<AppState>,
    auth: AuthUser,
    Path((guild_id, user_id)): Path<(u64, u64)>,
) -> Result<Json<Value>, ApiError> {
    let owner: Option<(u64,)> = sqlx::query_as("SELECT owner_id FROM guilds WHERE id = ?")
        .bind(guild_id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    let Some((owner_id,)) = owner else {
        return Err(ApiError::NotFound("guild not found"));
    };
    if auth.user.id != owner_id && !is_admin(&state, guild_id, auth.user.id).await? {
        return Err(ApiError::Forbidden("only the owner or an admin can unban members"));
    }
    sqlx::query("DELETE FROM guild_bans WHERE guild_id = ? AND user_id = ?")
        .bind(guild_id)
        .bind(user_id)
        .execute(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    Ok(Json(json!({ "status": "ok" })))
}
