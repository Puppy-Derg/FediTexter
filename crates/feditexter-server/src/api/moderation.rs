use axum::extract::{Path, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::api::error::ApiError;
use crate::auth::AuthUser;
use crate::db::AppState;

async fn is_blocked(state: &AppState, viewer: u64, target: u64) -> Result<bool, ApiError> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM blocks WHERE user_id = ? AND blocked_id = ?",
    )
    .bind(viewer)
    .bind(target)
    .fetch_one(&state.pool)
    .await
    .map_err(|e| {
        eprintln!("[mod] is_blocked failed viewer={viewer} target={target}: {e:?}");
        ApiError::Internal("db error")
    })?;
    Ok(count > 0)
}

async fn is_muted(state: &AppState, viewer: u64, target: u64) -> Result<bool, ApiError> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM mutes WHERE user_id = ? AND muted_id = ?",
    )
    .bind(viewer)
    .bind(target)
    .fetch_one(&state.pool)
    .await
    .map_err(|e| {
        eprintln!("[mutes] {e:?}");
        ApiError::Internal("db error")
    })?;
    Ok(count > 0)
}

async fn ensure_user_exists(state: &AppState, user_id: u64) -> Result<(), ApiError> {
    let row: Option<(u64,)> = sqlx::query_as("SELECT id FROM users WHERE id = ?")
        .bind(user_id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    if row.is_none() {
        return Err(ApiError::NotFound("user not found"));
    }
    Ok(())
}

async fn profile_value(state: &AppState, viewer_id: u64, target_id: u64) -> Result<Value, ApiError> {
    let row: Option<(String, String, Option<String>, Option<String>, bool, String, bool)> = sqlx::query_as(
        "SELECT u.username, u.display_name, s.domain, u.avatar_url, u.is_bot, u.bio, u.profile_visible
         FROM users u
         LEFT JOIN servers s ON s.id = u.server_id
         WHERE u.id = ?",
    )
    .bind(target_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|e| {
        eprintln!("[mod] profile select failed: {e:?}");
        ApiError::Internal("db error")
    })?;

    let (username, display_name, domain, avatar_url, is_bot, bio, profile_visible) = match row {
        Some(r) => r,
        None => return Err(ApiError::NotFound("user not found")),
    };

    let domain = domain.unwrap_or_else(|| state.federation.domain.clone());
    let is_self = viewer_id == target_id;
    let blocked = !is_self && is_blocked(state, viewer_id, target_id).await?;
    let muted = !is_self && is_muted(state, viewer_id, target_id).await?;
    let blocked_by = !is_self && is_blocked(state, target_id, viewer_id).await?;

    // Default visibility: users.profile_visible. A per-user override for this
    // viewer (set by the target) wins over the default when present.
    let mut effective_visible = profile_visible;
    if !is_self {
        let override_row: Option<bool> = sqlx::query_scalar(
            "SELECT visible FROM privacy_overrides WHERE user_id = ? AND target_id = ?",
        )
        .bind(target_id)
        .bind(viewer_id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|e| {
            eprintln!("[mod] override select failed: {e:?}");
            ApiError::Internal("db error")
        })?;
        if let Some(v) = override_row {
            effective_visible = v;
        }
    }

    // Privacy: when the target hides their profile from this viewer, only a
    // bare-bones record is returned (no avatar, no bio). Own profile is full.
    if !is_self && !effective_visible {
        return Ok(json!({
            "id": target_id,
            "username": username,
            "display_name": display_name,
            "domain": domain,
            "avatar_url": null,
            "is_bot": is_bot,
            "is_self": is_self,
            "blocked": blocked,
            "muted": muted,
            "blocked_by": blocked_by,
            "restricted": true,
            "bio": "",
        }));
    }

    Ok(json!({
        "id": target_id,
        "username": username,
        "display_name": display_name,
        "domain": domain,
        "avatar_url": avatar_url,
        "is_bot": is_bot,
        "is_self": is_self,
        "blocked": blocked,
        "muted": muted,
        "blocked_by": blocked_by,
        "restricted": false,
        "bio": bio,
    }))
}

pub async fn user_profile(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(user_id): Path<u64>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!({ "user": profile_value(&state, auth.user.id, user_id).await? })))
}

pub async fn block_user(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(user_id): Path<u64>,
) -> Result<Json<Value>, ApiError> {
    if user_id == auth.user.id {
        return Err(ApiError::BadRequest("cannot block yourself"));
    }
    ensure_user_exists(&state, user_id).await?;
    sqlx::query("INSERT IGNORE INTO blocks (user_id, blocked_id) VALUES (?, ?)")
        .bind(auth.user.id)
        .bind(user_id)
        .execute(&state.pool)
        .await
        .map_err(|e| {
            eprintln!("[mod] insert failed: {e:?}");
            ApiError::Internal("db error")
        })?;
    Ok(Json(json!({ "user": profile_value(&state, auth.user.id, user_id).await? })))
}

pub async fn unblock_user(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(user_id): Path<u64>,
) -> Result<Json<Value>, ApiError> {
    sqlx::query("DELETE FROM blocks WHERE user_id = ? AND blocked_id = ?")
        .bind(auth.user.id)
        .bind(user_id)
        .execute(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    Ok(Json(json!({ "user": profile_value(&state, auth.user.id, user_id).await? })))
}

pub async fn mute_user(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(user_id): Path<u64>,
) -> Result<Json<Value>, ApiError> {
    if user_id == auth.user.id {
        return Err(ApiError::BadRequest("cannot mute yourself"));
    }
    ensure_user_exists(&state, user_id).await?;
    sqlx::query("INSERT IGNORE INTO mutes (user_id, muted_id) VALUES (?, ?)")
        .bind(auth.user.id)
        .bind(user_id)
        .execute(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    Ok(Json(json!({ "user": profile_value(&state, auth.user.id, user_id).await? })))
}

pub async fn unmute_user(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(user_id): Path<u64>,
) -> Result<Json<Value>, ApiError> {
    sqlx::query("DELETE FROM mutes WHERE user_id = ? AND muted_id = ?")
        .bind(auth.user.id)
        .bind(user_id)
        .execute(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    Ok(Json(json!({ "user": profile_value(&state, auth.user.id, user_id).await? })))
}

// ---------------------------------------------------------------------------
// Privacy: default profile visibility + per-user SHOW/HIDE overrides
// ---------------------------------------------------------------------------

/// Result of the caller's privacy settings: their default visibility and every
/// per-user override they have set.
pub async fn list_privacy(
    State(state): State<AppState>,
    auth: AuthUser,
) -> Result<Json<Value>, ApiError> {
    let default: bool = sqlx::query_scalar("SELECT profile_visible FROM users WHERE id = ?")
        .bind(auth.user.id)
        .fetch_one(&state.pool)
        .await
        .map_err(|e| {
            eprintln!("[privacy] default select failed: {e:?}");
            ApiError::Internal("db error")
        })?;

    let overrides: Vec<(u64, String, String, Option<String>, bool)> = sqlx::query_as(
        "SELECT u.id, u.username, u.display_name, s.domain, po.visible
         FROM privacy_overrides po
         JOIN users u ON u.id = po.target_id
         LEFT JOIN servers s ON s.id = u.server_id
         WHERE po.user_id = ?
         ORDER BY u.username",
    )
    .bind(auth.user.id)
    .fetch_all(&state.pool)
    .await
    .map_err(|e| {
        eprintln!("[privacy] overrides select failed: {e:?}");
        ApiError::Internal("db error")
    })?;

    let domain = state.federation.domain.clone();
    let overrides = overrides
        .into_iter()
        .map(|(id, username, display_name, srv_domain, visible)| json!({
            "id": id,
            "username": username,
            "display_name": display_name,
            "domain": srv_domain.unwrap_or_else(|| domain.clone()),
            "visible": visible,
        }))
        .collect::<Vec<_>>();

    Ok(Json(json!({ "default": default, "overrides": overrides })))
}

#[derive(Deserialize)]
pub struct PrivacyOverrideRequest {
    pub visible: bool,
}

/// Set (upsert) a per-user SHOW/HIDE override for what `target_id` sees of the
/// caller's profile.
pub async fn set_privacy_override(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(target_id): Path<u64>,
    Json(body): Json<PrivacyOverrideRequest>,
) -> Result<Json<Value>, ApiError> {
    if target_id == auth.user.id {
        return Err(ApiError::BadRequest("cannot set an override for yourself"));
    }
    let exists: bool = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM users WHERE id = ?")
        .bind(target_id)
        .fetch_one(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?
        > 0;
    if !exists {
        return Err(ApiError::NotFound("user not found"));
    }

    sqlx::query(
        "INSERT INTO privacy_overrides (user_id, target_id, visible) VALUES (?, ?, ?)
         ON DUPLICATE KEY UPDATE visible = VALUES(visible)",
    )
    .bind(auth.user.id)
    .bind(target_id)
    .bind(body.visible)
    .execute(&state.pool)
    .await
    .map_err(|e| {
        eprintln!("[privacy] override upsert failed: {e:?}");
        ApiError::Internal("db error")
    })?;

    Ok(Json(json!({ "status": "ok", "visible": body.visible })))
}

/// Remove a per-user privacy override, falling back to the default.
pub async fn remove_privacy_override(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(target_id): Path<u64>,
) -> Result<Json<Value>, ApiError> {
    sqlx::query("DELETE FROM privacy_overrides WHERE user_id = ? AND target_id = ?")
        .bind(auth.user.id)
        .bind(target_id)
        .execute(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    Ok(Json(json!({ "status": "ok" })))
}
