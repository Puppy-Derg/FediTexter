//! User-generated sticker packs. Stickers are stored server-side as compressed
//! images (JPEG/WebP, 1024x1024 or smaller) and served to any authenticated
//! user, searchable by pack name and sticker name.

use axum::extract::{Path, Query, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::HeaderMap;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::api::error::ApiError;
use crate::auth::AuthUser;
use crate::db::AppState;

pub const MAX_STICKER_BYTES: usize = 512 * 1024; // 512 KiB (compressed image)
pub const MAX_PACK_NAME: usize = 100;
pub const MAX_STICKER_NAME: usize = 100;

#[derive(Deserialize)]
pub struct CreatePackRequest {
    pub name: String,
}

#[derive(Deserialize)]
pub struct AddStickerRequest {
    pub name: String,
    /// base64-encoded image bytes (JPEG or WebP, ≤1024x1024).
    pub data: String,
    pub mime: String,
}

#[derive(Deserialize)]
pub struct ListParams {
    #[serde(default)]
    pub q: Option<String>,
    /// When true, only the caller's own packs are returned (creation UI).
    #[serde(default)]
    pub mine: Option<bool>,
}

fn valid_sticker_mime(mime: &str) -> bool {
    matches!(mime, "image/jpeg" | "image/jpg" | "image/webp" | "image/png")
}

fn base64_encode(data: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(data)
}

fn base64_decode(s: &str) -> Option<Vec<u8>> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.decode(s).ok()
}

async fn pack_json(state: &AppState, pack_id: u64, name: &str, owner_id: u64) -> Result<Value, ApiError> {
    let owner_name: Option<String> = sqlx::query_scalar("SELECT username FROM users WHERE id = ?")
        .bind(owner_id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    let stickers: Vec<(u64, String, String)> = sqlx::query_as(
        "SELECT id, name, mime FROM stickers WHERE pack_id = ? ORDER BY id",
    )
    .bind(pack_id)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;
    Ok(json!({
        "id": pack_id,
        "name": name,
        "owner_id": owner_id,
        "owner_name": owner_name.unwrap_or_default(),
        "stickers": stickers.iter().map(|(id, sname, mime)| json!({ "id": id, "name": sname, "mime": mime })).collect::<Vec<_>>(),
    }))
}

/// List sticker packs. `?q=` filters by pack name OR sticker name (case-insensitive);
/// `?mine=1` limits to the caller's own packs.
pub async fn list_sticker_packs(
    State(state): State<AppState>,
    auth: AuthUser,
    Query(params): Query<ListParams>,
) -> Result<Json<Value>, ApiError> {
    let (packs, pack_names, owner_ids): (Vec<u64>, Vec<String>, Vec<u64>) = if params.mine.unwrap_or(false) {
        let rows: Vec<(u64, String, u64)> = sqlx::query_as(
            "SELECT id, name, owner_id FROM sticker_packs WHERE owner_id = ? ORDER BY name",
        )
        .bind(auth.user.id)
        .fetch_all(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
        (rows.iter().map(|r| r.0).collect(), rows.iter().map(|r| r.1.clone()).collect(), rows.iter().map(|r| r.2).collect())
    } else if let Some(q) = params.q.as_deref().map(|s| s.trim()).filter(|s| !s.is_empty()) {
        let like = format!("%{q}%");
        let pack_matches: Vec<(u64, String, u64)> = sqlx::query_as(
            "SELECT id, name, owner_id FROM sticker_packs WHERE name LIKE ? ORDER BY name",
        )
        .bind(&like)
        .fetch_all(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
        // Also match packs whose stickers carry a matching custom name.
        let sticker_pack_ids: Vec<(u64,)> = sqlx::query_as(
            "SELECT DISTINCT pack_id FROM stickers WHERE name LIKE ?",
        )
        .bind(&like)
        .fetch_all(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
        let mut ids: Vec<u64> = pack_matches.iter().map(|r| r.0).collect();
        let mut names: Vec<String> = pack_matches.iter().map(|r| r.1.clone()).collect();
        let mut owners: Vec<u64> = pack_matches.iter().map(|r| r.2).collect();
        for (pid,) in sticker_pack_ids {
            if !ids.contains(&pid) {
                if let Some((name, owner)) = sqlx::query_as::<_, (String, u64)>(
                    "SELECT name, owner_id FROM sticker_packs WHERE id = ?",
                )
                .bind(pid)
                .fetch_optional(&state.pool)
                .await
                .map_err(|_| ApiError::Internal("db error"))?
                {
                    ids.push(pid);
                    names.push(name);
                    owners.push(owner);
                }
            }
        }
        (ids, names, owners)
    } else {
        let rows: Vec<(u64, String, u64)> = sqlx::query_as(
            "SELECT id, name, owner_id FROM sticker_packs ORDER BY name",
        )
        .fetch_all(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
        (rows.iter().map(|r| r.0).collect(), rows.iter().map(|r| r.1.clone()).collect(), rows.iter().map(|r| r.2).collect())
    };

    let mut out = Vec::with_capacity(packs.len());
    for i in 0..packs.len() {
        out.push(pack_json(&state, packs[i], &pack_names[i], owner_ids[i]).await?);
    }
    Ok(Json(json!({ "packs": out })))
}

/// Create a sticker pack owned by the caller.
pub async fn create_sticker_pack(
    State(state): State<AppState>,
    auth: AuthUser,
    Json(body): Json<CreatePackRequest>,
) -> Result<(axum::http::StatusCode, Json<Value>), ApiError> {
    let name = body.name.trim().to_string();
    if name.is_empty() || name.len() > MAX_PACK_NAME {
        return Err(ApiError::BadRequest("pack name must be 1-100 characters"));
    }
    let inserted = sqlx::query("INSERT INTO sticker_packs (owner_id, name) VALUES (?, ?)")
        .bind(auth.user.id)
        .bind(&name)
        .execute(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    Ok((
        axum::http::StatusCode::CREATED,
        Json(json!({ "id": inserted.last_insert_id(), "name": name, "owner_id": auth.user.id })),
    ))
}

/// Add a sticker image to a pack the caller owns.
pub async fn add_sticker(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(pack_id): Path<u64>,
    Json(body): Json<AddStickerRequest>,
) -> Result<Json<Value>, ApiError> {
    let owner: Option<(u64,)> = sqlx::query_as("SELECT owner_id FROM sticker_packs WHERE id = ?")
        .bind(pack_id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    let Some((owner_id,)) = owner else {
        return Err(ApiError::NotFound("pack not found"));
    };
    if owner_id != auth.user.id {
        return Err(ApiError::Forbidden("only the pack owner can add stickers"));
    }
    let name = body.name.trim().to_string();
    if name.is_empty() || name.len() > MAX_STICKER_NAME {
        return Err(ApiError::BadRequest("sticker name must be 1-100 characters"));
    }
    if !valid_sticker_mime(&body.mime) {
        return Err(ApiError::BadRequest("sticker image must be JPEG or WebP"));
    }
    let data = base64_decode(&body.data).ok_or(ApiError::BadRequest("invalid base64 data"))?;
    if data.is_empty() || data.len() > MAX_STICKER_BYTES {
        return Err(ApiError::BadRequest("sticker image too large (max 512 KiB)"));
    }
    // Normalize jpg -> jpeg.
    let mime = if body.mime == "image/jpg" { "image/jpeg".to_string() } else { body.mime.clone() };

    let inserted = sqlx::query("INSERT INTO stickers (pack_id, name, data, mime) VALUES (?, ?, ?, ?)")
        .bind(pack_id)
        .bind(&name)
        .bind(&data)
        .bind(&mime)
        .execute(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    Ok(Json(json!({ "id": inserted.last_insert_id(), "pack_id": pack_id, "name": name, "mime": mime })))
}

/// Delete a single sticker (pack owner only).
pub async fn delete_sticker(
    State(state): State<AppState>,
    auth: AuthUser,
    Path((pack_id, sticker_id)): Path<(u64, u64)>,
) -> Result<Json<Value>, ApiError> {
    let owner: Option<(u64,)> = sqlx::query_as("SELECT owner_id FROM sticker_packs WHERE id = ?")
        .bind(pack_id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    let Some((owner_id,)) = owner else {
        return Err(ApiError::NotFound("pack not found"));
    };
    if owner_id != auth.user.id {
        return Err(ApiError::Forbidden("only the pack owner can delete stickers"));
    }
    sqlx::query("DELETE FROM stickers WHERE id = ? AND pack_id = ?")
        .bind(sticker_id)
        .bind(pack_id)
        .execute(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    Ok(Json(json!({ "status": "ok" })))
}

/// Delete a whole pack and its stickers (pack owner only).
pub async fn delete_sticker_pack(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(pack_id): Path<u64>,
) -> Result<Json<Value>, ApiError> {
    let owner: Option<(u64,)> = sqlx::query_as("SELECT owner_id FROM sticker_packs WHERE id = ?")
        .bind(pack_id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    let Some((owner_id,)) = owner else {
        return Err(ApiError::NotFound("pack not found"));
    };
    if owner_id != auth.user.id {
        return Err(ApiError::Forbidden("only the pack owner can delete the pack"));
    }
    sqlx::query("DELETE FROM sticker_packs WHERE id = ?")
        .bind(pack_id)
        .execute(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    Ok(Json(json!({ "status": "ok" })))
}

/// Serve a sticker image (authenticated). Content-Type matches the stored mime.
pub async fn sticker_image(
    State(state): State<AppState>,
    auth: AuthUser,
    Path(sticker_id): Path<u64>,
) -> Result<(HeaderMap, Vec<u8>), ApiError> {
    let _ = auth; // any authenticated user may view stickers
    let row: Option<(Vec<u8>, String)> = sqlx::query_as("SELECT data, mime FROM stickers WHERE id = ?")
        .bind(sticker_id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    let Some((data, mime)) = row else {
        return Err(ApiError::NotFound("sticker not found"));
    };
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, mime.parse().unwrap_or(axum::http::header::HeaderValue::from_static("image/jpeg")));
    headers.insert("Cache-Control", "public, max-age=86400".parse().unwrap());
    Ok((headers, data))
}

/// The sticker image as a data URL (used by the server-side link/message embed
/// paths and tests). Kept tiny and local.
pub fn sticker_data_url(data: &[u8], mime: &str) -> String {
    format!("data:{mime};base64,{}", base64_encode(data))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_roundtrip() {
        let bytes = vec![1u8, 2, 3, 255];
        let s = base64_encode(&bytes);
        assert_eq!(base64_decode(&s).unwrap(), bytes);
    }

    #[test]
    fn mime_validation() {
        assert!(valid_sticker_mime("image/webp"));
        assert!(valid_sticker_mime("image/jpeg"));
        assert!(!valid_sticker_mime("image/gif"));
        assert!(!valid_sticker_mime("text/html"));
    }
}
