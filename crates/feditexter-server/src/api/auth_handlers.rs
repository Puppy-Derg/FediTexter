use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::api::error::ApiError;
use crate::auth::{hash_password, verify_password, AuthUser, User};
use crate::db::AppState;

#[derive(Deserialize)]
pub struct RegisterRequest {
    pub email: String,
    pub username: String,
    pub password: String,
}

#[derive(Deserialize)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

pub async fn register(
    State(state): State<AppState>,
    Json(body): Json<RegisterRequest>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    if body.password.len() < 8 {
        return Err(ApiError::BadRequest("password must be at least 8 characters"));
    }
    if !body.email.contains('@') {
        return Err(ApiError::BadRequest("invalid email"));
    }
    if body.username.is_empty() || body.username.len() > 32 {
        return Err(ApiError::BadRequest("username must be 1-32 characters"));
    }
    if !body.username.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(ApiError::BadRequest("username may only contain letters, numbers, and underscores"));
    }

    let password_hash = hash_password(&body.password)
        .map_err(|_| ApiError::Internal("hashing failed"))?;

    let (email_verified, verification_code) = if state.verify_emails {
        (false, Some(crate::auth::generate_verification_code()))
    } else {
        (true, None)
    };

    let result = sqlx::query(
        "INSERT INTO users (email, username, password_hash, email_verified, verification_code)
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(&body.email)
    .bind(&body.username)
    .bind(&password_hash)
    .bind(email_verified)
    .bind(&verification_code)
    .execute(&state.pool)
    .await;

    let user_id = match result {
        Ok(r) => r.last_insert_id(),
        Err(sqlx::Error::Database(e)) if e.is_unique_violation() => {
            return Err(ApiError::Conflict("email or username already taken"));
        }
        Err(_) => return Err(ApiError::Internal("db error")),
    };

    let user = User {
        id: user_id,
        email: body.email,
        username: body.username,
        display_name: String::new(),
        email_verified,
        avatar_url: None,
    };

    let token = create_session(&state, user_id).await?;

    if state.verify_emails {
        if let Some(code) = &verification_code {
            match &state.mailer {
                Some(mailer) => {
                    if let Err(e) = mailer.send_verification_code(&user.email, code).await {
                        tracing::warn!("failed to send verification email to {}: {e}", user.email);
                        // Log the code so it can still be recovered in an outage.
                        tracing::warn!("verification code for {} is {code}", user.email);
                    }
                }
                None => {
                    // No SMTP configured: log the code so it can be used manually.
                    tracing::warn!("no SMTP configured; verification code for {} is {code}", user.email);
                }
            }
        }
    }

    Ok((StatusCode::CREATED, Json(json!({ "token": token, "user": user }))))
}

const DUMMY_PASSWORD_HASH: &str = "$argon2id$v=19$m=19456,t=2,p=1$fAWlNrU+t0yc2hZQVobo/Q$YcqXjlp/Iy9rWpLwLPTwDBUyV1umH3j88kucq4n1zuU";

pub async fn login(
    State(state): State<AppState>,
    Json(body): Json<LoginRequest>,
) -> Result<Json<Value>, ApiError> {
    let row: Option<(u64, String, String, String, String, bool, Option<String>)> = sqlx::query_as(
        "SELECT id, email, username, display_name, password_hash, email_verified, avatar_url
         FROM users WHERE email = ?",
    )
    .bind(&body.email)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;

    let (id, email, username, display_name, password_hash, email_verified, avatar_url) = match row {
        Some(r) => r,
        None => {
            verify_password(&body.password, DUMMY_PASSWORD_HASH);
            return Err(ApiError::Unauthorized("invalid credentials"));
        }
    };

    if !verify_password(&body.password, &password_hash) {
        return Err(ApiError::Unauthorized("invalid credentials"));
    }

    let user = User { id, email, username, display_name, email_verified, avatar_url };
    let token = create_session(&state, id).await?;
    Ok(Json(json!({ "token": token, "user": user })))
}

pub async fn logout(
    State(state): State<AppState>,
    auth: AuthUser,
) -> Result<StatusCode, ApiError> {
    sqlx::query("DELETE FROM sessions WHERE id = ?")
        .bind(auth.session_id)
        .execute(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn me(auth: AuthUser) -> Json<Value> {
    Json(json!({ "user": auth.user }))
}

#[derive(Deserialize)]
pub struct UpdateMeRequest {
    pub display_name: String,
}

pub async fn update_me(
    State(state): State<AppState>,
    auth: AuthUser,
    Json(body): Json<UpdateMeRequest>,
) -> Result<Json<Value>, ApiError> {
    let display_name = body.display_name.trim().to_string();
    if display_name.len() > 64 {
        return Err(ApiError::BadRequest("display name too long (max 64)"));
    }

    sqlx::query("UPDATE users SET display_name = ? WHERE id = ?")
        .bind(&display_name)
        .bind(auth.user.id)
        .execute(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;

    let user: User = sqlx::query_as(
        "SELECT id, email, username, display_name, email_verified, avatar_url FROM users WHERE id = ?",
    )
    .bind(auth.user.id)
    .fetch_one(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;

    Ok(Json(json!({ "user": user })))
}

#[derive(Deserialize)]
pub struct VerifyRequest {
    pub code: String,
}

pub async fn verify(
    State(state): State<AppState>,
    auth: AuthUser,
    Json(body): Json<VerifyRequest>,
) -> Result<Json<Value>, ApiError> {
    let code = body.code.trim().to_string();
    if code.is_empty() {
        return Err(ApiError::BadRequest("missing verification code"));
    }

    let result = sqlx::query(
        "UPDATE users SET email_verified = TRUE WHERE id = ? AND verification_code = ?",
    )
    .bind(auth.user.id)
    .bind(&code)
    .execute(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;

    if result.rows_affected() == 0 {
        return Err(ApiError::BadRequest("invalid verification code"));
    }

    let user: User = sqlx::query_as(
        "SELECT id, email, username, display_name, email_verified, avatar_url FROM users WHERE id = ?",
    )
    .bind(auth.user.id)
    .fetch_one(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;

    Ok(Json(json!({ "user": user })))
}

#[derive(Deserialize)]
pub struct SetAvatarRequest {
    pub avatar: String,
}

pub async fn set_avatar(
    State(state): State<AppState>,
    auth: AuthUser,
    Json(body): Json<SetAvatarRequest>,
) -> Result<Json<Value>, ApiError> {
    let avatar = body.avatar.trim().to_string();
    if avatar.len() > 2_000_000 {
        return Err(ApiError::BadRequest("avatar too large (max ~2MB)"));
    }
    let avatar_url: Option<String> = if avatar.is_empty() {
        None
    } else if avatar.starts_with("data:image/") {
        // Store only a small 32x32 thumbnail. High-resolution originals are
        // meant to be exchanged peer-to-peer between clients, so the server
        // keeps just enough for list avatars.
        match downscale_avatar(&avatar) {
            Some(thumb) => Some(thumb),
            None => return Err(ApiError::BadRequest("could not decode avatar image")),
        }
    } else {
        return Err(ApiError::BadRequest("avatar must be a data:image/... URL"));
    };

    sqlx::query("UPDATE users SET avatar_url = ? WHERE id = ?")
        .bind(&avatar_url)
        .bind(auth.user.id)
        .execute(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;

    let user: User = sqlx::query_as(
        "SELECT id, email, username, display_name, email_verified, avatar_url FROM users WHERE id = ?",
    )
    .bind(auth.user.id)
    .fetch_one(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;

    Ok(Json(json!({ "user": user })))
}

/// Decode a `data:image/...;base64,...` avatar and re-encode it as a 32x32 PNG.
fn downscale_avatar(data_url: &str) -> Option<String> {
    let b64 = data_url.split_once(";base64,")?.1;
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD.decode(b64).ok()?;
    let img = image::load_from_memory(&bytes).ok()?;
    let max_dim = 32u32;
    let img = if img.width() > max_dim || img.height() > max_dim {
        let scale = max_dim as f32 / img.width().max(img.height()) as f32;
        img.resize(
            ((img.width() as f32) * scale) as u32,
            ((img.height() as f32) * scale) as u32,
            image::imageops::FilterType::Lanczos3,
        )
    } else {
        img
    };
    let mut out = std::io::Cursor::new(Vec::new());
    img.write_to(&mut out, image::ImageFormat::Png).ok()?;
    let data = base64::engine::general_purpose::STANDARD.encode(out.into_inner());
    Some(format!("data:image/png;base64,{data}"))
}

async fn create_session(state: &AppState, user_id: u64) -> Result<String, ApiError> {
    let (token, token_hash) = crate::auth::generate_token_pair();
    let expires_at = chrono::Utc::now().naive_utc() + chrono::Duration::days(crate::auth::SESSION_DAYS);
    sqlx::query("INSERT INTO sessions (user_id, token_hash, expires_at) VALUES (?, ?, ?)")
        .bind(user_id)
        .bind(&token_hash)
        .bind(expires_at)
        .execute(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    Ok(token)
}