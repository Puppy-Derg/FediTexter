use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::api::error::ApiError;
use crate::auth::{hash_password, verify_password, AuthUser, AuthUserLax, User};
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
    /// Persist the session on this device for 60 days instead of the default
    /// session length.
    #[serde(default)]
    pub remember_me: Option<bool>,
}

pub async fn register(
    State(state): State<AppState>,
    headers: HeaderMap,
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
        totp_enabled: false,
    };

    let device_id = headers.get("x-device-id").and_then(|v| v.to_str().ok()).map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
    let login_ip = crate::auth::client_ip(&headers, None);

    let token = create_session(&state, user_id, false, device_id, login_ip).await?;

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
    headers: HeaderMap,
    Json(body): Json<LoginRequest>,
) -> Result<Json<Value>, ApiError> {
    let row: Option<(u64, String, String, String, String, bool, Option<String>, bool)> = sqlx::query_as(
        "SELECT id, email, username, display_name, password_hash, email_verified, avatar_url, totp_enabled
         FROM users WHERE email = ?",
    )
    .bind(&body.email)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;

    let (id, email, username, display_name, password_hash, email_verified, avatar_url, totp_enabled) = match row {
        Some(r) => r,
        None => {
            verify_password(&body.password, DUMMY_PASSWORD_HASH);
            return Err(ApiError::Unauthorized("invalid credentials"));
        }
    };

    if !verify_password(&body.password, &password_hash) {
        return Err(ApiError::Unauthorized("invalid credentials"));
    }

    let user = User { id, email, username, display_name, email_verified, avatar_url, totp_enabled };
    let device_id = headers.get("x-device-id").and_then(|v| v.to_str().ok()).map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
    let login_ip = crate::auth::client_ip(&headers, None);

    if totp_enabled {
        // Create a 2FA-pending session; the client must complete /api/login/2fa.
        let pending_token = create_pending_session(&state, id, device_id, login_ip).await?;
        return Ok(Json(json!({
            "requires_2fa": true,
            "pending_token": pending_token,
        })));
    }

    let token = create_session(&state, id, body.remember_me.unwrap_or(false), device_id, login_ip).await?;
    Ok(Json(json!({ "token": token, "user": user })))
}

#[derive(Deserialize)]
pub struct Login2faRequest {
    pub pending_token: String,
    pub code: String,
    #[serde(default)]
    pub remember_me: Option<bool>,
}

/// Complete a 2FA login: verify the TOTP code for the pending session.
pub async fn login_2fa(
    State(state): State<AppState>,
    Json(body): Json<Login2faRequest>,
) -> Result<Json<Value>, ApiError> {
    let token_hash = crate::auth::sha256(body.pending_token.trim());
    let row: Option<(u64, u64)> = sqlx::query_as(
        "SELECT id, user_id FROM sessions WHERE token_hash = ? AND is_2fa_pending = 1",
    )
    .bind(&token_hash)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;

    let (session_id, user_id) = match row {
        Some(r) => r,
        None => return Err(ApiError::Unauthorized("invalid or expired pending token")),
    };

    let secret: String = sqlx::query_scalar("SELECT totp_secret FROM users WHERE id = ?")
        .bind(user_id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?
        .ok_or(ApiError::Unauthorized("2fa not enabled"))?;

    if !totp_check(&secret, &body.code) {
        return Err(ApiError::Unauthorized("invalid code"));
    }

    // The pending session was created with a 10-minute expiry; promote it to a
    // full session now that 2FA passed.
    let days = if body.remember_me.unwrap_or(false) {
        crate::auth::SESSION_DAYS_REMEMBER
    } else {
        crate::auth::SESSION_DAYS
    };
    let expires_at = chrono::Utc::now().naive_utc() + chrono::Duration::days(days);
    sqlx::query("UPDATE sessions SET is_2fa_pending = 0, expires_at = ? WHERE id = ?")
        .bind(expires_at)
        .bind(session_id)
        .execute(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;

    let user: User = sqlx::query_as(
        "SELECT id, email, username, display_name, email_verified, avatar_url, totp_enabled FROM users WHERE id = ?",
    )
    .bind(user_id)
    .fetch_one(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;

    Ok(Json(json!({ "token": body.pending_token, "user": user })))
}

/// Generate a TOTP secret for the user and return the otpauth URI + QR PNG.
/// Uses the lax extractor so accounts without 2FA can still set it up.
pub async fn two_fa_setup(
    State(state): State<AppState>,
    auth: AuthUserLax,
) -> Result<Json<Value>, ApiError> {
    let base32 = totp_generate_base32();
    sqlx::query("UPDATE users SET totp_secret = ?, totp_enabled = 0 WHERE id = ?")
        .bind(&base32)
        .bind(auth.user.id)
        .execute(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;

    let uri = format!(
        "otpauth://totp/FediTexter:{}?secret={}&issuer=FediTexter&algorithm=SHA1&digits=6&period=30",
        auth.user.email, base32
    );
    let qr = totp_qr_png(&uri);

    Ok(Json(json!({
        "secret": base32,
        "uri": uri,
        "qr": qr,
    })))
}

#[derive(Deserialize)]
pub struct TwoFaCodeRequest {
    pub code: String,
}

/// Enable 2FA after verifying the current code against the stored secret.
/// Uses the lax extractor so accounts without 2FA can still enable it.
pub async fn two_fa_enable(
    State(state): State<AppState>,
    auth: AuthUserLax,
    Json(body): Json<TwoFaCodeRequest>,
) -> Result<Json<Value>, ApiError> {
    let secret: Option<String> = sqlx::query_scalar("SELECT totp_secret FROM users WHERE id = ?")
        .bind(auth.user.id)
        .fetch_one(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;

    match secret {
        Some(secret) if totp_check(&secret, &body.code) => {
            sqlx::query("UPDATE users SET totp_enabled = 1 WHERE id = ?")
                .bind(auth.user.id)
                .execute(&state.pool)
                .await
                .map_err(|_| ApiError::Internal("db error"))?;
        }
        _ => return Err(ApiError::BadRequest("invalid code")),
    }

    let user: User = sqlx::query_as(
        "SELECT id, email, username, display_name, email_verified, avatar_url, totp_enabled FROM users WHERE id = ?",
    )
    .bind(auth.user.id)
    .fetch_one(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;

    Ok(Json(json!({ "user": user })))
}

/// Disable 2FA after verifying the current code.
pub async fn two_fa_disable(
    State(state): State<AppState>,
    auth: AuthUser,
    Json(body): Json<TwoFaCodeRequest>,
) -> Result<Json<Value>, ApiError> {
    let secret: Option<String> = sqlx::query_scalar("SELECT totp_secret FROM users WHERE id = ?")
        .bind(auth.user.id)
        .fetch_one(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;

    match secret {
        Some(secret) if totp_check(&secret, &body.code) => {
            sqlx::query("UPDATE users SET totp_enabled = 0, totp_secret = NULL WHERE id = ?")
                .bind(auth.user.id)
                .execute(&state.pool)
                .await
                .map_err(|_| ApiError::Internal("db error"))?;
        }
        _ => return Err(ApiError::BadRequest("invalid code")),
    }

    let user: User = sqlx::query_as(
        "SELECT id, email, username, display_name, email_verified, avatar_url, totp_enabled FROM users WHERE id = ?",
    )
    .bind(auth.user.id)
    .fetch_one(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;

    Ok(Json(json!({ "user": user })))
}

/// Create a session that requires 2FA before it can be used.
async fn create_pending_session(
    state: &AppState,
    user_id: u64,
    device_id: Option<String>,
    login_ip: Option<String>,
) -> Result<String, ApiError> {
    let (token, token_hash) = crate::auth::generate_token_pair();
    let expires_at = chrono::Utc::now().naive_utc() + chrono::Duration::minutes(10);
    sqlx::query(
        "INSERT INTO sessions (user_id, token_hash, expires_at, is_2fa_pending, device_id, login_ip)
         VALUES (?, ?, ?, 1, ?, ?)",
    )
    .bind(user_id)
    .bind(&token_hash)
    .bind(expires_at)
    .bind(device_id)
    .bind(login_ip)
    .execute(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;
    Ok(token)
}

pub async fn logout(
    State(state): State<AppState>,
    auth: AuthUserLax,
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
        "SELECT id, email, username, display_name, email_verified, avatar_url, totp_enabled FROM users WHERE id = ?",
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
    auth: AuthUserLax,
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
        "SELECT id, email, username, display_name, email_verified, avatar_url, totp_enabled FROM users WHERE id = ?",
    )
    .bind(auth.user.id)
    .fetch_one(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;

    Ok(Json(json!({ "user": user })))
}

/// Generate a fresh verification code and (re)send it to the user's email.
pub async fn resend_verification(
    State(state): State<AppState>,
    auth: AuthUserLax,
) -> Result<Json<Value>, ApiError> {
    let email: String = sqlx::query_scalar("SELECT email FROM users WHERE id = ?")
        .bind(auth.user.id)
        .fetch_one(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;

    let code = crate::auth::generate_verification_code();
    sqlx::query("UPDATE users SET verification_code = ? WHERE id = ?")
        .bind(&code)
        .bind(auth.user.id)
        .execute(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;

    match &state.mailer {
        Some(mailer) => {
            if let Err(e) = mailer.send_verification_code(&email, &code).await {
                tracing::warn!("failed to send verification email to {email}: {e}");
                tracing::warn!("verification code for {email} is {code}");
            }
        }
        None => {
            tracing::warn!("no SMTP configured; verification code for {email} is {code}");
        }
    }

    let user: User = sqlx::query_as(
        "SELECT id, email, username, display_name, email_verified, avatar_url, totp_enabled FROM users WHERE id = ?",
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
        "SELECT id, email, username, display_name, email_verified, avatar_url, totp_enabled FROM users WHERE id = ?",
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

async fn create_session(
    state: &AppState,
    user_id: u64,
    remember_me: bool,
    device_id: Option<String>,
    login_ip: Option<String>,
) -> Result<String, ApiError> {
    let (token, token_hash) = crate::auth::generate_token_pair();
    let days = if remember_me {
        crate::auth::SESSION_DAYS_REMEMBER
    } else {
        crate::auth::SESSION_DAYS
    };
    let expires_at = chrono::Utc::now().naive_utc() + chrono::Duration::days(days);
    sqlx::query(
        "INSERT INTO sessions (user_id, token_hash, expires_at, device_id, login_ip)
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(user_id)
    .bind(&token_hash)
    .bind(expires_at)
    .bind(device_id)
    .bind(login_ip)
    .execute(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;
    Ok(token)
}
// ---------------------------------------------------------------------------
// TOTP (2FA) helpers
// ---------------------------------------------------------------------------

use rand_core::RngCore;
use totp_rs::{Algorithm, Secret, TOTP};

fn base32_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut out = String::new();
    let mut buffer: u32 = 0;
    let mut bits = 0;
    for &b in data {
        buffer = (buffer << 8) | b as u32;
        bits += 8;
        while bits >= 5 {
            out.push(ALPHABET[((buffer >> (bits - 5)) & 31) as usize] as char);
            bits -= 5;
        }
    }
    if bits > 0 {
        out.push(ALPHABET[((buffer << (5 - bits)) & 31) as usize] as char);
    }
    out
}

fn totp_generate_base32() -> String {
    let mut bytes = [0u8; 20];
    rand_core::OsRng.fill_bytes(&mut bytes);
    base32_encode(&bytes)
}

fn totp_check(secret_base32: &str, code: &str) -> bool {
    let code = code.trim();
    match Secret::Encoded(secret_base32.to_string()).to_bytes() {
        Ok(raw) => match TOTP::new(Algorithm::SHA1, 6, 1, 30, raw) {
            Ok(t) => t.check_current(code).unwrap_or(false),
            Err(_) => false,
        },
        Err(_) => false,
    }
}

fn totp_qr_png(uri: &str) -> Option<String> {
    let code = qrcode::QrCode::new(uri.as_bytes()).ok()?;
    let img: image::ImageBuffer<image::Luma<u8>, Vec<u8>> =
        code.render::<image::Luma<u8>>().min_dimensions(240, 240).build();
    let mut out = std::io::Cursor::new(Vec::new());
    image::DynamicImage::ImageLuma8(img).write_to(&mut out, image::ImageFormat::Png).ok()?;
    use base64::Engine;
    let data = base64::engine::general_purpose::STANDARD.encode(out.into_inner());
    Some(format!("data:image/png;base64,{data}"))
}
