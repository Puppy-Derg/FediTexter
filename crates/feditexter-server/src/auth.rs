use argon2::password_hash::SaltString;
use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use axum::extract::{ConnectInfo, FromRequestParts};
use axum::http::request::Parts;
use rand_core::{OsRng, RngCore};
use serde::Serialize;
use sha2::{Digest, Sha256};
use sqlx::FromRow;
use std::net::SocketAddr;

use crate::api::error::ApiError;
use crate::db::AppState;

#[derive(FromRow, Serialize, Debug, Clone)]
pub struct User {
    pub id: u64,
    pub email: String,
    pub username: String,
    pub display_name: String,
    pub email_verified: bool,
    #[serde(default)]
    pub avatar_url: Option<String>,
    #[serde(default)]
    pub totp_enabled: bool,
    #[serde(default)]
    pub is_bot: bool,
    #[serde(default)]
    pub bio: String,
    #[serde(default)]
    pub profile_visible: bool,
}

#[derive(FromRow)]
pub(crate) struct Session {
    pub(crate) id: u64,
    pub(crate) user_id: u64,
    pub(crate) expires_at: chrono::NaiveDateTime,
    pub(crate) is_2fa_pending: bool,
    pub(crate) device_id: Option<String>,
    pub(crate) login_ip: Option<String>,
}

pub struct AuthUser {
    pub user: User,
    pub(crate) session_id: u64,
}

/// Same as `AuthUser`, but skips the "2FA enabled" check so users who have
/// not configured 2FA yet can still reach the setup/verify/logout endpoints.
pub struct AuthUserLax(pub AuthUser);

impl std::ops::Deref for AuthUserLax {
    type Target = AuthUser;
    fn deref(&self) -> &AuthUser {
        &self.0
    }
}

pub fn hash_password(password: &str) -> Result<String, argon2::password_hash::Error> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
}

pub fn verify_password(password: &str, hash: &str) -> bool {
    PasswordHash::new(hash)
        .ok()
        .map(|parsed| Argon2::default().verify_password(password.as_bytes(), &parsed).is_ok())
        .unwrap_or(false)
}

pub fn sha256(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    hex::encode(hasher.finalize())
}

pub(crate) fn generate_token_pair() -> (String, String) {
    let mut bytes = [0u8; 32];
    OsRng.fill_bytes(&mut bytes);
    let token = hex::encode(bytes);
    let hash = sha256(&token);
    (token, hash)
}

pub(crate) fn generate_verification_code() -> String {
    let mut bytes = [0u8; 4];
    OsRng.fill_bytes(&mut bytes);
    let code = u32::from_le_bytes(bytes) % 1_000_000;
    format!("{code:06}")
}

pub(crate) const SESSION_DAYS: i64 = 30;
/// Length of a "remember me" session, in days.
pub(crate) const SESSION_DAYS_REMEMBER: i64 = 60;

impl FromRequestParts<AppState> for AuthUser {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, Self::Rejection> {
        let (session_id, user) = load_auth_user(state, parts).await?;
        if !user.totp_enabled {
            return Err(ApiError::TwoFaSetupRequired);
        }
        Ok(AuthUser { user, session_id })
    }
}

impl FromRequestParts<AppState> for AuthUserLax {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, Self::Rejection> {
        let (session_id, user) = load_auth_user(state, parts).await?;
        Ok(AuthUserLax(AuthUser { user, session_id }))
    }
}

async fn load_auth_user(
    state: &AppState,
    parts: &Parts,
) -> Result<(u64, User), ApiError> {
    let header = parts
        .headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or(ApiError::Unauthorized("missing bearer token"))?;

    let token_hash = sha256(header);

    let session: Session = sqlx::query_as(
        "SELECT id, user_id, expires_at, is_2fa_pending, device_id, login_ip
         FROM sessions WHERE token_hash = ?",
    )
    .bind(&token_hash)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?
    .ok_or(ApiError::Unauthorized("invalid token"))?;

    if session.expires_at < chrono::Utc::now().naive_utc() {
        return Err(ApiError::Unauthorized("token expired"));
    }
    if session.is_2fa_pending {
        return Err(ApiError::Unauthorized("2fa required"));
    }

    enforce_session_binding(state, parts, &session).await?;

    let user: User = sqlx::query_as(
        "SELECT id, email, username, display_name, email_verified, avatar_url, totp_enabled, is_bot, bio, profile_visible FROM users WHERE id = ?",
    )
    .bind(session.user_id)
    .fetch_one(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;

    Ok((session.id, user))
}

/// The device UUID presented by the client, if any.
pub(crate) fn request_device_id(parts: &Parts) -> Option<String> {
    request_device_id_from_headers(&parts.headers)
}

/// Same as [`request_device_id`] but takes a plain header map so handlers that
/// don't use [`FromRequestParts`] can also extract it.
pub(crate) fn request_device_id_from_headers(headers: &axum::http::HeaderMap) -> Option<String> {
    headers
        .get("x-device-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Best-effort client IP: forwarded headers first (the server typically sits
/// behind nginx), then the direct socket address.
pub(crate) fn client_ip(
    headers: &axum::http::HeaderMap,
    connect_info: Option<&ConnectInfo<SocketAddr>>,
) -> Option<String> {
    for name in ["x-forwarded-for", "x-real-ip", "cf-connecting-ip"] {
        if let Some(v) = headers.get(name).and_then(|v| v.to_str().ok()) {
            let first = v.split(',').next().unwrap_or(v).trim();
            if !first.is_empty() {
                return Some(first.to_string());
            }
        }
    }
    connect_info.map(|ci| ci.0.ip().to_string())
}

/// Reject requests whose device UUID or client IP no longer matches the one the
/// session was created with, so a stolen token can't be replayed elsewhere.
/// A device mismatch is treated as theft: the session is revoked outright so
/// neither the thief nor the original client can keep using it. Legacy sessions
/// (columns NULL) are bound on first use to upgrade them.
async fn enforce_session_binding(
    state: &AppState,
    parts: &Parts,
    session: &Session,
) -> Result<(), ApiError> {
    let presented = request_device_id(parts);
    match (&session.device_id, &presented) {
        (Some(expected), Some(actual)) if expected == actual => {}
        (Some(_), _) => {
            // The token is being used from a device it was never issued to (or
            // with no device header at all) — revoke the session so the token
            // can't be replayed anywhere else.
            if let Err(e) = sqlx::query("DELETE FROM sessions WHERE id = ?")
                .bind(session.id)
                .execute(&state.pool)
                .await
            {
                tracing::error!("failed to revoke compromised session {}: {e}", session.id);
            }
            return Err(ApiError::Unauthorized("session invalidated: token was used from another device"));
        }
        (None, Some(actual)) => {
            sqlx::query("UPDATE sessions SET device_id = ? WHERE id = ?")
                .bind(actual)
                .bind(session.id)
                .execute(&state.pool)
                .await
                .map_err(|_| ApiError::Internal("db error"))?;
        }
        (None, None) => {
            return Err(ApiError::Unauthorized("missing device id, please log in again"));
        }
    }

    let connect_info = parts.extensions.get::<ConnectInfo<SocketAddr>>();
    if let Some(ip) = client_ip(&parts.headers, connect_info) {
        match &session.login_ip {
            Some(expected) if expected != &ip => {
                return Err(ApiError::Unauthorized("session is bound to another ip"));
            }
            Some(_) => {}
            None => {
                sqlx::query("UPDATE sessions SET login_ip = ? WHERE id = ?")
                    .bind(&ip)
                    .bind(session.id)
                    .execute(&state.pool)
                    .await
                    .map_err(|_| ApiError::Internal("db error"))?;
            }
        }
    }

    Ok(())
}