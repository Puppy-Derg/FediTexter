use argon2::password_hash::SaltString;
use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use rand_core::{OsRng, RngCore};
use serde::Serialize;
use sha2::{Digest, Sha256};
use sqlx::FromRow;

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
}

#[derive(FromRow)]
struct Session {
    id: u64,
    user_id: u64,
    expires_at: chrono::NaiveDateTime,
    is_2fa_pending: bool,
}

pub struct AuthUser {
    pub user: User,
    pub(crate) session_id: u64,
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

impl FromRequestParts<AppState> for AuthUser {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, Self::Rejection> {
        let header = parts
            .headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .ok_or(ApiError::Unauthorized("missing bearer token"))?;

        let token_hash = sha256(header);

        let session: Session = sqlx::query_as(
            "SELECT id, user_id, expires_at, is_2fa_pending FROM sessions WHERE token_hash = ?",
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

        let user: User = sqlx::query_as(
            "SELECT id, email, username, display_name, email_verified, avatar_url, totp_enabled FROM users WHERE id = ?",
        )
        .bind(session.user_id)
        .fetch_one(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;

        Ok(AuthUser { user, session_id: session.id })
    }
}