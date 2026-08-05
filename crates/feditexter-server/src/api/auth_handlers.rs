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

    let password_hash = hash_password(&body.password)
        .map_err(|_| ApiError::Internal("hashing failed"))?;

    let result = sqlx::query("INSERT INTO users (email, username, password_hash) VALUES (?, ?, ?)")
        .bind(&body.email)
        .bind(&body.username)
        .bind(&password_hash)
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
    };

    let token = create_session(&state, user_id).await?;
    Ok((StatusCode::CREATED, Json(json!({ "token": token, "user": user }))))
}

pub async fn login(
    State(state): State<AppState>,
    Json(body): Json<LoginRequest>,
) -> Result<Json<Value>, ApiError> {
    let row: Option<(u64, String, String, String, String)> = sqlx::query_as(
        "SELECT id, email, username, display_name, password_hash FROM users WHERE email = ?",
    )
    .bind(&body.email)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;

    let (id, email, username, display_name, password_hash) =
        row.ok_or(ApiError::Unauthorized("invalid credentials"))?;

    if !verify_password(&body.password, &password_hash) {
        return Err(ApiError::Unauthorized("invalid credentials"));
    }

    let user = User { id, email, username, display_name };
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