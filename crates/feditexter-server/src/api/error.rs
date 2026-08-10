use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

pub enum ApiError {
    BadRequest(&'static str),
    Forbidden(&'static str),
    Conflict(&'static str),
    Unauthorized(&'static str),
    NotFound(&'static str),
    BadGateway(&'static str),
    Internal(&'static str),
    /// The account has no TOTP secret enabled, which is now mandatory.
    TwoFaSetupRequired,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            ApiError::Forbidden(m) => (StatusCode::FORBIDDEN, m),
            ApiError::Conflict(m) => (StatusCode::CONFLICT, m),
            ApiError::Unauthorized(m) => (StatusCode::UNAUTHORIZED, m),
            ApiError::NotFound(m) => (StatusCode::NOT_FOUND, m),
            ApiError::BadGateway(m) => (StatusCode::BAD_GATEWAY, m),
            ApiError::Internal(m) => (StatusCode::INTERNAL_SERVER_ERROR, m),
            ApiError::TwoFaSetupRequired => (StatusCode::FORBIDDEN, "2fa setup required"),
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}