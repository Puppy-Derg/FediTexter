use std::time::SystemTime;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use feditexter_server::{build_app, db::AppState};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use sqlx::MySqlPool;
use tokio::sync::OnceCell;
use tower::ServiceExt;

static MIGRATED: OnceCell<()> = OnceCell::const_new();

async fn test_state() -> AppState {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://feditexter:dev-pass@localhost:3306/feditexter_test".into());
    let pool = MySqlPool::connect(&url).await.unwrap();
    MIGRATED
        .get_or_init(|| async {
            sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
        })
        .await;
    AppState { pool }
}

fn unique_nanos() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

fn unique_email(prefix: &str) -> String {
    format!("{prefix}{}@example.com", unique_nanos())
}

fn unique_username(prefix: &str) -> String {
    format!("{prefix}{}", unique_nanos())
}

async fn post_json(state: &AppState, path: &str, body: Value) -> (StatusCode, Value) {
    let app = build_app(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

async fn get_json(state: &AppState, path: &str, token: &str) -> (StatusCode, Value) {
    let app = build_app(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(path)
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

async fn register_user(state: &AppState, prefix: &str) -> (String, String, String) {
    let email = unique_email(prefix);
    let username = unique_username(prefix);
    let (status, body) = post_json(state, "/api/register", json!({
        "email": email, "username": username, "password": "password123"
    })).await;
    assert_eq!(status, StatusCode::CREATED);
    let token = body["token"].as_str().unwrap().to_string();
    (email, username, token)
}

#[tokio::test]
async fn register_creates_user_and_token() {
    let state = test_state().await;
    let (email, username, _token) = register_user(&state, "user").await;

    let (status, body) = post_json(&state, "/api/login", json!({
        "email": email, "password": "password123"
    })).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["user"]["username"], username);
}

#[tokio::test]
async fn me_returns_user_with_valid_token() {
    let state = test_state().await;
    let (email, username, token) = register_user(&state, "me").await;

    let (status, body) = get_json(&state, "/api/me", &token).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["user"]["email"], email);
    assert_eq!(body["user"]["username"], username);
}

#[tokio::test]
async fn me_rejects_invalid_token() {
    let state = test_state().await;

    let (status, _) = get_json(&state, "/api/me", "garbage-token").await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn logout_invalidates_session() {
    let state = test_state().await;
    let (_email, _username, token) = register_user(&state, "out").await;

    let app = build_app(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/logout")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    let (status, _) = get_json(&state, "/api/me", &token).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn duplicate_email_conflicts() {
    let state = test_state().await;
    let email = unique_email("dup");
    let username1 = unique_username("bob");
    let username2 = unique_username("bob2");

    let (status, _) = post_json(&state, "/api/register", json!({
        "email": email, "username": username1, "password": "password123"
    })).await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, _) = post_json(&state, "/api/register", json!({
        "email": email, "username": username2, "password": "password123"
    })).await;

    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn register_conflict_message_is_generic() {
    let state = test_state().await;
    let email = unique_email("conflict");
    let username1 = unique_username("conflict");
    let username2 = unique_username("conflict2");

    let (status, _) = post_json(&state, "/api/register", json!({
        "email": email, "username": username1, "password": "password123"
    })).await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = post_json(&state, "/api/register", json!({
        "email": email, "username": username2, "password": "password123"
    })).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        body["error"],
        json!("email or username already taken"),
        "conflict must not reveal which field collided"
    );
}

#[tokio::test]
async fn login_unknown_email_returns_generic_401() {
    let state = test_state().await;

    let (status, body) = post_json(&state, "/api/login", json!({
        "email": unique_email("nobody"), "password": "password123"
    })).await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["error"], json!("invalid credentials"));
}
