use std::time::SystemTime;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use feditexter_server::{build_app, db::AppState, federation::Federation};
use futures_util::StreamExt;
use http_body_util::BodyExt;
use serde_json::{json, Value};
use sqlx::MySqlPool;
use tokio::sync::OnceCell;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
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
    let federation = Federation::init(&pool, "localhost").await.unwrap();
    AppState {
        pool,
        hub: Default::default(),
        federation,
        verify_emails: false,
        mailer: None,
        http: reqwest::Client::new(),
        presence: Default::default(),
        voice: Default::default(),
    }
}

fn unique_nanos() -> u128 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos()
}

fn unique_email(prefix: &str) -> String {
    format!("{prefix}{}@example.com", unique_nanos())
}

fn unique_username(prefix: &str) -> String {
    format!("{prefix}{}", unique_nanos())
}

async fn post_json(state: &AppState, path: &str, token: Option<&str>, body: Value) -> (StatusCode, Value) {
    let mut builder = Request::builder().method("POST").uri(path);
    if let Some(t) = token {
        builder = builder.header("authorization", format!("Bearer {t}"));
    }
    let app = build_app(state.clone());
    let resp = app
        .oneshot(
            builder
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

async fn register_user(state: &AppState, prefix: &str) -> (String, u64) {
    let email = unique_email(prefix);
    let username = unique_username(prefix);
    let (status, body) = post_json(
        state,
        "/api/register",
        None,
        json!({ "email": email, "username": username, "password": "password123" }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let token = body["token"].as_str().unwrap().to_string();
    let id = body["user"]["id"].as_u64().unwrap();
    (token, id)
}

async fn create_conversation(state: &AppState, token: &str, user_id: u64) -> u64 {
    let (status, body) = post_json(
        state,
        "/api/conversations",
        Some(token),
        json!({ "user_id": user_id }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create conversation failed: {body}");
    body["id"].as_u64().unwrap()
}

#[tokio::test]
async fn create_conversation_between_two_users() {
    let state = test_state().await;
    let (token_a, id_a) = register_user(&state, "ca").await;
    let (_token_b, id_b) = register_user(&state, "cb").await;

    let (status, body) = post_json(
        &state,
        "/api/conversations",
        Some(&token_a),
        json!({ "user_id": id_b }),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["kind"], json!("direct"));
    let members: Vec<u64> = body["members"].as_array().unwrap().iter().map(|m| m["id"].as_u64().unwrap()).collect();
    assert!(members.contains(&id_a));
    assert!(members.contains(&id_b));
}

#[tokio::test]
async fn direct_conversation_is_reused() {
    let state = test_state().await;
    let (token_a, _id_a) = register_user(&state, "re").await;
    let (_token_b, id_b) = register_user(&state, "re2").await;

    let conv1 = create_conversation(&state, &token_a, id_b).await;
    let conv2 = create_conversation(&state, &token_a, id_b).await;

    assert_eq!(conv1, conv2, "direct conversation should be reused, not duplicated");
}

#[tokio::test]
async fn cannot_converse_with_self() {
    let state = test_state().await;
    let (token_a, id_a) = register_user(&state, "self").await;

    let (status, _) = post_json(
        &state,
        "/api/conversations",
        Some(&token_a),
        json!({ "user_id": id_a }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn non_member_is_rejected() {
    let state = test_state().await;
    let (token_a, _id_a) = register_user(&state, "nm").await;
    let (_token_b, id_b) = register_user(&state, "nm2").await;
    let (token_c, _id_c) = register_user(&state, "nm3").await;

    let conv = create_conversation(&state, &token_a, id_b).await;

    let (status, _) = get_json(
        &state,
        &format!("/api/conversations/{conv}/messages"),
        &token_c,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "non-member must not see conversation");

    let (status, _) = post_json(
        &state,
        &format!("/api/conversations/{conv}/messages"),
        Some(&token_c),
        json!({ "body": "intrusion" }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "non-member must not send messages");
}

#[tokio::test]
async fn send_and_list_messages() {
    let state = test_state().await;
    let (token_a, id_a) = register_user(&state, "msg").await;
    let (_token_b, id_b) = register_user(&state, "msg2").await;

    let conv = create_conversation(&state, &token_a, id_b).await;

    let (status, body) = post_json(
        &state,
        &format!("/api/conversations/{conv}/messages"),
        Some(&token_a),
        json!({ "body": "hello there" }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "send failed: {body}");
    assert_eq!(body["message"]["sender_id"], json!(id_a));
    assert_eq!(body["message"]["body"], json!("hello there"));
    let msg_id = body["message"]["id"].as_u64().unwrap();

    let (status, body) = get_json(&state, &format!("/api/conversations/{conv}/messages"), &token_a).await;
    assert_eq!(status, StatusCode::OK);
    let messages = body["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0]["id"], json!(msg_id));
    assert_eq!(messages[0]["sender_id"], json!(id_a));
}

#[tokio::test]
async fn empty_message_is_rejected() {
    let state = test_state().await;
    let (token_a, _id_a) = register_user(&state, "emp").await;
    let (_token_b, id_b) = register_user(&state, "emp2").await;

    let conv = create_conversation(&state, &token_a, id_b).await;

    let (status, _) = post_json(
        &state,
        &format!("/api/conversations/{conv}/messages"),
        Some(&token_a),
        json!({ "body": "   " }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn ws_receives_message_in_real_time() {
    let state = test_state().await;
    let (token_a, _id_a) = register_user(&state, "ws").await;
    let (token_b, id_b) = register_user(&state, "ws2").await;

    let conv = create_conversation(&state, &token_a, id_b).await;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = build_app(state.clone());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let ws_url = format!("ws://{addr}/api/ws");
    let mut request: axum::http::Request<()> = ws_url
        .as_str()
        .into_client_request()
        .expect("build ws request");
    request.headers_mut().insert(
        "authorization",
        axum::http::HeaderValue::from_str(&format!("Bearer {token_b}")).unwrap(),
    );
    let (ws, _) = tokio_tungstenite::connect_async(request).await.expect("ws connect");
    let (_sink, mut stream) = ws.split();

    let (status, _) = post_json(
        &state,
        &format!("/api/conversations/{conv}/messages"),
        Some(&token_a),
        json!({ "body": "broadcast me" }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let msg = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        stream.next(),
    )
    .await
    .expect("timed out waiting for ws message")
    .expect("ws closed early")
    .unwrap();

    match msg {
        tokio_tungstenite::tungstenite::Message::Text(text) => {
            let v: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(v["kind"], json!("message"));
            assert_eq!(v["message"]["body"], json!("broadcast me"));
        }
        other => panic!("expected text message, got {other:?}"),
    }
}
