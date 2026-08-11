use std::time::{Duration, Instant};

use axum::http::StatusCode;
use feditexter_server::{build_app, db::AppState, federation::Federation};
use serde_json::{json, Value};
use sqlx::MySqlPool;

struct TestInstance {
    state: AppState,
    domain: String,
}

async fn admin_url() -> String {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://feditexter:dev-pass@localhost:3306/feditexter_test".into());
    url.rsplit_once('/').map(|(head, _)| head.to_string()).unwrap_or(url)
}

async fn reset_db(name: &str) -> String {
    let server = admin_url().await;
    let admin = MySqlPool::connect(&server).await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP DATABASE IF EXISTS `{name}`")))
        .execute(&admin)
        .await
        .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE `{name}` CHARACTER SET utf8mb4")))
        .execute(&admin)
        .await
        .unwrap();
    let url = format!("{server}/{name}");
    let pool = MySqlPool::connect(&url).await.unwrap();
    sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
    url
}

async fn start_instance(db_url: &str) -> TestInstance {
    let pool = MySqlPool::connect(db_url).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let domain = addr.to_string();
    let federation = Federation::init(&pool, &domain).await.unwrap();
    let state = AppState {
        pool,
        hub: Default::default(),
        federation,
        verify_emails: false,
        mailer: None,
        http: reqwest::Client::new(),
        presence: Default::default(),
    };
    let app = build_app(state.clone());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    TestInstance { state, domain }
}

fn client() -> reqwest::Client {
    reqwest::Client::builder().timeout(Duration::from_secs(10)).build().unwrap()
}

async fn register(inst: &TestInstance, username: &str) -> (String, u64) {
    let url = format!("http://{}/api/register", inst.domain);
    let resp = client()
        .post(&url)
        .json(&json!({
            "email": format!("{username}@example.com"),
            "username": username,
            "password": "password123",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED, "register failed");
    let v: Value = resp.json().await.unwrap();
    let token = v["token"].as_str().unwrap().to_string();
    let id = v["user"]["id"].as_u64().unwrap();
    (token, id)
}

async fn create_conversation_handle(inst: &TestInstance, token: &str, handle: &str) -> Value {
    let url = format!("http://{}/api/conversations", inst.domain);
    let resp = client()
        .post(&url)
        .bearer_auth(token)
        .json(&json!({ "handle": handle }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "create conversation with {handle} failed with {}: {:?}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );
    resp.json().await.unwrap()
}

async fn send_message(inst: &TestInstance, token: &str, conv: u64, body: &str) -> Value {
    let url = format!("http://{}/api/conversations/{conv}/messages", inst.domain);
    let resp = client()
        .post(&url)
        .bearer_auth(token)
        .json(&json!({ "body": body }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED, "send failed");
    resp.json().await.unwrap()
}

async fn list_conversations(inst: &TestInstance, token: &str) -> Value {
    let url = format!("http://{}/api/conversations", inst.domain);
    client().get(&url).bearer_auth(token).send().await.unwrap().json().await.unwrap()
}

async fn list_messages(inst: &TestInstance, token: &str, conv: u64) -> Value {
    let url = format!("http://{}/api/conversations/{conv}/messages", inst.domain);
    client().get(&url).bearer_auth(token).send().await.unwrap().json().await.unwrap()
}

async fn wait_for_messages(
    inst: &TestInstance,
    token: &str,
    want: &str,
    timeout: Duration,
) -> Vec<Value> {
    let deadline = Instant::now() + timeout;
    loop {
        let convs = list_conversations(inst, token).await;
        if let Some(arr) = convs["conversations"].as_array() {
            for c in arr {
                if let Some(id) = c["id"].as_u64() {
                    let msgs = list_messages(inst, token, id).await;
                    let msgs = msgs["messages"].as_array().unwrap().clone();
                    if msgs.iter().any(|m| m["body"].as_str() == Some(want)) {
                        return msgs;
                    }
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for message {want:?} on {}",
            inst.domain
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn federation_end_to_end_two_instances() {
    let db_a = reset_db("test_feditexter_a").await;
    let db_b = reset_db("test_feditexter_b").await;
    let a = start_instance(&db_a).await;
    let b = start_instance(&db_b).await;

    let (token_alice, id_alice) = register(&a, "alice").await;
    let (token_bob, id_bob) = register(&b, "bob").await;

    // well-known discovery
    let wk: Value = client()
        .get(format!("http://{}/.well-known/feditexter", a.domain))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(wk["domain"].as_str(), Some(a.domain.as_str()));
    assert_eq!(wk["public_key"].as_str().unwrap().len(), 64);

    // unsigned lookup must be rejected
    let status = client()
        .get(format!(
            "http://{}/api/federation/users/lookup?username=bob&domain={}",
            b.domain, a.domain
        ))
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // forged signature must be rejected
    let status = client()
        .get(format!(
            "http://{}/api/federation/users/lookup?username=bob&domain={}",
            b.domain, a.domain
        ))
        .header(
            "authorization",
            "Feditexter domain=\"example.com\" created=\"0\" sig=\"ffff\"",
        )
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // alice starts a conversation with bob on the other instance
    let body = create_conversation_handle(&a, &token_alice, &format!("@bob@{}", b.domain)).await;
    assert_eq!(body["kind"], json!("direct"));
    let conv_a = body["id"].as_u64().unwrap();
    let members: Vec<u64> = body["members"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["id"].as_u64().unwrap())
        .collect();
    assert!(members.contains(&id_alice));

    // alice sends a message; it must arrive on instance B
    send_message(&a, &token_alice, conv_a, "hello from alice").await;
    let msgs = wait_for_messages(&b, &token_bob, "hello from alice", Duration::from_secs(10)).await;
    assert_eq!(msgs[0]["body"], json!("hello from alice"));
    assert_ne!(msgs[0]["sender_id"].as_u64(), Some(id_bob), "sender must be alice's mirror");

    // bob replies; it must arrive on instance A
    let convs = list_conversations(&b, &token_bob).await;
    let conv_b = convs["conversations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["kind"] == json!("direct"))
        .map(|c| c["id"].as_u64().unwrap())
        .unwrap();
    send_message(&b, &token_bob, conv_b, "reply from bob").await;
    let msgs = wait_for_messages(&a, &token_alice, "reply from bob", Duration::from_secs(10)).await;
    assert_eq!(msgs[1]["body"], json!("reply from bob"));

    // mirror users are stored as remote on both sides
    let mirror_on_a: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM users WHERE username = 'bob' AND is_remote = TRUE",
    )
    .fetch_one(&a.state.pool)
    .await
    .unwrap();
    assert_eq!(mirror_on_a, 1);
    let mirror_on_b: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM users WHERE username = 'alice' AND is_remote = TRUE",
    )
    .fetch_one(&b.state.pool)
    .await
    .unwrap();
    assert_eq!(mirror_on_b, 1);
}
