use axum::http::header::AUTHORIZATION;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand_core::OsRng;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::MySqlPool;
use tracing::warn;

use crate::api::error::ApiError;
use crate::auth::User;
use crate::chat::Message;
use crate::db::AppState;

pub const INBOX_PATH: &str = "/api/federation/inbox";
pub const LOOKUP_PATH: &str = "/api/federation/users/lookup";
pub const AUTH_SCHEME: &str = "Feditexter";
const CLOCK_SKEW_SECS: i64 = 300;

#[derive(Clone)]
pub struct Federation {
    pub domain: String,
    pub public_key: [u8; 32],
    secret: SigningKey,
    pub client: reqwest::Client,
}

pub struct ServerInfo {
    pub id: u64,
    pub domain: String,
}

impl Federation {
    pub async fn init(pool: &MySqlPool, domain: &str) -> Result<Federation, sqlx::Error> {
        let row: Option<(Vec<u8>, Vec<u8>)> =
            sqlx::query_as("SELECT public_key, private_key FROM instance_meta WHERE id = 1")
                .fetch_optional(pool)
                .await?;

        let (public_key, private_key) = match row {
            Some((pk, sk)) if pk.len() == 32 && sk.len() == 32 => (pk, sk),
            _ => {
                let mut osrng = OsRng;
                let secret = SigningKey::generate(&mut osrng);
                let pk = secret.verifying_key().to_bytes().to_vec();
                let sk = secret.to_bytes().to_vec();
                sqlx::query(
                    "INSERT INTO instance_meta (id, domain, public_key, private_key) \
                     VALUES (1, ?, ?, ?) \
                     ON DUPLICATE KEY UPDATE domain = VALUES(domain), \
                     public_key = VALUES(public_key), private_key = VALUES(private_key)",
                )
                .bind(domain)
                .bind(&pk)
                .bind(&sk)
                .execute(pool)
                .await?;
                (pk, sk)
            }
        };

        sqlx::query("UPDATE instance_meta SET domain = ? WHERE id = 1")
            .bind(domain)
            .execute(pool)
            .await?;

        let mut pk = [0u8; 32];
        pk.copy_from_slice(&public_key);
        let mut sk = [0u8; 32];
        sk.copy_from_slice(&private_key);

        Ok(Federation {
            domain: domain.to_string(),
            public_key: pk,
            secret: SigningKey::from_bytes(&sk),
            client: reqwest::Client::new(),
        })
    }

    pub fn public_key_hex(&self) -> String {
        hex::encode(self.public_key)
    }

    pub fn sign_auth(&self, method: &str, path: &str, body: &[u8]) -> String {
        let created = chrono::Utc::now().timestamp().to_string();
        let input = signing_input(&created, &self.domain, method, path, body);
        let sig = self.secret.sign(input.as_bytes());
        format!(
            "{AUTH_SCHEME} domain=\"{}\" created=\"{created}\" sig=\"{}\"",
            self.domain,
            hex::encode(sig.to_bytes())
        )
    }
}

pub fn signing_input(created: &str, domain: &str, method: &str, path: &str, body: &[u8]) -> String {
    format!("{created}\n{domain}\n{method}\n{path}\n{}", sha256_hex(body))
}

pub async fn verify_request(
    pool: &MySqlPool,
    client: &reqwest::Client,
    auth_header: &str,
    method: &str,
    path: &str,
    body: &[u8],
) -> Result<ServerInfo, ApiError> {
    let (domain, created, sig_hex) = parse_auth(auth_header)
        .ok_or(ApiError::Unauthorized("malformed federation auth header"))?;

    let created_ts: i64 = created
        .parse()
        .map_err(|_| ApiError::Unauthorized("malformed created timestamp"))?;
    let now = chrono::Utc::now().timestamp();
    if (now - created_ts).abs() > CLOCK_SKEW_SECS {
        return Err(ApiError::Unauthorized("request timestamp out of range"));
    }

    let known: Option<(u64, Vec<u8>)> =
        sqlx::query_as("SELECT id, public_key FROM servers WHERE domain = ?")
            .bind(&domain)
            .fetch_optional(pool)
            .await
            .map_err(|_| ApiError::Internal("db error"))?;

    let (server_id, public_key) = match known {
        Some((id, pk)) => (id, pk),
        None => {
            let pk = discover_key(client, &domain)
                .await
                .map_err(|_| ApiError::Unauthorized("could not verify requesting server"))?;
            let inserted = sqlx::query("INSERT INTO servers (domain, public_key) VALUES (?, ?)")
                .bind(&domain)
                .bind(&pk)
                .execute(pool)
                .await
                .map_err(|_| ApiError::Internal("db error"))?;
            (inserted.last_insert_id(), pk)
        }
    };

    if public_key.len() != 32 {
        return Err(ApiError::Unauthorized("invalid server key"));
    }
    let mut pk = [0u8; 32];
    pk.copy_from_slice(&public_key);
    if !verify_signature(&pk, &created, &domain, method, path, body, &sig_hex) {
        return Err(ApiError::Unauthorized("bad signature"));
    }

    Ok(ServerInfo { id: server_id, domain })
}

pub async fn resolve_remote_user(state: &AppState, username: &str, domain: &str) -> Result<u64, ApiError> {
    let server_id = get_or_discover_server(state, domain).await?;

    let query = format!("username={username}&domain={}", state.federation.domain);
    let path = format!("{LOOKUP_PATH}?{query}");
    let auth = state.federation.sign_auth("GET", &path, b"");
    let url = format!("{}{}", base_url(domain), path);

    let resp = state
        .federation
        .client
        .get(&url)
        .header(AUTHORIZATION, auth)
        .send()
        .await
        .map_err(|_| ApiError::BadGateway("remote server unreachable"))?;

    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if status == axum::http::StatusCode::NOT_FOUND {
        return Err(ApiError::NotFound("user not found on remote server"));
    }
    if !status.is_success() {
        return Err(ApiError::BadGateway("remote lookup failed"));
    }

    let v: Value =
        serde_json::from_str(&text).map_err(|_| ApiError::BadGateway("malformed remote response"))?;
    let remote_id = v
        .get("id")
        .and_then(|i| i.as_u64())
        .ok_or(ApiError::BadGateway("malformed remote response"))?;

    get_or_create_mirror(state, server_id, remote_id, username).await
}

pub(crate) fn deliver_outbound(state: &AppState, message: &Message, sender: &User) {
    let state = state.clone();
    let message = message.clone();
    let sender = sender.clone();
    tokio::spawn(async move {
        let recipients: Vec<(u64, String)> = sqlx::query_as(
            "SELECT u.remote_id, s.domain
             FROM conversation_members cm
             JOIN users u ON u.id = cm.user_id AND u.is_remote = TRUE
             JOIN servers s ON s.id = u.server_id
             WHERE cm.conversation_id = ?",
        )
        .bind(message.conversation_id)
        .fetch_all(&state.pool)
        .await
        .unwrap_or_default();

        for (remote_id, domain) in recipients {
            if let Err(e) = deliver_to_server(&state, &message, &sender, remote_id, &domain).await {
                warn!("federation: delivery to {domain} failed: {e}");
            }
        }
    });
}

pub(crate) async fn get_or_create_mirror(
    state: &AppState,
    server_id: u64,
    remote_id: u64,
    username: &str,
) -> Result<u64, ApiError> {
    let existing: Option<(u64, String)> =
        sqlx::query_as("SELECT id, username FROM users WHERE server_id = ? AND remote_id = ?")
            .bind(server_id)
            .bind(remote_id)
            .fetch_optional(&state.pool)
            .await
            .map_err(|_| ApiError::Internal("db error"))?;

    if let Some((id, current_username)) = existing {
        if current_username != username {
            sqlx::query("UPDATE users SET username = ? WHERE id = ?")
                .bind(username)
                .bind(id)
                .execute(&state.pool)
                .await
                .map_err(|_| ApiError::Internal("db error"))?;
        }
        return Ok(id);
    }

    let inserted = sqlx::query(
        "INSERT INTO users (email, username, display_name, password_hash, is_remote, server_id, remote_id) \
         VALUES (NULL, ?, '', '!remote', TRUE, ?, ?)",
    )
    .bind(username)
    .bind(server_id)
    .bind(remote_id)
    .execute(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;

    Ok(inserted.last_insert_id())
}

async fn get_or_discover_server(state: &AppState, domain: &str) -> Result<u64, ApiError> {
    let existing: Option<(u64,)> = sqlx::query_as("SELECT id FROM servers WHERE domain = ?")
        .bind(domain)
        .fetch_optional(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    if let Some((id,)) = existing {
        return Ok(id);
    }

    let pk = discover_key(&state.federation.client, domain)
        .await
        .map_err(|_| ApiError::BadGateway("could not fetch remote server key"))?;
    let inserted = sqlx::query("INSERT INTO servers (domain, public_key) VALUES (?, ?)")
        .bind(domain)
        .bind(&pk)
        .execute(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
    Ok(inserted.last_insert_id())
}

async fn deliver_to_server(
    state: &AppState,
    message: &Message,
    sender: &User,
    to_remote_id: u64,
    domain: &str,
) -> Result<(), String> {
    let payload = json!({
        "type": "message",
        "from_server": state.federation.domain,
        "from_username": sender.username,
        "from_id": sender.id,
        "to_id": to_remote_id,
        "body": message.body,
        "sent_at": message.created_at,
    });
    let body = payload.to_string();
    let auth = state.federation.sign_auth("POST", INBOX_PATH, body.as_bytes());
    let url = format!("{}{}", base_url(domain), INBOX_PATH);

    let resp = state
        .federation
        .client
        .post(&url)
        .header(AUTHORIZATION, auth)
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await
        .map_err(|e| format!("request failed: {e}"))?;

    if !resp.status().is_success() {
        return Err(format!("remote responded {}", resp.status()));
    }
    Ok(())
}

async fn discover_key(client: &reqwest::Client, domain: &str) -> Result<Vec<u8>, ()> {
    let url = format!("{}/.well-known/feditexter", base_url(domain));
    let resp = client.get(&url).send().await.map_err(|_| ())?;
    if !resp.status().is_success() {
        return Err(());
    }
    let v: Value = resp.json().await.map_err(|_| ())?;
    let key_hex = v.get("public_key").and_then(|k| k.as_str()).ok_or(())?;
    let key = hex::decode(key_hex).map_err(|_| ())?;
    if key.len() != 32 {
        return Err(());
    }
    Ok(key)
}

fn parse_auth(header: &str) -> Option<(String, String, String)> {
    let rest = header.strip_prefix(AUTH_SCHEME)?.trim_start();
    let mut domain = None;
    let mut created = None;
    let mut sig = None;
    for part in rest.split_whitespace() {
        let (key, value) = part.split_once('=')?;
        let value = value.trim_matches('"');
        match key {
            "domain" => domain = Some(value.to_string()),
            "created" => created = Some(value.to_string()),
            "sig" => sig = Some(value.to_string()),
            _ => {}
        }
    }
    Some((domain?, created?, sig?))
}

fn verify_signature(
    public_key: &[u8; 32],
    created: &str,
    domain: &str,
    method: &str,
    path: &str,
    body: &[u8],
    sig_hex: &str,
) -> bool {
    let Ok(sig_bytes) = hex::decode(sig_hex) else {
        return false;
    };
    let Ok(sig) = Signature::from_slice(&sig_bytes) else {
        return false;
    };
    let Ok(key) = VerifyingKey::from_bytes(public_key) else {
        return false;
    };
    let input = signing_input(created, domain, method, path, body);
    key.verify(input.as_bytes(), &sig).is_ok()
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

pub fn base_url(domain: &str) -> String {
    if domain == "localhost" || domain.starts_with("localhost:") || domain.starts_with("127.0.0.1:") {
        format!("http://{domain}")
    } else {
        format!("https://{domain}")
    }
}
