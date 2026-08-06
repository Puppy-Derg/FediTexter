use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::Duration;

use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{json, Value};
use slint::{ComponentHandle, ModelRc, SharedString, VecModel, Weak};
use slint::winit_030::{EventResult, WinitWindowAccessor, winit};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;

slint::include_modules!();

// ---------------------------------------------------------------------------
// API types (mirrors the server's JSON)
// ---------------------------------------------------------------------------

#[derive(Deserialize, Clone, Debug)]
struct User {
    id: u64,
    username: String,
    #[serde(default)]
    display_name: String,
    #[serde(default = "default_email_verified")]
    email_verified: bool,
    #[serde(default)]
    avatar_url: Option<String>,
}

fn default_email_verified() -> bool {
    // Servers that predate email verification omit the field; treat them as
    // verified so users aren't stuck on a verify screen they can't complete.
    true
}

#[derive(Deserialize, Clone, Debug)]
struct AuthResponse {
    token: String,
    user: User,
}

#[derive(Deserialize, Clone, Debug)]
struct Member {
    id: u64,
    #[serde(default)]
    username: String,
    #[serde(default)]
    display_name: String,
    #[serde(default)]
    domain: String,
    #[serde(default)]
    avatar_url: Option<String>,
}

#[derive(Deserialize, Clone, Debug)]
struct Conversation {
    id: u64,
    kind: String,
    members: Vec<Member>,
}

#[derive(Deserialize, Clone, Debug)]
struct Message {
    id: u64,
    conversation_id: u64,
    sender_id: u64,
    body: String,
    created_at: String,
    #[serde(default)]
    attachment_mime: Option<String>,
    #[serde(default)]
    attachment_name: Option<String>,
    #[serde(default)]
    attachment_data: Option<String>,
}

#[derive(Deserialize)]
struct WsEvent {
    message: Message,
}

#[derive(Deserialize, Clone, Debug)]
struct Profile {
    id: u64,
    username: String,
    #[serde(default)]
    display_name: String,
    #[serde(default)]
    domain: String,
    #[serde(default)]
    avatar_url: Option<String>,
    #[serde(default)]
    is_self: bool,
    #[serde(default)]
    blocked: bool,
    #[serde(default)]
    muted: bool,
    #[serde(default)]
    blocked_by: bool,
}

/// A previously-contacted user, used for autocomplete suggestions.
#[derive(Clone)]
struct Contact {
    id: u64,
    name: String,
    handle: String,
}

/// A pending (or sent) file attachment carried with a message.
#[derive(Clone)]
struct Attachment {
    mime: String,
    name: String,
    data: String,
}

// ---------------------------------------------------------------------------
// Events flowing from background tasks to the UI
// ---------------------------------------------------------------------------

enum Event {
    LoggedIn { token: String, user: User },
    AuthFailed(String),
    Verified(User),
    UserUpdated(User),
    Conversations(Vec<Conversation>),
    ConversationCreated(Conversation),
    Messages { conversation_id: u64, messages: Vec<Message> },
    MessageSent(Message),
    WsMessage(Message),
    WsStatus(bool),
    Profile(Profile),
    ContextProfile(Profile),
    ModerationResult(Profile),
    ProfileError,
    ConversationDeleted(u64),
    UploadAvatar { server: String, token: String, data_url: String },
    AttachmentPicked(Attachment),
    Error(String),
}

// ---------------------------------------------------------------------------
// Backend: owns the tokio runtime + http client and spawns work
// ---------------------------------------------------------------------------

struct Backend {
    runtime: tokio::runtime::Runtime,
    http: reqwest::Client,
    tx: UnboundedSender<Event>,
    token: watch::Sender<Option<String>>,
}

impl Backend {
    fn new(tx: UnboundedSender<Event>, token: watch::Sender<Option<String>>) -> Self {
        let runtime = tokio::runtime::Runtime::new().expect("failed to start tokio runtime");
        let http = reqwest::Client::new();
        Backend { runtime, http, tx, token }
    }

    fn set_token(&self, token: Option<String>) {
        let _ = self.token.send(token);
    }

    fn login(&self, server: &str, email: &str, password: &str) {
        let tx = self.tx.clone();
        let http = self.http.clone();
        let server = server.to_string();
        let email = email.to_string();
        let password = password.to_string();
        self.runtime.spawn(async move {
            let url = api_url(&server, "/api/login");
            match http.post(&url).json(&json!({ "email": email, "password": password })).send().await {
                Ok(resp) if resp.status().is_success() => {
                    match resp.json::<AuthResponse>().await {
                        Ok(r) => {
                            let _ = tx.send(Event::LoggedIn { token: r.token, user: r.user });
                        }
                        Err(_) => {
                            let _ = tx.send(Event::AuthFailed("malformed server response".into()));
                        }
                    }
                }
                Ok(resp) => {
                    let msg = error_message(resp).await;
                    let _ = tx.send(Event::AuthFailed(msg));
                }
                Err(e) => {
                    let _ = tx.send(Event::AuthFailed(format!("{e}")));
                }
            }
        });
    }

    fn register(&self, server: &str, email: &str, username: &str, password: &str) {
        let tx = self.tx.clone();
        let http = self.http.clone();
        let server = server.to_string();
        let email = email.to_string();
        let username = username.to_string();
        let password = password.to_string();
        self.runtime.spawn(async move {
            let url = api_url(&server, "/api/register");
            let body = json!({ "email": email, "username": username, "password": password });
            match http.post(&url).json(&body).send().await {
                Ok(resp) if resp.status().is_success() => {
                    match resp.json::<AuthResponse>().await {
                        Ok(r) => {
                            let _ = tx.send(Event::LoggedIn { token: r.token, user: r.user });
                        }
                        Err(_) => {
                            let _ = tx.send(Event::AuthFailed("malformed server response".into()));
                        }
                    }
                }
                Ok(resp) => {
                    let msg = error_message(resp).await;
                    let _ = tx.send(Event::AuthFailed(msg));
                }
                Err(e) => {
                    let _ = tx.send(Event::AuthFailed(format!("{e}")));
                }
            }
        });
    }

    fn verify(&self, server: &str, token: &str, code: &str) {        let tx = self.tx.clone();
        let http = self.http.clone();
        let server = server.to_string();
        let token = token.to_string();
        let code = code.to_string();
        self.runtime.spawn(async move {
            let url = api_url(&server, "/api/verify");
            let resp = match http
                .post(&url)
                .bearer_auth(&token)
                .json(&json!({ "code": code }))
                .send()
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.send(Event::Error(format!("{e}")));
                    return;
                }
            };
            if !resp.status().is_success() {
                let _ = tx.send(Event::Error(error_message(resp).await));
                return;
            }
            let v: Value = match resp.json().await {
                Ok(v) => v,
                Err(_) => {
                    let _ = tx.send(Event::Error("malformed server response".into()));
                    return;
                }
            };
            if let Ok(u) = serde_json::from_value::<User>(v.get("user").cloned().unwrap_or(Value::Null)) {
                let _ = tx.send(Event::Verified(u));
            }
        });
    }

    fn resend_verification(&self, server: &str, token: &str) {
        let tx = self.tx.clone();
        let http = self.http.clone();
        let server = server.to_string();
        let token = token.to_string();
        self.runtime.spawn(async move {
            let url = api_url(&server, "/api/verify/resend");
            let resp = match http.post(&url).bearer_auth(&token).send().await {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.send(Event::Error(format!("{e}")));
                    return;
                }
            };
            if !resp.status().is_success() {
                let _ = tx.send(Event::Error(error_message(resp).await));
                return;
            }
            let v: Value = match resp.json().await {
                Ok(v) => v,
                Err(_) => {
                    let _ = tx.send(Event::Error("malformed server response".into()));
                    return;
                }
            };
            if let Ok(u) = serde_json::from_value::<User>(v.get("user").cloned().unwrap_or(Value::Null)) {
                let _ = tx.send(Event::Verified(u));
            }
        });
    }

    fn update_display_name(&self, server: &str, token: &str, display_name: &str) {
        let tx = self.tx.clone();
        let http = self.http.clone();
        let server = server.to_string();
        let token = token.to_string();
        let display_name = display_name.trim().to_string();
        self.runtime.spawn(async move {
            let url = api_url(&server, "/api/me");
            let resp = match http
                .patch(&url)
                .bearer_auth(&token)
                .json(&json!({ "display_name": display_name }))
                .send()
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.send(Event::Error(format!("{e}")));
                    return;
                }
            };
            if !resp.status().is_success() {
                let _ = tx.send(Event::Error(error_message(resp).await));
                return;
            }
            let v: Value = match resp.json().await {
                Ok(v) => v,
                Err(_) => {
                    let _ = tx.send(Event::Error("malformed server response".into()));
                    return;
                }
            };
            if let Ok(u) = serde_json::from_value::<User>(v.get("user").cloned().unwrap_or(Value::Null)) {
                let _ = tx.send(Event::UserUpdated(u));
            }
        });
    }

    fn set_avatar(&self, server: &str, token: &str, data_url: String) {
        let tx = self.tx.clone();
        let http = self.http.clone();
        let server = server.to_string();
        let token = token.to_string();
        self.runtime.spawn(async move {
            let url = api_url(&server, "/api/me/avatar");
            let resp = match http
                .post(&url)
                .bearer_auth(&token)
                .json(&json!({ "avatar": data_url }))
                .send()
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.send(Event::Error(format!("{e}")));
                    return;
                }
            };
            if !resp.status().is_success() {
                let _ = tx.send(Event::Error(error_message(resp).await));
                return;
            }
            let v: Value = match resp.json().await {
                Ok(v) => v,
                Err(_) => {
                    let _ = tx.send(Event::Error("malformed server response".into()));
                    return;
                }
            };
            if let Ok(u) = serde_json::from_value::<User>(v.get("user").cloned().unwrap_or(Value::Null)) {
                let _ = tx.send(Event::UserUpdated(u));
            }
        });
    }

    fn refresh_conversations(&self, server: &str, token: &str) {        let tx = self.tx.clone();
        let http = self.http.clone();
        let server = server.to_string();
        let token = token.to_string();
        self.runtime.spawn(async move {
            let url = api_url(&server, "/api/conversations");
            let resp = match http.get(&url).bearer_auth(&token).send().await {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.send(Event::Error(format!("{e}")));
                    return;
                }
            };
            if !resp.status().is_success() {
                let _ = tx.send(Event::Error(error_message(resp).await));
                return;
            }
            let v: Value = match resp.json().await {
                Ok(v) => v,
                Err(_) => {
                    let _ = tx.send(Event::Error("malformed server response".into()));
                    return;
                }
            };
            let list: Vec<Conversation> =
                serde_json::from_value(v.get("conversations").cloned().unwrap_or(Value::Null))
                    .unwrap_or_default();
            let _ = tx.send(Event::Conversations(list));
        });
    }

    fn refresh_messages(&self, server: &str, token: &str, conversation_id: u64) {
        let tx = self.tx.clone();
        let http = self.http.clone();
        let server = server.to_string();
        let token = token.to_string();
        self.runtime.spawn(async move {
            let url = api_url(&server, &format!("/api/conversations/{conversation_id}/messages"));
            let resp = match http.get(&url).bearer_auth(&token).send().await {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.send(Event::Error(format!("{e}")));
                    return;
                }
            };
            if !resp.status().is_success() {
                let _ = tx.send(Event::Error(error_message(resp).await));
                return;
            }
            let v: Value = match resp.json().await {
                Ok(v) => v,
                Err(_) => {
                    let _ = tx.send(Event::Error("malformed server response".into()));
                    return;
                }
            };
            let list: Vec<Message> =
                serde_json::from_value(v.get("messages").cloned().unwrap_or(Value::Null))
                    .unwrap_or_default();
            let _ = tx.send(Event::Messages { conversation_id, messages: list });
        });
    }

    fn create_conversation(&self, server: &str, token: &str, member_ids: Vec<u64>, handles: Vec<String>) {
        let tx = self.tx.clone();
        let http = self.http.clone();
        let server = server.to_string();
        let token = token.to_string();
        self.runtime.spawn(async move {
            let url = api_url(&server, "/api/conversations");
            let body = json!({ "member_ids": member_ids, "handles": handles });
            let resp = match http.post(&url).bearer_auth(&token).json(&body).send().await {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.send(Event::Error(format!("{e}")));
                    return;
                }
            };
            if !resp.status().is_success() {
                let _ = tx.send(Event::Error(error_message(resp).await));
                return;
            }
            match resp.json::<Conversation>().await {
                Ok(c) => {
                    let _ = tx.send(Event::ConversationCreated(c));
                }
                Err(_) => {
                    let _ = tx.send(Event::Error("malformed server response".into()));
                }
            }
        });
    }

    fn send_message(
        &self,
        server: &str,
        token: &str,
        conversation_id: u64,
        body: String,
        attachment: Option<Attachment>,
    ) {
        let tx = self.tx.clone();
        let http = self.http.clone();
        let server = server.to_string();
        let token = token.to_string();
        self.runtime.spawn(async move {
            let url = api_url(&server, &format!("/api/conversations/{conversation_id}/messages"));
            let mut payload = json!({ "body": body });
            if let Some(att) = attachment {
                payload["attachment_mime"] = json!(att.mime);
                payload["attachment_name"] = json!(att.name);
                payload["attachment_data"] = json!(att.data);
            }
            let resp = match http.post(&url).bearer_auth(&token).json(&payload).send().await {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.send(Event::Error(format!("{e}")));
                    return;
                }
            };
            if !resp.status().is_success() {
                let _ = tx.send(Event::Error(error_message(resp).await));
                return;
            }
            match resp.json::<Value>().await {
                Ok(v) => {
                    if let Some(m) = serde_json::from_value::<Message>(v.get("message").cloned().unwrap_or(Value::Null)).ok() {
                        let _ = tx.send(Event::MessageSent(m));
                    }
                }
                Err(_) => {
                    let _ = tx.send(Event::Error("malformed server response".into()));
                }
            }
        });
    }

    fn fetch_profile(&self, server: &str, token: &str, user_id: u64) {
        let tx = self.tx.clone();
        let http = self.http.clone();
        let server = server.to_string();
        let token = token.to_string();
        self.runtime.spawn(async move {
            let url = api_url(&server, &format!("/api/users/{user_id}"));
            let resp = match http.get(&url).bearer_auth(&token).send().await {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.send(Event::Error(format!("{e}")));
                    return;
                }
            };
            if !resp.status().is_success() {
                let _ = tx.send(Event::ProfileError);
                return;
            }
            let v: Value = match resp.json().await {
                Ok(v) => v,
                Err(_) => {
                    let _ = tx.send(Event::ProfileError);
                    return;
                }
            };
            match serde_json::from_value::<Profile>(v.get("user").cloned().unwrap_or(Value::Null)) {
                Ok(p) => {
                    let _ = tx.send(Event::Profile(p));
                }
                Err(_) => {
                    let _ = tx.send(Event::ProfileError);
                }
            }
        });
    }

    fn fetch_profile_context(&self, server: &str, token: &str, user_id: u64) {
        let tx = self.tx.clone();
        let http = self.http.clone();
        let server = server.to_string();
        let token = token.to_string();
        self.runtime.spawn(async move {
            let url = api_url(&server, &format!("/api/users/{user_id}"));
            let resp = match http.get(&url).bearer_auth(&token).send().await {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.send(Event::Error(format!("{e}")));
                    return;
                }
            };
            if !resp.status().is_success() {
                let _ = tx.send(Event::ProfileError);
                return;
            }
            let v: Value = match resp.json().await {
                Ok(v) => v,
                Err(_) => {
                    let _ = tx.send(Event::ProfileError);
                    return;
                }
            };
            match serde_json::from_value::<Profile>(v.get("user").cloned().unwrap_or(Value::Null)) {
                Ok(p) => {
                    let _ = tx.send(Event::ContextProfile(p));
                }
                Err(_) => {
                    let _ = tx.send(Event::ProfileError);
                }
            }
        });
    }

    /// POST /api/users/{id}/{action} where action is block|unblock|mute|unmute.
    /// The server replies with the updated profile.
    fn moderation(&self, server: &str, token: &str, user_id: u64, action: &str) {
        let tx = self.tx.clone();
        let http = self.http.clone();
        let server = server.to_string();
        let token = token.to_string();
        let action = action.to_string();
        self.runtime.spawn(async move {
            let url = api_url(&server, &format!("/api/users/{user_id}/{action}"));
            let resp = match http.post(&url).bearer_auth(&token).send().await {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.send(Event::Error(format!("{e}")));
                    return;
                }
            };
            if !resp.status().is_success() {
                let _ = tx.send(Event::Error(error_message(resp).await));
                return;
            }
            let v: Value = match resp.json().await {
                Ok(v) => v,
                Err(_) => {
                    let _ = tx.send(Event::Error("malformed server response".into()));
                    return;
                }
            };
            match serde_json::from_value::<Profile>(v.get("user").cloned().unwrap_or(Value::Null)) {
                Ok(p) => {
                    let _ = tx.send(Event::ModerationResult(p));
                }
                Err(_) => {
                    let _ = tx.send(Event::Error("malformed server response".into()));
                }
            }
        });
    }

    fn delete_conversation(&self, server: &str, token: &str, conversation_id: u64) {
        let tx = self.tx.clone();
        let http = self.http.clone();
        let server = server.to_string();
        let token = token.to_string();
        self.runtime.spawn(async move {
            let url = api_url(&server, &format!("/api/conversations/{conversation_id}"));
            let resp = match http.delete(&url).bearer_auth(&token).send().await {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.send(Event::Error(format!("{e}")));
                    return;
                }
            };
            if !resp.status().is_success() {
                let _ = tx.send(Event::Error(error_message(resp).await));
                return;
            }
            let _ = tx.send(Event::ConversationDeleted(conversation_id));
        });
    }
}

fn api_url(server: &str, path: &str) -> String {
    format!("{}/{}", server.trim_end_matches('/'), path.trim_start_matches('/'))
}

/// Fill in the scheme and default port for a server string entered by the user.
///
/// * `localhost` / `127.0.0.1` -> `http://localhost:3000` (local dev server)
/// * `dergdungeon.com.au` -> `https://dergdungeon.com.au` (standard https port)
/// * Anything already complete (`http://host:1234`) is left unchanged.
fn normalize_server(input: &str) -> String {
    let s = input.trim();
    if s.is_empty() {
        return "http://localhost:3000".to_string();
    }
    let (scheme, rest) = match s.find("://") {
        Some(i) => (s[..i].to_lowercase(), &s[i + 3..]),
        None => {
            let host = s.split('/').next().unwrap_or(s);
            let base = host.split(':').next().unwrap_or(host);
            let scheme = if is_localhost(base) { "http" } else { "https" };
            (scheme.to_string(), s)
        }
    };
    let host = rest.split('/').next().unwrap_or(rest);
    let base = host.split(':').next().unwrap_or(host);
    let is_local = is_localhost(base);
    let has_port = host.contains(':') && !host.starts_with('[');
    if has_port {
        format!("{scheme}://{host}")
    } else if is_local {
        format!("{scheme}://{host}:3000")
    } else {
        format!("{scheme}://{host}")
    }
}

fn is_localhost(host: &str) -> bool {
    host == "localhost" || host == "127.0.0.1" || host == "::1"
}

fn server_state_path() -> std::path::PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    std::path::Path::new(&home).join(".feditexter_server")
}

fn load_saved_server() -> Option<String> {
    load_settings().map(|s| s.server)
}

fn load_settings() -> Option<LocalSettings> {
    let raw = std::fs::read_to_string(server_state_path()).ok()?;
    let raw = raw.trim();
    if raw.starts_with('{') {
        serde_json::from_str::<LocalSettings>(raw).ok()
    } else if !raw.is_empty() {
        // Legacy: the file was just a server URL.
        Some(LocalSettings { server: raw.to_string(), accent: None })
    } else {
        None
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct LocalSettings {
    server: String,
    #[serde(default)]
    accent: Option<String>,
}

fn save_settings(settings: &LocalSettings) {
    if let Ok(json) = serde_json::to_string(settings)
        && let Err(e) = std::fs::write(server_state_path(), json)
    {
        eprintln!("[error] failed to save settings: {e}");
    }
}

fn save_server(server: &str) {
    let mut settings = load_settings().unwrap_or(LocalSettings { server: String::new(), accent: None });
    settings.server = server.trim().to_string();
    save_settings(&settings);
}

fn color_from_hex(hex: &str) -> Option<slint::Color> {
    let h = hex.trim_start_matches('#');
    if h.len() != 6 {
        return None;
    }
    let r = u8::from_str_radix(&h[0..2], 16).ok()?;
    let g = u8::from_str_radix(&h[2..4], 16).ok()?;
    let b = u8::from_str_radix(&h[4..6], 16).ok()?;
    Some(slint::Color::from_rgb_u8(r, g, b))
}

fn accent_to_hex(c: slint::Color) -> String {
    format!("#{:02X}{:02X}{:02X}", c.red(), c.green(), c.blue())
}

fn mime_from_path(path: &std::path::Path) -> &'static str {
    match path.extension().and_then(|e| e.to_str()).unwrap_or("").to_lowercase().as_str() {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "svg" => "image/svg+xml",
        "mp4" => "video/mp4",
        "webm" => "video/webm",
        "mp3" => "audio/mpeg",
        "wav" => "audio/wav",
        "pdf" => "application/pdf",
        "txt" | "md" => "text/plain",
        "zip" => "application/zip",
        "json" => "application/json",
        _ => "application/octet-stream",
    }
}

fn ws_url(server: &str) -> String {
    let base = server.trim_end_matches('/');
    if let Some(rest) = base.strip_prefix("https://") {
        format!("wss://{rest}/api/ws")
    } else if let Some(rest) = base.strip_prefix("http://") {
        format!("ws://{rest}/api/ws")
    } else {
        format!("ws://{base}/api/ws")
    }
}

async fn error_message(resp: reqwest::Response) -> String {
    let status = resp.status();
    match resp.json::<Value>().await {
        Ok(v) => v
            .get("error")
            .and_then(|e| e.as_str())
            .map(String::from)
            .unwrap_or_else(|| format!("HTTP {status}")),
        Err(_) => format!("HTTP {status}"),
    }
}

// ---------------------------------------------------------------------------
// WebSocket background task: live message push with auto-reconnect
// ---------------------------------------------------------------------------

fn spawn_ws(
    runtime: &tokio::runtime::Runtime,
    mut server_rx: watch::Receiver<String>,
    mut token_rx: watch::Receiver<Option<String>>,
    tx: UnboundedSender<Event>,
) {
    runtime.spawn(async move {
        loop {
            let token = loop {
                let current = token_rx.borrow().clone();
                match current {
                    Some(t) => break t,
                    None => {
                        if token_rx.changed().await.is_err() {
                            return;
                        }
                    }
                }
            };

            let server = server_rx.borrow().clone();
            let url = ws_url(&server);
            let mut request = match url.clone().into_client_request() {
                Ok(r) => r,
                Err(_) => {
                    let _ = tx.send(Event::Error("invalid websocket url".into()));
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    continue;
                }
            };
            if let Ok(header) = HeaderValue::from_str(&format!("Bearer {token}")) {
                request.headers_mut().insert("authorization", header);
            }

            match tokio_tungstenite::connect_async(request).await {
                Ok((ws, _)) => {
                    eprintln!("[ws] connected to {url}");
                    let _ = tx.send(Event::WsStatus(true));
                    let (_sink, mut stream) = ws.split();
                    loop {
                        tokio::select! {
                            changed = token_rx.changed() => {
                                if changed.is_err() {
                                    return;
                                }
                                break;
                            }
                            changed = server_rx.changed() => {
                                if changed.is_err() {
                                    return;
                                }
                                break;
                            }
                            msg = stream.next() => {
                                match msg {
                                    Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                                        if let Ok(ev) = serde_json::from_str::<WsEvent>(&text) {
                                            let _ = tx.send(Event::WsMessage(ev.message));
                                        }
                                    }
                                    Some(Ok(_)) => {}
                                    _ => break,
                                }
                            }
                        }
                    }
                    eprintln!("[ws] disconnected from {url}");
                    let _ = tx.send(Event::WsStatus(false));
                }
                Err(e) => {
                    eprintln!("[ws] connect error to {url}: {e}");
                    let _ = tx.send(Event::Error(format!("websocket error: {e}")));
                }
            }

            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    });
}

// ---------------------------------------------------------------------------
// Shared state bridging the UI and the event drain loop
// ---------------------------------------------------------------------------

struct Shared {
    backend: Rc<Backend>,
    ui: Weak<MainWindow>,
    rx: Rc<RefCell<UnboundedReceiver<Event>>>,
    token: Rc<RefCell<Option<String>>>,
    self_id: Rc<Cell<Option<u64>>>,
    conversations: Rc<RefCell<Vec<Conversation>>>,
    messages: Rc<RefCell<Vec<Message>>>,
    selected: Rc<Cell<i32>>,
    contacts: Rc<RefCell<Vec<Contact>>>,
    hidden: Rc<RefCell<Vec<u64>>>,
    unread: Rc<RefCell<std::collections::HashMap<u64, u32>>>,
    avatar_cache: Rc<RefCell<std::collections::HashMap<u64, slint::Image>>>,
    pending_attach: Rc<RefCell<Option<Attachment>>>,
}

impl Shared {
    fn ui(&self) -> MainWindow {
        self.ui.upgrade().expect("window is gone")
    }

    fn server(&self) -> String {
        normalize_server(&self.ui().get_server_input().to_string())
    }
}

fn empty_model<T>() -> ModelRc<T>
where
    T: Clone + 'static,
{
    ModelRc::new(VecModel::from(Vec::<T>::new()))
}

fn initials(name: &str) -> String {
    let mut words = name.split_whitespace().filter(|w| !w.is_empty());
    let mut out = String::new();
    if let Some(c) = words.next().and_then(|w| w.chars().next()) {
        out.push(c.to_ascii_uppercase());
    }
    if let Some(c) = words.next().and_then(|w| w.chars().next()) {
        out.push(c.to_ascii_uppercase());
    }
    if out.is_empty() {
        out.push('?');
    }
    out
}

fn avatar_color(name: &str) -> slint::Color {
    const PALETTE: [(u8, u8, u8); 8] = [
        (0xE5, 0x63, 0x7C),
        (0xF2, 0x9A, 0x2E),
        (0xDF, 0xB6, 0x2E),
        (0x7A, 0xB8, 0x54),
        (0x2A, 0xB0, 0x9A),
        (0x4F, 0x9D, 0xE9),
        (0x9A, 0x77, 0xE0),
        (0xE0, 0x7D, 0xC9),
    ];
    let hash: u32 = name.bytes().fold(0u32, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u32));
    let (r, g, b) = PALETTE[(hash as usize) % PALETTE.len()];
    slint::Color::from_rgb_u8(r, g, b)
}

/// Decode a `data:image/...;base64,...` avatar into a Slint image.
fn load_avatar_image(data_url: &str) -> Option<slint::Image> {
    let b64 = data_url.split_once(";base64,")?.1;
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD.decode(b64).ok()?;
    let img = image::load_from_memory(&bytes).ok()?;
    let rgba = img.to_rgba8();
    let (w, h) = rgba.dimensions();
    let buffer = slint::SharedPixelBuffer::<slint::Rgba8Pixel>::clone_from_slice(&rgba, w, h);
    Some(slint::Image::from_rgba8(buffer))
}

/// Return a cached (or freshly decoded) avatar image for a user.
fn avatar_image_for(sh: &Shared, user_id: u64, avatar_url: &Option<String>) -> slint::Image {
    if let Some(img) = sh.avatar_cache.borrow().get(&user_id) {
        return img.clone();
    }
    let img = avatar_url
        .as_deref()
        .and_then(load_avatar_image)
        .unwrap_or_default();
    sh.avatar_cache.borrow_mut().insert(user_id, img.clone());
    img
}

/// Turn a profile URL (`https://domain/@user`, `domain/@user`) into an @handle.
fn url_to_handle(s: &str) -> Option<String> {
    let rest = s.split("://").nth(1).unwrap_or(s);
    let idx = rest.find("/@")?;
    let domain = rest[..idx].split('/').next()?;
    let username = rest[idx + 2..].split(['/', '#', '?']).next()?.trim();
    if username.is_empty() || domain.is_empty() {
        return None;
    }
    Some(format!("@{username}@{domain}"))
}

/// Accepts a numeric user id, an `@username@domain` handle, or a profile URL.
fn conversation_target(input: &str) -> Result<(Option<u64>, Option<String>), String> {
    let s = input.trim();
    if s.is_empty() {
        return Err("enter a user id, @handle, or profile URL".into());
    }
    if let Ok(id) = s.parse::<u64>() {
        return Ok((Some(id), None));
    }
    if s.starts_with('@') && s.contains('@') {
        return Ok((None, Some(s.to_string())));
    }
    if let Some(handle) = url_to_handle(s) {
        return Ok((None, Some(handle)));
    }
    Err("enter a numeric user id, @handle, or profile URL".into())
}

/// Parse a comma/whitespace separated list of targets into user ids and handles.
fn parse_targets(input: &str) -> Result<(Vec<u64>, Vec<String>), String> {
    let mut ids = Vec::new();
    let mut handles = Vec::new();
    for token in input.split([',', ' ', '\t', '\n']).map(str::trim).filter(|s| !s.is_empty()) {
        match conversation_target(token) {
            Ok((Some(id), None)) => ids.push(id),
            Ok((None, Some(h))) => handles.push(h),
            _ => return Err(format!("could not understand '{token}'")),
        }
    }
    if ids.is_empty() && handles.is_empty() {
        return Err("enter at least one user id, @handle, or profile URL".into());
    }
    Ok((ids, handles))
}

fn set_error(sh: &Shared, msg: &str) {
    eprintln!("[error] {msg}");
    sh.ui().set_error_message(msg.into());
}

/// Scroll the message list to the bottom. The tick triggers a Slint-side
/// scroll; the delayed second tick re-applies it after the list has relaid out
/// so `viewport-height` reflects the new messages.
fn scroll_to_bottom(sh: &Shared) {
    let ui = sh.ui();
    ui.set_msg_scroll_tick(ui.get_msg_scroll_tick() + 1);
    let weak = sh.ui.clone();
    slint::Timer::single_shot(Duration::from_millis(60), move || {
        if let Some(ui) = weak.upgrade() {
            ui.set_msg_scroll_tick(ui.get_msg_scroll_tick() + 1);
        }
    });
}

/// Push a server profile into the profile card and keep the context menu's
/// block/mute state in sync.
fn apply_profile(sh: &Shared, p: &Profile) {
    let display_name = if p.display_name.is_empty() {
        p.username.clone()
    } else {
        p.display_name.clone()
    };
    let handle = if p.domain.is_empty() {
        format!("@{}", p.username)
    } else {
        format!("@{}@{}", p.username, p.domain)
    };
    let ui = sh.ui();
    ui.set_profile(UiProfile {
        id: p.id as i32,
        username: p.username.clone().into(),
        display_name: display_name.clone().into(),
        handle: handle.into(),
        avatar_text: initials(&display_name).into(),
        avatar_color: avatar_color(&display_name),
        avatar_image: avatar_image_for(sh, p.id, &p.avatar_url),
        is_self: p.is_self,
        blocked: p.blocked,
        muted: p.muted,
        blocked_by: p.blocked_by,
    });
    ui.set_context_user_id(p.id as i32);
    ui.set_context_blocked(p.blocked);
    ui.set_context_muted(p.muted);
}

fn conversation_title(c: &Conversation, self_id: Option<u64>) -> String {
    let others: Vec<&Member> = c.members.iter().filter(|m| Some(m.id) != self_id).collect();
    if c.kind == "group" || others.len() != 1 {
        let names: Vec<String> = others
            .iter()
            .map(|m| {
                if m.username.is_empty() {
                    format!("user {}", m.id)
                } else if m.display_name.is_empty() {
                    m.username.clone()
                } else {
                    m.display_name.clone()
                }
            })
            .collect();
        if names.is_empty() {
            return format!("Conversation {}", c.id);
        }
        format!("Group: {}", names.join(", "))
    } else {
        let m = others[0];
        if m.username.is_empty() {
            format!("user {}", m.id)
        } else if m.display_name.is_empty() {
            m.username.clone()
        } else {
            m.display_name.clone()
        }
    }
}

fn sender_name(members: &[Member], sender_id: u64, self_id: Option<u64>) -> String {
    if self_id == Some(sender_id) {
        return "You".to_string();
    }
    for m in members {
        if m.id == sender_id {
            if m.username.is_empty() {
                return format!("user {sender_id}");
            }
            if m.display_name.is_empty() {
                return format!("@{}", m.username);
            }
            return format!("@{} ({})", m.username, m.display_name);
        }
    }
    format!("user {sender_id}")
}

/// Convert a server UTC timestamp into local time. Shows HH:MM for today,
/// and date + time otherwise.
fn format_local_time(created_at: &str) -> String {
    let naive = chrono::NaiveDateTime::parse_from_str(created_at, "%Y-%m-%dT%H:%M:%S")
        .ok()
        .or_else(|| chrono::NaiveDateTime::parse_from_str(created_at, "%Y-%m-%d %H:%M:%S").ok());
    let Some(naive) = naive else {
        return created_at.to_string();
    };
    let utc = chrono::TimeZone::from_utc_datetime(&chrono::Utc, &naive);
    let local = utc.with_timezone(&chrono::Local);
    let now = chrono::Local::now();
    if local.date_naive() == now.date_naive() {
        local.format("%H:%M").to_string()
    } else {
        local.format("%d %b %H:%M").to_string()
    }
}

fn refresh_conversations_ui(sh: &Shared) {
    let self_id = sh.self_id.get();
    let hidden = sh.hidden.borrow().clone();
    let mut contacts: Vec<Contact> = Vec::new();
    let ui_convs: Vec<UiConversation> = sh
        .conversations
        .borrow()
        .iter()
        .filter(|c| !hidden.contains(&c.id))
        .map(|c| {
            let title = conversation_title(c, self_id);
            for m in c.members.iter().filter(|m| Some(m.id) != self_id) {
                if m.username.is_empty() {
                    continue;
                }
                if !contacts.iter().any(|x| x.id == m.id) {
                    let name = if m.display_name.is_empty() {
                        m.username.clone()
                    } else {
                        m.display_name.clone()
                    };
                    let domain = if m.domain.is_empty() {
                        String::from("localhost")
                    } else {
                        m.domain.clone()
                    };
                    contacts.push(Contact {
                        id: m.id,
                        name,
                        handle: format!("@{}@{}", m.username, domain),
                    });
                }
            }
            let avatar_url = c.members.iter().find(|m| Some(m.id) != self_id).and_then(|m| m.avatar_url.clone());
            let other_id = c
                .members
                .iter()
                .find(|m| Some(m.id) != self_id)
                .map(|m| m.id)
                .unwrap_or(0);
            UiConversation {
                id: c.id as i32,
                title: title.clone().into(),
                avatar_text: initials(&title).into(),
                avatar_color: avatar_color(&title),
                avatar_image: avatar_image_for(sh, other_id, &avatar_url),
                unread: sh.unread.borrow().get(&c.id).copied().unwrap_or(0) > 0,
                other_user_id: c
                    .members
                    .iter()
                    .find(|m| Some(m.id) != self_id)
                    .map(|m| m.id as i32)
                    .unwrap_or(-1),
            }
        })
        .collect();
    *sh.contacts.borrow_mut() = contacts;
    sh.ui().set_conversations(ModelRc::new(VecModel::from(ui_convs)));
    refresh_suggestions(sh);
}

fn refresh_suggestions(sh: &Shared) {
    let input = sh.ui().get_new_conversation_input().to_string();
    let fragments: Vec<&str> = input.split(',').map(str::trim).collect();
    let already: Vec<String> = fragments
        .iter()
        .filter(|s| {
            !s.is_empty()
                && (s.parse::<u64>().is_ok() || (s.starts_with('@') && s.matches('@').count() >= 2))
        })
        .map(|s| s.to_string())
        .collect();
    let last = fragments.last().map(|s| s.to_string()).unwrap_or_default();
    let last_complete = last.parse::<u64>().is_ok()
        || (last.starts_with('@') && last.matches('@').count() >= 2);
    let query = if last_complete { String::new() } else { last.to_lowercase() };

    let matches: Vec<UiContact> = sh
        .contacts
        .borrow()
        .iter()
        .filter(|c| {
            !already.iter().any(|a| *a == c.handle || a.parse::<u64>().ok() == Some(c.id))
        })
        .filter(|c| {
            query.is_empty()
                || c.name.to_lowercase().contains(&query)
                || c.handle.to_lowercase().contains(&query)
        })
        .take(6)
        .map(|c| UiContact {
            id: c.id as i32,
            name: c.name.clone().into(),
            handle: c.handle.clone().into(),
            avatar_text: initials(&c.name).into(),
            avatar_color: avatar_color(&c.name),
            avatar_image: avatar_image_for(sh, c.id, &None),
        })
        .collect();
    sh.ui().set_suggestions(ModelRc::new(VecModel::from(matches)));
}

fn refresh_messages_ui(sh: &Shared) {
    let self_id = sh.self_id.get();
    let selected = sh.selected.get();
    let members: Vec<Member> = sh
        .conversations
        .borrow()
        .iter()
        .find(|c| c.id == selected as u64)
        .map(|c| c.members.clone())
        .unwrap_or_default();
    let ui_msgs: Vec<UiMessage> = sh
        .messages
        .borrow()
        .iter()
        .map(|m| {
            let sender = sender_name(&members, m.sender_id, self_id);
            let avatar_name = if self_id == Some(m.sender_id) {
                sh.ui().get_user_name().to_string()
            } else {
                members
                    .iter()
                    .find(|x| x.id == m.sender_id)
                    .map(|x| {
                        if x.display_name.is_empty() {
                            x.username.clone()
                        } else {
                            x.display_name.clone()
                        }
                    })
                    .unwrap_or_else(|| format!("user {}", m.sender_id))
            };
            let sender_avatar_url = members
                .iter()
                .find(|x| x.id == m.sender_id)
                .and_then(|x| x.avatar_url.clone());
            let local_time = format_local_time(&m.created_at);
            let attachment_image = if m.attachment_data.is_some()
                && m.attachment_mime.as_deref().unwrap_or("").starts_with("image/")
            {
                m.attachment_data.as_deref().and_then(load_avatar_image).unwrap_or_default()
            } else {
                slint::Image::default()
            };
            UiMessage {
                id: m.id as i32,
                sender: sender.into(),
                body: m.body.clone().into(),
                created_at: local_time.into(),
                is_self: self_id == Some(m.sender_id),
                sender_id: m.sender_id as i32,
                avatar_text: initials(&avatar_name).into(),
                avatar_color: avatar_color(&avatar_name),
                avatar_image: avatar_image_for(sh, m.sender_id, &sender_avatar_url),
                attachment_image,
                attachment_name: m.attachment_name.clone().unwrap_or_default().into(),
                attachment_mime: m.attachment_mime.clone().unwrap_or_default().into(),
            }
        })
        .collect();
    sh.ui().set_messages(ModelRc::new(VecModel::from(ui_msgs)));
}

fn merge_conversation(sh: &Shared, c: Conversation) {
    let mut convs = sh.conversations.borrow_mut();
    match convs.iter_mut().find(|x| x.id == c.id) {
        Some(existing) => *existing = c,
        None => convs.push(c),
    }
    convs.sort_by_key(|c| c.id);
}

fn merge_message(sh: &Shared, m: Message) {
    if sh.selected.get() != m.conversation_id as i32 {
        return;
    }
    let mut msgs = sh.messages.borrow_mut();
    if !msgs.iter().any(|x| x.id == m.id) {
        msgs.push(m);
        msgs.sort_by_key(|m| m.id);
    }
}

fn handle_event(sh: &Shared, ev: Event) {
    match ev {
        Event::LoggedIn { token, user } => {
            save_server(&sh.server());
            sh.token.replace(Some(token.clone()));
            sh.self_id.replace(Some(user.id));
            sh.backend.set_token(Some(token.clone()));
            let ui = sh.ui();
            ui.set_logged_in(true);
            ui.set_user_name(user.username.clone().into());
            ui.set_display_name_input(user.display_name.clone().into());
            ui.set_needs_verify(!user.email_verified);
            ui.set_error_message(SharedString::default());
            ui.set_selected_conversation(-1);
            ui.set_profile_open(false);
            ui.set_context_open(false);
            ui.set_confirm_delete_open(false);
            if let Some(url) = &user.avatar_url
                && let Some(img) = load_avatar_image(url)
            {
                sh.avatar_cache.borrow_mut().insert(user.id, img.clone());
                ui.set_my_avatar(img);
            }
            sh.conversations.borrow_mut().clear();
            sh.messages.borrow_mut().clear();
            sh.hidden.borrow_mut().clear();
            sh.selected.replace(-1);
            refresh_conversations_ui(sh);
            refresh_messages_ui(sh);
            sh.backend.refresh_conversations(&sh.server(), &token);
        }
        Event::AuthFailed(m) => set_error(sh, &m),
        Event::Verified(u) => {
            if u.email_verified {
                let ui = sh.ui();
                ui.set_needs_verify(false);
                ui.set_error_message(SharedString::default());
                ui.set_user_name(u.username.clone().into());
            }
        }
        Event::UserUpdated(u) => {
            let ui = sh.ui();
            ui.set_display_name_input(u.display_name.clone().into());
            ui.set_error_message(SharedString::default());
            if let Some(url) = &u.avatar_url {
                if let Some(img) = load_avatar_image(url) {
                    sh.avatar_cache.borrow_mut().insert(u.id, img.clone());
                    ui.set_my_avatar(img);
                }
            } else {
                sh.avatar_cache.borrow_mut().remove(&u.id);
                ui.set_my_avatar(slint::Image::default());
            }
            refresh_conversations_ui(sh);
        }
        Event::Conversations(list) => {
            for c in list {
                merge_conversation(sh, c);
            }
            refresh_conversations_ui(sh);
        }
        Event::ConversationCreated(c) => {
            merge_conversation(sh, c.clone());
            sh.selected.replace(c.id as i32);
            refresh_conversations_ui(sh);
            let ui = sh.ui();
            ui.set_selected_conversation(c.id as i32);
            ui.set_new_conversation_input(SharedString::default());
            ui.set_error_message(SharedString::default());
            sh.messages.borrow_mut().clear();
            refresh_messages_ui(sh);
            scroll_to_bottom(sh);
            sh.backend.refresh_messages(&sh.server(), &sh.token.borrow().clone().unwrap_or_default(), c.id);
        }
        Event::Messages { conversation_id, messages } => {
            if sh.selected.get() == conversation_id as i32 {
                let mut msgs = sh.messages.borrow_mut();
                msgs.clear();
                msgs.extend(messages);
                msgs.sort_by_key(|m| m.id);
                drop(msgs);
                refresh_messages_ui(sh);
                scroll_to_bottom(sh);
            }
        }
        Event::MessageSent(m) => {
            merge_message(sh, m);
            refresh_messages_ui(sh);
        }
        Event::WsMessage(m) => {
            let known = sh.conversations.borrow().iter().any(|c| c.id == m.conversation_id);
            if !known {
                sh.backend.refresh_conversations(&sh.server(), &sh.token.borrow().clone().unwrap_or_default());
            }
            merge_message(sh, m.clone());
            let selected = sh.selected.get();
            if selected == m.conversation_id as i32 {
                sh.unread.borrow_mut().remove(&m.conversation_id);
            } else if sh.self_id.get() != Some(m.sender_id) {
                *sh.unread.borrow_mut().entry(m.conversation_id).or_insert(0) += 1;
                refresh_conversations_ui(sh);
            }
            let at_bottom = sh.ui().get_msg_at_bottom();
            refresh_messages_ui(sh);
            if at_bottom {
                scroll_to_bottom(sh);
            }
        }
        Event::WsStatus(b) => {
            sh.ui().set_ws_connected(b);
        }
        Event::Profile(p) => {
            apply_profile(sh, &p);
            sh.ui().set_profile_open(true);
        }
        Event::ContextProfile(p) => {
            apply_profile(sh, &p);
            sh.ui().set_context_open(true);
        }
        Event::ModerationResult(p) => {
            apply_profile(sh, &p);
        }
        Event::ProfileError => {
            sh.ui().set_context_open(false);
            set_error(sh, "could not load user profile");
        }
        Event::ConversationDeleted(id) => {
            let mut convs = sh.conversations.borrow_mut();
            convs.retain(|c| c.id != id);
            drop(convs);
            if sh.selected.get() == id as i32 {
                sh.selected.replace(-1);
                sh.ui().set_selected_conversation(-1);
                sh.messages.borrow_mut().clear();
            }
            refresh_conversations_ui(sh);
            refresh_messages_ui(sh);
        }
        Event::UploadAvatar { server, token, data_url } => {
            sh.backend.set_avatar(&server, &token, data_url);
        }
        Event::AttachmentPicked(att) => {
            let ui = sh.ui();
            ui.set_pending_attach_name(att.name.clone().into());
            if att.mime.starts_with("image/") {
                ui.set_pending_attach_image(
                    load_avatar_image(&att.data).unwrap_or_default(),
                );
            } else {
                ui.set_pending_attach_image(slint::Image::default());
            }
            *sh.pending_attach.borrow_mut() = Some(att);
        }
        Event::Error(m) => set_error(sh, &m),
    }
}

fn logout(sh: &Shared) {
    sh.backend.set_token(None);
    sh.token.replace(None);
    sh.self_id.replace(None);
    sh.conversations.borrow_mut().clear();
    sh.messages.borrow_mut().clear();
    sh.contacts.borrow_mut().clear();
    sh.hidden.borrow_mut().clear();
    sh.selected.replace(-1);
    let ui = sh.ui();
    ui.set_logged_in(false);
    ui.set_needs_verify(false);
    ui.set_user_name(SharedString::default());
    ui.set_selected_conversation(-1);
    ui.set_error_message(SharedString::default());
    ui.set_ws_connected(false);
    ui.set_conversations(empty_model::<UiConversation>());
    ui.set_messages(empty_model::<UiMessage>());
    ui.set_suggestions(empty_model::<UiContact>());
    ui.set_profile_open(false);
    ui.set_context_open(false);
    ui.set_confirm_delete_open(false);
    ui.set_confirm_delete_conversation_id(-1);
    ui.set_new_conversation_input(SharedString::default());
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn main() -> Result<(), slint::PlatformError> {
    let backend = i_slint_backend_winit::Backend::builder()
        .with_window_attributes_hook(|attributes| {
            #[cfg(target_os = "macos")]
            {
                use i_slint_backend_winit::winit::platform::macos::WindowAttributesExtMacOS;
                // Slint builds transparent windows on macOS, which renders the
                // title bar transparent/invisible. Restore the native bar.
                attributes
                    .with_transparent(false)
                    .with_titlebar_transparent(false)
                    .with_fullsize_content_view(false)
            }
            #[cfg(not(target_os = "macos"))]
            {
                attributes
            }
        })
        .build()?;
    if let Err(e) = slint::platform::set_platform(Box::new(backend)) {
        eprintln!("failed to set platform: {e}");
    }

    let ui = MainWindow::new()?;
    let (tx, rx) = mpsc::unbounded_channel();
    let (token_tx, token_rx) = watch::channel(None);
    let backend = Rc::new(Backend::new(tx.clone(), token_tx));
    let default_server = normalize_server(
        &std::env::var("FEDITEXTER_SERVER")
            .ok()
            .or_else(load_saved_server)
            .unwrap_or_else(|| "localhost:3000".into()),
    );
    let (server_tx, server_rx) = watch::channel(default_server.clone());
    ui.set_server_input(default_server.into());
    if let Some(settings) = load_settings()
        && let Some(hex) = &settings.accent
        && let Some(color) = color_from_hex(hex)
    {
        ui.set_accent_color(color);
    }
    spawn_ws(&backend.runtime, server_rx, token_rx, tx);

    let shared = Rc::new(Shared {
        backend,
        ui: ui.as_weak(),
        rx: Rc::new(RefCell::new(rx)),
        token: Rc::new(RefCell::new(None)),
        self_id: Rc::new(Cell::new(None)),
        conversations: Rc::new(RefCell::new(Vec::new())),
        messages: Rc::new(RefCell::new(Vec::new())),
        selected: Rc::new(Cell::new(-1)),
        contacts: Rc::new(RefCell::new(Vec::new())),
        hidden: Rc::new(RefCell::new(Vec::new())),
        unread: Rc::new(RefCell::new(std::collections::HashMap::new())),
        avatar_cache: Rc::new(RefCell::new(std::collections::HashMap::new())),
        pending_attach: Rc::new(RefCell::new(None)),
    });

    {
        let sh = shared.clone();
        let mut mods = winit::keyboard::ModifiersState::default();
        ui.window().on_winit_window_event(move |_window, event| {
            match event {
                winit::event::WindowEvent::ModifiersChanged(m) => {
                    mods = m.state();
                    EventResult::Propagate
                }
                winit::event::WindowEvent::KeyboardInput { event: key, .. } => {
                    if key.state == winit::event::ElementState::Pressed && (mods.control_key() || mods.super_key()) {
                        use winit::keyboard::{KeyCode, PhysicalKey};
                        let code = match key.physical_key {
                            PhysicalKey::Code(c) => Some(c),
                            _ => None,
                        };
                        let zoom_in = matches!(code, Some(KeyCode::Equal) | Some(KeyCode::NumpadAdd) | Some(KeyCode::NumpadEqual));
                        let zoom_out = matches!(code, Some(KeyCode::Minus) | Some(KeyCode::NumpadSubtract));
                        let zoom_reset = matches!(code, Some(KeyCode::Digit0) | Some(KeyCode::Numpad0));
                        if zoom_in || zoom_out || zoom_reset {
                            let current = sh.ui().get_ui_scale();
                            let next = if zoom_reset {
                                1.0
                            } else if zoom_in {
                                (current + 0.1).min(1.8)
                            } else {
                                (current - 0.1).max(0.5)
                            };
                            sh.ui().set_ui_scale(next);
                            return EventResult::PreventDefault;
                        }
                    }
                    EventResult::Propagate
                }
                _ => EventResult::Propagate,
            }
        });
    }

    {
        let sh = shared.clone();
        let server_tx = server_tx.clone();
        ui.on_login(move |email, password| {
            let _ = server_tx.send(sh.server());
            sh.backend.login(&sh.server(), &email, &password);
        });
    }
    {
        let sh = shared.clone();
        let server_tx = server_tx.clone();
        ui.on_register(move |email, username, password| {
            let _ = server_tx.send(sh.server());
            sh.backend.register(&sh.server(), &email, &username, &password);
        });
    }
    {
        let sh = shared.clone();
        ui.on_logout(move || logout(&sh));
    }
    {
        let sh = shared.clone();
        ui.on_select_conversation(move |id| {
            sh.selected.replace(id);
            if id >= 0 {
                sh.unread.borrow_mut().remove(&(id as u64));
                refresh_conversations_ui(&sh);
                scroll_to_bottom(&sh);
                if let Some(token) = sh.token.borrow().clone() {
                    sh.backend.refresh_messages(&sh.server(), &token, id as u64);
                }
            }
        });
    }
    {
        let sh = shared.clone();
        ui.on_send_message(move |body| {
            if let Some(token) = sh.token.borrow().clone() {
                let conv = sh.selected.get();
                if conv >= 0 {
                    let body = body.trim().to_string();
                    let attachment = sh.pending_attach.borrow_mut().take();
                    let has_attach = attachment.is_some();
                    if !body.is_empty() || has_attach {
                        sh.backend.send_message(&sh.server(), &token, conv as u64, body, attachment);
                    }
                    if has_attach {
                        let ui = sh.ui();
                        ui.set_pending_attach_name(SharedString::default());
                        ui.set_pending_attach_image(slint::Image::default());
                    }
                }
            }
        });
    }
    {
        let sh = shared.clone();
        ui.on_new_conversation(move |input| {
            let token = match sh.token.borrow().clone() {
                Some(t) => t,
                None => return,
            };
            let input = input.trim().to_string();
            match parse_targets(&input) {
                Ok((ids, handles)) => {
                    set_error(&sh, "");
                    sh.backend.create_conversation(&sh.server(), &token, ids, handles);
                }
                Err(msg) => set_error(&sh, &msg),
            }
        });
    }
    {
        let sh = shared.clone();
        ui.on_verify(move |code| {
            if let Some(token) = sh.token.borrow().clone() {
                sh.backend.verify(&sh.server(), &token, code.trim());
            }
        });
    }
    {
        let sh = shared.clone();
        ui.on_resend_code(move || {
            if let Some(token) = sh.token.borrow().clone() {
                sh.backend.resend_verification(&sh.server(), &token);
            }
        });
    }
    {
        let sh = shared.clone();
        let server_tx = server_tx.clone();
        ui.on_settings_saved(move || {
            save_server(&sh.server());
            let accent = sh.ui().get_accent_color();
            let mut settings = load_settings().unwrap_or(LocalSettings { server: String::new(), accent: None });
            settings.server = sh.server();
            settings.accent = Some(accent_to_hex(accent));
            save_settings(&settings);
            let _ = server_tx.send(sh.server());
            if let Some(token) = sh.token.borrow().clone() {
                let name = sh.ui().get_display_name_input().to_string();
                if !name.trim().is_empty() {
                    sh.backend.update_display_name(&sh.server(), &token, &name);
                }
            }
        });
    }
    {
        let sh = shared.clone();
        ui.on_settings_choose_avatar(move || {
            let tx = sh.backend.tx.clone();
            let server = sh.server();
            let token = match sh.token.borrow().clone() {
                Some(t) => t,
                None => return,
            };
            sh.backend.runtime.spawn(async move {
                let file = rfd::AsyncFileDialog::new()
                    .add_filter("Image", &["png", "jpg", "jpeg", "webp"])
                    .pick_file()
                    .await;
                let Some(handle) = file else { return };
                let bytes = handle.read().await;
                let img = match image::load_from_memory(&bytes) {
                    Ok(img) => img,
                    Err(_) => {
                        eprintln!("[error] unsupported image format");
                        return;
                    }
                };
                let max_dim = 256u32;
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
                if img.write_to(&mut out, image::ImageFormat::Png).is_err() {
                    eprintln!("[error] failed to encode avatar");
                    return;
                }
                use base64::Engine;
                let data = base64::engine::general_purpose::STANDARD.encode(out.into_inner());
                let data_url = format!("data:image/png;base64,{data}");
                let _ = tx.send(Event::UploadAvatar { server, token, data_url });
            });
        });
    }
    {
        let sh = shared.clone();
        ui.on_settings_remove_avatar(move || {
            if let Some(token) = sh.token.borrow().clone() {
                sh.backend.set_avatar(&sh.server(), &token, String::new());
            }
        });
    }
    {
        let sh = shared.clone();
        ui.on_choose_attachment(move || {
            let tx = sh.backend.tx.clone();
            sh.backend.runtime.spawn(async move {
                let file = rfd::AsyncFileDialog::new().pick_file().await;
                let Some(handle) = file else { return };
                let bytes = handle.read().await;
                let name = handle.file_name();
                let mime = mime_from_path(handle.path()).to_string();
                use base64::Engine;
                let data = base64::engine::general_purpose::STANDARD.encode(&bytes);
                let _ = tx.send(Event::AttachmentPicked(Attachment {
                    mime: mime.clone(),
                    name,
                    data: format!("data:{mime};base64,{data}"),
                }));
            });
        });
    }
    {
        let sh = shared.clone();
        ui.on_clear_attachment(move || {
            *sh.pending_attach.borrow_mut() = None;
            let ui = sh.ui();
            ui.set_pending_attach_name(SharedString::default());
            ui.set_pending_attach_image(slint::Image::default());
        });
    }
    {
        let sh = shared.clone();
        ui.on_suggestion_selected(move |id| {
            let handle = sh
                .contacts
                .borrow()
                .iter()
                .find(|c| c.id == id as u64)
                .map(|c| c.handle.clone());
            match handle {
                Some(h) => {
                    let current = sh.ui().get_new_conversation_input().to_string();
                    let mut parts: Vec<String> = current
                        .split(',')
                        .map(str::trim)
                        .filter(|s| {
                            !s.is_empty()
                                && *s != "@"
                                && !(s.starts_with('@') && s.matches('@').count() < 2)
                        })
                        .map(str::to_string)
                        .collect();
                    parts.push(h);
                    let joined = parts.join(", ");
                    sh.ui().set_nc_cursor_len(joined.chars().count() as i32);
                    sh.ui().set_new_conversation_input(joined.into());
                    sh.ui().set_nc_append(true);
                    set_error(&sh, "");
                }
                None => set_error(&sh, "contact not found"),
            }
        });
    }
    {
        let sh = shared.clone();
        ui.on_request_profile(move |user_id| {
            if user_id < 0 {
                return;
            }
            if let Some(token) = sh.token.borrow().clone() {
                sh.backend.fetch_profile(&sh.server(), &token, user_id as u64);
            }
        });
    }
    {
        let sh = shared.clone();
        ui.on_request_context(move |user_id, conversation_id, x, y| {
            let ui = sh.ui();
            ui.set_context_user_id(user_id);
            ui.set_context_conversation_id(conversation_id);
            ui.set_context_x(x);
            ui.set_context_y(y);
            if user_id >= 0 {
                if let Some(token) = sh.token.borrow().clone() {
                    sh.backend.fetch_profile_context(&sh.server(), &token, user_id as u64);
                }
            } else {
                ui.set_context_open(true);
            }
        });
    }
    {
        let sh = shared.clone();
        ui.on_context_profile(move || {
            let user_id = sh.ui().get_context_user_id();
            if user_id >= 0 && let Some(token) = sh.token.borrow().clone() {
                sh.backend.fetch_profile(&sh.server(), &token, user_id as u64);
            }
        });
    }
    {
        let sh = shared.clone();
        ui.on_toggle_block(move || {
            let ui = sh.ui();
            let (user_id, blocked) = if ui.get_profile_open() {
                (ui.get_profile().id, ui.get_profile().blocked)
            } else {
                (ui.get_context_user_id(), ui.get_context_blocked())
            };
            if user_id < 0 {
                return;
            }
            let action = if blocked { "unblock" } else { "block" };
            if let Some(token) = sh.token.borrow().clone() {
                sh.backend.moderation(&sh.server(), &token, user_id as u64, action);
            }
        });
    }
    {
        let sh = shared.clone();
        ui.on_toggle_mute(move || {
            let ui = sh.ui();
            let (user_id, muted) = if ui.get_profile_open() {
                (ui.get_profile().id, ui.get_profile().muted)
            } else {
                (ui.get_context_user_id(), ui.get_context_muted())
            };
            if user_id < 0 {
                return;
            }
            let action = if muted { "unmute" } else { "mute" };
            if let Some(token) = sh.token.borrow().clone() {
                sh.backend.moderation(&sh.server(), &token, user_id as u64, action);
            }
        });
    }
    {
        let sh = shared.clone();
        ui.on_context_delete(move || {
            let conversation_id = sh.ui().get_context_conversation_id();
            if conversation_id < 0 {
                return;
            }
            let title = sh
                .conversations
                .borrow()
                .iter()
                .find(|c| c.id == conversation_id as u64)
                .map(|c| conversation_title(c, sh.self_id.get()))
                .unwrap_or_else(|| "this conversation".into());
            let ui = sh.ui();
            ui.set_confirm_delete_conversation_id(conversation_id);
            ui.set_confirm_delete_title(title.into());
            ui.set_confirm_delete_open(true);
        });
    }
    {
        let sh = shared.clone();
        ui.on_confirm_delete(move || {
            let conversation_id = sh.ui().get_confirm_delete_conversation_id();
            if conversation_id >= 0 && let Some(token) = sh.token.borrow().clone() {
                sh.backend.delete_conversation(&sh.server(), &token, conversation_id as u64);
            }
        });
    }

    {
        let sh = shared.clone();
        ui.on_open_chat(move |user_id| {
            if user_id < 0 {
                return;
            }
            let existing = sh
                .conversations
                .borrow()
                .iter()
                .find(|c| c.kind != "group" && c.members.iter().any(|m| m.id == user_id as u64))
                .map(|c| c.id);
            let ui = sh.ui();
            ui.set_profile_open(false);
            if let Some(cid) = existing {
                sh.selected.replace(cid as i32);
                sh.unread.borrow_mut().remove(&cid);
                refresh_conversations_ui(&sh);
                ui.set_selected_conversation(cid as i32);
                scroll_to_bottom(&sh);
                if let Some(token) = sh.token.borrow().clone() {
                    sh.backend.refresh_messages(&sh.server(), &token, cid);
                }
            } else if let Some(token) = sh.token.borrow().clone() {
                sh.backend.create_conversation(&sh.server(), &token, vec![user_id as u64], Vec::new());
            }
        });
    }

    let drain = shared.clone();
    let timer = slint::Timer::default();
    timer.start(slint::TimerMode::Repeated, Duration::from_millis(50), move || {
        let mut events = Vec::new();
        {
            let mut rx = drain.rx.borrow_mut();
            while let Ok(ev) = rx.try_recv() {
                events.push(ev);
            }
        }
        for ev in events {
            handle_event(&drain, ev);
        }
        if drain.ui().get_logged_in() && drain.ui().get_selected_conversation() < 0 {
            refresh_suggestions(&drain);
        }
    });

    ui.run()
}
