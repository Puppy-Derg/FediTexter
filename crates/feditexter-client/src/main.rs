use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::Duration;

use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{json, Value};
use slint::{ComponentHandle, ModelRc, SharedString, VecModel, Weak};
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

    fn create_conversation(&self, server: &str, token: &str, user_id: Option<u64>, handle: Option<String>) {
        let tx = self.tx.clone();
        let http = self.http.clone();
        let server = server.to_string();
        let token = token.to_string();
        let handle = handle.clone();
        self.runtime.spawn(async move {
            let url = api_url(&server, "/api/conversations");
            let mut body = json!({});
            if let Some(id) = user_id {
                body["user_id"] = json!(id);
            }
            if let Some(h) = handle {
                body["handle"] = json!(h);
            }
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

    fn send_message(&self, server: &str, token: &str, conversation_id: u64, body: String) {
        let tx = self.tx.clone();
        let http = self.http.clone();
        let server = server.to_string();
        let token = token.to_string();
        self.runtime.spawn(async move {
            let url = api_url(&server, &format!("/api/conversations/{conversation_id}/messages"));
            let resp = match http
                .post(&url)
                .bearer_auth(&token)
                .json(&json!({ "body": body }))
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
    std::fs::read_to_string(server_state_path())
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn save_server(server: &str) {
    if let Err(e) = std::fs::write(server_state_path(), server.trim()) {
        eprintln!("[error] failed to save server: {e}");
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

fn set_error(sh: &Shared, msg: &str) {
    eprintln!("[error] {msg}");
    sh.ui().set_error_message(msg.into());
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
            UiConversation {
                id: c.id as i32,
                title: title.clone().into(),
                avatar_text: initials(&title).into(),
                avatar_color: avatar_color(&title),
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
    let query = sh.ui().get_new_conversation_input().to_string().to_lowercase();
    let matches: Vec<UiContact> = sh
        .contacts
        .borrow()
        .iter()
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
            UiMessage {
                id: m.id as i32,
                sender: sender.into(),
                body: m.body.clone().into(),
                created_at: m.created_at.clone().into(),
                is_self: self_id == Some(m.sender_id),
                sender_id: m.sender_id as i32,
                avatar_text: initials(&avatar_name).into(),
                avatar_color: avatar_color(&avatar_name),
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
            ui.set_new_conversation_open(false);
            ui.set_new_conversation_input(SharedString::default());
            ui.set_error_message(SharedString::default());
            sh.messages.borrow_mut().clear();
            refresh_messages_ui(sh);
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
            merge_message(sh, m);
            refresh_messages_ui(sh);
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
    });

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
                    if !body.is_empty() {
                        sh.backend.send_message(&sh.server(), &token, conv as u64, body);
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
            match conversation_target(&input) {
                Ok((user_id, handle)) => {
                    set_error(&sh, "");
                    sh.backend.create_conversation(&sh.server(), &token, user_id, handle);
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
        let server_tx = server_tx.clone();
        ui.on_settings_saved(move || {
            save_server(&sh.server());
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
        ui.on_suggestion_selected(move |id| {
            let token = match sh.token.borrow().clone() {
                Some(t) => t,
                None => return,
            };
            let handle = sh
                .contacts
                .borrow()
                .iter()
                .find(|c| c.id == id as u64)
                .map(|c| c.handle.clone());
            match handle {
                Some(h) => {
                    set_error(&sh, "");
                    sh.backend.create_conversation(&sh.server(), &token, None, Some(h));
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
        if drain.ui().get_new_conversation_open() {
            refresh_suggestions(&drain);
        }
    });

    ui.run()
}
