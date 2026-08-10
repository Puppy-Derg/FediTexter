use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use hmac::Mac as _;
use serde::Deserialize;
use serde_json::{json, Value};
use slint::{ComponentHandle, ModelRc, SharedString, VecModel, Weak};
use slint::winit_030::{EventResult, WinitWindowAccessor, winit};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;

mod p2p;

use p2p::{P2pManager, ServingFile, SignalEvent};

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
    #[serde(default)]
    totp_enabled: bool,
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
    #[serde(default)]
    file_id: Option<String>,
    #[serde(default)]
    file_size: Option<i64>,
    #[serde(default)]
    thumbnail_data: Option<String>,
    #[serde(default)]
    edited_at: Option<String>,
    #[serde(default)]
    original_body: Option<String>,
    #[serde(default)]
    deleted_at: Option<String>,
}

/// A HubEvent as pushed over the WebSocket (tagged `kind`).
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
enum WsHubEvent {
    Message { message: Message },
    #[serde(rename = "message_edited")]
    MessageEdited { message: Message },
    #[serde(rename = "message_deleted")]
    MessageDeleted { conversation_id: u64, message_id: u64 },
    Signal { signal: SignalEvent },
    Typing {
        conversation_id: u64,
        from_user_id: u64,
        from_username: String,
    },
    Presence { user_id: u64, online: bool },
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
    /// P2P transfer id (uuid v4), generated when the file is picked.
    file_id: String,
    /// Full size of the file in bytes.
    file_size: u64,
    /// Small image thumbnail as a data: URL (only for image mimes).
    thumbnail: String,
    /// The raw file bytes, kept in memory to serve over the data channel.
    bytes: Vec<u8>,
}

/// A file we've fully downloaded this session (or loaded from the disk cache).
#[derive(Clone)]
struct DownloadedFile {
    /// data: URL of the full image (images only), built lazily.
    image_data: Option<String>,
    /// On-disk cache path for the raw bytes.
    path: Option<std::path::PathBuf>,
}

/// A file we sent this session, used to render our own bubbles at full res.
struct OwnFile {
    thumbnail: String,
    bytes: Vec<u8>,
}

#[derive(Deserialize, Clone, Debug)]
struct LinkPreview {
    url: String,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    image: Option<String>,
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
    WsMessageEdited(Message),
    WsMessageDeleted { conversation_id: u64, message_id: u64 },
    WsStatus(bool),
    P2pSignal(SignalEvent),
    P2pStatus { file_id: String, status: String },
    P2pProgress { file_id: String, received: u64, total: u64 },
    #[allow(dead_code)]
    P2pComplete { file_id: String, mime: String, name: String, bytes: Vec<u8> },
    P2pFailed { file_id: String, reason: String },
    #[allow(dead_code)]
    Typing { conversation_id: u64, from_user_id: u64, from_username: String },
    TypingExpired { conversation_id: u64 },
    Presence { user_id: u64, online: bool },
    PresenceBatch(std::collections::HashMap<u64, bool>),
    Profile(Profile),
    ContextProfile(Profile),
    ModerationResult(Profile),
    ProfileError,
    ConversationDeleted(u64),
    UploadAvatar { server: String, token: String, data_url: String },
    AttachmentPicked(Attachment),
    LinkPreview(LinkPreview),
    LinkPreviewFailed(String),
    TwoFaRequired { pending_token: String },
    TwoFaSetup { secret: String, qr: String },
    Info(String),
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
    /// Outgoing P2P signaling frames, forwarded by the WebSocket loop.
    ws_tx: UnboundedSender<String>,
}

impl Backend {
    fn new(
        tx: UnboundedSender<Event>,
        token: watch::Sender<Option<String>>,
        ws_tx: UnboundedSender<String>,
        device_id: String,
    ) -> Self {
        let runtime = tokio::runtime::Runtime::new().expect("failed to start tokio runtime");
        // Every request carries the persistent device UUID so the server can
        // bind the session to this installation and reject replays elsewhere.
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "X-Device-Id",
            reqwest::header::HeaderValue::from_str(&device_id).expect("valid header value"),
        );
        let http = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .expect("failed to build http client");
        Backend { runtime, http, tx, token, ws_tx }
    }

    fn set_token(&self, token: Option<String>) {
        let _ = self.token.send(token);
    }

    fn login(&self, server: &str, email: &str, password: &str, remember_me: bool) {
        let tx = self.tx.clone();
        let http = self.http.clone();
        let server = server.to_string();
        let email = email.to_string();
        let password = password.to_string();
        self.runtime.spawn(async move {
            let url = api_url(&server, "/api/login");
            match http
                .post(&url)
                .json(&json!({ "email": email, "password": password, "remember_me": remember_me }))
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    match resp.json::<Value>().await {
                        Ok(v) => {
                            if v.get("requires_2fa").and_then(|b| b.as_bool()).unwrap_or(false) {
                                let pending = v.get("pending_token").and_then(|t| t.as_str()).unwrap_or("").to_string();
                                let _ = tx.send(Event::TwoFaRequired { pending_token: pending });
                            } else if let Ok(r) = serde_json::from_value::<AuthResponse>(v.clone()) {
                                let _ = tx.send(Event::LoggedIn { token: r.token, user: r.user });
                            } else {
                                let _ = tx.send(Event::AuthFailed("malformed server response".into()));
                            }
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

    fn login_2fa(&self, server: &str, pending_token: String, code: String, remember_me: bool) {
        let tx = self.tx.clone();
        let http = self.http.clone();
        let server = server.to_string();
        self.runtime.spawn(async move {
            let url = api_url(&server, "/api/login/2fa");
            match http
                .post(&url)
                .json(&json!({ "pending_token": pending_token, "code": code, "remember_me": remember_me }))
                .send()
                .await
            {
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

    /// Validate a token saved by "remember me" against `/api/me`. Emits
    /// `LoggedIn` on success (which restores the whole UI state); on a 401/403
    /// the caller clears the saved session so the user logs in again.
    fn restore_session(&self, server: String, token: String) {
        let tx = self.tx.clone();
        let http = self.http.clone();
        self.runtime.spawn(async move {
            let url = api_url(&server, "/api/me");
            match http.get(&url).bearer_auth(&token).send().await {
                Ok(resp) if resp.status().is_success() => {
                    match resp.json::<Value>().await {
                        Ok(v) => {
                            if let Ok(user) =
                                serde_json::from_value::<User>(v.get("user").cloned().unwrap_or(Value::Null))
                            {
                                let _ = tx.send(Event::LoggedIn { token, user });
                                return;
                            }
                        }
                        Err(_) => {}
                    }
                    clear_saved_session();
                    let _ = tx.send(Event::AuthFailed("session expired, please log in again".into()));
                }
                Ok(resp) => {
                    if resp.status() == reqwest::StatusCode::UNAUTHORIZED
                        || resp.status() == reqwest::StatusCode::FORBIDDEN
                    {
                        clear_saved_session();
                        let _ = tx.send(Event::AuthFailed("session expired, please log in again".into()));
                    } else {
                        let msg = error_message(resp).await;
                        let _ = tx.send(Event::AuthFailed(msg));
                    }
                }
                Err(e) => {
                    // Server unreachable — keep the saved session and let the
                    // user retry from the login screen (token stays on disk).
                    let _ = tx.send(Event::AuthFailed(format!("could not reach server: {e}")));
                }
            }
        });
    }

    fn twofa_setup(&self, server: &str, token: &str) {
        let tx = self.tx.clone();
        let http = self.http.clone();
        let server = server.to_string();
        let token = token.to_string();
        self.runtime.spawn(async move {
            let url = api_url(&server, "/api/me/2fa/setup");
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
            match resp.json::<Value>().await {
                Ok(v) => {
                    let secret = v.get("secret").and_then(|s| s.as_str()).unwrap_or("").to_string();
                    let qr = v.get("qr").and_then(|q| q.as_str()).unwrap_or("").to_string();
                    let _ = tx.send(Event::TwoFaSetup { secret, qr });
                }
                Err(_) => {
                    let _ = tx.send(Event::Error("malformed server response".into()));
                }
            }
        });
    }

    fn twofa_enable(&self, server: &str, token: &str, code: String) {
        let tx = self.tx.clone();
        let http = self.http.clone();
        let server = server.to_string();
        let token = token.to_string();
        self.runtime.spawn(async move {
            let url = api_url(&server, "/api/me/2fa/enable");
            let resp = match http.post(&url).bearer_auth(&token).json(&json!({ "code": code })).send().await {
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
            if let Ok(v) = resp.json::<Value>().await
                && let Ok(u) = serde_json::from_value::<User>(v.get("user").cloned().unwrap_or(Value::Null))
            {
                let _ = tx.send(Event::UserUpdated(u));
            }
        });
    }

    fn twofa_disable(&self, server: &str, token: &str, code: String) {
        let tx = self.tx.clone();
        let http = self.http.clone();
        let server = server.to_string();
        let token = token.to_string();
        self.runtime.spawn(async move {
            let url = api_url(&server, "/api/me/2fa/disable");
            let resp = match http.post(&url).bearer_auth(&token).json(&json!({ "code": code })).send().await {
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
            if let Ok(v) = resp.json::<Value>().await
                && let Ok(u) = serde_json::from_value::<User>(v.get("user").cloned().unwrap_or(Value::Null))
            {
                let _ = tx.send(Event::UserUpdated(u));
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
            let _ = tx.send(Event::Info("Verification code sent — check your email".into()));
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

    fn fetch_link_preview(&self, server: &str, url: String) {
        let tx = self.tx.clone();
        let http = self.http.clone();
        let server = server.to_string();
        self.runtime.spawn(async move {
            let url_api = api_url(&server, "/api/link-preview");
            let resp = match http.post(&url_api).json(&json!({ "url": url })).send().await {
                Ok(r) => r,
                Err(_) => {
                    let _ = tx.send(Event::LinkPreviewFailed(url));
                    return;
                }
            };
            if !resp.status().is_success() {
                let _ = tx.send(Event::LinkPreviewFailed(url));
                return;
            }
            match resp.json::<LinkPreview>().await {
                Ok(p) => {
                    let _ = tx.send(Event::LinkPreview(p));
                }
                Err(_) => {
                    let _ = tx.send(Event::LinkPreviewFailed(url));
                }
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

    /// Broadcast a typing notification for a conversation over the WebSocket.
    fn send_typing(&self, conversation_id: u64) {
        let _ = self.ws_tx.send(
            json!({ "type": "typing", "conversation_id": conversation_id }).to_string(),
        );
    }

    /// Fetch which of the given users are currently online.
    fn fetch_presence(&self, server: &str, token: &str, ids: Vec<u64>) {
        let tx = self.tx.clone();
        let http = self.http.clone();
        let server = server.to_string();
        let token = token.to_string();
        let ids_str = ids
            .iter()
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(",");
        self.runtime.spawn(async move {
            let url = api_url(&server, &format!("/api/presence?ids={ids_str}"));
            let resp = match http.get(&url).bearer_auth(&token).send().await {
                Ok(r) => r,
                Err(_) => return,
            };
            if !resp.status().is_success() {
                return;
            }
            let v: Value = match resp.json().await {
                Ok(v) => v,
                Err(_) => return,
            };
            let mut map = std::collections::HashMap::new();
            if let Some(obj) = v.get("presence").and_then(|p| p.as_object()) {
                for (id, online) in obj {
                    if let (Ok(id), Some(on)) = (id.parse::<u64>(), online.as_bool()) {
                        map.insert(id, on);
                    }
                }
            }
            let _ = tx.send(Event::PresenceBatch(map));
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
                // The bytes travel P2P; the server only stores metadata + a small
                // thumbnail so the recipient can render the bubble immediately.
                payload["file_id"] = json!(att.file_id);
                payload["file_size"] = json!(att.file_size);
                if !att.thumbnail.is_empty() {
                    payload["thumbnail_data"] = json!(att.thumbnail);
                }
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

    fn edit_message(
        &self,
        server: &str,
        token: &str,
        conversation_id: u64,
        message_id: u64,
        body: String,
    ) {
        let tx = self.tx.clone();
        let http = self.http.clone();
        let server = server.to_string();
        let token = token.to_string();
        self.runtime.spawn(async move {
            let url = api_url(&server, &format!("/api/conversations/{conversation_id}/messages/{message_id}"));
            let payload = json!({ "body": body });
            let resp = match http.patch(&url).bearer_auth(&token).json(&payload).send().await {
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
                        let _ = tx.send(Event::WsMessageEdited(m));
                    }
                }
                Err(_) => {
                    let _ = tx.send(Event::Error("malformed server response".into()));
                }
            }
        });
    }

    fn delete_message(
        &self,
        server: &str,
        token: &str,
        conversation_id: u64,
        message_id: u64,
    ) {
        let tx = self.tx.clone();
        let http = self.http.clone();
        let server = server.to_string();
        let token = token.to_string();
        self.runtime.spawn(async move {
            let url = api_url(&server, &format!("/api/conversations/{conversation_id}/messages/{message_id}"));
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
            let _ = tx.send(Event::WsMessageDeleted { conversation_id, message_id });
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

/// A "remember me" session persisted between app restarts.
#[derive(serde::Serialize, serde::Deserialize)]
struct SavedSession {
    server: String,
    token: String,
}

fn session_path() -> std::path::PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    std::path::Path::new(&home).join(".feditexter_session")
}

fn save_session(server: &str, token: &str) {
    if let Ok(json) = serde_json::to_string(&SavedSession { server: server.to_string(), token: token.to_string() })
        && let Err(e) = std::fs::write(session_path(), json)
    {
        eprintln!("[error] failed to save session: {e}");
    }
}

fn load_session() -> Option<SavedSession> {
    let raw = std::fs::read_to_string(session_path()).ok()?;
    serde_json::from_str(&raw).ok()
}

fn clear_saved_session() {
    let _ = std::fs::remove_file(session_path());
}

/// Directory where fully-downloaded P2P files are cached so they survive restarts.
fn files_cache_dir() -> std::path::PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    std::path::Path::new(&home).join(".feditexter_files")
}

fn cache_path_for(file_id: &str) -> std::path::PathBuf {
    files_cache_dir().join(file_id)
}

/// Pre-load files cached on disk from earlier sessions so previously-downloaded
/// attachments render as complete instead of re-fetching.
fn load_cached_files(
    downloaded: &Rc<RefCell<std::collections::HashMap<String, DownloadedFile>>>,
) {
    let dir = files_cache_dir();
    if let Ok(entries) = std::fs::read_dir(&dir) {
        let mut map = downloaded.borrow_mut();
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file()
                && let Some(name) = path.file_name().and_then(|n| n.to_str())
            {
                map.insert(
                    name.to_string(),
                    DownloadedFile { image_data: None, path: Some(path) },
                );
            }
        }
    }
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
        Some(LocalSettings { server: raw.to_string(), accent: None, device_id: None })
    } else {
        None
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct LocalSettings {
    server: String,
    #[serde(default)]
    accent: Option<String>,
    /// Persistent device identifier sent with every authenticated request so
    /// the server can bind sessions to this installation.
    #[serde(default)]
    device_id: Option<String>,
}

/// Return this installation's persistent device UUID, generating and saving it
/// the first time the app runs.
fn load_or_create_device_id() -> String {
    let mut settings = load_settings().unwrap_or(LocalSettings {
        server: String::new(),
        accent: None,
        device_id: None,
    });
    if let Some(d) = &settings.device_id {
        return d.clone();
    }
    let d = uuid::Uuid::new_v4().to_string();
    settings.device_id = Some(d.clone());
    save_settings(&settings);
    d
}

fn save_settings(settings: &LocalSettings) {
    if let Ok(json) = serde_json::to_string(settings)
        && let Err(e) = std::fs::write(server_state_path(), json)
    {
        eprintln!("[error] failed to save settings: {e}");
    }
}

fn save_server(server: &str) {
    let mut settings = load_settings().unwrap_or(LocalSettings { server: String::new(), accent: None, device_id: None });
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
    mut ws_rx: UnboundedReceiver<String>,
    device_id: String,
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
                    let _ = tx.send(Event::WsStatus(false));
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    continue;
                }
            };
            if let Ok(header) = HeaderValue::from_str(&format!("Bearer {token}")) {
                request.headers_mut().insert("authorization", header);
            }
            if let Ok(header) = HeaderValue::from_str(&device_id) {
                request.headers_mut().insert("x-device-id", header);
            }

            match tokio_tungstenite::connect_async(request).await {
                Ok((ws, _)) => {
                    eprintln!("[ws] connected to {url}");
                    let _ = tx.send(Event::WsStatus(true));
                    let (mut sink, mut stream) = ws.split();
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
                            outgoing = ws_rx.recv() => {
                                match outgoing {
                                    Some(text) => {
                                        if sink.send(tokio_tungstenite::tungstenite::Message::Text(text.into())).await.is_err() {
                                            break;
                                        }
                                    }
                                    None => return,
                                }
                            }
                            msg = stream.next() => {
                                match msg {
                                    Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                                        if let Ok(ev) = serde_json::from_str::<WsHubEvent>(&text) {
                                            match ev {
                                                WsHubEvent::Message { message } => {
                                                    let _ = tx.send(Event::WsMessage(message));
                                                }
                                                WsHubEvent::MessageEdited { message } => {
                                                    let _ = tx.send(Event::WsMessageEdited(message));
                                                }
                                                WsHubEvent::MessageDeleted { conversation_id, message_id } => {
                                                    let _ = tx.send(Event::WsMessageDeleted { conversation_id, message_id });
                                                }
                                                WsHubEvent::Signal { signal } => {
                                                    let _ = tx.send(Event::P2pSignal(signal));
                                                }
                                                WsHubEvent::Typing { conversation_id, from_user_id, from_username } => {
                                                    let _ = tx.send(Event::Typing {
                                                        conversation_id,
                                                        from_user_id,
                                                        from_username,
                                                    });
                                                }
                                                WsHubEvent::Presence { user_id, online } => {
                                                    let _ = tx.send(Event::Presence { user_id, online });
                                                }
                                            }
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
                    let _ = tx.send(Event::WsStatus(false));
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
    preview_cache: Rc<RefCell<std::collections::HashMap<String, Option<LinkPreview>>>>,
    preview_requested: Rc<RefCell<std::collections::HashSet<String>>>,
    p2p: Arc<P2pManager>,
    p2p_status: Rc<RefCell<std::collections::HashMap<String, P2pUi>>>,
    downloaded: Rc<RefCell<std::collections::HashMap<String, DownloadedFile>>>,
    own_files: Rc<RefCell<std::collections::HashMap<String, OwnFile>>>,
    /// Whether the user asked to persist this login for 60 days.
    remember_me: Rc<Cell<bool>>,
    /// user_id -> online status (presence).
    presence: Rc<RefCell<std::collections::HashMap<u64, bool>>>,
    /// conversation_id -> (who is typing, when they last typed) for the
    /// "… is typing" indicator.
    typing: Rc<RefCell<std::collections::HashMap<u64, (String, std::time::Instant)>>>,
    /// Last time we broadcast "I'm typing" to avoid flooding the server.
    last_typing_sent: Rc<Cell<std::time::Instant>>,
}

/// Transfer state shown under a P2P attachment bubble.
struct P2pUi {
    status: String,
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

/// Encode raw image bytes as a PNG data: URL so Slint can render them.
fn image_data_url_from_bytes(bytes: &[u8]) -> Option<String> {
    let img = image::load_from_memory(bytes).ok()?;
    let mut out = std::io::Cursor::new(Vec::new());
    img.write_to(&mut out, image::ImageFormat::Png).ok()?;
    use base64::Engine;
    let data = base64::engine::general_purpose::STANDARD.encode(out.into_inner());
    Some(format!("data:image/png;base64,{data}"))
}

fn human_size(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
    let b = bytes as f64;
    if b >= GIB {
        format!("{:.1} GB", b / GIB)
    } else if b >= MIB {
        format!("{:.1} MB", b / MIB)
    } else if b >= KIB {
        format!("{:.0} KB", b / KIB)
    } else {
        format!("{bytes} B")
    }
}

/// Compute what to show for a message's attachment: the image to render, the
/// transfer status line, and (for non-image P2P files) the file size. Triggers
/// a P2P fetch for undownloaded files (deduped by the manager).
fn p2p_display(sh: &Shared, m: &Message) -> (slint::Image, String, String) {
    let mime = m.attachment_mime.clone().unwrap_or_default();
    let is_image = mime.starts_with("image/");

    // Legacy inline attachment: the payload is already in the message.
    if let Some(data) = &m.attachment_data {
        let img = if is_image {
            load_avatar_image(data).unwrap_or_default()
        } else {
            slint::Image::default()
        };
        return (img, String::new(), String::new());
    }

    let Some(file_id) = &m.file_id else {
        return (slint::Image::default(), String::new(), String::new());
    };
    let file_id = file_id.clone();

    if self_id_of(sh) == Some(m.sender_id) {
        // Our own sent file: render the full-res thumbnail we kept.
        if let Some(own) = sh.own_files.borrow().get(&file_id) {
            let img = if is_image {
                load_avatar_image(&own.thumbnail).unwrap_or_default()
            } else {
                slint::Image::default()
            };
            return (img, String::new(), human_size(own.bytes.len() as u64));
        }
    }

    // Fully downloaded (this session or from the on-disk cache).
    if let Some(dl) = sh.downloaded.borrow().get(&file_id).cloned() {
        let cached = dl.image_data.clone();
        let path = dl.path.clone();
        let mut img = slint::Image::default();
        if is_image {
            let data = if let Some(d) = cached {
                Some(d)
            } else if let Some(path) = &path {
                if let Ok(bytes) = std::fs::read(path) {
                    let d = image_data_url_from_bytes(&bytes);
                    if let Some(d) = &d
                        && let Some(map) = sh.downloaded.borrow_mut().get_mut(&file_id)
                    {
                        map.image_data = Some(d.clone());
                    }
                    d
                } else {
                    None
                }
            } else {
                None
            };
            if let Some(d) = data {
                img = load_avatar_image(&d).unwrap_or_default();
            }
        }
        let size = path
            .as_ref()
            .and_then(|p| std::fs::metadata(p).ok().map(|md| md.len()))
            .unwrap_or(0);
        return (img, String::new(), human_size(size));
    }

    // Not available yet: show the thumbnail + status, and ask the sender to
    // serve the file (the manager dedupes repeated calls).
    let img = if is_image {
        m.thumbnail_data.as_deref().and_then(load_avatar_image).unwrap_or_default()
    } else {
        slint::Image::default()
    };
    let status = sh
        .p2p_status
        .borrow()
        .get(&file_id)
        .map(|s| s.status.clone())
        .unwrap_or_default();
    // Only request transfers from other people — fetching from ourselves after
    // a restart (when this session no longer holds the bytes) makes no sense.
    if self_id_of(sh) != Some(m.sender_id) {
        sh.p2p.fetch(&file_id, m.sender_id);
    }
    let size = m.file_size.unwrap_or(0) as u64;
    (img, status, human_size(size))
}

fn self_id_of(sh: &Shared) -> Option<u64> {
    sh.self_id.get()
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

/// Open a URL in the system browser.
fn open_in_browser(url: &str) {
    #[cfg(target_os = "macos")]
    {
        let _ = std::process::Command::new("open").arg(url).spawn();
    }
    #[cfg(target_os = "windows")]
    {
        let _ = std::process::Command::new("cmd").args(["/c", "start", "", url]).spawn();
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        let _ = std::process::Command::new("xdg-open").arg(url).spawn();
    }
}

/// Write an attachment's bytes to a temp file and open it in the OS viewer
/// (Preview on macOS, the default viewer elsewhere). On mobile the OS viewer
/// renders fullscreen; on desktop it opens in its own window.
fn open_attachment(mime: &str, name: &str, data_url: &str) {
    let Some(b64) = data_url.split_once(";base64,") else { return };
    use base64::Engine;
    let bytes = match base64::engine::general_purpose::STANDARD.decode(b64.1) {
        Ok(b) if !b.is_empty() => b,
        _ => return,
    };
    let ext = match mime {
        "image/png" => "png",
        "image/jpeg" => "jpg",
        "image/gif" => "gif",
        "image/webp" => "webp",
        "image/svg+xml" => "svg",
        "video/mp4" => "mp4",
        "video/webm" => "webm",
        "audio/mpeg" => "mp3",
        "audio/wav" => "wav",
        "application/pdf" => "pdf",
        "text/plain" => "txt",
        _ => "bin",
    };
    let mut base: String = name
        .rsplit('.')
        .next_back()
        .filter(|_| !name.is_empty())
        .map(|stem| stem.to_string())
        .unwrap_or_else(|| "feditexter".to_string());
    base = base
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
        .take(40)
        .collect();
    if base.is_empty() {
        base = "feditexter".to_string();
    }
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let path = std::env::temp_dir().join(format!("{base}-{stamp}.{ext}"));
    if std::fs::write(&path, bytes).is_err() {
        return;
    }
    let path = path.to_string_lossy().into_owned();
    #[cfg(target_os = "macos")]
    {
        let _ = std::process::Command::new("open").arg(&path).spawn();
    }
    #[cfg(target_os = "windows")]
    {
        let _ = std::process::Command::new("cmd").args(["/c", "start", "", &path]).spawn();
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        let _ = std::process::Command::new("xdg-open").arg(&path).spawn();
    }
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

/// Find the first http(s):// URL in a string.
fn first_url(text: &str) -> Option<String> {
    for (i, _) in text.match_indices("http") {
        let rest = &text[i..];
        if let Some(start) = rest.strip_prefix("https://").or_else(|| rest.strip_prefix("http://")) {
            let end = start.find(|c: char| c.is_whitespace() || c == '"' || c == '\'').unwrap_or(start.len());
            if end > 0 {
                return Some(format!("http{}://{}", if rest.starts_with("https") { "s" } else { "" }, &start[..end]));
            }
        }
    }
    None
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
                online: c
                    .members
                    .iter()
                    .find(|m| Some(m.id) != self_id)
                    .map(|m| sh.presence.borrow().get(&m.id).copied().unwrap_or(false))
                    .unwrap_or(false),
            }
        })
        .collect();
    *sh.contacts.borrow_mut() = contacts;
    sh.ui().set_conversations(ModelRc::new(VecModel::from(ui_convs)));
    refresh_suggestions(sh);
}

/// Update the chat-area header (title + typing/online status) for the selected
/// conversation.
fn refresh_chat_header(sh: &Shared) {
    let self_id = sh.self_id.get();
    let selected = sh.selected.get();
    let conv = sh
        .conversations
        .borrow()
        .iter()
        .find(|c| c.id == selected as u64)
        .cloned();
    let ui = sh.ui();
    match conv {
        Some(c) => {
            let title = conversation_title(&c, self_id);
            ui.set_chat_header_title(title.into());
            let status = chat_status(sh, &c, self_id);
            ui.set_chat_header_status(status.into());
        }
        None => {
            ui.set_chat_header_title(SharedString::default());
            ui.set_chat_header_status(SharedString::default());
        }
    }
}

/// Status line for a conversation: "… is typing" takes priority, then online
/// status (direct chats) or a count (group chats).
fn chat_status(sh: &Shared, c: &Conversation, self_id: Option<u64>) -> String {
    if let Some((name, at)) = sh.typing.borrow().get(&c.id) {
        if at.elapsed() < Duration::from_secs(3) {
            return format!("{name} is typing…");
        }
    }
    let others: Vec<u64> = c
        .members
        .iter()
        .map(|m| m.id)
        .filter(|id| Some(*id) != self_id)
        .collect();
    if others.len() == 1 {
        return match sh.presence.borrow().get(&others[0]) {
            Some(true) => "online".to_string(),
            _ => "offline".to_string(),
        };
    }
    let online = others
        .iter()
        .filter(|id| *sh.presence.borrow().get(id).unwrap_or(&false))
        .count();
    if online > 0 {
        format!("{online} online")
    } else {
        String::new()
    }
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
            let (attachment_image, file_status, file_size) = p2p_display(sh, m);

            let mut preview_title = SharedString::default();
            let mut preview_desc = SharedString::default();
            let mut preview_image = slint::Image::default();
            let mut preview_url = SharedString::default();
            if let Some(url) = first_url(&m.body) {
                let cached = sh.preview_cache.borrow().get(&url).cloned();
                match cached {
                    Some(p) => {
                        if let Some(p) = p {
                            if let Some(img) = &p.image {
                                preview_image = load_avatar_image(img).unwrap_or_default();
                            }
                            preview_title = p.title.clone().unwrap_or_default().into();
                            preview_desc = p.description.clone().unwrap_or_default().into();
                            preview_url = p.url.clone().into();
                        }
                    }
                    None => {
                        let mut requested = sh.preview_requested.borrow_mut();
                        if requested.insert(url.clone()) {
                            drop(requested);
                            sh.backend.fetch_link_preview(&sh.server(), url);
                        }
                    }
                }
            }

            let preview_image_ratio = {
                let size = preview_image.size();
                if size.width > 0 {
                    size.height as f32 / size.width as f32
                } else {
                    0.0
                }
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
                file_id: m.file_id.clone().unwrap_or_default().into(),
                file_status: file_status.into(),
                file_size: file_size.into(),
                preview_title,
                preview_description: preview_desc,
                preview_image,
                preview_image_ratio,
                preview_url,
                is_edited: m.edited_at.is_some(),
                original_body: m.original_body.clone().unwrap_or_default().into(),
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
    if let Some(existing) = msgs.iter_mut().find(|x| x.id == m.id) {
        *existing = m;
    } else {
        msgs.push(m);
        msgs.sort_by_key(|m| m.id);
    }
}

fn merge_message_deleted(sh: &Shared, conversation_id: u64, message_id: u64) {
    if sh.selected.get() != conversation_id as i32 {
        return;
    }
    sh.messages.borrow_mut().retain(|m| m.id != message_id);
}

fn handle_event(sh: &Shared, ev: Event) {
    match ev {
        Event::LoggedIn { token, user } => {
            save_server(&sh.server());
            if sh.remember_me.get() {
                save_session(&sh.server(), &token);
            } else {
                clear_saved_session();
            }
            sh.token.replace(Some(token.clone()));
            sh.self_id.replace(Some(user.id));
            sh.backend.set_token(Some(token.clone()));
            let ui = sh.ui();
            ui.set_logged_in(true);
            ui.set_user_name(user.username.clone().into());
            ui.set_display_name_input(user.display_name.clone().into());
            ui.set_needs_verify(!user.email_verified);
            ui.set_email_verified(user.email_verified);
            ui.set_needs_2fa(false);
            ui.set_needs_2fa_setup(user.email_verified && !user.totp_enabled);
            ui.set_totp_enabled(user.totp_enabled);
            ui.set_error_message(SharedString::default());
            ui.set_info_message(SharedString::default());
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
            if user.email_verified && user.totp_enabled {
                sh.backend.refresh_conversations(&sh.server(), &token);
            }
        }
        Event::AuthFailed(m) => set_error(sh, &m),
        Event::Verified(u) => {
            if u.email_verified {
                let ui = sh.ui();
                ui.set_needs_verify(false);
                ui.set_email_verified(true);
                ui.set_error_message(SharedString::default());
                ui.set_info_message(SharedString::default());
                ui.set_user_name(u.username.clone().into());
                ui.set_needs_2fa_setup(!u.totp_enabled);
                if u.totp_enabled {
                    sh.backend.refresh_conversations(&sh.server(), &sh.token.borrow().clone().unwrap_or_default());
                }
            }
        }
        Event::UserUpdated(u) => {
            let ui = sh.ui();
            ui.set_display_name_input(u.display_name.clone().into());
            ui.set_error_message(SharedString::default());
            ui.set_info_message(SharedString::default());
            ui.set_email_verified(u.email_verified);
            ui.set_totp_enabled(u.totp_enabled);
            if u.totp_enabled {
                ui.set_twofa_setup_open(false);
                ui.set_twofa_disable_open(false);
                ui.set_needs_2fa_setup(false);
            }
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
            // Fetch initial online status for everyone we talk to.
            let ids: Vec<u64> = sh
                .conversations
                .borrow()
                .iter()
                .flat_map(|c| c.members.iter().map(|m| m.id))
                .filter(|id| Some(*id) != sh.self_id.get())
                .collect();
            if !ids.is_empty() {
                sh.backend
                    .fetch_presence(&sh.server(), &sh.token.borrow().clone().unwrap_or_default(), ids);
            }
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
        Event::WsMessageEdited(m) => {
            merge_message(sh, m.clone());
            let at_bottom = sh.ui().get_msg_at_bottom();
            refresh_messages_ui(sh);
            if at_bottom {
                scroll_to_bottom(sh);
            }
        }
        Event::WsMessageDeleted { conversation_id, message_id } => {
            merge_message_deleted(sh, conversation_id, message_id);
            refresh_messages_ui(sh);
        }
        Event::WsStatus(b) => {
            sh.ui().set_ws_connected(b);
        }
        Event::P2pSignal(sig) => {
            sh.p2p.handle_signal(sig);
        }
        Event::Typing { conversation_id, from_username, .. } => {
            sh.typing.borrow_mut().insert(conversation_id, (from_username, std::time::Instant::now()));
            refresh_chat_header(sh);
            let tx = sh.backend.tx.clone();
            let conv = conversation_id;
            slint::Timer::single_shot(Duration::from_secs(3), move || {
                let _ = tx.send(Event::TypingExpired { conversation_id: conv });
            });
        }
        Event::TypingExpired { conversation_id } => {
            let expired = {
                let t = sh.typing.borrow();
                t.get(&conversation_id)
                    .map(|(_, at)| at.elapsed() >= Duration::from_secs(3))
                    .unwrap_or(false)
            };
            if expired {
                sh.typing.borrow_mut().remove(&conversation_id);
                refresh_chat_header(sh);
            }
        }
        Event::Presence { user_id, online } => {
            sh.presence.borrow_mut().insert(user_id, online);
            refresh_conversations_ui(sh);
            refresh_chat_header(sh);
        }
        Event::PresenceBatch(map) => {
            let mut p = sh.presence.borrow_mut();
            for (id, on) in map {
                p.insert(id, on);
            }
            drop(p);
            refresh_conversations_ui(sh);
            refresh_chat_header(sh);
        }
        Event::P2pStatus { file_id, status } => {
            sh.p2p_status.borrow_mut().insert(file_id, P2pUi { status });
            refresh_messages_ui(sh);
        }
        Event::P2pProgress { file_id, received, total } => {
            let status = if total > 0 {
                let pct = ((received as f64 / total as f64) * 100.0) as u64;
                format!("receiving · {pct}%")
            } else {
                "receiving…".to_string()
            };
            sh.p2p_status.borrow_mut().insert(file_id, P2pUi { status });
            refresh_messages_ui(sh);
        }
        Event::P2pComplete { file_id, mime, name: _, bytes } => {
            std::fs::create_dir_all(files_cache_dir()).ok();
            let path = cache_path_for(&file_id);
            let _ = std::fs::write(&path, &bytes);
            sh.downloaded.borrow_mut().insert(
                file_id.clone(),
                DownloadedFile {
                    image_data: if mime.starts_with("image/") {
                        image_data_url_from_bytes(&bytes)
                    } else {
                        None
                    },
                    path: Some(path),
                },
            );
            sh.p2p_status.borrow_mut().remove(&file_id);
            refresh_messages_ui(sh);
        }
        Event::P2pFailed { file_id, reason } => {
            let status = if reason.contains("offline") || reason.contains("cancel") {
                "offline"
            } else {
                "error"
            };
            sh.p2p_status
                .borrow_mut()
                .insert(file_id, P2pUi { status: status.to_string() });
            refresh_messages_ui(sh);
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
                    load_avatar_image(&att.thumbnail).unwrap_or_default(),
                );
            } else {
                ui.set_pending_attach_image(slint::Image::default());
            }
            *sh.pending_attach.borrow_mut() = Some(att);
        }
        Event::LinkPreview(p) => {
            sh.preview_requested.borrow_mut().remove(&p.url);
            sh.preview_cache.borrow_mut().insert(p.url.clone(), Some(p));
            refresh_messages_ui(sh);
        }
        Event::LinkPreviewFailed(url) => {
            sh.preview_requested.borrow_mut().remove(&url);
            sh.preview_cache.borrow_mut().insert(url, None);
            refresh_messages_ui(sh);
        }
        Event::TwoFaRequired { pending_token } => {
            let ui = sh.ui();
            ui.set_pending_token(pending_token.into());
            ui.set_error_message(SharedString::default());
            ui.set_needs_2fa(true);
        }
        Event::TwoFaSetup { secret, qr } => {
            let ui = sh.ui();
            ui.set_twofa_secret(secret.into());
            ui.set_twofa_qr(load_avatar_image(&qr).unwrap_or_default());
            ui.set_error_message(SharedString::default());
            ui.set_twofa_setup_open(true);
        }
        Event::Info(m) => {
            sh.ui().set_info_message(m.into());
        }
        Event::Error(m) => set_error(sh, &m),
    }
}

fn logout(sh: &Shared) {
    clear_saved_session();
    sh.backend.set_token(None);
    sh.token.replace(None);
    sh.self_id.replace(None);
    sh.conversations.borrow_mut().clear();
    sh.messages.borrow_mut().clear();
    sh.contacts.borrow_mut().clear();
    sh.hidden.borrow_mut().clear();
    sh.p2p_status.borrow_mut().clear();
    sh.own_files.borrow_mut().clear();
    sh.presence.borrow_mut().clear();
    sh.typing.borrow_mut().clear();
    sh.selected.replace(-1);
    let ui = sh.ui();
    ui.set_chat_header_title(SharedString::default());
    ui.set_chat_header_status(SharedString::default());
    ui.set_logged_in(false);
    ui.set_needs_verify(false);
    ui.set_needs_2fa(false);
    ui.set_needs_2fa_setup(false);
    ui.set_email_verified(false);
    ui.set_pending_token(SharedString::default());
    ui.set_user_name(SharedString::default());
    ui.set_selected_conversation(-1);
    ui.set_error_message(SharedString::default());
    ui.set_info_message(SharedString::default());
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

/// RFC 6238 TOTP for the bot's 2FA login. `secret` is a base32-encoded shared
/// secret (same algorithm the server uses via totp_rs).
fn totp_now(secret_base32: &str) -> String {
    fn base32_decode(s: &str) -> Vec<u8> {
        const ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
        let mut bits: u64 = 0;
        let mut nbits = 0u32;
        let mut out = Vec::new();
        for &c in s.as_bytes() {
            let c = c.to_ascii_uppercase();
            let Some(val) = ALPHABET.iter().position(|&a| a == c) else {
                continue;
            };
            bits = (bits << 5) | val as u64;
            nbits += 5;
            if nbits >= 8 {
                nbits -= 8;
                out.push((bits >> nbits) as u8);
            }
        }
        out
    }

    let secret = base32_decode(secret_base32);
    let counter = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() / 30)
        .unwrap_or(0);
    let mut mac = hmac::Hmac::<sha1::Sha1>::new_from_slice(&secret).expect("hmac key");
    mac.update(&counter.to_be_bytes());
    let digest = mac.finalize().into_bytes();
    let offset = (digest[19] & 0x0f) as usize;
    let code = u32::from_be_bytes([
        digest[offset],
        digest[offset + 1],
        digest[offset + 2],
        digest[offset + 3],
    ]) & 0x7fff_ffff;
    format!("{:06}", code % 1_000_000)
}

/// Headless test bot (`--bot`): logs in, stays online, echoes messages back and
/// auto-receives P2P file transfers into `~/.feditexter_files`.
///
/// Config via env: `FEDITEXTER_BOT_SERVER` (default localhost:3000),
/// `FEDITEXTER_BOT_EMAIL`, `FEDITEXTER_BOT_PASSWORD`, `FEDITEXTER_BOT_TOTP`
/// (base32 secret).
fn run_bot() -> Result<(), slint::PlatformError> {
    let server = normalize_server(
        &std::env::var("FEDITEXTER_BOT_SERVER").unwrap_or_else(|_| "localhost:3000".into()),
    );
    let email = match std::env::var("FEDITEXTER_BOT_EMAIL") {
        Ok(e) => e,
        Err(_) => {
            eprintln!("[bot] FEDITEXTER_BOT_EMAIL is required");
            return Ok(());
        }
    };
    let password = match std::env::var("FEDITEXTER_BOT_PASSWORD") {
        Ok(p) => p,
        Err(_) => {
            eprintln!("[bot] FEDITEXTER_BOT_PASSWORD is required");
            return Ok(());
        }
    };
    let totp_secret = std::env::var("FEDITEXTER_BOT_TOTP").unwrap_or_default();

    let (tx, mut rx) = mpsc::unbounded_channel();
    let (token_tx, token_rx) = watch::channel(None);
    let (ws_tx, ws_rx) = mpsc::unbounded_channel();
    let device_id = load_or_create_device_id();
    let backend = Backend::new(tx.clone(), token_tx, ws_tx, device_id.clone());
    let (_server_tx, server_rx) = watch::channel(server.clone());
    spawn_ws(&backend.runtime, server_rx, token_rx, tx.clone(), ws_rx, device_id);
    let p2p = P2pManager::new(backend.runtime.handle().clone(), backend.ws_tx.clone(), tx.clone());
    let handle = backend.runtime.handle().clone();

    handle.block_on(async move {
        backend.login(&server, &email, &password, false);
        let (self_id, token) = loop {
            match rx.recv().await {
                Some(Event::TwoFaRequired { pending_token }) => {
                    if totp_secret.is_empty() {
                        eprintln!("[bot] 2FA required but FEDITEXTER_BOT_TOTP is not set");
                        return;
                    }
                    let code = totp_now(&totp_secret);
                    backend.login_2fa(&server, pending_token, code, false);
                }
                Some(Event::LoggedIn { token, user }) => {
                    backend.set_token(Some(token.clone()));
                    eprintln!("[bot] logged in as {} (user {}) on {server}", user.username, user.id);
                    break (user.id, token);
                }
                Some(Event::AuthFailed(m)) => {
                    eprintln!("[bot] login failed: {m}; retrying in 5s");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    backend.login(&server, &email, &password, false);
                }
                Some(_) => {}
                None => {
                    eprintln!("[bot] event channel closed");
                    return;
                }
            }
        };

        let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
        while let Some(ev) = rx.recv().await {
            match ev {
                Event::WsMessage(m) => {
                    if m.sender_id == self_id || !seen.insert(m.id) {
                        continue;
                    }
                    if let Some(fid) = &m.file_id {
                        p2p.fetch(fid, m.sender_id);
                    }
                    let reply = if let Some(fid) = &m.file_id {
                        let name = m.attachment_name.clone().unwrap_or_else(|| "file".into());
                        format!("📎 got your file \"{name}\" (id {fid})")
                    } else if m.body.trim().eq_ignore_ascii_case("ping") {
                        "pong".to_string()
                    } else if !m.body.trim().is_empty() {
                        format!("🤖 echo: {}", m.body.trim())
                    } else {
                        String::new()
                    };
                    if !reply.is_empty() {
                        eprintln!("[bot] replying to message {}: {reply}", m.id);
                        backend.send_message(&server, &token, m.conversation_id, reply, None);
                    }
                }
                Event::P2pComplete { file_id, bytes, .. } => {
                    if std::fs::create_dir_all(files_cache_dir()).is_ok() {
                        let _ = std::fs::write(cache_path_for(&file_id), &bytes);
                    }
                    eprintln!("[bot] saved received file {file_id} ({} bytes)", bytes.len());
                }
                Event::P2pFailed { file_id, reason } => {
                    eprintln!("[bot] transfer for {file_id} failed: {reason}");
                }
                _ => {}
            }
        }
    });
    Ok(())
}

fn main() -> Result<(), slint::PlatformError> {
    if std::env::args().any(|a| a == "--bot") {
        return run_bot();
    }
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
    let (ws_tx, ws_rx) = mpsc::unbounded_channel();
    let device_id = load_or_create_device_id();
    let backend = Rc::new(Backend::new(tx.clone(), token_tx, ws_tx, device_id.clone()));
    // A remembered session's server takes priority so auto-login targets the
    // same server the token belongs to.
    let saved_session = load_session();
    let default_server = normalize_server(
        &saved_session
            .as_ref()
            .map(|s| s.server.clone())
            .or_else(|| std::env::var("FEDITEXTER_SERVER").ok())
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
    spawn_ws(&backend.runtime, server_rx, token_rx, tx.clone(), ws_rx, device_id);

    let downloaded = Rc::new(RefCell::new(std::collections::HashMap::new()));
    load_cached_files(&downloaded);
    let p2p = P2pManager::new(
        backend.runtime.handle().clone(),
        backend.ws_tx.clone(),
        tx.clone(),
    );

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
        preview_cache: Rc::new(RefCell::new(std::collections::HashMap::new())),
        preview_requested: Rc::new(RefCell::new(std::collections::HashSet::new())),
        p2p,
        p2p_status: Rc::new(RefCell::new(std::collections::HashMap::new())),
        downloaded,
        own_files: Rc::new(RefCell::new(std::collections::HashMap::new())),
        remember_me: Rc::new(Cell::new(true)),
        presence: Rc::new(RefCell::new(std::collections::HashMap::new())),
        typing: Rc::new(RefCell::new(std::collections::HashMap::new())),
        last_typing_sent: Rc::new(Cell::new(std::time::Instant::now())),
    });

    // Auto-login from a remembered session: point the UI at the saved server and
    // validate the token against /api/me. On success the LoggedIn event restores
    // the full app state; on failure the session is cleared and login shows.
    if let Some(saved) = saved_session {
        let _ = server_tx.send(saved.server.clone());
        ui.set_server_input(saved.server.clone().into());
        shared.remember_me.replace(true);
        shared.backend.restore_session(saved.server, saved.token);
    }

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
        ui.on_login(move |email, password, remember| {
            sh.remember_me.replace(remember);
            let _ = server_tx.send(sh.server());
            sh.backend.login(&sh.server(), &email, &password, remember);
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
                refresh_chat_header(&sh);
                scroll_to_bottom(&sh);
                if let Some(token) = sh.token.borrow().clone() {
                    sh.backend.refresh_messages(&sh.server(), &token, id as u64);
                }
            }
        });
    }
    {
        let sh = shared.clone();
        // Broadcast "I'm typing" at most once every 2s per burst. The UI only
        // calls this when the composer text changes.
        ui.on_send_typing(move || {
            let conv = sh.selected.get();
            if conv < 0 {
                return;
            }
            if sh.last_typing_sent.get().elapsed() >= Duration::from_secs(2) {
                sh.last_typing_sent.replace(std::time::Instant::now());
                sh.backend.send_typing(conv as u64);
            }
        });
    }
    {
        let sh = shared.clone();
        ui.on_send_message(move |body| {
            if sh.ui().get_editing_message() {
                return;
            }
            if let Some(token) = sh.token.borrow().clone() {
                let conv = sh.selected.get();
                if conv >= 0 {
                    let body = body.trim().to_string();
                    let attachment = sh.pending_attach.borrow_mut().take();
                    let has_attach = attachment.is_some();
                    if !body.is_empty() || has_attach {
                        if let Some(att) = &attachment {
                            // Keep the bytes in memory so we can serve them P2P and
                            // render our own bubble; the message body only carries
                            // the file_id + thumbnail to the server.
                            sh.p2p.serve(ServingFile {
                                file_id: att.file_id.clone(),
                                mime: att.mime.clone(),
                                name: att.name.clone(),
                                size: att.file_size,
                                bytes: att.bytes.clone(),
                            });
                            sh.own_files.borrow_mut().insert(
                                att.file_id.clone(),
                                OwnFile {
                                    thumbnail: att.thumbnail.clone(),
                                    bytes: att.bytes.clone(),
                                },
                            );
                        }
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
        ui.on_submit_2fa(move |code| {
            let pending = sh.ui().get_pending_token().to_string();
            if !pending.is_empty() {
                let remember = sh.remember_me.get();
                sh.backend.login_2fa(&sh.server(), pending, code.trim().to_string(), remember);
            }
        });
    }
    {
        let sh = shared.clone();
        ui.on_twofa_setup(move || {
            if let Some(token) = sh.token.borrow().clone() {
                sh.backend.twofa_setup(&sh.server(), &token);
            }
        });
    }
    {
        let sh = shared.clone();
        ui.on_twofa_enable(move |code| {
            if let Some(token) = sh.token.borrow().clone() {
                sh.backend.twofa_enable(&sh.server(), &token, code.trim().to_string());
            }
        });
    }
    {
        let sh = shared.clone();
        ui.on_twofa_disable(move |code| {
            if let Some(token) = sh.token.borrow().clone() {
                sh.backend.twofa_disable(&sh.server(), &token, code.trim().to_string());
            }
        });
    }
    {
        ui.on_open_link(move |url| {
            open_in_browser(&url);
        });
    }
    {
        let sh = shared.clone();
        ui.on_open_image(move |id| {
            if id < 0 {
                return;
            }
            let msg = sh.messages.borrow().iter().find(|m| m.id == id as u64).cloned();
            let Some(m) = msg else { return };
            let mime = m.attachment_mime.clone().unwrap_or_default();
            let name = m.attachment_name.clone().unwrap_or_default();
            if let Some(file_id) = &m.file_id {
                // P2P file: open the bytes we hold, or (re)request the transfer.
                let is_own = sh.self_id.get() == Some(m.sender_id);
                let bytes = if is_own {
                    sh.own_files.borrow().get(file_id).map(|o| o.bytes.clone())
                } else {
                    sh.downloaded
                        .borrow()
                        .get(file_id)
                        .and_then(|d| d.path.clone())
                        .and_then(|p| std::fs::read(p).ok())
                };
                match bytes {
                    Some(bytes) => {
                        use base64::Engine;
                        let data = format!(
                            "data:{};base64,{}",
                            mime,
                            base64::engine::general_purpose::STANDARD.encode(&bytes)
                        );
                        open_attachment(&mime, &name, &data);
                    }
                    None => {
                        sh.p2p.retry_fetch(file_id, m.sender_id);
                    }
                }
            } else if let Some(data) = m.attachment_data {
                open_attachment(&mime, &name, &data);
            }
        });
    }
    {
        let sh = shared.clone();
        let server_tx = server_tx.clone();
        ui.on_settings_saved(move || {
            save_server(&sh.server());
            let accent = sh.ui().get_accent_color();
            let mut settings = load_settings().unwrap_or(LocalSettings { server: String::new(), accent: None, device_id: None });
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
                const MAX_FILE: usize = 1024 * 1024 * 1024;
                if bytes.len() > MAX_FILE {
                    let _ = tx.send(Event::Error("file too large (max 1 GB)".into()));
                    return;
                }
                let name = handle.file_name();
                let mime = mime_from_path(handle.path()).to_string();
                let file_id = uuid::Uuid::new_v4().to_string();
                let file_size = bytes.len() as u64;
                // Small thumbnail so the recipient can render the bubble before
                // the P2P transfer finishes (only for images). JPEG keeps it well
                // under the server's ~300KB thumbnail limit.
                let thumbnail = if mime.starts_with("image/") {
                    image::load_from_memory(&bytes)
                        .ok()
                        .map(|img| {
                            let max_dim = 480u32;
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
                            let _ = img.write_to(
                                &mut out,
                                image::ImageFormat::Jpeg,
                            );
                            use base64::Engine;
                            let data =
                                base64::engine::general_purpose::STANDARD.encode(out.into_inner());
                            format!("data:image/jpeg;base64,{data}")
                        })
                        .unwrap_or_default()
                } else {
                    String::new()
                };
                let _ = tx.send(Event::AttachmentPicked(Attachment {
                    mime: mime.clone(),
                    name,
                    file_id,
                    file_size,
                    thumbnail,
                    bytes,
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

    // ------------------------------------------------------------------
    // Message context menu callbacks (edit / delete)
    // ------------------------------------------------------------------
    {
        let sh = shared.clone();
        ui.on_msg_context_edit(move |msg_id| {
            let ui = sh.ui();
            let self_id = sh.self_id.get();
            // Find the message in the current conversation.
            let msg = sh.messages.borrow().iter().find(|m| m.id == msg_id as u64).cloned();
            if let Some(m) = msg {
                if self_id != Some(m.sender_id) {
                    return;
                }
                ui.set_editing_message(true);
                ui.set_editing_message_id(msg_id);
                ui.set_editing_message_body(m.body.as_str().into());
                ui.set_draft(m.body.as_str().into());
            }
        });
    }
    {
        let sh = shared.clone();
        ui.on_msg_context_delete(move |msg_id| {
            if let Some(token) = sh.token.borrow().clone() {
                let conv = sh.selected.get();
                if conv >= 0 {
                    sh.backend.delete_message(&sh.server(), &token, conv as u64, msg_id as u64);
                }
            }
        });
    }
    {
        let sh = shared.clone();
        ui.on_cancel_edit(move || {
            let ui = sh.ui();
            ui.set_editing_message(false);
            ui.set_editing_message_id(-1);
            ui.set_editing_message_body(SharedString::default());
            ui.set_draft(SharedString::default());
        });
    }
    {
        let sh = shared.clone();
        ui.on_confirm_edit(move |body| {
            let ui = sh.ui();
            let msg_id = ui.get_editing_message_id();
            let body = body.trim().to_string();
            if body.is_empty() || msg_id < 0 {
                ui.set_editing_message(false);
                ui.set_editing_message_id(-1);
                ui.set_editing_message_body(SharedString::default());
                ui.set_draft(SharedString::default());
                return;
            }
            if let Some(token) = sh.token.borrow().clone() {
                let conv = sh.selected.get();
                if conv >= 0 {
                    sh.backend.edit_message(&sh.server(), &token, conv as u64, msg_id as u64, body);
                }
            }
            ui.set_editing_message(false);
            ui.set_editing_message_id(-1);
            ui.set_editing_message_body(SharedString::default());
            ui.set_draft(SharedString::default());
        });
    }
    {
        let sh = shared.clone();
        ui.on_show_original(move |body| {
            let ui = sh.ui();
            ui.set_original_body_text(body);
            ui.set_original_open(true);
        });
    }

    ui.run()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ws_hub_event_parses_messages_and_signals() {
        // Message event (the shape the server's HubEvent::Message serializes to).
        let raw = r#"{"kind":"message","message":{"id":1,"conversation_id":17,"sender_id":31,"body":"hi","created_at":"2026-08-06T00:00:00","attachment_mime":null,"attachment_name":null,"attachment_data":null,"file_id":"abc","file_size":12345,"thumbnail_data":"data:image/jpeg;base64,xx"}}"#;
        let ev: WsHubEvent = serde_json::from_str(raw).unwrap();
        match ev {
            WsHubEvent::Message { message } => {
                assert_eq!(message.id, 1);
                assert_eq!(message.file_id.as_deref(), Some("abc"));
                assert_eq!(message.file_size, Some(12345));
                assert_eq!(message.thumbnail_data.as_deref(), Some("data:image/jpeg;base64,xx"));
                assert!(message.attachment_data.is_none());
            }
            _ => panic!("expected message event"),
        }

        // Signal event (HubEvent::Signal). target_user_id is skip_serializing.
        let raw = r#"{"kind":"signal","signal":{"file_id":"abc","type":"offer","data":"{\"sdp\":\"v=0\"}","from_username":"p2pa","from_user_id":31}}"#;
        let ev: WsHubEvent = serde_json::from_str(raw).unwrap();
        match ev {
            WsHubEvent::Signal { signal } => {
                assert_eq!(signal.file_id, "abc");
                assert_eq!(signal.kind, "offer");
                assert_eq!(signal.data.as_deref(), Some(r#"{"sdp":"v=0"}"#));
                assert_eq!(signal.from_user_id, Some(31));
            }
            _ => panic!("expected signal event"),
        }

        // Legacy message without P2P fields must still parse.
        let raw = r#"{"kind":"message","message":{"id":2,"conversation_id":17,"sender_id":31,"body":"old","created_at":"2026-08-06T00:00:00"}}"#;
        let ev: WsHubEvent = serde_json::from_str(raw).unwrap();
        match ev {
            WsHubEvent::Message { message } => {
                assert!(message.file_id.is_none());
                assert_eq!(message.body, "old");
            }
            _ => panic!("expected message event"),
        }

        // MessageEdited event.
        let raw = r#"{"kind":"message_edited","message":{"id":3,"conversation_id":17,"sender_id":31,"body":"edited text","created_at":"2026-08-06T00:00:00","edited_at":"2026-08-06T01:00:00","original_body":"old text"}}"#;
        let ev: WsHubEvent = serde_json::from_str(raw).unwrap();
        match ev {
            WsHubEvent::MessageEdited { message } => {
                assert_eq!(message.id, 3);
                assert_eq!(message.body, "edited text");
                assert!(message.edited_at.is_some());
                assert_eq!(message.original_body.as_deref(), Some("old text"));
            }
            _ => panic!("expected message_edited event"),
        }

        // MessageDeleted event.
        let raw = r#"{"kind":"message_deleted","conversation_id":17,"message_id":5}"#;
        let ev: WsHubEvent = serde_json::from_str(raw).unwrap();
        match ev {
            WsHubEvent::MessageDeleted { conversation_id, message_id } => {
                assert_eq!(conversation_id, 17);
                assert_eq!(message_id, 5);
            }
            _ => panic!("expected message_deleted event"),
        }
    }

    #[test]
    fn human_size_formats() {
        assert_eq!(human_size(512), "512 B");
        assert_eq!(human_size(2048), "2 KB");
        assert_eq!(human_size(5 * 1024 * 1024), "5.0 MB");
    }
}
