#![allow(dead_code)]

use std::collections::HashMap;
use std::time::Duration;

use iced::widget::{button, column, container, mouse_area, row, rule, scrollable, space, stack, text, text_input};
use iced::widget::scrollable::Viewport;
use iced::widget::Id;
use iced::{Element, Length, Task};

// ---------------------------------------------------------------------------
// API types
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize, Clone, Debug, Default)]
struct User {
    id: u64,
    #[serde(default)]
    email: String,
    username: String,
    #[serde(default)]
    display_name: String,
    #[serde(default = "default_true")]
    email_verified: bool,
    #[serde(default)]
    avatar_url: Option<String>,
    #[serde(default)]
    totp_enabled: bool,
}
fn default_true() -> bool { true }

#[derive(serde::Deserialize, Clone, Debug)]
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

#[derive(serde::Deserialize, Clone, Debug)]
struct Conversation {
    id: u64,
    kind: String,
    members: Vec<Member>,
}

#[derive(serde::Deserialize, Clone, Debug)]
struct ApiMsg {
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

#[derive(serde::Deserialize, Clone, Debug)]
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

#[derive(serde::Deserialize, Clone, Debug)]
struct LinkPreview {
    url: String,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    image: Option<String>,
    #[serde(skip)]
    image_handle: Option<iced::widget::image::Handle>,
}

// ---------------------------------------------------------------------------
// WebSocket hub events
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize, Clone, Debug)]
#[serde(tag = "kind", rename_all = "lowercase")]
enum WsHubEvent {
    Message { message: ApiMsg },
    #[serde(rename = "message_edited")]
    MessageEdited { message: ApiMsg },
    #[serde(rename = "message_deleted")]
    MessageDeleted { conversation_id: u64, message_id: u64 },
    Signal { signal: serde_json::Value },
    Typing {
        conversation_id: u64,
        from_user_id: u64,
        from_username: String,
    },
    Presence { user_id: u64, online: bool },
}

// ---------------------------------------------------------------------------
// Elm messages
// ---------------------------------------------------------------------------

enum LoginResult {
    Ok(String, User),
    Needs2fa(String),
    Err(String),
}

#[derive(Debug, Clone)]
enum Msg {
    LoginEmailChanged(String),
    LoginPasswordChanged(String),
    LoginServerChanged(String),
    LoginSubmit,
    LoginResult(Result<(String, User), String>),
    LoginNeeds2fa(String),
    ShowRegister(bool),
    RegisterEmailChanged(String),
    RegisterUsernameChanged(String),
    RegisterPasswordChanged(String),
    RegisterSubmit,
    RegisterResult(Result<(String, User), String>),
    TwoFaCodeChanged(String),
    TwoFaSubmit,
    TwoFaResult(Result<(String, User), String>),
    VerifyCodeChanged(String),
    VerifySubmit,
    VerifyResult(Result<User, String>),
    SelectConversation(u64),
    ConversationsLoaded(Vec<Conversation>),
    MessagesLoaded { conversation_id: u64, messages: Vec<ApiMsg> },
    DraftChanged(String),
    SendMessage,
    MessageSent(Result<ApiMsg, String>),
    StartEdit(u64),
    CancelEdit,
    ConfirmEdit,
    EditResult(Result<ApiMsg, String>),
    DeleteMessage(u64),
    DeleteResult(u64),
    ShowOriginal(String),
    CloseOriginal,
    WsConnected,
    WsDisconnected,
    WsEvent(WsHubEvent),
    TypingExpired,
    ShowProfile(u64),
    ProfileLoaded(Profile),
    CloseProfile,
    DisplayNameChanged(String),
    SaveSettings,
    SettingsResult(Result<User, String>),
    ToggleSettings,
    Error(String),
    Info(String),
    RefreshConversations,
    CreateConversation,
    Tick,
    Scrolled(Viewport),
    JumpToBottom,
    Logout,
    ContextMenu { msg_id: u64, sender_id: u64, x: f32, y: f32 },
    CloseContextMenu,
    OpenLink(String),
    MouseClicked(iced::mouse::Event),
    MouseMoved(iced::mouse::Event),
    ClickedOutside,
    LinkPreviewLoaded { url: String, preview: Option<LinkPreview> },
    FetchLinkPreview(String),
    ConvContextMenu { conv_id: u64, x: f32, y: f32 },
    CloseConvContextMenu,
    DeleteConversation(u64),
    DeleteConversationResult(Result<(), String>),
    Resized(iced::Size),
}

// ---------------------------------------------------------------------------
// App state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Screen {
    Login,
    Register,
    Verify,
    TwoFa,
    Chat,
}

struct AppState {
    screen: Screen,
    server: String,
    login_email: String,
    login_password: String,
    register_email: String,
    register_username: String,
    register_password: String,
    twofa_code: String,
    verify_code: String,
    token: Option<String>,
    pending_token: Option<String>,
    user: Option<User>,
    error: String,
    info: String,
    conversations: Vec<Conversation>,
    messages: Vec<ApiMsg>,
    selected_conversation: Option<u64>,
    draft: String,
    unread: HashMap<u64, u32>,
    editing_message_id: Option<u64>,
    original_body_text: Option<String>,
    profile: Option<Profile>,
    profile_open: bool,
    settings_open: bool,
    display_name_input: String,
    presence: HashMap<u64, bool>,
    typing: HashMap<u64, (String, std::time::Instant)>,
    msg_scroll_id: Id,
    scrolled_away: bool,
    context_menu_msg: Option<u64>,
    context_menu_pos: Option<(f32, f32)>,
    conv_menu_conv: Option<u64>,
    conv_menu_pos: Option<(f32, f32)>,
    cursor_pos: iced::Point,
    link_previews: HashMap<String, LinkPreview>,
    ws_connected: bool,
    window_size: iced::Size,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            screen: Screen::Login,
            server: "https://dergdungeon.com.au".to_string(),
            login_email: String::new(),
            login_password: String::new(),
            register_email: String::new(),
            register_username: String::new(),
            register_password: String::new(),
            twofa_code: String::new(),
            verify_code: String::new(),
            token: None,
            pending_token: None,
            user: None,
            error: String::new(),
            info: String::new(),
            conversations: Vec::new(),
            messages: Vec::new(),
            selected_conversation: None,
            draft: String::new(),
            unread: HashMap::new(),
            editing_message_id: None,
            original_body_text: None,
            profile: None,
            profile_open: false,
            settings_open: false,
            display_name_input: String::new(),
            presence: HashMap::new(),
            typing: HashMap::new(),
            msg_scroll_id: Id::unique(),
            scrolled_away: false,
            context_menu_msg: None,
            context_menu_pos: None,
            conv_menu_conv: None,
            conv_menu_pos: None,
            cursor_pos: iced::Point::ORIGIN,
            link_previews: HashMap::new(),
            ws_connected: false,
            window_size: iced::Size::new(1024.0, 768.0),
        }
    }
}

// ---------------------------------------------------------------------------
// Device ID (persistent, like the Slint client)
// ---------------------------------------------------------------------------

fn device_id() -> String {
    let path = dirs_next::home_dir().unwrap_or_default().join(".feditexter_device_id");
    if let Ok(id) = std::fs::read_to_string(&path) {
        let id = id.trim().to_string();
        if !id.is_empty() { return id; }
    }
    let id = uuid::Uuid::new_v4().to_string();
    let _ = std::fs::write(&path, &id);
    id
}

fn make_client() -> reqwest::Client {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("X-Device-Id", device_id().parse().unwrap());
    reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn normalize_server(input: &str) -> String {
    let s = input.trim();
    if s.starts_with("http://") || s.starts_with("https://") {
        s.trim_end_matches('/').to_string()
    } else {
        format!("https://{s}")
    }
}

fn sender_name(members: &[Member], sender_id: u64, self_id: Option<u64>) -> String {
    if self_id == Some(sender_id) {
        return "You".to_string();
    }
    members.iter()
        .find(|m| m.id == sender_id)
        .map(|m| {
            if m.display_name.is_empty() { m.username.clone() }
            else { m.display_name.clone() }
        })
        .unwrap_or_else(|| format!("user {sender_id}"))
}

fn format_local_time(ts: &str) -> String {
    if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(ts, "%Y-%m-%dT%H:%M:%S") {
        let local = naive + chrono::Duration::hours(10);
        local.format("%I:%M %p").to_string()
    } else {
        ts.to_string()
    }
}

// ---------------------------------------------------------------------------
// Boot
// ---------------------------------------------------------------------------

fn boot() -> (AppState, Task<Msg>) {
    (AppState::default(), Task::none())
}

fn main() -> iced::Result {
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).try_init();
    std::panic::set_hook(Box::new(|info| {
        eprintln!("PANIC: {info}");
        let bt = std::backtrace::Backtrace::force_capture();
        eprintln!("BACKTRACE:\n{bt}");
    }));
    iced::application(boot, update, view)
        .title(|state: &AppState| {
            match &state.user {
                Some(u) => format!("FediTexter - {}", u.username),
                None => "FediTexter".to_string(),
            }
        })
        .subscription(subscription)
        .run()
}

async fn restore_session() -> Option<(String, String, User)> {
    let path = dirs_next::home_dir()?.join(".feditexter_session");
    let data = std::fs::read_to_string(path).ok()?;
    let v: serde_json::Value = serde_json::from_str(&data).ok()?;
    let server = v.get("server")?.as_str()?.to_string();
    let token = v.get("token")?.as_str()?.to_string();
    let client = make_client();
    let resp = client.get(format!("{server}/api/me"))
        .bearer_auth(&token).send().await.ok()?;
    let v: serde_json::Value = resp.json().await.ok()?;
    let user: User = serde_json::from_value(v.get("user").cloned().unwrap_or_default()).ok()?;
    Some((server, token, user))
}

// ---------------------------------------------------------------------------
// Subscription
// ---------------------------------------------------------------------------

fn subscription(state: &AppState) -> iced::Subscription<Msg> {
    let mut subs = Vec::new();
    subs.push(iced::window::resize_events().map(|(_id, size)| Msg::Resized(size)));
    if state.token.is_some() && state.screen == Screen::Chat {
        subs.push(iced::time::every(Duration::from_secs(1)).map(|_| Msg::TypingExpired));
        subs.push(iced::event::listen_with(|event, _status, _window| {
            match event {
                iced::Event::Mouse(mouse_event) => Some(Msg::MouseClicked(mouse_event)),
                _ => None,
            }
        }));
    }
    if !subs.is_empty() {
        iced::Subscription::batch(subs)
    } else {
        iced::Subscription::none()
    }
}

// ---------------------------------------------------------------------------
// Update
// ---------------------------------------------------------------------------

fn msg_short(msg: &Msg) -> String {
    match msg {
        Msg::LoginEmailChanged(_) => "LoginEmailChanged".into(),
        Msg::LoginPasswordChanged(_) => "LoginPasswordChanged".into(),
        Msg::LoginServerChanged(_) => "LoginServerChanged".into(),
        Msg::LoginSubmit => "LoginSubmit".into(),
        Msg::LoginResult(r) => format!("LoginResult({})", if r.is_ok() { "Ok" } else { "Err" }),
        Msg::LoginNeeds2fa(_) => "LoginNeeds2fa".into(),
        Msg::ShowRegister(_) => "ShowRegister".into(),
        Msg::RegisterEmailChanged(_) => "RegisterEmailChanged".into(),
        Msg::RegisterUsernameChanged(_) => "RegisterUsernameChanged".into(),
        Msg::RegisterPasswordChanged(_) => "RegisterPasswordChanged".into(),
        Msg::RegisterSubmit => "RegisterSubmit".into(),
        Msg::RegisterResult(r) => format!("RegisterResult({})", if r.is_ok() { "Ok" } else { "Err" }),
        Msg::TwoFaCodeChanged(_) => "TwoFaCodeChanged".into(),
        Msg::TwoFaSubmit => "TwoFaSubmit".into(),
        Msg::TwoFaResult(r) => format!("TwoFaResult({})", if r.is_ok() { "Ok" } else { "Err" }),
        Msg::VerifyCodeChanged(_) => "VerifyCodeChanged".into(),
        Msg::VerifySubmit => "VerifySubmit".into(),
        Msg::VerifyResult(r) => format!("VerifyResult({})", if r.is_ok() { "Ok" } else { "Err" }),
        Msg::SelectConversation(id) => format!("SelectConversation({id})"),
        Msg::ConversationsLoaded(v) => format!("ConversationsLoaded({})", v.len()),
        Msg::MessagesLoaded { conversation_id, messages } => format!("MessagesLoaded(conv={conversation_id}, n={})", messages.len()),
        Msg::DraftChanged(_) => "DraftChanged".into(),
        Msg::SendMessage => "SendMessage".into(),
        Msg::MessageSent(r) => format!("MessageSent({})", if r.is_ok() { "Ok" } else { "Err" }),
        Msg::StartEdit(_) => "StartEdit".into(),
        Msg::CancelEdit => "CancelEdit".into(),
        Msg::ConfirmEdit => "ConfirmEdit".into(),
        Msg::EditResult(r) => format!("EditResult({})", if r.is_ok() { "Ok" } else { "Err" }),
        Msg::DeleteMessage(_) => "DeleteMessage".into(),
        Msg::DeleteResult(_) => "DeleteResult".into(),
        Msg::ShowOriginal(_) => "ShowOriginal".into(),
        Msg::CloseOriginal => "CloseOriginal".into(),
        Msg::WsConnected => "WsConnected".into(),
        Msg::WsDisconnected => "WsDisconnected".into(),
        Msg::WsEvent(_) => "WsEvent".into(),
        Msg::TypingExpired => "TypingExpired".into(),
        Msg::ShowProfile(_) => "ShowProfile".into(),
        Msg::ProfileLoaded(_) => "ProfileLoaded".into(),
        Msg::CloseProfile => "CloseProfile".into(),
        Msg::DisplayNameChanged(_) => "DisplayNameChanged".into(),
        Msg::SaveSettings => "SaveSettings".into(),
        Msg::SettingsResult(r) => format!("SettingsResult({})", if r.is_ok() { "Ok" } else { "Err" }),
        Msg::ToggleSettings => "ToggleSettings".into(),
        Msg::Error(_) => "Error".into(),
        Msg::Info(_) => "Info".into(),
        Msg::RefreshConversations => "RefreshConversations".into(),
        Msg::CreateConversation => "CreateConversation".into(),
        Msg::Tick => "Tick".into(),
        Msg::Scrolled(_) => "Scrolled".into(),
        Msg::JumpToBottom => "JumpToBottom".into(),
        Msg::Logout => "Logout".into(),
        Msg::ContextMenu { .. } => "ContextMenu".into(),
        Msg::CloseContextMenu => "CloseContextMenu".into(),
        Msg::OpenLink(_) => "OpenLink".into(),
        Msg::MouseClicked(_) => "MouseClicked".into(),
        Msg::MouseMoved(_) => "MouseMoved".into(),
        Msg::ClickedOutside => "ClickedOutside".into(),
        Msg::LinkPreviewLoaded { url, .. } => format!("LinkPreviewLoaded(url={url})"),
        Msg::FetchLinkPreview(url) => format!("FetchLinkPreview(url={url})"),
        Msg::ConvContextMenu { .. } => "ConvContextMenu".into(),
        Msg::CloseConvContextMenu => "CloseConvContextMenu".into(),
        Msg::DeleteConversation(_) => "DeleteConversation".into(),
        Msg::DeleteConversationResult(_) => "DeleteConversationResult".into(),
        Msg::Resized(_) => "Resized".into(),
    }
}

fn update(state: &mut AppState, msg: Msg) -> Task<Msg> {
    eprintln!("UPDATE: {:?}", msg_short(&msg));
    match msg {
        Msg::LoginEmailChanged(e) => { state.login_email = e; Task::none() }
        Msg::LoginPasswordChanged(p) => { state.login_password = p; Task::none() }
        Msg::LoginServerChanged(s) => { state.server = s; Task::none() }
        Msg::LoginSubmit => {
            state.error.clear();
            let email = state.login_email.clone();
            let password = state.login_password.clone();
            let server = normalize_server(&state.server);
            Task::perform(async move {
                let client = make_client();
                let resp = client.post(format!("{server}/api/login"))
                    .json(&serde_json::json!({ "email": email, "password": password }))
                    .send().await;
                match resp {
                    Ok(r) if r.status().is_success() => {
                        let v: serde_json::Value = r.json().await.unwrap_or_default();
                        if v.get("requires_2fa").and_then(|b| b.as_bool()).unwrap_or(false) {
                            let pending = v.get("pending_token").and_then(|t| t.as_str()).unwrap_or("").to_string();
                            Err(LoginResult::Needs2fa(pending))
                        } else {
                            let token = v.get("token").and_then(|t| t.as_str()).unwrap_or("").to_string();
                            let user: User = serde_json::from_value(v.get("user").cloned().unwrap_or_default()).unwrap_or_default();
                            Err(LoginResult::Ok(token, user))
                        }
                    }
                    Ok(r) => {
                        let body = r.text().await.unwrap_or_default();
                        Err(LoginResult::Err(format!("login failed: {body}")))
                    }
                    Err(e) => Err(LoginResult::Err(format!("{e}"))),
                }
            }, |r: Result<(), LoginResult>| match r {
                Ok(_) => unreachable!(),
                Err(LoginResult::Ok(t, u)) => Msg::LoginResult(Ok((t, u))),
                Err(LoginResult::Needs2fa(p)) => Msg::LoginNeeds2fa(p),
                Err(LoginResult::Err(e)) => Msg::LoginResult(Err(e)),
            })
        }
        Msg::LoginResult(Ok((token, user))) => {
            state.token = Some(token.clone());
            state.user = Some(user.clone());
            state.display_name_input = user.display_name.clone();
            if user.email_verified && user.totp_enabled {
                state.screen = Screen::Chat;
                state.ws_connected = true;
                let server = state.server.clone();
                Task::perform(load_conversations(server, token), Msg::ConversationsLoaded)
            } else if !user.email_verified {
                state.screen = Screen::Verify;
                Task::none()
            } else {
                state.screen = Screen::TwoFa;
                Task::none()
            }
        }
        Msg::LoginResult(Err(e)) => { state.error = e; Task::none() }
        Msg::LoginNeeds2fa(pending) => {
            state.pending_token = Some(pending);
            state.screen = Screen::TwoFa;
            Task::none()
        }
        Msg::ShowRegister(b) => {
            state.screen = if b { Screen::Register } else { Screen::Login };
            Task::none()
        }
        Msg::RegisterEmailChanged(e) => { state.register_email = e; Task::none() }
        Msg::RegisterUsernameChanged(u) => { state.register_username = u; Task::none() }
        Msg::RegisterPasswordChanged(p) => { state.register_password = p; Task::none() }
        Msg::RegisterSubmit => {
            state.error.clear();
            let email = state.register_email.clone();
            let username = state.register_username.clone();
            let password = state.register_password.clone();
            let server = state.server.clone();
            Task::perform(async move {
                let client = make_client();
                let resp = client.post(format!("{server}/api/register"))
                    .json(&serde_json::json!({ "email": email, "username": username, "password": password }))
                    .send().await;
                match resp {
                    Ok(r) if r.status().is_success() => {
                        let v: serde_json::Value = r.json().await.unwrap_or_default();
                        let token = v.get("token").and_then(|t| t.as_str()).unwrap_or("").to_string();
                        let user: User = serde_json::from_value(v.get("user").cloned().unwrap_or_default()).unwrap_or_default();
                        Ok((token, user))
                    }
                    Ok(r) => Err(format!("register failed: {}", r.status())),
                    Err(e) => Err(format!("{e}")),
                }
            }, Msg::RegisterResult)
        }
        Msg::RegisterResult(Ok((token, user))) => {
            state.token = Some(token);
            state.user = Some(user.clone());
            state.screen = if user.email_verified { Screen::Chat } else { Screen::Verify };
            Task::none()
        }
        Msg::RegisterResult(Err(e)) => { state.error = e; Task::none() }
        Msg::TwoFaCodeChanged(c) => { state.twofa_code = c; Task::none() }
        Msg::TwoFaSubmit => {
            state.error.clear();
            let code = state.twofa_code.clone();
            let pending = state.pending_token.clone().unwrap_or_default();
            let server = state.server.clone();
            Task::perform(async move {
                let client = make_client();
                let resp = client.post(format!("{server}/api/login/2fa"))
                    .json(&serde_json::json!({ "pending_token": pending, "code": code }))
                    .send().await;
                match resp {
                    Ok(r) if r.status().is_success() => {
                        let v: serde_json::Value = r.json().await.unwrap_or_default();
                        let token = v.get("token").and_then(|t| t.as_str()).unwrap_or("").to_string();
                        let user: User = serde_json::from_value(v.get("user").cloned().unwrap_or_default()).unwrap_or_default();
                        Ok((token, user))
                    }
                    Ok(r) => {
                        let body = r.text().await.unwrap_or_default();
                        Err(format!("2fa failed: {body}"))
                    }
                    Err(e) => Err(format!("{e}")),
                }
            }, Msg::TwoFaResult)
        }
        Msg::TwoFaResult(Ok((token, user))) => {
            state.token = Some(token.clone());
            state.user = Some(user);
            state.screen = Screen::Chat;
            let server = state.server.clone();
            Task::perform(load_conversations(server, token), Msg::ConversationsLoaded)
        }
        Msg::TwoFaResult(Err(e)) => { state.error = e; Task::none() }
        Msg::VerifyCodeChanged(c) => { state.verify_code = c; Task::none() }
        Msg::VerifySubmit => {
            state.error.clear();
            let code = state.verify_code.clone();
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            Task::perform(async move {
                let client = make_client();
                let resp = client.post(format!("{server}/api/verify"))
                    .bearer_auth(&token)
                    .json(&serde_json::json!({ "code": code }))
                    .send().await;
                match resp {
                    Ok(r) if r.status().is_success() => {
                        let v: serde_json::Value = r.json().await.unwrap_or_default();
                        serde_json::from_value::<User>(v.get("user").cloned().unwrap_or_default())
                            .map_err(|_| String::from("parse error"))
                    }
                    Ok(r) => Err(format!("verify failed: {}", r.status())),
                    Err(e) => Err(format!("{e}")),
                }
            }, Msg::VerifyResult)
        }
        Msg::VerifyResult(Ok(user)) => {
            state.user = Some(user);
            state.screen = Screen::Chat;
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            Task::perform(load_conversations(server, token), Msg::ConversationsLoaded)
        }
        Msg::VerifyResult(Err(e)) => { state.error = e; Task::none() }
        Msg::SelectConversation(id) => {
            state.selected_conversation = Some(id);
            state.unread.remove(&id);
            state.messages.clear();
            state.draft.clear();
            state.editing_message_id = None;
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            Task::perform(async move { load_messages(&server, &token, id).await },
                move |msgs| Msg::MessagesLoaded { conversation_id: id, messages: msgs })
        }
        Msg::ConversationsLoaded(convs) => { state.conversations = convs; Task::none() }
        Msg::MessagesLoaded { conversation_id, messages } => {
            if state.selected_conversation == Some(conversation_id) {
                let mut tasks = Vec::new();
                for m in &messages {
                    for url in extract_urls(&m.body) {
                        if !state.link_previews.contains_key(&url) {
                            tasks.push(Task::perform(async move { Msg::FetchLinkPreview(url) }, |m| m));
                        }
                    }
                }
                state.messages = messages;
                if tasks.is_empty() {
                    Task::none()
                } else {
                    Task::batch(tasks)
                }
            } else {
                Task::none()
            }
        }
        Msg::DraftChanged(d) => { state.draft = d; Task::none() }
        Msg::SendMessage => {
            let body = state.draft.trim().to_string();
            let conv = match state.selected_conversation { Some(c) => c, None => return Task::none() };
            if body.is_empty() { return Task::none(); }
            state.draft.clear();
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            Task::perform(async move {
                let client = make_client();
                let resp = client.post(format!("{server}/api/conversations/{conv}/messages"))
                    .bearer_auth(&token)
                    .json(&serde_json::json!({ "body": body }))
                    .send().await;
                match resp {
                    Ok(r) if r.status().is_success() => {
                        let v: serde_json::Value = r.json().await.unwrap_or_default();
                        serde_json::from_value::<ApiMsg>(v.get("message").cloned().unwrap_or_default())
                            .map_err(|_| String::from("parse error"))
                    }
                    Ok(r) => Err(format!("send failed: {}", r.status())),
                    Err(e) => Err(format!("{e}")),
                }
            }, Msg::MessageSent)
        }
        Msg::MessageSent(Ok(m)) => {
            if state.selected_conversation == Some(m.conversation_id) {
                if !state.messages.iter().any(|x| x.id == m.id) {
                    state.messages.push(m);
                }
            }
            Task::none()
        }
        Msg::MessageSent(Err(e)) => { state.error = e; Task::none() }
        Msg::StartEdit(msg_id) => {
            if let Some(m) = state.messages.iter().find(|x| x.id == msg_id) {
                state.editing_message_id = Some(msg_id);
                state.draft = m.body.clone();
            }
            Task::none()
        }
        Msg::CancelEdit => { state.editing_message_id = None; state.draft.clear(); Task::none() }
        Msg::ConfirmEdit => {
            let msg_id = match state.editing_message_id { Some(id) => id, None => return Task::none() };
            let conv = match state.selected_conversation { Some(c) => c, None => return Task::none() };
            let body = state.draft.trim().to_string();
            state.editing_message_id = None;
            state.draft.clear();
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            Task::perform(async move {
                let client = make_client();
                let resp = client.patch(format!("{server}/api/conversations/{conv}/messages/{msg_id}"))
                    .bearer_auth(&token)
                    .json(&serde_json::json!({ "body": body }))
                    .send().await;
                match resp {
                    Ok(r) if r.status().is_success() => {
                        let v: serde_json::Value = r.json().await.unwrap_or_default();
                        serde_json::from_value::<ApiMsg>(v.get("message").cloned().unwrap_or_default())
                            .map_err(|_| String::from("parse error"))
                    }
                    Ok(r) => Err(format!("edit failed: {}", r.status())),
                    Err(e) => Err(format!("{e}")),
                }
            }, Msg::EditResult)
        }
        Msg::EditResult(Ok(m)) => {
            if let Some(existing) = state.messages.iter_mut().find(|x| x.id == m.id) { *existing = m; }
            Task::none()
        }
        Msg::EditResult(Err(e)) => { state.error = e; Task::none() }
        Msg::DeleteMessage(msg_id) => {
            let conv = match state.selected_conversation { Some(c) => c, None => return Task::none() };
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            Task::perform(async move {
                let client = make_client();
                let resp = client.delete(format!("{server}/api/conversations/{conv}/messages/{msg_id}"))
                    .bearer_auth(&token).send().await;
                match resp {
                    Ok(r) if r.status().is_success() => Ok(msg_id),
                    Ok(r) => Err(format!("delete failed: {}", r.status())),
                    Err(e) => Err(format!("{e}")),
                }
            }, |r| match r { Ok(id) => Msg::DeleteResult(id), Err(e) => Msg::Error(e) })
        }
        Msg::DeleteResult(msg_id) => { state.messages.retain(|m| m.id != msg_id); Task::none() }
        Msg::ShowOriginal(body) => { state.original_body_text = Some(body); Task::none() }
        Msg::CloseOriginal => { state.original_body_text = None; Task::none() }
        Msg::WsConnected => { state.ws_connected = true; Task::none() }
        Msg::WsDisconnected => { state.ws_connected = false; Task::none() }
        Msg::WsEvent(ev) => {
            match ev {
                WsHubEvent::Message { message } => {
                    let conv_id = message.conversation_id;
                    let msg_id = message.id;
                    if state.selected_conversation == Some(conv_id) {
                        if !state.messages.iter().any(|x| x.id == msg_id) {
                            state.messages.push(message);
                        }
                    } else {
                        *state.unread.entry(conv_id).or_insert(0) += 1;
                    }
                }
                WsHubEvent::MessageEdited { message } => {
                    if let Some(existing) = state.messages.iter_mut().find(|x| x.id == message.id) {
                        *existing = message;
                    }
                }
                WsHubEvent::MessageDeleted { conversation_id, message_id } => {
                    if state.selected_conversation == Some(conversation_id) {
                        state.messages.retain(|m| m.id != message_id);
                    }
                }
                WsHubEvent::Typing { conversation_id, from_username, .. } => {
                    state.typing.insert(conversation_id, (from_username, std::time::Instant::now()));
                }
                WsHubEvent::Presence { user_id, online } => {
                    state.presence.insert(user_id, online);
                }
                _ => {}
            }
            Task::none()
        }
        Msg::TypingExpired => {
            let now = std::time::Instant::now();
            state.typing.retain(|_, (_, at)| now.duration_since(*at) < Duration::from_secs(3));
            Task::none()
        }
        Msg::ShowProfile(user_id) => {
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            Task::perform(async move {
                let client = make_client();
                let resp = client.get(format!("{server}/api/users/{user_id}"))
                    .bearer_auth(&token).send().await;
                match resp {
                    Ok(r) if r.status().is_success() => {
                        let v: serde_json::Value = r.json().await.unwrap_or_default();
                        serde_json::from_value::<Profile>(v.get("user").cloned().unwrap_or_default())
                            .map_err(|_| String::from("parse error"))
                    }
                    _ => Err(String::from("failed")),
                }
            }, |r: Result<Profile, String>| match r { Ok(p) => Msg::ProfileLoaded(p), Err(e) => Msg::Error(e) })
        }
        Msg::ProfileLoaded(p) => { state.profile = Some(p); state.profile_open = true; Task::none() }
        Msg::CloseProfile => { state.profile_open = false; Task::none() }
        Msg::DisplayNameChanged(s) => { state.display_name_input = s; Task::none() }
        Msg::SaveSettings => {
            let token = state.token.clone().unwrap_or_default();
            let display_name = state.display_name_input.clone();
            let server = state.server.clone();
            Task::perform(async move {
                let client = make_client();
                let resp = client.patch(format!("{server}/api/me"))
                    .bearer_auth(&token)
                    .json(&serde_json::json!({ "display_name": display_name }))
                    .send().await;
                match resp {
                    Ok(r) if r.status().is_success() => {
                        let v: serde_json::Value = r.json().await.unwrap_or_default();
                        serde_json::from_value::<User>(v.get("user").cloned().unwrap_or_default())
                            .map_err(|_| String::from("parse error"))
                    }
                    Ok(r) => Err(format!("update failed: {}", r.status())),
                    Err(e) => Err(format!("{e}")),
                }
            }, Msg::SettingsResult)
        }
        Msg::SettingsResult(Ok(u)) => { state.user = Some(u); state.settings_open = false; Task::none() }
        Msg::SettingsResult(Err(e)) => { state.error = e; Task::none() }
        Msg::ToggleSettings => { state.settings_open = !state.settings_open; Task::none() }
        Msg::Error(e) => { state.error = e; Task::none() }
        Msg::Info(i) => { state.info = i; Task::none() }
        Msg::RefreshConversations => {
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            Task::perform(load_conversations(server, token), Msg::ConversationsLoaded)
        }
        Msg::CreateConversation => {
            let input = state.info.trim().to_string();
            if input.is_empty() { return Task::none(); }
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            Task::perform(async move {
                let client = make_client();
                let resp = client.post(format!("{server}/api/conversations"))
                    .bearer_auth(&token)
                    .json(&serde_json::json!({ "handles": [&input] }))
                    .send().await;
                match resp {
                    Ok(r) if r.status().is_success() => {
                        let v: serde_json::Value = r.json().await.unwrap_or_default();
                        serde_json::from_value::<Conversation>(v).map_err(|_| String::from("parse error"))
                    }
                    Ok(r) => {
                        let body = r.text().await.unwrap_or_default();
                        Err(format!("create failed: {body}"))
                    }
                    Err(e) => Err(format!("{e}")),
                }
            }, |r: Result<Conversation, String>| match r {
                Ok(c) => {
                    let _ = Msg::Info(String::new());
                    let _ = c.id;
                    Msg::ConversationsLoaded(vec![]) // placeholder - will reload
                }
                Err(e) => Msg::Error(e),
            })
        }
        Msg::Tick => Task::none(),
        Msg::Scrolled(viewport) => {
            let relative = viewport.relative_offset();
            state.scrolled_away = relative.y < 0.90;
            Task::none()
        }
        Msg::JumpToBottom => {
            state.scrolled_away = false;
            iced::widget::operation::scroll_to(
                state.msg_scroll_id.clone(),
                iced::widget::operation::AbsoluteOffset { x: 0.0, y: 0.0 },
            )
        }
        Msg::ContextMenu { msg_id, sender_id, x, y } => {
            eprintln!("CTXMENU msg={msg_id} x={x} y={y} cursor={:?}", state.cursor_pos);
            let self_id = state.user.as_ref().map(|u| u.id);
            if self_id == Some(sender_id) {
                state.context_menu_msg = Some(msg_id);
                state.context_menu_pos = Some((x, y));
            }
            Task::none()
        }
        Msg::CloseContextMenu => {
            state.context_menu_msg = None;
            Task::none()
        }
        Msg::ConvContextMenu { conv_id, x, y } => {
            eprintln!("CONVMENU conv={conv_id} x={x} y={y} cursor={:?}", state.cursor_pos);
            state.conv_menu_conv = Some(conv_id);
            state.conv_menu_pos = Some((x, y));
            Task::none()
        }
        Msg::CloseConvContextMenu => {
            state.conv_menu_conv = None;
            Task::none()
        }
        Msg::DeleteConversation(conv_id) => {
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            state.conv_menu_conv = None;
            Task::perform(async move {
                let client = make_client();
                let resp = client.delete(format!("{server}/api/conversations/{conv_id}"))
                    .bearer_auth(&token)
                    .send().await;
                match resp {
                    Ok(r) if r.status().is_success() => Ok(()),
                    Ok(r) => Err(format!("delete failed: {}", r.status())),
                    Err(e) => Err(format!("{e}")),
                }
            }, Msg::DeleteConversationResult)
        }
        Msg::DeleteConversationResult(Ok(())) => {
            state.conversations.retain(|c| c.id != state.selected_conversation.unwrap_or(0));
            if state.selected_conversation.is_some() {
                let id = state.selected_conversation.unwrap();
                state.conversations.retain(|c| c.id != id);
            }
            state.selected_conversation = None;
            state.messages.clear();
            state.conv_menu_conv = None;
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            Task::perform(load_conversations(server, token), Msg::ConversationsLoaded)
        }
        Msg::DeleteConversationResult(Err(e)) => { state.error = e; state.conv_menu_conv = None; Task::none() }
        Msg::Resized(size) => {
            state.window_size = size;
            Task::none()
        }
        Msg::OpenLink(url) => {
            let _ = open::that(&url);
            Task::none()
        }
        Msg::MouseClicked(mouse_event) => {
            match mouse_event {
                iced::mouse::Event::CursorMoved { position } => {
                    state.cursor_pos = position;
                    Task::none()
                }
                iced::mouse::Event::ButtonPressed(iced::mouse::Button::Left) => {
                    state.context_menu_msg = None;
                    state.context_menu_pos = None;
                    state.conv_menu_conv = None;
                    state.conv_menu_pos = None;
                    Task::none()
                }
                _ => Task::none(),
            }
        }
        Msg::MouseMoved(_) => Task::none(),
        Msg::ClickedOutside => {
            state.context_menu_msg = None;
            state.context_menu_pos = None;
            Task::none()
        }
        Msg::FetchLinkPreview(url) => {
            if state.link_previews.contains_key(&url) {
                return Task::none();
            }
            Task::perform(
                fetch_link_preview(url.clone()),
                move |preview| Msg::LinkPreviewLoaded { url: url.clone(), preview },
            )
        }
        Msg::LinkPreviewLoaded { url, preview } => {
            if let Some(mut p) = preview {
                if let Some(ref image_uri) = p.image {
                    let b64 = image_uri.rsplit(',').next().unwrap_or(image_uri.as_str());
                    if let Ok(bytes) = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64) {
                        p.image_handle = Some(iced::widget::image::Handle::from_bytes(bytes));
                    }
                }
                state.link_previews.insert(url, p);
            }
            Task::none()
        }
        Msg::Logout => {
            state.token = None;
            state.user = None;
            state.screen = Screen::Login;
            state.conversations.clear();
            state.messages.clear();
            state.selected_conversation = None;
            let _ = std::fs::remove_file(dirs_next::home_dir().unwrap_or_default().join(".feditexter_session"));
            Task::none()
        }
    }
}

async fn load_conversations(server: String, token: String) -> Vec<Conversation> {
    let client = make_client();
    let resp = client.get(format!("{server}/api/conversations"))
        .bearer_auth(&token).send().await;
    match resp {
        Ok(r) if r.status().is_success() => {
            let v: serde_json::Value = r.json().await.unwrap_or_default();
            serde_json::from_value(v.get("conversations").cloned().unwrap_or_default())
                .unwrap_or_default()
        }
        _ => Vec::new(),
    }
}

async fn load_messages(server: &str, token: &str, conv_id: u64) -> Vec<ApiMsg> {
    let client = make_client();
    let resp = client.get(format!("{server}/api/conversations/{conv_id}/messages"))
        .bearer_auth(token).send().await;
    match resp {
        Ok(r) if r.status().is_success() => {
            let v: serde_json::Value = r.json().await.unwrap_or_default();
            serde_json::from_value(v.get("messages").cloned().unwrap_or_default())
                .unwrap_or_default()
        }
        _ => Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Styles
// ---------------------------------------------------------------------------

fn bubble_sent(theme: &iced::Theme) -> iced::widget::container::Style {
    let p = theme.extended_palette();
    iced::widget::container::Style {
        background: Some(p.primary.base.color.into()),
        text_color: Some(p.primary.base.text),
        border: iced::Border {
            radius: 16.0.into(),
            ..iced::Border::default()
        },
        ..iced::widget::container::Style::default()
    }
}

fn bubble_received(theme: &iced::Theme) -> iced::widget::container::Style {
    let p = theme.extended_palette();
    iced::widget::container::Style {
        background: Some(p.background.weak.color.into()),
        text_color: Some(p.background.weak.text),
        border: iced::Border {
            radius: 16.0.into(),
            ..iced::Border::default()
        },
        ..iced::widget::container::Style::default()
    }
}

fn sidebar_style(theme: &iced::Theme) -> iced::widget::container::Style {
    let p = theme.extended_palette();
    iced::widget::container::Style {
        background: Some(p.background.weakest.color.into()),
        ..iced::widget::container::Style::default()
    }
}

fn header_style(theme: &iced::Theme) -> iced::widget::container::Style {
    let p = theme.extended_palette();
    iced::widget::container::Style {
        background: Some(p.background.weak.color.into()),
        border: iced::Border {
            width: 0.0,
            color: p.background.weak.color,
            ..iced::Border::default()
        },
        ..iced::widget::container::Style::default()
    }
}

fn composer_style(theme: &iced::Theme) -> iced::widget::container::Style {
    let p = theme.extended_palette();
    iced::widget::container::Style {
        background: Some(p.background.weakest.color.into()),
        border: iced::Border {
            width: 1.0,
            color: p.background.weak.color,
            radius: 12.0.into(),
            ..iced::Border::default()
        },
        ..iced::widget::container::Style::default()
    }
}

fn hsl_to_rgb(h: f32, s: f32, l: f32) -> iced::Color {
    let c = (1.0 - (2.0 * l - 1.0).abs()) * s;
    let x = c * (1.0 - ((h / 60.0) % 2.0 - 1.0).abs());
    let m = l - c / 2.0;
    let (r, g, b) = if h < 60.0 { (c, x, 0.0) }
    else if h < 120.0 { (x, c, 0.0) }
    else if h < 180.0 { (0.0, c, x) }
    else if h < 240.0 { (0.0, x, c) }
    else if h < 300.0 { (x, 0.0, c) }
    else { (c, 0.0, x) };
    iced::Color::from_rgb(r + m, g + m, b + m)
}

fn avatar_circle(initials: String, hue: f32) -> Element<'static, Msg> {
    let color = hsl_to_rgb(hue, 0.6, 0.4);
    container(
        text(initials).size(14).color(iced::Color::WHITE)
    )
    .width(36)
    .height(36)
    .center_x(Length::Fixed(36.0))
    .center_y(Length::Fixed(36.0))
    .style(move |_: &iced::Theme| iced::widget::container::Style {
        background: Some(color.into()),
        border: iced::Border {
            radius: 18.0.into(),
            ..iced::Border::default()
        },
        ..iced::widget::container::Style::default()
    })
    .into()
}

fn user_initials(name: &str) -> String {
    let mut chars = name.chars().filter(|c| !c.is_whitespace());
    let first = chars.next().map(|c| c.to_ascii_uppercase()).unwrap_or('?');
    let second = chars.next().map(|c| c.to_ascii_uppercase()).unwrap_or('\0');
    if second == '\0' { first.to_string() } else { format!("{first}{second}") }
}

fn name_hue(name: &str) -> f32 {
    let hash: u32 = name.bytes().fold(0u32, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u32));
    (hash % 360) as f32
}

fn extract_urls(text: &str) -> Vec<String> {
    let mut urls = Vec::new();
    for word in text.split_whitespace() {
        let cleaned: String = word.chars().filter(|c| !c.is_ascii_punctuation() || *c == '/' || *c == ':' || *c == '.' || *c == '-' || *c == '_' || *c == '%' || *c == '?' || *c == '&' || *c == '=').collect();
        if cleaned.starts_with("http://") || cleaned.starts_with("https://") {
            urls.push(cleaned);
        }
    }
    urls
}

// ---------------------------------------------------------------------------
// Client-side link preview
// ---------------------------------------------------------------------------

const PREVIEW_MAX_PAGE_BYTES: usize = 2_000_000;
const PREVIEW_MAX_IMAGE_BYTES: usize = 3_000_000;
const PREVIEW_IMAGE_MAX_DIM: u32 = 1024;

fn preview_http_client() -> reqwest::Client {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        "User-Agent",
        reqwest::header::HeaderValue::from_static(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0 Safari/537.36",
        ),
    );
    reqwest::Client::builder()
        .default_headers(headers)
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap_or_default()
}

fn preview_looks_private(url: &str) -> bool {
    let host = url
        .split("://")
        .nth(1)
        .unwrap_or(url)
        .split('/')
        .next()
        .unwrap_or("")
        .trim_start_matches('[')
        .split(']')
        .next()
        .unwrap_or("")
        .split(':')
        .next()
        .unwrap_or("");
    if host.is_empty() || host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    host.parse::<std::net::IpAddr>().is_ok_and(|ip| match ip {
        std::net::IpAddr::V4(v4) => v4.is_loopback() || v4.is_private() || v4.is_link_local(),
        std::net::IpAddr::V6(v6) => {
            v6.is_loopback() || v6.is_unspecified() || v6.is_unicast_link_local()
        }
    })
}

fn preview_meta_content(dom: &scraper::Html, selector: &scraper::Selector, prop: &str) -> Option<String> {
    dom.select(selector).find_map(|m| {
        let property = m.value().attr("property").unwrap_or("");
        let name = m.value().attr("name").unwrap_or("");
        if property == prop || name == prop {
            m.value().attr("content").map(|s| s.trim().to_string())
        } else {
            None
        }
    })
}

/// Synchronous og:meta extraction from raw HTML.
fn preview_parse_meta(base_url: &str, html: &str) -> (Option<String>, Option<String>, Option<String>) {
    let dom = scraper::Html::parse_document(html);
    let title_sel = scraper::Selector::parse("title").unwrap();
    let meta_sel = scraper::Selector::parse("meta").unwrap();

    let title = preview_meta_content(&dom, &meta_sel, "og:title")
        .or_else(|| preview_meta_content(&dom, &meta_sel, "twitter:title"))
        .or_else(|| {
            dom.select(&title_sel).next().map(|t| {
                t.text().collect::<String>().trim().to_string()
            })
        })
        .filter(|s| !s.is_empty());

    let description = preview_meta_content(&dom, &meta_sel, "og:description")
        .or_else(|| preview_meta_content(&dom, &meta_sel, "twitter:description"))
        .filter(|s| !s.is_empty());

    let image_url = preview_meta_content(&dom, &meta_sel, "og:image")
        .or_else(|| preview_meta_content(&dom, &meta_sel, "twitter:image"))
        .and_then(|raw| {
            if raw.starts_with("http://") || raw.starts_with("https://") {
                return (!preview_looks_private(&raw)).then_some(raw);
            }
            url::Url::parse(base_url)
                .ok()
                .and_then(|base| base.join(&raw).ok())
                .map(|u| u.to_string())
                .filter(|u| u.starts_with("http") && !preview_looks_private(u))
        });

    (title, description, image_url)
}

/// Parse a Bluesky post URL (bsky.app or fxbsky.app) into (handle, post_rkey).
fn preview_parse_bsky_url(url: &str) -> Option<(String, String)> {
    let parsed = url::Url::parse(url).ok()?;
    let host = parsed.host_str()?;
    if !(host == "bsky.app" || host == "fxbsky.app" || host.ends_with(".bsky.app") || host.ends_with(".fxbsky.app")) {
        return None;
    }
    let path = parsed.path();
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let (handle, rkey): (String, String) = if segments.len() >= 4 && segments[0] == "profile" && segments[2] == "post" {
        (segments[1].to_string(), segments[3].to_string())
    } else if segments.len() >= 3 && segments[0].starts_with('@') && segments[1] == "post" {
        let handle = segments[0]
            .trim_start_matches('@')
            .split('@')
            .next()
            .unwrap_or("")
            .to_string();
        (handle, segments[2].to_string())
    } else {
        return None;
    };
    Some((handle, rkey))
}

fn preview_url_encode(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            ':' | '/' | '#' => format!("%{:02X}", c as u32),
            c => c.to_string(),
        })
        .collect()
}

/// Fetch media + metadata for a Bluesky/fxbsky post via the public API.
async fn preview_bsky_post(client: &reqwest::Client, url: &str) -> Option<(Option<String>, Option<String>, Option<String>)> {
    let (handle, rkey) = preview_parse_bsky_url(url)?;
    let uri = format!("at://{handle}/app.bsky.feed.post/{rkey}");
    let api = format!(
        "https://public.api.bsky.app/xrpc/app.bsky.feed.getPostThread?uri={}&depth=0",
        preview_url_encode(&uri)
    );
    let text = client.get(&api).send().await.ok()?.text().await.ok()?;
    let v: serde_json::Value = serde_json::from_str(&text).ok()?;
    let thread = v.get("thread")?.get("post")?;

    let author = thread.get("author")?;
    let author_name = author
        .get("displayName")
        .and_then(|s| s.as_str())
        .unwrap_or_else(|| author.get("handle").and_then(|s| s.as_str()).unwrap_or(&handle))
        .to_string();
    let body = thread
        .get("record")
        .and_then(|r| r.get("text"))
        .and_then(|s| s.as_str())
        .unwrap_or_default()
        .trim()
        .to_string();
    let title = if body.is_empty() {
        author_name
    } else {
        let truncated = if body.len() > 80 { format!("{}…", &body[..80]) } else { body };
        format!("{author_name}: {truncated}")
    };

    let embed = thread.get("embed")?;
    let img = if let Some(images) = embed.get("images") {
        images.get(0)?.get("fullsize")?.as_str().map(str::to_string)
    } else if let Some(external) = embed.get("external") {
        external.get("thumb")?.as_str().map(str::to_string)
    } else if let Some(video) = embed.get("video") {
        video.get("thumbnail")?.as_str().map(str::to_string)
    } else if let Some(media) = embed.get("media") {
        media.get("thumbnail")?.as_str().map(str::to_string)
    } else {
        None
    };
    let img = img.filter(|i| !preview_looks_private(i));

    Some((Some(title), None, img))
}

/// Download an image, resize to max dimension, return a JPEG data URI.
async fn preview_fetch_image(client: &reqwest::Client, img: &str) -> Option<String> {
    let bytes = client.get(img).send().await.ok()?.bytes().await.ok()?;
    if bytes.len() > PREVIEW_MAX_IMAGE_BYTES {
        return None;
    }
    let decoded = image::load_from_memory(&bytes).ok()?;
    let (w, h) = (decoded.width(), decoded.height());
    let scaled = if w > PREVIEW_IMAGE_MAX_DIM || h > PREVIEW_IMAGE_MAX_DIM {
        let scale = PREVIEW_IMAGE_MAX_DIM as f32 / w.max(h) as f32;
        decoded.resize(
            ((w as f32) * scale) as u32,
            ((h as f32) * scale) as u32,
            image::imageops::FilterType::Lanczos3,
        )
    } else {
        decoded
    };
    let mut out = std::io::Cursor::new(Vec::new());
    scaled.write_to(&mut out, image::ImageFormat::Jpeg).ok()?;
    use base64::Engine;
    let data = base64::engine::general_purpose::STANDARD.encode(out.into_inner());
    Some(format!("data:image/jpeg;base64,{data}"))
}

async fn fetch_link_preview(url: String) -> Option<LinkPreview> {
    let client = preview_http_client();
    let mut title = None;
    let mut description = None;
    let mut image_url = None;

    // Non-Bluesky links: fetch + parse HTML meta tags.
    if preview_parse_bsky_url(&url).is_none() {
        if let Ok(resp) = client.get(&url).send().await {
            if let Ok(bytes) = resp.bytes().await {
                if bytes.len() <= PREVIEW_MAX_PAGE_BYTES {
                    let html = String::from_utf8_lossy(&bytes);
                    let (t, d, i) = preview_parse_meta(&url, &html);
                    title = t;
                    description = d;
                    image_url = i;
                }
            }
        }
    }

    // Bluesky/fxbsky: the page is JS-rendered; use the public API.
    if title.is_none() && image_url.is_none() {
        if let Some((t, d, i)) = preview_bsky_post(&client, &url).await {
            title = title.or(t);
            description = description.or(d);
            image_url = image_url.or(i);
        }
    }

    let image = if let Some(ref img) = image_url {
        preview_fetch_image(&client, img).await
    } else {
        None
    };

    Some(LinkPreview { url, title, description, image, image_handle: None })
}

// ---------------------------------------------------------------------------
// View
// ---------------------------------------------------------------------------

fn view(state: &AppState) -> Element<'_, Msg> {
    eprintln!("VIEW called, screen={:?}", state.screen);
    match state.screen {        Screen::Login | Screen::Register => view_auth(state),
        Screen::Verify => view_verify(state),
        Screen::TwoFa => view_2fa(state),
        Screen::Chat => view_chat(state),
    }
}

fn view_auth(state: &AppState) -> Element<'_, Msg> {
    let is_register = state.screen == Screen::Register;

    let logo = text("FediTexter").size(36);
    let subtitle = text(if is_register { "Create account" } else { "Sign in" }).size(16)
        .color(iced::Color::from_rgb(0.6, 0.6, 0.6));

    let server = text_input("Server URL", &state.server)
        .on_input(Msg::LoginServerChanged)
        .width(Length::Fixed(320.0));

    let email = text_input("Email", &state.login_email)
        .on_input(Msg::LoginEmailChanged)
        .width(Length::Fixed(320.0));

    let password = text_input("Password", &state.login_password)
        .on_input(Msg::LoginPasswordChanged)
        .on_submit(Msg::LoginSubmit)
        .secure(true)
        .width(Length::Fixed(320.0));

    let login_btn = if is_register {
        button("Create account").on_press(Msg::RegisterSubmit).width(Length::Fixed(320.0))
    } else {
        button("Sign in").on_press(Msg::LoginSubmit).width(Length::Fixed(320.0))
    };

    let toggle = if is_register {
        button(text("Already have an account? Sign in").size(13))
            .on_press(Msg::ShowRegister(false))
    } else {
        button(text("Create account").size(13))
            .on_press(Msg::ShowRegister(true))
    };

    let mut form = column![logo, subtitle, server, email, password, login_btn, toggle]
        .spacing(14)
        .align_x(iced::Alignment::Center);

    if !state.error.is_empty() {
        form = form.push(
            container(text(&state.error).size(13).color(iced::Color::from_rgb(0.9, 0.3, 0.2)))
                .padding(8)
                .style(|_: &iced::Theme| iced::widget::container::Style {
                    background: Some(iced::Color::from_rgba(0.9, 0.3, 0.2, 0.15).into()),
                    border: iced::Border {
                        radius: 8.0.into(),
                        color: iced::Color::from_rgb(0.9, 0.3, 0.2),
                        width: 1.0,
                    },
                    ..iced::widget::container::Style::default()
                })
        );
    }

    container(form)
        .center_x(Length::Fill)
        .center_y(Length::Fill)
        .into()
}

fn view_verify(state: &AppState) -> Element<'_, Msg> {
    let title = text("Verify email").size(28);
    let desc = text("Enter the code sent to your email").size(14)
        .color(iced::Color::from_rgb(0.6, 0.6, 0.6));
    let code_input = text_input("Code", &state.verify_code)
        .on_input(Msg::VerifyCodeChanged)
        .on_submit(Msg::VerifySubmit)
        .width(Length::Fixed(320.0));
    let verify_btn = button("Verify").on_press(Msg::VerifySubmit).width(Length::Fixed(320.0));
    let mut form = column![title, desc, code_input, verify_btn].spacing(14).align_x(iced::Alignment::Center);
    if !state.error.is_empty() {
        form = form.push(text(&state.error).color(iced::Color::from_rgb(0.9, 0.3, 0.2)));
    }
    container(form).center_x(Length::Fill).center_y(Length::Fill).into()
}

fn view_2fa(state: &AppState) -> Element<'_, Msg> {
    let title = text("Two-factor authentication").size(28);
    let desc = text("Enter your TOTP code").size(14)
        .color(iced::Color::from_rgb(0.6, 0.6, 0.6));
    let code_input = text_input("6-digit code", &state.twofa_code)
        .on_input(Msg::TwoFaCodeChanged)
        .on_submit(Msg::TwoFaSubmit)
        .width(Length::Fixed(320.0));
    let submit_btn = button("Verify").on_press(Msg::TwoFaSubmit).width(Length::Fixed(320.0));
    let mut form = column![title, desc, code_input, submit_btn].spacing(14).align_x(iced::Alignment::Center);
    if !state.error.is_empty() {
        form = form.push(text(&state.error).color(iced::Color::from_rgb(0.9, 0.3, 0.2)));
    }
    container(form).center_x(Length::Fill).center_y(Length::Fill).into()
}

fn clamp_menu_pos(state: &AppState, x: f32, y: f32, w: f32, h: f32) -> (f32, f32) {
    let win = state.window_size;
    let margin = 4.0;
    let cx = x.max(margin).min((win.width - w - margin).max(margin));
    let cy = y.max(margin).min((win.height - h - margin).max(margin));
    (cx, cy)
}

fn view_chat(state: &AppState) -> Element<'_, Msg> {
    if state.settings_open {
        return view_settings(state);
    }
    let sidebar = view_sidebar(state);
    let chat_area = view_chat_area(state);

    let main_content = row![sidebar, chat_area]
        .width(Length::Fill)
        .height(Length::Fill)
        .into();

    let mut layers: Vec<Element<'_, Msg>> = vec![main_content];

    if state.profile_open {
        if let Some(ref profile) = state.profile {
            let close_btn = button(text("Close").size(13)).on_press(Msg::CloseProfile).padding([6.0f32, 16.0]);
            let avatar = avatar_circle(user_initials(&profile.display_name), name_hue(&profile.display_name));

            let mut info = column![
                avatar,
                text(&profile.display_name).size(20),
                text(format!("@{}", profile.username)).size(14).color(iced::Color::from_rgb(0.6, 0.6, 0.6)),
                text(&profile.domain).size(12).color(iced::Color::from_rgb(0.5, 0.5, 0.5)),
            ].spacing(8).align_x(iced::Alignment::Center);

            if profile.blocked {
                info = info.push(text("Blocked").size(12).color(iced::Color::from_rgb(0.9, 0.3, 0.2)));
            }
            if profile.muted {
                info = info.push(text("Muted").size(12).color(iced::Color::from_rgb(0.9, 0.7, 0.2)));
            }

            info = info.push(close_btn);

            let card = container(info)
                .padding(24)
                .max_width(320)
                .style(|theme: &iced::Theme| {
                    let p = theme.extended_palette();
                    iced::widget::container::Style {
                        background: Some(p.background.weakest.color.into()),
                        border: iced::Border {
                            width: 1.0,
                            color: p.background.weak.color,
                            radius: 12.0.into(),
                        },
                        shadow: iced::Shadow { color: iced::Color::from_rgba(0.0, 0.0, 0.0, 0.3), offset: iced::Vector::new(0.0, 4.0), blur_radius: 8.0 },
                        ..iced::widget::container::Style::default()
                    }
                });

            let overlay = container(card)
                .center_x(Length::Fill)
                .center_y(Length::Fill)
                .width(Length::Fill)
                .height(Length::Fill)
                .style(|_: &iced::Theme| iced::widget::container::Style {
                    background: Some(iced::Color::from_rgba(0.0, 0.0, 0.0, 0.4).into()),
                    ..iced::widget::container::Style::default()
                });

            layers.push(overlay.into());
        }
    }

    if let Some(conv_id) = state.conv_menu_conv {
        let menu_items = column![
            button(text("Delete conversation").color(iced::Color::from_rgb(0.9, 0.3, 0.2)))
                .on_press(Msg::DeleteConversation(conv_id)).width(Length::Fill).padding([6.0f32, 12.0]),
            button("Close").on_press(Msg::CloseConvContextMenu).width(Length::Fill).padding([6.0f32, 12.0]),
        ].spacing(2);

        let menu = container(menu_items)
            .padding(4)
            .max_width(180)
            .style(|theme: &iced::Theme| {
                let p = theme.extended_palette();
                iced::widget::container::Style {
                    background: Some(p.background.weakest.color.into()),
                    border: iced::Border {
                        width: 1.0,
                        color: p.background.weak.color,
                        radius: 8.0.into(),
                    },
                    shadow: iced::Shadow { color: iced::Color::from_rgba(0.0, 0.0, 0.0, 0.3), offset: iced::Vector::new(0.0, 4.0), blur_radius: 8.0 },
                    ..iced::widget::container::Style::default()
                }
            });

        let (mut cx, mut cy) = state.conv_menu_pos.unwrap_or((0.0, 0.0));
        (cx, cy) = clamp_menu_pos(state, cx, cy, 190.0, 90.0);
        let inner = container(menu)
            .align_x(iced::Alignment::Start)
            .align_y(iced::Alignment::Start)
            .padding(iced::Padding::new(0.0).top(cy).right(0.0).bottom(0.0).left(cx))
            .width(Length::Fill)
            .height(Length::Fill);
        layers.push(mouse_area(inner).on_press(Msg::ClickedOutside).into());
    }

    iced::widget::Stack::from_vec(layers)
        .width(Length::Fill)
        .height(Length::Fill)
        .into()
}

fn view_settings(state: &AppState) -> Element<'_, Msg> {
    let back_btn = button(text("← Back").size(14)).on_press(Msg::ToggleSettings);
    let title = text("Settings").size(24);
    let username = state.user.as_ref().map(|u| u.username.as_str()).unwrap_or("");
    let email = state.user.as_ref().map(|u| u.email.as_str()).unwrap_or("");

    let display_name_label = text("Display name").size(14)
        .color(iced::Color::from_rgb(0.6, 0.6, 0.6));
    let display_name_input = text_input("Display name", &state.display_name_input)
        .on_input(Msg::DisplayNameChanged)
        .width(Length::Fixed(320.0));
    let save_btn = button("Save").on_press(Msg::SaveSettings).width(Length::Fixed(120.0));

    let logout_btn = button(text("Sign out").size(14).color(iced::Color::from_rgb(0.9, 0.3, 0.2)))
        .on_press(Msg::Logout);

    let content = column![
        back_btn,
        title,
        text(format!("Username: {username}")).size(14).color(iced::Color::from_rgb(0.6, 0.6, 0.6)),
        text(format!("Email: {email}")).size(14).color(iced::Color::from_rgb(0.6, 0.6, 0.6)),
        display_name_label,
        display_name_input,
        save_btn,
        rule::horizontal(1),
        logout_btn,
    ].spacing(12).padding(24).max_width(480);

    container(content)
        .center_x(Length::Fill)
        .center_y(Length::Fill)
        .into()
}

fn view_sidebar(state: &AppState) -> Element<'_, Msg> {
    let settings_btn = button(text("⚙").size(18)).on_press(Msg::ToggleSettings);

    let header = row![
        text("Conversations").size(18),
        space::horizontal(),
        settings_btn,
    ].align_y(iced::Alignment::Center).spacing(8);

    let conv_list: Element<'_, Msg> = if state.conversations.is_empty() {
        container(text("No conversations yet").size(13).color(iced::Color::from_rgb(0.5, 0.5, 0.5)))
            .center_x(Length::Fill)
            .center_y(Length::Fill)
            .into()
    } else {
        let items: Vec<Element<'_, Msg>> = state.conversations.iter().map(|c| {
            let other = c.members.iter()
                .find(|m| Some(m.id) != state.user.as_ref().map(|u| u.id));

            let name = other
                .map(|m| if m.display_name.is_empty() { m.username.as_str() } else { m.display_name.as_str() })
                .unwrap_or("Unknown");

            let initials = user_initials(name);
            let hue = name_hue(name);
            let avatar = avatar_circle(initials.clone(), hue);

            let unread = state.unread.get(&c.id).copied().unwrap_or(0);
            let online = other
                .map(|m| state.presence.get(&m.id).copied().unwrap_or(false))
                .unwrap_or(false);

            let status_dot = if online {
                text("●").size(8).color(iced::Color::from_rgb(0.3, 0.8, 0.3))
            } else {
                text("●").size(8).color(iced::Color::from_rgb(0.4, 0.4, 0.4))
            };

            let mut name_row = row![status_dot, text(name).size(14)]
                .spacing(6).align_y(iced::Alignment::Center);

            if unread > 0 {
                name_row = name_row.push(
                    container(text(format!("{unread}")).size(11).color(iced::Color::WHITE))
                        .padding([2.0f32, 6.0])
                        .style(|_: &iced::Theme| iced::widget::container::Style {
                            background: Some(iced::Color::from_rgb(0.49, 0.36, 0.88).into()),
                            border: iced::Border { radius: 10.0.into(), ..iced::Border::default() },
                            ..iced::widget::container::Style::default()
                        })
                );
            }

            let label = row![avatar, name_row].spacing(10).align_y(iced::Alignment::Center);

            let is_selected = state.selected_conversation == Some(c.id);
            let btn = button(label)
                .on_press(Msg::SelectConversation(c.id))
                .width(Length::Fill)
                .padding([8.0f32, 10.0]);

            let styled = if is_selected {
                btn.style(button::primary)
            } else {
                btn.style(button::secondary)
            };

            mouse_area(styled)
                .on_right_press(Msg::ConvContextMenu { conv_id: c.id, x: state.cursor_pos.x, y: state.cursor_pos.y })
                .into()
        }).collect();
        column(items).spacing(2).into()
    };

    let new_conv_input = text_input("New conversation (@user@domain)", &state.info)
        .on_input(Msg::Info)
        .on_submit(Msg::CreateConversation)
        .width(Length::Fill);

    let sidebar_content = column![
        header,
        scrollable(conv_list).height(Length::Fill),
        new_conv_input,
    ].spacing(8).padding(12);

    container(sidebar_content)
        .width(Length::Fixed(280.0))
        .height(Length::Fill)
        .style(sidebar_style)
        .into()
}

fn view_chat_area(state: &AppState) -> Element<'_, Msg> {
    let Some(conv_id) = state.selected_conversation else {
        return container(
            column![
                text("FediTexter").size(28),
                text("Select a conversation").size(14).color(iced::Color::from_rgb(0.5, 0.5, 0.5)),
            ].spacing(8).align_x(iced::Alignment::Center)
        )
        .center_x(Length::Fill)
        .center_y(Length::Fill)
        .into();
    };

    let conv = state.conversations.iter().find(|c| c.id == conv_id);
    let members = conv.map(|c| c.members.clone()).unwrap_or_default();
    let self_id = state.user.as_ref().map(|u| u.id);

    let other_member = conv
        .and_then(|c| c.members.iter().find(|m| Some(m.id) != self_id));

    let header_name = other_member
        .map(|m| if m.display_name.is_empty() { m.username.as_str() } else { m.display_name.as_str() })
        .unwrap_or("Unknown")
        .to_string();

    let header_initials = user_initials(&header_name);
    let header_hue = name_hue(&header_name);

    let online = other_member
        .map(|m| state.presence.get(&m.id).copied().unwrap_or(false))
        .unwrap_or(false);
    let status = if online { "Online" } else { "Offline" };
    let status_color = if online { iced::Color::from_rgb(0.3, 0.8, 0.3) } else { iced::Color::from_rgb(0.5, 0.5, 0.5) };

    let typing_text = state.typing.get(&conv_id)
        .filter(|(_, at)| at.elapsed() < Duration::from_secs(3))
        .map(|(name, _)| format!("{name} is typing…"))
        .unwrap_or_default();

    let header_avatar_btn = other_member
        .map(|m| {
            button(avatar_circle(header_initials.clone(), header_hue))
                .on_press(Msg::ShowProfile(m.id))
                .style(button::text)
                .padding(0)
        })
        .unwrap_or_else(|| button(avatar_circle(header_initials.clone(), header_hue)).style(button::text).padding(0));

    let header_content = if typing_text.is_empty() {
        row![
            header_avatar_btn,
            column![
                text(header_name).size(15),
                text(status).size(11).color(status_color),
            ].spacing(2),
        ].spacing(10).align_y(iced::Alignment::Center)
    } else {
        row![
            header_avatar_btn,
            column![
                text(header_name).size(15),
                text(typing_text).size(11).color(iced::Color::from_rgb(0.49, 0.36, 0.88)),
            ].spacing(2),
        ].spacing(10).align_y(iced::Alignment::Center)
    };

    let header = container(header_content)
        .padding([10.0f32, 16.0])
        .width(Length::Fill)
        .style(header_style);

    let msg_elements: Vec<Element<'_, Msg>> = state.messages.iter().map(|m| {
        let is_self = self_id == Some(m.sender_id);
        let sender = sender_name(&members, m.sender_id, self_id);
        let time = format_local_time(&m.created_at);

        let sender_label = if is_self { "You".to_string() } else { sender };

        let sender_name_widget = button(
            text(sender_label.clone()).size(11)
                .color(if is_self { iced::Color::from_rgba(1.0, 1.0, 1.0, 0.6) } else { iced::Color::from_rgba(0.0, 0.0, 0.0, 0.5) })
        )
        .on_press(Msg::ShowProfile(m.sender_id))
        .padding(0)
        .style(button::text);

        let sender_line = row![
            sender_name_widget,
            text(format!(" · {time}")).size(11).color(if is_self { iced::Color::from_rgba(1.0, 1.0, 1.0, 0.4) } else { iced::Color::from_rgba(0.0, 0.0, 0.0, 0.35) }),
        ].spacing(0).align_y(iced::Alignment::Center);

        let body_elements: Vec<Element<'_, Msg>> = m.body.split('\n').flat_map(|line| {
            let mut segments: Vec<Element<'_, Msg>> = Vec::new();
            let mut remaining = line;
            while !remaining.is_empty() {
                if let Some(start) = remaining.find("http://").or_else(|| remaining.find("https://")) {
                    if start > 0 {
                        segments.push(text(remaining[..start].to_string()).size(14).into());
                    }
                    let url_end = remaining[start..].find(|c: char| c.is_whitespace()).unwrap_or(remaining[start..].len());
                    let url = &remaining[start..start + url_end];
                    segments.push(
                        button(text(url).size(14).color(iced::Color::from_rgb(0.6, 0.8, 1.0)))
                            .on_press(Msg::OpenLink(url.to_string()))
                            .style(button::text)
                            .padding(0)
                            .into()
                    );
                    remaining = &remaining[start + url_end..];
                } else {
                    segments.push(text(remaining.to_string()).size(14).into());
                    remaining = "";
                }
            }
            if segments.is_empty() {
                segments.push(text("").size(14).into());
            }
            segments
        }).collect();

        let mut bubble_content = column![sender_line].spacing(2);
        for elem in body_elements {
            bubble_content = bubble_content.push(elem);
        }

        for url in extract_urls(&m.body) {
            if let Some(preview) = state.link_previews.get(&url) {
                let mut card_content = column![].spacing(4);
                if let Some(ref handle) = preview.image_handle {
                    card_content = card_content.push(
                        iced::widget::Image::new(handle.clone())
                            .width(Length::Fill)
                            .height(Length::Shrink)
                    );
                }
                if let Some(ref title) = preview.title {
                    if !title.is_empty() {
                        card_content = card_content.push(text(title.as_str()).size(13).color(iced::Color::from_rgb(0.9, 0.9, 0.9)));
                    }
                }
                if let Some(ref desc) = preview.description {
                    if !desc.is_empty() {
                        let truncated = if desc.len() > 200 { format!("{}…", &desc[..200]) } else { desc.clone() };
                        card_content = card_content.push(text(truncated).size(12).color(iced::Color::from_rgb(0.7, 0.7, 0.7)));
                    }
                }
                card_content = card_content.push(
                    button(text(&preview.url).size(11).color(iced::Color::from_rgb(0.5, 0.7, 1.0)))
                        .on_press(Msg::OpenLink(preview.url.clone()))
                        .style(button::text).padding(0)
                );
                let card = container(card_content)
                    .padding(8)
                    .max_width(380)
                    .style(|theme: &iced::Theme| {
                        let p = theme.extended_palette();
                        iced::widget::container::Style {
                            background: Some(p.background.weak.color.into()),
                            border: iced::Border {
                                width: 1.0,
                                color: p.background.weak.color,
                                radius: 8.0.into(),
                            },
                            ..iced::widget::container::Style::default()
                        }
                    });
                bubble_content = bubble_content.push(card);
            }
        }

        if m.edited_at.is_some() {
            let edit_color = if is_self { iced::Color::from_rgba(1.0, 1.0, 1.0, 0.5) } else { iced::Color::from_rgb(0.6, 0.6, 0.6) };
            bubble_content = bubble_content.push(text("edited").size(10).color(edit_color));
        }
        if let Some(ref body) = m.original_body {
            bubble_content = bubble_content.push(
                button(text("view original").size(10).color(iced::Color::from_rgb(0.7, 0.8, 1.0)))
                    .on_press(Msg::ShowOriginal(body.clone()))
            );
        }

        let style_fn = if is_self { bubble_sent as fn(&iced::Theme) -> iced::widget::container::Style } else { bubble_received };

        let bubble = container(bubble_content)
            .padding([8.0f32, 12.0])
            .max_width(420)
            .style(style_fn);

        let pfp_label = sender_label.clone();
        let pfp = avatar_circle(user_initials(&pfp_label), name_hue(&pfp_label));
        let pfp_btn = button(pfp)
            .on_press(Msg::ShowProfile(m.sender_id))
            .style(button::text)
            .padding(0);

        let bubble_wrapped = mouse_area(bubble)
            .on_right_press(Msg::ContextMenu { msg_id: m.id, sender_id: m.sender_id, x: state.cursor_pos.x, y: state.cursor_pos.y });

        let msg_row = if is_self {
            row![space::horizontal(), bubble_wrapped, pfp_btn]
                .spacing(8)
                .align_y(iced::Alignment::End)
                .padding([0.0f32, 16.0])
        } else {
            row![pfp_btn, bubble_wrapped]
                .spacing(8)
                .align_y(iced::Alignment::End)
                .padding([0.0f32, 16.0])
        };

        msg_row.into()
    }).collect();

    let messages_scroll = scrollable(
        column![
            space::vertical().height(Length::Fixed(8.0)),
            column(msg_elements).spacing(4),
            space::vertical().height(Length::Fixed(8.0)),
        ].width(Length::Fill)
    )
    .id(state.msg_scroll_id.clone())
    .height(Length::Fill)
    .on_scroll(Msg::Scrolled)
    .anchor_bottom();

    let jump_btn: Element<'_, Msg> = if state.scrolled_away {
        container(
            button(text("↓").size(18))
                .on_press(Msg::JumpToBottom)
                .style(button::primary)
                .padding(10)
        )
        .align_x(iced::Alignment::End)
        .width(Length::Fill)
        .padding(iced::Padding::new(0.0).top(0.0).right(16.0).bottom(8.0).left(0.0))
        .into()
    } else {
        space::horizontal().into()
    };

    let scroll_with_btn = column![messages_scroll, jump_btn].spacing(0).width(Length::Fill).height(Length::Fill);

    let context_overlay: Element<'_, Msg> = if let Some(msg_id) = state.context_menu_msg {
        let is_self = state.messages.iter().find(|m| m.id == msg_id)
            .map(|m| self_id == Some(m.sender_id))
            .unwrap_or(false);

        let mut menu_items = column![].spacing(2);
        if is_self {
            menu_items = menu_items.push(
                button("Edit").on_press(Msg::StartEdit(msg_id)).width(Length::Fill).padding([6.0f32, 12.0])
            );
            menu_items = menu_items.push(
                button(text("Delete").color(iced::Color::from_rgb(0.9, 0.3, 0.2)))
                    .on_press(Msg::DeleteMessage(msg_id)).width(Length::Fill).padding([6.0f32, 12.0])
            );
        }
        menu_items = menu_items.push(
            button("Close").on_press(Msg::CloseContextMenu).width(Length::Fill).padding([6.0f32, 12.0])
        );

        let menu = container(menu_items)
            .padding(4)
            .max_width(160)
            .style(|theme: &iced::Theme| {
                let p = theme.extended_palette();
                iced::widget::container::Style {
                    background: Some(p.background.weakest.color.into()),
                    border: iced::Border {
                        width: 1.0,
                        color: p.background.weak.color,
                        radius: 8.0.into(),
                    },
                    shadow: iced::Shadow { color: iced::Color::from_rgba(0.0, 0.0, 0.0, 0.3), offset: iced::Vector::new(0.0, 4.0), blur_radius: 8.0 },
                    ..iced::widget::container::Style::default()
                }
            });
        let (mut cx, mut cy) = state.context_menu_pos.unwrap_or((0.0, 0.0));
        (cx, cy) = clamp_menu_pos(state, cx, cy, 170.0, 140.0);
        let inner = container(menu)
            .align_x(iced::Alignment::Start)
            .align_y(iced::Alignment::Start)
            .padding(iced::Padding::new(0.0).top(cy).right(0.0).bottom(0.0).left(cx))
            .width(Length::Fill)
            .height(Length::Fill);
        mouse_area(inner)
            .on_press(Msg::ClickedOutside)
            .into()
    } else {
        space::horizontal().into()
    };

    let original_overlay: Element<'_, Msg> = if let Some(ref body) = state.original_body_text {
        let close_btn = button(text("Close").size(13)).on_press(Msg::CloseOriginal).padding([6.0f32, 16.0]);
        let content = column![
            text("Original message").size(16),
            text(body.as_str()).size(14),
            close_btn,
        ].spacing(12).padding(20).max_width(500);

        let overlay = container(content)
            .center_x(Length::Fill)
            .center_y(Length::Fill)
            .width(Length::Fill)
            .height(Length::Fill)
            .style(|_: &iced::Theme| iced::widget::container::Style {
                background: Some(iced::Color::from_rgba(0.0, 0.0, 0.0, 0.5).into()),
                ..iced::widget::container::Style::default()
            });
        overlay.into()
    } else {
        space::horizontal().into()
    };

    let composer: Element<'_, Msg> = if let Some(_edit_id) = state.editing_message_id {
        let cancel = button(text("Cancel").size(13)).on_press(Msg::CancelEdit).padding([6.0f32, 12.0]);
        let save = button(text("Save").size(13)).on_press(Msg::ConfirmEdit).padding([6.0f32, 12.0]).style(button::primary);
        let edit_bar = row![
            text("Editing").size(12).color(iced::Color::from_rgb(0.49, 0.36, 0.88)),
            space::horizontal(),
            cancel, save
        ].spacing(8).align_y(iced::Alignment::Center);
        column![
            edit_bar,
            text_input("Edit message…", &state.draft)
                .on_input(Msg::DraftChanged)
                .on_submit(Msg::ConfirmEdit)
                .width(Length::Fill),
        ].spacing(8).into()
    } else {
        let send_btn = button(text("↑").size(16))
            .on_press(Msg::SendMessage)
            .style(button::primary)
            .padding([8.0f32, 12.0]);
        row![
            text_input("Type a message…", &state.draft)
                .on_input(Msg::DraftChanged)
                .on_submit(Msg::SendMessage)
                .width(Length::Fill),
            send_btn,
        ].spacing(8).align_y(iced::Alignment::Center).into()
    };

    let composer_container = container(composer)
        .padding([10.0f32, 16.0])
        .width(Length::Fill)
        .style(composer_style);

    let chat_content = column![
        header,
        scroll_with_btn,
        composer_container,
    ].spacing(0);

    let content: Element<'_, Msg> = container(chat_content)
        .width(Length::Fill)
        .height(Length::Fill)
        .into();

    let has_overlay = state.context_menu_msg.is_some() || state.original_body_text.is_some();
    if has_overlay {
        stack![content, context_overlay, original_overlay].into()
    } else {
        content
    }
}
