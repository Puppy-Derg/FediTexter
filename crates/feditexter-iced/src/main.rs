#![allow(dead_code)]

mod p2p;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use iced::widget::{button, column, container, mouse_area, row, rule, scrollable, space, text, text_input};
use iced::widget::scrollable::Viewport;
use iced::widget::Id;
use iced::{Element, Length, Subscription, Task};
use p2p::{P2pEvent, P2pManager, ServingFile, SignalEvent};
use tokio::sync::mpsc::UnboundedSender;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;

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
    #[serde(default)]
    is_bot: bool,
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
    #[serde(default)]
    is_bot: bool,
}

#[derive(serde::Deserialize, Clone, Debug)]
struct Conversation {
    id: u64,
    kind: String,
    members: Vec<Member>,
}

#[derive(serde::Deserialize, Clone, Debug)]
struct SearchUser {
    id: u64,
    username: String,
    #[serde(default)]
    display_name: String,
    #[serde(default)]
    domain: String,
    #[serde(default)]
    avatar_url: Option<String>,
    #[serde(default)]
    is_bot: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NewConvKind {
    Direct,
    Group,
    Channel,
}

impl NewConvKind {
    fn label(self) -> &'static str {
        match self {
            NewConvKind::Direct => "Direct message",
            NewConvKind::Group => "Group chat",
            NewConvKind::Channel => "Channel",
        }
    }

    fn api_kind(self) -> &'static str {
        match self {
            NewConvKind::Direct => "direct",
            NewConvKind::Group => "group",
            NewConvKind::Channel => "large_group",
        }
    }

    fn description(self) -> &'static str {
        match self {
            NewConvKind::Direct => "Talk to one person",
            NewConvKind::Group => "A small group conversation",
            NewConvKind::Channel => "A large open space for many people",
        }
    }
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
    is_bot: bool,
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
    /// Source image URLs. Handles are resolved via the shared `media_handles`
    /// RAM cache (keyed by image URL) so the same image across messages reuses
    /// a single GPU texture.
    #[serde(default)]
    images: Vec<String>,
}

/// A picked-but-not-yet-sent file attachment.
#[derive(Clone, Debug)]
struct Attachment {
    mime: String,
    name: String,
    file_id: String,
    file_size: u64,
    thumbnail: String,
    bytes: Vec<u8>,
}

/// A file we sent this session, kept to render our own bubble at full res.
#[derive(Clone)]
struct OwnFile {
    thumbnail: String,
    bytes: Vec<u8>,
}

/// A file fully downloaded this session (or loaded from the on-disk cache).
#[derive(Clone)]
struct DownloadedFile {
    /// Cached image handle for image mimes.
    image_handle: Option<iced::widget::image::Handle>,
    /// On-disk cache path for the raw bytes.
    path: Option<std::path::PathBuf>,
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
    Signal { signal: SignalEvent },
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
    RememberMeChanged(bool),
    LoginSubmit,
    LoginResult(Result<(String, User), String>),
    SessionRestored(Option<(String, String, User)>),
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
    ConversationsLoaded(Result<Vec<Conversation>, String>),
    MessagesLoaded { conversation_id: u64, messages: Result<Vec<ApiMsg>, String> },
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
    PickAvatar,
    AvatarChosen(Result<Vec<u8>, String>),
    AvatarSaved(Result<User, String>),
    RemoveAvatar,
    AvatarFetched { user_id: u64, result: Result<Vec<u8>, String> },
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
    SetAccent(iced::Color),
    OpenNewConv,
    CloseNewConv,
    NewConvWithUser(u64),
    NewConvKindChanged(NewConvKind),
    NewConvSearchChanged(String),
    NewConvSearchResults(Result<Vec<SearchUser>, String>),
    NewConvToggleUser(u64),
    NewConvCreate,
    NewConvCreated(Result<Conversation, String>),
    Noop,
    ZoomIn,
    ZoomOut,
    ZoomReset,
    WsSenderReady(UnboundedSender<String>),
    P2pReady(Arc<P2pManager>),
    P2pEvent(P2pEvent),
    PickFile,
    FilePicked(Result<Attachment, String>),
    ClearAttachment,
    OpenFile(u64),
    RetryFile(u64),
    SessionExpired(String),
    ToggleBlock(u64),
    ToggleMute(u64),
    ModerationResult(Result<Profile, String>),
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
    remember_me: bool,
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
    preview_loading: HashSet<String>,
    /// RAM cache of decoded image handles, keyed by the source image URL, so
    /// the same image across messages/previews shares one GPU texture.
    media_handles: HashMap<String, iced::widget::image::Handle>,
    ws_connected: bool,
    window_size: iced::Size,
    accent: iced::Color,
    new_conv_open: bool,
    new_conv_kind: NewConvKind,
    new_conv_search: String,
    new_conv_results: Vec<SearchUser>,
    new_conv_selected: Vec<u64>,
    new_conv_busy: bool,
    busy: bool,
    loading_messages: bool,
    auth_busy: bool,
    avatar_busy: bool,
    picking_file: bool,
    zoom: f32,
    avatar_handles: HashMap<u64, iced::widget::image::Handle>,
    avatar_attempted: HashSet<u64>,
    ws_tx: Option<UnboundedSender<String>>,
    p2p: Option<Arc<P2pManager>>,
    pending_attachment: Option<Attachment>,
    own_files: HashMap<String, OwnFile>,
    downloaded: HashMap<String, DownloadedFile>,
    p2p_status: HashMap<String, String>,
    thumb_handles: HashMap<String, iced::widget::image::Handle>,
    own_full_handles: HashMap<String, iced::widget::image::Handle>,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            screen: Screen::Login,
            server: "https://dergdungeon.com.au".to_string(),
            login_email: String::new(),
            login_password: String::new(),
            remember_me: true,
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
            preview_loading: HashSet::new(),
            media_handles: HashMap::new(),
            ws_connected: false,
            window_size: iced::Size::new(1024.0, 768.0),
            accent: accent_from_file(),
            new_conv_open: false,
            new_conv_kind: NewConvKind::Direct,
            new_conv_search: String::new(),
            new_conv_results: Vec::new(),
            new_conv_selected: Vec::new(),
            new_conv_busy: false,
            busy: false,
            loading_messages: false,
            auth_busy: false,
            avatar_busy: false,
            picking_file: false,
            zoom: 1.0,
            avatar_handles: HashMap::new(),
            avatar_attempted: HashSet::new(),
            ws_tx: None,
            p2p: None,
            pending_attachment: None,
            own_files: HashMap::new(),
            downloaded: HashMap::new(),
            p2p_status: HashMap::new(),
            thumb_handles: HashMap::new(),
            own_full_handles: HashMap::new(),
        }
    }
}

trait ZoomNum {
    fn to_f32(self) -> f32;
}
impl ZoomNum for f32 { fn to_f32(self) -> f32 { self } }
impl ZoomNum for i32 { fn to_f32(self) -> f32 { self as f32 } }
impl ZoomNum for u32 { fn to_f32(self) -> f32 { self as f32 } }
impl ZoomNum for u16 { fn to_f32(self) -> f32 { self as f32 } }

impl AppState {
    fn z(&self, base: impl ZoomNum) -> f32 {
        base.to_f32() * self.zoom
    }

    fn zs(&self, base: u16) -> f32 {
        ((base as f32) * self.zoom).max(6.0)
    }
}

fn accent_from_file() -> iced::Color {
    let path = dirs_next::home_dir().unwrap_or_default().join(".feditexter_settings.json");
    if let Ok(data) = std::fs::read_to_string(&path) {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&data) {
            if let Some(hex) = v.get("accent").and_then(|x| x.as_str()) {
                if let Ok(color) = parse_hex_color(hex) {
                    return color;
                }
            }
        }
    }
    iced::Color::from_rgb(0.49, 0.36, 0.88)
}

fn parse_hex_color(s: &str) -> Result<iced::Color, ()> {
    let s = s.trim_start_matches('#');
    if s.len() != 6 {
        return Err(());
    }
    let r = u8::from_str_radix(&s[0..2], 16).map_err(|_| ())?;
    let g = u8::from_str_radix(&s[2..4], 16).map_err(|_| ())?;
    let b = u8::from_str_radix(&s[4..6], 16).map_err(|_| ())?;
    Ok(iced::Color::from_rgb8(r, g, b))
}

fn accent_to_hex(c: iced::Color) -> String {
    let r = (c.r * 255.0).round() as u8;
    let g = (c.g * 255.0).round() as u8;
    let b = (c.b * 255.0).round() as u8;
    format!("#{r:02X}{g:02X}{b:02X}")
}

fn save_accent(c: iced::Color) {
    let path = dirs_next::home_dir().unwrap_or_default().join(".feditexter_settings.json");
    let v = serde_json::json!({ "accent": accent_to_hex(c) });
    let _ = std::fs::write(&path, v.to_string());
}

fn color_eq(a: iced::Color, b: iced::Color) -> bool {
    (a.r - b.r).abs() < 0.01 && (a.g - b.g).abs() < 0.01 && (a.b - b.b).abs() < 0.01
}

fn app_theme(state: &AppState) -> iced::Theme {
    let mut p = iced::theme::Palette::DARK;
    p.primary = state.accent;
    iced::Theme::custom("FediTexter", p)
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

/// Sentinel error string returned by API helpers when the server reports the
/// session as invalid/revoked (HTTP 401), so the UI can force a re-login.
const AUTH_FAILED: &str = "__auth_failed__";

/// If `e` is the AUTH_FAILED sentinel, return a SessionExpired task; otherwise
/// store it as a plain error and continue.
fn handle_api_error(state: &mut AppState, e: String) -> Task<Msg> {
    if e == AUTH_FAILED {
        Task::done(Msg::SessionExpired(
            "Your session was invalidated (token used from another device). Please log in again.".to_string(),
        ))
    } else {
        state.error = e;
        Task::none()
    }
}

/// Classify a response: Ok(()) on success, AUTH_FAILED on 401, otherwise a
/// descriptive error. HTTP 403 (moderation, 2FA-setup) is left untouched.
fn auth_aware_error(resp: &reqwest::Response) -> Result<(), String> {
    if resp.status() == reqwest::StatusCode::UNAUTHORIZED {
        return Err(AUTH_FAILED.to_string());
    }
    if resp.status().is_success() {
        return Ok(());
    }
    Err(format!("request failed: {}", resp.status()))
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
    let mut state = AppState::default();
    load_cached_files(&mut state);
    (state, Task::perform(restore_session(), Msg::SessionRestored))
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
        .theme(app_theme)
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

/// Persist the session so the next launch auto-logs-in (used only when the user
/// ticks "Remember me").
fn save_session(server: &str, token: &str) {
    if server.is_empty() || token.is_empty() {
        return;
    }
    let path = dirs_next::home_dir().unwrap_or_default().join(".feditexter_session");
    let data = serde_json::json!({ "server": server, "token": token }).to_string();
    let _ = std::fs::write(&path, data);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
    }
}

// ---------------------------------------------------------------------------
// Subscription
// ---------------------------------------------------------------------------

fn subscription(state: &AppState) -> iced::Subscription<Msg> {
    let mut subs = Vec::new();
    subs.push(iced::window::resize_events().map(|(_id, size)| Msg::Resized(size)));
    subs.push(iced::event::listen_with(|event, _status, _window| {
        if let iced::Event::Keyboard(iced::keyboard::Event::KeyPressed { physical_key, modifiers, .. }) = event {
            let zoom_key = match physical_key {
                iced::keyboard::key::Physical::Code(iced::keyboard::key::Code::Equal)
                | iced::keyboard::key::Physical::Code(iced::keyboard::key::Code::NumpadAdd) => Some(Msg::ZoomIn),
                iced::keyboard::key::Physical::Code(iced::keyboard::key::Code::Minus)
                | iced::keyboard::key::Physical::Code(iced::keyboard::key::Code::NumpadSubtract) => Some(Msg::ZoomOut),
                iced::keyboard::key::Physical::Code(iced::keyboard::key::Code::Digit0) => Some(Msg::ZoomReset),
                _ => None,
            };
            if let Some(msg) = zoom_key
                && (modifiers.command() || modifiers.control())
            {
                return Some(msg);
            }
        }
        None
    }));
    if state.token.is_some() && state.screen == Screen::Chat {
        subs.push(iced::time::every(Duration::from_secs(1)).map(|_| Msg::TypingExpired));
        subs.push(iced::event::listen_with(|event, _status, _window| {
            match event {
                iced::Event::Mouse(mouse_event) => Some(Msg::MouseClicked(mouse_event)),
                _ => None,
            }
        }));
        if let Some(token) = state.token.clone() {
            subs.push(ws_subscription(state.server.clone(), token, device_id()));
        }
    }
    if !subs.is_empty() {
        iced::Subscription::batch(subs)
    } else {
        iced::Subscription::none()
    }
}

fn ws_subscription(server: String, token: String, device_id: String) -> Subscription<Msg> {
    Subscription::run_with(
        (server, token, device_id),
        |data: &(String, String, String)| {
            let (server, token, device_id) = data.clone();
            iced::stream::channel(100, async move |mut output| {
                ws_worker(server, token, device_id, &mut output).await;
            })
        },
    )
}

async fn ws_worker(
    server: String,
    token: String,
    device_id: String,
    output: &mut iced::futures::channel::mpsc::Sender<Msg>,
) {
    let url = ws_url(&server);
    let (ws_tx, mut ws_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let (p2p_tx, mut p2p_rx) = tokio::sync::mpsc::unbounded_channel::<P2pEvent>();
    let handle = tokio::runtime::Handle::current();
    let p2p = P2pManager::new(handle, ws_tx.clone(), p2p_tx);
    let _ = output.send(Msg::WsSenderReady(ws_tx.clone())).await;
    let _ = output.send(Msg::P2pReady(p2p.clone())).await;

    loop {
        let mut request = match url.clone().into_client_request() {
            Ok(r) => r,
            Err(_) => {
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
                let _ = output.send(Msg::WsConnected).await;
                let (mut sink, mut stream) = ws.split();
                loop {
                    let out_msg: Option<Msg> = tokio::select! {
                        outgoing = ws_rx.recv() => {
                            match outgoing {
                                Some(text) => {
                                    if sink.send(Message::Text(text.into())).await.is_err() {
                                        break;
                                    }
                                    None
                                }
                                None => return,
                            }
                        }
                        ev = p2p_rx.recv() => ev.map(Msg::P2pEvent),
                        incoming = stream.next() => {
                            match incoming {
                                Some(Ok(Message::Text(text))) => {
                                    serde_json::from_str::<WsHubEvent>(&text).ok().map(Msg::WsEvent)
                                }
                                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                                _ => None,
                            }
                        }
                    };
                    if let Some(msg) = out_msg {
                        if output.send(msg).await.is_err() {
                            return;
                        }
                    }
                }
                let _ = output.send(Msg::WsDisconnected).await;
            }
            Err(e) => {
                // A 401/403 on the handshake means the session is dead (revoked,
                // expired, or token used from another device). Stop retrying and
                // prompt the user to log in again.
                if let tokio_tungstenite::tungstenite::Error::Http(resp) = &e {
                    let status = resp.status().as_u16();
                    if status == 401 || status == 403 {
                        let _ = output.send(Msg::SessionExpired(
                            "Your session was invalidated or expired. Please log in again.".to_string(),
                        )).await;
                        return;
                    }
                }
                let _ = output.send(Msg::WsDisconnected).await;
            }
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
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

// ---------------------------------------------------------------------------
// Update
// ---------------------------------------------------------------------------

fn msg_short(msg: &Msg) -> String {
    match msg {
        Msg::LoginEmailChanged(_) => "LoginEmailChanged".into(),
        Msg::LoginPasswordChanged(_) => "LoginPasswordChanged".into(),
        Msg::LoginServerChanged(_) => "LoginServerChanged".into(),
        Msg::RememberMeChanged(_) => "RememberMeChanged".into(),
        Msg::LoginSubmit => "LoginSubmit".into(),
        Msg::LoginResult(r) => format!("LoginResult({})", if r.is_ok() { "Ok" } else { "Err" }),
        Msg::SessionRestored(_) => "SessionRestored".into(),
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
        Msg::ConversationsLoaded(v) => format!("ConversationsLoaded({})", v.as_ref().map(|x| x.len()).unwrap_or(0)),
        Msg::MessagesLoaded { conversation_id, messages } => format!("MessagesLoaded(conv={conversation_id}, n={})", messages.as_ref().map(|x| x.len()).unwrap_or(0)),
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
        Msg::PickAvatar => "PickAvatar".into(),
        Msg::AvatarChosen(_) => "AvatarChosen".into(),
        Msg::AvatarSaved(_) => "AvatarSaved".into(),
        Msg::RemoveAvatar => "RemoveAvatar".into(),
        Msg::AvatarFetched { .. } => "AvatarFetched".into(),
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
        Msg::SetAccent(_) => "SetAccent".into(),
        Msg::OpenNewConv => "OpenNewConv".into(),
        Msg::CloseNewConv => "CloseNewConv".into(),
        Msg::NewConvWithUser(_) => "NewConvWithUser".into(),
        Msg::NewConvKindChanged(_) => "NewConvKindChanged".into(),
        Msg::NewConvSearchChanged(_) => "NewConvSearchChanged".into(),
        Msg::NewConvSearchResults(_) => "NewConvSearchResults".into(),
        Msg::NewConvToggleUser(_) => "NewConvToggleUser".into(),
        Msg::NewConvCreate => "NewConvCreate".into(),
        Msg::NewConvCreated(_) => "NewConvCreated".into(),
        Msg::Noop => "Noop".into(),
        Msg::ZoomIn => "ZoomIn".into(),
        Msg::ZoomOut => "ZoomOut".into(),
        Msg::ZoomReset => "ZoomReset".into(),
        Msg::WsSenderReady(_) => "WsSenderReady".into(),
        Msg::P2pReady(_) => "P2pReady".into(),
        Msg::P2pEvent(_) => "P2pEvent".into(),
        Msg::PickFile => "PickFile".into(),
        Msg::FilePicked(_) => "FilePicked".into(),
        Msg::ClearAttachment => "ClearAttachment".into(),
        Msg::OpenFile(_) => "OpenFile".into(),
        Msg::RetryFile(_) => "RetryFile".into(),
        Msg::SessionExpired(_) => "SessionExpired".into(),
        Msg::ToggleBlock(_) => "ToggleBlock".into(),
        Msg::ToggleMute(_) => "ToggleMute".into(),
        Msg::ModerationResult(_) => "ModerationResult".into(),
    }
}

fn update(state: &mut AppState, msg: Msg) -> Task<Msg> {
    if std::env::var("FEDITEXTER_VERBOSE").is_ok() {
        eprintln!("UPDATE: {:?}", msg_short(&msg));
    }
    match msg {
        Msg::LoginEmailChanged(e) => { state.login_email = e; Task::none() }
        Msg::LoginPasswordChanged(p) => { state.login_password = p; Task::none() }
        Msg::LoginServerChanged(s) => { state.server = s; Task::none() }
        Msg::RememberMeChanged(b) => { state.remember_me = b; Task::none() }
        Msg::LoginSubmit => {
            state.error.clear();
            state.auth_busy = true;
            let email = state.login_email.clone();
            let password = state.login_password.clone();
            let remember_me = state.remember_me;
            let server = normalize_server(&state.server);
            Task::perform(async move {
                let client = make_client();
                let resp = client.post(format!("{server}/api/login"))
                    .json(&serde_json::json!({ "email": email, "password": password, "remember_me": remember_me }))
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
            state.auth_busy = false;
            if state.remember_me {
                save_session(&state.server, &token);
            }
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
        Msg::LoginResult(Err(e)) => { state.auth_busy = false; state.error = e; Task::none() }
        Msg::SessionRestored(Some((server, token, user))) => {
            state.server = server;
            state.token = Some(token.clone());
            state.user = Some(user.clone());
            state.display_name_input = user.display_name.clone();
            state.screen = Screen::Chat;
            state.ws_connected = true;
            let conv_token = token;
            Task::perform(
                load_conversations(state.server.clone(), conv_token),
                Msg::ConversationsLoaded,
            )
        }
        Msg::SessionRestored(None) => Task::none(),
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
            state.auth_busy = true;
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
            state.auth_busy = false;
            if state.remember_me {
                save_session(&state.server, &token);
            }
            state.token = Some(token);
            state.user = Some(user.clone());
            state.screen = if user.email_verified { Screen::Chat } else { Screen::Verify };
            Task::none()
        }
        Msg::RegisterResult(Err(e)) => { state.auth_busy = false; handle_api_error(state, e) }
        Msg::TwoFaCodeChanged(c) => { state.twofa_code = c; Task::none() }
        Msg::TwoFaSubmit => {
            state.error.clear();
            state.auth_busy = true;
            let code = state.twofa_code.clone();
            let pending = state.pending_token.clone().unwrap_or_default();
            let remember_me = state.remember_me;
            let server = state.server.clone();
            Task::perform(async move {
                let client = make_client();
                let resp = client.post(format!("{server}/api/login/2fa"))
                    .json(&serde_json::json!({ "pending_token": pending, "code": code, "remember_me": remember_me }))
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
            state.auth_busy = false;
            if state.remember_me {
                save_session(&state.server, &token);
            }
            state.token = Some(token.clone());
            state.user = Some(user);
            state.screen = Screen::Chat;
            let server = state.server.clone();
            Task::perform(load_conversations(server, token), Msg::ConversationsLoaded)
        }
        Msg::TwoFaResult(Err(e)) => { state.auth_busy = false; handle_api_error(state, e) }
        Msg::VerifyCodeChanged(c) => { state.verify_code = c; Task::none() }
        Msg::VerifySubmit => {
            state.error.clear();
            state.auth_busy = true;
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
            state.auth_busy = false;
            state.user = Some(user.clone());
            if user.totp_enabled {
                state.screen = Screen::Chat;
                let token = state.token.clone().unwrap_or_default();
                let server = state.server.clone();
                Task::perform(load_conversations(server, token), Msg::ConversationsLoaded)
            } else {
                state.screen = Screen::TwoFa;
                Task::none()
            }
        }
        Msg::VerifyResult(Err(e)) => handle_api_error(state, e),
        Msg::SelectConversation(id) => {
            state.selected_conversation = Some(id);
            state.unread.remove(&id);
            state.messages.clear();
            state.draft.clear();
            state.editing_message_id = None;
            state.context_menu_msg = None;
            state.context_menu_pos = None;
            state.conv_menu_conv = None;
            state.conv_menu_pos = None;
            state.loading_messages = true;
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            Task::perform(async move { load_messages(&server, &token, id).await },
                move |msgs| Msg::MessagesLoaded { conversation_id: id, messages: msgs })
        }
        Msg::ConversationsLoaded(Ok(convs)) => { state.conversations = convs; ensure_avatars(state) }
        Msg::ConversationsLoaded(Err(e)) => {
            if e == AUTH_FAILED {
                return Task::done(Msg::SessionExpired("Your session was invalidated (token used from another device). Please log in again.".to_string()));
            }
            state.error = e;
            Task::none()
        }
        Msg::MessagesLoaded { conversation_id, messages } => {
            state.loading_messages = false;
            if state.selected_conversation == Some(conversation_id) {
                let mut tasks = Vec::new();
                match messages {
                    Ok(msgs) => {
                        for m in &msgs {
                            for url in extract_urls(&m.body) {
                                if !state.link_previews.contains_key(&url) {
                                    tasks.push(Task::perform(async move { Msg::FetchLinkPreview(url) }, |m| m));
                                }
                            }
                        }
                        state.messages = msgs;
                        build_thumb_handles(state);
                        auto_fetch_files(state);
                    }
                    Err(e) => {
                        if e == AUTH_FAILED {
                            return Task::done(Msg::SessionExpired("Your session was invalidated (token used from another device). Please log in again.".to_string()));
                        }
                        state.error = e;
                    }
                }
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
            let attachment = state.pending_attachment.take();
            let has_attach = attachment.is_some();
            if body.is_empty() && !has_attach { return Task::none(); }
            state.draft.clear();
            state.busy = true;
            if let Some(att) = &attachment {
                if let Some(p2p) = &state.p2p {
                    p2p.serve(ServingFile {
                        file_id: att.file_id.clone(),
                        mime: att.mime.clone(),
                        name: att.name.clone(),
                        size: att.file_size,
                        bytes: att.bytes.clone(),
                    });
                }
                if att.mime.starts_with("image/") {
                    state.own_full_handles.insert(att.file_id.clone(), iced::widget::image::Handle::from_bytes(att.bytes.clone()));
                }
                state.own_files.insert(
                    att.file_id.clone(),
                    OwnFile { thumbnail: att.thumbnail.clone(), bytes: att.bytes.clone() },
                );
                // Persist the bytes to the on-disk cache so our own sent file
                // survives a restart (the bubble can then render it full-res and
                // OpenFile can read it back — otherwise the bytes are RAM-only
                // and P2P-fetching from ourselves makes no sense).
                let path = cache_path_for(&att.file_id);
                if let Some(dir) = path.parent() {
                    let _ = std::fs::create_dir_all(dir);
                }
                let _ = std::fs::write(&path, &att.bytes);
                let image_handle = if att.mime.starts_with("image/") {
                    Some(iced::widget::image::Handle::from_bytes(att.bytes.clone()))
                } else {
                    None
                };
                state.downloaded.insert(
                    att.file_id.clone(),
                    DownloadedFile { image_handle, path: Some(path) },
                );
            }
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            Task::perform(async move {
                let client = make_client();
                let mut payload = serde_json::json!({ "body": body });
                if let Some(att) = attachment {
                    payload["attachment_mime"] = serde_json::json!(att.mime);
                    payload["attachment_name"] = serde_json::json!(att.name);
                    payload["file_id"] = serde_json::json!(att.file_id);
                    payload["file_size"] = serde_json::json!(att.file_size);
                    if !att.thumbnail.is_empty() {
                        payload["thumbnail_data"] = serde_json::json!(att.thumbnail);
                    }
                }
                let resp = client.post(format!("{server}/api/conversations/{conv}/messages"))
                    .bearer_auth(&token)
                    .json(&payload)
                    .send().await;
                match resp {
                    Ok(r) if r.status().is_success() => {
                        let v: serde_json::Value = r.json().await.unwrap_or_default();
                        serde_json::from_value::<ApiMsg>(v.get("message").cloned().unwrap_or_default())
                            .map_err(|_| String::from("parse error"))
                    }
                    Ok(r) => {
                        auth_aware_error(&r)?;
                        Err(format!("send failed: {}", r.status()))
                    }
                    Err(e) => Err(format!("{e}")),
                }
            }, Msg::MessageSent)
        }
        Msg::MessageSent(Ok(m)) => {
            state.busy = false;
            if state.selected_conversation == Some(m.conversation_id) {
                if !state.messages.iter().any(|x| x.id == m.id) {
                    state.messages.push(m);
                }
                build_thumb_handles(state);
            }
            Task::none()
        }
        Msg::MessageSent(Err(e)) => { state.busy = false; handle_api_error(state, e) }
        Msg::StartEdit(msg_id) => {
            state.context_menu_msg = None;
            state.context_menu_pos = None;
            if let Some(m) = state.messages.iter().find(|x| x.id == msg_id) {
                state.editing_message_id = Some(msg_id);
                state.draft = m.body.clone();
            }
            Task::none()
        }
        Msg::CancelEdit => { state.editing_message_id = None; state.draft.clear(); state.context_menu_msg = None; state.context_menu_pos = None; Task::none() }
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
                    Ok(r) => {
                        auth_aware_error(&r)?;
                        Err(format!("edit failed: {}", r.status()))
                    }
                    Err(e) => Err(format!("{e}")),
                }
            }, Msg::EditResult)
        }
        Msg::EditResult(Ok(m)) => {
            if let Some(existing) = state.messages.iter_mut().find(|x| x.id == m.id) { *existing = m; }
            Task::none()
        }
        Msg::EditResult(Err(e)) => handle_api_error(state, e),
        Msg::DeleteMessage(msg_id) => {
            state.context_menu_msg = None;
            state.context_menu_pos = None;
            let conv = match state.selected_conversation { Some(c) => c, None => return Task::none() };
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            Task::perform(async move {
                let client = make_client();
                let resp = client.delete(format!("{server}/api/conversations/{conv}/messages/{msg_id}"))
                    .bearer_auth(&token).send().await;
                match resp {
                    Ok(r) if r.status().is_success() => Ok(msg_id),
                    Ok(r) => {
                        auth_aware_error(&r)?;
                        Err(format!("delete failed: {}", r.status()))
                    }
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
                        build_thumb_handles(state);
                        auto_fetch_files(state);
                        Task::none()
                    } else {
                        *state.unread.entry(conv_id).or_insert(0) += 1;
                        if state.conversations.iter().any(|c| c.id == conv_id) {
                            Task::none()
                        } else {
                            // New conversation we don't know about yet (e.g. the
                            // bot reached out) — refresh the list so it shows up.
                            let server = state.server.clone();
                            let token = state.token.clone().unwrap_or_default();
                            Task::perform(load_conversations(server, token), Msg::ConversationsLoaded)
                        }
                    }
                }
                WsHubEvent::MessageEdited { message } => {
                    if let Some(existing) = state.messages.iter_mut().find(|x| x.id == message.id) {
                        *existing = message;
                    }
                    Task::none()
                }
                WsHubEvent::MessageDeleted { conversation_id, message_id } => {
                    if state.selected_conversation == Some(conversation_id) {
                        state.messages.retain(|m| m.id != message_id);
                    }
                    Task::none()
                }
                WsHubEvent::Typing { conversation_id, from_username, .. } => {
                    state.typing.insert(conversation_id, (from_username, std::time::Instant::now()));
                    Task::none()
                }
                WsHubEvent::Presence { user_id, online } => {
                    state.presence.insert(user_id, online);
                    Task::none()
                }
                WsHubEvent::Signal { signal } => {
                    if let Some(p2p) = &state.p2p {
                        p2p.handle_signal(signal);
                    }
                    Task::none()
                }
            }
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
                    Ok(r) => {
                        auth_aware_error(&r)?;
                        Err(String::from("failed"))
                    }
                    Err(e) => Err(format!("{e}")),
                }
            }, |r: Result<Profile, String>| match r { Ok(p) => Msg::ProfileLoaded(p), Err(e) => Msg::Error(e) })
        }
        Msg::ProfileLoaded(p) => { state.profile = Some(p); state.profile_open = true; ensure_avatars(state) }
        Msg::CloseProfile => { state.profile_open = false; Task::none() }
        Msg::ToggleBlock(user_id) => {
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            let currently_blocked = state.profile.as_ref().map(|p| p.blocked).unwrap_or(false);
            Task::perform(
                moderation_action(server, token, user_id, if currently_blocked { "unblock" } else { "block" }),
                Msg::ModerationResult,
            )
        }
        Msg::ToggleMute(user_id) => {
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            let currently_muted = state.profile.as_ref().map(|p| p.muted).unwrap_or(false);
            Task::perform(
                moderation_action(server, token, user_id, if currently_muted { "unmute" } else { "mute" }),
                Msg::ModerationResult,
            )
        }
        Msg::ModerationResult(Ok(p)) => {
            state.profile = Some(p);
            ensure_avatars(state)
        }
        Msg::ModerationResult(Err(e)) => handle_api_error(state, e),
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
                    Ok(r) => {
                        auth_aware_error(&r)?;
                        Err(format!("update failed: {}", r.status()))
                    }
                    Err(e) => Err(format!("{e}")),
                }
            }, Msg::SettingsResult)
        }
        Msg::SettingsResult(Ok(u)) => {
            state.user = Some(u);
            state.settings_open = false;
            ensure_avatars(state)
        }
        Msg::SettingsResult(Err(e)) => handle_api_error(state, e),
        Msg::PickAvatar => {
            Task::perform(
                async {
                    let path = rfd::FileDialog::new()
                        .add_filter("Images", &["png", "jpg", "jpeg", "webp", "gif", "bmp", "ico"])
                        .pick_file();
                    match path {
                        Some(p) => std::fs::read(&p).map_err(|e| format!("could not read file: {e}")),
                        None => Err("no file selected".to_string()),
                    }
                },
                Msg::AvatarChosen,
            )
        }
        Msg::AvatarChosen(Ok(bytes)) => {
            state.avatar_busy = true;
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            Task::perform(upload_avatar(server, token, bytes), Msg::AvatarSaved)
        }
        Msg::AvatarChosen(Err(e)) => { state.error = format!("avatar: {e}"); Task::none() }
        Msg::AvatarSaved(Ok(u)) => {
            state.avatar_busy = false;
            state.avatar_handles.remove(&u.id);
            state.avatar_attempted.remove(&u.id);
            state.user = Some(u);
            state.info = "Profile picture updated".to_string();
            ensure_avatars(state)
        }
        Msg::AvatarSaved(Err(e)) => { state.avatar_busy = false; handle_api_error(state, format!("avatar upload failed: {e}")) }
        Msg::RemoveAvatar => {
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            Task::perform(
                async move {
                    let client = make_client();
                    let resp = client
                        .post(format!("{server}/api/me/avatar"))
                        .bearer_auth(&token)
                        .json(&serde_json::json!({ "avatar": "" }))
                        .send()
                        .await;
                    match resp {
                        Ok(r) if r.status().is_success() => {
                            let v: serde_json::Value = r.json().await.unwrap_or_default();
                            serde_json::from_value::<User>(v.get("user").cloned().unwrap_or_default())
                                .map_err(|_| String::from("parse error"))
                        }
                        Ok(r) => {
                            auth_aware_error(&r)?;
                            Err(format!("update failed: {}", r.status()))
                        }
                        Err(e) => Err(format!("{e}")),
                    }
                },
                Msg::AvatarSaved,
            )
        }
        Msg::AvatarFetched { user_id, result } => {
            if let Ok(bytes) = result {
                if let Some(handle) = make_avatar_handle(bytes) {
                    state.avatar_handles.insert(user_id, handle);
                }
            }
            Task::none()
        }
        Msg::WsSenderReady(tx) => { state.ws_tx = Some(tx); Task::none() }
        Msg::P2pReady(p2p) => {
            state.p2p = Some(p2p);
            auto_fetch_files(state);
            Task::none()
        }
        Msg::P2pEvent(ev) => {
            match ev {
                P2pEvent::Status { file_id, status } => {
                    state.p2p_status.insert(file_id, status);
                }
                P2pEvent::Progress { file_id, received, total } => {
                    let status = if total > 0 {
                        let pct = ((received as f64 / total as f64) * 100.0) as u64;
                        format!("receiving · {pct}%")
                    } else {
                        "receiving…".to_string()
                    };
                    state.p2p_status.insert(file_id, status);
                }
                P2pEvent::Complete { file_id, mime, name: _, bytes } => {
                    std::fs::create_dir_all(files_cache_dir()).ok();
                    let path = cache_path_for(&file_id);
                    let _ = std::fs::write(&path, &bytes);
                    let image_handle = if mime.starts_with("image/") {
                        iced::widget::image::Handle::from_bytes(bytes)
                    } else {
                        iced::widget::image::Handle::from_rgba(1, 1, vec![0, 0, 0, 0])
                    };
                    let image_handle = if mime.starts_with("image/") { Some(image_handle) } else { None };
                    state.downloaded.insert(file_id.clone(), DownloadedFile {
                        image_handle,
                        path: Some(path),
                    });
                    state.p2p_status.remove(&file_id);
                }
                P2pEvent::Failed { file_id, reason } => {
                    let status = if reason.contains("offline") || reason.contains("cancel") {
                        "offline"
                    } else {
                        "error"
                    };
                    state.p2p_status.insert(file_id, status.to_string());
                }
            }
            Task::none()
        }
        Msg::PickFile => {
            Task::perform(
                async {
                    let file = rfd::AsyncFileDialog::new().pick_file().await;
                    let Some(handle) = file else { return Err("no file selected".to_string()); };
                    let bytes = handle.read().await;
                    const MAX_FILE: usize = 1024 * 1024 * 1024;
                    if bytes.len() > MAX_FILE {
                        return Err("file too large (max 1 GB)".to_string());
                    }
                    let name = handle.file_name().to_string();
                    let mime = mime_from_path(handle.path()).to_string();
                    let file_id = uuid::Uuid::new_v4().to_string();
                    let file_size = bytes.len() as u64;
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
                                let _ = img.write_to(&mut out, image::ImageFormat::Jpeg);
                                use base64::Engine;
                                let data = base64::engine::general_purpose::STANDARD.encode(out.into_inner());
                                format!("data:image/jpeg;base64,{data}")
                            })
                            .unwrap_or_default()
                    } else {
                        String::new()
                    };
                    Ok(Attachment { mime, name, file_id, file_size, thumbnail, bytes })
                },
                Msg::FilePicked,
            )
        }
        Msg::FilePicked(Ok(att)) => {
            state.picking_file = false;
            if !att.thumbnail.is_empty() {
                if let Some(bytes) = data_url_bytes(&att.thumbnail) {
                    state.thumb_handles.insert(att.file_id.clone(), iced::widget::image::Handle::from_bytes(bytes));
                }
            }
            state.pending_attachment = Some(att);
            Task::none()
        }
        Msg::FilePicked(Err(e)) => { state.picking_file = false; state.error = e; Task::none() }
        Msg::ClearAttachment => { state.pending_attachment = None; Task::none() }
        Msg::OpenFile(msg_id) => {
            let m = state.messages.iter().find(|m| m.id == msg_id).cloned();
            let Some(m) = m else { return Task::none() };
            let Some(file_id) = &m.file_id else { return Task::none() };
            let mime = m.attachment_mime.clone().unwrap_or_default();
            let name = m.attachment_name.clone().unwrap_or_default();
            let is_own = state.user.as_ref().map(|u| u.id) == Some(m.sender_id);
            let bytes = if is_own {
                state.own_files.get(file_id).map(|o| o.bytes.clone())
                    .or_else(|| {
                        state.downloaded.get(file_id)
                            .and_then(|d| d.path.clone())
                            .and_then(|p| std::fs::read(p).ok())
                    })
            } else {
                state.downloaded.get(file_id)
                    .and_then(|d| d.path.clone())
                    .and_then(|p| std::fs::read(p).ok())
            };
            match bytes {
                Some(bytes) => { open_bytes(&mime, &name, &bytes); Task::none() }
                None => {
                    if let Some(p2p) = state.p2p.clone() {
                        p2p.retry_fetch(file_id, m.sender_id);
                    }
                    Task::none()
                }
            }
        }
        Msg::RetryFile(msg_id) => {
            let m = state.messages.iter().find(|m| m.id == msg_id).cloned();
            let Some(m) = m else { return Task::none() };
            let Some(file_id) = &m.file_id else { return Task::none() };
            if let Some(p2p) = state.p2p.clone() {
                p2p.retry_fetch(file_id, m.sender_id);
            }
            Task::none()
        }
        Msg::ToggleSettings => { state.settings_open = !state.settings_open; Task::none() }
        Msg::Error(e) => handle_api_error(state, e),
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
                    Msg::ConversationsLoaded(Ok(vec![])) // placeholder - will reload
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
            if std::env::var("FEDITEXTER_VERBOSE").is_ok() {
                eprintln!("CTXMENU msg={msg_id} x={x} y={y} cursor={:?}", state.cursor_pos);
            }
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
            if std::env::var("FEDITEXTER_VERBOSE").is_ok() {
                eprintln!("CONVMENU conv={conv_id} x={x} y={y} cursor={:?}", state.cursor_pos);
            }
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
                    Ok(r) => {
                        auth_aware_error(&r)?;
                        Err(format!("delete failed: {}", r.status()))
                    }
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
        Msg::DeleteConversationResult(Err(e)) => { state.conv_menu_conv = None; handle_api_error(state, e) }
        Msg::Resized(size) => {
            state.window_size = size;
            Task::none()
        }
        Msg::SetAccent(color) => {
            state.accent = color;
            save_accent(color);
            Task::none()
        }
        Msg::OpenNewConv => {
            state.context_menu_msg = None;
            state.context_menu_pos = None;
            state.conv_menu_conv = None;
            state.conv_menu_pos = None;
            state.new_conv_open = true;
            state.new_conv_kind = NewConvKind::Direct;
            state.new_conv_search.clear();
            state.new_conv_results.clear();
            state.new_conv_selected.clear();
            let server = state.server.clone();
            let token = state.token.clone().unwrap_or_default();
            Task::perform(
                search_users_api(server, token, String::new()),
                Msg::NewConvSearchResults,
            )
        }
        Msg::CloseNewConv => {
            state.new_conv_open = false;
            state.new_conv_results.clear();
            state.new_conv_selected.clear();
            Task::none()
        }
        Msg::NewConvWithUser(user_id) => {
            state.profile_open = false;
            state.context_menu_msg = None;
            state.context_menu_pos = None;
            state.conv_menu_conv = None;
            state.conv_menu_pos = None;
            state.new_conv_open = true;
            state.new_conv_kind = NewConvKind::Direct;
            state.new_conv_search.clear();
            state.new_conv_selected.clear();
            state.new_conv_results.clear();
            if let Some(p) = state.profile.as_ref().filter(|p| p.id == user_id) {
                state.new_conv_results.push(SearchUser {
                    id: p.id,
                    username: p.username.clone(),
                    display_name: p.display_name.clone(),
                    domain: p.domain.clone(),
                    avatar_url: p.avatar_url.clone(),
                    is_bot: false,
                });
                state.new_conv_selected.push(user_id);
            }
            Task::none()
        }
        Msg::NewConvKindChanged(kind) => {
            state.new_conv_kind = kind;
            state.new_conv_selected.clear();
            Task::none()
        }
        Msg::NewConvSearchChanged(q) => {
            state.new_conv_search = q.clone();
            let server = state.server.clone();
            let token = state.token.clone().unwrap_or_default();
            Task::perform(
                search_users_api(server, token, q),
                Msg::NewConvSearchResults,
            )
        }
        Msg::NewConvSearchResults(result) => {
            match result {
                Ok(users) => {
                    state.new_conv_results = users;
                    return ensure_avatars(state);
                }
                Err(e) => return handle_api_error(state, e),
            }
        }
        Msg::NewConvToggleUser(id) => {
            if state.new_conv_selected.contains(&id) {
                state.new_conv_selected.retain(|x| *x != id);
            } else {
                if state.new_conv_kind == NewConvKind::Direct && !state.new_conv_selected.is_empty() {
                    state.new_conv_selected.clear();
                }
                state.new_conv_selected.push(id);
            }
            Task::none()
        }
        Msg::NewConvCreate => {
            let valid = match state.new_conv_kind {
                NewConvKind::Direct => state.new_conv_selected.len() == 1,
                NewConvKind::Group => state.new_conv_selected.len() >= 2,
                NewConvKind::Channel => !state.new_conv_selected.is_empty(),
            };
            if !valid {
                return Task::none();
            }
            let server = state.server.clone();
            let token = state.token.clone().unwrap_or_default();
            let kind = state.new_conv_kind.api_kind().to_string();
            let members = state.new_conv_selected.clone();
            state.new_conv_busy = true;
            Task::perform(
                create_conversation_api(server, token, kind, members),
                Msg::NewConvCreated,
            )
        }
        Msg::NewConvCreated(result) => {
            state.new_conv_busy = false;
            match result {
                Ok(conv) => {
                    state.new_conv_open = false;
                    state.new_conv_results.clear();
                    state.new_conv_selected.clear();
                    state.selected_conversation = Some(conv.id);
                    let token = state.token.clone().unwrap_or_default();
                    let server = state.server.clone();
                    Task::perform(load_conversations(server, token), Msg::ConversationsLoaded)
                }
                Err(e) => return handle_api_error(state, e),
            }
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
            state.preview_loading.insert(url.clone());
            Task::perform(
                fetch_link_preview(url.clone()),
                move |preview| Msg::LinkPreviewLoaded { url: url.clone(), preview },
            )
        }
        Msg::LinkPreviewLoaded { url, preview } => {
            if let Some(p) = preview {
                // Decode each image's processed JPEG once and keep the handle in
                // the RAM cache keyed by image URL, so the same image shared by
                // several messages reuses one GPU texture.
                for img in &p.images {
                    if state.media_handles.contains_key(img) {
                        continue;
                    }
                    if let Ok(bytes) = std::fs::read(preview_cache_path(img)) {
                        state.media_handles.insert(img.clone(), iced::widget::image::Handle::from_bytes(bytes));
                    }
                }
                state.link_previews.insert(url.clone(), p);
            }
            state.preview_loading.remove(&url);
            Task::none()
        }
        Msg::Logout => {
            state.token = None;
            state.user = None;
            state.screen = Screen::Login;
            state.conversations.clear();
            state.messages.clear();
            state.selected_conversation = None;
            state.pending_attachment = None;
            state.own_files.clear();
            state.own_full_handles.clear();
            state.p2p_status.clear();
            state.ws_tx = None;
            state.p2p = None;
            state.busy = false;
            state.loading_messages = false;
            state.auth_busy = false;
            state.avatar_busy = false;
            state.picking_file = false;
            state.preview_loading.clear();
            let _ = std::fs::remove_file(dirs_next::home_dir().unwrap_or_default().join(".feditexter_session"));
            Task::none()
        }
        Msg::SessionExpired(reason) => {
            state.token = None;
            state.user = None;
            state.screen = Screen::Login;
            state.conversations.clear();
            state.messages.clear();
            state.selected_conversation = None;
            state.pending_attachment = None;
            state.own_files.clear();
            state.own_full_handles.clear();
            state.p2p_status.clear();
            state.ws_tx = None;
            state.p2p = None;
            state.busy = false;
            state.loading_messages = false;
            state.auth_busy = false;
            state.avatar_busy = false;
            state.picking_file = false;
            state.preview_loading.clear();
            let _ = std::fs::remove_file(dirs_next::home_dir().unwrap_or_default().join(".feditexter_session"));
            state.error = reason;
            Task::none()
        }
        Msg::Noop => Task::none(),
        Msg::ZoomIn => { state.zoom = (state.zoom * 1.1).min(1.5); Task::none() }
        Msg::ZoomOut => { state.zoom = (state.zoom / 1.1).max(0.75); Task::none() }
        Msg::ZoomReset => { state.zoom = 1.0; Task::none() }
    }
}

async fn load_conversations(server: String, token: String) -> Result<Vec<Conversation>, String> {
    let client = make_client();
    let resp = client.get(format!("{server}/api/conversations"))
        .bearer_auth(&token).send().await;
    match resp {
        Ok(r) => {
            auth_aware_error(&r)?;
            let v: serde_json::Value = r.json().await.unwrap_or_default();
            Ok(serde_json::from_value(v.get("conversations").cloned().unwrap_or_default())
                .unwrap_or_default())
        }
        Err(e) => Err(format!("{e}")),
    }
}

async fn search_users_api(server: String, token: String, q: String) -> Result<Vec<SearchUser>, String> {
    let client = make_client();
    let url = format!("{server}/api/users/search?q={}", url::form_urlencoded::byte_serialize(q.as_bytes()).collect::<String>());
    let resp = client.get(&url).bearer_auth(&token).send().await;
    match resp {
        Ok(r) => {
            auth_aware_error(&r)?;
            let v: serde_json::Value = r.json().await.unwrap_or_default();
            serde_json::from_value(v.get("users").cloned().unwrap_or_default())
                .map_err(|e| format!("parse error: {e}"))
        }
        Err(e) => Err(format!("{e}")),
    }
}

async fn create_conversation_api(
    server: String,
    token: String,
    kind: String,
    member_ids: Vec<u64>,
) -> Result<Conversation, String> {
    let client = make_client();
    let resp = client
        .post(format!("{server}/api/conversations"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "member_ids": member_ids, "kind": kind }))
        .send()
        .await;
    match resp {
        Ok(r) if r.status().is_success() => {
            let v: serde_json::Value = r.json().await.unwrap_or_default();
            serde_json::from_value(v).map_err(|e| format!("parse error: {e}"))
        }
        Ok(r) => {
            auth_aware_error(&r)?;
            let body = r.text().await.unwrap_or_default();
            Err(format!("create failed: {body}"))
        }
        Err(e) => Err(format!("{e}")),
    }
}

async fn load_messages(server: &str, token: &str, conv_id: u64) -> Result<Vec<ApiMsg>, String> {
    let client = make_client();
    let resp = client.get(format!("{server}/api/conversations/{conv_id}/messages"))
        .bearer_auth(token).send().await;
    match resp {
        Ok(r) => {
            auth_aware_error(&r)?;
            let v: serde_json::Value = r.json().await.unwrap_or_default();
            Ok(serde_json::from_value(v.get("messages").cloned().unwrap_or_default())
                .unwrap_or_default())
        }
        Err(e) => Err(format!("{e}")),
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

fn danger_text_button(theme: &iced::Theme, status: button::Status) -> button::Style {
    let mut style = button::secondary(theme, status);
    style.text_color = theme.extended_palette().danger.base.text;
    style
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

fn avatar_circle(initials: String, hue: f32, zoom: f32) -> Element<'static, Msg> {
    avatar_circle_sized(initials, hue, zoom, 36.0)
}

fn avatar_circle_sized(initials: String, hue: f32, zoom: f32, size: f32) -> Element<'static, Msg> {
    let color = hsl_to_rgb(hue, 0.6, 0.4);
    let size = size * zoom;
    container(
        text(initials).size((14.0 * zoom).max(6.0)).color(iced::Color::WHITE)
    )
    .width(size)
    .height(size)
    .center_x(Length::Fixed(size))
    .center_y(Length::Fixed(size))
    .style(move |_: &iced::Theme| iced::widget::container::Style {
        background: Some(color.into()),
        border: iced::Border {
            radius: (size / 2.0).into(),
            ..iced::Border::default()
        },
        ..iced::widget::container::Style::default()
    })
    .into()
}

/// Render a user's profile picture if we have one cached, else initials.
fn avatar_element<'a>(state: &'a AppState, user_id: u64, fallback_name: &str, size: f32) -> Element<'a, Msg> {
    if let Some(handle) = state.avatar_handles.get(&user_id) {
        let s = size * state.zoom;
        iced::widget::Image::new(handle.clone())
            .width(Length::Fixed(s))
            .height(Length::Fixed(s))
            .into()
    } else {
        avatar_circle_sized(user_initials(fallback_name), name_hue(fallback_name), state.zoom, size)
    }
}

/// An indeterminate loading spinner. Uses iced_aw's self-animating spinner so
/// no extra timer subscription is needed.
fn throbber(size: f32) -> Element<'static, Msg> {
    iced_aw::Spinner::new()
        .width(size)
        .height(size)
        .circle_radius((size / 8.0).max(1.0))
        .into()
}


fn data_url_bytes(url: &str) -> Option<Vec<u8>> {
    let b64 = url.split_once(";base64,")?.1;
    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64).ok()
}

fn files_cache_dir() -> std::path::PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    std::path::Path::new(&home).join(".feditexter_files")
}

fn cache_path_for(file_id: &str) -> std::path::PathBuf {
    files_cache_dir().join(file_id)
}

/// Directory for cached network media (avatars, link-preview images).
fn media_cache_dir() -> std::path::PathBuf {
    files_cache_dir().join("media")
}

/// Deterministic on-disk path for a URL's cached bytes (sha1 of the URL).
fn media_cache_path(url: &str) -> std::path::PathBuf {
    use sha1::{Digest, Sha1};
    let mut hasher = Sha1::new();
    hasher.update(url.as_bytes());
    let digest = hasher.finalize();
    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    media_cache_dir().join(hex)
}

/// Read cached bytes for `url`, or fetch them (writing through to the cache on
/// success). Used so avatars aren't re-downloaded on every launch / re-render.
/// This caches the RAW bytes (avatars are further processed by the renderer).
async fn cached_bytes(url: &str) -> Result<Vec<u8>, String> {
    cached_bytes_with_retry(url, false).await
}

async fn cached_bytes_with_retry(url: &str, retry: bool) -> Result<Vec<u8>, String> {
    let path = media_cache_path(url);
    if !retry {
        if let Ok(bytes) = std::fs::read(&path)
            && !bytes.is_empty()
        {
            return Ok(bytes);
        }
    }
    let client = preview_http_client();
    let resp = client.get(url).send().await.map_err(|e| format!("{e}"))?;
    if !resp.status().is_success() {
        return Err(format!("status {}", resp.status()));
    }
    let bytes = resp.bytes().await.map(|b| b.to_vec()).map_err(|e| format!("{e}"))?;
    if bytes.is_empty() {
        return Err("empty response".into());
    }
    if let Some(dir) = path.parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    let _ = std::fs::write(&path, &bytes);
    Ok(bytes)
}

/// Deterministic on-disk path for a URL's PROCESSED preview image (a downscaled
/// JPEG). Kept separate from [`media_cache_path`] so the same URL cached raw for
/// avatars and processed for previews never collides.
fn preview_cache_path(url: &str) -> std::path::PathBuf {
    use sha1::{Digest, Sha1};
    let mut hasher = Sha1::new();
    hasher.update(url.as_bytes());
    let digest = hasher.finalize();
    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    media_cache_dir().join(format!("preview-{hex}.jpg"))
}

/// Fetch a preview image, downscale it, and cache the PROCESSED JPEG on disk so
/// subsequent reads skip the (expensive) decode/resize/encode entirely. A
/// corrupt cached file is dropped and re-downloaded once.
async fn cached_preview_jpeg(url: &str) -> Option<Vec<u8>> {
    let path = preview_cache_path(url);

    // Cache hit: read the processed JPEG (self-heal on a corrupt file).
    if let Ok(bytes) = std::fs::read(&path)
        && !bytes.is_empty()
    {
        if image::load_from_memory(&bytes).is_ok() {
            return Some(bytes);
        }
        let _ = std::fs::remove_file(&path);
    }

    // Fetch raw bytes (network or raw disk cache), then process once.
    let raw = match cached_bytes(url).await {
        Ok(b) => b,
        Err(_) => return None,
    };
    if raw.len() > PREVIEW_MAX_IMAGE_BYTES {
        return None;
    }
    let decoded = image::load_from_memory(&raw).ok()?;
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
    let jpeg = out.into_inner();
    if let Some(dir) = path.parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    let _ = std::fs::write(&path, &jpeg);
    Some(jpeg)
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

fn mime_from_path(path: &std::path::Path) -> &'static str {
    match path.extension().and_then(|e| e.to_str()).map(|e| e.to_ascii_lowercase()).as_deref() {
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("webp") => "image/webp",
        Some("bmp") => "image/bmp",
        Some("svg") => "image/svg+xml",
        Some("mp4") => "video/mp4",
        Some("webm") => "video/webm",
        Some("mov") => "video/quicktime",
        Some("mp3") => "audio/mpeg",
        Some("wav") => "audio/wav",
        Some("ogg") => "audio/ogg",
        Some("pdf") => "application/pdf",
        Some("txt") => "text/plain",
        Some("zip") => "application/zip",
        Some("json") => "application/json",
        _ => "application/octet-stream",
    }
}

/// Write an attachment's bytes to a temp file and open it in the OS viewer.
fn open_bytes(mime: &str, name: &str, bytes: &[u8]) {
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
    let unique = uuid::Uuid::new_v4();
    let path = std::env::temp_dir().join(format!("{base}-{unique}.{ext}"));
    if std::fs::write(&path, bytes).is_err() {
        return;
    }
    let _ = open::that(&path);
}

/// Request P2P transfers for any visible file attachments we don't hold yet.
/// Idempotent per file (the manager dedupes).
fn auto_fetch_files(state: &AppState) {
    let Some(p2p) = &state.p2p else { return };
    let self_id = state.user.as_ref().map(|u| u.id);
    for m in &state.messages {
        if let Some(file_id) = &m.file_id {
            if self_id == Some(m.sender_id) {
                continue;
            }
            if state.downloaded.contains_key(file_id) {
                continue;
            }
            p2p.fetch(file_id, m.sender_id);
        }
    }
}

/// Decode message thumbnails once and cache the image handles (the iced raster
/// cache is keyed by handle id, so rebuilding a handle each frame leaks GPU
/// texture space). Unlike avatars, thumbnails are NOT cropped or circularly
/// masked.
fn build_thumb_handles(state: &mut AppState) {
    for m in &state.messages {
        let Some(file_id) = &m.file_id else { continue };
        if state.thumb_handles.contains_key(file_id) {
            continue;
        }
        if let Some(thumb) = &m.thumbnail_data {
            if let Some(bytes) = data_url_bytes(thumb)
                && let Ok(img) = image::load_from_memory(&bytes)
            {
                let (w, h) = (img.width(), img.height());
                let max_dim = 256u32;
                let thumb_img = if w > max_dim || h > max_dim {
                    let scale = max_dim as f32 / w.max(h) as f32;
                    img.resize(
                        ((w as f32) * scale) as u32,
                        ((h as f32) * scale) as u32,
                        image::imageops::FilterType::Lanczos3,
                    )
                } else {
                    img
                };
                let mut buf = Vec::new();
                let mut cursor = std::io::Cursor::new(&mut buf);
                if thumb_img.write_to(&mut cursor, image::ImageFormat::Png).is_ok() {
                    state.thumb_handles.insert(file_id.clone(), iced::widget::image::Handle::from_bytes(buf));
                }
            }
        }
    }
}

/// Pre-load files cached on disk from earlier sessions so previously-downloaded
/// (or sent) attachments render as complete instead of re-fetching. For images,
/// decode the full-res handle once so bubbles show the real picture.
fn load_cached_files(state: &mut AppState) {
    let dir = files_cache_dir();
    if let Ok(entries) = std::fs::read_dir(&dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file()
                && let Some(name) = path.file_name().and_then(|n| n.to_str())
            {
                let image_handle = std::fs::read(&path).ok().and_then(|bytes| {
                    // Guard against loading enormous cached files on boot.
                    if bytes.len() > 50_000_000 {
                        return None;
                    }
                    image::load_from_memory(&bytes).ok()?;
                    Some(iced::widget::image::Handle::from_bytes(bytes))
                });
                state.downloaded.insert(
                    name.to_string(),
                    DownloadedFile { image_handle, path: Some(path) },
                );
            }
        }
    }
}

/// Decode avatar bytes, upscale/crop to a square, apply a circular alpha mask
/// with a soft edge, and return a PNG handle suitable for `image()`.
fn make_avatar_handle(bytes: Vec<u8>) -> Option<iced::widget::image::Handle> {
    let img = image::load_from_memory(&bytes).ok()?.to_rgba8();
    let (w, h) = img.dimensions();
    let target: u32 = 128;
    let scale = target as f32 / w.max(h) as f32;
    let (nw, nh) = ((w as f32 * scale).round().max(1.0) as u32, (h as f32 * scale).round().max(1.0) as u32);
    let resized = image::imageops::resize(&img, nw, nh, image::imageops::FilterType::Lanczos3);
    let sq = nw.min(nh);
    let (x0, y0) = ((nw - sq) / 2, (nh - sq) / 2);
    let cropped = image::imageops::crop_imm(&resized, x0, y0, sq, sq).to_image();
    let cx = sq as f32 / 2.0;
    let cy = sq as f32 / 2.0;
    let radius = sq as f32 / 2.0;
    let feather = 1.5f32;
    let mut out = cropped;
    for y in 0..sq {
        for x in 0..sq {
            let dx = x as f32 + 0.5 - cx;
            let dy = y as f32 + 0.5 - cy;
            let dist = (dx * dx + dy * dy).sqrt();
            let cover = ((radius - dist) / feather).clamp(0.0, 1.0);
            let px = out.get_pixel_mut(x, y);
            px[3] = ((px[3] as f32) * cover).round() as u8;
        }
    }
    let mut buf = Vec::new();
    let mut cursor = std::io::Cursor::new(&mut buf);
    image::DynamicImage::ImageRgba8(out).write_to(&mut cursor, image::ImageFormat::Png).ok()?;
    Some(iced::widget::image::Handle::from_bytes(buf))
}

async fn fetch_avatar_bytes(url: String) -> Result<Vec<u8>, String> {
    cached_bytes(&url).await
}

/// Scan every known user (self, conversation members, open profile) and load
/// any avatar we haven't attempted yet. `data:` URLs are decoded inline; http(s)
/// URLs are fetched in the background.
fn ensure_avatars(state: &mut AppState) -> Task<Msg> {
    let mut targets: Vec<(u64, String)> = Vec::new();
    if let Some(u) = state.user.as_ref() {
        if let Some(url) = u.avatar_url.as_ref().filter(|s| !s.is_empty()) {
            targets.push((u.id, url.clone()));
        }
    }
    for c in &state.conversations {
        for m in &c.members {
            if let Some(url) = m.avatar_url.as_ref().filter(|s| !s.is_empty()) {
                targets.push((m.id, url.clone()));
            }
        }
    }
    if let Some(p) = &state.profile {
        if let Some(url) = p.avatar_url.as_ref().filter(|s| !s.is_empty()) {
            targets.push((p.id, url.clone()));
        }
    }
    for u in &state.new_conv_results {
        if let Some(url) = u.avatar_url.as_ref().filter(|s| !s.is_empty()) {
            targets.push((u.id, url.clone()));
        }
    }

    let mut pending: Vec<(u64, String)> = Vec::new();
    for (id, url) in targets {
        if !state.avatar_attempted.insert(id) {
            continue;
        }
        if url.starts_with("data:") {
            if let Some(bytes) = data_url_bytes(&url) {
                if let Some(handle) = make_avatar_handle(bytes) {
                    state.avatar_handles.insert(id, handle);
                }
            }
        } else if url.starts_with("http://") || url.starts_with("https://") {
            pending.push((id, url));
        }
    }
    if pending.is_empty() {
        return Task::none();
    }
    let tasks: Vec<Task<Msg>> = pending
        .into_iter()
        .map(|(id, url)| Task::perform(fetch_avatar_bytes(url), move |result| Msg::AvatarFetched { user_id: id, result }))
        .collect();
    Task::batch(tasks)
}

/// Downscale to ≤256px, re-encode as a PNG data URL for POST /api/me/avatar.
fn avatar_data_url(bytes: Vec<u8>) -> Result<String, String> {
    let img = image::load_from_memory(&bytes).map_err(|_| String::from("could not decode image"))?;
    const MAX: u32 = 256;
    let img = if img.width() > MAX || img.height() > MAX {
        let scale = MAX as f32 / img.width().max(img.height()) as f32;
        img.resize(
            ((img.width() as f32) * scale) as u32,
            ((img.height() as f32) * scale) as u32,
            image::imageops::FilterType::Lanczos3,
        )
    } else {
        img
    };
    let mut out = std::io::Cursor::new(Vec::new());
    img.write_to(&mut out, image::ImageFormat::Png).map_err(|_| String::from("could not encode image"))?;
    let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, out.into_inner());
    Ok(format!("data:image/png;base64,{b64}"))
}

async fn moderation_action(server: String, token: String, user_id: u64, action: &str) -> Result<Profile, String> {
    let client = make_client();
    let resp = client
        .post(format!("{server}/api/users/{user_id}/{action}"))
        .bearer_auth(&token)
        .send()
        .await;
    match resp {
        Ok(r) => {
            auth_aware_error(&r)?;
            let v: serde_json::Value = r.json().await.unwrap_or_default();
            serde_json::from_value::<Profile>(v.get("user").cloned().unwrap_or_default())
                .map_err(|e| format!("parse error: {e}"))
        }
        Err(e) => Err(format!("{e}")),
    }
}

async fn upload_avatar(server: String, token: String, bytes: Vec<u8>) -> Result<User, String> {
    let data_url = avatar_data_url(bytes)?;
    let client = make_client();
    let resp = client
        .post(format!("{server}/api/me/avatar"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "avatar": data_url }))
        .send()
        .await;
    match resp {
        Ok(r) if r.status().is_success() => {
            let v: serde_json::Value = r.json().await.unwrap_or_default();
            serde_json::from_value::<User>(v.get("user").cloned().unwrap_or_default())
                .map_err(|_| String::from("parse error"))
        }
        Ok(r) => {
            auth_aware_error(&r)?;
            Err(format!("update failed: {}", r.status()))
        }
        Err(e) => Err(format!("{e}")),
    }
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
/// Returns (title, description, Vec<image urls>).
async fn preview_bsky_post(client: &reqwest::Client, url: &str) -> Option<(Option<String>, Option<String>, Vec<String>)> {
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
    let imgs: Vec<String> = if let Some(images) = embed.get("images").and_then(|x| x.as_array()) {
        images
            .iter()
            .filter_map(|i| i.get("fullsize").and_then(|f| f.as_str()).map(str::to_string))
            .collect()
    } else if let Some(external) = embed.get("external") {
        external.get("thumb").and_then(|t| t.as_str()).map(str::to_string).into_iter().collect()
    } else if let Some(video) = embed.get("video") {
        video.get("thumbnail").and_then(|t| t.as_str()).map(str::to_string).into_iter().collect()
    } else if let Some(media) = embed.get("media") {
        media.get("thumbnail").and_then(|t| t.as_str()).map(str::to_string).into_iter().collect()
    } else {
        Vec::new()
    };
    let imgs: Vec<String> = imgs.into_iter().filter(|i| !preview_looks_private(i)).collect();

    Some((Some(title), None, imgs))
}

/// Fetch a preview image's processed JPEG bytes (disk/network cached), returning
/// them directly. The caller builds a handle from these once and caches it in
/// `media_handles`.
async fn preview_fetch_image(img: &str) -> Option<Vec<u8>> {
    cached_preview_jpeg(img).await
}

async fn fetch_link_preview(url: String) -> Option<LinkPreview> {
    let client = preview_http_client();
    let mut title = None;
    let mut description = None;
    let mut image_urls: Vec<String> = Vec::new();

    // Non-Bluesky links: fetch + parse HTML meta tags.
    if preview_parse_bsky_url(&url).is_none() {
        if let Ok(resp) = client.get(&url).send().await {
            if let Ok(bytes) = resp.bytes().await {
                if bytes.len() <= PREVIEW_MAX_PAGE_BYTES {
                    let html = String::from_utf8_lossy(&bytes);
                    let (t, d, i) = preview_parse_meta(&url, &html);
                    title = t;
                    description = d;
                    if let Some(i) = i {
                        image_urls.push(i);
                    }
                }
            }
        }
    }

    // Bluesky/fxbsky: the page is JS-rendered; use the public API.
    if title.is_none() && image_urls.is_empty() {
        if let Some((t, d, mut imgs)) = preview_bsky_post(&client, &url).await {
            title = title.or(t);
            description = description.or(d);
            if !imgs.is_empty() {
                image_urls.append(&mut imgs);
            }
        }
    }

    // Pre-fetch all images now (populates the processed-JPEG disk cache) so the
    // subsequent handle build in `LinkPreviewLoaded` is a fast disk read.
    let mut images: Vec<String> = Vec::new();
    for img in image_urls {
        if preview_fetch_image(&img).await.is_some() {
            images.push(img);
        }
    }

    Some(LinkPreview { url, title, description, images })
}

// ---------------------------------------------------------------------------
// View
// ---------------------------------------------------------------------------

fn view(state: &AppState) -> Element<'_, Msg> {
    if std::env::var("FEDITEXTER_VERBOSE").is_ok() {
        eprintln!("VIEW called, screen={:?}", state.screen);
    }
    match state.screen {        Screen::Login | Screen::Register => view_auth(state),
        Screen::Verify => view_verify(state),
        Screen::TwoFa => view_2fa(state),
        Screen::Chat => view_chat(state),
    }
}

fn view_auth(state: &AppState) -> Element<'_, Msg> {
    let is_register = state.screen == Screen::Register;

    let logo = text("FediTexter").size(state.zs(36));
    let subtitle = text(if is_register { "Create account" } else { "Sign in" }).size(state.zs(16))
        .color(iced::Color::from_rgb(0.6, 0.6, 0.6));

    let server = text_input("Server URL", &state.server)
        .on_input(Msg::LoginServerChanged)
        .width(Length::Fixed(state.z(320.0)));

    let email = text_input("Email", &state.login_email)
        .on_input(Msg::LoginEmailChanged)
        .width(Length::Fixed(state.z(320.0)));

    let password = text_input("Password", &state.login_password)
        .on_input(Msg::LoginPasswordChanged)
        .on_submit(Msg::LoginSubmit)
        .secure(true)
        .width(Length::Fixed(state.z(320.0)));

    let login_btn: Element<'_, Msg> = if state.auth_busy {
        button(throbber(state.zs(16))).width(Length::Fixed(state.z(320.0))).into()
    } else if is_register {
        button("Create account").on_press(Msg::RegisterSubmit).width(Length::Fixed(state.z(320.0))).into()
    } else {
        button("Sign in").on_press(Msg::LoginSubmit).width(Length::Fixed(state.z(320.0))).into()
    };

    let remember_row: Option<Element<'_, Msg>> = if is_register {
        None
    } else {
        Some(
            iced::widget::checkbox(state.remember_me)
                .label("Remember me on this device")
                .on_toggle(Msg::RememberMeChanged)
                .into(),
        )
    };

    let toggle = if is_register {
        button(text("Already have an account? Sign in").size(state.zs(13)))
            .on_press(Msg::ShowRegister(false))
    } else {
        button(text("Create account").size(state.zs(13)))
            .on_press(Msg::ShowRegister(true))
    };

    let mut form = column![logo, subtitle, server, email, password]
        .spacing(state.z(14))
        .align_x(iced::Alignment::Center);
    if let Some(remember) = remember_row {
        form = form.push(remember);
    }
    form = form.push(login_btn);
    form = form.push(toggle);

    if !state.error.is_empty() {
        form = form.push(
            container(text(&state.error).size(state.zs(13)).color(iced::Color::from_rgb(0.9, 0.3, 0.2)))
                .padding(state.z(8))
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
    let title = text("Verify email").size(state.zs(28));
    let desc = text("Enter the code sent to your email").size(state.zs(14))
        .color(iced::Color::from_rgb(0.6, 0.6, 0.6));
    let code_input = text_input("Code", &state.verify_code)
        .on_input(Msg::VerifyCodeChanged)
        .on_submit(Msg::VerifySubmit)
        .width(Length::Fixed(state.z(320.0)));
    let verify_btn: Element<'_, Msg> = if state.auth_busy {
        button(throbber(state.zs(16))).width(Length::Fixed(state.z(320.0))).into()
    } else {
        button("Verify").on_press(Msg::VerifySubmit).width(Length::Fixed(state.z(320.0))).into()
    };
    let mut form = column![title, desc, code_input, verify_btn].spacing(state.z(14)).align_x(iced::Alignment::Center);
    if !state.error.is_empty() {
        form = form.push(text(&state.error).color(iced::Color::from_rgb(0.9, 0.3, 0.2)));
    }
    container(form).center_x(Length::Fill).center_y(Length::Fill).into()
}

fn view_2fa(state: &AppState) -> Element<'_, Msg> {
    let title = text("Two-factor authentication").size(state.zs(28));
    let desc = text("Enter your TOTP code").size(state.zs(14))
        .color(iced::Color::from_rgb(0.6, 0.6, 0.6));
    let code_input = text_input("6-digit code", &state.twofa_code)
        .on_input(Msg::TwoFaCodeChanged)
        .on_submit(Msg::TwoFaSubmit)
        .width(Length::Fixed(state.z(320.0)));
    let submit_btn: Element<'_, Msg> = if state.auth_busy {
        button(throbber(state.zs(16))).width(Length::Fixed(state.z(320.0))).into()
    } else {
        button("Verify").on_press(Msg::TwoFaSubmit).width(Length::Fixed(state.z(320.0))).into()
    };
    let mut form = column![title, desc, code_input, submit_btn].spacing(state.z(14)).align_x(iced::Alignment::Center);
    if !state.error.is_empty() {
        form = form.push(text(&state.error).color(iced::Color::from_rgb(0.9, 0.3, 0.2)));
    }
    container(form).center_x(Length::Fill).center_y(Length::Fill).into()
}

fn clamp_menu_pos(state: &AppState, x: f32, y: f32, w: f32, h: f32) -> (f32, f32) {
    let win = state.window_size;
    let margin = 4.0;
    let mut cx = if x + w > win.width {
        (x - w - margin).max(margin)
    } else {
        x.max(margin)
    };
    cx = cx.min((win.width - w - margin).max(margin));
    let mut cy = if y + h > win.height {
        (y - h - margin).max(margin)
    } else {
        y.max(margin)
    };
    cy = cy.min((win.height - h - margin).max(margin));
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
            let close_btn = button(text("Close").size(state.zs(13))).on_press(Msg::CloseProfile).padding([state.z(6.0), state.z(16.0)]);
            let avatar = avatar_element(state, profile.id, &profile.display_name, 64.0);

            let username_el: Element<'_, Msg> = if profile.is_self {
                text(format!("@{}", profile.username)).size(state.zs(14)).color(iced::Color::from_rgb(0.6, 0.6, 0.6)).into()
            } else {
                button(text(format!("@{}", profile.username)).size(state.zs(14)).color(state.accent))
                    .on_press(Msg::NewConvWithUser(profile.id))
                    .style(button::text)
                    .padding(state.z(0))
                    .into()
            };

            let mut info = column![
                avatar,
                text(&profile.display_name).size(state.zs(20)),
                username_el,
                text(&profile.domain).size(state.zs(12)).color(iced::Color::from_rgb(0.5, 0.5, 0.5)),
            ].spacing(state.z(8)).align_x(iced::Alignment::Center);

            if profile.is_bot {
                info = info.push(
                    container(text("BOT").size(state.zs(9)).color(iced::Color::WHITE))
                        .padding([state.z(2.0), state.z(8.0)])
                        .style(|_: &iced::Theme| iced::widget::container::Style {
                            background: Some(iced::Color::from_rgb(0.4, 0.6, 0.8).into()),
                            border: iced::Border { radius: 8.0.into(), ..iced::Border::default() },
                            ..iced::widget::container::Style::default()
                        })
                );
            }

            if !profile.is_self {
                info = info.push(
                    button(text("✉  Send message").size(state.zs(13)))
                        .on_press(Msg::NewConvWithUser(profile.id))
                        .style(button::primary)
                        .padding([state.z(6.0), state.z(16.0)])
                );
            }

            if profile.blocked {
                info = info.push(text("Blocked").size(state.zs(12)).color(iced::Color::from_rgb(0.9, 0.3, 0.2)));
            }
            if profile.muted {
                info = info.push(text("Muted").size(state.zs(12)).color(iced::Color::from_rgb(0.9, 0.7, 0.2)));
            }

            if !profile.is_self && !profile.blocked_by {
                let block_btn = button(
                    text(if profile.blocked { "Unblock" } else { "Block" }).size(state.zs(13))
                )
                .on_press(Msg::ToggleBlock(profile.id))
                .style(if profile.blocked { button::primary } else { danger_text_button })
                .padding([state.z(6.0), state.z(16.0)]);

                let mute_btn = button(
                    text(if profile.muted { "Unmute" } else { "Mute" }).size(state.zs(13))
                )
                .on_press(Msg::ToggleMute(profile.id))
                .padding([state.z(6.0), state.z(16.0)]);

                let mod_row = row![block_btn, mute_btn]
                    .spacing(state.z(8))
                    .align_y(iced::Alignment::Center);
                info = info.push(mod_row);
            }

            info = info.push(close_btn);

            let card = container(info)
                .padding(state.z(24))
                .max_width(state.z(320))
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

            let card_wrapped = mouse_area(card).on_press(Msg::Noop);

            let overlay = mouse_area(
                container(card_wrapped)
                    .center_x(Length::Fill)
                    .center_y(Length::Fill)
                    .width(Length::Fill)
                    .height(Length::Fill)
                    .style(|_: &iced::Theme| iced::widget::container::Style {
                        background: Some(iced::Color::from_rgba(0.0, 0.0, 0.0, 0.4).into()),
                        ..iced::widget::container::Style::default()
                    })
            )
            .on_press(Msg::CloseProfile);

            layers.push(overlay.into());
        }
    }

    if let Some(conv_id) = state.conv_menu_conv {
        let menu_items = column![
            button(text("Delete conversation"))
                .on_press(Msg::DeleteConversation(conv_id)).width(Length::Fill).padding([state.z(6.0), state.z(12.0)])
                .style(danger_text_button),
            button("Close").on_press(Msg::CloseConvContextMenu).width(Length::Fill).padding([state.z(6.0), state.z(12.0)]),
        ].spacing(state.z(2));

        let menu = container(menu_items)
            .padding(state.z(4))
            .max_width(state.z(180))
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

    if let Some(msg_id) = state.context_menu_msg {
        let is_self = state.messages.iter().find(|m| m.id == msg_id)
            .map(|m| state.user.as_ref().map(|u| u.id) == Some(m.sender_id))
            .unwrap_or(false);

        let mut menu_items = column![].spacing(state.z(2));
        if is_self {
            menu_items = menu_items.push(
                button("Edit").on_press(Msg::StartEdit(msg_id)).width(Length::Fill).padding([state.z(6.0), state.z(12.0)])
            );
            menu_items = menu_items.push(
                button(text("Delete"))
                    .on_press(Msg::DeleteMessage(msg_id)).width(Length::Fill).padding([state.z(6.0), state.z(12.0)])
                    .style(danger_text_button)
            );
        }
        menu_items = menu_items.push(
            button("Close").on_press(Msg::CloseContextMenu).width(Length::Fill).padding([state.z(6.0), state.z(12.0)])
        );

        let menu = container(menu_items)
            .padding(state.z(4))
            .max_width(state.z(160))
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
        layers.push(mouse_area(inner).on_press(Msg::ClickedOutside).into());
    }

    if let Some(ref body) = state.original_body_text {
        let close_btn = button(text("Close").size(state.zs(13))).on_press(Msg::CloseOriginal).padding([state.z(6.0), state.z(16.0)]);
        let card = container(column![
            text("Original message").size(state.zs(16)),
            text(body.as_str()).size(state.zs(14)),
            close_btn,
        ].spacing(state.z(12)).padding(state.z(20)).max_width(state.z(500)))
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
        let card_wrapped = mouse_area(card).on_press(Msg::Noop);
        let overlay = mouse_area(
            container(card_wrapped)
                .center_x(Length::Fill)
                .center_y(Length::Fill)
                .width(Length::Fill)
                .height(Length::Fill)
                .style(|_: &iced::Theme| iced::widget::container::Style {
                    background: Some(iced::Color::from_rgba(0.0, 0.0, 0.0, 0.5).into()),
                    ..iced::widget::container::Style::default()
                })
        )
        .on_press(Msg::CloseOriginal);
        layers.push(overlay.into());
    }

    if state.new_conv_open {
        layers.push(view_new_conv(state));
    }

    iced::widget::Stack::from_vec(layers)
        .width(Length::Fill)
        .height(Length::Fill)
        .into()
}

fn new_conv_kind_button(state: &AppState, kind: NewConvKind) -> Element<'_, Msg> {
    let selected = state.new_conv_kind == kind;
    let label = column![
        text(kind.label()).size(state.zs(13)),
        text(kind.description()).size(state.zs(10)).color(iced::Color::from_rgb(0.75, 0.75, 0.75)),
    ].spacing(state.z(2)).align_x(iced::Alignment::Center).width(Length::Fill);
    button(label)
        .on_press(Msg::NewConvKindChanged(kind))
        .width(Length::Fill)
        .height(Length::Fixed(state.z(64.0)))
        .padding([state.z(8.0), state.z(8.0)])
        .style(if selected { button::primary } else { button::secondary })
        .into()
}

fn view_new_conv(state: &AppState) -> Element<'_, Msg> {
    let close_btn = button(text("✕").size(state.zs(16))).on_press(Msg::CloseNewConv).style(button::text).padding(state.z(6));

    let kind_row = row![
        new_conv_kind_button(state, NewConvKind::Direct),
        new_conv_kind_button(state, NewConvKind::Group),
        new_conv_kind_button(state, NewConvKind::Channel),
    ].spacing(state.z(8));

    let search = text_input("Search users…", &state.new_conv_search)
        .on_input(Msg::NewConvSearchChanged)
        .width(Length::Fill);

    let chips: Element<'_, Msg> = if state.new_conv_selected.is_empty() {
        space::horizontal().into()
    } else {
        let chip_els: Vec<Element<'_, Msg>> = state.new_conv_results
            .iter()
            .filter(|u| state.new_conv_selected.contains(&u.id))
            .map(|u| {
                let name = if u.display_name.is_empty() { u.username.as_str() } else { u.display_name.as_str() };
                container(row![
                    text(name).size(state.zs(12)),
                    button(text("×").size(state.zs(12))).on_press(Msg::NewConvToggleUser(u.id)).style(button::text).padding(state.z(0)),
                ].spacing(state.z(6)).align_y(iced::Alignment::Center))
                    .padding([state.z(4.0), state.z(8.0)])
                    .style(|theme: &iced::Theme| {
                        let p = theme.extended_palette();
                        iced::widget::container::Style {
                            background: Some(p.primary.weak.color.into()),
                            border: iced::Border { radius: 12.0.into(), ..iced::Border::default() },
                            ..iced::widget::container::Style::default()
                        }
                    })
                    .into()
            })
            .collect();
        row(chip_els).spacing(state.z(6)).into()
    };

    let results: Element<'_, Msg> = if state.new_conv_results.is_empty() {
        container(text("No users found").size(state.zs(13)).color(iced::Color::from_rgb(0.7, 0.7, 0.7)))
            .center_x(Length::Fill)
            .height(Length::Fixed(state.z(80.0)))
            .into()
    } else {
        let items: Vec<Element<'_, Msg>> = state.new_conv_results.iter().map(|u| {
            let selected = state.new_conv_selected.contains(&u.id);
            let name = if u.display_name.is_empty() { u.username.as_str() } else { u.display_name.as_str() };
            let handle = if u.domain.is_empty() {
                format!("@{}", u.username)
            } else {
                format!("@{}@{}", u.username, u.domain)
            };
            let label = row![
                avatar_element(state, u.id, name, 36.0),
                column![
                    text(name).size(state.zs(14)),
                    text(handle).size(state.zs(11)).color(iced::Color::from_rgb(0.75, 0.75, 0.75)),
                ].spacing(state.z(2)),
                space::horizontal(),
                if selected { text("✓").size(state.zs(16)).color(state.accent) } else { text("").size(state.zs(16)) },
            ].spacing(state.z(10)).align_y(iced::Alignment::Center);
            button(label)
                .on_press(Msg::NewConvToggleUser(u.id))
                .width(Length::Fill)
                .padding([state.z(8.0), state.z(10.0)])
                .style(if selected { button::primary } else { button::secondary })
                .into()
        }).collect();
        scrollable(column(items).spacing(state.z(2))).height(Length::Fixed(state.z(260.0))).into()
    };

    let valid = match state.new_conv_kind {
        NewConvKind::Direct => state.new_conv_selected.len() == 1,
        NewConvKind::Group => state.new_conv_selected.len() >= 2,
        NewConvKind::Channel => !state.new_conv_selected.is_empty(),
    };
    let create_label = match state.new_conv_kind {
        NewConvKind::Direct => "Start chat",
        NewConvKind::Group => "Create group",
        NewConvKind::Channel => "Create channel",
    };
    let create_btn: Element<'_, Msg> = if state.new_conv_busy {
        button(row![throbber(state.zs(14)), text("Creating…").size(state.zs(13))].spacing(state.z(8))).width(Length::Fill).into()
    } else if valid {
        button(create_label).on_press(Msg::NewConvCreate).width(Length::Fill).style(button::primary).into()
    } else {
        button(create_label).width(Length::Fill).into()
    };

    let popup = container(column![
        row![text("New conversation").size(state.zs(18)), space::horizontal(), close_btn].align_y(iced::Alignment::Center),
        text("What kind of conversation?").size(state.zs(13)).color(iced::Color::from_rgb(0.75, 0.75, 0.75)),
        kind_row,
        search,
        chips,
        results,
        create_btn,
    ].spacing(state.z(10)))
        .padding(state.z(20))
        .width(Length::Fixed(state.z(460.0)))
        .max_height(state.z(620.0))
        .style(|theme: &iced::Theme| {
            let p = theme.extended_palette();
            iced::widget::container::Style {
                background: Some(p.background.weakest.color.into()),
                border: iced::Border {
                    width: 1.0,
                    color: p.background.weak.color,
                    radius: 12.0.into(),
                },
                shadow: iced::Shadow { color: iced::Color::from_rgba(0.0, 0.0, 0.0, 0.4), offset: iced::Vector::new(0.0, 4.0), blur_radius: 12.0 },
                ..iced::widget::container::Style::default()
            }
        });

    let dim = mouse_area(
        container(space::horizontal())
            .width(Length::Fill)
            .height(Length::Fill)
            .style(|_: &iced::Theme| iced::widget::container::Style {
                background: Some(iced::Color::from_rgba(0.0, 0.0, 0.0, 0.55).into()),
                ..iced::widget::container::Style::default()
            })
    )
    .on_press(Msg::CloseNewConv)
    .into();

    let centered = container(mouse_area(popup).on_press(Msg::Noop))
        .center_x(Length::Fill)
        .center_y(Length::Fill)
        .width(Length::Fill)
        .height(Length::Fill)
        .into();

    iced::widget::Stack::from_vec(vec![dim, centered])
        .width(Length::Fill)
        .height(Length::Fill)
        .into()
}

fn view_settings(state: &AppState) -> Element<'_, Msg> {
    let back_btn = button(text("← Back").size(state.zs(14))).on_press(Msg::ToggleSettings);
    let title = text("Settings").size(state.zs(24));
    let username = state.user.as_ref().map(|u| u.username.as_str()).unwrap_or("");
    let email = state.user.as_ref().map(|u| u.email.as_str()).unwrap_or("");

    let display_name_label = text("Display name").size(state.zs(14))
        .color(iced::Color::from_rgb(0.6, 0.6, 0.6));
    let display_name_input = text_input("Display name", &state.display_name_input)
        .on_input(Msg::DisplayNameChanged)
        .width(Length::Fixed(state.z(320.0)));
    let save_btn = button("Save").on_press(Msg::SaveSettings).width(Length::Fixed(state.z(120.0)));

    let logout_btn = button(text("Sign out").size(state.zs(14)))
        .on_press(Msg::Logout)
        .style(danger_text_button);

    let accent_label = text("Accent colour").size(state.zs(14))
        .color(iced::Color::from_rgb(0.6, 0.6, 0.6));

    const ACCENTS: &[iced::Color] = &[
        iced::Color::from_rgb(0.49, 0.36, 0.88),
        iced::Color::from_rgb(0.32, 0.55, 0.95),
        iced::Color::from_rgb(0.25, 0.72, 0.78),
        iced::Color::from_rgb(0.35, 0.72, 0.45),
        iced::Color::from_rgb(0.85, 0.68, 0.22),
        iced::Color::from_rgb(0.92, 0.49, 0.28),
        iced::Color::from_rgb(0.90, 0.35, 0.45),
        iced::Color::from_rgb(0.78, 0.40, 0.68),
    ];

    let swatches: Vec<Element<'_, Msg>> = ACCENTS.iter().map(|color| {
        let selected = color_eq(*color, state.accent);
        let dot = container(text("").size(state.zs(1)))
            .width(Length::Fixed(state.z(22.0)))
            .height(Length::Fixed(state.z(22.0)))
            .style(move |_: &iced::Theme| iced::widget::container::Style {
                background: Some((*color).into()),
                border: iced::Border {
                    radius: 11.0.into(),
                    width: if selected { 3.0 } else { 0.0 },
                    color: if selected { iced::Color::WHITE } else { iced::Color::TRANSPARENT },
                    ..iced::Border::default()
                },
                ..iced::widget::container::Style::default()
            });
        button(dot)
            .on_press(Msg::SetAccent(*color))
            .style(button::text)
            .padding(state.z(2))
            .into()
    }).collect();

    let accent_row = row(swatches).spacing(state.z(8));

    let pfp_label = text("Profile picture").size(state.zs(14))
        .color(iced::Color::from_rgb(0.6, 0.6, 0.6));

    let current_avatar = match &state.user {
        Some(u) => avatar_element(state, u.id, &u.display_name, 64.0),
        None => avatar_circle_sized(user_initials("?"), 0.0, state.zoom, 64.0),
    };

    let choose_btn: Element<'_, Msg> = if state.avatar_busy {
        button(throbber(state.zs(13)))
            .padding([state.z(6.0), state.z(14.0)])
            .into()
    } else {
        button(text("Choose image…").size(state.zs(13)))
            .on_press(Msg::PickAvatar)
            .padding([state.z(6.0), state.z(14.0)])
            .into()
    };

    let remove_btn = button(text("Remove").size(state.zs(13)))
        .on_press(Msg::RemoveAvatar)
        .style(danger_text_button)
        .padding(state.z(6));

    let pfp_row = row![current_avatar, column![choose_btn, remove_btn].spacing(state.z(6))]
        .spacing(state.z(16))
        .align_y(iced::Alignment::Center);

    let content = column![
        back_btn,
        title,
        text(format!("Username: {username}")).size(state.zs(14)).color(iced::Color::from_rgb(0.6, 0.6, 0.6)),
        text(format!("Email: {email}")).size(state.zs(14)).color(iced::Color::from_rgb(0.6, 0.6, 0.6)),
        rule::horizontal(1),
        pfp_label,
        pfp_row,
        rule::horizontal(1),
        display_name_label,
        display_name_input,
        save_btn,
        rule::horizontal(1),
        accent_label,
        accent_row,
        rule::horizontal(1),
        logout_btn,
    ].spacing(state.z(12)).padding(state.z(24)).max_width(state.z(480));

    container(content)
        .center_x(Length::Fill)
        .center_y(Length::Fill)
        .into()
}

fn view_sidebar(state: &AppState) -> Element<'_, Msg> {
    let settings_btn = button(text("⚙").size(state.zs(18))).on_press(Msg::ToggleSettings);

    let header = row![
        text("Conversations").size(state.zs(18)),
        space::horizontal(),
        settings_btn,
    ].align_y(iced::Alignment::Center).spacing(state.z(8));

    let conv_list: Element<'_, Msg> = if state.conversations.is_empty() {
        container(text("No conversations yet").size(state.zs(13)).color(iced::Color::from_rgb(0.5, 0.5, 0.5)))
            .center_x(Length::Fill)
            .center_y(Length::Fill)
            .into()
    } else {
        let items: Vec<Element<'_, Msg>> = state.conversations.iter().map(|c| {
            let other = c.members.iter()
                .find(|m| Some(m.id) != state.user.as_ref().map(|u| u.id));

            let name = if c.kind == "direct" {
                other
                    .map(|m| if m.display_name.is_empty() { m.username.as_str() } else { m.display_name.as_str() })
                    .unwrap_or("Unknown")
                    .to_string()
            } else {
                let label = if c.kind == "large_group" { "Channel" } else { "Group" };
                format!("{label} ({})", c.members.len())
            };

            let initials = user_initials(&name);
            let hue = name_hue(&name);
            let avatar = if c.kind == "direct" {
                match other {
                    Some(m) => avatar_element(state, m.id, &name, 36.0),
                    None => avatar_circle(initials.clone(), hue, state.zoom),
                }
            } else {
                avatar_circle(initials.clone(), hue, state.zoom)
            };

            let unread = state.unread.get(&c.id).copied().unwrap_or(0);
            let online = other
                .map(|m| state.presence.get(&m.id).copied().unwrap_or(false))
                .unwrap_or(false);

            let status_dot = if online {
                text("●").size(state.zs(8)).color(iced::Color::from_rgb(0.3, 0.8, 0.3))
            } else {
                text("●").size(state.zs(8)).color(iced::Color::from_rgb(0.4, 0.4, 0.4))
            };

            let mut name_row = row![status_dot, text(name.clone()).size(state.zs(14))]
                .spacing(state.z(6)).align_y(iced::Alignment::Center);

            if c.kind != "direct" {
                let tag = if c.kind == "large_group" { "Channel" } else { "Group" };
                name_row = name_row.push(
                    container(text(tag).size(state.zs(9)).color(iced::Color::WHITE))
                        .padding([state.z(1.0), state.z(5.0)])
                        .style(move |_: &iced::Theme| iced::widget::container::Style {
                            background: Some(state.accent.into()),
                            border: iced::Border { radius: 8.0.into(), ..iced::Border::default() },
                            ..iced::widget::container::Style::default()
                        })
                );
            } else if other.map(|m| m.is_bot).unwrap_or(false) {
                name_row = name_row.push(
                    container(text("BOT").size(state.zs(9)).color(iced::Color::WHITE))
                        .padding([state.z(1.0), state.z(5.0)])
                        .style(move |_: &iced::Theme| iced::widget::container::Style {
                            background: Some(iced::Color::from_rgb(0.4, 0.6, 0.8).into()),
                            border: iced::Border { radius: 8.0.into(), ..iced::Border::default() },
                            ..iced::widget::container::Style::default()
                        })
                );
            }

            if unread > 0 {
                name_row = name_row.push(
                    container(text(format!("{unread}")).size(state.zs(11)).color(iced::Color::WHITE))
                        .padding([state.z(2.0), state.z(6.0)])
                        .style(move |_: &iced::Theme| iced::widget::container::Style {
                            background: Some(state.accent.into()),
                            border: iced::Border { radius: 10.0.into(), ..iced::Border::default() },
                            ..iced::widget::container::Style::default()
                        })
                );
            }

            let label = row![avatar, name_row].spacing(state.z(10)).align_y(iced::Alignment::Center);

            let is_selected = state.selected_conversation == Some(c.id);
            let btn = button(label)
                .on_press(Msg::SelectConversation(c.id))
                .width(Length::Fill)
                .padding([state.z(8.0), state.z(10.0)]);

            let styled = if is_selected {
                btn.style(button::primary)
            } else {
                btn.style(button::secondary)
            };

            mouse_area(styled)
                .on_right_press(Msg::ConvContextMenu { conv_id: c.id, x: state.cursor_pos.x, y: state.cursor_pos.y })
                .into()
        }).collect();
        column(items).spacing(state.z(2)).into()
    };

    let new_conv_btn = button(row![text("＋").size(state.zs(16)), text("New conversation").size(state.zs(14))]
        .spacing(state.z(6)).align_y(iced::Alignment::Center))
        .on_press(Msg::OpenNewConv)
        .width(Length::Fill)
        .style(button::primary)
        .padding([state.z(8.0), state.z(10.0)]);

    let sidebar_content = column![
        header,
        new_conv_btn,
        scrollable(conv_list).height(Length::Fill),
    ].spacing(state.z(8)).padding(state.z(12));

    container(sidebar_content)
        .width(Length::Fixed(state.z(280.0)))
        .height(Length::Fill)
        .style(sidebar_style)
        .into()
}

fn view_chat_area(state: &AppState) -> Element<'_, Msg> {
    let Some(conv_id) = state.selected_conversation else {
        return container(
            column![
                text("FediTexter").size(state.zs(28)),
                text("Select a conversation").size(state.zs(14)).color(iced::Color::from_rgb(0.5, 0.5, 0.5)),
            ].spacing(state.z(8)).align_x(iced::Alignment::Center)
        )
        .center_x(Length::Fill)
        .center_y(Length::Fill)
        .into();
    };

    let conv = state.conversations.iter().find(|c| c.id == conv_id);
    let members = conv.map(|c| c.members.clone()).unwrap_or_default();
    let self_id = state.user.as_ref().map(|u| u.id);
    let is_group = matches!(conv.map(|c| c.kind.as_str()), Some("group") | Some("large_group"));

    let other_member = conv
        .and_then(|c| c.members.iter().find(|m| Some(m.id) != self_id));

    let header_name = if is_group {
        let label = match conv.map(|c| c.kind.as_str()) {
            Some("large_group") => "Channel",
            _ => "Group",
        };
        format!("{label} · {} members", members.len())
    } else {
        other_member
            .map(|m| if m.display_name.is_empty() { m.username.as_str() } else { m.display_name.as_str() })
            .unwrap_or("Unknown")
            .to_string()
    };

    let header_initials = user_initials(&header_name);
    let header_hue = name_hue(&header_name);

    let online = other_member
        .map(|m| state.presence.get(&m.id).copied().unwrap_or(false))
        .unwrap_or(false);
    let status = if is_group {
        "Group conversation".to_string()
    } else if online {
        "Online".to_string()
    } else {
        "Offline".to_string()
    };
    let status_color = if online { iced::Color::from_rgb(0.3, 0.8, 0.3) } else { iced::Color::from_rgb(0.5, 0.5, 0.5) };

    let typing_text = state.typing.get(&conv_id)
        .filter(|(_, at)| at.elapsed() < Duration::from_secs(3))
        .map(|(name, _)| format!("{name} is typing…"))
        .unwrap_or_default();

    let header_avatar_btn = match other_member {
        Some(m) => button(avatar_element(state, m.id, &header_name, 36.0))
            .on_press(Msg::ShowProfile(m.id))
            .style(button::text)
            .padding(state.z(0)),
        None => button(avatar_circle(header_initials.clone(), header_hue, state.zoom)).style(button::text).padding(state.z(0)),
    };

    let header_badge = other_member
        .filter(|m| m.is_bot && !is_group)
        .map(|_| {
            container(text("BOT").size(state.zs(9)).color(iced::Color::WHITE))
                .padding([state.z(1.0), state.z(5.0)])
                .style(|_: &iced::Theme| iced::widget::container::Style {
                    background: Some(iced::Color::from_rgb(0.4, 0.6, 0.8).into()),
                    border: iced::Border { radius: 8.0.into(), ..iced::Border::default() },
                    ..iced::widget::container::Style::default()
                })
        });

    let header_name_el: Element<'_, Msg> = match header_badge {
        Some(badge) => row![text(header_name.clone()).size(state.zs(15)), badge]
            .spacing(state.z(6))
            .align_y(iced::Alignment::Center)
            .into(),
        None => text(header_name.clone()).size(state.zs(15)).into(),
    };

    let header_content = if typing_text.is_empty() {
        row![
            header_avatar_btn,
            column![
                header_name_el,
                text(status).size(state.zs(11)).color(status_color),
            ].spacing(state.z(2)),
        ].spacing(state.z(10)).align_y(iced::Alignment::Center)
    } else {
        row![
            header_avatar_btn,
            column![
                header_name_el,
                text(typing_text).size(state.zs(11)).color(iced::Color::from_rgb(0.49, 0.36, 0.88)),
            ].spacing(state.z(2)),
        ].spacing(state.z(10)).align_y(iced::Alignment::Center)
    };

    let header = container(header_content)
        .padding([state.z(10.0), state.z(16.0)])
        .width(Length::Fill)
        .style(header_style);

    let msg_elements: Vec<Element<'_, Msg>> = state.messages.iter().map(|m| {
        let is_self = self_id == Some(m.sender_id);
        let sender = sender_name(&members, m.sender_id, self_id);
        let time = format_local_time(&m.created_at);

        let sender_label = if is_self { "You".to_string() } else { sender };

        let sender_name_widget = button(
            text(sender_label.clone()).size(state.zs(11))
                .color(if is_self { iced::Color::from_rgba(1.0, 1.0, 1.0, 0.6) } else { iced::Color::from_rgba(0.0, 0.0, 0.0, 0.5) })
        )
        .on_press(Msg::ShowProfile(m.sender_id))
        .padding(state.z(0))
        .style(button::text);

        let sender_line = row![
            sender_name_widget,
            text(format!(" · {time}")).size(state.zs(11)).color(if is_self { iced::Color::from_rgba(1.0, 1.0, 1.0, 0.4) } else { iced::Color::from_rgba(0.0, 0.0, 0.0, 0.35) }),
        ].spacing(state.z(0)).align_y(iced::Alignment::Center);

        let body_elements: Vec<Element<'_, Msg>> = m.body.split('\n').flat_map(|line| {
            let mut segments: Vec<Element<'_, Msg>> = Vec::new();
            let mut remaining = line;
            while !remaining.is_empty() {
                if let Some(start) = remaining.find("http://").or_else(|| remaining.find("https://")) {
                    if start > 0 {
                        segments.push(text(remaining[..start].to_string()).size(state.zs(14)).into());
                    }
                    let url_end = remaining[start..].find(|c: char| c.is_whitespace()).unwrap_or(remaining[start..].len());
                    let url = &remaining[start..start + url_end];
                    segments.push(
                        button(text(url).size(state.zs(14)).color(iced::Color::from_rgb(0.6, 0.8, 1.0)))
                            .on_press(Msg::OpenLink(url.to_string()))
                            .style(button::text)
                            .padding(state.z(0))
                            .into()
                    );
                    remaining = &remaining[start + url_end..];
                } else {
                    segments.push(text(remaining.to_string()).size(state.zs(14)).into());
                    remaining = "";
                }
            }
            if segments.is_empty() {
                segments.push(text("").size(state.zs(14)).into());
            }
            segments
        }).collect();

        let mut bubble_content = column![sender_line].spacing(state.z(2));
        for elem in body_elements {
            bubble_content = bubble_content.push(elem);
        }

        for url in extract_urls(&m.body) {
            if state.preview_loading.contains(&url) {
                let loading_card = container(
                    row![throbber(state.z(16)), text("Loading preview…").size(state.zs(11)).color(iced::Color::from_rgb(0.5, 0.5, 0.5))]
                        .spacing(state.z(8))
                        .align_y(iced::Alignment::Center)
                )
                .padding(state.z(8))
                .max_width(state.z(380))
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
                bubble_content = bubble_content.push(loading_card);
                continue;
            }
            if let Some(preview) = state.link_previews.get(&url) {
                let mut card_content = column![].spacing(state.z(4));
                let img_handles: Vec<iced::widget::image::Handle> = preview.images
                    .iter()
                    .filter_map(|img| state.media_handles.get(img).cloned())
                    .collect();
                if !img_handles.is_empty() {
                    let imgs: Vec<Element<'_, Msg>> = img_handles.into_iter().map(|handle| {
                        iced::widget::Image::new(handle)
                            .width(Length::Fill)
                            .height(Length::Shrink)
                            .into()
                    }).collect();
                    card_content = card_content.push(column(imgs).spacing(state.z(4)));
                }
                if let Some(ref title) = preview.title {
                    if !title.is_empty() {
                        card_content = card_content.push(text(title.as_str()).size(state.zs(13)).color(iced::Color::from_rgb(0.9, 0.9, 0.9)));
                    }
                }
                if let Some(ref desc) = preview.description {
                    if !desc.is_empty() {
                        let truncated = if desc.len() > 200 { format!("{}…", &desc[..200]) } else { desc.clone() };
                        card_content = card_content.push(text(truncated).size(state.zs(12)).color(iced::Color::from_rgb(0.7, 0.7, 0.7)));
                    }
                }
                card_content = card_content.push(
                    button(text(&preview.url).size(state.zs(11)).color(iced::Color::from_rgb(0.5, 0.7, 1.0)))
                        .on_press(Msg::OpenLink(preview.url.clone()))
                        .style(button::text).padding(state.z(0))
                );
                let card = container(card_content)
                    .padding(state.z(8))
                    .max_width(state.z(380))
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

        if let Some(file_id) = &m.file_id {
            let mime = m.attachment_mime.clone().unwrap_or_default();
            let name = m.attachment_name.clone().unwrap_or_default();
            let size = m.file_size.unwrap_or(0) as u64;
            let is_image = mime.starts_with("image/");

            let mut card_content = column![].spacing(state.z(4));

            // Full-res image if we hold the file (own send or downloaded). For
            // own sent files the handle lives in `own_full_handles` (this
            // session) or `downloaded` (persisted to disk, survives restart).
            let full_handle = if is_self {
                state.own_full_handles.get(file_id).cloned()
                    .or_else(|| state.downloaded.get(file_id).and_then(|d| d.image_handle.clone()))
            } else {
                state.downloaded.get(file_id).and_then(|d| d.image_handle.clone())
            };

            let thumb_handle = state.thumb_handles.get(file_id).cloned();

            if is_image {
                let img_el = if let Some(h) = full_handle {
                    iced::widget::Image::new(h)
                        .width(Length::Fixed(state.z(280)))
                        .height(Length::Shrink)
                } else if let Some(h) = thumb_handle {
                    iced::widget::Image::new(h)
                        .width(Length::Fixed(state.z(200)))
                        .height(Length::Shrink)
                } else {
                    iced::widget::Image::new(iced::widget::image::Handle::from_rgba(1, 1, vec![0, 0, 0, 0]))
                };
                card_content = card_content.push(img_el);
            }

            let held = is_self || state.downloaded.contains_key(file_id);
            if !held {
                let status = state.p2p_status.get(file_id).cloned().unwrap_or_default();
                let status_text = if status.is_empty() { "waiting…".to_string() } else { status.clone() };
                let status_el: Element<'_, Msg> = if status == "offline" || status == "error" {
                    button(text(format!("{status_text} · retry")).size(state.zs(11)).color(iced::Color::from_rgb(0.9, 0.7, 0.5)))
                        .on_press(Msg::RetryFile(m.id))
                        .style(button::text)
                        .padding(state.z(0))
                        .into()
                } else {
                    text(status_text).size(state.zs(11)).color(iced::Color::from_rgb(0.6, 0.6, 0.6)).into()
                };
                card_content = card_content.push(status_el);
            }

            let file_line = row![
                text("📎").size(state.zs(14)),
                text(name).size(state.zs(12)),
                text(human_size(size)).size(state.zs(10)).color(iced::Color::from_rgb(0.5, 0.5, 0.5)),
                space::horizontal(),
                if held {
                    button(text("Open").size(state.zs(11)).color(iced::Color::from_rgb(0.6, 0.8, 1.0)))
                        .on_press(Msg::OpenFile(m.id))
                        .style(button::text)
                        .padding(state.z(0))
                } else {
                    button(text("Open").size(state.zs(11)).color(iced::Color::from_rgb(0.4, 0.4, 0.4)))
                        .style(button::text)
                        .padding(state.z(0))
                },
            ].spacing(state.z(8)).align_y(iced::Alignment::Center);
            card_content = card_content.push(file_line);

            let card = container(card_content)
                .padding(state.z(8))
                .max_width(state.z(320))
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

        if m.edited_at.is_some() {
            let edit_color = if is_self { iced::Color::from_rgba(1.0, 1.0, 1.0, 0.5) } else { iced::Color::from_rgb(0.6, 0.6, 0.6) };
            bubble_content = bubble_content.push(text("edited").size(state.zs(10)).color(edit_color));
        }
        if let Some(ref body) = m.original_body {
            bubble_content = bubble_content.push(
                button(text("view original").size(state.zs(10)).color(iced::Color::from_rgb(0.7, 0.8, 1.0)))
                    .on_press(Msg::ShowOriginal(body.clone()))
            );
        }

        let style_fn = if is_self { bubble_sent as fn(&iced::Theme) -> iced::widget::container::Style } else { bubble_received };

        let bubble = container(bubble_content)
            .padding([state.z(8.0), state.z(12.0)])
            .max_width(state.z(420))
            .style(style_fn);

        let pfp_label = sender_label.clone();
        let pfp = avatar_element(state, m.sender_id, &pfp_label, 36.0);
        let pfp_btn = button(pfp)
            .on_press(Msg::ShowProfile(m.sender_id))
            .style(button::text)
            .padding(state.z(0));

        let bubble_wrapped = mouse_area(bubble)
            .on_right_press(Msg::ContextMenu { msg_id: m.id, sender_id: m.sender_id, x: state.cursor_pos.x, y: state.cursor_pos.y });

        let msg_row = if is_self {
            row![space::horizontal(), bubble_wrapped, pfp_btn]
                .spacing(state.z(8))
                .align_y(iced::Alignment::End)
                .padding([state.z(0.0), state.z(16.0)])
        } else {
            row![pfp_btn, bubble_wrapped]
                .spacing(state.z(8))
                .align_y(iced::Alignment::End)
                .padding([state.z(0.0), state.z(16.0)])
        };

        msg_row.into()
    }).collect();

    let loading_indicator: Element<'_, Msg> = if state.loading_messages {
        container(row![throbber(state.z(18)), text("Loading…").size(state.zs(12)).color(iced::Color::from_rgb(0.5, 0.5, 0.5))]
            .spacing(state.z(8)).align_y(iced::Alignment::Center))
        .center_x(Length::Fill)
        .padding(state.z(12))
        .into()
    } else {
        space::horizontal().into()
    };

    let messages_scroll = scrollable(
        column![
            space::vertical().height(Length::Fixed(state.z(8.0))),
            loading_indicator,
            column(msg_elements).spacing(state.z(4)),
            space::vertical().height(Length::Fixed(state.z(8.0))),
        ].width(Length::Fill)
    )
    .id(state.msg_scroll_id.clone())
    .height(Length::Fill)
    .on_scroll(Msg::Scrolled)
    .anchor_bottom();

    let jump_btn: Element<'_, Msg> = if state.scrolled_away {
        container(
            button(text("↓").size(state.zs(18)))
                .on_press(Msg::JumpToBottom)
                .style(button::primary)
                .padding(state.z(10))
        )
        .align_x(iced::Alignment::End)
        .width(Length::Fill)
        .padding(iced::Padding::new(0.0).top(0.0).right(16.0).bottom(8.0).left(0.0))
        .into()
    } else {
        space::horizontal().into()
    };

    let scroll_with_btn = column![messages_scroll, jump_btn].spacing(state.z(0)).width(Length::Fill).height(Length::Fill);

    let composer: Element<'_, Msg> = if let Some(_edit_id) = state.editing_message_id {
        let cancel = button(text("Cancel").size(state.zs(13))).on_press(Msg::CancelEdit).padding([state.z(6.0), state.z(12.0)]);
        let save = button(text("Save").size(state.zs(13))).on_press(Msg::ConfirmEdit).padding([state.z(6.0), state.z(12.0)]).style(button::primary);
        let edit_bar = row![
            text("Editing").size(state.zs(12)).color(state.accent),
            space::horizontal(),
            cancel, save
        ].spacing(state.z(8)).align_y(iced::Alignment::Center);
        column![
            edit_bar,
            text_input("Edit message…", &state.draft)
                .on_input(Msg::DraftChanged)
                .on_submit(Msg::ConfirmEdit)
                .width(Length::Fill),
        ].spacing(state.z(8)).into()
    } else {
        let attach_btn: Element<'_, Msg> = if state.picking_file {
            button(throbber(state.zs(16))).padding([state.z(8.0), state.z(10.0)]).into()
        } else {
            button(text("📎").size(state.zs(16)))
                .on_press(Msg::PickFile)
                .padding([state.z(8.0), state.z(10.0)])
                .into()
        };
        let send_btn: Element<'_, Msg> = if state.busy {
            button(throbber(state.zs(16))).padding([state.z(8.0), state.z(12.0)]).into()
        } else {
            button(text("↑").size(state.zs(16)))
                .on_press(Msg::SendMessage)
                .style(button::primary)
                .padding([state.z(8.0), state.z(12.0)])
                .into()
        };
        let processing_chip: Option<Element<'_, Msg>> = if state.picking_file {
            Some(
                container(
                    row![throbber(state.z(14)), text("Processing…").size(state.zs(12)).color(iced::Color::from_rgb(0.5, 0.5, 0.5))]
                        .spacing(state.z(8))
                        .align_y(iced::Alignment::Center)
                )
                .padding(state.z(6))
                .style(composer_style)
                .into(),
            )
        } else {
            None
        };
        let pending_chip: Option<Element<'_, Msg>> = state.pending_attachment.as_ref().map(|att| {
            let thumb = if !att.thumbnail.is_empty() {
                state.thumb_handles.get(&att.file_id).cloned()
            } else {
                None
            };
            let img: Option<iced::widget::Image<iced::widget::image::Handle>> = thumb
                .map(|h| iced::widget::Image::new(h.clone()).width(state.z(32)).height(state.z(32)));
            let clear = button(text("✕").size(state.zs(12))).on_press(Msg::ClearAttachment).padding(state.z(2));
            let img_el: Element<'_, Msg> = match img {
                Some(i) => i.into(),
                None => text("📎").size(state.zs(16)).into(),
            };
            let row: Element<'_, Msg> = row![
                img_el,
                text(att.name.clone()).size(state.zs(12)),
                text(human_size(att.file_size)).size(state.zs(10)).color(iced::Color::from_rgb(0.5, 0.5, 0.5)),
                space::horizontal(),
                clear,
            ].spacing(state.z(8)).align_y(iced::Alignment::Center).into();
            container(row).padding(state.z(6)).style(composer_style).into()
        });
        let input_row = row![
            attach_btn,
            text_input("Type a message…", &state.draft)
                .on_input(Msg::DraftChanged)
                .on_submit(Msg::SendMessage)
                .width(Length::Fill),
            send_btn,
        ].spacing(state.z(8)).align_y(iced::Alignment::Center);
        let mut col = column![input_row].spacing(state.z(6));
        if let Some(chip) = processing_chip {
            col = col.push(chip);
        }
        if let Some(chip) = pending_chip {
            col = col.push(chip);
        }
        col.into()
    };

    let composer_container = container(composer)
        .padding([state.z(10.0), state.z(16.0)])
        .width(Length::Fill)
        .style(composer_style);

    let chat_content = column![
        header,
        scroll_with_btn,
        composer_container,
    ].spacing(state.z(0));

    let content: Element<'_, Msg> = container(chat_content)
        .width(Length::Fill)
        .height(Length::Fill)
        .into();

    content
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn media_cache_path_is_deterministic() {
        let url = "https://cdn.bsky.app/img/feed_fullsize/plain/did:plc:x/bafkrei1";
        let p1 = media_cache_path(url);
        let p2 = media_cache_path(url);
        assert_eq!(p1, p2);
        // Different URLs map to different cache files.
        let p3 = media_cache_path("https://cdn.bsky.app/img/feed_fullsize/plain/did:plc:x/bafkrei2");
        assert_ne!(p1, p3);
        // Path is under the media cache dir and is a plain filename (no slashes).
        let name = p1.file_name().unwrap().to_string_lossy().to_string();
        assert_eq!(name.len(), 40, "sha1 hex is 40 chars");
        assert!(!name.contains('/') && !name.contains('\\'));
        assert!(p1.starts_with(files_cache_dir().join("media")));
    }

    #[test]
    fn preview_cache_path_is_distinct_from_raw() {
        let url = "https://cdn.bsky.app/img/feed_fullsize/plain/did:plc:x/bafkrei1";
        // Processed preview cache is a .jpg file, separate from the raw cache,
        // so a URL cached both ways never collides.
        assert_ne!(preview_cache_path(url), media_cache_path(url));
        assert!(preview_cache_path(url).extension().is_some());
        assert!(preview_cache_path(url).starts_with(media_cache_dir()));
    }
}
