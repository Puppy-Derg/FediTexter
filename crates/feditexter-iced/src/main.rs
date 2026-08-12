#![allow(dead_code)]

mod p2p;
mod voice;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use hmac::Mac as _;
use iced::widget::{button, column, container, mouse_area, row, rule, scrollable, space, text, text_input};
use iced::widget::scrollable::Viewport;
use iced::widget::Id;
use iced::{Element, Length, Subscription, Task};
use p2p::{P2pEvent, P2pManager, ServingFile, SignalEvent};
use tokio::sync::mpsc::UnboundedSender;
use voice::{VoiceEvent, VoiceManager, VoiceVideoKind};
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
    #[serde(default)]
    bio: String,
    #[serde(default = "default_true")]
    profile_visible: bool,
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
    #[serde(default)]
    guild_id: Option<u64>,
    #[serde(default)]
    channel_name: Option<String>,
    members: Vec<Member>,
}

/// A Discord-like server (guild). Guilds are fetched separately from plain
/// conversations; each owns several channel conversations.
#[derive(serde::Deserialize, Clone, Debug)]
struct Guild {
    id: u64,
    name: String,
    #[serde(default)]
    owner_id: u64,
    #[serde(default)]
    member_count: u64,
    #[serde(default)]
    channels: Vec<GuildChannel>,
    #[serde(default)]
    members: Vec<GuildMember>,
    #[serde(default)]
    can_manage: bool,
    #[serde(default)]
    roles: Vec<GuildRole>,
    #[serde(default)]
    bans: Vec<GuildMember>,
}

#[derive(serde::Deserialize, Clone, Debug)]
struct GuildMember {
    id: u64,
    username: String,
    #[serde(default)]
    display_name: String,
}

#[derive(serde::Deserialize, Clone, Debug)]
struct GuildChannel {
    id: u64,
    name: String,
    #[serde(default)]
    channel_type: String,
}

impl GuildChannel {
    fn is_voice(&self) -> bool {
        self.channel_type == "voice"
    }
}

/// A named role inside a guild (admin is the built-in one).
#[derive(serde::Deserialize, Clone, Debug)]
struct GuildRole {
    id: u64,
    name: String,
    #[serde(default)]
    is_admin: bool,
    #[serde(default)]
    member_ids: Vec<u64>,
}

/// A sticker inside a pack (metadata only; image bytes fetched separately).
#[derive(serde::Deserialize, Clone, Debug)]
struct Sticker {
    id: u64,
    name: String,
    #[serde(default)]
    mime: String,
}

#[derive(serde::Deserialize, Clone, Debug)]
struct StickerPack {
    id: u64,
    name: String,
    #[serde(default)]
    owner_id: u64,
    #[serde(default)]
    owner_name: String,
    #[serde(default)]
    stickers: Vec<Sticker>,
}

/// A logged-in device/session shown in Settings -> Devices.
#[derive(serde::Deserialize, Clone, Debug)]
struct SessionInfo {
    id: u64,
    #[serde(default)]
    device_id: Option<String>,
    #[serde(default)]
    login_ip: Option<String>,
    #[serde(default)]
    created_at: String,
    #[serde(default)]
    current: bool,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeftTab {
    Dms,
    Servers,
}

impl NewConvKind {
    fn label(self) -> &'static str {
        match self {
            NewConvKind::Direct => "Direct message",
            NewConvKind::Group => "Group chat",
        }
    }

    fn api_kind(self) -> &'static str {
        match self {
            NewConvKind::Direct => "direct",
            NewConvKind::Group => "group",
        }
    }

    fn description(self) -> &'static str {
        match self {
            NewConvKind::Direct => "Talk to one person",
            NewConvKind::Group => "A small group conversation",
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
    #[serde(default)]
    read: bool,
}

#[derive(serde::Deserialize, Clone, Debug)]
struct TwoFaSetupInfo {
    secret: String,
    uri: String,
    qr: String,
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
    #[serde(default)]
    restricted: bool,
    #[serde(default)]
    bio: String,
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
    #[serde(rename = "voicepresence")]
    VoicePresence { channel_id: u64, user_id: u64, username: String, joined: bool },
    #[serde(rename = "voicestate")]
    VoiceState { channel_id: u64, users: Vec<(u64, String)> },
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
    RegisterBirthdateChanged(String),
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
    VoiceReady(Arc<VoiceManager>),
    VoiceEvent(VoiceEvent),
    VoiceJoin(u64),
    VoiceLeave,
    VoiceToggleMute,
    VoiceToggleCamera,
    VoiceToggleScreen,
    PickFile,
    FilePicked(Result<Attachment, String>),
    ClearAttachment,
    OpenFile(u64),
    RetryFile(u64),
    SessionExpired(String),
    ToggleBlock(u64),
    ToggleMute(u64),
    ModerationResult(Result<Profile, String>),
    TwoFaSetup,
    TwoFaSetupResult(Result<TwoFaSetupInfo, String>),
    TwoFaEnable,
    TwoFaToggleResult(Result<User, String>),
    TwoFaCodeInput(String),
    GuildsLoaded(Result<Vec<Guild>, String>),
    SelectGuild(Option<u64>),
    OpenGuildModal,
    OpenServerModal,
    CloseGuildModal,
    GuildNameInput(String),
    GuildJoinCodeInput(String),
    CreateGuildSubmit,
    GuildCreated(Result<u64, String>),
    JoinGuildSubmit,
    GuildJoined(Result<(), String>),
    CreateChannelSubmit(u64),
    ChannelCreated(Result<(), String>),
    ChannelNameInput(String),
    ChannelTypeChanged(bool),
    SetLeftTab(LeftTab),
    DeleteGuild(u64),
    GuildDeleteResult(Result<(), String>),
    CreateInvite(u64),
    InviteResult(Result<String, String>),
    SetRole { guild_id: u64, user_id: u64, is_admin: bool },
    TransferOwner { guild_id: u64, user_id: u64 },
    KickMember { guild_id: u64, user_id: u64 },
    GuildMemberAction(Result<(), String>),
    ToggleStickerMenu,
    StickerSearchChanged(String),
    StickersLoaded(Result<Vec<StickerPack>, String>),
    SendSticker(u64),
    StickerImageFetched { sticker_id: u64, result: Result<Vec<u8>, String> },
    StickerPackNameInput(String),
    ToggleStickerPackCreate,
    CreateStickerPackSubmit,
    StickerPackCreated(Result<u64, String>),
    PickStickerImages(u64),
    StickerImagesPicked { pack_id: u64, result: Result<Vec<(String, String, Vec<u8>)>, String> },
    StickerAction(Result<(), String>),
    DeleteSticker { pack_id: u64, sticker_id: u64 },
    DeleteStickerPack(u64),
    OpenGuildSettings,
    CloseGuildSettings,
    GuildSettingsLoaded(Result<Guild, String>),
    GuildSettingsTabChanged(GuildSettingsTab),
    RoleNameInput(String),
    CreateRoleSubmit(u64),
    DeleteRole { guild_id: u64, role_id: u64 },
    AssignRole { guild_id: u64, role_id: u64, user_id: u64, on: bool },
    RenameChannel { channel_id: u64, name: String },
    ChannelRenameInput { channel_id: u64, value: String },
    DeleteChannel { channel_id: u64 },
    BanMember { guild_id: u64, user_id: u64 },
    UnbanMember { guild_id: u64, user_id: u64 },
    GuildAdminAction(Result<(), String>),
    SettingsTabChanged(SettingsTab),
    BioChanged(String),
    ProfileVisibleToggled(bool),
    SessionsLoaded(Result<Vec<SessionInfo>, String>),
    RevokeSession(u64),
    SessionRevoked(Result<(), String>),
    AccentHexChanged(String),
    ApplyAccentHex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GuildSettingsTab {
    Channels,
    Roles,
    Bans,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SettingsTab {
    General,
    Privacy,
    Devices,
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
    TwoFaSetup,
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
    register_birthdate: String,
    twofa_code: String,
    verify_code: String,
    token: Option<String>,
    pending_token: Option<String>,
    user: Option<User>,
    error: String,
    info: String,
    conversations: Vec<Conversation>,
    guilds: Vec<Guild>,
    selected_guild: Option<u64>,
    left_tab: LeftTab,
    guild_modal_open: bool,
    guild_name_input: String,
    guild_join_code_input: String,
    channel_name_input: String,
    channel_is_voice: bool,
    guild_busy: bool,
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
    twofa_setup: Option<TwoFaSetupInfo>,
    twofa_toggle_code: String,
    twofa_busy: bool,
    presence: HashMap<u64, bool>,
    typing: HashMap<u64, (String, std::time::Instant)>,
    last_typing_sent: std::time::Instant,
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
    voice: Option<Arc<VoiceManager>>,
    /// user_id -> kind -> (image handle, w, h) for the latest remote frame.
    voice_frames: HashMap<(u64, VoiceVideoKind), (iced::widget::image::Handle, u32, u32)>,
    voice_panel_open: bool,
    pending_attachment: Option<Attachment>,
    own_files: HashMap<String, OwnFile>,
    downloaded: HashMap<String, DownloadedFile>,
    p2p_status: HashMap<String, String>,
    thumb_handles: HashMap<String, iced::widget::image::Handle>,
    own_full_handles: HashMap<String, iced::widget::image::Handle>,
    sticker_menu_open: bool,
    sticker_search: String,
    sticker_packs: Vec<StickerPack>,
    sticker_busy: bool,
    sticker_bytes: HashMap<u64, Vec<u8>>,
    sticker_handles: HashMap<u64, iced::widget::image::Handle>,
    sticker_pack_name_input: String,
    sticker_pack_create_open: bool,
    guild_settings_open: bool,
    guild_settings_tab: GuildSettingsTab,
    role_name_input: String,
    channel_rename_inputs: HashMap<u64, String>,
    settings_tab: SettingsTab,
    bio_input: String,
    sessions: Vec<SessionInfo>,
    sessions_busy: bool,
    accent_hex_input: String,
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
            register_birthdate: String::new(),
            twofa_code: String::new(),
            verify_code: String::new(),
            token: None,
            pending_token: None,
            user: None,
            error: String::new(),
            info: String::new(),
            conversations: Vec::new(),
            guilds: Vec::new(),
            selected_guild: None,
            left_tab: LeftTab::Dms,
            guild_modal_open: false,
            guild_name_input: String::new(),
            guild_join_code_input: String::new(),
            channel_name_input: String::new(),
            channel_is_voice: false,
            guild_busy: false,
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
            twofa_setup: None,
            twofa_toggle_code: String::new(),
            twofa_busy: false,
            presence: HashMap::new(),
            typing: HashMap::new(),
            last_typing_sent: std::time::Instant::now(),
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
            voice: None,
            voice_frames: HashMap::new(),
            voice_panel_open: false,
            pending_attachment: None,
            own_files: HashMap::new(),
            downloaded: HashMap::new(),
            p2p_status: HashMap::new(),
            thumb_handles: HashMap::new(),
            own_full_handles: HashMap::new(),
            sticker_menu_open: false,
            sticker_search: String::new(),
            sticker_packs: Vec::new(),
            sticker_busy: false,
            sticker_bytes: HashMap::new(),
            sticker_handles: HashMap::new(),
            sticker_pack_name_input: String::new(),
            sticker_pack_create_open: false,
            guild_settings_open: false,
            guild_settings_tab: GuildSettingsTab::Channels,
            role_name_input: String::new(),
            channel_rename_inputs: HashMap::new(),
            settings_tab: SettingsTab::General,
            bio_input: String::new(),
            sessions: Vec::new(),
            sessions_busy: false,
            accent_hex_input: String::new(),
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

// Auto-generated accent shades: lighter (for highlights/glows) and darker
// (for borders/active states). Derived from the base accent colour.
fn accent_light(c: iced::Color) -> iced::Color {
    let factor = 0.35;
    iced::Color::from_rgb(
        c.r + (1.0 - c.r) * factor,
        c.g + (1.0 - c.g) * factor,
        c.b + (1.0 - c.b) * factor,
    )
}

fn accent_dark(c: iced::Color) -> iced::Color {
    let factor = 0.45;
    iced::Color::from_rgb(c.r * (1.0 - factor), c.g * (1.0 - factor), c.b * (1.0 - factor))
}

fn accent_faint(c: iced::Color) -> iced::Color {
    let light = accent_light(c);
    iced::Color::from_rgba(light.r, light.g, light.b, 0.18)
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
fn run_bot() -> Result<(), Box<dyn std::error::Error>> {
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

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async move {
        // Login (handles 2FA via the TOTP secret when required).
        let client = make_client();
        let token = loop {
            let resp = client
                .post(format!("{server}/api/login"))
                .json(&serde_json::json!({ "email": email, "password": password, "remember_me": false }))
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    let v: serde_json::Value = r.json().await.unwrap_or_default();
                    if v.get("requires_2fa").and_then(|b| b.as_bool()).unwrap_or(false) {
                        if totp_secret.is_empty() {
                            eprintln!("[bot] 2FA required but FEDITEXTER_BOT_TOTP is not set");
                            return;
                        }
                        let pending = v.get("pending_token").and_then(|t| t.as_str()).unwrap_or("").to_string();
                        let code = totp_now(&totp_secret);
                        let r2 = client
                            .post(format!("{server}/api/login/2fa"))
                            .json(&serde_json::json!({ "pending_token": pending, "code": code, "remember_me": false }))
                            .send()
                            .await;
                        match r2 {
                            Ok(r) if r.status().is_success() => {
                                let v2: serde_json::Value = r.json().await.unwrap_or_default();
                                break v2.get("token").and_then(|t| t.as_str()).unwrap_or("").to_string();
                            }
                            _ => {
                                eprintln!("[bot] 2fa failed, retrying in 5s");
                            }
                        }
                    } else {
                        break v.get("token").and_then(|t| t.as_str()).unwrap_or("").to_string();
                    }
                }
                Ok(r) => {
                    eprintln!("[bot] login failed: {}; retrying in 5s", r.status());
                }
                Err(e) => {
                    eprintln!("[bot] login error: {e}; retrying in 5s");
                }
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        };
        eprintln!("[bot] logged in on {server}");

        let (ws_tx, mut ws_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let (p2p_tx, mut p2p_rx) = tokio::sync::mpsc::unbounded_channel::<P2pEvent>();
        let handle = tokio::runtime::Handle::current();
        let p2p = P2pManager::new(handle, ws_tx.clone(), p2p_tx);
        let device = device_id();

        let url = ws_url(&server);
        let mut request = match url.clone().into_client_request() {
            Ok(r) => r,
            Err(_) => return,
        };
        if let Ok(header) = HeaderValue::from_str(&format!("Bearer {token}")) {
            request.headers_mut().insert("authorization", header);
        }
        if let Ok(header) = HeaderValue::from_str(&device) {
            request.headers_mut().insert("x-device-id", header);
        }
        let (ws, _) = match tokio_tungstenite::connect_async(request).await {
            Ok((ws, _)) => (ws, true),
            Err(e) => {
                eprintln!("[bot] ws connect failed: {e}");
                return;
            }
        };
        eprintln!("[bot] ws connected");
        let (mut sink, mut stream) = ws.split();

        let mut seen: HashSet<u64> = HashSet::new();
        loop {
            tokio::select! {
                outgoing = ws_rx.recv() => {
                    if let Some(text) = outgoing {
                        if sink.send(Message::Text(text.into())).await.is_err() {
                            return;
                        }
                    }
                }
                ev = p2p_rx.recv() => {
                    if let Some(P2pEvent::Complete { file_id, path, .. }) = ev {
                        let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                        eprintln!("[bot] saved received file {file_id} ({} bytes)", size);
                    }
                }
                incoming = stream.next() => {
                    match incoming {
                        Some(Ok(Message::Text(text))) => {
                            let Ok(ev) = serde_json::from_str::<WsHubEvent>(&text) else { continue };
                            if let WsHubEvent::Message { message: m } = ev {
                                if m.sender_id == 0 || !seen.insert(m.id) {
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
                                    let resp = client
                                        .post(format!("{server}/api/conversations/{}/messages", m.conversation_id))
                                        .bearer_auth(&token)
                                        .json(&serde_json::json!({ "body": reply }))
                                        .send()
                                        .await;
                                    if let Err(e) = resp {
                                        eprintln!("[bot] reply failed: {e}");
                                    }
                                }
                            }
                        }
                        Some(Ok(_)) => {}
                        Some(Err(e)) => {
                            eprintln!("[bot] ws error: {e}; reconnecting in 5s");
                            tokio::time::sleep(Duration::from_secs(5)).await;
                        }
                        None => {
                            eprintln!("[bot] ws closed; reconnecting in 5s");
                            tokio::time::sleep(Duration::from_secs(5)).await;
                        }
                    }
                }
            }
        }
    });
    Ok(())
}

fn main() -> iced::Result {
    if std::env::args().any(|a| a == "--bot") {
        let _ = run_bot();
        return Ok(());
    }
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
    let (voice_tx, mut voice_rx) = tokio::sync::mpsc::unbounded_channel::<VoiceEvent>();
    let handle = tokio::runtime::Handle::current();
    let p2p = P2pManager::new(handle.clone(), ws_tx.clone(), p2p_tx);
    let voice = VoiceManager::new(handle, ws_tx.clone(), voice_tx);
    let _ = output.send(Msg::WsSenderReady(ws_tx.clone())).await;
    let _ = output.send(Msg::P2pReady(p2p.clone())).await;
    let _ = output.send(Msg::VoiceReady(voice.clone())).await;

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
                        ev = voice_rx.recv() => ev.map(Msg::VoiceEvent),
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
        Msg::RegisterBirthdateChanged(_) => "RegisterBirthdateChanged".into(),
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
        Msg::VoiceReady(_) => "VoiceReady".into(),
        Msg::VoiceEvent(_) => "VoiceEvent".into(),
        Msg::VoiceJoin(_) => "VoiceJoin".into(),
        Msg::VoiceLeave => "VoiceLeave".into(),
        Msg::VoiceToggleMute => "VoiceToggleMute".into(),
        Msg::VoiceToggleCamera => "VoiceToggleCamera".into(),
        Msg::VoiceToggleScreen => "VoiceToggleScreen".into(),
        Msg::PickFile => "PickFile".into(),
        Msg::FilePicked(_) => "FilePicked".into(),
        Msg::ClearAttachment => "ClearAttachment".into(),
        Msg::OpenFile(_) => "OpenFile".into(),
        Msg::RetryFile(_) => "RetryFile".into(),
        Msg::SessionExpired(_) => "SessionExpired".into(),
        Msg::ToggleBlock(_) => "ToggleBlock".into(),
        Msg::ToggleMute(_) => "ToggleMute".into(),
        Msg::ModerationResult(_) => "ModerationResult".into(),
        Msg::TwoFaSetup => "TwoFaSetup".into(),
        Msg::TwoFaSetupResult(_) => "TwoFaSetupResult".into(),
        Msg::TwoFaEnable => "TwoFaEnable".into(),
        Msg::TwoFaToggleResult(_) => "TwoFaToggleResult".into(),
        Msg::TwoFaCodeInput(_) => "TwoFaCodeInput".into(),
        Msg::GuildsLoaded(_) => "GuildsLoaded".into(),
        Msg::SelectGuild(_) => "SelectGuild".into(),
        Msg::OpenGuildModal => "OpenGuildModal".into(),
        Msg::OpenServerModal => "OpenServerModal".into(),
        Msg::CloseGuildModal => "CloseGuildModal".into(),
        Msg::GuildNameInput(_) => "GuildNameInput".into(),
        Msg::GuildJoinCodeInput(_) => "GuildJoinCodeInput".into(),
        Msg::CreateGuildSubmit => "CreateGuildSubmit".into(),
        Msg::GuildCreated(_) => "GuildCreated".into(),
        Msg::JoinGuildSubmit => "JoinGuildSubmit".into(),
        Msg::GuildJoined(_) => "GuildJoined".into(),
        Msg::CreateChannelSubmit(_) => "CreateChannelSubmit".into(),
        Msg::ChannelCreated(_) => "ChannelCreated".into(),
        Msg::ChannelNameInput(_) => "ChannelNameInput".into(),
        Msg::ChannelTypeChanged(_) => "ChannelTypeChanged".into(),
        Msg::SetLeftTab(_) => "SetLeftTab".into(),
        Msg::DeleteGuild(_) => "DeleteGuild".into(),
        Msg::GuildDeleteResult(_) => "GuildDeleteResult".into(),
        Msg::CreateInvite(_) => "CreateInvite".into(),
        Msg::InviteResult(_) => "InviteResult".into(),
        Msg::SetRole { .. } => "SetRole".into(),
        Msg::TransferOwner { .. } => "TransferOwner".into(),
        Msg::KickMember { .. } => "KickMember".into(),
        Msg::GuildMemberAction(_) => "GuildMemberAction".into(),
        Msg::ToggleStickerMenu => "ToggleStickerMenu".into(),
        Msg::StickerSearchChanged(_) => "StickerSearchChanged".into(),
        Msg::StickersLoaded(_) => "StickersLoaded".into(),
        Msg::SendSticker(_) => "SendSticker".into(),
        Msg::StickerImageFetched { .. } => "StickerImageFetched".into(),
        Msg::StickerPackNameInput(_) => "StickerPackNameInput".into(),
        Msg::ToggleStickerPackCreate => "ToggleStickerPackCreate".into(),
        Msg::CreateStickerPackSubmit => "CreateStickerPackSubmit".into(),
        Msg::StickerPackCreated(_) => "StickerPackCreated".into(),
        Msg::PickStickerImages(_) => "PickStickerImages".into(),
        Msg::StickerImagesPicked { .. } => "StickerImagesPicked".into(),
        Msg::StickerAction(_) => "StickerAction".into(),
        Msg::DeleteSticker { .. } => "DeleteSticker".into(),
        Msg::DeleteStickerPack(_) => "DeleteStickerPack".into(),
        Msg::OpenGuildSettings => "OpenGuildSettings".into(),
        Msg::CloseGuildSettings => "CloseGuildSettings".into(),
        Msg::GuildSettingsLoaded(_) => "GuildSettingsLoaded".into(),
        Msg::GuildSettingsTabChanged(_) => "GuildSettingsTabChanged".into(),
        Msg::RoleNameInput(_) => "RoleNameInput".into(),
        Msg::CreateRoleSubmit(_) => "CreateRoleSubmit".into(),
        Msg::DeleteRole { .. } => "DeleteRole".into(),
        Msg::AssignRole { .. } => "AssignRole".into(),
        Msg::RenameChannel { .. } => "RenameChannel".into(),
        Msg::ChannelRenameInput { .. } => "ChannelRenameInput".into(),
        Msg::DeleteChannel { .. } => "DeleteChannel".into(),
        Msg::BanMember { .. } => "BanMember".into(),
        Msg::UnbanMember { .. } => "UnbanMember".into(),
        Msg::GuildAdminAction(_) => "GuildAdminAction".into(),
        Msg::SettingsTabChanged(_) => "SettingsTabChanged".into(),
        Msg::BioChanged(_) => "BioChanged".into(),
        Msg::ProfileVisibleToggled(_) => "ProfileVisibleToggled".into(),
        Msg::SessionsLoaded(_) => "SessionsLoaded".into(),
        Msg::RevokeSession(_) => "RevokeSession".into(),
        Msg::SessionRevoked(_) => "SessionRevoked".into(),
        Msg::AccentHexChanged(_) => "AccentHexChanged".into(),
        Msg::ApplyAccentHex => "ApplyAccentHex".into(),
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
                // 2FA is mandatory: this account has no TOTP secret yet, so the
                // user must set it up before using the app.
                state.screen = Screen::TwoFaSetup;
                Task::done(Msg::TwoFaSetup)
            }
        }
        Msg::LoginResult(Err(e)) => { state.auth_busy = false; state.error = e; Task::none() }
        Msg::SessionRestored(Some((server, token, user))) => {
            state.server = server;
            state.token = Some(token.clone());
            state.user = Some(user.clone());
            state.display_name_input = user.display_name.clone();
            if !user.totp_enabled {
                // 2FA is mandatory: prompt the user to set it up.
                state.screen = Screen::TwoFaSetup;
                return Task::done(Msg::TwoFaSetup);
            }
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
        Msg::RegisterBirthdateChanged(d) => { state.register_birthdate = d; Task::none() }
        Msg::RegisterSubmit => {
            state.error.clear();
            state.auth_busy = true;
            let email = state.register_email.clone();
            let username = state.register_username.clone();
            let password = state.register_password.clone();
            let birthdate = state.register_birthdate.clone();
            let server = state.server.clone();
            Task::perform(async move {
                let client = make_client();
                let resp = client.post(format!("{server}/api/register"))
                    .json(&serde_json::json!({ "email": email, "username": username, "password": password, "birthdate": birthdate }))
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
            if !user.email_verified {
                state.screen = Screen::Verify;
                Task::none()
            } else if !user.totp_enabled {
                // 2FA is mandatory: set it up before using the app.
                state.screen = Screen::TwoFaSetup;
                Task::done(Msg::TwoFaSetup)
            } else {
                state.screen = Screen::Chat;
                Task::perform(
                    load_conversations(state.server.clone(), state.token.clone().unwrap_or_default()),
                    Msg::ConversationsLoaded,
                )
            }
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
                // 2FA is mandatory: prompt the user to set it up now.
                state.screen = Screen::TwoFaSetup;
                Task::done(Msg::TwoFaSetup)
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
        Msg::ConversationsLoaded(Ok(convs)) => {
            state.conversations = convs;
            let avatars = ensure_avatars(state);
            // Also refresh the guild list so the server rail stays in sync.
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            Task::batch(vec![
                avatars,
                Task::perform(load_guilds(server, token), Msg::GuildsLoaded),
            ])
        }
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
                        // Mark the conversation as read up to the newest message.
                        if let Some(last) = state.messages.last() {
                            let server = state.server.clone();
                            let token = state.token.clone().unwrap_or_default();
                            let cid = conversation_id;
                            let mid = last.id;
                            tasks.push(Task::perform(
                                mark_read_api(server, token, cid, mid),
                                |r| match r { Ok(_) => Msg::Noop, Err(e) => Msg::Error(e) },
                            ));
                        }
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
        Msg::DraftChanged(d) => {
            state.draft = d;
            // Broadcast "typing" at most once per 2s per burst. The WS worker
            // relays the frame to the server, which fans it out to the other
            // members of the open conversation.
            if state.last_typing_sent.elapsed() >= Duration::from_secs(2)
                && let Some(conv) = state.selected_conversation
                && let Some(ws_tx) = &state.ws_tx
            {
                state.last_typing_sent = std::time::Instant::now();
                let frame = serde_json::json!({ "type": "typing", "conversation_id": conv }).to_string();
                let _ = ws_tx.send(frame);
            }
            Task::none()
        }
        Msg::ToggleStickerMenu => {
            state.sticker_menu_open = !state.sticker_menu_open;
            if state.sticker_menu_open && state.sticker_packs.is_empty() {
                let server = state.server.clone();
                let token = state.token.clone().unwrap_or_default();
                let q = state.sticker_search.clone();
                Task::perform(load_sticker_packs(server, token, q), Msg::StickersLoaded)
            } else {
                Task::none()
            }
        }
        Msg::StickerSearchChanged(q) => {
            state.sticker_search = q.clone();
            let server = state.server.clone();
            let token = state.token.clone().unwrap_or_default();
            Task::perform(load_sticker_packs(server, token, q), Msg::StickersLoaded)
        }
        Msg::StickersLoaded(Ok(packs)) => {
            state.sticker_busy = false;
            state.sticker_packs = packs;
            // Eagerly fetch each sticker's image bytes so the grid can render
            // instantly and clicking one has bytes to send.
            let server = state.server.clone();
            let token = state.token.clone().unwrap_or_default();
            let ids: Vec<u64> = state.sticker_packs.iter()
                .flat_map(|p| p.stickers.iter())
                .filter(|s| !state.sticker_bytes.contains_key(&s.id))
                .map(|s| s.id)
                .collect();
            if ids.is_empty() {
                Task::none()
            } else {
                let tasks: Vec<Task<Msg>> = ids.into_iter().map(|id| {
                    let server = server.clone();
                    let token = token.clone();
                    Task::perform(sticker_image_api(server, token, id), move |result| {
                        Msg::StickerImageFetched { sticker_id: id, result }
                    })
                }).collect();
                Task::batch(tasks)
            }
        }
        Msg::StickersLoaded(Err(e)) => { state.sticker_busy = false; handle_api_error(state, e) }
        Msg::StickerImageFetched { sticker_id, result } => {
            match result {
                Ok(bytes) => {
                    state.sticker_bytes.insert(sticker_id, bytes.clone());
                    state.sticker_handles.insert(sticker_id, iced::widget::image::Handle::from_bytes(bytes));
                }
                Err(_) => {}
            }
            Task::none()
        }
        Msg::StickerPackNameInput(s) => { state.sticker_pack_name_input = s; Task::none() }
        Msg::ToggleStickerPackCreate => { state.sticker_pack_create_open = !state.sticker_pack_create_open; Task::none() }
        Msg::CreateStickerPackSubmit => {
            let name = state.sticker_pack_name_input.trim().to_string();
            if name.is_empty() { return Task::none(); }
            let server = state.server.clone();
            let token = state.token.clone().unwrap_or_default();
            Task::perform(create_sticker_pack_api(server, token, name), Msg::StickerPackCreated)
        }
        Msg::StickerPackCreated(Ok(id)) => {
            state.sticker_pack_name_input.clear();
            let server = state.server.clone();
            let token = state.token.clone().unwrap_or_default();
            Task::perform(load_sticker_packs(server, token, state.sticker_search.clone()), Msg::StickersLoaded).then(move |_| {
                Task::done(Msg::PickStickerImages(id))
            })
        }
        Msg::StickerPackCreated(Err(e)) => handle_api_error(state, e),
        Msg::PickStickerImages(pack_id) => {
            let pack_id_clone = pack_id;
            Task::perform(
                async move {
                    let files = rfd::AsyncFileDialog::new().pick_files().await;
                    let Some(files) = files else { return Err("no files selected".to_string()) };
                    let mut out = Vec::new();
                    for f in files {
                        let bytes = f.read().await;
                        if bytes.is_empty() || bytes.len() > 512 * 1024 {
                            return Err("sticker image too large (max 512 KiB)".to_string());
                        }
                        let path = f.path();
                        let mime = mime_from_path(path).to_string();
                        if !mime.starts_with("image/") {
                            return Err("stickers must be image files".to_string());
                        }
                        let name = path.file_stem()
                            .map(|s| s.to_string_lossy().to_string())
                            .unwrap_or_else(|| "sticker".to_string());
                        out.push((name, mime, bytes));
                    }
                    Ok(out)
                },
                move |result| Msg::StickerImagesPicked { pack_id: pack_id_clone, result },
            )
        }
        Msg::StickerImagesPicked { pack_id, result } => {
            let items = match result {
                Ok(items) => items,
                Err(e) => { state.error = e; return Task::none(); }
            };
            let server = state.server.clone();
            let token = state.token.clone().unwrap_or_default();
            let tasks: Vec<Task<Msg>> = items.into_iter().map(|(name, mime, data)| {
                let server = server.clone();
                let token = token.clone();
                Task::perform(upload_sticker_api(server, token, pack_id, name, data, mime), Msg::StickerAction)
            }).collect();
            Task::batch(tasks)
        }
        Msg::StickerAction(Ok(())) => {
            let server = state.server.clone();
            let token = state.token.clone().unwrap_or_default();
            let q = state.sticker_search.clone();
            Task::perform(load_sticker_packs(server, token, q), Msg::StickersLoaded)
        }
        Msg::StickerAction(Err(e)) => handle_api_error(state, e),
        Msg::DeleteSticker { pack_id, sticker_id } => {
            let server = state.server.clone();
            let token = state.token.clone().unwrap_or_default();
            Task::perform(delete_sticker_api(server, token, pack_id, sticker_id), Msg::StickerAction)
        }
        Msg::DeleteStickerPack(pack_id) => {
            let server = state.server.clone();
            let token = state.token.clone().unwrap_or_default();
            Task::perform(delete_sticker_pack_api(server, token, pack_id), Msg::StickerAction)
        }
        Msg::OpenGuildSettings => {
            let Some(guild_id) = state.selected_guild else { return Task::none() };
            state.guild_settings_open = true;
            state.guild_settings_tab = GuildSettingsTab::Channels;
            let server = state.server.clone();
            let token = state.token.clone().unwrap_or_default();
            Task::perform(load_guild_detail(server, token, guild_id), Msg::GuildSettingsLoaded)
        }
        Msg::CloseGuildSettings => { state.guild_settings_open = false; Task::none() }
        Msg::GuildSettingsLoaded(Ok(g)) => {
            if let Some(existing) = state.guilds.iter_mut().find(|x| x.id == g.id) {
                *existing = g;
            }
            Task::none()
        }
        Msg::GuildSettingsLoaded(Err(e)) => handle_api_error(state, e),
        Msg::GuildSettingsTabChanged(t) => { state.guild_settings_tab = t; Task::none() }
        Msg::RoleNameInput(s) => { state.role_name_input = s; Task::none() }
        Msg::CreateRoleSubmit(guild_id) => {
            let name = state.role_name_input.trim().to_string();
            if name.is_empty() { return Task::none(); }
            state.role_name_input.clear();
            let server = state.server.clone();
            let token = state.token.clone().unwrap_or_default();
            Task::perform(create_role_api(server, token, guild_id, name), Msg::GuildAdminAction)
        }
        Msg::DeleteRole { guild_id, role_id } => {
            let server = state.server.clone();
            let token = state.token.clone().unwrap_or_default();
            Task::perform(delete_role_api(server, token, guild_id, role_id), Msg::GuildAdminAction)
        }
        Msg::AssignRole { guild_id, role_id, user_id, on } => {
            let server = state.server.clone();
            let token = state.token.clone().unwrap_or_default();
            Task::perform(assign_role_api(server, token, guild_id, role_id, user_id, on), Msg::GuildAdminAction)
        }
        Msg::RenameChannel { channel_id, name } => {
            let Some(guild_id) = state.selected_guild else { return Task::none() };
            let server = state.server.clone();
            let token = state.token.clone().unwrap_or_default();
            Task::perform(rename_channel_api(server, token, guild_id, channel_id, name), Msg::GuildAdminAction)
        }
        Msg::ChannelRenameInput { channel_id, value } => {
            state.channel_rename_inputs.insert(channel_id, value);
            Task::none()
        }
        Msg::DeleteChannel { channel_id } => {
            let Some(guild_id) = state.selected_guild else { return Task::none() };
            let server = state.server.clone();
            let token = state.token.clone().unwrap_or_default();
            Task::perform(delete_channel_api(server, token, guild_id, channel_id), Msg::GuildAdminAction)
        }
        Msg::BanMember { guild_id, user_id } => {
            let server = state.server.clone();
            let token = state.token.clone().unwrap_or_default();
            Task::perform(ban_member_api(server, token, guild_id, user_id), Msg::GuildAdminAction)
        }
        Msg::UnbanMember { guild_id, user_id } => {
            let server = state.server.clone();
            let token = state.token.clone().unwrap_or_default();
            Task::perform(unban_member_api(server, token, guild_id, user_id), Msg::GuildAdminAction)
        }
        Msg::GuildAdminAction(Ok(())) => {
            if let Some(guild_id) = state.selected_guild {
                let server = state.server.clone();
                let token = state.token.clone().unwrap_or_default();
                return Task::perform(load_guild_detail(server, token, guild_id), Msg::GuildSettingsLoaded);
            }
            Task::none()
        }
        Msg::GuildAdminAction(Err(e)) => handle_api_error(state, e),
        Msg::SettingsTabChanged(t) => {
            state.settings_tab = t;
            if t == SettingsTab::Devices && state.sessions.is_empty() && !state.sessions_busy {
                state.sessions_busy = true;
                let server = state.server.clone();
                let token = state.token.clone().unwrap_or_default();
                Task::perform(load_sessions_api(server, token), Msg::SessionsLoaded)
            } else {
                Task::none()
            }
        }
        Msg::BioChanged(s) => { state.bio_input = s; Task::none() }
        Msg::ProfileVisibleToggled(visible) => {
            let server = state.server.clone();
            let token = state.token.clone().unwrap_or_default();
            Task::perform(async move {
                let client = make_client();
                let resp = client.patch(format!("{server}/api/me"))
                    .bearer_auth(&token)
                    .json(&serde_json::json!({ "profile_visible": visible }))
                    .send().await;
                match resp {
                    Ok(r) => {
                        auth_aware_error(&r)?;
                        let v: serde_json::Value = r.json().await.unwrap_or_default();
                        serde_json::from_value::<User>(v.get("user").cloned().unwrap_or_default())
                            .map_err(|_| String::from("parse error"))
                    }
                    Err(e) => Err(format!("{e}")),
                }
            }, Msg::SettingsResult)
        }
        Msg::SessionsLoaded(Ok(sessions)) => { state.sessions_busy = false; state.sessions = sessions; Task::none() }
        Msg::SessionsLoaded(Err(e)) => { state.sessions_busy = false; handle_api_error(state, e) }
        Msg::RevokeSession(session_id) => {
            let server = state.server.clone();
            let token = state.token.clone().unwrap_or_default();
            Task::perform(revoke_session_api(server, token, session_id), Msg::SessionRevoked)
        }
        Msg::SessionRevoked(Ok(())) => {
            let server = state.server.clone();
            let token = state.token.clone().unwrap_or_default();
            Task::perform(load_sessions_api(server, token), Msg::SessionsLoaded)
        }
        Msg::SessionRevoked(Err(e)) => handle_api_error(state, e),
        Msg::AccentHexChanged(s) => { state.accent_hex_input = s; Task::none() }
        Msg::ApplyAccentHex => {
            let s = state.accent_hex_input.trim().trim_start_matches('#');
            if s.len() == 6 && s.chars().all(|c| c.is_ascii_hexdigit()) {
                if let (Ok(r), Ok(g), Ok(b)) = (u8::from_str_radix(&s[0..2], 16), u8::from_str_radix(&s[2..4], 16), u8::from_str_radix(&s[4..6], 16)) {
                    let color = iced::Color::from_rgb(r as f32 / 255.0, g as f32 / 255.0, b as f32 / 255.0);
                    state.accent = color;
                    save_accent(color);
                    return Task::none();
                }
            }
            state.error = "Invalid hex colour (e.g. #7a5cf0)".to_string();
            Task::none()
        }
        Msg::SendMessage => do_send(state),
        Msg::SendSticker(sticker_id) => {
            let Some(bytes) = state.sticker_bytes.get(&sticker_id).cloned() else { return Task::none() };
            let Some(conv) = state.selected_conversation else { return Task::none() };
            let _ = conv;
            let mime = state.sticker_packs.iter()
                .flat_map(|p| p.stickers.iter())
                .find(|s| s.id == sticker_id)
                .map(|s| s.mime.clone())
                .unwrap_or_else(|| "image/webp".to_string());
            let file_id = uuid::Uuid::new_v4().to_string();
            // Compress the 1024x1024 sticker into a small bubble thumbnail.
            let thumbnail = image::load_from_memory(&bytes)
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
                    base64::engine::general_purpose::STANDARD.encode(out.get_ref())
                })
                .unwrap_or_default();
            state.pending_attachment = Some(Attachment {
                mime: mime.clone(),
                name: "sticker".to_string(),
                file_id,
                file_size: bytes.len() as u64,
                thumbnail,
                bytes,
            });
            state.draft.clear();
            state.busy = true;
            do_send(state)
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
        Msg::WsDisconnected => {
            state.ws_connected = false;
            if let Some(voice) = &state.voice {
                voice.leave();
            }
            Task::none()
        }
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
                    if signal.file_id.starts_with("voice-") {
                        if let Some(voice) = &state.voice {
                            voice.handle_signal(signal);
                        }
                    } else if let Some(p2p) = &state.p2p {
                        p2p.handle_signal(signal);
                    }
                    Task::none()
                }
                WsHubEvent::VoicePresence { channel_id, user_id, username, joined } => {
                    if let Some(voice) = &state.voice {
                        voice.handle_voice_presence(channel_id, user_id, username, joined);
                    }
                    Task::none()
                }
                WsHubEvent::VoiceState { channel_id, users } => {
                    if let Some(voice) = &state.voice {
                        voice.handle_voice_state(channel_id, users);
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
        Msg::TwoFaSetup => {
            state.twofa_busy = true;
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            Task::perform(
                async move {
                    let client = make_client();
                    let resp = client.post(format!("{server}/api/me/2fa/setup"))
                        .bearer_auth(&token)
                        .send().await;
                    match resp {
                        Ok(r) => {
                            auth_aware_error(&r)?;
                            let v: serde_json::Value = r.json().await.unwrap_or_default();
                            serde_json::from_value::<TwoFaSetupInfo>(v).map_err(|e| format!("parse error: {e}"))
                        }
                        Err(e) => Err(format!("{e}")),
                    }
                },
                Msg::TwoFaSetupResult,
            )
        }
        Msg::TwoFaSetupResult(Ok(info)) => {
            state.twofa_busy = false;
            state.twofa_setup = Some(info);
            Task::none()
        }
        Msg::TwoFaSetupResult(Err(e)) => { state.twofa_busy = false; handle_api_error(state, e) }
        Msg::TwoFaCodeInput(c) => { state.twofa_toggle_code = c; Task::none() }
        Msg::TwoFaEnable => {
            state.twofa_busy = true;
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            let code = state.twofa_toggle_code.clone();
            Task::perform(
                async move {
                    let client = make_client();
                    let resp = client.post(format!("{server}/api/me/2fa/enable"))
                        .bearer_auth(&token)
                        .json(&serde_json::json!({ "code": code }))
                        .send().await;
                    match resp {
                        Ok(r) => {
                            auth_aware_error(&r)?;
                            let v: serde_json::Value = r.json().await.unwrap_or_default();
                            serde_json::from_value::<User>(v.get("user").cloned().unwrap_or_default())
                                .map_err(|e| format!("parse error: {e}"))
                        }
                        Err(e) => Err(format!("{e}")),
                    }
                },
                Msg::TwoFaToggleResult,
            )
        }
        Msg::TwoFaToggleResult(Ok(u)) => {
            state.twofa_busy = false;
            state.twofa_setup = None;
            state.twofa_toggle_code.clear();
            state.user = Some(u);
            state.info = "Two-factor authentication enabled".to_string();
            // If we were mid-setup (2FA just enabled), enter the app.
            if state.screen == Screen::TwoFaSetup {
                state.screen = Screen::Chat;
                let token = state.token.clone().unwrap_or_default();
                let server = state.server.clone();
                return Task::perform(load_conversations(server, token), Msg::ConversationsLoaded);
            }
            Task::none()
        }
        Msg::TwoFaToggleResult(Err(e)) => { state.twofa_busy = false; handle_api_error(state, e) }
        Msg::GuildsLoaded(Ok(guilds)) => { state.guilds = guilds; Task::none() }
        Msg::GuildsLoaded(Err(e)) => handle_api_error(state, e),
        Msg::SelectGuild(g) => {
            state.selected_guild = g;
            state.left_tab = LeftTab::Servers;
            state.selected_conversation = None;
            state.messages.clear();
            state.conv_menu_conv = None;
            Task::none()
        }
        Msg::SetLeftTab(tab) => {
            state.left_tab = tab;
            match tab {
                LeftTab::Dms => {
                    state.selected_guild = None;
                    state.selected_conversation = None;
                    state.messages.clear();
                }
                LeftTab::Servers => {
                    state.selected_conversation = None;
                    state.messages.clear();
                }
            }
            Task::none()
        }
        Msg::OpenGuildModal => { state.guild_modal_open = true; Task::none() }
        Msg::OpenServerModal => { state.selected_guild = None; state.guild_modal_open = true; Task::none() }
        Msg::CloseGuildModal => { state.guild_modal_open = false; Task::none() }
        Msg::GuildNameInput(s) => { state.guild_name_input = s; Task::none() }
        Msg::GuildJoinCodeInput(s) => { state.guild_join_code_input = s; Task::none() }
        Msg::CreateGuildSubmit => {
            state.guild_busy = true;
            let name = state.guild_name_input.trim().to_string();
            if name.is_empty() { state.guild_busy = false; return Task::none(); }
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            Task::perform(create_guild_api(server, token, name), Msg::GuildCreated)
        }
        Msg::GuildCreated(Ok(_gid)) => {
            state.guild_busy = false;
            state.guild_modal_open = false;
            state.guild_name_input.clear();
            state.selected_guild = None;
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            let load = load_guilds(server.clone(), token.clone());
            let convs = load_conversations(server, token);
            Task::batch(vec![
                Task::perform(load, Msg::GuildsLoaded),
                Task::perform(convs, Msg::ConversationsLoaded),
            ])
        }
        Msg::GuildCreated(Err(e)) => { state.guild_busy = false; handle_api_error(state, e) }
        Msg::JoinGuildSubmit => {
            state.guild_busy = true;
            let code = state.guild_join_code_input.trim().to_string();
            if code.is_empty() { state.guild_busy = false; return Task::none(); }
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            Task::perform(join_guild_api(server, token, code), Msg::GuildJoined)
        }
        Msg::GuildJoined(Ok(())) => {
            state.guild_busy = false;
            state.guild_modal_open = false;
            state.guild_join_code_input.clear();
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            let load = load_guilds(server.clone(), token.clone());
            let convs = load_conversations(server, token);
            Task::batch(vec![
                Task::perform(load, Msg::GuildsLoaded),
                Task::perform(convs, Msg::ConversationsLoaded),
            ])
        }
        Msg::GuildJoined(Err(e)) => { state.guild_busy = false; handle_api_error(state, e) }
        Msg::ChannelNameInput(s) => { state.channel_name_input = s; Task::none() }
        Msg::ChannelTypeChanged(v) => { state.channel_is_voice = v; Task::none() }
        Msg::CreateChannelSubmit(guild_id) => {
            state.guild_busy = true;
            let name = state.channel_name_input.trim().to_string();
            if name.is_empty() { state.guild_busy = false; return Task::none(); }
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            let channel_type = if state.channel_is_voice { "voice".to_string() } else { "text".to_string() };
            Task::perform(create_channel_api(server, token, guild_id, name, channel_type), Msg::ChannelCreated)
        }
        Msg::ChannelCreated(Ok(())) => {
            state.guild_busy = false;
            state.channel_name_input.clear();
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            let load = load_guilds(server.clone(), token.clone());
            let convs = load_conversations(server, token);
            Task::batch(vec![
                Task::perform(load, Msg::GuildsLoaded),
                Task::perform(convs, Msg::ConversationsLoaded),
            ])
        }
        Msg::ChannelCreated(Err(e)) => { state.guild_busy = false; handle_api_error(state, e) }
        Msg::DeleteGuild(guild_id) => {
            state.guild_busy = true;
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            Task::perform(delete_guild_api(server, token, guild_id), Msg::GuildDeleteResult)
        }
        Msg::GuildDeleteResult(Ok(())) => {
            state.guild_busy = false;
            state.guild_modal_open = false;
            state.selected_guild = None;
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            let load = load_guilds(server.clone(), token.clone());
            let convs = load_conversations(server, token);
            Task::batch(vec![
                Task::perform(load, Msg::GuildsLoaded),
                Task::perform(convs, Msg::ConversationsLoaded),
            ])
        }
        Msg::GuildDeleteResult(Err(e)) => { state.guild_busy = false; handle_api_error(state, e) }
        Msg::CreateInvite(guild_id) => {
            state.guild_busy = true;
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            Task::perform(create_invite_api(server, token, guild_id), Msg::InviteResult)
        }
        Msg::InviteResult(Ok(code)) => {
            state.guild_busy = false;
            state.info = format!("Invite code: {code} (valid 7 days)");
            let _ = iced::clipboard::write::<Msg>(code);
            Task::none()
        }
        Msg::InviteResult(Err(e)) => { state.guild_busy = false; handle_api_error(state, e) }
        Msg::SetRole { guild_id, user_id, is_admin } => {
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            Task::perform(set_role_api(server, token, guild_id, user_id, is_admin), Msg::GuildMemberAction)
        }
        Msg::TransferOwner { guild_id, user_id } => {
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            Task::perform(transfer_owner_api(server, token, guild_id, user_id), Msg::GuildMemberAction)
        }
        Msg::KickMember { guild_id, user_id } => {
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            Task::perform(kick_member_api(server, token, guild_id, user_id), Msg::GuildMemberAction)
        }
        Msg::GuildMemberAction(Ok(())) => {
            // Refresh the guild list + conversations after a role/kick change.
            let token = state.token.clone().unwrap_or_default();
            let server = state.server.clone();
            let load = load_guilds(server.clone(), token.clone());
            let convs = load_conversations(server, token);
            Task::batch(vec![
                Task::perform(load, Msg::GuildsLoaded),
                Task::perform(convs, Msg::ConversationsLoaded),
            ])
        }
        Msg::GuildMemberAction(Err(e)) => handle_api_error(state, e),
        Msg::DisplayNameChanged(s) => { state.display_name_input = s; Task::none() }
        Msg::SaveSettings => {
            let token = state.token.clone().unwrap_or_default();
            let display_name = state.display_name_input.clone();
            let bio = state.bio_input.clone();
            let profile_visible = state.user.as_ref().map(|u| u.profile_visible).unwrap_or(true);
            let server = state.server.clone();
            Task::perform(async move {
                let client = make_client();
                let resp = client.patch(format!("{server}/api/me"))
                    .bearer_auth(&token)
                    .json(&serde_json::json!({ "display_name": display_name, "bio": bio, "profile_visible": profile_visible }))
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
        Msg::VoiceReady(voice) => {
            state.voice = Some(voice);
            Task::none()
        }
        Msg::VoiceEvent(ev) => {
            match ev {
                VoiceEvent::Joined { .. } => {
                    state.voice_frames.clear();
                    state.voice_panel_open = true;
                }
                VoiceEvent::MemberLeft { user_id } => {
                    state.voice_frames.retain(|(uid, _), _| *uid != user_id);
                }
                VoiceEvent::Video { user_id, kind, width, height, rgba } => {
                    let handle = iced::widget::image::Handle::from_rgba(width, height, rgba);
                    state.voice_frames.insert((user_id, kind), (handle, width, height));
                }
                VoiceEvent::Left => {
                    state.voice_frames.clear();
                    state.voice_panel_open = false;
                }
                VoiceEvent::MemberJoined { .. } | VoiceEvent::Error(_) => {}
            }
            Task::none()
        }
        Msg::VoiceJoin(channel_id) => {
            let guild_id = state.guilds.iter().find_map(|g| {
                g.channels.iter().find(|c| c.id == channel_id).map(|_| g.id)
            });
            let (Some(guild_id), Some(voice), Some(user)) = (guild_id, &state.voice, &state.user)
            else {
                return Task::none();
            };
            voice.join(guild_id, channel_id, user.id);
            Task::none()
        }
        Msg::VoiceLeave => {
            if let Some(voice) = &state.voice {
                voice.leave();
            }
            Task::none()
        }
        Msg::VoiceToggleMute => {
            if let Some(voice) = &state.voice {
                voice.set_muted(!voice.is_muted());
            }
            Task::none()
        }
        Msg::VoiceToggleCamera => {
            if let Some(voice) = &state.voice {
                voice.set_camera(!voice.camera_on());
            }
            Task::none()
        }
        Msg::VoiceToggleScreen => {
            if let Some(voice) = &state.voice {
                voice.set_screen(!voice.screen_on());
            }
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
                P2pEvent::Complete { file_id, mime, name: _, path } => {
                    // The receiver already streamed the bytes to `path`.
                    let image_handle = std::fs::read(&path).ok().and_then(|bytes| {
                        if mime.starts_with("image/") {
                            Some(iced::widget::image::Handle::from_bytes(bytes))
                        } else {
                            None
                        }
                    });
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
        Msg::ToggleSettings => {
            state.settings_open = !state.settings_open;
            if state.settings_open {
                if let Some(u) = &state.user {
                    state.display_name_input = u.display_name.clone();
                    state.bio_input = u.bio.clone();
                }
                if state.accent_hex_input.is_empty() {
                    state.accent_hex_input = accent_to_hex(state.accent);
                }
            }
            Task::none()
        }
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
            state.left_tab = LeftTab::Dms;
            state.guilds.clear();
            state.selected_guild = None;
            let _ = std::fs::remove_file(dirs_next::home_dir().unwrap_or_default().join(".feditexter_session"));
            Task::none()
        }
        Msg::SessionExpired(reason) => {
            if let Some(voice) = &state.voice {
                voice.leave();
            }
            state.voice_frames.clear();
            state.voice_panel_open = false;
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
            state.left_tab = LeftTab::Dms;
            state.guilds.clear();
            state.selected_guild = None;
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

/// Send the current draft + pending attachment (if any) to the selected
/// conversation. Shared by `Msg::SendMessage` and sticker sends.
fn do_send(state: &mut AppState) -> Task<Msg> {
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

async fn mark_read_api(server: String, token: String, conv_id: u64, message_id: u64) -> Result<(), String> {
    let client = make_client();
    let resp = client.post(format!("{server}/api/conversations/{conv_id}/read"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "message_id": message_id }))
        .send().await;
    match resp {
        Ok(r) => {
            auth_aware_error(&r)?;
            if r.status().is_success() { Ok(()) } else { Err(format!("read failed: {}", r.status())) }
        }
        Err(e) => Err(format!("{e}")),
    }
}

async fn load_guilds(server: String, token: String) -> Result<Vec<Guild>, String> {
    let client = make_client();
    let resp = client.get(format!("{server}/api/servers"))
        .bearer_auth(&token).send().await;
    match resp {
        Ok(r) => {
            auth_aware_error(&r)?;
            let v: serde_json::Value = r.json().await.unwrap_or_default();
            serde_json::from_value(v.get("guilds").cloned().unwrap_or_default())
                .map_err(|e| format!("parse error: {e}"))
        }
        Err(e) => Err(format!("{e}")),
    }
}

async fn create_guild_api(server: String, token: String, name: String) -> Result<u64, String> {
    let client = make_client();
    let resp = client.post(format!("{server}/api/servers"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": name }))
        .send().await;
    match resp {
        Ok(r) => {
            auth_aware_error(&r)?;
            let v: serde_json::Value = r.json().await.unwrap_or_default();
            v.get("guild_id").and_then(|x| x.as_u64()).ok_or_else(|| "missing guild_id".to_string())
        }
        Err(e) => Err(format!("{e}")),
    }
}

async fn join_guild_api(server: String, token: String, code: String) -> Result<(), String> {
    let client = make_client();
    let resp = client.post(format!("{server}/api/servers/join"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "code": code }))
        .send().await;
    match resp {
        Ok(r) => {
            auth_aware_error(&r)?;
            if r.status().is_success() { Ok(()) } else { Err(format!("join failed: {}", r.status())) }
        }
        Err(e) => Err(format!("{e}")),
    }
}

async fn create_channel_api(
    server: String,
    token: String,
    guild_id: u64,
    name: String,
    channel_type: String,
) -> Result<(), String> {
    let client = make_client();
    let resp = client.post(format!("{server}/api/servers/{guild_id}/channels"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": name, "channel_type": channel_type }))
        .send().await;
    match resp {
        Ok(r) => {
            auth_aware_error(&r)?;
            if r.status().is_success() { Ok(()) } else { Err(format!("create failed: {}", r.status())) }
        }
        Err(e) => Err(format!("{e}")),
    }
}

async fn delete_guild_api(server: String, token: String, guild_id: u64) -> Result<(), String> {
    let client = make_client();
    let resp = client.delete(format!("{server}/api/servers/{guild_id}"))
        .bearer_auth(&token)
        .send().await;
    match resp {
        Ok(r) => {
            auth_aware_error(&r)?;
            if r.status().is_success() { Ok(()) } else { Err(format!("delete failed: {}", r.status())) }
        }
        Err(e) => Err(format!("{e}")),
    }
}

async fn create_invite_api(server: String, token: String, guild_id: u64) -> Result<String, String> {
    let client = make_client();
    let resp = client.post(format!("{server}/api/servers/{guild_id}/invite"))
        .bearer_auth(&token)
        .send().await;
    match resp {
        Ok(r) => {
            auth_aware_error(&r)?;
            let v: serde_json::Value = r.json().await.unwrap_or_default();
            v.get("code").and_then(|c| c.as_str()).map(str::to_string)
                .ok_or_else(|| "missing invite code".to_string())
        }
        Err(e) => Err(format!("{e}")),
    }
}

async fn set_role_api(server: String, token: String, guild_id: u64, user_id: u64, is_admin: bool) -> Result<(), String> {
    let client = make_client();
    let resp = client.post(format!("{server}/api/servers/{guild_id}/role"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "user_id": user_id, "is_admin": is_admin }))
        .send().await;
    match resp {
        Ok(r) => {
            auth_aware_error(&r)?;
            if r.status().is_success() { Ok(()) } else { Err(format!("role failed: {}", r.status())) }
        }
        Err(e) => Err(format!("{e}")),
    }
}

async fn transfer_owner_api(server: String, token: String, guild_id: u64, user_id: u64) -> Result<(), String> {
    let client = make_client();
    let resp = client.post(format!("{server}/api/servers/{guild_id}/transfer"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "user_id": user_id }))
        .send().await;
    match resp {
        Ok(r) => {
            auth_aware_error(&r)?;
            if r.status().is_success() { Ok(()) } else { Err(format!("transfer failed: {}", r.status())) }
        }
        Err(e) => Err(format!("{e}")),
    }
}

async fn kick_member_api(server: String, token: String, guild_id: u64, user_id: u64) -> Result<(), String> {
    let client = make_client();
    let resp = client.post(format!("{server}/api/servers/{guild_id}/kick"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "user_id": user_id }))
        .send().await;
    match resp {
        Ok(r) => {
            auth_aware_error(&r)?;
            if r.status().is_success() { Ok(()) } else { Err(format!("kick failed: {}", r.status())) }
        }
        Err(e) => Err(format!("{e}")),
    }
}

async fn load_sticker_packs(server: String, token: String, query: String) -> Result<Vec<StickerPack>, String> {
    let client = make_client();
    let url = if query.is_empty() {
        format!("{server}/api/stickers")
    } else {
        format!("{server}/api/stickers?q={}", url::form_urlencoded::byte_serialize(query.as_bytes()).collect::<String>())
    };
    let resp = client.get(&url).bearer_auth(&token).send().await;
    match resp {
        Ok(r) => {
            auth_aware_error(&r)?;
            let v: serde_json::Value = r.json().await.unwrap_or_default();
            serde_json::from_value(v.get("packs").cloned().unwrap_or_default())
                .map_err(|e| format!("parse error: {e}"))
        }
        Err(e) => Err(format!("{e}")),
    }
}

async fn create_sticker_pack_api(server: String, token: String, name: String) -> Result<u64, String> {
    let client = make_client();
    let resp = client.post(format!("{server}/api/stickers/packs"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": name }))
        .send().await;
    match resp {
        Ok(r) => {
            auth_aware_error(&r)?;
            let v: serde_json::Value = r.json().await.unwrap_or_default();
            v.get("id").and_then(|x| x.as_u64()).ok_or_else(|| "missing pack id".to_string())
        }
        Err(e) => Err(format!("{e}")),
    }
}

async fn sticker_image_api(server: String, token: String, sticker_id: u64) -> Result<Vec<u8>, String> {
    let client = make_client();
    let resp = client.get(format!("{server}/api/stickers/{sticker_id}/image"))
        .bearer_auth(&token)
        .send().await;
    match resp {
        Ok(r) => {
            auth_aware_error(&r)?;
            if r.status().is_success() {
                r.bytes().await.map(|b| b.to_vec()).map_err(|e| format!("{e}"))
            } else {
                Err(format!("sticker fetch failed: {}", r.status()))
            }
        }
        Err(e) => Err(format!("{e}")),
    }
}

async fn upload_sticker_api(server: String, token: String, pack_id: u64, name: String, data: Vec<u8>, mime: String) -> Result<(), String> {
    use base64::Engine;
    let data_b64 = base64::engine::general_purpose::STANDARD.encode(&data);
    let client = make_client();
    let resp = client.post(format!("{server}/api/stickers/packs/{pack_id}/stickers"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": name, "data": data_b64, "mime": mime }))
        .send().await;
    match resp {
        Ok(r) => {
            auth_aware_error(&r)?;
            if r.status().is_success() { Ok(()) } else { Err(format!("upload failed: {}", r.status())) }
        }
        Err(e) => Err(format!("{e}")),
    }
}

async fn delete_sticker_api(server: String, token: String, pack_id: u64, sticker_id: u64) -> Result<(), String> {
    let client = make_client();
    let resp = client.delete(format!("{server}/api/stickers/packs/{pack_id}/stickers/{sticker_id}"))
        .bearer_auth(&token).send().await;
    match resp {
        Ok(r) => {
            auth_aware_error(&r)?;
            if r.status().is_success() { Ok(()) } else { Err(format!("delete failed: {}", r.status())) }
        }
        Err(e) => Err(format!("{e}")),
    }
}

async fn delete_sticker_pack_api(server: String, token: String, pack_id: u64) -> Result<(), String> {
    let client = make_client();
    let resp = client.delete(format!("{server}/api/stickers/packs/{pack_id}"))
        .bearer_auth(&token).send().await;
    match resp {
        Ok(r) => {
            auth_aware_error(&r)?;
            if r.status().is_success() { Ok(()) } else { Err(format!("delete failed: {}", r.status())) }
        }
        Err(e) => Err(format!("{e}")),
    }
}

async fn load_guild_detail(server: String, token: String, guild_id: u64) -> Result<Guild, String> {
    let client = make_client();
    let resp = client.get(format!("{server}/api/servers/{guild_id}"))
        .bearer_auth(&token).send().await;
    match resp {
        Ok(r) => {
            auth_aware_error(&r)?;
            let v: serde_json::Value = r.json().await.unwrap_or_default();
            serde_json::from_value(v).map_err(|e| format!("parse error: {e}"))
        }
        Err(e) => Err(format!("{e}")),
    }
}

async fn create_role_api(server: String, token: String, guild_id: u64, name: String) -> Result<(), String> {
    let client = make_client();
    let resp = client.post(format!("{server}/api/servers/{guild_id}/roles"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": name }))
        .send().await;
    match resp {
        Ok(r) => {
            auth_aware_error(&r)?;
            if r.status().is_success() { Ok(()) } else { Err(format!("role failed: {}", r.status())) }
        }
        Err(e) => Err(format!("{e}")),
    }
}

async fn delete_role_api(server: String, token: String, guild_id: u64, role_id: u64) -> Result<(), String> {
    let client = make_client();
    let resp = client.delete(format!("{server}/api/servers/{guild_id}/roles/{role_id}"))
        .bearer_auth(&token).send().await;
    match resp {
        Ok(r) => {
            auth_aware_error(&r)?;
            if r.status().is_success() { Ok(()) } else { Err(format!("role failed: {}", r.status())) }
        }
        Err(e) => Err(format!("{e}")),
    }
}

async fn assign_role_api(server: String, token: String, guild_id: u64, role_id: u64, user_id: u64, on: bool) -> Result<(), String> {
    let client = make_client();
    let resp = client.post(format!("{server}/api/servers/{guild_id}/roles/{role_id}/assign"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "user_id": user_id, "on": on }))
        .send().await;
    match resp {
        Ok(r) => {
            auth_aware_error(&r)?;
            if r.status().is_success() { Ok(()) } else { Err(format!("assign failed: {}", r.status())) }
        }
        Err(e) => Err(format!("{e}")),
    }
}

async fn rename_channel_api(server: String, token: String, guild_id: u64, channel_id: u64, name: String) -> Result<(), String> {
    let client = make_client();
    let resp = client.patch(format!("{server}/api/servers/{guild_id}/channels/{channel_id}"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": name }))
        .send().await;
    match resp {
        Ok(r) => {
            auth_aware_error(&r)?;
            if r.status().is_success() { Ok(()) } else { Err(format!("rename failed: {}", r.status())) }
        }
        Err(e) => Err(format!("{e}")),
    }
}

async fn delete_channel_api(server: String, token: String, guild_id: u64, channel_id: u64) -> Result<(), String> {
    let client = make_client();
    let resp = client.delete(format!("{server}/api/servers/{guild_id}/channels/{channel_id}"))
        .bearer_auth(&token).send().await;
    match resp {
        Ok(r) => {
            auth_aware_error(&r)?;
            if r.status().is_success() { Ok(()) } else { Err(format!("delete failed: {}", r.status())) }
        }
        Err(e) => Err(format!("{e}")),
    }
}

async fn ban_member_api(server: String, token: String, guild_id: u64, user_id: u64) -> Result<(), String> {
    let client = make_client();
    let resp = client.post(format!("{server}/api/servers/{guild_id}/bans"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "user_id": user_id }))
        .send().await;
    match resp {
        Ok(r) => {
            auth_aware_error(&r)?;
            if r.status().is_success() { Ok(()) } else { Err(format!("ban failed: {}", r.status())) }
        }
        Err(e) => Err(format!("{e}")),
    }
}

async fn unban_member_api(server: String, token: String, guild_id: u64, user_id: u64) -> Result<(), String> {
    let client = make_client();
    let resp = client.delete(format!("{server}/api/servers/{guild_id}/bans/{user_id}"))
        .bearer_auth(&token).send().await;
    match resp {
        Ok(r) => {
            auth_aware_error(&r)?;
            if r.status().is_success() { Ok(()) } else { Err(format!("unban failed: {}", r.status())) }
        }
        Err(e) => Err(format!("{e}")),
    }
}

async fn load_sessions_api(server: String, token: String) -> Result<Vec<SessionInfo>, String> {
    let client = make_client();
    let resp = client.get(format!("{server}/api/me/sessions"))
        .bearer_auth(&token).send().await;
    match resp {
        Ok(r) => {
            auth_aware_error(&r)?;
            let v: serde_json::Value = r.json().await.unwrap_or_default();
            serde_json::from_value(v.get("sessions").cloned().unwrap_or_default())
                .map_err(|e| format!("parse error: {e}"))
        }
        Err(e) => Err(format!("{e}")),
    }
}

async fn revoke_session_api(server: String, token: String, session_id: u64) -> Result<(), String> {
    let client = make_client();
    let resp = client.delete(format!("{server}/api/me/sessions/{session_id}"))
        .bearer_auth(&token).send().await;
    match resp {
        Ok(r) => {
            auth_aware_error(&r)?;
            if r.status().is_success() { Ok(()) } else { Err(format!("revoke failed: {}", r.status())) }
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

    // Bluesky/fxbsky: the page is JS-rendered; use the public API.
    if preview_parse_bsky_url(&url).is_none() {
        // Specialised oEmbed handlers first (more reliable than scraping).
        if let Some((t, a, img)) = preview_youtube_oembed(&client, &url).await {
            title = Some(t);
            description = Some(a);
            image_urls.push(img);
        } else if let Some((t, a, img)) = preview_mastodon_oembed(&client, &url).await {
            title = Some(t);
            description = Some(a);
            image_urls.push(img);
        } else {
            // Generic: fetch + parse HTML meta tags.
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

/// YouTube oEmbed: reliable title/author/thumbnail without scraping.
async fn preview_youtube_oembed(client: &reqwest::Client, url: &str) -> Option<(String, String, String)> {
    if !(url.contains("youtube.com") || url.contains("youtu.be") || url.contains("youtube-nocookie.com")) {
        return None;
    }
    let api = format!(
        "https://www.youtube.com/oembed?url={}&format=json",
        url::form_urlencoded::byte_serialize(url.as_bytes()).collect::<String>()
    );
    let resp = client.get(&api).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let v: serde_json::Value = resp.json().await.ok()?;
    let title = v.get("title")?.as_str()?.to_string();
    let author = v.get("author_name").and_then(|a| a.as_str()).unwrap_or("YouTube").to_string();
    let thumb = v.get("thumbnail_url")?.as_str()?.to_string();
    if preview_looks_private(&thumb) {
        return None;
    }
    Some((title, format!("by {author}"), thumb))
}

/// Mastodon oEmbed: detect the instance from the URL and ask its oEmbed API.
async fn preview_mastodon_oembed(client: &reqwest::Client, url: &str) -> Option<(String, String, String)> {
    let host = url::Url::parse(url).ok()?.host_str()?.to_string();
    if host == "bsky.app" || host.ends_with(".bsky.app") {
        return None;
    }
    let api = format!(
        "https://{host}/api/oembed?url={}&format=json",
        url::form_urlencoded::byte_serialize(url.as_bytes()).collect::<String>()
    );
    let resp = client.get(&api).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let v: serde_json::Value = resp.json().await.ok()?;
    let title = v.get("title")?.as_str()?.to_string();
    let author = v.get("author_name").and_then(|a| a.as_str()).unwrap_or(&host).to_string();
    let thumb = v.get("thumbnail_url").and_then(|t| t.as_str()).unwrap_or("").to_string();
    if !thumb.is_empty() && preview_looks_private(&thumb) {
        return None;
    }
    Some((title, format!("@{author} · {host}"), thumb))
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
        Screen::TwoFaSetup => view_2fa_setup(state),
        Screen::Chat => view_chat(state),
    }
}

fn view_auth(state: &AppState) -> Element<'_, Msg> {
    let is_register = state.screen == Screen::Register;

    let logo: Element<'_, Msg> = text("FediTexter").size(state.zs(36)).into();
    let subtitle: Element<'_, Msg> = text(if is_register { "Create account" } else { "Sign in" }).size(state.zs(16))
        .color(iced::Color::from_rgb(0.6, 0.6, 0.6)).into();

    let server: Element<'_, Msg> = text_input("Server URL", &state.server)
        .on_input(Msg::LoginServerChanged)
        .width(Length::Fixed(state.z(320.0)))
        .into();

    let email: Element<'_, Msg> = text_input("Email", if is_register { &state.register_email } else { &state.login_email })
        .on_input(if is_register { Msg::RegisterEmailChanged } else { Msg::LoginEmailChanged })
        .width(Length::Fixed(state.z(320.0)))
        .into();

    let username: Option<Element<'_, Msg>> = if is_register {
        Some(text_input("Username", &state.register_username)
            .on_input(Msg::RegisterUsernameChanged)
            .width(Length::Fixed(state.z(320.0)))
            .into())
    } else {
        None
    };

    let password: Element<'_, Msg> = text_input("Password", if is_register { &state.register_password } else { &state.login_password })
        .on_input(if is_register { Msg::RegisterPasswordChanged } else { Msg::LoginPasswordChanged })
        .on_submit(if is_register { Msg::RegisterSubmit } else { Msg::LoginSubmit })
        .secure(true)
        .width(Length::Fixed(state.z(320.0)))
        .into();

    let birthdate: Option<Element<'_, Msg>> = if is_register {
        Some(text_input("Date of birth (YYYY-MM-DD, 18+)", &state.register_birthdate)
            .on_input(Msg::RegisterBirthdateChanged)
            .on_submit(Msg::RegisterSubmit)
            .width(Length::Fixed(state.z(320.0)))
            .into())
    } else {
        None
    };

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

    let toggle: Element<'_, Msg> = if is_register {
        button(text("Already have an account? Sign in").size(state.zs(13)))
            .on_press(Msg::ShowRegister(false))
            .into()
    } else {
        button(text("Create account").size(state.zs(13)))
            .on_press(Msg::ShowRegister(true))
            .into()
    };

    let mut form: iced::widget::Column<'_, Msg> = column![].spacing(state.z(14)).align_x(iced::Alignment::Center);
    form = form.push(logo);
    form = form.push(subtitle);
    form = form.push(server);
    form = form.push(email);
    if let Some(u) = username {
        form = form.push(u);
    }
    form = form.push(password);
    if let Some(b) = birthdate {
        form = form.push(b);
    }
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

/// Mandatory 2FA setup: show the QR + secret and let the user confirm a code
/// before they can use the app (2FA cannot be disabled).
fn view_2fa_setup(state: &AppState) -> Element<'_, Msg> {
    let title = text("Set up two-factor authentication").size(state.zs(28));
    let desc = text("Scan this QR code with your authenticator app, then enter the 6-digit code").size(state.zs(14))
        .color(iced::Color::from_rgb(0.6, 0.6, 0.6));

    let mut form = column![title, desc].spacing(state.z(14)).align_x(iced::Alignment::Center);

    match &state.twofa_setup {
        None => {
            // Setup request still in flight (or not started yet).
            let btn: Element<'_, Msg> = if state.twofa_busy {
                button(throbber(state.zs(16))).padding(state.z(6)).into()
            } else {
                button("Generate setup code").on_press(Msg::TwoFaSetup).padding(state.z(6)).into()
            };
            form = form.push(btn);
        }
        Some(info) => {
            let qr_handle = data_url_bytes(&info.qr)
                .map(iced::widget::image::Handle::from_bytes)
                .unwrap_or_else(|| iced::widget::image::Handle::from_rgba(1, 1, vec![0, 0, 0, 0]));
            let qr_img = iced::widget::Image::new(qr_handle).width(state.z(200)).height(state.z(200));
            let secret = text(format!("Secret: {}", info.secret)).size(state.zs(12))
                .color(iced::Color::from_rgb(0.7, 0.7, 0.7));
            let code_input = text_input("6-digit code", &state.twofa_toggle_code)
                .on_input(Msg::TwoFaCodeInput)
                .on_submit(Msg::TwoFaEnable)
                .width(Length::Fixed(state.z(320.0)));
            let enable_btn: Element<'_, Msg> = if state.twofa_busy {
                button(throbber(state.zs(16))).width(Length::Fixed(state.z(320.0))).into()
            } else {
                button("Enable 2FA").on_press(Msg::TwoFaEnable).width(Length::Fixed(state.z(320.0))).style(button::primary).into()
            };
            form = form.push(qr_img);
            form = form.push(secret);
            form = form.push(code_input);
            form = form.push(enable_btn);
        }
    }

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

            if profile.restricted {
                info = info.push(
                    container(row![
                        text("🔒").size(state.zs(12)),
                        text("This profile is private — no bio or avatar shown").size(state.zs(11)).color(iced::Color::from_rgb(0.6, 0.6, 0.6)),
                    ].spacing(state.z(6)).align_y(iced::Alignment::Center))
                        .padding([state.z(6.0), state.z(12.0)])
                        .style(composer_style)
                );
            } else if !profile.bio.is_empty() {
                info = info.push(text(&profile.bio).size(state.zs(13)).color(iced::Color::from_rgb(0.75, 0.75, 0.75)));
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

    if state.guild_modal_open {
        layers.push(view_guild_modal(state));
    }

    if state.guild_settings_open {
        layers.push(view_guild_settings(state));
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
        button(
            column![
                text("Server").size(state.zs(13)),
                text("Create or join a server").size(state.zs(10)).color(iced::Color::from_rgb(0.75, 0.75, 0.75)),
            ].spacing(state.z(2)).align_x(iced::Alignment::Center).width(Length::Fill)
        )
        .on_press(Msg::OpenServerModal)
        .width(Length::Fill)
        .height(Length::Fixed(state.z(64.0)))
        .padding([state.z(8.0), state.z(8.0)]),
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
    };
    let create_label = match state.new_conv_kind {
        NewConvKind::Direct => "Start chat",
        NewConvKind::Group => "Create group",
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

fn view_guild_modal(state: &AppState) -> Element<'_, Msg> {
    let close_btn = button(text("✕").size(state.zs(14))).on_press(Msg::CloseGuildModal).padding(state.z(4));

    let selected_guild = state.selected_guild
        .and_then(|gid| state.guilds.iter().find(|g| g.id == gid));

    let mut content: Option<iced::widget::Column<'_, Msg>> = None;

    if let Some(g) = selected_guild {
        // A guild is selected: the modal is only about adding a channel.
        let title = text("Add channel").size(state.zs(18));
        let channel_input = text_input("Channel name (e.g. general)", &state.channel_name_input)
            .on_input(Msg::ChannelNameInput)
            .on_submit(Msg::CreateChannelSubmit(g.id))
            .width(Length::Fill);
        let channel_btn: Element<'_, Msg> = if state.guild_busy {
            button(throbber(state.zs(14))).padding(state.z(6)).into()
        } else {
            button(text("Add channel").size(state.zs(13)))
                .on_press(Msg::CreateChannelSubmit(g.id))
                .style(button::primary)
                .padding([state.z(6.0), state.z(14.0)])
                .into()
        };
        let channel_type_row = row![
            button(text("# Text").size(state.zs(12)))
                .on_press(Msg::ChannelTypeChanged(false))
                .style(if state.channel_is_voice { button::secondary } else { button::primary })
                .padding(state.z(4)),
            button(text("🔊 Voice").size(state.zs(12)))
                .on_press(Msg::ChannelTypeChanged(true))
                .style(if state.channel_is_voice { button::primary } else { button::secondary })
                .padding(state.z(4)),
        ].spacing(state.z(6));
        content = Some(column![
            row![title, space::horizontal(), close_btn].align_y(iced::Alignment::Center),
            text(format!("Add a channel to {}", g.name)).size(state.zs(13)).color(iced::Color::from_rgb(0.75, 0.75, 0.75)),
            channel_type_row,
            channel_input,
            channel_btn,
        ].spacing(state.z(10)).align_x(iced::Alignment::Start));
    } else {
        let title = text("Servers").size(state.zs(18));
        let name_input = text_input("Server name (e.g. My Server)", &state.guild_name_input)
            .on_input(Msg::GuildNameInput)
            .on_submit(Msg::CreateGuildSubmit)
            .width(Length::Fill);
        let create_btn: Element<'_, Msg> = if state.guild_busy {
            button(throbber(state.zs(14))).padding(state.z(6)).into()
        } else {
            button(text("Create server").size(state.zs(13)))
                .on_press(Msg::CreateGuildSubmit)
                .style(button::primary)
                .padding([state.z(6.0), state.z(14.0)])
                .into()
        };

        let join_code_input = text_input("Invite code (e.g. 5f3a9c2b1d7e8a4f)", &state.guild_join_code_input)
            .on_input(Msg::GuildJoinCodeInput)
            .on_submit(Msg::JoinGuildSubmit)
            .width(Length::Fill);
        let join_btn: Element<'_, Msg> = if state.guild_busy {
            button(throbber(state.zs(14))).padding(state.z(6)).into()
        } else {
            button(text("Join server").size(state.zs(13)))
                .on_press(Msg::JoinGuildSubmit)
                .padding([state.z(6.0), state.z(14.0)])
                .into()
        };

        content = Some(column![
            row![title, space::horizontal(), close_btn].align_y(iced::Alignment::Center),
            text("Create a new server").size(state.zs(13)).color(iced::Color::from_rgb(0.75, 0.75, 0.75)),
            name_input,
            create_btn,
            rule::horizontal(1),
            text("Join an existing server").size(state.zs(13)).color(iced::Color::from_rgb(0.75, 0.75, 0.75)),
            join_code_input,
            join_btn,
        ].spacing(state.z(10)).align_x(iced::Alignment::Start));
    }

    let mut content = content.unwrap();

    if !state.error.is_empty() {
        content = content.push(text(&state.error).size(state.zs(12)).color(iced::Color::from_rgb(0.9, 0.3, 0.2)));
    }
    if !state.info.is_empty() {
        content = content.push(text(&state.info).size(state.zs(12)).color(iced::Color::from_rgb(0.5, 0.8, 0.5)));
    }

    let popup = container(content)
        .padding(state.z(20))
        .width(Length::Fixed(state.z(360.0)))
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
    .on_press(Msg::CloseGuildModal)
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

fn view_guild_settings(state: &AppState) -> Element<'_, Msg> {
    let close_btn = button(text("✕").size(state.zs(14))).on_press(Msg::CloseGuildSettings).padding(state.z(4));
    let guild = state.selected_guild.and_then(|id| state.guilds.iter().find(|g| g.id == id));
    let guild_name = guild.map(|g| g.name.clone()).unwrap_or_else(|| "Server".to_string());
    let title = text(format!("{guild_name} settings")).size(state.zs(18));
    let can_manage = guild.map(|g| g.can_manage).unwrap_or(false);
    let self_id = state.user.as_ref().map(|u| u.id);
    let is_owner = guild.map(|g| self_id == Some(g.owner_id)).unwrap_or(false);

    let tab_bar: Vec<Element<'_, Msg>> = [GuildSettingsTab::Channels, GuildSettingsTab::Roles, GuildSettingsTab::Bans].iter().map(|t| {
        let selected = state.guild_settings_tab == *t;
        let label = match t {
            GuildSettingsTab::Channels => "Channels",
            GuildSettingsTab::Roles => "Roles",
            GuildSettingsTab::Bans => "Bans",
        };
        button(text(label).size(state.zs(13)).color(if selected { iced::Color::WHITE } else { iced::Color::from_rgb(0.7, 0.7, 0.7) }))
            .on_press(Msg::GuildSettingsTabChanged(*t))
            .style(if selected { button::primary } else { button::secondary })
            .padding([state.z(6.0), state.z(12.0)])
            .into()
    }).collect();

    let mut content = column![
        row![title, space::horizontal(), close_btn].align_y(iced::Alignment::Center),
        row(tab_bar).spacing(state.z(6)),
        rule::horizontal(1),
    ].spacing(state.z(10));

    if !can_manage {
        content = content.push(
            text("Only the owner or an admin can manage this server.").size(state.zs(12)).color(iced::Color::from_rgb(0.6, 0.6, 0.6))
        );
    } else if let Some(g) = guild {
        match state.guild_settings_tab {
            GuildSettingsTab::Channels => {
                content = content.push(
                    text("Rename or delete channels, or add a new one.").size(state.zs(12)).color(iced::Color::from_rgb(0.75, 0.75, 0.75))
                );
                for ch in &g.channels {
                    let rename_value = state.channel_rename_inputs.get(&ch.id).map(String::as_str).unwrap_or(ch.name.as_str());
                    let rename_input = text_input("Channel name", rename_value)
                        .on_input(move |v| Msg::ChannelRenameInput { channel_id: ch.id, value: v })
                        .width(Length::Fixed(state.z(180.0)));
                    let row_el: Element<'_, Msg> = row![
                        text(if ch.is_voice() { "🔊" } else { "#" }).size(state.zs(13)).color(iced::Color::from_rgb(0.6, 0.6, 0.6)),
                        text(ch.name.clone()).size(state.zs(13)),
                        space::horizontal(),
                        rename_input,
                        button(text("Rename").size(state.zs(11))).on_press(Msg::RenameChannel { channel_id: ch.id, name: rename_value.to_string() }).padding(state.z(4)),
                        button(text("Delete").size(state.zs(11)).color(iced::Color::from_rgb(0.9, 0.5, 0.5)))
                            .on_press(Msg::DeleteChannel { channel_id: ch.id })
                            .style(button::text)
                            .padding(state.z(4)),
                    ].spacing(state.z(8)).align_y(iced::Alignment::Center).into();
                    content = content.push(row_el);
                }
                let add_input = text_input("New channel name", &state.channel_name_input)
                    .on_input(Msg::ChannelNameInput)
                    .on_submit(Msg::CreateChannelSubmit(g.id))
                    .width(Length::Fixed(state.z(200.0)));
                let add_type = row![
                    button(text("#").size(state.zs(12)))
                        .on_press(Msg::ChannelTypeChanged(false))
                        .style(if state.channel_is_voice { button::secondary } else { button::primary })
                        .padding(state.z(2)),
                    button(text("🔊").size(state.zs(12)))
                        .on_press(Msg::ChannelTypeChanged(true))
                        .style(if state.channel_is_voice { button::primary } else { button::secondary })
                        .padding(state.z(2)),
                ].spacing(state.z(4));
                content = content.push(row![add_type, add_input, button(text("Add channel").size(state.zs(12))).on_press(Msg::CreateChannelSubmit(g.id)).padding(state.z(4))].spacing(state.z(8)).align_y(iced::Alignment::Center));
            }
            GuildSettingsTab::Roles => {
                content = content.push(
                    text("Roles are shown per member; toggling assigns or revokes them.").size(state.zs(12)).color(iced::Color::from_rgb(0.75, 0.75, 0.75))
                );
                for role in &g.roles {
                    let role_tag = if role.is_admin { " (admin)" } else { "" };
                    let role_header: Element<'_, Msg> = row![
                        text(format!("{}{role_tag}", role.name)).size(state.zs(13)),
                        if role.is_admin || !is_owner {
                            let spacer: Element<'_, Msg> = space::horizontal().into();
                            spacer
                        } else {
                            row![space::horizontal(), button(text("delete").size(state.zs(10)).color(iced::Color::from_rgb(0.9, 0.5, 0.5)))
                                .on_press(Msg::DeleteRole { guild_id: g.id, role_id: role.id })
                                .style(button::text).padding(state.z(0))].into()
                        },
                    ].spacing(state.z(6)).align_y(iced::Alignment::Center).into();
                    content = content.push(role_header);
                    if !role.is_admin {
                        for m in &g.members {
                            if Some(m.id) == self_id { continue; }
                            let assigned = role.member_ids.contains(&m.id);
                            let name = if m.display_name.is_empty() { m.username.as_str() } else { m.display_name.as_str() };
                            let toggle = button(text(if assigned { "✓ on" } else { "off" }).size(state.zs(11)))
                                .on_press(Msg::AssignRole { guild_id: g.id, role_id: role.id, user_id: m.id, on: !assigned })
                                .padding(state.z(2));
                            content = content.push(row![text(name.to_string()).size(state.zs(12)), space::horizontal(), toggle].spacing(state.z(8)).align_y(iced::Alignment::Center));
                        }
                    }
                }
                let role_input = text_input("New role name", &state.role_name_input)
                    .on_input(Msg::RoleNameInput)
                    .on_submit(Msg::CreateRoleSubmit(g.id))
                    .width(Length::Fixed(state.z(180.0)));
                content = content.push(row![role_input, button(text("Create role").size(state.zs(12))).on_press(Msg::CreateRoleSubmit(g.id)).padding(state.z(4))].spacing(state.z(8)).align_y(iced::Alignment::Center));
            }
            GuildSettingsTab::Bans => {
                content = content.push(text("Banned users").size(state.zs(13)).color(iced::Color::from_rgb(0.75, 0.75, 0.75)));
                if g.bans.is_empty() {
                    content = content.push(text("No banned users").size(state.zs(12)).color(iced::Color::from_rgb(0.5, 0.5, 0.5)));
                }
                for b in &g.bans {
                    let name = if b.display_name.is_empty() { b.username.as_str() } else { b.display_name.as_str() };
                    content = content.push(row![text(name.to_string()).size(state.zs(12)), space::horizontal(),
                        button(text("Unban").size(state.zs(11)).color(iced::Color::from_rgb(0.5, 0.8, 0.5)))
                            .on_press(Msg::UnbanMember { guild_id: g.id, user_id: b.id })
                            .style(button::text).padding(state.z(0))]
                        .spacing(state.z(8)).align_y(iced::Alignment::Center));
                }
                content = content.push(rule::horizontal(1));
                content = content.push(text("Members").size(state.zs(13)).color(iced::Color::from_rgb(0.75, 0.75, 0.75)));
                let admin_ids: Vec<u64> = g.roles.iter()
                    .filter(|r| r.is_admin)
                    .flat_map(|r| r.member_ids.iter().copied())
                    .collect();
                for m in &g.members {
                    if Some(m.id) == self_id { continue; }
                    let name = if m.display_name.is_empty() { m.username.as_str() } else { m.display_name.as_str() };
                    let is_owner_target = Some(m.id) == Some(g.owner_id);
                    let is_admin_member = admin_ids.contains(&m.id);
                    let mut ctrl: Vec<Element<'_, Msg>> = Vec::new();
                    if is_owner_target {
                        ctrl.push(text("👑 owner").size(state.zs(11)).color(iced::Color::from_rgb(0.9, 0.8, 0.3)).into());
                    }
                    if is_admin_member {
                        ctrl.push(text("admin").size(state.zs(11)).color(iced::Color::from_rgb(0.5, 0.8, 0.5)).into());
                    }
                    if is_owner {
                        if !is_owner_target {
                            ctrl.push(button(text(if is_admin_member { "demote" } else { "promote" }).size(state.zs(11)))
                                .on_press(Msg::SetRole { guild_id: g.id, user_id: m.id, is_admin: !is_admin_member })
                                .style(button::text).padding(state.z(0)).into());
                            ctrl.push(button(text("transfer").size(state.zs(11)).color(iced::Color::from_rgb(0.9, 0.8, 0.3)))
                                .on_press(Msg::TransferOwner { guild_id: g.id, user_id: m.id })
                                .style(button::text).padding(state.z(0)).into());
                        }
                    }
                    if can_manage && !is_owner_target {
                        ctrl.push(button(text("Ban").size(state.zs(11)).color(iced::Color::from_rgb(0.9, 0.5, 0.5)))
                            .on_press(Msg::BanMember { guild_id: g.id, user_id: m.id })
                            .style(button::text).padding(state.z(0)).into());
                    }
                    content = content.push(row![
                        text(name.to_string()).size(state.zs(12)),
                        space::horizontal(),
                        row(ctrl).spacing(state.z(8)).align_y(iced::Alignment::Center),
                    ].spacing(state.z(8)).align_y(iced::Alignment::Center));
                }
            }
        }
        if is_owner {
            content = content.push(rule::horizontal(1));
            content = content.push(button(text("Delete server…").size(state.zs(12))).on_press(Msg::DeleteGuild(g.id)).style(danger_text_button));
        }
    }

    if !state.error.is_empty() {
        content = content.push(text(&state.error).size(state.zs(12)).color(iced::Color::from_rgb(0.9, 0.3, 0.2)));
    }

    let popup = container(scrollable(content).height(Length::Shrink))
        .padding(state.z(20))
        .width(Length::Fixed(state.z(460.0)))
        .max_height(state.z(560.0))
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
    .on_press(Msg::CloseGuildSettings)
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

    // Sub-tabs: General / Privacy / Devices.
    let tab_bar: Vec<Element<'_, Msg>> = [SettingsTab::General, SettingsTab::Privacy, SettingsTab::Devices].iter().map(|t| {
        let selected = state.settings_tab == *t;
        let label = match t {
            SettingsTab::General => "General",
            SettingsTab::Privacy => "Privacy",
            SettingsTab::Devices => "Devices",
        };
        button(text(label).size(state.zs(13)).color(if selected { iced::Color::WHITE } else { iced::Color::from_rgb(0.7, 0.7, 0.7) }))
            .on_press(Msg::SettingsTabChanged(*t))
            .style(if selected { button::primary } else { button::secondary })
            .padding([state.z(6.0), state.z(12.0)])
            .into()
    }).collect();
    let tab_row = row(tab_bar).spacing(state.z(6));

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

    // Auto-shades preview: light / base / dark generated from the current accent.
    let shade_swatch = |c: iced::Color| {
        container(text("").size(state.zs(1)))
            .width(Length::Fixed(state.z(30.0)))
            .height(Length::Fixed(state.z(18.0)))
            .style(move |_: &iced::Theme| iced::widget::container::Style {
                background: Some(c.into()),
                border: iced::Border { radius: 4.0.into(), width: 1.0, color: iced::Color::from_rgb(0.3, 0.3, 0.3), ..iced::Border::default() },
                ..iced::widget::container::Style::default()
            })
    };
    let shades_row = row![
        shade_swatch(accent_light(state.accent)),
        shade_swatch(state.accent),
        shade_swatch(accent_dark(state.accent)),
        text("auto-shades").size(state.zs(10)).color(iced::Color::from_rgb(0.5, 0.5, 0.5)),
    ].spacing(state.z(6)).align_y(iced::Alignment::Center);

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

    // ---- Two-factor section ----
    let totp_enabled = state.user.as_ref().map(|u| u.totp_enabled).unwrap_or(false);
    let twofa_label = text("Two-factor authentication").size(state.zs(14))
        .color(iced::Color::from_rgb(0.6, 0.6, 0.6));

    let twofa_section: Element<'_, Msg> = if totp_enabled {
        // 2FA is mandatory and cannot be disabled — show a status line only.
        row![
            text("●").size(state.zs(10)).color(iced::Color::from_rgb(0.3, 0.8, 0.3)),
            text("Enabled (required)").size(state.zs(13)).color(iced::Color::from_rgb(0.7, 0.7, 0.7)),
        ].spacing(state.z(8)).align_y(iced::Alignment::Center).into()
    } else {
        // Defensive: if a user somehow reaches settings without 2FA, offer setup.
        let setup_btn: Element<'_, Msg> = if state.twofa_busy {
            button(throbber(state.zs(13))).padding(state.z(6)).into()
        } else if state.twofa_setup.is_none() {
            button(text("Set up 2FA").size(state.zs(13)))
                .on_press(Msg::TwoFaSetup)
                .padding([state.z(6.0), state.z(14.0)])
                .into()
        } else {
            button(text("  ").size(state.zs(13))).padding(state.z(6)).into()
        };

        let setup_body: Option<Element<'_, Msg>> = state.twofa_setup.as_ref().map(|info| {
            let secret = text(format!("Secret: {}", info.secret)).size(state.zs(12))
                .color(iced::Color::from_rgb(0.7, 0.7, 0.7));
            let qr_handle = data_url_bytes(&info.qr)
                .map(iced::widget::image::Handle::from_bytes)
                .unwrap_or_else(|| iced::widget::image::Handle::from_rgba(1, 1, vec![0, 0, 0, 0]));
            let qr_img = iced::widget::Image::new(qr_handle).width(state.z(160)).height(state.z(160));
            let code_input = text_input("Enter code from app", &state.twofa_toggle_code)
                .on_input(Msg::TwoFaCodeInput)
                .width(Length::Fixed(state.z(200.0)));
            let confirm_btn: Element<'_, Msg> = if state.twofa_busy {
                button(throbber(state.zs(13))).padding(state.z(6)).into()
            } else {
                button(text("Enable 2FA").size(state.zs(13)))
                    .on_press(Msg::TwoFaEnable)
                    .style(button::primary)
                    .padding([state.z(6.0), state.z(14.0)])
                    .into()
            };
            column![qr_img, secret, row![code_input, confirm_btn].spacing(state.z(8)).align_y(iced::Alignment::Center)]
                .spacing(state.z(8))
                .into()
        });

        let mut col = column![row![setup_btn].spacing(state.z(8))];
        if let Some(body) = setup_body {
            col = col.push(body);
        }
        col.into()
    };

    let bio_label = text("Bio").size(state.zs(14))
        .color(iced::Color::from_rgb(0.6, 0.6, 0.6));
    let bio_input = text_input("A short bio shown on your profile", &state.bio_input)
        .on_input(Msg::BioChanged)
        .width(Length::Fixed(state.z(320.0)));

    let accent_hex_input = text_input("#rrggbb", &state.accent_hex_input)
        .on_input(Msg::AccentHexChanged)
        .on_submit(Msg::ApplyAccentHex)
        .width(Length::Fixed(state.z(110.0)));
    let apply_hex_btn = button(text("Apply").size(state.zs(12)))
        .on_press(Msg::ApplyAccentHex)
        .padding([state.z(5.0), state.z(10.0)]);

    // General tab: profile fields, accent (swatches + custom hex), 2FA.
    let general_tab = column![
        rule::horizontal(1),
        pfp_label,
        pfp_row,
        rule::horizontal(1),
        display_name_label,
        display_name_input,
        bio_label,
        bio_input,
        save_btn,
        rule::horizontal(1),
        accent_label,
        accent_row,
        shades_row,
        row![accent_hex_input, apply_hex_btn].spacing(state.z(6)).align_y(iced::Alignment::Center),
        text("Custom colour (auto shades are generated from it)").size(state.zs(10)).color(iced::Color::from_rgb(0.5, 0.5, 0.5)),
        rule::horizontal(1),
        twofa_label,
        twofa_section,
    ].spacing(state.z(12));

    // Privacy tab: profile visibility.
    let profile_visible = state.user.as_ref().map(|u| u.profile_visible).unwrap_or(true);
    let privacy_tab = column![
        rule::horizontal(1),
        text("Profile visibility").size(state.zs(14)).color(iced::Color::from_rgb(0.6, 0.6, 0.6)),
        text("When hidden, other users can still find and message you, but your profile (bio, avatar) appears bare-bones.").size(state.zs(11)).color(iced::Color::from_rgb(0.5, 0.5, 0.5)),
        row![
            text(if profile_visible { "Visible to everyone" } else { "Hidden (bare-bones for others)" }).size(state.zs(12)),
            space::horizontal(),
            button(text(if profile_visible { "Hide" } else { "Show" }).size(state.zs(12)))
                .on_press(Msg::ProfileVisibleToggled(!profile_visible))
                .padding([state.z(5.0), state.z(12.0)]),
        ].spacing(state.z(8)).align_y(iced::Alignment::Center),
    ].spacing(state.z(10));

    // Devices tab: logged-in sessions.
    let mut devices_tab = column![
        rule::horizontal(1),
        text("Logged-in devices").size(state.zs(14)).color(iced::Color::from_rgb(0.6, 0.6, 0.6)),
    ].spacing(state.z(10));
    if state.sessions_busy {
        devices_tab = devices_tab.push(row![throbber(state.z(14)), text("Loading…").size(state.zs(11)).color(iced::Color::from_rgb(0.5, 0.5, 0.5))]
            .spacing(state.z(6)).align_y(iced::Alignment::Center));
    } else if state.sessions.is_empty() {
        devices_tab = devices_tab.push(text("No other devices found").size(state.zs(12)).color(iced::Color::from_rgb(0.5, 0.5, 0.5)));
    } else {
        for s in &state.sessions {
            let dev = s.device_id.clone().unwrap_or_else(|| "Unknown device".to_string());
            let ip = s.login_ip.clone().unwrap_or_else(|| "unknown IP".to_string());
            let tag = if s.current { "this device" } else { "" };
            let revoke: Element<'_, Msg> = if s.current {
                text("").into()
            } else {
                button(text("Revoke").size(state.zs(11)).color(iced::Color::from_rgb(0.9, 0.5, 0.5)))
                    .on_press(Msg::RevokeSession(s.id))
                    .style(button::text)
                    .padding(state.z(0))
                    .into()
            };
            let row_el: Element<'_, Msg> = container(
                row![
                    column![
                        text(format!("{dev} {tag}")).size(state.zs(12)),
                        text(format!("{ip} · {}", format_local_time(&s.created_at))).size(state.zs(10)).color(iced::Color::from_rgb(0.5, 0.5, 0.5)),
                    ].spacing(state.z(2)),
                    space::horizontal(),
                    revoke,
                ].spacing(state.z(8)).align_y(iced::Alignment::Center),
            )
            .padding(state.z(8))
            .style(composer_style)
            .into();
            devices_tab = devices_tab.push(row_el);
        }
    }

    let tab_content: Element<'_, Msg> = match state.settings_tab {
        SettingsTab::General => general_tab.into(),
        SettingsTab::Privacy => privacy_tab.into(),
        SettingsTab::Devices => devices_tab.into(),
    };

    let content = column![
        back_btn,
        title,
        text(format!("Username: {username}")).size(state.zs(14)).color(iced::Color::from_rgb(0.6, 0.6, 0.6)),
        text(format!("Email: {email}")).size(state.zs(14)).color(iced::Color::from_rgb(0.6, 0.6, 0.6)),
        tab_row,
        tab_content,
        rule::horizontal(1),
        logout_btn,
    ].spacing(state.z(12)).padding(state.z(24)).max_width(state.z(560));

    container(scrollable(content))
        .center_x(Length::Fill)
        .center_y(Length::Fill)
        .into()
}

/// A single guild channel row in the sidebar: text channels open a
/// conversation, voice channels join the voice chat.
fn guild_channel_item<'a>(state: &'a AppState, ch: &'a GuildChannel) -> Element<'a, Msg> {
    let is_selected = state.selected_conversation == Some(ch.id);
    let is_in_voice = state.voice.as_ref().map(|v| v.in_channel()) == Some(Some(ch.id));
    let unread = state.unread.get(&ch.id).copied().unwrap_or(0);
    let prefix = if ch.is_voice() { "🔊" } else { "#" };
    let label: Element<'_, Msg> = if unread > 0 && !ch.is_voice() {
        row![
            container(text(prefix).size(state.zs(14)).color(iced::Color::from_rgb(0.6, 0.6, 0.6))),
            text(&ch.name).size(state.zs(14)),
            space::horizontal(),
            container(text(format!("{unread}")).size(state.zs(10)).color(iced::Color::WHITE))
                .padding([state.z(1.0), state.z(5.0)])
                .style(|_: &iced::Theme| iced::widget::container::Style {
                    background: Some(state.accent.into()),
                    border: iced::Border { radius: 9.0.into(), ..iced::Border::default() },
                    ..iced::widget::container::Style::default()
                }),
        ].spacing(state.z(6)).align_y(iced::Alignment::Center).into()
    } else {
        row![
            container(text(prefix).size(state.zs(14)).color(iced::Color::from_rgb(0.6, 0.6, 0.6))),
            text(&ch.name).size(state.zs(14)),
            space::horizontal(),
        ].spacing(state.z(6)).align_y(iced::Alignment::Center).into()
    };
    let msg = if ch.is_voice() {
        Msg::VoiceJoin(ch.id)
    } else {
        Msg::SelectConversation(ch.id)
    };
    let style = if is_selected {
        button::primary
    } else if is_in_voice {
        button::success
    } else {
        button::secondary
    };
    button(label)
        .on_press(msg)
        .width(Length::Fill)
        .padding([state.z(6.0), state.z(8.0)])
        .style(style)
        .into()
}

fn view_sidebar(state: &AppState) -> Element<'_, Msg> {    let header = row![
        text("Conversations").size(state.zs(18)),
    ].align_y(iced::Alignment::Center).spacing(state.z(8));

    // ----- Server rail (leftmost, Discord-style) -----
    let guild_rail_items: Vec<Element<'_, Msg>> = state.guilds.iter().map(|g| {        let selected = state.selected_guild == Some(g.id);
        let initials = user_initials(&g.name);
        let hue = name_hue(&g.name);
        let btn = button(
            container(text(initials).size(state.zs(13)).color(iced::Color::WHITE))
                .width(state.z(44.0))
                .height(state.z(44.0))
                .center_x(Length::Fixed(state.z(44.0)))
                .center_y(Length::Fixed(state.z(44.0)))
                .style(move |_: &iced::Theme| iced::widget::container::Style {
                    background: Some(hsl_to_rgb(hue, 0.6, 0.4).into()),
                    border: iced::Border {
                        radius: if selected { 14.0 } else { 22.0 }.into(),
                        ..iced::Border::default()
                    },
                    ..iced::widget::container::Style::default()
                })
        )
        .on_press(Msg::SelectGuild(Some(g.id)))
        .style(if selected { button::primary } else { button::text })
        .padding(state.z(2));
        mouse_area(btn)
            .on_right_press(Msg::SelectGuild(Some(g.id)))
            .into()
    }).collect();

    let new_guild_btn = button(
        container(text("＋").size(state.zs(20)).color(state.accent))
            .width(state.z(44.0))
            .height(state.z(44.0))
            .center_x(Length::Fixed(state.z(44.0)))
            .center_y(Length::Fixed(state.z(44.0)))
            .style(|_: &iced::Theme| iced::widget::container::Style {
                background: Some(iced::Color::from_rgb(0.16, 0.17, 0.19).into()),
                border: iced::Border {
                    radius: 22.0.into(),
                    width: 1.0,
                    color: iced::Color::from_rgb(0.3, 0.3, 0.3),
                    ..iced::Border::default()
                },
                ..iced::widget::container::Style::default()
            })
    )
    .on_press(Msg::OpenServerModal)
    .style(button::text)
    .padding(state.z(2));

    let guild_rail = column(
        guild_rail_items
            .into_iter()
            .collect::<Vec<_>>()
    )
    .push(new_guild_btn)
    .spacing(state.z(6))
    .padding(state.z(8))
    .width(Length::Fixed(state.z(56.0)))
    .height(Length::Fill);

    // ----- Main list: guild channels or flat conversations -----
    let conv_list: Element<'_, Msg> = if let Some(guild_id) = state.selected_guild {
        let guild = state.guilds.iter().find(|g| g.id == guild_id);
        match guild {
            Some(g) => {
                let text_channels: Vec<&GuildChannel> =
                    g.channels.iter().filter(|ch| !ch.is_voice()).collect();
                let voice_channels: Vec<&GuildChannel> =
                    g.channels.iter().filter(|ch| ch.is_voice()).collect();

                let mut col = column![].spacing(state.z(2));
                for ch in text_channels {
                    col = col.push(guild_channel_item(state, ch));
                }
                if !voice_channels.is_empty() {
                    col = col.push(
                        container(text("VOICE").size(state.zs(10)).color(iced::Color::from_rgb(0.5, 0.5, 0.5)))
                            .padding([state.z(8.0), 0.0]),
                    );
                    for ch in voice_channels {
                        col = col.push(guild_channel_item(state, ch));
                    }
                }

                let add_channel_btn: Element<'_, Msg> = if state.guild_busy {
                    button(throbber(state.z(12))).padding(state.z(4)).into()
                } else {
                    button(text("＋ channel").size(state.zs(12)))
                        .on_press(Msg::OpenGuildModal)
                        .padding(state.z(4))
                        .into()
                };
                column![
                    col,
                    add_channel_btn,
                ].spacing(state.z(6)).into()
            }
            None => text("Guild not found").size(state.zs(13)).into(),
        }
    } else if state.conversations.iter().all(|c| c.guild_id.is_some()) {
        container(text("No conversations yet").size(state.zs(13)).color(iced::Color::from_rgb(0.5, 0.5, 0.5)))
            .center_x(Length::Fill)
            .center_y(Length::Fill)
            .into()
    } else {
        let items: Vec<Element<'_, Msg>> = state.conversations.iter().filter(|c| c.guild_id.is_none()).map(|c| {
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

    let main_panel: Element<'_, Msg> = match state.left_tab {
        LeftTab::Dms => column![
            header,
            new_conv_btn,
            scrollable(conv_list).height(Length::Fill),
        ].spacing(state.z(8)).padding(state.z(12)).into(),
        LeftTab::Servers => {
            if state.selected_guild.is_some() {
                let guild = state.guilds.iter().find(|g| Some(g.id) == state.selected_guild);
                let guild_header = row![
                    text(guild.map(|g| g.name.clone()).unwrap_or_default()).size(state.zs(16)),
                    space::horizontal(),
                    button(text("⚙").size(state.zs(14)))
                        .on_press(Msg::OpenGuildSettings)
                        .padding(state.z(2)),
                ].spacing(state.z(8)).align_y(iced::Alignment::Center);
                column![
                    guild_header,
                    scrollable(conv_list).height(Length::Fill),
                ].spacing(state.z(8)).padding(state.z(12)).into()
            } else {
                column![
                    text("Servers").size(state.zs(16)),
                    text("Pick a server from the left rail, or create one").size(state.zs(12)).color(iced::Color::from_rgb(0.6, 0.6, 0.6)),
                    space::vertical().height(state.z(4)),
                    button(text("＋ Create / join a server").size(state.zs(13)))
                        .on_press(Msg::OpenServerModal)
                        .width(Length::Fill)
                        .style(button::primary)
                        .padding([state.z(8.0), state.z(10.0)]),
                ].spacing(state.z(8)).padding(state.z(12)).into()
            }
        }
    };

    // ----- Bottom tab bar: DMs / Servers / Settings -----
    let dms_selected = state.left_tab == LeftTab::Dms;
    let servers_selected = state.left_tab == LeftTab::Servers;
    let dms_tab = button(text("DMs").size(state.zs(12)))
        .on_press(Msg::SetLeftTab(LeftTab::Dms))
        .width(Length::Fill)
        .padding([state.z(6.0), state.z(6.0)])
        .style(if dms_selected { button::primary } else { button::secondary });
    let servers_tab = button(text("Servers").size(state.zs(12)))
        .on_press(Msg::SetLeftTab(LeftTab::Servers))
        .width(Length::Fill)
        .padding([state.z(6.0), state.z(6.0)])
        .style(if servers_selected { button::primary } else { button::secondary });
    let settings_tab = button(text("Settings").size(state.zs(12)))
        .on_press(Msg::ToggleSettings)
        .width(Length::Fill)
        .padding([state.z(6.0), state.z(6.0)]);
    let bottom_bar = container(row![dms_tab, servers_tab, settings_tab].spacing(state.z(4)))
        .padding(state.z(6))
        .width(Length::Fill)
        .style(|theme: &iced::Theme| {
            let p = theme.extended_palette();
            iced::widget::container::Style {
                background: Some(p.background.weak.color.into()),
                ..iced::widget::container::Style::default()
            }
        });

    let rail_panel: Element<'_, Msg> = if state.left_tab == LeftTab::Servers {
        row![
            guild_rail,
            container(main_panel)
                .width(Length::Fixed(state.z(224.0)))
                .height(Length::Fill),
        ].spacing(state.z(0)).height(Length::Fill).into()
    } else {
        container(main_panel)
            .width(Length::Fill)
            .height(Length::Fill)
            .into()
    };

    let sidebar_content = column![
        rail_panel,
        bottom_bar,
    ].height(Length::Fill);

    container(sidebar_content)
        .width(Length::Fixed(state.z(280.0)))
        .height(Length::Fill)
        .style(sidebar_style)
        .into()
}

 fn view_chat_area(state: &AppState) -> Element<'_, Msg> {
    let Some(conv_id) = state.selected_conversation else {
        let placeholder: Element<'_, Msg> = container(
            column![
                text("FediTexter").size(state.zs(28)),
                text("Select a conversation").size(state.zs(14)).color(iced::Color::from_rgb(0.5, 0.5, 0.5)),
            ].spacing(state.z(8)).align_x(iced::Alignment::Center)
        )
        .center_x(Length::Fill)
        .center_y(Length::Fill)
        .into();
        return if let Some(panel) = view_voice_panel(state) {
            column![panel, placeholder].spacing(state.z(6)).into()
        } else {
            placeholder
        };
    };

    let conv = state.conversations.iter().find(|c| c.id == conv_id);
    let members = conv.map(|c| c.members.clone()).unwrap_or_default();
    let self_id = state.user.as_ref().map(|u| u.id);
    let is_group = matches!(conv.map(|c| c.kind.as_str()), Some("group") | Some("large_group"));

    let other_member = conv
        .and_then(|c| c.members.iter().find(|m| Some(m.id) != self_id));

    let guild_name = state.selected_guild.and_then(|id| state.guilds.iter().find(|g| g.id == id).map(|g| g.name.clone()));
    let channel_name = if is_group {
        conv.and_then(|c| c.guild_id.and_then(|_| {
            state.selected_guild.and_then(|gid| state.guilds.iter().find(|g| g.id == gid))
                .and_then(|g| g.channels.iter().find(|ch| ch.id == conv_id).map(|ch| ch.name.clone()))
        }))
    } else {
        None
    };

    let header_name = if let Some(gname) = &guild_name {
        gname.clone()
    } else if is_group {
        format!("Group · {} members", members.len())
    } else {
        other_member
            .map(|m| if m.display_name.is_empty() { m.username.as_str() } else { m.display_name.as_str() })
            .unwrap_or("Unknown")
            .to_string()
    };

    let header_sub = if let Some(cname) = &channel_name {
        Some(cname.clone())
    } else if is_group {
        Some(format!("{} members", members.len()))
    } else {
        None
    };

    let header_initials = user_initials(&header_name);
    let header_hue = name_hue(&header_name);

    let online = other_member
        .map(|m| state.presence.get(&m.id).copied().unwrap_or(false))
        .unwrap_or(false);
    let status = if is_group {
        header_sub.clone().unwrap_or_else(|| "Group conversation".to_string())
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

    let header = container(row![header_content, space::horizontal()])
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
                card_content = card_content.push(
                    button(img_el)
                        .on_press(Msg::OpenFile(m.id))
                        .style(button::text)
                        .padding(state.z(0))
                );
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

            let mut file_row = row![
                text("📎").size(state.zs(14)),
                text(name).size(state.zs(12)),
                text(human_size(size)).size(state.zs(10)).color(iced::Color::from_rgb(0.5, 0.5, 0.5)),
                space::horizontal(),
            ];
            if !is_image {
                file_row = file_row.push(
                    if held {
                        button(text("Open").size(state.zs(11)).color(iced::Color::from_rgb(0.6, 0.8, 1.0)))
                            .on_press(Msg::OpenFile(m.id))
                            .style(button::text)
                            .padding(state.z(0))
                    } else {
                        button(text("Open").size(state.zs(11)).color(iced::Color::from_rgb(0.4, 0.4, 0.4)))
                            .style(button::text)
                            .padding(state.z(0))
                    }
                );
            }
            card_content = card_content.push(file_row);

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
        let sticker_btn = button(text("🖼️").size(state.zs(16)))
            .on_press(Msg::ToggleStickerMenu)
            .padding([state.z(8.0), state.z(10.0)]);
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
            sticker_btn,
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

    let chat_pane: Element<'_, Msg> = container(chat_content)
        .width(Length::Fill)
        .height(Length::Fill)
        .into();

    let base: Element<'_, Msg> = if let Some(panel) = view_voice_panel(state) {
        column![panel, chat_pane].spacing(state.z(6)).into()
    } else {
        chat_pane
    };

    if state.sticker_menu_open {
        row![base, sticker_panel(state)]
            .width(Length::Fill)
            .height(Length::Fill)
            .into()
    } else {
        base
    }
}

/// Top dock shown while connected to a voice channel: current members, control
/// buttons (mute / camera / screen / leave) and live remote video tiles.
fn view_voice_panel(state: &AppState) -> Option<Element<'_, Msg>> {
    let voice = state.voice.as_ref()?;
    let channel_id = voice.in_channel()?;
    let channel_name = state
        .guilds
        .iter()
        .flat_map(|g| g.channels.iter())
        .find(|ch| ch.id == channel_id)
        .map(|ch| ch.name.clone())
        .unwrap_or_else(|| "Voice".to_string());
    let members = voice.members();
    let self_id = state.user.as_ref().map(|u| u.id);
    let muted = voice.is_muted();
    let camera_on = voice.camera_on();
    let screen_on = voice.screen_on();

    let member_chips: Vec<Element<'_, Msg>> = members
        .iter()
        .map(|(uid, name)| {
            let is_self = Some(*uid) == self_id;
            let label = if is_self { format!("{name} (you)") } else { name.clone() };
            row![
                text("🎤").size(state.zs(12)),
                text(label).size(state.zs(12)),
            ]
            .spacing(state.z(4))
            .align_y(iced::Alignment::Center)
            .into()
        })
        .collect();

    let mute_btn = button(
        container(text(if muted { "🔇" } else { "🎤" }).size(state.zs(16)))
            .padding(state.z(6)),
    )
    .on_press(Msg::VoiceToggleMute)
    .style(if muted { button::danger } else { button::secondary })
    .padding(state.z(0));
    let cam_btn = button(
        container(text(if camera_on { "📷" } else { "🚫" }).size(state.zs(16)))
            .padding(state.z(6)),
    )
    .on_press(Msg::VoiceToggleCamera)
    .style(if camera_on { button::primary } else { button::secondary })
    .padding(state.z(0));
    let screen_btn = button(
        container(text(if screen_on { "🖥️" } else { "🚫" }).size(state.zs(16)))
            .padding(state.z(6)),
    )
    .on_press(Msg::VoiceToggleScreen)
    .style(if screen_on { button::primary } else { button::secondary })
    .padding(state.z(0));
    let leave_btn = button(text("📵 Leave").size(state.zs(13)))
        .on_press(Msg::VoiceLeave)
        .style(button::danger)
        .padding([state.z(6.0), state.z(12.0)]);

    let tiles: Vec<Element<'_, Msg>> = state
        .voice_frames
        .iter()
        .map(|((uid, kind), (handle, _w, _h))| {
            let name = members
                .iter()
                .find(|(u, _)| u == uid)
                .map(|(_, n)| n.clone())
                .unwrap_or_else(|| format!("User {uid}"));
            let tag = match kind {
                VoiceVideoKind::Camera => "📷",
                VoiceVideoKind::Screen => "🖥️",
            };
            column![
                iced::widget::Image::new(handle.clone())
                    .width(Length::Fixed(state.z(160.0)))
                    .height(Length::Fixed(state.z(90.0))),
                row![text(tag).size(state.zs(11)), text(name).size(state.zs(11))]
                    .spacing(state.z(4))
                    .align_y(iced::Alignment::Center),
            ]
            .spacing(state.z(4))
            .into()
        })
        .collect();

    let controls = row![mute_btn, cam_btn, screen_btn, space::horizontal(), leave_btn]
        .spacing(state.z(8))
        .align_y(iced::Alignment::Center);

    let mut body = column![
        row![
            text("🔊 Voice: ").size(state.zs(13)),
            text(format!("#{channel_name}")).size(state.zs(13)).color(state.accent),
            space::horizontal(),
            controls,
        ].spacing(state.z(6)).align_y(iced::Alignment::Center),
    ].spacing(state.z(6));
    if !member_chips.is_empty() {
        body = body.push(row(member_chips).spacing(state.z(12)));
    }
    if !tiles.is_empty() {
        body = body.push(row(tiles).spacing(state.z(10)));
    }

    Some(
        container(body)
            .padding(state.z(10))
            .width(Length::Fill)
            .style(|theme: &iced::Theme| iced::widget::container::Style {
                background: Some(theme.extended_palette().background.weak.color.into()),
                border: iced::Border {
                    width: 1.0,
                    color: theme.extended_palette().background.weak.color,
                    radius: 10.0.into(),
                },
                ..iced::widget::container::Style::default()
            })
            .into(),
    )
}

/// Right-hand sticker picker. Slides in when toggled: searchable by pack name
/// and sticker name, plus a pack-creation UI for the user's own packs.
fn sticker_panel(state: &AppState) -> Element<'_, Msg> {
    let self_id = state.user.as_ref().map(|u| u.id);

    let header = row![
        text("Stickers").size(state.zs(16)),
        space::horizontal(),
        button(text("✕").size(state.zs(14))).on_press(Msg::ToggleStickerMenu).padding(state.z(4)),
    ].spacing(state.z(6)).align_y(iced::Alignment::Center);

    let search_input = text_input("Search packs or stickers…", &state.sticker_search)
        .on_input(Msg::StickerSearchChanged)
        .width(Length::Fill);

    let new_pack_btn: Element<'_, Msg> = if state.sticker_pack_create_open {
        column![
            text_input("Pack name", &state.sticker_pack_name_input)
                .on_input(Msg::StickerPackNameInput)
                .on_submit(Msg::CreateStickerPackSubmit)
                .width(Length::Fill),
            button(text("Create pack").size(state.zs(12))).on_press(Msg::CreateStickerPackSubmit),
        ].spacing(state.z(4)).into()
    } else {
        button(text("＋ New pack").size(state.zs(12)))
            .on_press(Msg::ToggleStickerPackCreate)
            .into()
    };

    let mut body: Vec<Element<'_, Msg>> = Vec::new();
    if state.sticker_busy {
        body.push(row![throbber(state.z(14)), text("Loading…").size(state.zs(11)).color(iced::Color::from_rgb(0.5, 0.5, 0.5))]
            .spacing(state.z(6)).align_y(iced::Alignment::Center).into());
    } else if state.sticker_packs.is_empty() {
        body.push(text("No sticker packs found").size(state.zs(12)).color(iced::Color::from_rgb(0.5, 0.5, 0.5)).into());
    }

    for pack in &state.sticker_packs {
        let is_owner = self_id == Some(pack.owner_id);
        let mut pack_header = row![
            text(&pack.name).size(state.zs(13)),
            text(format!("by {}", pack.owner_name)).size(state.zs(10)).color(iced::Color::from_rgb(0.5, 0.5, 0.5)),
            space::horizontal(),
        ].spacing(state.z(6)).align_y(iced::Alignment::Center);
        if is_owner {
            pack_header = pack_header.push(
                button(text("＋").size(state.zs(13)))
                    .on_press(Msg::PickStickerImages(pack.id))
                    .padding(state.z(2))
            );
            pack_header = pack_header.push(
                button(text("🗑").size(state.zs(13)))
                    .on_press(Msg::DeleteStickerPack(pack.id))
                    .padding(state.z(2))
            );
        }

        let grid: Vec<Element<'_, Msg>> = pack.stickers.iter().map(|s| {
            let img_el: Element<'_, Msg> = match state.sticker_handles.get(&s.id) {
                Some(h) => iced::widget::Image::new(h.clone()).width(state.z(64)).height(state.z(64)).into(),
                None => container(text("…").size(state.zs(12))).width(state.z(64)).height(state.z(64)).center_x(Length::Fixed(state.z(64))).center_y(Length::Fixed(state.z(64))).into(),
            };
            button(img_el)
                .on_press(Msg::SendSticker(s.id))
                .padding(state.z(2))
                .into()
        }).collect();

        let pack_body = column![pack_header, row(grid).spacing(state.z(4))].spacing(state.z(4));
        body.push(
            container(pack_body)
                .padding(state.z(8))
                .width(Length::Fill)
                .style(composer_style)
                .into(),
        );
    }

    container(
        column![
            header,
            search_input,
            new_pack_btn,
            rule::horizontal(1),
            scrollable(column(body).spacing(state.z(8))).width(Length::Fill).height(Length::Fill),
        ]
        .spacing(state.z(8))
        .padding(state.z(12)),
    )
    .width(Length::Fixed(state.z(300.0)))
    .height(Length::Fill)
    .style(|_: &iced::Theme| {
        let accent_faint_bg = accent_faint(state.accent);
        let accent_border = accent_dark(state.accent);
        iced::widget::container::Style {
            background: Some(accent_faint_bg.into()),
            border: iced::Border {
                width: 1.0,
                color: accent_border,
                radius: 0.0.into(),
                ..iced::Border::default()
            },
            ..iced::widget::container::Style::default()
        }
    })
    .into()
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
