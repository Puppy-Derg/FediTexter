pub mod api;
pub mod auth;
pub mod bot;
pub mod chat;
pub mod db;
pub mod federation;
pub mod mail;
pub mod tui;

use axum::routing::{delete, get, post};
use axum::Router;
use api::auth_handlers::{
    list_sessions, login, login_2fa, logout, me, register, resend_verification, revoke_session,
    set_avatar, two_fa_enable, two_fa_setup, update_me, verify,
};
use api::chat_handlers::{
    create_conversation, delete_conversation, delete_message, edit_message, list_conversations,
    list_messages, mark_read, presence, search_users, send_message,
};
use api::federation_handlers::{inbox, user_lookup, well_known};
use api::guilds::{
    assign_role, ban_member, create_channel, create_guild, create_invite, create_role,
    delete_channel, delete_guild, delete_role, guild_detail, join_guild, kick_member, leave_guild,
    list_guilds, rename_channel, revoke_invite, set_role, transfer_owner, unban_member,
};
use api::moderation::{block_user, mute_user, unblock_user, unmute_user, user_profile};
use api::preview::link_preview;
use api::stickers::{
    add_sticker, create_sticker_pack, delete_sticker, delete_sticker_pack, list_sticker_packs,
    sticker_image,
};
use api::ws::ws_handler;
use api::voice::voice_occupancy;
use db::AppState;

pub fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(crate::healthz))
        .route("/.well-known/feditexter", get(well_known))
        .route("/api/register", post(register))
        .route("/api/login", post(login))
        .route("/api/login/2fa", post(login_2fa))
        .route("/api/logout", post(logout))
        .route("/api/me", get(me).patch(update_me))
        .route("/api/me/avatar", post(set_avatar))
        .route("/api/me/sessions", get(list_sessions))
        .route("/api/me/sessions/{session_id}", delete(revoke_session))
        .route("/api/me/2fa/setup", post(two_fa_setup))
        .route("/api/me/2fa/enable", post(two_fa_enable))
        .route("/api/verify", post(verify))
        .route("/api/verify/resend", post(resend_verification))
        .route("/api/link-preview", post(link_preview))
        .route("/api/users/{id}", get(user_profile))
        .route("/api/users/{id}/block", post(block_user))
        .route("/api/users/{id}/unblock", post(unblock_user))
        .route("/api/users/{id}/mute", post(mute_user))
        .route("/api/users/{id}/unmute", post(unmute_user))
        .route("/api/servers", get(list_guilds).post(create_guild))
        .route("/api/servers/join", post(join_guild))
        .route("/api/servers/{id}", get(guild_detail).delete(delete_guild))
        .route("/api/servers/{id}/channels", post(create_channel))
        .route("/api/servers/{id}/channels/{channel_id}", axum::routing::patch(rename_channel).delete(delete_channel))
        .route("/api/servers/{id}/invite", post(create_invite))
        .route("/api/servers/{id}/invite", delete(revoke_invite))
        .route("/api/servers/{id}/leave", post(leave_guild))
        .route("/api/servers/{id}/transfer", post(transfer_owner))
        .route("/api/servers/{id}/role", post(set_role))
        .route("/api/servers/{id}/roles", post(create_role))
        .route("/api/servers/{id}/roles/{role_id}", delete(delete_role))
        .route("/api/servers/{id}/roles/{role_id}/assign", post(assign_role))
        .route("/api/servers/{id}/kick", post(kick_member))
        .route("/api/servers/{id}/bans", post(ban_member))
        .route("/api/servers/{id}/bans/{user_id}", delete(unban_member))
        .route("/api/stickers", get(list_sticker_packs))
        .route("/api/stickers/packs", post(create_sticker_pack))
        .route("/api/stickers/packs/{pack_id}", delete(delete_sticker_pack))
        .route("/api/stickers/packs/{pack_id}/stickers", post(add_sticker))
        .route("/api/stickers/packs/{pack_id}/stickers/{sticker_id}", delete(delete_sticker))
        .route("/api/stickers/{sticker_id}/image", get(sticker_image))
        .route("/api/conversations", post(create_conversation))
        .route("/api/conversations", get(list_conversations))
        .route("/api/users/search", get(search_users))
        .route("/api/conversations/{id}", delete(delete_conversation))
        .route("/api/conversations/{id}/messages", get(list_messages))
        .route("/api/conversations/{id}/read", post(mark_read))
        .route("/api/conversations/{id}/messages", post(send_message))
        .route("/api/conversations/{id}/messages/{msg_id}", axum::routing::patch(edit_message))
        .route("/api/conversations/{id}/messages/{msg_id}", delete(delete_message))
        .route("/api/presence", get(presence))
        .route("/api/federation/users/lookup", get(user_lookup))
        .route("/api/federation/inbox", post(inbox))
        .route("/api/ws", get(ws_handler))
        .route("/api/voice/occupancy", get(voice_occupancy))
        .with_state(state)
}

async fn healthz() -> &'static str {
    concat!("ok v", env!("CARGO_PKG_VERSION"))
}
