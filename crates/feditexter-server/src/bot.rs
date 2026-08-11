//! In-app bot: an account the server itself operates.
//!
//! It sends users reminders to verify their email / enable 2FA, and announces
//! new releases (patch notes) fetched from GitHub. Runs on a background loop
//! spawned from `main.rs`.

use std::sync::Arc;

use crate::api::error::ApiError;
use crate::chat::Message;
use crate::db::AppState;

const BOT_USERNAME: &str = "feditexter-bot";
const BOT_DISPLAY_NAME: &str = "FediTexter Bot";
const RELEASE_REPO: &str = "Puppy-Derg/FediTexter";
const DUMMY_PASSWORD_HASH: &str = "$argon2id$v=19$m=19456,t=2,p=1$fAWlNrU+t0yc2hZQVobo/Q$YcqXjlp/Iy9rWpLwLPTwDBUyV1umH3j88kucq4n1zuU";

/// Idempotently create (or find) the bot user and return its id.
pub async fn ensure_bot_user(state: &AppState) -> Result<u64, sqlx::Error> {
    if let Some((id,)) = sqlx::query_as("SELECT id FROM users WHERE username = ? AND is_bot = 1")
        .bind(BOT_USERNAME)
        .fetch_optional(&state.pool)
        .await?
    {
        return Ok(id);
    }

    let email = format!("bot@{bot}", bot = state.federation.domain);
    let inserted = sqlx::query(
        "INSERT INTO users (email, username, display_name, password_hash, email_verified, is_bot, totp_enabled)
         VALUES (?, ?, ?, ?, 1, 1, 1)",
    )
    .bind(&email)
    .bind(BOT_USERNAME)
    .bind(BOT_DISPLAY_NAME)
    .bind(DUMMY_PASSWORD_HASH)
    .execute(&state.pool)
    .await;

    match inserted {
        Ok(r) => Ok(r.last_insert_id()),
        Err(sqlx::Error::Database(e)) if e.is_unique_violation() => {
            // Another process created it between our check and insert.
            let (id,) = sqlx::query_as("SELECT id FROM users WHERE username = ? AND is_bot = 1")
                .bind(BOT_USERNAME)
                .fetch_one(&state.pool)
                .await?;
            Ok(id)
        }
        Err(e) => Err(e),
    }
}

async fn insert_bot_message(
    state: &AppState,
    bot_id: u64,
    user_id: u64,
    body: String,
) -> Result<u64, ApiError> {
    let conv_id = crate::api::chat_handlers::ensure_direct_conversation(state, bot_id, user_id).await?;
    let created_at = chrono::Utc::now().naive_utc();
    let inserted = sqlx::query(
        "INSERT INTO messages (conversation_id, sender_id, body, created_at) VALUES (?, ?, ?, ?)",
    )
    .bind(conv_id)
    .bind(bot_id)
    .bind(&body)
    .bind(created_at)
    .execute(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;
    Ok(inserted.last_insert_id())
}

async fn message_by_id(state: &AppState, id: u64) -> Result<Message, ApiError> {
    sqlx::query_as(
        "SELECT id, conversation_id, sender_id, body, created_at, attachment_mime, attachment_name, attachment_data,
                file_id, file_size, thumbnail_data, edited_at, original_body, deleted_at, remote_message_id
         FROM messages WHERE id = ?",
    )
    .bind(id)
    .fetch_one(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))
}

/// Send a one-way message from the bot to a user, pushing it live via the hub.
pub async fn send_bot_message(
    state: &AppState,
    bot_id: u64,
    user_id: u64,
    body: String,
) -> Result<(), ApiError> {
    let msg_id = insert_bot_message(state, bot_id, user_id, body).await?;
    let message = message_by_id(state, msg_id).await?;
    state.hub.publish_message(message);
    Ok(())
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(max).collect();
        out.push('…');
        out
    }
}

/// Message users who haven't verified their email or enabled 2FA, at most once
/// per `cooldown_days`. Returns how many reminder messages were sent.
pub async fn run_verification_reminders(
    state: &AppState,
    bot_id: u64,
    cooldown_days: i64,
) -> Result<usize, ApiError> {
    let cutoff = chrono::Utc::now().naive_utc() - chrono::Duration::days(cooldown_days);

    let unverified: Vec<(u64, String)> = sqlx::query_as(
        "SELECT id, email FROM users
         WHERE is_bot = 0 AND email_verified = 0
           AND (last_reminder_at IS NULL OR last_reminder_at < ?)",
    )
    .bind(cutoff)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;

    let no_2fa: Vec<(u64,)> = sqlx::query_as(
        "SELECT id FROM users
         WHERE is_bot = 0 AND totp_enabled = 0
           AND (last_reminder_at IS NULL OR last_reminder_at < ?)",
    )
    .bind(cutoff)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;

    let mut needs: std::collections::HashMap<u64, Vec<&'static str>> = std::collections::HashMap::new();
    let mut emails: std::collections::HashMap<u64, String> = std::collections::HashMap::new();
    for (id, email) in &unverified {
        needs.entry(*id).or_default().push("email verification");
        emails.insert(*id, email.clone());
    }
    for (id,) in &no_2fa {
        needs.entry(*id).or_default().push("two-factor authentication");
    }
    if needs.is_empty() {
        return Ok(0);
    }

    let mut sent = 0;
    for (user_id, items) in needs {
        let mut text = String::from("Hi! I'm the FediTexter bot. A couple of things need your attention:\n\n");
        for item in &items {
            text.push_str(&format!("• You haven't set up {item} yet.\n"));
        }
        if let Some(email) = emails.get(&user_id) {
            text.push_str(&format!(
                "\nYour email on this account is {email}. Verify it in Settings (or via the link you were emailed).\n"
            ));
        }
        text.push_str("\nKeeping these on keeps your account secure. Thanks! — FediTexter Bot");
        match send_bot_message(state, bot_id, user_id, text).await {
            Ok(()) => {
                sqlx::query("UPDATE users SET last_reminder_at = ? WHERE id = ?")
                    .bind(chrono::Utc::now().naive_utc())
                    .bind(user_id)
                    .execute(&state.pool)
                    .await
                    .map_err(|_| ApiError::Internal("db error"))?;
                sent += 1;
            }
            Err(e) => tracing::warn!("bot reminder to {user_id} failed: {e:?}"),
        }
    }
    Ok(sent)
}

/// Announce the latest GitHub release to users who haven't seen that tag yet.
pub async fn run_patch_notes(state: &AppState, bot_id: u64) -> Result<usize, ApiError> {
    let url = format!("https://api.github.com/repos/{RELEASE_REPO}/releases/latest");
    let resp = state
        .http
        .get(&url)
        .header("User-Agent", "FediTexterServer/1.0")
        .header("Accept", "application/vnd.github+json")
        .send()
        .await;

    let Ok(resp) = resp else { return Ok(0) };
    if !resp.status().is_success() {
        return Ok(0);
    }
    let Ok(v) = resp.json::<serde_json::Value>().await else { return Ok(0) };
    let Some(tag) = v.get("tag_name").and_then(|t| t.as_str()) else {
        return Ok(0);
    };
    if tag.is_empty() {
        return Ok(0);
    }
    let body = v.get("body").and_then(|b| b.as_str()).unwrap_or("");
    let mut text = format!("📣 FediTexter {tag} is out!\n\n");
    text.push_str(&truncate(body, 1400));

    let users: Vec<(u64,)> = sqlx::query_as(
        "SELECT id FROM users WHERE is_bot = 0 AND (last_patch_tag IS NULL OR last_patch_tag <> ?)",
    )
    .bind(tag)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| ApiError::Internal("db error"))?;

    let mut sent = 0;
    for (user_id,) in users {
        match send_bot_message(state, bot_id, user_id, text.clone()).await {
            Ok(()) => {
                sqlx::query("UPDATE users SET last_patch_tag = ? WHERE id = ?")
                    .bind(tag)
                    .bind(user_id)
                    .execute(&state.pool)
                    .await
                    .map_err(|_| ApiError::Internal("db error"))?;
                sent += 1;
            }
            Err(e) => tracing::warn!("bot patch note to {user_id} failed: {e:?}"),
        }
    }
    Ok(sent)
}

/// Background loop: run the checks periodically until the process exits.
pub async fn bot_loop(state: Arc<AppState>) {
    if std::env::var("BOT_ENABLED").map(|v| v == "0").unwrap_or(false) {
        tracing::info!("bot disabled (BOT_ENABLED=0)");
        return;
    }
    let bot_id = match ensure_bot_user(&state).await {
        Ok(id) => id,
        Err(e) => {
            tracing::error!("could not ensure bot user: {e:?}");
            return;
        }
    };
    tracing::info!("bot user ready (id={bot_id})");

    let hours = std::env::var("BOT_REMINDER_INTERVAL_HOURS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(6)
        .max(1);
    let cooldown_days = std::env::var("BOT_REMINDER_COOLDOWN_DAYS")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(3)
        .max(1);
    let announce_patch_notes = std::env::var("BOT_PATCH_NOTES").map(|v| v != "0").unwrap_or(true);

    // First run shortly after boot, then every `hours`.
    tokio::time::sleep(tokio::time::Duration::from_secs(20)).await;
    loop {
        match run_verification_reminders(&state, bot_id, cooldown_days).await {
            Ok(n) => tracing::info!("bot sent {n} verification/2FA reminders"),
            Err(e) => tracing::warn!("bot reminder run failed: {e:?}"),
        }
        if announce_patch_notes {
            match run_patch_notes(&state, bot_id).await {
                Ok(n) => tracing::info!("bot announced patch notes to {n} users"),
                Err(e) => tracing::warn!("bot patch-note run failed: {e:?}"),
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(hours * 3600)).await;
    }
}
