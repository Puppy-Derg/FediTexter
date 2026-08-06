use sqlx::MySqlPool;

use crate::chat::ChatHub;
use crate::federation::Federation;
use crate::mail::Mailer;

#[derive(Clone)]
pub struct AppState {
    pub pool: MySqlPool,
    pub hub: ChatHub,
    pub federation: Federation,
    /// When true, new accounts require email verification; otherwise accounts
    /// are auto-verified (dev mode).
    pub verify_emails: bool,
    /// SMTP mailer, present when SMTP is configured.
    pub mailer: Option<Mailer>,
    /// Shared HTTP client for outbound fetches (link previews, federation).
    pub http: reqwest::Client,
}
