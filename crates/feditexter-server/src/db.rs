use sqlx::MySqlPool;

use crate::chat::ChatHub;
use crate::federation::Federation;

#[derive(Clone)]
pub struct AppState {
    pub pool: MySqlPool,
    pub hub: ChatHub,
    pub federation: Federation,
    /// When true, new accounts require email verification; otherwise accounts
    /// are auto-verified (dev mode).
    pub verify_emails: bool,
}
