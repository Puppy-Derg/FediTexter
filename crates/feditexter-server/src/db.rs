use sqlx::MySqlPool;

use crate::chat::ChatHub;
use crate::federation::Federation;

#[derive(Clone)]
pub struct AppState {
    pub pool: MySqlPool,
    pub hub: ChatHub,
    pub federation: Federation,
}
