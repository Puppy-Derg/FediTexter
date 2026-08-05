use sqlx::MySqlPool;

use crate::chat::ChatHub;

#[derive(Clone)]
pub struct AppState {
    pub pool: MySqlPool,
    pub hub: ChatHub,
}
