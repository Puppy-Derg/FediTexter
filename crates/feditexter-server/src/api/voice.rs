//! Voice channel status endpoints.

use axum::extract::State;
use axum::Json;
use serde_json::{json, Value};

use crate::api::error::ApiError;
use crate::auth::AuthUser;
use crate::db::AppState;

/// Current voice channel occupancy for every guild the caller is a member of.
/// Lets the client show who is in each voice channel without joining it
/// (refreshed periodically; occupancy is otherwise only pushed in real time to
/// users who are themselves in the channel).
pub async fn voice_occupancy(
    State(state): State<AppState>,
    auth: AuthUser,
) -> Result<Json<Value>, ApiError> {
    let data: Vec<(u64, u64, Vec<(u64, String)>)> = {
        let voice = state.voice.lock().unwrap();
        voice
            .iter()
            .map(|((guild_id, channel_id), occupants)| {
                (
                    *guild_id,
                    *channel_id,
                    occupants.iter().map(|(uid, name)| (*uid, name.clone())).collect(),
                )
            })
            .collect()
    };

    let mut out = Vec::new();
    for (guild_id, channel_id, occupants) in data {
        let member: Option<(u64,)> = sqlx::query_as(
            "SELECT user_id FROM guild_members WHERE guild_id = ? AND user_id = ?",
        )
        .bind(guild_id)
        .bind(auth.user.id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|_| ApiError::Internal("db error"))?;
        if member.is_some() {
            out.push(json!({
                "channel_id": channel_id,
                "users": occupants.iter().map(|(id, name)| json!({ "id": id, "username": name })).collect::<Vec<_>>(),
            }));
        }
    }

    Ok(Json(json!({ "channels": out })))
}
