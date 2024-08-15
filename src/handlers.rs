use axum::{extract::State, Json};

use crate::models::{AppState, LogMessage};
use crate::producer;

pub async fn root() -> &'static str {
    "Hello, World!"
}

pub async fn log_message(
    State(state): State<AppState>,
    Json(payload): Json<LogMessage>,
) -> Result<&'static str, String> {
    producer::produce(&state.producer, "test-topic", &payload).await?;
    Ok("Message logged successfully")
}
