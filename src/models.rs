use std::sync::Arc;

use rdkafka::producer::FutureProducer;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct AppState {
    pub producer: Arc<FutureProducer>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LogMessage {
    pub level: String,
    pub message: String,
}
