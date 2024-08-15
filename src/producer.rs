use std::time::Duration;

use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
    ClientConfig,
};
use serde::Serialize;

pub fn create(bootstrap_server: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap_server)
        .set("queue.buffering.max.ms", "0")
        .create()
        .expect("Failed to create client")
}

pub async fn produce<T: Serialize>(
    producer: &FutureProducer,
    topic: &str,
    message: &T,
) -> Result<(), String> {
    let payload = serde_json::to_string(message).map_err(|e| e.to_string())?;
    let record = FutureRecord::to(topic)
        .payload(&payload)
        .key("test-key")
        .timestamp(chrono::Utc::now().timestamp_millis());

    let res = producer
        .send(record, Timeout::After(Duration::from_secs(1)))
        .await;

    match res {
        Ok(delivery_status) => {
            println!("Message delivered with status: {:?}", delivery_status);
            Ok(())
        }
        Err((e, _)) => {
            let error_msg = format!("Failed to deliver message: {:?}", e);
            println!("{}", error_msg);
            Err(error_msg)
        }
    }
}
