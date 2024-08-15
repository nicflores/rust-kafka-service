use std::{num::NonZeroU32, sync::Arc};

use governor::{Quota, RateLimiter};
use rdkafka::producer::FutureProducer;
use serde_json::Value;
use tokio::sync::mpsc::Receiver;

pub async fn process_messages(
    mut rx: Receiver<Value>,
    producer: Arc<FutureProducer>,
    api_url: String,
    output_topic: String,
) {
    // Create a rate limiter: 10 requests per second
    let rate_limiter = Arc::new(RateLimiter::direct(Quota::per_second(
        NonZeroU32::new(10).unwrap(),
    )));

    // Create an HTTP client
    let client = reqwest::Client::new();

    while let Some(message) = rx.recv().await {
        // Wait for rate limit
        rate_limiter.until_ready().await;

        let producer_clone = producer.clone();
        let client_clone = client.clone();
        let api_url_clone = api_url.clone();
        let output_topic_clone = output_topic.clone();

        tokio::spawn(async move {
            match process_single_message(message, &client_clone, &api_url_clone).await {
                Ok(response_json) => {
                    // Publish the response to Kafka
                    match producer_clone
                        .send(
                            rdkafka::producer::FutureRecord::to(&output_topic_clone)
                                .payload(&serde_json::to_string(&response_json).unwrap())
                                .key("processed_message"),
                            tokio::time::Duration::from_secs(0),
                        )
                        .await
                    {
                        Ok(_) => println!("Message published to Kafka successfully"),
                        Err((e, _)) => eprintln!("Error publishing to Kafka: {:?}", e),
                    }
                }
                Err(e) => eprintln!("Error processing message: {:?}", e),
            }
        });
    }
}

async fn process_single_message(
    message: Value,
    client: &reqwest::Client,
    api_url: &str,
) -> anyhow::Result<Value> {
    // Make the HTTP request
    let response = client.post(api_url).json(&message).send().await?;

    // Check if the request was successful
    if !response.status().is_success() {
        return Err(anyhow::anyhow!(
            "API request failed with status: {}",
            response.status()
        ));
        //return Err(format!("API request failed with status: {}", response.status()).into());
    }

    // Parse the response JSON
    let response_json: Value = response.json().await?;

    Ok(response_json)
}

pub async fn process_message(mut rx: Receiver<Value>) {
    while let Some(message) = rx.recv().await {
        println!("Processing message: {:?}", message);
    }
}
