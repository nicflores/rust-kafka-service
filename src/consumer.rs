use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    error::KafkaError,
    types::RDKafkaErrorCode,
    ClientConfig, Message,
};
use serde_json::Value;
use tokio::time::Duration;
use tokio::{sync::mpsc::Sender, time::sleep};

pub fn create(bootstrap_server: &str, topics: Vec<&str>) -> StreamConsumer {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_server)
        .set("auto.offset.reset", "earliest")
        .set("group.id", "test-group")
        .set("socket.timeout.ms", "4000")
        .create()
        .expect("Failed to create consumer.");

    // Subscribe the consumer to the provided topics.
    consumer
        .subscribe(&topics)
        .expect("Couldn't subscirbe to topics.");

    consumer
}

pub async fn consume(consumer: StreamConsumer, tx: Sender<Value>) {
    let mut backoff_duration = Duration::from_millis(100);
    let max_backoff = Duration::from_secs(30);

    loop {
        match consumer.recv().await {
            Err(e) => match e {
                KafkaError::MessageConsumption(RDKafkaErrorCode::UnknownTopicOrPartition) => {
                    eprintln!(
                        "Topic does not exist yet. Retrying in {:?}",
                        backoff_duration
                    );
                    sleep(backoff_duration).await;
                    backoff_duration = std::cmp::min(backoff_duration * 2, max_backoff);
                }
                _ => {
                    eprintln!("Error while consuming from kafka: {:?}", e);
                    sleep(Duration::from_secs(1)).await;
                }
            },
            Ok(message) => {
                backoff_duration = Duration::from_millis(100);

                let payload = message
                    .payload()
                    .expect("Empty kafka message payload")
                    .to_vec();

                let data: Value =
                    match tokio::task::spawn_blocking(move || serde_json::from_slice(&payload))
                        .await
                    {
                        Ok(Ok(data)) => data,
                        Ok(Err(e)) => {
                            eprintln!("Error while deserializing message: {}", e);
                            continue;
                        }
                        Err(e) => {
                            eprintln!("Error in spawn_blocking: {}", e);
                            continue;
                        }
                    };

                //if let Ok(data) = serde_json::from_slice(payload) {
                if tx.send(data).await.is_err() {
                    eprintln!("Error while sending data to the channel");
                    break;
                }
                //}
                // match message.payload_view::<str>() {
                //     None => eprintln!("Error while deserializing message"),
                //     Some(Ok(msg)) => {
                //         println!("Consumed message: {}", msg);
                //     }
                //     Some(Err(e)) => eprintln!("Error while deserializing message: {}", e),
                // }
                // consumer
                //     .commit_message(&message, CommitMode::Async)
                //     .unwrap();
            }
        }
    }
}

// pub async fn start(bootstrap_server: &str, topics: Vec<&str>) {
//     let consumer = create(bootstrap_server);
//     consume(consumer, topics).await;
// }
