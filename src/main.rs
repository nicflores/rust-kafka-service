use axum::{
    routing::{get, post},
    Router,
};
use rust_kafka_test::{
    consumer,
    handlers::{log_message, root},
    models::AppState,
    processor, producer,
};
use serde_json::Value;
use std::sync::Arc;
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::JoinHandle,
};

#[tokio::main]
async fn main() {
    // Config
    let api_url = "https://api.example.com/endpoint".to_string(); // Replace with your API URL
    let output_topic = "processed-topic".to_string();
    let input_topics = vec!["test-topic"];

    // Setup
    let producer = Arc::new(producer::create("localhost:29092"));
    let consumer = consumer::create("localhost:29092", input_topics);
    let (tx, rx): (Sender<Value>, Receiver<Value>) = mpsc::channel(100);
    let app_state = AppState {
        producer: Arc::clone(&producer),
    };

    let api_task: JoinHandle<()> = tokio::spawn(async move {
        let app = Router::new()
            .route("/", get(root))
            .route("/log", post(log_message))
            .with_state(app_state);
        let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
        println!("Listening on {}", listener.local_addr().unwrap());
        axum::serve(listener, app).await.unwrap();
    });

    let consumer_task: JoinHandle<()> = tokio::spawn(async move {
        consumer::consume(consumer, tx).await;
    });

    let processor_task: JoinHandle<()> = tokio::spawn(async move {
        processor::process_messages(rx, producer.clone(), api_url, output_topic).await;
    });

    tokio::try_join!(api_task, consumer_task, processor_task).unwrap();
}
