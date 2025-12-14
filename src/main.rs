// src/main.rs
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::env;
use std::time::Duration;
use url::Url;

#[tokio::main]
async fn main() {
    // 1. Load Environment Variables
    dotenvy::dotenv().ok();
    let bootstrap_servers = env::var("KAFKA_SERVER").unwrap_or_else(|_| "localhost:9093".to_string());
    let topic_name = "urls-to-crawl";

    println!("--- Arachne Seeder ---");
    println!("Bootstrap Servers: {}", bootstrap_servers);
    println!("Target Topic:      {}", topic_name);

    // 2. Define Seed URLs
    // Add any starting points you want here
    let seed_urls = vec![
        "https://www.wikipedia.org",
        "https://www.google.com",
        "https://www.bing.com",
        "https://duckduckgo.com",
        "https://yandex.com",
        "https://baidu.com",
        "https://archive.org",
    ];

    // 3. Create the Producer
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation failed");

    println!("\nSeeding {} URLs...", seed_urls.len());

    // 4. Send URLs
    for url_str in seed_urls {
        // Extract domain to use as the partition key.
        // This ensures the seed URL goes to the same partition as future links discovered from it.
        let key = match Url::parse(url_str) {
            Ok(u) => u.domain().unwrap_or("unknown").to_string(),
            Err(_) => "unknown".to_string(),
        };

        let record = FutureRecord::to(topic_name)
            .payload(url_str)
            .key(&key); // <-- Important for partitioning

        // Send asynchronously
        match producer.send(record, Duration::from_secs(0)).await {
            Ok((partition, offset)) => {
                println!("✅ Sent: {:<50} (Part: {}, Off: {})", url_str, partition, offset);
            }
            Err((e, _msg)) => {
                eprintln!("❌ Failed to send {}: {}", url_str, e);
            }
        }
    }

    println!("\n--- Seeding Complete ---");
}