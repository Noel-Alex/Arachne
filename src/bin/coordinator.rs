// src/bin/coordinator.rs
use rdkafka::consumer::{Consumer, StreamConsumer, CommitMode};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::message::Message;
use rdkafka::ClientConfig;
use tokio::time::{self, Duration, Instant};
use arachne::{CrawlResult, db};
use std::env;
use std::collections::HashSet;
use url::Url;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    // 1. Setup DB
    println!("Coordinator connecting to DB...");
    let session = db::connect_to_db().await.expect("DB Connection failed");

    // Prepare statements ONCE to save overhead
    let insert_stmt = session.prepare("INSERT INTO Arachne.crawled_pages (source_url, content, http_status_code) VALUES (?, ?, ?)").await.unwrap();
    let check_stmt = session.prepare("SELECT source_url FROM Arachne.crawled_pages WHERE source_url = ?").await.unwrap();

    // 2. Setup Kafka
    let bootstrap_servers = env::var("KAFKA_SERVER").expect("KAFKA_SERVER missing");
    let consume_topic = "crawl-results";
    let produce_topic = "urls-to-crawl";
    let group_id = "arachne-coordinator-group";

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false") // IMPORTANT: Manual commits
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Consumer creation failed");

    consumer.subscribe(&[consume_topic]).unwrap();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("queue.buffering.max.messages", "100000")
        .set("acks", "1")
        .create()
        .expect("Producer creation failed");

    // --- BATCH SETTINGS ---
    const BATCH_SIZE: usize = 100; // Adjust based on memory
    const BATCH_TIMEOUT: Duration = Duration::from_millis(500); // Flush every 0.5s if buffer isn't full

    let mut result_buffer: Vec<CrawlResult> = Vec::with_capacity(BATCH_SIZE);
    let mut last_flush = Instant::now();

    println!("Coordinator running. Waiting for results...");

    loop {
        // Use tokio::select! to listen for EITHER a message OR a timeout
        tokio::select! {
            // Case A: Receive a message from Kafka
            msg_res = consumer.recv() => {
                match msg_res {
                    Ok(m) => {
                        // Attempt to deserialize
                        if let Some(Ok(payload)) = m.payload_view::<str>() {
                            println!("Coordinator received payload: {:.50}...", payload);
                            match serde_json::from_str::<CrawlResult>(payload) {
                                Ok(result) => {
                                    result_buffer.push(result);
                                }
                                Err(e) => eprintln!("Deserialization error: {}", e),
                            }
                        }
                    },
                    Err(e) => eprintln!("Kafka Error: {}", e),
                }
            }

            // Case B: Timeout reached (Flush time)
            _ = time::sleep_until(last_flush + BATCH_TIMEOUT) => {
                // The block below handles the actual flushing
            }
        }

        // --- FLUSH LOGIC ---
        // We flush if buffer is full OR if time has passed and buffer is not empty
        if result_buffer.len() >= BATCH_SIZE || (last_flush.elapsed() >= BATCH_TIMEOUT && !result_buffer.is_empty()) {
            let count = result_buffer.len();
            println!("Flushing batch of {} items...", count);

            // 1. Insert Batch into ScyllaDB (Concurrent Writes)
            if let Err(e) = db::add_crawled_pages_concurrently(&session, &result_buffer, &insert_stmt).await {
                eprintln!("CRITICAL: DB Batch Insert Failed: {}", e);
                // In a real app, you might break/retry here.
                // For now, we clear the buffer to prevent infinite loops, but we DO NOT commit offsets.
                result_buffer.clear();
                continue;
            }

            // 2. Aggregate all discovered URLs from this batch
            let mut all_discovered: HashSet<String> = HashSet::new();
            for res in &result_buffer {
                for url in &res.discovered_urls {
                    all_discovered.insert(url.clone());
                }
            }

            // 3. Filter Duplicates via DB (Concurrent Reads)
            if !all_discovered.is_empty() {
                let urls_to_check: Vec<String> = all_discovered.into_iter().collect();
                let existing = db::check_existing_urls(&session, urls_to_check.clone(), &check_stmt)
                    .await
                    .unwrap_or_default();

                // 4. Produce NEW URLs to Kafka
                //println!("{:?}", urls_to_check);
                for url in urls_to_check {
                    if !existing.contains(&url) {
                        // Extract Domain for Partition Key
                        let key = Url::parse(&url)
                            .ok()
                            .and_then(|u| u.domain().map(|d| d.to_string()))
                            .unwrap_or_else(|| "unknown".to_string());

                        // Fire and forget (async send)
                        //println!("{}", url);
                        let _ = producer.send(
                            FutureRecord::to(produce_topic).key(&key).payload(&url),
                            Duration::from_secs(0)
                        ).await;
                    }
                }
            }

            // 5. Commit Offsets
            // We commit "Async" so we don't block processing waiting for Kafka to confirm.
            if let Err(e) = consumer.commit_consumer_state(CommitMode::Async) {
                 eprintln!("Offset commit failed: {}", e);
            } else {
                 println!("Batch of {} committed successfully.", count);
            }

            // Reset
            result_buffer.clear();
            last_flush = Instant::now();
        }
    }
}