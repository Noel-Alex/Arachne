/*

use chrono::Local;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task;

// --- Configuration Constants ---
const BROKERS: &str = "localhost:9093";
const TOPIC: &str = "urls-to-crawl"; // Using the 16-partition topic for max parallelism
const PRODUCER_THREADS: usize = 4;   // Number of concurrent producer tasks
const CONSUMER_THREADS: usize = 8;   // Number of concurrent consumer tasks (up to 16 for this topic)
const MESSAGE_SIZE: usize = 1024;    // 1KB payload (simulates a decent URL + metadata)
const TEST_DURATION_SECS: u64 = 60;  // Run the test for 1 minute

// --- Global Counters for Real-time Reporting ---
static TOTAL_SENT: AtomicU64 = AtomicU64::new(0);
static TOTAL_RECV: AtomicU64 = AtomicU64::new(0);
static BYTES_SENT: AtomicU64 = AtomicU64::new(0);

#[tokio::main]
async fn main() {
    println!("=== ARACHNE KAFKA CLUSTER STRESS TEST ===");
    println!("Target: {}", BROKERS);
    println!("Topic: {}", TOPIC);
    println!("Payload Size: {} bytes", MESSAGE_SIZE);
    println!("Producers: {}, Consumers: {}", PRODUCER_THREADS, CONSUMER_THREADS);
    println!("Duration: {} seconds", TEST_DURATION_SECS);
    println!("=========================================\n");

    // 1. Start the Reporting Task (prints stats every second)
    let reporter_handle = tokio::spawn(async move {
        let mut last_sent = 0;
        let mut last_recv = 0;
        let mut last_bytes = 0;
        let start_time = Instant::now();

        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let elapsed = start_time.elapsed().as_secs();
            if elapsed > TEST_DURATION_SECS { break; }

            let curr_sent = TOTAL_SENT.load(Ordering::Relaxed);
            let curr_recv = TOTAL_RECV.load(Ordering::Relaxed);
            let curr_bytes = BYTES_SENT.load(Ordering::Relaxed);

            let sent_rate = curr_sent - last_sent;
            let recv_rate = curr_recv - last_recv;
            let mb_rate = (curr_bytes - last_bytes) as f64 / 1024.0 / 1024.0;

            println!(
                "[{}] T+{:<3}s | Sent: {:>7} msg/s | Recv: {:>7} msg/s | Bw: {:>6.2} MB/s | Lag: {:>5} msgs",
                Local::now().format("%H:%M:%S"),
                elapsed,
                sent_rate,
                recv_rate,
                mb_rate,
                curr_sent.saturating_sub(curr_recv)
            );

            last_sent = curr_sent;
            last_recv = curr_recv;
            last_bytes = curr_bytes;
        }
    });

    // 2. Start Consumer Tasks
    // We use a unique group ID for each run to ensure we don't read old data
    let group_id = format!("stress-test-group-{}", Local::now().timestamp());
    let mut consumer_handles = vec![];
    for i in 0..CONSUMER_THREADS {
        let g_id = group_id.clone();
        consumer_handles.push(tokio::spawn(async move {
            run_consumer(i, &g_id).await;
        }));
    }
    // Give consumers a moment to register and rebalance
    println!("> Waiting 5s for consumers to stabilize...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // 3. Start Producer Tasks
    println!("> GO! Starting producers...");
    let mut producer_handles = vec![];
    for i in 0..PRODUCER_THREADS {
        producer_handles.push(tokio::spawn(async move {
            run_producer(i).await;
        }));
    }

    // 4. Wait for the test duration
    reporter_handle.await.unwrap();

    println!("\n> Test finished. Stopping tasks...");
    // In a real app we'd use cancellation tokens, but here we just let main exit,
    // which kills the background tokio tasks.
}

async fn run_producer(id: usize) {
    // High-performance producer config
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .set("message.timeout.ms", "5000")
        .set("queue.buffering.max.messages", "100000") // Large local buffer
        .set("linger.ms", "10")                        // Wait 10ms to batch messages
        .set("batch.size", "65536")                    // 64KB batches
        .set("compression.type", "lz4")                // Fast compression to save bandwidth
        .set("acks", "1")                              // Leader-only acks for max speed
        .create()
        .expect("Producer creation failed");

    // Pre-allocate a static payload to avoid reallocation in the hot loop
    let payload = vec![b'x'; MESSAGE_SIZE];
    // Use a fixed set of keys to ensure good partition distribution (assuming 16 partitions)
    let keys: Vec<String> = (0..16).map(|i| format!("domain-{}.com", i)).collect();

    loop {
        // Round-robin through keys to hit all partitions evenly
        for key in &keys {
            match producer.send_result(
                FutureRecord::to(TOPIC)
                    .payload(&payload)
                    .key(key),
            ) {
                Ok(_) => {
                    // NOTE: We are NOT `.await`ing the future here for maximum throughput.
                    // We just fire and forget into the local library buffer.
                    // rdkafka handles the actual network I/O in the background.
                    TOTAL_SENT.fetch_add(1, Ordering::Relaxed);
                    BYTES_SENT.fetch_add(MESSAGE_SIZE as u64, Ordering::Relaxed);
                }
                Err(e) => {
                    // Queue is full, backpressure. Wait a tiny bit.
                     tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        }
        // Small yield to let the async runtime breathe if needed
        tokio::task::yield_now().await;
    }
}

async fn run_consumer(id: usize, group_id: &str) {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .set("group.id", group_id)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")      // Auto-commit for simplicity in stress test
        .set("auto.commit.interval.ms", "1000") // Commit every second
        .set("auto.offset.reset", "latest")     // Only read new stress data
        .set("fetch.message.max.bytes", "1048576") // Fetch up to 1MB per request
        .create()
        .expect("Consumer creation failed");

    consumer.subscribe(&[TOPIC]).expect("Subscribe failed");

    loop {
        // Using recv() instead of a stream for tightest possible loop
        match consumer.recv().await {
            Ok(_) => {
                TOTAL_RECV.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                eprintln!("Consumer {} error: {}", id, e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}
*/

// src/bin/stress.rs

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;

// --- Configuration for Maximum Pain ---
const BROKERS: &str = "localhost:9093";
const TOPIC: &str = "urls-to-crawl";
const PRODUCER_THREADS: usize = 4; // Enough to saturate most local links if batched correctly
const CONSUMER_THREADS: usize = 16; // One per partition for maximum parallelism
const MESSAGE_SIZE: usize = 100; // Small messages like URLs (100 bytes)
const REPORT_INTERVAL: u64 = 1; // Report stats every 1 second

// Shared statistics counters
struct Stats {
    produced: AtomicU64,
    consumed: AtomicU64,
}

#[tokio::test]
async fn test() {
    println!("--- ARACHNE KAFKA CLUSTER STRESS TEST ---");
    println!("Target: {} [{} partitions]", TOPIC, CONSUMER_THREADS);
    println!("Producers: {}", PRODUCER_THREADS);
    println!("Consumers: {}", CONSUMER_THREADS);
    println!("Press Ctrl+C to stop.");
    println!("-------------------------------------------");

    let stats = Arc::new(Stats {
        produced: AtomicU64::new(0),
        consumed: AtomicU64::new(0),
    });

    // 1. Start the Monitor Task
    let monitor_stats = stats.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(REPORT_INTERVAL));
        let mut last_produced = 0;
        let mut last_consumed = 0;

        loop {
            interval.tick().await;
            let curr_produced = monitor_stats.produced.load(Ordering::Relaxed);
            let curr_consumed = monitor_stats.consumed.load(Ordering::Relaxed);

            let produced_rate = (curr_produced - last_produced) / REPORT_INTERVAL;
            let consumed_rate = (curr_consumed - last_consumed) / REPORT_INTERVAL;

            println!(
                "[MONITOR] Write: {:>6}/s | Read: {:>6}/s | Lag: {:>8} msgs",
                produced_rate,
                consumed_rate,
                curr_produced.saturating_sub(curr_consumed)
            );

            last_produced = curr_produced;
            last_consumed = curr_consumed;
        }
    });

    // 2. Start Consumers (simulating your Worker fleet)
    // We use a single group ID so they load-balance the 16 partitions between them.
    for i in 0..CONSUMER_THREADS {
        let stats_clone = stats.clone();
        tokio::spawn(async move {
            run_consumer(i, stats_clone).await;
        });
    }

    // Give consumers a moment to connect and rebalance
    time::sleep(Duration::from_secs(5)).await;
    println!("Consumers ready. Launching producers...");

    // 3. Start Producers (simulating high-speed crawling/seeding)
    let mut producer_handles = vec![];
    for i in 0..PRODUCER_THREADS {
        let stats_clone = stats.clone();
        producer_handles.push(tokio::spawn(async move {
            run_producer(i, stats_clone).await;
        }));
    }

    // Keep main alive
    futures::future::join_all(producer_handles).await;
}

async fn run_producer(id: usize, stats: Arc<Stats>) {
    // High-throughput producer configuration
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .set("message.timeout.ms", "5000")
        // Linger: wait up to 5ms to bunch messages into a single network request
        .set("linger.ms", "5")
        // Batch size: try to reach 32KB before sending
        .set("batch.size", "32768")
        // Compression: trades a little CPU for MUCH higher network throughput
//        .set("compression.type", "lz4")
        // Acks=1: Leader only. Fastest 'safe' setting.
        // Use 'acks=0' for absolute max speed (YOLO mode, potential data loss).
        .set("acks", "1")
        .create()
        .expect("Producer creation failed");

    // Pre-generate a payload to avoid allocation in the hot loop
    let payload = "a".repeat(MESSAGE_SIZE);
    // A rotating set of keys to simulate different domains and spread load across partitions
    let keys = ["domain_a", "domain_b", "domain_c", "domain_d", "domain_e", "domain_f", "domain_g", "domain_h"];

    let mut counter = 0;
    loop {
        let key = keys[counter % keys.len()];

        // We use send_result (fire and forget managed by library buffer)
        // instead of 'await'ing every single message for maximum throughput.
        // If the internal queue is full, we wait a tiny bit and retry.
        loop {
            match producer.send_result(FutureRecord::to(TOPIC).payload(&payload).key(key)) {
                Ok(_) => {
                    stats.produced.fetch_add(1, Ordering::Relaxed);
                    break;
                }
                Err(_) => {
                    // Internal queue full, backpressure is kicking in.
                    tokio::task::yield_now().await;
                }
            }
        }
        counter = counter.wrapping_add(1);
    }
}

async fn run_consumer(id: usize, stats: Arc<Stats>) {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", BROKERS)
        .set("group.id", "stress-test-group")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        // Crucial for throughput: fetch larger chunks of data at once
        .set("fetch.message.max.bytes", "1048576")
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "1000")
        .set("auto.offset.reset", "latest") // Only care about new data for stress test
        .create()
        .expect("Consumer creation failed");

    consumer.subscribe(&[TOPIC]).expect("Can't subscribe");

    loop {
        match consumer.recv().await {
            Ok(_) => {
                stats.consumed.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                eprintln!("Consumer {} error: {}", id, e);
                time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}