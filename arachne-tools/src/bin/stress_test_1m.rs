use anyhow::Result;
use arachne_core::config::ArachneConfig;
use arachne_core::models::CrawlTask;
use arachne_core::nats::NatsManager;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use uuid::Uuid;

const TOTAL_URLS: usize = 1_000_000;
const CONCURRENT_PRODUCERS: usize = 8;
const BATCH_PER_PRODUCER: usize = 5_000;

#[tokio::main]
async fn main() -> Result<()> {
    arachne_core::logging::init_logging();
    println!("===============================================================");
    println!("🚀 ARACHNE 1,000,000 (1 MILLION) NATS TASK PUBLISH BENCHMARK");
    println!("===============================================================");

    let config = ArachneConfig::load(None)?;
    let nats = Arc::new(NatsManager::connect(&config.nats).await?);
    nats.ensure_streams().await?;

    let job_id = Uuid::new_v4();
    println!("Job ID: {}", job_id);
    println!(
        "Target: {} URLs across {} parallel producer tasks (batch size: {})...",
        TOTAL_URLS, CONCURRENT_PRODUCERS, BATCH_PER_PRODUCER
    );

    let published_counter = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();

    let urls_per_producer = TOTAL_URLS / CONCURRENT_PRODUCERS;
    let mut handles = Vec::new();

    for p_id in 0..CONCURRENT_PRODUCERS {
        let nats_ref = Arc::clone(&nats);
        let counter_ref = Arc::clone(&published_counter);

        let handle = tokio::spawn(async move {
            let mut tasks = Vec::with_capacity(BATCH_PER_PRODUCER);
            let start_idx = p_id * urls_per_producer;
            let end_idx = start_idx + urls_per_producer;

            for i in start_idx..end_idx {
                let domain_id = i % 5000;
                let task = CrawlTask {
                    url: format!("https://domain-{}.com/page-{}", domain_id, i),
                    job_id,
                    domain: format!("domain-{}.com", domain_id),
                    depth: 0,
                    priority: 1,
                    kind: Default::default(),
                    media: None,
                };
                tasks.push(task);

                if tasks.len() >= BATCH_PER_PRODUCER {
                    match nats_ref.publish_tasks_batch(&tasks).await {
                        Ok(_) => {
                            counter_ref.fetch_add(tasks.len(), Ordering::Relaxed);
                        }
                        Err(e) => {
                            eprintln!("Producer error: {:?}", e);
                        }
                    }
                    tasks.clear();
                }
            }

            if !tasks.is_empty() {
                let count = tasks.len();
                if nats_ref.publish_tasks_batch(&tasks).await.is_ok() {
                    counter_ref.fetch_add(count, Ordering::Relaxed);
                }
            }
        });

        handles.push(handle);
    }

    // Monitor progress loop
    let monitor_counter = Arc::clone(&published_counter);
    let monitor_handle = tokio::spawn(async move {
        let mut last_reported = 0;
        loop {
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            let current = monitor_counter.load(Ordering::Relaxed);
            if current > last_reported {
                let elapsed = start.elapsed().as_secs_f64();
                let rate = current as f64 / elapsed;
                println!(
                    "> Progress: {} / {} URLs published ({:.1} msg/sec)",
                    current, TOTAL_URLS, rate
                );
                last_reported = current;
            }
            if current >= TOTAL_URLS {
                break;
            }
        }
    });

    for h in handles {
        let _ = h.await;
    }
    let _ = monitor_handle.await;

    let elapsed = start.elapsed().as_secs_f64();
    let total = published_counter.load(Ordering::Relaxed);
    let final_rate = total as f64 / elapsed;

    println!("\n🔥 CONFIRMED ACKNOWLEDGED PUBLISH BENCHMARK RESULT 🔥");
    println!("  Total Messages ACKed: {}", total);
    println!("  Time Taken:           {:.2} seconds", elapsed);
    println!(
        "  Throughput:           {:.1} msg/sec ({:.2} Million msg/min)",
        final_rate,
        (final_rate * 60.0) / 1_000_000.0
    );
    println!("===============================================================");

    Ok(())
}
