use anyhow::Result;
use arachne_core::config::ArachneConfig;
use arachne_core::db::ArachneRepo;
use arachne_core::models::{CrawlResult, CrawlStatus};
use chrono::Utc;
use futures::future::join_all;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

const TOTAL_RECORDS: usize = 100_000;
const PARALLEL_WORKERS: usize = 16;
const BATCH_SIZE: usize = 250;

#[tokio::main]
async fn main() -> Result<()> {
    arachne_core::logging::init_logging();
    println!("===============================================================");
    println!("⚡ SCYLLADB NATIVE UNLOGGED BATCH WRITE BENCHMARK (100K ROWS)");
    println!("===============================================================");

    let config = ArachneConfig::load(None)?;
    let repo = Arc::new(ArachneRepo::new(&config.scylla).await?);

    let job_id = Uuid::new_v4();
    println!("Job ID: {}", job_id);
    println!("Inserting {} rows across {} parallel ScyllaDB futures (batch size: {})...", TOTAL_RECORDS, PARALLEL_WORKERS, BATCH_SIZE);

    let counter = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();

    let records_per_worker = TOTAL_RECORDS / PARALLEL_WORKERS;
    let mut handles = Vec::new();

    for w_id in 0..PARALLEL_WORKERS {
        let repo_ref = Arc::clone(&repo);
        let counter_ref = Arc::clone(&counter);

        let handle = tokio::spawn(async move {
            let mut batch = Vec::with_capacity(BATCH_SIZE);
            let start_idx = w_id * records_per_worker;
            let end_idx = start_idx + records_per_worker;

            for i in start_idx..end_idx {
                let domain = format!("domain-{}.com", i % 1000);
                let result = CrawlResult {
                    source_url: format!("https://domain-{}.com/page-{}", i % 1000, i),
                    job_id,
                    status: CrawlStatus::Success,
                    domain: Some(domain.clone()),
                    content_ref: Some(format!("file:///crawled_data/{}/hash-{}.html", domain, i)),
                    title: Some(format!("Test Title Page {}", i)),
                    language: Some("en".into()),
                    content_length: Some(15000),
                    content_hash: Some(format!("hash-{}", i)),
                    discovered_urls: vec![],
                    crawl_duration_ms: 45,
                    crawled_at: Utc::now(),
                };

                batch.push((domain, result));

                if batch.len() >= BATCH_SIZE {
                    match repo_ref.insert_crawl_results_batch(&batch).await {
                        Ok(_) => {
                            counter_ref.fetch_add(batch.len(), Ordering::Relaxed);
                        }
                        Err(e) => {
                            eprintln!("ScyllaDB batch error: {:?}", e);
                        }
                    }
                    batch.clear();
                }
            }

            if !batch.is_empty() {
                let count = batch.len();
                if repo_ref.insert_crawl_results_batch(&batch).await.is_ok() {
                    counter_ref.fetch_add(count, Ordering::Relaxed);
                }
            }
        });

        handles.push(handle);
    }

    let _ = join_all(handles).await;

    let elapsed = start.elapsed().as_secs_f64();
    let total = counter.load(Ordering::Relaxed);
    let rate = total as f64 / elapsed;

    println!("\n🔥 CONFIRMED SCYLLADB BENCHMARK RESULT 🔥");
    println!("  Total Rows Inserted: {}", total);
    println!("  Time Elapsed:        {:.2} seconds", elapsed);
    println!("  Write Throughput:    {:.1} CQL inserts/sec ({:.2} Million rows/min)", rate, (rate * 60.0) / 1_000_000.0);
    println!("===============================================================");

    Ok(())
}
