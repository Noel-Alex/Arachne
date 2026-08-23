use anyhow::Result;
use arachne_core::config::ArachneConfig;
use arachne_core::models::CrawlTask;
use arachne_core::nats::NatsManager;
use std::time::Instant;
use uuid::Uuid;

const TOTAL_URLS: usize = 1_000_000;
const BATCH_SIZE: usize = 5_000;

#[tokio::main]
async fn main() -> Result<()> {
    arachne_core::logging::init_logging();
    println!("=== ARACHNE ULTRA HIGH-THROUGHPUT STRESS TEST (100K URLs) ===");

    let config = ArachneConfig::load(None)?;
    let nats = NatsManager::connect(&config.nats).await?;
    nats.ensure_streams().await?;

    let job_id = Uuid::new_v4();
    println!("Job ID: {}", job_id);
    println!(
        "Pushing {} synthetic URLs with pipelined batching (batch size: {})...",
        TOTAL_URLS, BATCH_SIZE
    );

    let start = Instant::now();
    let mut tasks = Vec::with_capacity(BATCH_SIZE);

    for i in 0..TOTAL_URLS {
        let domain_id = i % 2000;
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

        if tasks.len() >= BATCH_SIZE {
            nats.publish_tasks_batch(&tasks).await?;
            tasks.clear();

            if (i + 1) % 200_000 == 0 {
                let elapsed = start.elapsed().as_secs_f64();
                let rate = (i + 1) as f64 / elapsed;
                println!(
                    "> Progress: {} / {} URLs pushed ({:.1} tasks/sec)",
                    i + 1,
                    TOTAL_URLS,
                    rate
                );
            }
        }
    }

    if !tasks.is_empty() {
        nats.publish_tasks_batch(&tasks).await?;
    }

    let elapsed = start.elapsed().as_secs_f64();
    let rate = TOTAL_URLS as f64 / elapsed;
    println!(
        "\n🚀 SUCCESS! Published {} tasks in {:.2}s ({:.1} tasks/sec)",
        TOTAL_URLS, elapsed, rate
    );
    println!("===============================================================");

    Ok(())
}
