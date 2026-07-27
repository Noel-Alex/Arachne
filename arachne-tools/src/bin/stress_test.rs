use anyhow::Result;
use arachne_core::config::ArachneConfig;
use arachne_core::models::CrawlTask;
use arachne_core::nats::NatsManager;
use std::time::Instant;
use tracing::info;
use uuid::Uuid;

const STRESS_URL_COUNT: usize = 10_000;

#[tokio::main]
async fn main() -> Result<()> {
    arachne_core::logging::init_logging();
    println!("=== ARACHNE 10K URL STRESS TEST ===");

    let config = ArachneConfig::load(None)?;
    let nats = NatsManager::connect(&config.nats).await?;
    nats.ensure_streams().await?;

    let job_id = Uuid::new_v4();
    println!("Job ID: {}", job_id);
    println!("Pushing {} synthetic URLs to NATS JetStream...", STRESS_URL_COUNT);

    let start = Instant::now();

    for i in 0..STRESS_URL_COUNT {
        let domain_id = i % 500;
        let url = format!("https://domain-{}.com/page-{}", domain_id, i);

        let task = CrawlTask {
            url: url.clone(),
            job_id,
            domain: format!("domain-{}.com", domain_id),
            depth: 0,
            priority: 1,
        };

        nats.publish_task(&task).await?;

        if (i + 1) % 2500 == 0 {
            let elapsed = start.elapsed().as_secs_f64();
            let rate = (i + 1) as f64 / elapsed;
            println!("> Progress: {} / {} URLs pushed ({:.1} msg/s)", i + 1, STRESS_URL_COUNT, rate);
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    let rate = STRESS_URL_COUNT as f64 / elapsed;
    println!("\n✔ Successfully published {} tasks in {:.2}s ({:.1} tasks/sec)", STRESS_URL_COUNT, elapsed, rate);
    println!("=====================================");

    Ok(())
}
