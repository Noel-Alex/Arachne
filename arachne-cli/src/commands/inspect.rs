use anyhow::Result;
use arachne_core::config::ArachneConfig;
use arachne_core::db::ArachneRepo;
use tracing::info;

/// Inspect a domain's crawl metadata.
pub async fn run(config: ArachneConfig, domain: String) -> Result<()> {
    let db = ArachneRepo::new(&config.scylla).await?;

    info!(domain = %domain, "Inspecting domain");

    match db.get_domain_metadata(&domain).await? {
        Some(meta) => {
            println!("Domain: {}", domain);
            if let Some(delay) = meta.crawl_delay_ms {
                println!("  Crawl delay: {}ms", delay);
            }
            if let Some(fetched) = meta.robots_fetched_at {
                println!("  Robots.txt fetched: {}", fetched);
            }
            if let Some(last) = meta.last_crawled_at {
                println!("  Last crawled: {}", last);
            }
        }
        None => {
            println!("No metadata found for domain: {}", domain);
        }
    }

    Ok(())
}
