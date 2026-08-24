use anyhow::Result;
use arachne_core::config::ArachneConfig;
use arachne_core::db::ArachneRepo;
use tracing::info;

/// Inspect a domain's crawl metadata.
pub async fn run(config: ArachneConfig, domain: String) -> Result<()> {
    let db = ArachneRepo::new(&config).await?;

    info!(domain = %domain, "Inspecting domain");

    match db.get_domain_metadata_raw(&domain).await? {
        Some((_, robots_fetched_at, crawl_delay_ms, last_crawled_at)) => {
            println!("Domain: {}", domain);
            if let Some(delay) = crawl_delay_ms {
                println!("  Crawl delay: {}ms", delay);
            }
            if let Some(fetched) = robots_fetched_at {
                println!("  Robots.txt fetched: {}ms epoch", fetched);
            }
            if let Some(last) = last_crawled_at {
                println!("  Last crawled: {}ms epoch", last);
            }
        }
        None => {
            println!("No metadata found for domain: {}", domain);
        }
    }

    Ok(())
}
