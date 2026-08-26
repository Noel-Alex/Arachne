use anyhow::Result;
use arachne_core::config::ArachneConfig;
use arachne_core::db::ArachneRepo;
use tracing::info;

/// Render epoch-milliseconds as "YYYY-MM-DD HH:MM:SS UTC", or a placeholder
/// when the value is missing/unparseable.
fn format_epoch_ms(ms: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms)
        .map(|t| t.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| format!("{ms}ms epoch"))
}

/// Inspect a domain's crawl metadata.
pub async fn run(config: ArachneConfig, domain: String) -> Result<()> {
    let db = ArachneRepo::new(&config).await?;

    info!(domain = %domain, "Inspecting domain");

    match db.get_domain_metadata_raw(&domain).await? {
        Some((robots_txt, robots_fetched_at, crawl_delay_ms, last_crawled_at)) => {
            println!("Domain: {}", domain);
            if let Some(txt) = robots_txt {
                println!("  Robots.txt: {} bytes cached", txt.len());
            }
            if let Some(delay) = crawl_delay_ms {
                println!("  Crawl delay: {}ms", delay);
            }
            if let Some(fetched) = robots_fetched_at {
                println!("  Robots.txt fetched: {}", format_epoch_ms(fetched));
            }
            if let Some(last) = last_crawled_at {
                println!("  Last crawled: {}", format_epoch_ms(last));
            }
        }
        None => {
            println!("No metadata found for domain: {}", domain);
        }
    }

    Ok(())
}
