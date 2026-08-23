use anyhow::Result;
use arachne_core::config::ArachneConfig;
use arachne_core::db::ArachneRepo;
use arachne_core::models::{CrawlJob, CrawlTask, JobStatus};
use arachne_core::nats::NatsManager;
use chrono::Utc;
use tracing::info;
use uuid::Uuid;

/// Parse a human-readable size string like "5MB" or "1GB" into bytes.
fn parse_size(s: &str) -> Option<usize> {
    let s = s.trim().to_uppercase();
    if let Some(num) = s.strip_suffix("GB") {
        num.trim()
            .parse::<usize>()
            .ok()
            .map(|n| n * 1024 * 1024 * 1024)
    } else if let Some(num) = s.strip_suffix("MB") {
        num.trim().parse::<usize>().ok().map(|n| n * 1024 * 1024)
    } else if let Some(num) = s.strip_suffix("KB") {
        num.trim().parse::<usize>().ok().map(|n| n * 1024)
    } else {
        s.parse::<usize>().ok()
    }
}

/// Start a new crawl job.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    config: ArachneConfig,
    seeds: Vec<String>,
    name: Option<String>,
    max_pages: Option<u64>,
    max_pages_per_domain: Option<i64>,
    max_depth: Option<u32>,
    allowed_domains: Option<Vec<String>>,
    follow_external: bool,
    crawl_delay: Option<u64>,
    topic: Option<Vec<String>>,
    max_content_size: Option<String>,
    store_html: bool,
    store_text: bool,
    ignore_robots: bool,
) -> Result<()> {
    let nats = NatsManager::connect(&config.nats).await?;
    nats.ensure_streams().await?;

    let db = ArachneRepo::new(&config.scylla).await?;

    let job_id = Uuid::new_v4();
    let job_name = name.unwrap_or_else(|| format!("crawl-{}", &job_id.to_string()[..8]));

    let max_content_bytes = max_content_size.and_then(|s| parse_size(&s));

    // Create the job
    let job = CrawlJob {
        id: job_id,
        name: job_name.clone(),
        status: JobStatus::Running,
        created_at: Utc::now(),
        seed_urls: seeds.clone(),
        allowed_domains,
        url_patterns: None,
        exclude_patterns: None,
        max_pages,
        max_pages_per_domain: max_pages_per_domain
            .or(Some(config.coordinator.max_pages_per_domain)),
        max_depth,
        max_content_size: max_content_bytes,
        follow_external_links: follow_external,
        respect_robots_txt: !ignore_robots,
        custom_user_agent: None,
        custom_headers: None,
        crawl_delay_ms: crawl_delay,
        topic_keywords: topic,
        store_raw_html: store_html,
        store_text,
    };

    // Persist the job
    db.insert_job(&job).await?;
    info!(job_id = %job_id, name = %job_name, seeds = seeds.len(), "Created crawl job");

    // Seed the initial URLs
    for url_str in &seeds {
        let normalized = match arachne_core::domain::normalize_url(url_str) {
            Some(u) => u,
            None => {
                tracing::warn!(url = %url_str, "Skipping invalid seed URL");
                continue;
            }
        };

        let domain = arachne_core::domain::extract_root_domain(&normalized)
            .unwrap_or_else(|| "unknown".to_string());

        let task = CrawlTask {
            url: normalized,
            job_id,
            domain,
            depth: 0,
            priority: 100,
        };

        nats.publish_task(&task).await?;
    }

    println!("✔ Crawl job '{}' started (id: {})", job_name, job_id);
    println!("  Seeds: {}", seeds.len());
    if let Some(mp) = max_pages {
        println!("  Max pages: {}", mp);
    }
    if let Some(md) = max_depth {
        println!("  Max depth: {}", md);
    }

    Ok(())
}
