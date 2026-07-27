use anyhow::Result;
use arachne_core::config::ArachneConfig;
use arachne_core::nats::NatsManager;
use arachne_core::models::CrawlTask;
use tracing::info;
use uuid::Uuid;
use std::io::{self, BufRead};

/// Seed URLs into the crawl queue.
pub async fn run(
    config: ArachneConfig,
    urls: Option<Vec<String>>,
    file: Option<String>,
    stdin: bool,
    label: String,
) -> Result<()> {
    let nats = NatsManager::connect(&config.nats).await?;
    nats.ensure_streams().await?;

    let mut all_urls: Vec<String> = Vec::new();

    // Collect URLs from all sources
    if let Some(url_list) = urls {
        all_urls.extend(url_list);
    }

    if let Some(file_path) = file {
        let content = std::fs::read_to_string(&file_path)?;
        for line in content.lines() {
            let trimmed = line.trim();
            if !trimmed.is_empty() && !trimmed.starts_with('#') {
                all_urls.push(trimmed.to_string());
            }
        }
        info!(file = %file_path, count = all_urls.len(), "Loaded URLs from file");
    }

    if stdin {
        let stdin_handle = io::stdin();
        for line in stdin_handle.lock().lines() {
            if let Ok(url) = line {
                let trimmed = url.trim().to_string();
                if !trimmed.is_empty() {
                    all_urls.push(trimmed);
                }
            }
        }
        info!("Loaded URLs from stdin");
    }

    if all_urls.is_empty() {
        eprintln!("No URLs provided. Use --urls, --file, or --stdin.");
        return Ok(());
    }

    // Generate a job ID for this seed batch
    let job_id = Uuid::new_v4();
    info!(job_id = %job_id, count = all_urls.len(), label = %label, "Seeding URLs");

    let mut success_count = 0;
    for url_str in &all_urls {
        let normalized = match arachne_core::domain::normalize_url(url_str) {
            Some(u) => u,
            None => {
                tracing::warn!(url = %url_str, "Skipping invalid URL");
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
            priority: 100, // Seeds get highest priority
        };

        nats.publish_task(&task).await?;
        success_count += 1;
    }

    info!(seeded = success_count, skipped = all_urls.len() - success_count, "Seeding complete");
    println!("✔ Seeded {} URLs (job: {})", success_count, job_id);
    Ok(())
}
