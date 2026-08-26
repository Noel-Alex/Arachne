use anyhow::{Context, Result};
use arachne_core::config::ArachneConfig;
use arachne_core::db::ArachneRepo;
use arachne_core::models::{CrawlJob, CrawlTask, JobStatus};
use arachne_core::nats::NatsManager;
use std::io::{self, BufRead};
use tracing::info;

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
            match line {
                Ok(url) => {
                    let trimmed = url.trim().to_string();
                    if !trimmed.is_empty() {
                        all_urls.push(trimmed);
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Failed reading stdin line, stopping stdin read");
                    break;
                }
            }
        }
        info!("Loaded URLs from stdin");
    }

    if all_urls.is_empty() {
        eprintln!("No URLs provided. Use --urls, --file, or --stdin.");
        return Ok(());
    }

    // Normalize up front so the job row only ever lists usable seeds.
    let mut seeds = Vec::new();
    for url_str in &all_urls {
        match arachne_core::domain::normalize_url(url_str) {
            Some(u) => seeds.push(u),
            None => tracing::warn!(url = %url_str, "Skipping invalid URL"),
        }
    }

    info!(count = all_urls.len(), label = %label, "Seeding URLs");

    // Same rule as crawl: never insert a Running job row with no tasks.
    if seeds.is_empty() {
        anyhow::bail!("no valid seed URLs");
    }

    // Seeded batches get a real job row so their results stay trackable via
    // `arachne status` and remain exempt from the robots policy; seeding into
    // thin air would orphan every task published below.
    let db = ArachneRepo::new(&config)
        .await
        .context("Cannot reach database; seeding requires a persisted job row for tracking")?;

    let job = CrawlJob {
        name: label.clone(),
        status: JobStatus::Running,
        seed_urls: seeds.clone(),
        ..Default::default()
    };

    // Persist before publishing so tasks can never outlive a missing job row.
    db.insert_job(&job).await?;

    let mut success_count = 0;
    for url in &seeds {
        let domain =
            arachne_core::domain::extract_root_domain(url).unwrap_or_else(|| "unknown".to_string());

        let task = CrawlTask {
            url: url.clone(),
            job_id: job.id,
            domain,
            depth: 0,
            priority: 100, // Seeds get highest priority
            kind: Default::default(),
            media: None,
        };

        nats.publish_task(&task).await?;
        success_count += 1;
    }

    info!(
        seeded = success_count,
        skipped = all_urls.len() - success_count,
        job_id = %job.id,
        "Seeding complete"
    );
    println!("✔ Seeded {} URLs (job: {})", success_count, job.id);
    Ok(())
}
