use anyhow::{Context, Result};
use arachne_core::config::ArachneConfig;
use arachne_core::db::ArachneRepo;
use std::fs::File;
use std::io::Write;
use tracing::info;
use uuid::Uuid;

/// Export crawled data in various formats (JSON, CSV).
pub async fn run(
    config: ArachneConfig,
    job_id: Option<String>,
    domain: Option<String>,
    format: String,
    output: String,
) -> Result<()> {
    info!(format = %format, output = %output, "Starting export");

    let target_domain = match domain {
        Some(d) => d,
        None => {
            eprintln!("Please specify a domain using --domain <DOMAIN>");
            return Ok(());
        }
    };

    let filter_job_uuid = job_id.and_then(|id_str| Uuid::parse_str(&id_str).ok());

    let repo = ArachneRepo::new(&config)
        .await
        .context("Failed to connect to database")?;

    let mut pages = repo.get_pages_by_domain(&target_domain).await?;

    if let Some(target_uuid) = filter_job_uuid {
        pages.retain(|p| p.job_id == target_uuid);
    }

    println!("Found {} pages for domain '{}'", pages.len(), target_domain);

    let output_format = format.to_lowercase();
    match output_format.as_str() {
        "json" => {
            let file_path = if output.ends_with(".json") {
                output
            } else {
                format!("{}.json", output)
            };
            let file = File::create(&file_path)?;
            serde_json::to_writer_pretty(file, &pages)?;
            println!("✔ Successfully exported JSON to {}", file_path);
        }
        "csv" => {
            let file_path = if output.ends_with(".csv") {
                output
            } else {
                format!("{}.csv", output)
            };
            let mut file = File::create(&file_path)?;
            writeln!(
                file,
                "domain,job_id,url,http_status,content_length,content_hash,title,language,content_ref,crawled_at"
            )?;
            for page in pages {
                writeln!(
                    file,
                    "\"{}\",\"{}\",\"{}\",{},{},\"{}\",\"{}\",\"{}\",\"{}\",\"{}\"",
                    page.domain,
                    page.job_id,
                    page.url,
                    page.http_status,
                    page.content_length.unwrap_or(0),
                    page.content_hash.unwrap_or_default(),
                    page.title.unwrap_or_default().replace('"', "\"\""),
                    page.language.unwrap_or_default(),
                    page.content_ref.unwrap_or_default(),
                    page.crawled_at.map(|t| t.to_rfc3339()).unwrap_or_default()
                )?;
            }
            println!("✔ Successfully exported CSV to {}", file_path);
        }
        _ => {
            eprintln!(
                "Unsupported format '{}'. Supported formats: json, csv",
                format
            );
        }
    }

    Ok(())
}
