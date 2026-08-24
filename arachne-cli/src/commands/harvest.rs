//! `arachne harvest <source>` — enumerate a legal catalog, admit manifest rows,
//! and queue AudioFile download tasks for workers.

use anyhow::{Context, Result};
use arachne_core::config::ArachneConfig;
use arachne_core::db::ArachneRepo;
use arachne_core::nats::NatsManager;
use std::sync::Arc;
use tracing::info;

pub async fn run(
    config: ArachneConfig,
    source: String,
    limit: Option<u64>,
    jamendo_client_id: Option<String>,
    contact: Option<String>,
) -> Result<()> {
    let repo = Arc::new(
        ArachneRepo::new(&config)
            .await
            .context("Failed to connect to database")?,
    );
    let nats = Arc::new(
        NatsManager::connect(&config.nats)
            .await
            .context("Failed to connect to NATS")?,
    );
    nats.ensure_streams().await?;

    match source.as_str() {
        "jamendo" => {
            let client_id = jamendo_client_id
                .or_else(|| std::env::var("JAMENDO_CLIENT_ID").ok())
                .context("Jamendo requires --jamendo-client-id or JAMENDO_CLIENT_ID")?;
            let mut cfg = arachne_tools::adapters::jamendo::JamendoConfig::new(client_id);
            cfg.max_tracks = limit;
            let (admitted, existing) =
                arachne_tools::adapters::jamendo::harvest(&cfg, repo, nats).await?;
            info!(source = "jamendo", admitted, existing, "harvest complete");
            println!("✔ jamendo: {admitted} new tracks queued, {existing} already in manifest");
        }
        "archive-org" => {
            let contact_addr = contact
                .or_else(|| std::env::var("ARACHNE_CONTACT").ok())
                .context(
                    "archive.org policy REQUIRES a contact address: pass --contact or ARACHNE_CONTACT",
                )?;
            let mut cfg =
                arachne_tools::adapters::archive_org::ArchiveOrgConfig::new(contact_addr);
            cfg.max_items = limit;
            let (admitted, existing) =
                arachne_tools::adapters::archive_org::harvest(&cfg, repo, nats).await?;
            info!(source = "archive-org", admitted, existing, "harvest complete");
            println!(
                "✔ archive-org (netlabels, redistributable-only): {admitted} files queued, {existing} already in manifest"
            );
        }
        "fma" | "fma-large" | "fma-medium" | "fma-small" => {
            let subset = if source == "fma" { "fma_large" } else { source.as_str() };
            let mut cfg = arachne_tools::adapters::fma::FmaConfig::new(subset);
            cfg.max_tracks = limit;
            let (admitted, existing) =
                arachne_tools::adapters::fma::harvest(&cfg, repo, nats).await?;
            info!(source = "fma", subset, admitted, existing, "harvest complete");
            println!(
                "✔ fma ({subset}): {admitted} new tracks admitted, {existing} already in manifest"
            );
        }
        other => {
            eprintln!("Unknown source '{other}'. Available: jamendo, archive-org, fma");
        }
    }

    Ok(())
}
