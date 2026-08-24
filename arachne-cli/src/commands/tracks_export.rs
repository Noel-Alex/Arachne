use anyhow::{Context, Result};
use arachne_core::config::ArachneConfig;
use arachne_core::db::ArachneRepo;
use std::fs::File;
use std::io::Write as _;
use std::path::PathBuf;

use crate::commands::attribution;

/// Export the track manifest for a source as a Sivana handoff snapshot.
///
/// Emits:
/// - `manifest.jsonl.zst` — one TrackRecord per line, zstd-compressed
/// - `manifest.json` — dataset-level summary (counts by status/format/license)
/// - `attribution.txt` — human-readable credit lines grouped by license
pub async fn run(
    config: ArachneConfig,
    source: String,
    output_dir: String,
    only_done: bool,
) -> Result<()> {
    let repo = ArachneRepo::new(&config)
        .await
        .context("Failed to connect to database")?;

    let tracks = repo
        .list_tracks_by_source(&source, i64::MAX)
        .await
        .context("Failed to list tracks")?;

    if tracks.is_empty() {
        eprintln!("No tracks found for source '{source}'");
        return Ok(());
    }

    let out = PathBuf::from(&output_dir);
    tokio::fs::create_dir_all(&out).await?;

    // ---- manifest.jsonl.zst ----
    let manifest_path = out.join("manifest.jsonl.zst");
    let total_before_filter = tracks.len();
    let tracks: Vec<_> = tracks
        .into_iter()
        .filter(|t| !only_done || t.status == arachne_core::models::TrackStatus::Done)
        .collect();

    {
        let file = File::create(&manifest_path)
            .with_context(|| format!("creating {}", manifest_path.display()))?;
        let mut enc = zstd::Encoder::new(file, 3)?;
        for track in &tracks {
            let mut line = serde_json::to_vec(track)?;
            line.push(b'\n');
            enc.write_all(&line)?;
        }
        enc.finish()?.sync_all()?;
    }

    // ---- manifest.json summary ----
    let summary = build_summary(&source, &tracks, total_before_filter);
    let summary_path = out.join("manifest.json");
    serde_json::to_writer_pretty(
        File::create(&summary_path).context("creating manifest.json")?,
        &summary,
    )?;

    // ---- attribution.txt ----
    let attribution_path = out.join("attribution.txt");
    attribution::write_attribution(&tracks, &attribution_path)?;

    println!(
        "✔ Exported {} tracks (of {} total) → {}, {}, {}",
        tracks.len(),
        total_before_filter,
        manifest_path.display(),
        summary_path.display(),
        attribution_path.display()
    );

    Ok(())
}

fn build_summary(
    source: &str,
    tracks: &[arachne_core::models::TrackRecord],
    total_rows: usize,
) -> serde_json::Value {
    use std::collections::BTreeMap;
    let mut by_status: BTreeMap<String, u64> = BTreeMap::new();
    let mut by_format: BTreeMap<String, u64> = BTreeMap::new();
    let mut by_license: BTreeMap<String, u64> = BTreeMap::new();
    let mut total_bytes: u64 = 0;
    let mut total_duration_secs: f64 = 0.0;

    for t in tracks {
        *by_status.entry(t.status.as_str().to_string()).or_default() += 1;
        if let Some(f) = &t.format {
            *by_format.entry(f.clone()).or_default() += 1;
        }
        *by_license.entry(t.license.clone()).or_default() += 1;
        total_bytes += t.bytes.unwrap_or(0) as u64;
        total_duration_secs += t.duration_secs.unwrap_or(0.0);
    }

    serde_json::json!({
        "arachne_manifest_version": 1,
        "generated_at": chrono::Utc::now().to_rfc3339(),
        "source": source,
        "total_rows_in_store": total_rows,
        "tracks_exported": tracks.len(),
        "total_bytes": total_bytes,
        "total_duration_secs": total_duration_secs,
        "total_duration_hours": (total_duration_secs / 3600.0 * 100.0).round() / 100.0,
        "by_status": by_status,
        "by_format": by_format,
        "by_license": by_license,
    })
}
