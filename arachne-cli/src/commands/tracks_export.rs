use anyhow::{Context, Result};
use arachne_core::config::ArachneConfig;
use arachne_core::db::ArachneRepo;
use arachne_core::models::{TrackRecord, TrackStatus};
use std::fs::File;
use std::io::Write as _;
use std::path::PathBuf;

use crate::commands::attribution;

/// Licenses under which redistribution is permitted. CONTRACT.md promises
/// Sivana a redistributable-only snapshot by default, so anything else (e.g.
/// Jamendo-admitted NC tracks) ships only when `--all-licenses` is passed.
const REDISTRIBUTABLE: [&str; 5] = ["cc-by", "cc-by-sa", "cc0-1.0", "pd-mark", "pd-us"];

/// Export the track manifest for a source as a Sivana handoff snapshot.
///
/// Default output is done-only and restricted to redistributable licenses;
/// `--include-incomplete` adds pending/failed/rejected rows and
/// `--all-licenses` lifts the license restriction.
///
/// Emits:
/// - `manifest.jsonl.zst` — one TrackRecord per line, zstd-compressed
/// - `manifest.json` — dataset-level summary (counts by status/format/license)
/// - `attribution.txt` — human-readable credit lines grouped by license
pub async fn run(
    config: ArachneConfig,
    source: String,
    output_dir: String,
    include_incomplete: bool,
    all_licenses: bool,
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
        .filter(|t| track_exported(t, include_incomplete, all_licenses))
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

/// A row ships only when it is complete (unless incompletes were requested)
/// and its license permits redistribution (unless all licenses were requested).
fn track_exported(t: &TrackRecord, include_incomplete: bool, all_licenses: bool) -> bool {
    (include_incomplete || t.status == TrackStatus::Done)
        && (all_licenses || REDISTRIBUTABLE.contains(&t.license.as_str()))
}

fn build_summary(source: &str, tracks: &[TrackRecord], total_rows: usize) -> serde_json::Value {
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

#[cfg(test)]
mod tests {
    use super::track_exported;
    use arachne_core::models::{TrackRecord, TrackStatus};
    use uuid::Uuid;

    fn record(status: TrackStatus, license: &str) -> TrackRecord {
        TrackRecord {
            source: "test".into(),
            source_id: "t1".into(),
            job_id: Uuid::new_v4(),
            url: "https://example.com/a.mp3".into(),
            title: None,
            artist: None,
            album: None,
            year: None,
            genre: None,
            license: license.into(),
            license_url: None,
            origin_page_url: None,
            discovered_from_url: None,
            collection: None,
            duration_secs: None,
            bitrate_kbps: None,
            format: None,
            sha256: None,
            bytes: None,
            object_path: None,
            status,
            error: None,
        }
    }

    #[test]
    fn done_redistributable_exports_by_default() {
        assert!(track_exported(
            &record(TrackStatus::Done, "cc-by"),
            false,
            false
        ));
        assert!(track_exported(
            &record(TrackStatus::Done, "cc0-1.0"),
            false,
            false
        ));
    }

    #[test]
    fn incomplete_rows_need_include_incomplete_flag() {
        let t = record(TrackStatus::Pending, "cc-by");
        assert!(!track_exported(&t, false, false));
        assert!(track_exported(&t, true, false));
    }

    #[test]
    fn non_redistributable_needs_all_licenses_flag() {
        let t = record(TrackStatus::Done, "cc-by-nc");
        assert!(!track_exported(&t, false, false));
        assert!(track_exported(&t, false, true));
    }
}
