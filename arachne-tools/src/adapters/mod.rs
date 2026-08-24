//! Source adapters: turn a source's catalog API into Arachne track-manifest
//! rows + AudioFile crawl tasks.
//!
//! Adapters are modules, not crates (charter rule). Each one:
//! 1. enumerates the catalog (paged API walks or offline metadata),
//! 2. emits `TrackRecord`s (status=pending) via `insert_track_if_absent`,
//! 3. emits `CrawlTask`s (kind=AudioFile) for pending tracks onto NATS.
//!
//! License is MANDATORY on every task — the manifest is the legal ledger.

pub mod archive_org;
pub mod fma;
pub mod jamendo;

use anyhow::Result;
use arachne_core::db::ArachneRepo;
use arachne_core::models::{CrawlTask, MediaMeta, TaskKind, TrackRecord, TrackStatus};
use std::sync::Arc;
use uuid::Uuid;

/// A job id shared by every task an adapter run produces.
pub fn harvest_job_id() -> Uuid {
    // Deterministic per source name would be nicer, but a fresh id per run is
    // fine: the manifest dedupes on (source, source_id), not job_id.
    Uuid::new_v4()
}

/// Provenance links captured by adapters for one track.
#[derive(Debug, Clone, Default)]
pub struct OriginLinks {
    /// Human-facing catalog page (Jamendo shareurl, archive.org /details/...).
    pub page_url: Option<String>,
    /// Canonical license deed URL.
    pub license_url: Option<String>,
}

/// Build the manifest row + crawl task pair for one enumerated item.
/// Returns `None` when the license is unknown (never admit unlicensed audio).
#[allow(clippy::too_many_arguments)]
pub fn build_task_and_record(
    job_id: Uuid,
    source: &str,
    source_id: String,
    url: String,
    license: String,
    origin: OriginLinks,
    collection: Option<String>,
    title: Option<String>,
    artist: Option<String>,
    album: Option<String>,
) -> Option<(CrawlTask, TrackRecord)> {
    if license.is_empty() || license == "unknown" {
        return None;
    }

    let meta = MediaMeta {
        source_id,
        source: source.to_string(),
        collection: collection.clone(),
        license,
        origin_page_url: origin.page_url.clone(),
        license_url: origin.license_url.clone(),
        // Adapters enumerate the catalog directly; the catalog page IS where
        // this file was discovered.
        discovered_from_url: origin.page_url.clone(),
        title: title.clone(),
        artist: artist.clone(),
        album,
    };

    // Domain = the download host's root domain (drives politeness/sharding).
    let domain = arachne_core::domain::extract_root_domain(&url).unwrap_or_else(|| "unknown".into());

    let task = CrawlTask {
        url,
        job_id,
        domain,
        depth: 0,
        priority: 0,
        kind: TaskKind::AudioFile,
        media: Some(meta),
    };

    let record = record_from_task(&task, title);
    Some((task, record))
}

/// The pending manifest row matching a freshly-built task.
fn record_from_task(task: &CrawlTask, title: Option<String>) -> TrackRecord {
    let meta = task.media.as_ref().expect("adapter tasks carry MediaMeta");
    TrackRecord {
        source: meta.source.clone(),
        source_id: meta.source_id.clone(),
        job_id: task.job_id,
        url: task.url.clone(),
        title,
        artist: meta.artist.clone(),
        album: meta.album.clone(),
        year: None,
        genre: None,
        license: meta.license.clone(),
        license_url: meta.license_url.clone(),
        origin_page_url: meta.origin_page_url.clone(),
        discovered_from_url: meta.discovered_from_url.clone(),
        collection: meta.collection.clone(),
        duration_secs: None,
        bitrate_kbps: None,
        format: None,
        sha256: None,
        bytes: None,
        object_path: None,
        status: TrackStatus::Pending,
        error: None,
    }
}

/// Admission helper: insert the pending row. Returns true when newly admitted
/// (task should be published); false when the row already existed (skip —
/// re-runs resume instead of re-downloading).
pub async fn admit(repo: &Arc<ArachneRepo>, record: &TrackRecord) -> Result<bool> {
    repo.insert_track_if_absent(record).await
}
