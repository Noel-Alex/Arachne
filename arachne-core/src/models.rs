//! Data models used throughout Arachne.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Versioned envelope wrapper for all wire messages across NATS.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionedEnvelope<T> {
    pub version: u32,
    pub event_id: Uuid,
    pub timestamp_ms: i64,
    pub payload: T,
}

impl<T> VersionedEnvelope<T> {
    pub fn new(payload: T) -> Self {
        Self {
            version: 1,
            event_id: Uuid::new_v4(),
            timestamp_ms: Utc::now().timestamp_millis(),
            payload,
        }
    }
}

/// Represents a crawl job configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrawlJob {
    pub id: Uuid,
    pub name: String,
    pub status: JobStatus,
    pub created_at: DateTime<Utc>,
    pub seed_urls: Vec<String>,
    pub allowed_domains: Option<Vec<String>>,
    pub url_patterns: Option<Vec<String>>,
    pub exclude_patterns: Option<Vec<String>>,
    pub max_pages: Option<u64>,
    pub max_pages_per_domain: Option<i64>,
    pub max_depth: Option<u32>,
    pub max_content_size: Option<usize>,
    pub follow_external_links: bool,
    pub respect_robots_txt: bool,
    pub custom_user_agent: Option<String>,
    pub custom_headers: Option<HashMap<String, String>>,
    pub crawl_delay_ms: Option<u64>,
    pub topic_keywords: Option<Vec<String>>,
    pub store_raw_html: bool,
    pub store_text: bool,
    /// License applied to audio files discovered organically (no adapter
    /// metadata) during this job. Audio found without a license AND without
    /// this default is never admitted to the manifest.
    #[serde(default)]
    pub default_license: Option<String>,
}

impl CrawlJob {
    /// Evaluate whether a candidate URL and depth adhere to job rules.
    pub fn is_url_allowed(
        &self,
        candidate_url: &str,
        current_depth: u32,
        candidate_root_domain: &str,
    ) -> bool {
        // 1. Max depth check
        if let Some(max_depth) = self.max_depth
            && current_depth > max_depth
        {
            return false;
        }

        // 2. Allowed domains check
        if let Some(ref domains) = self.allowed_domains
            && !domains.is_empty()
            && !domains
                .iter()
                .any(|d| d.eq_ignore_ascii_case(candidate_root_domain))
        {
            return false;
        }

        // 3. Exclude patterns check
        if let Some(ref excludes) = self.exclude_patterns {
            for pattern in excludes {
                if candidate_url.contains(pattern) {
                    return false;
                }
            }
        }

        // 4. URL patterns check
        if let Some(ref includes) = self.url_patterns
            && !includes.is_empty()
            && !includes.iter().any(|p| candidate_url.contains(p))
        {
            return false;
        }

        true
    }
}

impl Default for CrawlJob {
    fn default() -> Self {
        Self {
            id: Uuid::new_v4(),
            name: "default_job".to_string(),
            status: JobStatus::Pending,
            created_at: Utc::now(),
            seed_urls: vec![],
            allowed_domains: None,
            url_patterns: None,
            exclude_patterns: None,
            max_pages: None,
            max_pages_per_domain: None,
            max_depth: None,
            max_content_size: None,
            follow_external_links: false,
            respect_robots_txt: true,
            custom_user_agent: None,
            custom_headers: None,
            crawl_delay_ms: None,
            topic_keywords: None,
            store_raw_html: false,
            store_text: true,
            default_license: None,
        }
    }
}

/// Status of a crawl job.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum JobStatus {
    Pending,
    Running,
    Paused,
    Completed,
    Failed,
}

/// Represents a single task to crawl a URL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrawlTask {
    pub url: String,
    pub job_id: Uuid,
    pub domain: String,
    pub depth: u32,
    pub priority: i32,
    /// What kind of resource this task targets; defaults to [`TaskKind::Page`]
    /// so pre-existing serialized tasks decode unchanged.
    #[serde(default)]
    pub kind: TaskKind,
    /// Source-adapter metadata carried through to the track manifest.
    #[serde(default)]
    pub media: Option<MediaMeta>,
}

/// The class of resource a task fetches. Drives worker dispatch:
/// pages go through the HTML extraction pipeline, media kinds through
/// the streaming binary downloader with per-kind verification.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskKind {
    #[default]
    Page,
    /// Audio (mp3/flac/ogg/wav/m4a/opus/aac) — full probe + quality gates.
    AudioFile,
    /// Video containers (mp4/mkv/webm/avi/mov) — magic-byte verified only;
    /// no duration/bitrate probing yet (symphonia is audio-only).
    VideoFile,
    /// Documents (pdf/doc(x)/epub/txt) — magic-byte verified; PDFs get
    /// page-count metadata when parseable.
    DocumentFile,
    /// Any other binary asset (images, datasets, archives) stored
    /// content-addressed without format-specific validation.
    BinaryFile,
}

impl TaskKind {
    /// Whether this kind flows through the streaming binary downloader.
    pub fn is_media(self) -> bool {
        !matches!(self, Self::Page)
    }
}

/// Provenance metadata attached to media download tasks by source adapters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaMeta {
    /// Stable identifier of this track within its source (e.g. Jamendo track id).
    pub source_id: String,
    /// Which adapter produced this task ("jamendo", "archive-org", "fma", ...).
    pub source: String,
    /// Collection/album within the source, if known.
    pub collection: Option<String>,
    /// License identifier (SPDX-ish: "cc-by-40", "cc-by-nc-40", "pd-us-mma", "unknown").
    pub license: String,
    /// Human-facing catalog page for this track (Jamendo shareurl, archive.org
    /// /details/<id>, …). The link a user can visit to verify provenance.
    #[serde(default)]
    pub origin_page_url: Option<String>,
    /// Canonical license deed URL (e.g. https://creativecommons.org/licenses/by/4.0/).
    #[serde(default)]
    pub license_url: Option<String>,
    /// The page that linked/discovered this audio. For organic discovery this
    /// is the crawled page containing the <a href>; adapters set it to the
    /// catalog page too when no better page exists.
    #[serde(default)]
    pub discovered_from_url: Option<String>,
    /// Expected file title, from the source's own metadata.
    pub title: Option<String>,
    pub artist: Option<String>,
    pub album: Option<String>,
}

/// Technical + tagged audio properties measured by the worker's probe, carried
/// back on the result so the coordinator can complete the track manifest
/// without re-probing the file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaProbe {
    pub duration_secs: f64,
    pub bitrate_kbps: Option<i32>,
    /// Normalized container/codec extension ("mp3", "flac", ...).
    pub format: String,
    pub title: Option<String>,
    pub artist: Option<String>,
    pub album: Option<String>,
    pub year: Option<i32>,
    pub genre: Option<String>,
}

/// Represents the result of crawling a URL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrawlResult {
    pub source_url: String,
    pub job_id: Uuid,
    pub status: CrawlStatus,
    pub domain: Option<String>,
    pub content_ref: Option<String>,
    pub title: Option<String>,
    pub language: Option<String>,
    pub content_length: Option<usize>,
    pub content_hash: Option<String>,
    pub discovered_urls: Vec<DiscoveredUrl>,
    pub crawl_duration_ms: u64,
    pub crawled_at: DateTime<Utc>,
    /// Populated when the fetched resource was an audio file.
    #[serde(default)]
    pub media_meta: Option<MediaMeta>,
    /// Probe results for audio downloads (duration/bitrate/tags/format).
    #[serde(default)]
    pub media_probe: Option<MediaProbe>,
}

/// Represents a URL discovered during crawling.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredUrl {
    pub url: String,
    pub source_url: String,
    pub job_id: Uuid,
    pub depth: u32,
}

/// Batch container for discovered URLs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredUrlBatch {
    pub urls: Vec<DiscoveredUrl>,
}

/// Lifecycle of a track in the manifest (crash-recovery aware).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TrackStatus {
    Pending,
    Downloading,
    Done,
    Rejected,
    Failed,
}

impl TrackStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Downloading => "downloading",
            Self::Done => "done",
            Self::Rejected => "rejected",
            Self::Failed => "failed",
        }
    }

    pub fn parse(s: &str) -> Self {
        match s {
            "downloading" => Self::Downloading,
            "done" => Self::Done,
            "rejected" => Self::Rejected,
            "failed" => Self::Failed,
            _ => Self::Pending,
        }
    }
}

/// One row of the Sivana handoff manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackRecord {
    pub source: String,
    pub source_id: String,
    pub job_id: Uuid,
    pub url: String,
    pub title: Option<String>,
    pub artist: Option<String>,
    pub album: Option<String>,
    pub year: Option<i32>,
    pub genre: Option<String>,
    /// License identifier; mandatory for admission to the manifest.
    pub license: String,
    /// Canonical license deed URL, when the source provides one.
    #[serde(default)]
    pub license_url: Option<String>,
    /// Human-facing catalog page for this track (verify provenance here).
    #[serde(default)]
    pub origin_page_url: Option<String>,
    /// The page that linked/discovered this audio file.
    #[serde(default)]
    pub discovered_from_url: Option<String>,
    pub collection: Option<String>,
    pub duration_secs: Option<f64>,
    pub bitrate_kbps: Option<i32>,
    pub format: Option<String>,
    pub sha256: Option<String>,
    pub bytes: Option<i64>,
    pub object_path: Option<String>,
    pub status: TrackStatus,
    pub error: Option<String>,
}

/// Status of crawling a single URL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CrawlStatus {
    Success,
    HttpError(u16),
    FetchError(String),
    RobotsBlocked,
    ContentTooLarge,
    InvalidContentType,
    Timeout,
    /// Downloaded file could not be probed as audio (corrupt or wrong magic bytes).
    ProbeFailed,
    /// File downloaded fine but rejected by quality gates (duration/bitrate bounds).
    QualityRejected,
    /// Downloaded content hash does not match expectation (e.g. FMA md5 check).
    ChecksumMismatch,
}

impl CrawlStatus {
    /// Convert to an integer representation.
    pub fn as_i32(&self) -> i32 {
        match self {
            CrawlStatus::Success => 0,
            CrawlStatus::HttpError(code) => *code as i32,
            CrawlStatus::FetchError(_) => -1,
            CrawlStatus::RobotsBlocked => -2,
            CrawlStatus::ContentTooLarge => -3,
            CrawlStatus::InvalidContentType => -4,
            CrawlStatus::Timeout => -5,
            CrawlStatus::ProbeFailed => -6,
            CrawlStatus::QualityRejected => -7,
            CrawlStatus::ChecksumMismatch => -8,
        }
    }

    /// Check if the status represents success.
    pub fn is_success(&self) -> bool {
        matches!(self, CrawlStatus::Success)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crawl_status_roundtrip_i32() {
        assert_eq!(CrawlStatus::Success.as_i32(), 0);
        assert_eq!(CrawlStatus::HttpError(404).as_i32(), 404);
        assert_eq!(CrawlStatus::FetchError("boom".into()).as_i32(), -1);
        assert_eq!(CrawlStatus::RobotsBlocked.as_i32(), -2);
        assert_eq!(CrawlStatus::ContentTooLarge.as_i32(), -3);
        assert_eq!(CrawlStatus::InvalidContentType.as_i32(), -4);
        assert_eq!(CrawlStatus::Timeout.as_i32(), -5);
        assert_eq!(CrawlStatus::ProbeFailed.as_i32(), -6);
        assert_eq!(CrawlStatus::QualityRejected.as_i32(), -7);
        assert_eq!(CrawlStatus::ChecksumMismatch.as_i32(), -8);
    }

    #[test]
    fn track_status_str_parse_roundtrip() {
        for status in [
            TrackStatus::Pending,
            TrackStatus::Downloading,
            TrackStatus::Done,
            TrackStatus::Rejected,
            TrackStatus::Failed,
        ] {
            assert_eq!(TrackStatus::parse(status.as_str()), status);
        }
        assert_eq!(TrackStatus::parse("nonsense"), TrackStatus::Pending);
        assert_eq!(TrackStatus::parse(""), TrackStatus::Pending);
    }

    #[test]
    fn task_kind_default_is_page() {
        assert_eq!(TaskKind::default(), TaskKind::Page);
        assert!(!TaskKind::Page.is_media());
        for kind in [
            TaskKind::AudioFile,
            TaskKind::VideoFile,
            TaskKind::DocumentFile,
            TaskKind::BinaryFile,
        ] {
            assert!(kind.is_media());
        }
    }

    #[test]
    fn crawl_job_policy_matrix() {
        // Depth boundary: depth == max_depth passes, +1 fails.
        let depth_job = CrawlJob {
            max_depth: Some(2),
            ..CrawlJob::default()
        };
        assert!(depth_job.is_url_allowed("https://ex.com/a", 2, "ex.com"));
        assert!(!depth_job.is_url_allowed("https://ex.com/a", 3, "ex.com"));

        // Allowed domains match case-insensitively.
        let domain_job = CrawlJob {
            allowed_domains: Some(vec!["EXAMPLE.COM".to_string()]),
            ..CrawlJob::default()
        };
        assert!(domain_job.is_url_allowed("https://example.com/a", 0, "example.com"));
        assert!(!domain_job.is_url_allowed("https://other.com/a", 0, "other.com"));

        // Exclude patterns reject on any substring hit.
        let exclude_job = CrawlJob {
            exclude_patterns: Some(vec!["/admin".to_string()]),
            ..CrawlJob::default()
        };
        assert!(!exclude_job.is_url_allowed("https://ex.com/admin/panel", 0, "ex.com"));
        assert!(exclude_job.is_url_allowed("https://ex.com/public", 0, "ex.com"));

        // URL patterns require at least one substring hit.
        let pattern_job = CrawlJob {
            url_patterns: Some(vec!["/blog/".to_string()]),
            ..CrawlJob::default()
        };
        assert!(pattern_job.is_url_allowed("https://ex.com/blog/post-1", 0, "ex.com"));
        assert!(!pattern_job.is_url_allowed("https://ex.com/about", 0, "ex.com"));
    }

    #[test]
    fn crawl_job_serde_defaults() {
        let job = CrawlJob::default();

        // Round-trip preserves the payload exactly.
        let value = serde_json::to_value(&job).unwrap();
        let roundtrip: CrawlJob = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(value, serde_json::to_value(&roundtrip).unwrap());

        // An older payload lacking the #[serde(default)] field
        // (default_license) still deserializes.
        let mut legacy = value;
        let _ = legacy
            .as_object_mut()
            .expect("CrawlJob serializes to an object")
            .remove("default_license");
        let old_job: CrawlJob = serde_json::from_value(legacy).unwrap();
        assert_eq!(old_job.default_license, None);
        assert_eq!(old_job.name, job.name);
        assert_eq!(old_job.status, JobStatus::Pending);
        assert!(old_job.respect_robots_txt);
    }
}
