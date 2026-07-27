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
}

impl CrawlJob {
    /// Evaluate whether a candidate URL and depth adhere to job rules.
    pub fn is_url_allowed(&self, candidate_url: &str, current_depth: u32, candidate_root_domain: &str) -> bool {
        // 1. Max depth check
        if let Some(max_depth) = self.max_depth {
            if current_depth > max_depth {
                return false;
            }
        }

        // 2. Allowed domains check
        if let Some(ref domains) = self.allowed_domains {
            if !domains.is_empty() && !domains.iter().any(|d| d.eq_ignore_ascii_case(candidate_root_domain)) {
                return false;
            }
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
        if let Some(ref includes) = self.url_patterns {
            if !includes.is_empty() && !includes.iter().any(|p| candidate_url.contains(p)) {
                return false;
            }
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
        }
    }

    /// Check if the status represents success.
    pub fn is_success(&self) -> bool {
        matches!(self, CrawlStatus::Success)
    }
}
