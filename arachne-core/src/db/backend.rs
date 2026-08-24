//! Repository facade: dispatches to the configured storage backend.
//!
//! `ArachneRepo` keeps its historical name and method set so every call site
//! (coordinator, CLI, adapters) compiles unchanged; the backend enum selects
//! PostgreSQL (default) or legacy ScyllaDB at construction time.

use anyhow::Result;
use uuid::Uuid;

use crate::config::{ArachneConfig, DbBackend};
use crate::models::{
    CrawlJob, CrawlResult, JobStatus, TrackRecord,
};

use super::postgres::PostgresRepo;
pub use super::repo::{CrawledPageRecord, DomainMetadata};
use super::repo::ScyllaRepo;

pub enum ArachneRepo {
    Postgres(Box<PostgresRepo>),
    Scylla(Box<ScyllaRepo>),
}

impl ArachneRepo {
    /// Construct the configured backend and ensure schema.
    pub async fn new(config: &ArachneConfig) -> Result<Self> {
        match config.database.backend {
            DbBackend::Postgres => Ok(Self::Postgres(Box::new(PostgresRepo::new(&config.database).await?))),
            DbBackend::Scylla => Ok(Self::Scylla(Box::new(ScyllaRepo::new(&config.scylla).await?))),
        }
    }

    pub async fn insert_crawl_result(&self, domain: &str, result: &CrawlResult) -> Result<()> {
        match self {
            Self::Postgres(r) => r.insert_crawl_result(domain, result).await,
            Self::Scylla(r) => r.insert_crawl_result(domain, result).await,
        }
    }

    pub async fn insert_crawl_results_batch(
        &self,
        results: &[(String, CrawlResult)],
    ) -> Result<()> {
        match self {
            Self::Postgres(r) => r.insert_crawl_results_batch(results).await,
            Self::Scylla(r) => r.insert_crawl_results_batch(results).await,
        }
    }

    pub async fn check_url_exists(&self, domain: &str, job_id: Uuid, url: &str) -> Result<bool> {
        match self {
            Self::Postgres(r) => r.check_url_exists(domain, job_id, url).await,
            Self::Scylla(r) => r.check_url_exists(domain, job_id, url).await,
        }
    }

    pub async fn check_urls_batch(
        &self,
        urls: Vec<(String, Uuid, String)>,
    ) -> Result<std::collections::HashSet<String>> {
        match self {
            Self::Postgres(r) => r.check_urls_batch(urls).await,
            Self::Scylla(r) => r.check_urls_batch(urls).await,
        }
    }

    pub async fn insert_job(&self, job: &CrawlJob) -> Result<()> {
        match self {
            Self::Postgres(r) => r.insert_job(job).await,
            Self::Scylla(r) => r.insert_job(job).await,
        }
    }

    pub async fn update_job_status(&self, job_id: &Uuid, status: &JobStatus) -> Result<()> {
        match self {
            Self::Postgres(r) => r.update_job_status(job_id, status).await,
            Self::Scylla(r) => r.update_job_status(job_id, status).await,
        }
    }

    pub async fn get_job(&self, job_id: &Uuid) -> Result<Option<CrawlJob>> {
        match self {
            Self::Postgres(r) => r.get_job(job_id).await,
            Self::Scylla(r) => r.get_job(job_id).await,
        }
    }

    pub async fn list_jobs(&self) -> Result<Vec<CrawlJob>> {
        match self {
            Self::Postgres(r) => r.list_jobs().await,
            Self::Scylla(r) => r.list_jobs().await,
        }
    }

    /// Domain metadata (robots cache state). Postgres returns raw rows;
    /// Scylla converts to the typed record.
    pub async fn get_domain_metadata_raw(
        &self,
        domain: &str,
    ) -> Result<Option<(Option<String>, Option<i64>, Option<i32>, Option<i64>)>> {
        match self {
            Self::Postgres(r) => Ok(r
                .get_domain_metadata(domain)
                .await?
                .map(|m| (m.robots_txt, m.robots_fetched_at, m.crawl_delay_ms, m.last_crawled_at))),
            Self::Scylla(r) => Ok(r
                .get_domain_metadata(domain)
                .await?
                .map(|m| (m.robots_txt, m.robots_fetched_at.map(|t| t.timestamp_millis()), m.crawl_delay_ms, m.last_crawled_at.map(|t| t.timestamp_millis())))),
        }
    }

    /// Pages recorded for a domain (CLI export path).
    pub async fn get_pages_by_domain(&self, domain: &str) -> Result<Vec<CrawledPageRecord>> {
        /// (domain, job_id, url, status, len, hash, title, lang, ref, ts)
        type PageTuple = (
            String,
            Uuid,
            String,
            i32,
            Option<i32>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<i64>,
        );
        match self {
            Self::Postgres(r) => {
                let rows: Vec<PageTuple> = sqlx::query_as(
                    "SELECT domain, job_id, url, http_status, content_length, content_hash, \
                     title, language, content_ref, crawled_at \
                     FROM crawled_pages WHERE domain = $1",
                )
                .bind(domain)
                .fetch_all(r.pool())
                .await?;
                Ok(rows
                    .into_iter()
                    .map(|(domain, job_id, url, http_status, content_length, content_hash, title, language, content_ref, crawled_at)| CrawledPageRecord {
                        domain,
                        job_id,
                        url,
                        http_status,
                        content_length,
                        content_hash,
                        title,
                        language,
                        content_ref,
                        crawled_at: crawled_at.and_then(chrono::DateTime::from_timestamp_millis),
                    })
                    .collect())
            }
            Self::Scylla(r) => r.get_pages_by_domain(domain).await,
        }
    }

    /// Insert or update a track row (full upsert by (source, source_id)).
    pub async fn upsert_track(&self, t: &TrackRecord) -> Result<()> {
        match self {
            Self::Postgres(r) => r.upsert_track(t).await,
            Self::Scylla(r) => r.upsert_track(t).await,
        }
    }

    /// Manifest-first admission: false when the row already existed.
    pub async fn insert_track_if_absent(&self, t: &TrackRecord) -> Result<bool> {
        match self {
            Self::Postgres(r) => r.insert_track_if_absent(t).await,
            Self::Scylla(r) => r.insert_track_if_absent(t).await,
        }
    }

    /// All tracks for a source regardless of state (for exports).
    pub async fn list_tracks_by_source(
        &self,
        source: &str,
        limit: i64,
    ) -> Result<Vec<TrackRecord>> {
        match self {
            Self::Postgres(r) => r.list_tracks_by_source(source, limit).await,
            // Scylla's prepared statement takes i32 for LIMIT.
            Self::Scylla(r) => r.list_tracks_by_source(source, limit as i32).await,
        }
    }
}
