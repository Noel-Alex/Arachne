//! ScyllaDB repository implementation.

use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::stream::{FuturesUnordered, StreamExt};
use scylla::batch::{Batch, BatchType};
use scylla::{prepared_statement::PreparedStatement, Session, SessionBuilder};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use uuid::Uuid;

use crate::config::ScyllaConfig;
use crate::models::{CrawlJob, CrawlResult, JobStatus, TrackRecord, TrackStatus};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainMetadata {
    pub domain: String,
    pub robots_txt: Option<String>,
    pub robots_fetched_at: Option<DateTime<Utc>>,
    pub crawl_delay_ms: Option<i32>,
    pub last_crawled_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrawledPageRecord {
    pub domain: String,
    pub job_id: Uuid,
    pub url: String,
    pub http_status: i32,
    pub content_length: Option<i32>,
    pub content_hash: Option<String>,
    pub title: Option<String>,
    pub language: Option<String>,
    pub content_ref: Option<String>,
    pub crawled_at: Option<DateTime<Utc>>,
}

/// Arachne Repository for ScyllaDB access.
pub struct ArachneRepo {
    session: Session,
    insert_crawl_result_stmt: PreparedStatement,
    check_url_exists_stmt: PreparedStatement,
    insert_job_stmt: PreparedStatement,
    update_job_status_stmt: PreparedStatement,
    get_job_stmt: PreparedStatement,
    list_jobs_stmt: PreparedStatement,
    save_domain_metadata_stmt: PreparedStatement,
    get_domain_metadata_stmt: PreparedStatement,
    get_pages_by_domain_stmt: PreparedStatement,
    get_pending_tracks_stmt: PreparedStatement,
    get_tracks_by_source_stmt: PreparedStatement,
}

impl ArachneRepo {
    /// Create a new repository and prepare statements.
    pub async fn new(config: &ScyllaConfig) -> Result<Self> {
        let session = SessionBuilder::new()
            .known_node(&config.uri)
            .build()
            .await?;

        // Initialize schema if needed
        crate::db::schema::setup_schema(&session).await?;
        session.use_keyspace(&config.keyspace, false).await?;

        // Prepare statements
        let insert_crawl_result_stmt = session
            .prepare("INSERT INTO crawled_pages (domain, job_id, url, http_status, content_length, content_hash, title, language, content_ref, crawled_at, crawl_duration_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .await?;

        let check_url_exists_stmt = session
            .prepare("SELECT url FROM crawled_pages WHERE domain = ? AND job_id = ? AND url = ?")
            .await?;

        let insert_job_stmt = session
            .prepare("INSERT INTO crawl_jobs (job_id, name, status, config, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)")
            .await?;

        let update_job_status_stmt = session
            .prepare("UPDATE crawl_jobs SET status = ?, updated_at = ? WHERE job_id = ?")
            .await?;

        let get_job_stmt = session
            .prepare("SELECT config FROM crawl_jobs WHERE job_id = ?")
            .await?;

        let list_jobs_stmt = session.prepare("SELECT config FROM crawl_jobs").await?;

        let save_domain_metadata_stmt = session
            .prepare("INSERT INTO domain_metadata (domain, robots_txt, robots_fetched_at, crawl_delay_ms) VALUES (?, ?, ?, ?)")
            .await?;

        let get_domain_metadata_stmt = session
            .prepare("SELECT domain, robots_txt, robots_fetched_at, crawl_delay_ms, last_crawled_at FROM domain_metadata WHERE domain = ?")
            .await?;

        let get_pages_by_domain_stmt = session
            .prepare("SELECT domain, job_id, url, http_status, content_length, content_hash, title, language, content_ref, crawled_at FROM crawled_pages WHERE domain = ?")
            .await?;

        // Crash-recovery claim: pending rows, or downloading rows whose lease expired.
        let get_pending_tracks_stmt = session
            .prepare("SELECT source, source_id, job_id, url, title, artist, album, year, genre, license, collection, status, error FROM tracks WHERE source = ? AND (status = 'pending' OR (status = 'downloading' AND leased_until < ?)) PER PARTITION LIMIT ?")
            .await?;

        let get_tracks_by_source_stmt = session
            .prepare("SELECT source, source_id, job_id, url, title, artist, album, year, genre, license, collection, duration_secs, bitrate_kbps, format, sha256, bytes, object_path, status, error FROM tracks WHERE source = ? PER PARTITION LIMIT ?")
            .await?;

        Ok(Self {
            session,
            insert_crawl_result_stmt,
            check_url_exists_stmt,
            insert_job_stmt,
            update_job_status_stmt,
            get_job_stmt,
            list_jobs_stmt,
            save_domain_metadata_stmt,
            get_domain_metadata_stmt,
            get_pages_by_domain_stmt,
            get_pending_tracks_stmt,
            get_tracks_by_source_stmt,
        })
    }

    pub async fn insert_crawl_result(&self, domain: &str, result: &CrawlResult) -> Result<()> {
        let timestamp = chrono::Utc::now().timestamp_millis();
        self.session
            .execute(
                &self.insert_crawl_result_stmt,
                (
                    domain,
                    result.job_id,
                    &result.source_url,
                    result.status.as_i32(),
                    result.content_length.map(|l| l as i32),
                    result.content_hash.as_deref(),
                    result.title.as_deref(),
                    result.language.as_deref(),
                    result.content_ref.as_deref(),
                    timestamp,
                    result.crawl_duration_ms as i32,
                ),
            )
            .await?;
        Ok(())
    }

    /// High-throughput ScyllaDB Unlogged Batch insertion (shard-aware, single round-trip).
    pub async fn insert_crawl_results_batch(
        &self,
        results: &[(String, CrawlResult)],
    ) -> Result<()> {
        if results.is_empty() {
            return Ok(());
        }

        let mut batch = Batch::new(BatchType::Unlogged);
        let mut batch_values = Vec::with_capacity(results.len());
        let timestamp = chrono::Utc::now().timestamp_millis();

        for (domain, result) in results {
            batch.append_statement(self.insert_crawl_result_stmt.clone());
            batch_values.push((
                domain.clone(),
                result.job_id,
                result.source_url.clone(),
                result.status.as_i32(),
                result.content_length.map(|l| l as i32),
                result.content_hash.clone(),
                result.title.clone(),
                result.language.clone(),
                result.content_ref.clone(),
                timestamp,
                result.crawl_duration_ms as i32,
            ));
        }

        self.session.batch(&batch, batch_values).await?;
        Ok(())
    }

    pub async fn check_url_exists(&self, domain: &str, job_id: Uuid, url: &str) -> Result<bool> {
        let rows = self
            .session
            .execute(&self.check_url_exists_stmt, (domain, job_id, url))
            .await?
            .rows_or_empty();
        Ok(!rows.is_empty())
    }

    pub async fn check_urls_batch(
        &self,
        urls: Vec<(String, Uuid, String)>,
    ) -> Result<HashSet<String>> {
        let mut futures = FuturesUnordered::new();
        for (domain, job_id, url) in urls {
            let stmt = &self.check_url_exists_stmt;
            let session = &self.session;
            futures.push(async move {
                let exists = !session
                    .execute(stmt, (&domain, job_id, &url))
                    .await?
                    .rows_or_empty()
                    .is_empty();
                if exists {
                    Ok::<_, anyhow::Error>(Some(url))
                } else {
                    Ok::<_, anyhow::Error>(None)
                }
            });
        }

        let mut existing = HashSet::new();
        while let Some(res) = futures.next().await {
            if let Ok(Some(url)) = res {
                existing.insert(url);
            }
        }
        Ok(existing)
    }

    pub async fn insert_job(&self, job: &CrawlJob) -> Result<()> {
        let config_str = serde_json::to_string(job)?;
        let ts = chrono::Utc::now().timestamp_millis();
        self.session
            .execute(
                &self.insert_job_stmt,
                (
                    job.id,
                    &job.name,
                    format!("{:?}", job.status),
                    config_str,
                    ts,
                    ts,
                ),
            )
            .await?;
        Ok(())
    }

    pub async fn update_job_status(&self, job_id: &Uuid, status: &JobStatus) -> Result<()> {
        let ts = chrono::Utc::now().timestamp_millis();
        self.session
            .execute(
                &self.update_job_status_stmt,
                (format!("{:?}", status), ts, job_id),
            )
            .await?;
        Ok(())
    }

    pub async fn get_job(&self, job_id: &Uuid) -> Result<Option<CrawlJob>> {
        if let Some(row) = self
            .session
            .execute(&self.get_job_stmt, (job_id,))
            .await?
            .rows_or_empty()
            .into_iter()
            .next()
        {
            let (config,): (String,) = row.into_typed()?;
            let job: CrawlJob = serde_json::from_str(&config)?;
            Ok(Some(job))
        } else {
            Ok(None)
        }
    }

    pub async fn list_jobs(&self) -> Result<Vec<CrawlJob>> {
        let mut jobs = Vec::new();
        let rows = self
            .session
            .execute(&self.list_jobs_stmt, &[])
            .await?
            .rows_or_empty();
        for row in rows {
            let (config,): (String,) = row.into_typed()?;
            if let Ok(job) = serde_json::from_str(&config) {
                jobs.push(job);
            }
        }
        Ok(jobs)
    }

    pub async fn save_domain_metadata(
        &self,
        domain: &str,
        robots_txt: Option<&str>,
        crawl_delay_ms: Option<i32>,
    ) -> Result<()> {
        let ts = chrono::Utc::now().timestamp_millis();
        self.session
            .execute(
                &self.save_domain_metadata_stmt,
                (domain, robots_txt, ts, crawl_delay_ms),
            )
            .await?;
        Ok(())
    }

    pub async fn get_domain_metadata(&self, domain: &str) -> Result<Option<DomainMetadata>> {
        if let Some(row) = self
            .session
            .execute(&self.get_domain_metadata_stmt, (domain,))
            .await?
            .rows_or_empty()
            .into_iter()
            .next()
        {
            let (d, r, r_ts, d_ms, l_ts): (
                String,
                Option<String>,
                Option<i64>,
                Option<i32>,
                Option<i64>,
            ) = row.into_typed()?;
            let meta = DomainMetadata {
                domain: d,
                robots_txt: r,
                robots_fetched_at: r_ts.map(|ts| DateTime::from_timestamp_millis(ts).unwrap()),
                crawl_delay_ms: d_ms,
                last_crawled_at: l_ts.map(|ts| DateTime::from_timestamp_millis(ts).unwrap()),
            };
            Ok(Some(meta))
        } else {
            Ok(None)
        }
    }

    pub async fn get_pages_by_domain(&self, domain: &str) -> Result<Vec<CrawledPageRecord>> {
        let mut records = Vec::new();
        let rows = self
            .session
            .execute(&self.get_pages_by_domain_stmt, (domain,))
            .await?
            .rows_or_empty();
        for row in rows {
            // (domain, job_id, url, http_status, content_length, content_hash, title, language, content_ref, crawled_at_ms)
            type PageRow = (
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
            let (d, j, u, s, l, h, t, lang, r, c_ts): PageRow = row.into_typed()?;

            records.push(CrawledPageRecord {
                domain: d,
                job_id: j,
                url: u,
                http_status: s,
                content_length: l,
                content_hash: h,
                title: t,
                language: lang,
                content_ref: r,
                crawled_at: c_ts.map(|ts| DateTime::from_timestamp_millis(ts).unwrap()),
            });
        }
        Ok(records)
    }

    // ---- Track manifest (Sivana handoff) ----

    /// Insert or update a track row (full upsert by (source, source_id)).
    /// Split into two statements because tuple serialization caps at 16 values.
    pub async fn upsert_track(&self, t: &TrackRecord) -> Result<()> {
        let now = Utc::now().timestamp_millis();
        self.session
            .query(
                "INSERT INTO tracks (source, source_id, job_id, url, title, artist, album, year, genre, license, collection) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    &t.source,
                    &t.source_id,
                    t.job_id,
                    &t.url,
                    &t.title,
                    &t.artist,
                    &t.album,
                    &t.year,
                    &t.genre,
                    &t.license,
                    &t.collection,
                ),
            )
            .await?;

        self.session
            .query(
                "UPDATE tracks SET duration_secs = ?, bitrate_kbps = ?, format = ?, sha256 = ?, bytes = ?, object_path = ?, status = ?, error = ?, updated_at = ? WHERE source = ? AND source_id = ?",
                (
                    &t.duration_secs,
                    &t.bitrate_kbps,
                    &t.format,
                    &t.sha256,
                    &t.bytes,
                    &t.object_path,
                    t.status.as_str(),
                    &t.error,
                    now,
                    &t.source,
                    &t.source_id,
                ),
            )
            .await?;
        Ok(())
    }

    /// Claim pending/expired-lease tracks for download. `lease_ms` is how long
    /// the claimant holds them before another pass may reclaim.
    pub async fn claim_pending_tracks(
        &self,
        source: &str,
        limit: i32,
        lease_ms: i64,
    ) -> Result<Vec<TrackRecord>> {
        let now = Utc::now().timestamp_millis();
        let rows = self
            .session
            .execute(&self.get_pending_tracks_stmt, (source, now, limit))
            .await?
            .rows_or_empty();

        let mut claimed = Vec::new();
        let lease_until = now + lease_ms;
        for row in rows {
            type PendingRow = (
                String,
                String,
                Uuid,
                String,
                Option<String>,
                Option<String>,
                Option<String>,
                Option<i32>,
                Option<String>,
                String,
                Option<String>,
                String,
                Option<String>,
            );
            let (source, source_id, job_id, url, title, artist, album, year, genre, license, collection, status_s, error): PendingRow =
                row.into_typed()?;

            // Take the lease before returning it to the caller.
            self.session
                .query(
                    "UPDATE tracks SET status = 'downloading', leased_until = ?, updated_at = ? WHERE source = ? AND source_id = ?",
                    (lease_until, Utc::now().timestamp_millis(), &source, &source_id),
                )
                .await?;

            claimed.push(TrackRecord {
                source,
                source_id,
                job_id,
                url,
                title,
                artist,
                album,
                year,
                genre,
                license,
                collection,
                duration_secs: None,
                bitrate_kbps: None,
                format: None,
                sha256: None,
                bytes: None,
                object_path: None,
                status: TrackStatus::Downloading,
                error,
            });
            let _ = status_s;
        }
        Ok(claimed)
    }

    /// All tracks for a source regardless of state (for exports).
    pub async fn list_tracks_by_source(&self, source: &str, limit: i32) -> Result<Vec<TrackRecord>> {
        let rows = self
            .session
            .execute(&self.get_tracks_by_source_stmt, (source, limit))
            .await?
            .rows_or_empty();

        // Derived row type: tuple serialization caps at 16 values, this query has 19 columns.
        #[derive(scylla::macros::FromRow)]
        #[scylla_crate = "scylla"]
        struct TrackRow {
            source: String,
            source_id: String,
            job_id: Uuid,
            url: String,
            title: Option<String>,
            artist: Option<String>,
            album: Option<String>,
            year: Option<i32>,
            genre: Option<String>,
            license: String,
            collection: Option<String>,
            duration_secs: Option<f64>,
            bitrate_kbps: Option<i32>,
            format: Option<String>,
            sha256: Option<String>,
            bytes: Option<i64>,
            object_path: Option<String>,
            status: String,
            error: Option<String>,
        }

        let mut tracks = Vec::new();
        for row in rows {
            let r: TrackRow = row.into_typed()?;
            tracks.push(TrackRecord {
                source: r.source,
                source_id: r.source_id,
                job_id: r.job_id,
                url: r.url,
                title: r.title,
                artist: r.artist,
                album: r.album,
                year: r.year,
                genre: r.genre,
                license: r.license,
                collection: r.collection,
                duration_secs: r.duration_secs,
                bitrate_kbps: r.bitrate_kbps,
                format: r.format,
                sha256: r.sha256,
                bytes: r.bytes,
                object_path: r.object_path,
                status: TrackStatus::parse(&r.status),
                error: r.error,
            });
        }
        Ok(tracks)
    }
}
