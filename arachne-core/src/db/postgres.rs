//! PostgreSQL repository backend.
//!
//! The audit verdict (2026-08): single-node Scylla reserved 2-4GB RAM + pinned
//! cores for a workload that is fundamentally relational metadata (jobs,
//! page results keyed by URL, a track manifest queried by source, batched
//! existence checks). Postgres handles this comfortably on the same laptop
//! and gives Sivana ad-hoc SQL for free. Every Scylla-specific workaround
//! (LWT, unlogged batches, ALLOW FILTERING, 16-value tuple splits) collapses
//! to plain SQL here.

use anyhow::{Context, Result};
use sqlx::postgres::{PgPool, PgPoolOptions, PgQueryResult};
use uuid::Uuid;

use crate::config::DatabaseConfig;
use crate::models::{CrawlJob, CrawlResult, JobStatus, TrackRecord, TrackStatus};

/// Raw domain_metadata row (timestamps as epoch millis, matching the Scylla
/// backend's storage convention).
pub struct DomainMetadataRow {
    pub domain: String,
    pub robots_txt: Option<String>,
    pub robots_fetched_at: Option<i64>,
    pub crawl_delay_ms: Option<i32>,
    pub last_crawled_at: Option<i64>,
}

pub struct PostgresRepo {
    pool: PgPool,
}

/// Row shape shared by pending-claim and stale-lease queries.
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
    license_url: Option<String>,
    origin_page_url: Option<String>,
    discovered_from_url: Option<String>,
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

const TRACK_COLUMNS: &str = "source, source_id, job_id, url, title, artist, album, year, genre, \
license, license_url, origin_page_url, discovered_from_url, collection, \
duration_secs, bitrate_kbps, format, sha256, bytes, object_path, status, error";

/// Hot admission read: ONE round trip for the whole coordinator batch. The
/// batch arrives as three parallel arrays, unnested into rows and joined
/// against crawled_pages — replaces N point SELECTs inside a transaction.
const CHECK_URLS_BATCH_SQL: &str = r#"
SELECT c.url
FROM crawled_pages c
JOIN unnest($1::text[], $2::uuid[], $3::text[])
     AS q(domain, job_id, url)
ON c.domain = q.domain AND c.job_id = q.job_id AND c.url = q.url"#;

/// High-throughput batch insert: ONE statement, ONE round trip. Parallel
/// arrays are unnested into rows and inserted with the same conflict target
/// as the single-row path — idempotent under re-harvests.
const INSERT_CRAWL_RESULTS_BATCH_SQL: &str = r#"
INSERT INTO crawled_pages
    (domain, job_id, url, http_status, content_length, content_hash,
     title, language, content_ref, crawled_at, crawl_duration_ms)
SELECT q.domain, q.job_id, q.url, q.http_status, q.content_length, q.content_hash,
       q.title, q.language, q.content_ref, $11::bigint, q.crawl_duration_ms
FROM unnest(
        $1::text[], $2::uuid[], $3::text[], $4::int[], $5::int[],
        $6::text[], $7::text[], $8::text[], $9::text[], $10::int[]
     ) AS q(domain, job_id, url, http_status, content_length,
            content_hash, title, language, content_ref, crawl_duration_ms)
ON CONFLICT (domain, job_id, url) DO NOTHING"#;

impl PostgresRepo {
    /// Connect, run migrations, and return the repo.
    pub async fn new(config: &DatabaseConfig) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .connect(&config.url)
            .await
            .with_context(|| format!("failed to connect to postgres at {}", config.url))?;
        Self::with_pool(pool).await
    }

    pub async fn with_pool(pool: PgPool) -> Result<Self> {
        let repo = Self { pool };
        repo.migrate().await?;
        Ok(repo)
    }

    /// Underlying pool (for facade queries that need bespoke row mapping).
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn migrate(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS crawl_jobs (
                job_id UUID PRIMARY KEY,
                name TEXT NOT NULL,
                status TEXT NOT NULL,
                config JSONB NOT NULL,
                created_at BIGINT NOT NULL,
                updated_at BIGINT NOT NULL
            )"#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS crawled_pages (
                domain TEXT NOT NULL,
                job_id UUID NOT NULL,
                url TEXT NOT NULL,
                http_status INT NOT NULL,
                content_length INT,
                content_hash TEXT,
                title TEXT,
                language TEXT,
                content_ref TEXT,
                crawled_at BIGINT NOT NULL,
                crawl_duration_ms INT NOT NULL,
                PRIMARY KEY (domain, job_id, url)
            )"#,
        )
        .execute(&self.pool)
        .await?;
        // Existence checks are THE hot admission read; index job-scoped lookups.
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_crawled_pages_url ON crawled_pages (url)")
            .execute(&self.pool)
            .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS domain_metadata (
                domain TEXT PRIMARY KEY,
                robots_txt TEXT,
                robots_fetched_at BIGINT,
                crawl_delay_ms INT,
                last_crawled_at BIGINT
            )"#,
        )
        .execute(&self.pool)
        .await?;

        // The Sivana handoff manifest. status: pending|downloading|done|rejected|failed.
        // leased_until: crash-recovery lease timestamp (ms epoch).
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS tracks (
                source TEXT NOT NULL,
                source_id TEXT NOT NULL,
                job_id UUID NOT NULL,
                url TEXT NOT NULL,
                title TEXT,
                artist TEXT,
                album TEXT,
                year INT,
                genre TEXT,
                license TEXT NOT NULL,
                license_url TEXT,
                origin_page_url TEXT,
                discovered_from_url TEXT,
                collection TEXT,
                duration_secs DOUBLE PRECISION,
                bitrate_kbps INT,
                format TEXT,
                sha256 TEXT,
                bytes BIGINT,
                object_path TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                error TEXT,
                leased_until BIGINT,
                updated_at BIGINT NOT NULL DEFAULT 0,
                PRIMARY KEY (source, source_id)
            )"#,
        )
        .execute(&self.pool)
        .await?;
        // Claim queries filter by status within a source partition.
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_tracks_source_status ON tracks (source, status)",
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    fn row_to_track(r: TrackRow) -> TrackRecord {
        TrackRecord {
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
            license_url: r.license_url,
            origin_page_url: r.origin_page_url,
            discovered_from_url: r.discovered_from_url,
            collection: r.collection,
            duration_secs: r.duration_secs,
            bitrate_kbps: r.bitrate_kbps,
            format: r.format,
            sha256: r.sha256,
            bytes: r.bytes,
            object_path: r.object_path,
            status: TrackStatus::parse(&r.status),
            error: r.error,
        }
    }

    // ---- crawled_pages ----

    pub async fn insert_crawl_result(&self, domain: &str, result: &CrawlResult) -> Result<()> {
        let ts = chrono::Utc::now().timestamp_millis();
        sqlx::query(
            r#"
            INSERT INTO crawled_pages
                (domain, job_id, url, http_status, content_length, content_hash,
                 title, language, content_ref, crawled_at, crawl_duration_ms)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            ON CONFLICT (domain, job_id, url) DO UPDATE SET
                http_status = EXCLUDED.http_status,
                content_length = EXCLUDED.content_length,
                content_hash = EXCLUDED.content_hash,
                title = EXCLUDED.title,
                language = EXCLUDED.language,
                content_ref = EXCLUDED.content_ref,
                crawled_at = EXCLUDED.crawled_at,
                crawl_duration_ms = EXCLUDED.crawl_duration_ms"#,
        )
        .bind(domain)
        .bind(result.job_id)
        .bind(&result.source_url)
        .bind(result.status.as_i32())
        .bind(result.content_length.map(|l| l as i32))
        .bind(&result.content_hash)
        .bind(&result.title)
        .bind(&result.language)
        .bind(&result.content_ref)
        .bind(ts)
        .bind(result.crawl_duration_ms as i32)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// High-throughput batch insert. ONE multi-row statement (unnest over
    /// parallel arrays) — single round trip, atomic without an explicit
    /// transaction, and far faster than row-at-a-time.
    pub async fn insert_crawl_results_batch(
        &self,
        results: &[(String, CrawlResult)],
    ) -> Result<()> {
        if results.is_empty() {
            return Ok(());
        }
        let ts = chrono::Utc::now().timestamp_millis();
        let mut domains: Vec<&str> = Vec::with_capacity(results.len());
        let mut job_ids: Vec<Uuid> = Vec::with_capacity(results.len());
        let mut urls: Vec<&str> = Vec::with_capacity(results.len());
        let mut statuses: Vec<i32> = Vec::with_capacity(results.len());
        let mut content_lengths: Vec<Option<i32>> = Vec::with_capacity(results.len());
        let mut content_hashes: Vec<Option<&str>> = Vec::with_capacity(results.len());
        let mut titles: Vec<Option<&str>> = Vec::with_capacity(results.len());
        let mut languages: Vec<Option<&str>> = Vec::with_capacity(results.len());
        let mut content_refs: Vec<Option<&str>> = Vec::with_capacity(results.len());
        let mut durations: Vec<i32> = Vec::with_capacity(results.len());

        for (domain, result) in results {
            domains.push(domain);
            job_ids.push(result.job_id);
            urls.push(&result.source_url);
            statuses.push(result.status.as_i32());
            content_lengths.push(result.content_length.map(|l| l as i32));
            content_hashes.push(result.content_hash.as_deref());
            titles.push(result.title.as_deref());
            languages.push(result.language.as_deref());
            content_refs.push(result.content_ref.as_deref());
            durations.push(result.crawl_duration_ms as i32);
        }

        sqlx::query(INSERT_CRAWL_RESULTS_BATCH_SQL)
            .bind(&domains)
            .bind(&job_ids)
            .bind(&urls)
            .bind(&statuses)
            .bind(&content_lengths)
            .bind(&content_hashes)
            .bind(&titles)
            .bind(&languages)
            .bind(&content_refs)
            .bind(&durations)
            .bind(ts)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Batch existence check for admission control — THE hottest read.
    /// Single-round-trip semantics: the whole coordinator batch (up to
    /// batch_size URLs) is one unnest-join statement, so no pool connection
    /// is pinned for N sequential point SELECTs. A single statement runs in
    /// one atomic snapshot; no explicit transaction needed.
    pub async fn check_urls_batch(
        &self,
        urls: Vec<(String, Uuid, String)>,
    ) -> Result<std::collections::HashSet<String>> {
        let mut existing: std::collections::HashSet<String> = std::collections::HashSet::new();
        if urls.is_empty() {
            return Ok(existing);
        }
        let mut domains: Vec<&str> = Vec::with_capacity(urls.len());
        let mut job_ids: Vec<Uuid> = Vec::with_capacity(urls.len());
        let mut page_urls: Vec<&str> = Vec::with_capacity(urls.len());
        for (domain, job_id, url) in &urls {
            domains.push(domain.as_str());
            job_ids.push(*job_id);
            page_urls.push(url.as_str());
        }
        let rows: Vec<(String,)> = sqlx::query_as(CHECK_URLS_BATCH_SQL)
            .bind(&domains)
            .bind(&job_ids)
            .bind(&page_urls)
            .fetch_all(&self.pool)
            .await?;
        existing.extend(rows.into_iter().map(|(url,)| url));
        Ok(existing)
    }

    pub async fn check_url_exists(&self, domain: &str, job_id: Uuid, url: &str) -> Result<bool> {
        let hit: Option<(String,)> = sqlx::query_as(
            "SELECT url FROM crawled_pages WHERE domain=$1 AND job_id=$2 AND url=$3",
        )
        .bind(domain)
        .bind(job_id)
        .bind(url)
        .fetch_optional(&self.pool)
        .await?;
        Ok(hit.is_some())
    }

    // ---- crawl_jobs ----

    pub async fn insert_job(&self, job: &CrawlJob) -> Result<()> {
        let config_str = serde_json::to_string(job)?;
        let ts = chrono::Utc::now().timestamp_millis();
        sqlx::query(
            r#"
            INSERT INTO crawl_jobs (job_id, name, status, config, created_at, updated_at)
            VALUES ($1,$2,$3,$4,$5,$5)
            ON CONFLICT (job_id) DO UPDATE SET
                status = EXCLUDED.status, config = EXCLUDED.config, updated_at = EXCLUDED.updated_at"#,
        )
        .bind(job.id)
        .bind(&job.name)
        .bind(format!("{:?}", job.status))
        .bind(config_str)
        .bind(ts)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_job_status(&self, job_id: &Uuid, status: &JobStatus) -> Result<()> {
        let ts = chrono::Utc::now().timestamp_millis();
        sqlx::query("UPDATE crawl_jobs SET status=$1, updated_at=$2 WHERE job_id=$3")
            .bind(format!("{:?}", status))
            .bind(ts)
            .bind(job_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn get_job(&self, job_id: &Uuid) -> Result<Option<CrawlJob>> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT config FROM crawl_jobs WHERE job_id=$1")
                .bind(job_id)
                .fetch_optional(&self.pool)
                .await?;
        match row {
            Some((config,)) => Ok(Some(serde_json::from_str(&config)?)),
            None => Ok(None),
        }
    }

    pub async fn list_jobs(&self) -> Result<Vec<CrawlJob>> {
        let rows: Vec<(String,)> = sqlx::query_as("SELECT config FROM crawl_jobs")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .filter_map(|(c,)| serde_json::from_str(&c).ok())
            .collect())
    }

    // ---- domain_metadata ----

    pub async fn save_domain_metadata(
        &self,
        domain: &str,
        robots_txt: Option<&str>,
        crawl_delay_ms: Option<i32>,
    ) -> Result<()> {
        let ts = chrono::Utc::now().timestamp_millis();
        sqlx::query(
            r#"
            INSERT INTO domain_metadata (domain, robots_txt, robots_fetched_at, crawl_delay_ms)
            VALUES ($1,$2,$3,$4)
            ON CONFLICT (domain) DO UPDATE SET
                robots_txt = EXCLUDED.robots_txt,
                robots_fetched_at = EXCLUDED.robots_fetched_at,
                crawl_delay_ms = EXCLUDED.crawl_delay_ms"#,
        )
        .bind(domain)
        .bind(robots_txt)
        .bind(ts)
        .bind(crawl_delay_ms)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Domain metadata (robots cache). Returns None until a row exists.
    pub async fn get_domain_metadata(&self, domain: &str) -> Result<Option<DomainMetadataRow>> {
        /// (domain, robots_txt, robots_fetched_at, crawl_delay_ms, last_crawled_at)
        type DomainTuple = (
            String,
            Option<String>,
            Option<i64>,
            Option<i32>,
            Option<i64>,
        );
        let row: Option<DomainTuple> = sqlx::query_as(
            "SELECT domain, robots_txt, robots_fetched_at, crawl_delay_ms, last_crawled_at \
                 FROM domain_metadata WHERE domain = $1",
        )
        .bind(domain)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(
            |(domain, robots_txt, robots_fetched_at, crawl_delay_ms, last_crawled_at)| {
                DomainMetadataRow {
                    domain,
                    robots_txt,
                    robots_fetched_at,
                    crawl_delay_ms,
                    last_crawled_at,
                }
            },
        ))
    }

    // ---- tracks ----

    /// Manifest-first admission. Returns false when the row already exists.
    pub async fn insert_track_if_absent(&self, t: &TrackRecord) -> Result<bool> {
        let now = chrono::Utc::now().timestamp_millis();
        let res: PgQueryResult = sqlx::query(
            &format!(
                r#"
            INSERT INTO tracks ({TRACK_COLUMNS}, updated_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)
            ON CONFLICT (source, source_id) DO NOTHING"#
            ),
        )
        .bind(&t.source)
        .bind(&t.source_id)
        .bind(t.job_id)
        .bind(&t.url)
        .bind(&t.title)
        .bind(&t.artist)
        .bind(&t.album)
        .bind(t.year)
        .bind(&t.genre)
        .bind(&t.license)
        .bind(&t.license_url)
        .bind(&t.origin_page_url)
        .bind(&t.discovered_from_url)
        .bind(&t.collection)
        .bind(t.duration_secs)
        .bind(t.bitrate_kbps)
        .bind(&t.format)
        .bind(&t.sha256)
        .bind(t.bytes)
        .bind(&t.object_path)
        .bind(t.status.as_str())
        .bind(&t.error)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    /// Full upsert by (source, source_id).
    pub async fn upsert_track(&self, t: &TrackRecord) -> Result<()> {
        let now = chrono::Utc::now().timestamp_millis();
        sqlx::query(
            &format!(
                r#"
            INSERT INTO tracks ({TRACK_COLUMNS}, updated_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)
            ON CONFLICT (source, source_id) DO UPDATE SET
                title = COALESCE(EXCLUDED.title, tracks.title),
                artist = COALESCE(EXCLUDED.artist, tracks.artist),
                album = COALESCE(EXCLUDED.album, tracks.album),
                year = COALESCE(EXCLUDED.year, tracks.year),
                genre = COALESCE(EXCLUDED.genre, tracks.genre),
                duration_secs = EXCLUDED.duration_secs,
                bitrate_kbps = EXCLUDED.bitrate_kbps,
                format = COALESCE(EXCLUDED.format, tracks.format),
                sha256 = COALESCE(EXCLUDED.sha256, tracks.sha256),
                bytes = EXCLUDED.bytes,
                object_path = COALESCE(EXCLUDED.object_path, tracks.object_path),
                status = EXCLUDED.status,
                error = EXCLUDED.error,
                updated_at = EXCLUDED.updated_at"#
            ),
        )
        .bind(&t.source)
        .bind(&t.source_id)
        .bind(t.job_id)
        .bind(&t.url)
        .bind(&t.title)
        .bind(&t.artist)
        .bind(&t.album)
        .bind(t.year)
        .bind(&t.genre)
        .bind(&t.license)
        .bind(&t.license_url)
        .bind(&t.origin_page_url)
        .bind(&t.discovered_from_url)
        .bind(&t.collection)
        .bind(t.duration_secs)
        .bind(t.bitrate_kbps)
        .bind(&t.format)
        .bind(&t.sha256)
        .bind(t.bytes)
        .bind(&t.object_path)
        .bind(t.status.as_str())
        .bind(&t.error)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Claim pending/expired-lease tracks atomically via FOR UPDATE SKIP LOCKED
    /// — strictly safer than the Scylla SELECT-then-UPDATE dance.
    #[allow(dead_code)]
    pub async fn claim_pending_tracks(
        &self,
        source: &str,
        limit: i64,
        lease_ms: i64,
    ) -> Result<Vec<TrackRecord>> {
        let now = chrono::Utc::now().timestamp_millis();
        let lease_until = now + lease_ms;
        let rows: Vec<TrackRowSql> = sqlx::query_as::<_, TrackRowSql>(&format!(
            r#"
            WITH candidates AS (
                SELECT source, source_id FROM tracks
                WHERE source = $1
                  AND (status = 'pending' OR (status = 'downloading' AND leased_until < $2))
                ORDER BY CASE status WHEN 'pending' THEN 0 ELSE 1 END, source_id
                LIMIT $3
                FOR UPDATE SKIP LOCKED
            )
            UPDATE tracks t SET status='downloading', leased_until=$4, updated_at=$2
            FROM candidates c
            WHERE t.source = c.source AND t.source_id = c.source_id
            RETURNING t.{TRACK_COLUMNS}"#
        ))
        .bind(source)
        .bind(now)
        .bind(limit)
        .bind(lease_until)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| {
                let mut row: TrackRow = r.into();
                // The UPDATE above already set these; reflect them locally.
                row.status = "downloading".to_string();
                Self::row_to_track(row)
            })
            .collect())
    }

    /// All tracks for a source regardless of state (for exports).
    pub async fn list_tracks_by_source(
        &self,
        source: &str,
        limit: i64,
    ) -> Result<Vec<TrackRecord>> {
        let rows: Vec<TrackRow> = sqlx::query_as::<_, TrackRowSql>(&format!(
            "SELECT {TRACK_COLUMNS} FROM tracks WHERE source = $1 LIMIT $2"
        ))
        .bind(source)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|r: TrackRowSql| r.into())
        .collect();

        Ok(rows.into_iter().map(Self::row_to_track).collect())
    }
}

/// sqlx::FromRow mirror of [`TrackRow`].
#[derive(sqlx::FromRow)]
struct TrackRowSql {
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
    license_url: Option<String>,
    origin_page_url: Option<String>,
    discovered_from_url: Option<String>,
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

impl From<TrackRowSql> for TrackRow {
    fn from(s: TrackRowSql) -> Self {
        Self {
            source: s.source,
            source_id: s.source_id,
            job_id: s.job_id,
            url: s.url,
            title: s.title,
            artist: s.artist,
            album: s.album,
            year: s.year,
            genre: s.genre,
            license: s.license,
            license_url: s.license_url,
            origin_page_url: s.origin_page_url,
            discovered_from_url: s.discovered_from_url,
            collection: s.collection,
            duration_secs: s.duration_secs,
            bitrate_kbps: s.bitrate_kbps,
            format: s.format,
            sha256: s.sha256,
            bytes: s.bytes,
            object_path: s.object_path,
            status: s.status,
            error: s.error,
        }
    }
}

/// DB-less regression tripwires: pin the single-statement shape of the two
/// batched crawled_pages paths (live-DB behavior is covered by e2e_test).
#[cfg(test)]
mod postgres_batch_sql_tests {
    use super::{CHECK_URLS_BATCH_SQL, INSERT_CRAWL_RESULTS_BATCH_SQL};

    /// Admission read must stay a single unnest-join round trip, not regress
    /// to per-row SELECTs.
    #[test]
    fn check_urls_batch_is_single_unnest_join() {
        assert!(
            CHECK_URLS_BATCH_SQL.contains("unnest"),
            "check_urls_batch lost its unnest-array form"
        );
        assert!(
            CHECK_URLS_BATCH_SQL.contains("JOIN"),
            "check_urls_batch lost its join against crawled_pages"
        );
        for col in ["domain", "job_id", "url"] {
            assert!(
                CHECK_URLS_BATCH_SQL.contains(&format!("c.{col} = q.{col}")),
                "join predicate missing c.{col} = q.{col}"
            );
        }
        assert!(
            !CHECK_URLS_BATCH_SQL.to_uppercase().contains("INSERT"),
            "admission read must not mutate"
        );
    }

    /// Batch insert must stay one multi-row statement with the same conflict
    /// target as the single-row path.
    #[test]
    fn insert_crawl_results_batch_is_multirow_upsert_shape() {
        assert!(
            INSERT_CRAWL_RESULTS_BATCH_SQL.contains("unnest"),
            "insert_crawl_results_batch lost its unnest-array form"
        );
        assert!(
            INSERT_CRAWL_RESULTS_BATCH_SQL.contains("ON CONFLICT (domain, job_id, url) DO NOTHING"),
            "conflict clause drifted from ON CONFLICT (domain, job_id, url) DO NOTHING"
        );
        // All 11 columns present in the column list.
        for col in [
            "domain",
            "job_id",
            "url",
            "http_status",
            "content_length",
            "content_hash",
            "title",
            "language",
            "content_ref",
            "crawled_at",
            "crawl_duration_ms",
        ] {
            assert!(
                INSERT_CRAWL_RESULTS_BATCH_SQL.contains(col),
                "column list missing {col}"
            );
        }
        assert!(
            INSERT_CRAWL_RESULTS_BATCH_SQL.contains("$11::bigint"),
            "crawled_at must come from the scalar timestamp bind ($11)"
        );
    }
}
