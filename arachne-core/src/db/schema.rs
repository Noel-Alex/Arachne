//! ScyllaDB schema definitions and setup.

use anyhow::Result;
use scylla::Session;

/// Create the keyspace and tables for Arachne.
pub async fn setup_schema(session: &Session) -> Result<()> {
    // Keyspace
    session
        .query(
            "CREATE KEYSPACE IF NOT EXISTS arachne WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } AND TABLETS = { 'enabled' : false }",
            &[],
        )
        .await?;

    // crawl_jobs
    session
        .query(
            "CREATE TABLE IF NOT EXISTS arachne.crawl_jobs (
                job_id uuid PRIMARY KEY,
                name text,
                status text,
                config text,
                created_at bigint,
                updated_at bigint
            )",
            &[],
        )
        .await?;

    // crawled_pages (Job-scoped primary key for recrawl history and multi-job isolation)
    session
        .query(
            "CREATE TABLE IF NOT EXISTS arachne.crawled_pages (
                domain text,
                job_id uuid,
                url text,
                http_status int,
                content_type text,
                content_length int,
                content_hash text,
                title text,
                language text,
                content_ref text,
                crawled_at bigint,
                crawl_duration_ms int,
                PRIMARY KEY ((domain), job_id, url)
            )",
            &[],
        )
        .await?;

    // domain_metadata
    session
        .query(
            "CREATE TABLE IF NOT EXISTS arachne.domain_metadata (
                domain text PRIMARY KEY,
                robots_txt text,
                robots_fetched_at bigint,
                crawl_delay_ms int,
                last_crawled_at bigint
            )",
            &[],
        )
        .await?;

    Ok(())
}
