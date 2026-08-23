//! Prometheus metrics.

use anyhow::Result;
use axum::{routing::get, Router};
use prometheus::{Encoder, Histogram, HistogramOpts, IntCounter, IntGauge, Registry, TextEncoder};
use std::sync::Arc;
use tracing::info;

/// Crawler metrics repository.
#[derive(Clone)]
pub struct CrawlerMetrics {
    pub pages_crawled: IntCounter,
    pub pages_failed: IntCounter,
    pub urls_discovered: IntCounter,
    pub urls_deduped: IntCounter,
    pub urls_robots_blocked: IntCounter,
    pub bytes_downloaded: IntCounter,
    /// Audio files that passed probe + quality gates.
    pub audio_harvested: IntCounter,
    /// Audio files rejected by probe/quality gates (quarantined).
    pub audio_rejected: IntCounter,
    /// Audio downloads that failed at transport level (HTTP, network).
    pub audio_failed: IntCounter,
    pub crawl_duration_ms: Histogram,
    pub page_size_bytes: Histogram,
    pub active_tasks: IntGauge,
    pub frontier_size: IntGauge,
    pub jobs_running: IntGauge,
    pub registry: Registry,
}

impl CrawlerMetrics {
    /// Initialize all metrics and register them.
    pub fn new() -> Self {
        let registry = Registry::new();

        let pages_crawled = IntCounter::new(
            "arachne_pages_crawled_total",
            "Total pages successfully crawled",
        )
        .unwrap();
        let pages_failed =
            IntCounter::new("arachne_pages_failed_total", "Total pages failed to crawl").unwrap();
        let urls_discovered =
            IntCounter::new("arachne_urls_discovered_total", "Total URLs discovered").unwrap();
        let urls_deduped = IntCounter::new(
            "arachne_urls_deduped_total",
            "Total URLs skipped due to dedup",
        )
        .unwrap();
        let urls_robots_blocked = IntCounter::new(
            "arachne_urls_robots_blocked_total",
            "Total URLs blocked by robots.txt",
        )
        .unwrap();
        let bytes_downloaded =
            IntCounter::new("arachne_bytes_downloaded_total", "Total bytes downloaded").unwrap();
        let audio_harvested = IntCounter::new(
            "arachne_audio_harvested_total",
            "Audio files passing probe and quality gates",
        )
        .unwrap();
        let audio_rejected = IntCounter::new(
            "arachne_audio_rejected_total",
            "Audio files rejected by probe/quality gates",
        )
        .unwrap();
        let audio_failed = IntCounter::new(
            "arachne_audio_failed_total",
            "Audio downloads failed at transport level",
        )
        .unwrap();

        let crawl_duration_ms = Histogram::with_opts(
            HistogramOpts::new("arachne_crawl_duration_ms", "Time to crawl a page")
                .buckets(prometheus::exponential_buckets(10.0, 2.0, 10).unwrap()),
        )
        .unwrap();

        let page_size_bytes = Histogram::with_opts(
            HistogramOpts::new("arachne_page_size_bytes", "Size of crawled pages")
                .buckets(prometheus::exponential_buckets(1024.0, 2.0, 10).unwrap()),
        )
        .unwrap();

        let active_tasks =
            IntGauge::new("arachne_active_tasks", "Currently active crawl tasks").unwrap();
        let frontier_size =
            IntGauge::new("arachne_frontier_size", "Estimated size of URL frontier").unwrap();
        let jobs_running =
            IntGauge::new("arachne_jobs_running", "Number of currently running jobs").unwrap();

        registry.register(Box::new(pages_crawled.clone())).unwrap();
        registry.register(Box::new(pages_failed.clone())).unwrap();
        registry
            .register(Box::new(urls_discovered.clone()))
            .unwrap();
        registry.register(Box::new(urls_deduped.clone())).unwrap();
        registry
            .register(Box::new(urls_robots_blocked.clone()))
            .unwrap();
        registry
            .register(Box::new(bytes_downloaded.clone()))
            .unwrap();
        registry
            .register(Box::new(audio_harvested.clone()))
            .unwrap();
        registry
            .register(Box::new(audio_rejected.clone()))
            .unwrap();
        registry.register(Box::new(audio_failed.clone())).unwrap();
        registry
            .register(Box::new(crawl_duration_ms.clone()))
            .unwrap();
        registry
            .register(Box::new(page_size_bytes.clone()))
            .unwrap();
        registry.register(Box::new(active_tasks.clone())).unwrap();
        registry.register(Box::new(frontier_size.clone())).unwrap();
        registry.register(Box::new(jobs_running.clone())).unwrap();

        Self {
            pages_crawled,
            pages_failed,
            urls_discovered,
            urls_deduped,
            urls_robots_blocked,
            bytes_downloaded,
            audio_harvested,
            audio_rejected,
            audio_failed,
            crawl_duration_ms,
            page_size_bytes,
            active_tasks,
            frontier_size,
            jobs_running,
            registry,
        }
    }
}

impl Default for CrawlerMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Start a metrics HTTP server on the given port.
pub async fn serve_metrics(metrics: Arc<CrawlerMetrics>, port: u16) -> Result<()> {
    let app = Router::new().route(
        "/metrics",
        get(move || {
            let registry = metrics.registry.clone();
            async move {
                let mut buffer = vec![];
                let encoder = TextEncoder::new();
                let metric_families = registry.gather();
                encoder.encode(&metric_families, &mut buffer).unwrap();
                String::from_utf8(buffer).unwrap()
            }
        }),
    );

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    info!("Starting metrics server on port {}", port);
    axum::serve(listener, app).await?;

    Ok(())
}
