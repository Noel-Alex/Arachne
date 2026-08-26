pub mod media_download;

use anyhow::{Context, Result};
use arachne_core::{
    config::ArachneConfig,
    content::{extractor, filter},
    domain, logging,
    metrics::{self, CrawlerMetrics},
    models::{CrawlResult, CrawlStatus, CrawlTask, DiscoveredUrl},
    nats::NatsManager,
    politeness::PolitenessLimiter,
    robots::RobotsManager,
};
use chrono::Utc;
use futures::StreamExt;
use media_download::{MediaContext, harvest_media};
use reqwest::Client;
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;
use tokio::time::{sleep, timeout};
use tracing::{debug, error, info, warn};
use url::Url;

struct WorkerContext {
    config: ArachneConfig,
    nats: Arc<NatsManager>,
    robots: RobotsManager,
    politeness: PolitenessLimiter,
    http_client: Client,
    media: Arc<MediaContext>,
    metrics: Arc<CrawlerMetrics>,
}

#[tokio::main]
async fn main() -> Result<()> {
    logging::init_logging();
    info!("Starting Arachne Worker");

    let config = ArachneConfig::load(None).context("Failed to load configuration")?;

    let metrics = Arc::new(CrawlerMetrics::new());
    let metrics_clone = Arc::clone(&metrics);
    let metrics_port = config.metrics.port;
    if config.metrics.enabled {
        tokio::spawn(async move {
            info!("Starting metrics server on port {}", metrics_port);
            if let Err(e) = metrics::serve_metrics(metrics_clone, metrics_port).await {
                error!("Metrics server failed: {:?}", e);
            }
        });
    }

    let nats = Arc::new(
        NatsManager::connect(&config.nats)
            .await
            .context("Failed to connect to NATS")?,
    );
    nats.ensure_streams()
        .await
        .context("Failed to ensure NATS streams")?;

    let robots = {
        let manager = RobotsManager::new(
            &config.worker.user_agent,
            Duration::from_secs(config.politeness.robots_cache_ttl_secs),
        );
        // Best-effort robots persistence so `arachne inspect <domain>` has
        // data: a small DB pool is opened only for the Postgres backend.
        // Scylla deployments skip persistence; any failure here leaves the
        // worker fully functional without it.
        match config.database.backend {
            arachne_core::config::DbBackend::Postgres => {
                match arachne_core::db::ArachneRepo::new(&config).await {
                    Ok(repo) => manager.with_repo(Arc::new(repo)),
                    Err(e) => {
                        warn!("robots metadata persistence disabled (repo init failed): {e:#}");
                        manager
                    }
                }
            }
            arachne_core::config::DbBackend::Scylla => {
                debug!("robots metadata persistence skipped on Scylla backend");
                manager
            }
        }
    };

    let politeness = PolitenessLimiter::new(config.politeness.default_crawl_delay_ms);

    let http_client = Client::builder()
        .user_agent(&config.worker.user_agent)
        // NOTE: no client-level .timeout() here — in reqwest that is a TOTAL
        // request deadline including body streaming, so it killed every media
        // download slower than request_timeout_secs mid-body. Bounds are
        // enforced per-path instead: connect_timeout below, a headers deadline
        // + per-chunk idle timeout on the page path (see process_task), and
        // size caps + host concurrency on the media path.
        .connect_timeout(Duration::from_secs(config.worker.connect_timeout_secs))
        // SSRF: re-validate every redirect hop through the egress guard
        // (Policy::limited alone follows 30x chains to any host).
        .redirect(arachne_core::egress::guarded_redirect_policy(
            config.worker.max_redirects,
        ))
        .pool_max_idle_per_host(100)
        .pool_idle_timeout(Duration::from_secs(90))
        .tcp_keepalive(Duration::from_secs(60))
        .tcp_nodelay(true)
        .build()
        .context("Failed to build HTTP client")?;

    let media_ctx =
        Arc::new(MediaContext::new(config.clone()).context("Failed to initialize media store")?);

    let ctx = Arc::new(WorkerContext {
        config: config.clone(),
        nats: Arc::clone(&nats),
        robots,
        politeness,
        http_client,
        media: media_ctx,
        metrics: Arc::clone(&metrics),
    });

    // Stable durable name derived from the host: a restarted worker adopts
    // its own previous consumer instead of leaving orphans that block new
    // consumers on the workqueue stream. Multiple workers on one host would
    // share the durable — run one worker per host, or set ARACHNE_WORKER_ID.
    let worker_name = match std::env::var("ARACHNE_WORKER_ID") {
        Ok(id) if !id.trim().is_empty() => format!("worker-{}", id.trim()),
        _ => {
            let host = std::env::var("COMPUTERNAME")
                .or_else(|_| std::env::var("HOSTNAME"))
                .unwrap_or_else(|_| "unknown-host".to_string())
                .to_lowercase()
                .replace([' ', '.', '/'], "-");
            format!("worker-{host}")
        }
    };
    let consumer = nats
        .create_task_consumer(&worker_name)
        .await
        .context("Failed to create NATS task consumer")?;

    let semaphore = Arc::new(Semaphore::new(config.worker.max_concurrent_requests));
    // In-flight counter for graceful drain: incremented at spawn, decremented
    // when the spawned task finishes. The semaphore itself is NOT closed on
    // shutdown — running tasks hold their permits and must be allowed to
    // complete (and ack) rather than be aborted mid-download.
    let in_flight = Arc::new(AtomicUsize::new(0));
    info!(
        worker_name = %worker_name,
        max_concurrent = config.worker.max_concurrent_requests,
        "Worker loop initialized"
    );

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel(1);
    let shutdown_tx_clone = shutdown_tx.clone();

    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            info!("Shutdown signal received, initiating graceful shutdown...");
            let _ = shutdown_tx_clone.send(());
        }
    });

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Worker shutting down...");
                break;
            }
            fetch_res = consumer.fetch().max_messages(10).messages() => {
                match fetch_res {
                    Ok(mut messages) => {
                        while let Some(msg_res) = messages.next().await {
                            match msg_res {
                                Ok(msg) => {
                                    let task: CrawlTask = match serde_json::from_slice(&msg.payload) {
                                        Ok(t) => t,
                                        Err(e) => {
                                            error!("Failed to parse CrawlTask payload: {}", e);
                                            ctx.metrics.messages_malformed.inc();
                                            let _ = msg.ack().await;
                                            continue;
                                        }
                                    };

                                    let permit = semaphore.clone().acquire_owned().await?;
                                    let ctx_clone = ctx.clone();

                                    in_flight.fetch_add(1, Ordering::SeqCst);
                                    let in_flight_task = Arc::clone(&in_flight);
                                    tokio::spawn(async move {
                                        let _permit = permit;
                                        if process_task(ctx_clone, task).await {
                                            let _ = msg.ack().await;
                                        } else {
                                            warn!("Task processing or result publish failed, leaving task unacknowledged for redelivery");
                                        }
                                        // Decrement LAST so the drain below never
                                        // observes zero while a task still runs.
                                        in_flight_task.fetch_sub(1, Ordering::SeqCst);
                                    });
                                }
                                Err(e) => {
                                    warn!("Error reading message: {}", e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        debug!("No tasks fetched or timeout: {}", e);
                        sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }
    }

    // Graceful drain: give in-flight tasks up to 30s to finish (download,
    // store, publish, ack). Aborting them would orphan .parts files and drop
    // results/acks; after the deadline we leave the stragglers to die with
    // the process and NATS redelivers their unacked messages.
    const DRAIN_DEADLINE: Duration = Duration::from_secs(30);
    let drain_start = Instant::now();
    while in_flight.load(Ordering::SeqCst) > 0 && drain_start.elapsed() < DRAIN_DEADLINE {
        info!(
            remaining = in_flight.load(Ordering::SeqCst),
            elapsed_ms = drain_start.elapsed().as_millis() as u64,
            "Draining in-flight tasks..."
        );
        sleep(Duration::from_millis(250)).await;
    }
    let drained = in_flight.load(Ordering::SeqCst);
    if drained > 0 {
        warn!(
            remaining = drained,
            "Drain deadline exceeded; {} task(s) will be abandoned for redelivery", drained
        );
    }

    info!("Arachne Worker stopped.");
    Ok(())
}

async fn process_task(ctx: Arc<WorkerContext>, task: CrawlTask) -> bool {
    let start_time = Instant::now();
    ctx.metrics.active_tasks.inc();

    // 1. SSRF & Egress Boundary Check
    if !domain::is_safe_egress_url(&task.url) {
        warn!(url = %task.url, "URL blocked by SSRF boundary guard");
        let res = record_failure(
            &ctx,
            &task,
            CrawlStatus::FetchError("Blocked by SSRF boundary guard".into()),
            0,
        )
        .await;
        ctx.metrics.active_tasks.dec();
        return res;
    }

    let target_url = match Url::parse(&task.url) {
        Ok(u) => u,
        Err(e) => {
            error!(url = %task.url, "Invalid target URL: {}", e);
            let res = record_failure(&ctx, &task, CrawlStatus::FetchError(e.to_string()), 0).await;
            ctx.metrics.active_tasks.dec();
            return res;
        }
    };

    // Media tasks take the streaming binary download path.
    if task.kind.is_media() {
        return process_media_task(ctx, task).await;
    }

    let domain = domain::extract_root_domain(&task.url).unwrap_or_else(|| "unknown".to_string());

    if ctx.config.politeness.respect_robots_txt && !ctx.robots.is_allowed(&target_url).await {
        info!(url = %task.url, "URL blocked by robots.txt");
        ctx.metrics.urls_robots_blocked.inc();
        let res = record_failure(&ctx, &task, CrawlStatus::RobotsBlocked, 0).await;
        ctx.metrics.active_tasks.dec();
        return res;
    }

    if let Some(delay) = ctx.robots.get_crawl_delay(&target_url).await {
        apply_crawl_delay(&ctx, &domain, delay);
    }
    ctx.politeness.wait_for_permission(&domain).await;

    let mut attempts = 0;
    let max_attempts = ctx.config.worker.retry_attempts;
    let mut response_res = None;

    while attempts < max_attempts {
        attempts += 1;
        // Politeness applies to every ATTEMPT: re-arm the limiter before each
        // retry so a crawl-delay robots.txt refreshed mid-flight is honored
        // rather than bypassed by the backoff path.
        if attempts > 1 {
            ctx.politeness.wait_for_permission(&domain).await;
        }
        // Headers deadline only (NOT a total-request timeout): the client has
        // no client-level .timeout(), so bound just the request/send phase
        // here and let the body read below be bounded per-chunk instead.
        let send = ctx.http_client.get(target_url.clone()).send();
        match timeout(
            Duration::from_secs(ctx.config.worker.request_timeout_secs),
            send,
        )
        .await
        {
            Ok(Ok(resp)) => {
                // Retry only statuses meaning "try again later"; success and
                // hard 4xx answers are terminal either way. On the final
                // attempt even retryable statuses fall through so the real
                // code reaches the failure path below.
                if is_retryable_status(resp.status().as_u16()) && attempts < max_attempts {
                    warn!(
                        attempt = attempts,
                        max = max_attempts,
                        status = resp.status().as_u16(),
                        url = %task.url,
                        "Retryable status; backing off before next attempt"
                    );
                    sleep(Duration::from_millis(
                        ctx.config.worker.retry_backoff_ms * (attempts as u64),
                    ))
                    .await;
                } else {
                    response_res = Some(resp);
                    break;
                }
            }
            Ok(Err(e)) => {
                warn!(attempt = attempts, max = max_attempts, url = %task.url, "Fetch failed: {}", e);
                if attempts < max_attempts {
                    sleep(Duration::from_millis(
                        ctx.config.worker.retry_backoff_ms * (attempts as u64),
                    ))
                    .await;
                }
            }
            Err(_) => {
                warn!(
                    attempt = attempts,
                    max = max_attempts,
                    url = %task.url,
                    "response headers timed out after {}s",
                    ctx.config.worker.request_timeout_secs
                );
                if attempts < max_attempts {
                    sleep(Duration::from_millis(
                        ctx.config.worker.retry_backoff_ms * (attempts as u64),
                    ))
                    .await;
                }
            }
        }
    }

    let response = match response_res {
        Some(r) => r,
        None => {
            let res = record_failure(
                &ctx,
                &task,
                CrawlStatus::FetchError("Max retries exceeded".into()),
                start_time.elapsed().as_millis() as u64,
            )
            .await;
            ctx.metrics.active_tasks.dec();
            return res;
        }
    };

    let status_code = response.status().as_u16();
    if !response.status().is_success() {
        let res = record_failure(
            &ctx,
            &task,
            CrawlStatus::HttpError(status_code),
            start_time.elapsed().as_millis() as u64,
        )
        .await;
        ctx.metrics.active_tasks.dec();
        return res;
    }

    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    if !filter::is_html_content_type(&content_type) {
        info!(url = %task.url, content_type = %content_type, "Skipping non-HTML content type");
        let res = record_failure(
            &ctx,
            &task,
            CrawlStatus::InvalidContentType,
            start_time.elapsed().as_millis() as u64,
        )
        .await;
        ctx.metrics.active_tasks.dec();
        return res;
    }

    let max_bytes = ctx.config.worker.max_content_size_bytes;
    let mut body_bytes = Vec::new();
    let mut stream = response.bytes_stream();

    // Idle-read bound: with no client-level total timeout, a stalled body
    // (slow-loris server) could otherwise hang this task forever. The size
    // cap bounds bytes; this bounds TIME — error if no chunk arrives within
    // the idle window.
    const READ_IDLE: Duration = Duration::from_secs(30);
    while let Some(chunk_res) = next_chunk(&mut stream, READ_IDLE).await {
        match chunk_res {
            Some(Ok(chunk)) => {
                if !filter::is_within_size_limit(body_bytes.len() + chunk.len(), max_bytes) {
                    warn!(url = %task.url, "Exceeded max content size");
                    let res = record_failure(
                        &ctx,
                        &task,
                        CrawlStatus::ContentTooLarge,
                        start_time.elapsed().as_millis() as u64,
                    )
                    .await;
                    ctx.metrics.active_tasks.dec();
                    return res;
                }
                body_bytes.extend_from_slice(&chunk);
            }
            Some(Err(e)) => {
                error!(url = %task.url, "Error reading response body stream: {}", e);
                let res = record_failure(
                    &ctx,
                    &task,
                    CrawlStatus::FetchError(e.to_string()),
                    start_time.elapsed().as_millis() as u64,
                )
                .await;
                ctx.metrics.active_tasks.dec();
                return res;
            }
            None => {
                // Idle timeout: no chunk arrived within the window.
                warn!(
                    url = %task.url,
                    "read timed out (no data for {}s)",
                    READ_IDLE.as_secs()
                );
                let res = record_failure(
                    &ctx,
                    &task,
                    CrawlStatus::FetchError(format!(
                        "read timed out (no data for {}s)",
                        READ_IDLE.as_secs()
                    )),
                    start_time.elapsed().as_millis() as u64,
                )
                .await;
                ctx.metrics.active_tasks.dec();
                return res;
            }
        }
    }

    ctx.metrics.page_size_bytes.observe(body_bytes.len() as f64);

    let html_str = decode_page(&body_bytes, &content_type);
    let extracted = extractor::extract_from_html(&html_str, &target_url);

    // Media-link discovery: audio/video/document URLs found on the page
    // become media candidates (classified at admission by the coordinator).
    // Parsed ONCE here — the previous find_audio_links/links_by_extension
    // calls each re-ran Html::parse_document (four parses per page total).
    // The core predicates are reused verbatim so classification semantics are
    // unchanged, incl. rel=enclosure anchors counting as audio regardless of
    // extension. extractor::extract_from_html still parses internally; one
    // extra parse is accepted for now.
    // Parsed inside this scope: scraper::Html is !Send, so it must be dropped
    // before the probe_discovery_candidates().await below or the tokio::spawn
    // caller rejects process_task's future as non-Send.
    let mut audio_links: Vec<String> = Vec::new();
    let mut video_links: Vec<String> = Vec::new();
    let mut doc_links: Vec<String> = Vec::new();
    {
        let doc = scraper::Html::parse_document(&html_str);
        use arachne_core::discovery::audio_links::has_audio_extension;
        use arachne_core::discovery::media_links::{has_document_extension, has_video_extension};

        // Shared resolve + scheme filter, mirroring the core helpers:
        // http/https only, deduped per list, discovery order preserved.
        // Empty hrefs/srcs are skipped — joining "" would yield the page
        // URL itself.
        let push_resolved = |list: &mut Vec<String>, raw: &str| {
            if raw.is_empty() {
                return;
            }
            let Ok(resolved) = target_url.join(raw) else {
                return;
            };
            if resolved.scheme() != "http" && resolved.scheme() != "https" {
                return;
            }
            let s = resolved.to_string();
            if !list.contains(&s) {
                list.push(s);
            }
        };

        for el in doc.tree.nodes().filter_map(|n| n.value().as_element()) {
            if el.name() == "a" {
                let href = el.attr("href").unwrap_or_default();
                if href.is_empty() {
                    continue;
                }
                let is_enclosure = el.attr("rel").is_some_and(|r| {
                    r.split_whitespace()
                        .any(|t| t.eq_ignore_ascii_case("enclosure"))
                });
                if has_audio_extension(href) || is_enclosure {
                    push_resolved(&mut audio_links, href);
                }
                if has_video_extension(href) {
                    push_resolved(&mut video_links, href);
                }
                if has_document_extension(href) {
                    push_resolved(&mut doc_links, href);
                }
            } else if matches!(el.name(), "audio" | "source" | "embed")
                && let Some(src) = el.attr("src")
            {
                push_resolved(&mut audio_links, src);
            }
        }
    }
    audio_links.extend(video_links);
    audio_links.extend(doc_links);

    // Sitemap/feed discovery probes: a page's own links may point at
    // sitemaps or syndication feeds whose URLs never appear as plain <a>
    // hrefs elsewhere on the site. Runs AFTER regular link/media discovery;
    // harvested page URLs merge into all_links below (depth = task.depth + 1
    // like everything else) and audio enclosures join audio_links so they
    // flow through the coordinator's license gating like organic media.
    let discovery_pages = probe_discovery_candidates(
        &ctx.http_client,
        &ctx.robots,
        ctx.config.politeness.respect_robots_txt,
        &extracted.links,
        &mut audio_links,
    )
    .await;

    let mut all_links = extracted.links;
    for url in discovery_pages {
        if !all_links.contains(&url) {
            all_links.push(url);
        }
    }
    for audio in audio_links {
        if !all_links.contains(&audio) {
            all_links.push(audio);
        }
    }

    let mut hasher = Sha256::new();
    hasher.update(&body_bytes);
    let content_hash = hex::encode(hasher.finalize());

    let mut storage_path = PathBuf::from(&ctx.config.storage.content_dir);
    storage_path.push(&domain);
    if let Err(e) = fs::create_dir_all(&storage_path).await {
        error!("Failed to create storage directory: {}", e);
    }
    storage_path.push(format!("{}.html", content_hash));

    // When raw-HTML storage is enabled, a failed write must NOT masquerade as
    // Success-with-no-artifact (silent data loss): fail the task instead. The
    // store_raw_html=false branch legitimately yields Success + None.
    let content_ref = if ctx.config.storage.store_raw_html {
        match fs::File::create(&storage_path).await {
            Ok(mut file) => {
                // Byte-faithful artifact: store the RAW network bytes —
                // content_hash is SHA-256 over these same bytes, so hash and
                // artifact always agree even for non-UTF-8 pages. The
                // charset-transcoded `html_str` above feeds extraction and
                // discovery only.
                if let Err(e) = file.write_all(&body_bytes).await {
                    let res = record_failure(
                        &ctx,
                        &task,
                        CrawlStatus::FetchError(format!("storage write failed: {e}")),
                        start_time.elapsed().as_millis() as u64,
                    )
                    .await;
                    ctx.metrics.active_tasks.dec();
                    return res;
                }
                let abs_path = fs::canonicalize(&storage_path)
                    .await
                    .unwrap_or(storage_path);
                Some(format!(
                    "file:///{}",
                    abs_path.to_string_lossy().replace('\\', "/")
                ))
            }
            Err(e) => {
                let res = record_failure(
                    &ctx,
                    &task,
                    CrawlStatus::FetchError(format!("storage write failed: {e}")),
                    start_time.elapsed().as_millis() as u64,
                )
                .await;
                ctx.metrics.active_tasks.dec();
                return res;
            }
        }
    } else {
        None
    };

    let discovered_urls: Vec<DiscoveredUrl> = all_links
        .into_iter()
        .map(|link| DiscoveredUrl {
            url: link,
            source_url: task.url.clone(),
            job_id: task.job_id,
            depth: task.depth + 1,
        })
        .collect();

    let duration_ms = start_time.elapsed().as_millis() as u64;

    // Wire bloat guard: discovered URLs already travel on DISCOVERED_URLS via
    // publish_discovered below; the results-stream copy is never read by the
    // coordinator, so it ships empty (field stays for serde compatibility).
    let result = CrawlResult {
        source_url: task.url.clone(),
        job_id: task.job_id,
        status: CrawlStatus::Success,
        domain: Some(domain.clone()),
        content_ref,
        title: extracted.title,
        language: extracted.language,
        content_length: Some(body_bytes.len()),
        content_hash: Some(content_hash),
        discovered_urls: vec![],
        crawl_duration_ms: duration_ms,
        crawled_at: Utc::now(),
        media_meta: None,
        media_probe: None,
    };

    if let Err(e) = ctx.nats.publish_result(&result).await {
        error!("Failed to publish crawl result to NATS: {}", e);
        ctx.metrics.active_tasks.dec();
        return false;
    }

    if !discovered_urls.is_empty() {
        ctx.metrics
            .urls_discovered
            .inc_by(discovered_urls.len() as u64);
        if let Err(e) = ctx.nats.publish_discovered(&discovered_urls).await {
            error!("Failed to publish discovered URLs to NATS: {}", e);
            ctx.metrics.active_tasks.dec();
            return false;
        }
    }

    ctx.metrics.pages_crawled.inc();
    ctx.metrics.bytes_downloaded.inc_by(body_bytes.len() as u64);
    ctx.metrics.crawl_duration_ms.observe(duration_ms as f64);
    ctx.metrics.active_tasks.dec();

    true
}

async fn process_media_task(ctx: Arc<WorkerContext>, task: CrawlTask) -> bool {
    let start_time = Instant::now();
    let domain = domain::extract_root_domain(&task.url).unwrap_or_else(|| "unknown".to_string());
    let target_url = match Url::parse(&task.url) {
        Ok(u) => u,
        Err(e) => {
            error!(url = %task.url, "Invalid media URL: {}", e);
            let res = record_failure(&ctx, &task, CrawlStatus::FetchError(e.to_string()), 0).await;
            ctx.metrics.active_tasks.dec();
            return res;
        }
    };

    // Media hosts get the SAME politeness as pages: robots.txt rules and
    // crawl-delay (archive.org's documented bulk envelope depends on it).
    if ctx.config.politeness.respect_robots_txt && !ctx.robots.is_allowed(&target_url).await {
        info!(url = %task.url, "Media URL blocked by robots.txt");
        ctx.metrics.urls_robots_blocked.inc();
        let res = record_failure(&ctx, &task, CrawlStatus::RobotsBlocked, 0).await;
        ctx.metrics.active_tasks.dec();
        return res;
    }
    if let Some(delay) = ctx.robots.get_crawl_delay(&target_url).await {
        apply_crawl_delay(&ctx, &domain, delay);
    }
    ctx.politeness.wait_for_permission(&domain).await;

    let outcome = harvest_media(&ctx.media, &ctx.http_client, &task).await;

    // Classify for metrics + always log the terminal state (a silent
    // download is indistinguishable from a hung one when watching logs).
    match &outcome.status {
        CrawlStatus::Success => {
            ctx.metrics.audio_harvested.inc();
            ctx.metrics.bytes_downloaded.inc_by(outcome.bytes);
            info!(
                url = %task.url,
                bytes = outcome.bytes,
                duration_s = outcome.probe.as_ref().map(|p| p.duration_secs).unwrap_or(0.0),
                elapsed_ms = start_time.elapsed().as_millis() as u64,
                "audio harvested"
            );
        }
        CrawlStatus::ProbeFailed | CrawlStatus::QualityRejected => {
            ctx.metrics.audio_rejected.inc();
            warn!(
                url = %task.url,
                status = ?outcome.status,
                error = outcome.error.as_deref().unwrap_or(""),
                "audio rejected"
            );
        }
        _ => {
            ctx.metrics.audio_failed.inc();
            warn!(
                url = %task.url,
                status = ?outcome.status,
                error = outcome.error.as_deref().unwrap_or(""),
                "audio download failed"
            );
        }
    }

    let result = CrawlResult {
        source_url: task.url.clone(),
        job_id: task.job_id,
        status: outcome.status,
        domain: Some(domain),
        content_ref: outcome.stored.as_ref().map(|s| {
            s.fs_path
                .as_ref()
                .map(|p| format!("file://{}", p.to_string_lossy().replace('\\', "/")))
                .unwrap_or_else(|| format!("object://{}", s.object_path))
        }),
        title: outcome
            .probe
            .as_ref()
            .and_then(|p| p.title.clone())
            .or_else(|| task.media.as_ref().and_then(|m| m.title.clone())),
        language: None,
        content_length: Some(outcome.bytes as usize),
        content_hash: None, // set below from the store path
        discovered_urls: vec![],
        crawl_duration_ms: start_time.elapsed().as_millis() as u64,
        crawled_at: Utc::now(),
        media_meta: task.media.clone(),
        media_probe: outcome
            .format
            .as_ref()
            .map(|fmt| arachne_core::models::MediaProbe {
                duration_secs: outcome
                    .probe
                    .as_ref()
                    .map(|p| p.duration_secs)
                    .unwrap_or(0.0),
                bitrate_kbps: outcome
                    .probe
                    .as_ref()
                    .and_then(|p| p.bitrate_kbps)
                    .map(|b| b as i32),
                format: fmt.clone(),
                title: outcome.probe.as_ref().and_then(|p| p.title.clone()),
                artist: outcome.probe.as_ref().and_then(|p| p.artist.clone()),
                album: outcome.probe.as_ref().and_then(|p| p.album.clone()),
                year: outcome.probe.as_ref().and_then(|p| p.year),
                genre: outcome.probe.as_ref().and_then(|p| p.genre.clone()),
            }),
    };

    // The store path encodes the sha256; carry it explicitly for the manifest.
    let mut result = result;
    if let Some(stored) = &outcome.stored {
        let hash = stored
            .object_path
            .rsplit('/')
            .next()
            .and_then(|f| f.split('.').next())
            .unwrap_or("")
            .to_string();
        if hash.len() == 64 {
            result.content_hash = Some(hash);
        } else {
            result.content_hash = None; // dedup-skip case ("already-present:<hash>")
        }
    } else {
        result.content_hash = None;
    }

    // Decrement on every terminal path (page path does this per-branch; the
    // audio path has a single exit here).
    ctx.metrics.active_tasks.dec();

    match ctx.nats.publish_result(&result).await {
        Ok(_) => true,
        Err(e) => {
            error!("Failed to publish media crawl result: {}", e);
            false
        }
    }
}

/// Clamp a robots.txt Crawl-delay at `max_ms`. Tiny pure fn so the capping
/// math is unit-testable without standing up a full WorkerContext.
fn clamp_delay(delay: Duration, max_ms: u64) -> Duration {
    delay.min(Duration::from_millis(max_ms))
}

/// True for statuses whose only remedy is trying again: request timeout (408),
/// rate limiting (429), and server-side 5xx failures. Hard 4xx answers are
/// permanent for this URL — retrying them just burns politeness budget and
/// backoff time without changing the outcome.
fn is_retryable_status(status: u16) -> bool {
    status == 408 || status == 429 || (500..600).contains(&status)
}

#[cfg(test)]
mod retryable_status_tests {
    use super::is_retryable_status;

    #[test]
    fn timeout_and_rate_limit_retry() {
        assert!(is_retryable_status(408));
        assert!(is_retryable_status(429));
    }

    #[test]
    fn server_errors_retry() {
        assert!(is_retryable_status(500));
        assert!(is_retryable_status(502));
        assert!(is_retryable_status(503));
        assert!(is_retryable_status(599));
    }

    #[test]
    fn success_is_not_retryable() {
        assert!(!is_retryable_status(200));
        assert!(!is_retryable_status(301));
    }

    #[test]
    fn hard_4xx_is_not_retryable() {
        assert!(!is_retryable_status(400));
        assert!(!is_retryable_status(403));
        assert!(!is_retryable_status(404));
        assert!(!is_retryable_status(410));
    }
}

/// Apply a robots.txt Crawl-delay capped at politeness.max_crawl_delay_ms so a
/// hostile or over-conservative value cannot park semaphore permits indefinitely.
/// (The config knob previously had no reader - this wires it up.)
fn apply_crawl_delay(ctx: &WorkerContext, domain: &str, delay: Duration) {
    let capped = clamp_delay(delay, ctx.config.politeness.max_crawl_delay_ms);
    if capped != delay {
        warn!(
            domain,
            raw_ms = delay.as_millis() as u64,
            capped_ms = capped.as_millis() as u64,
            "robots Crawl-delay exceeds max_crawl_delay_ms; clamping"
        );
    }
    ctx.politeness.set_domain_delay(domain, capped);
}

async fn record_failure(
    ctx: &Arc<WorkerContext>,
    task: &CrawlTask,
    status: CrawlStatus,
    duration_ms: u64,
) -> bool {
    ctx.metrics.pages_failed.inc();
    let result = CrawlResult {
        source_url: task.url.clone(),
        job_id: task.job_id,
        status,
        domain: domain::extract_root_domain(&task.url),
        content_ref: None,
        title: None,
        language: None,
        content_length: None,
        content_hash: None,
        discovered_urls: vec![],
        crawl_duration_ms: duration_ms,
        crawled_at: Utc::now(),
        media_meta: task.media.clone(),
        media_probe: None,
    };

    if let Err(e) = ctx.nats.publish_result(&result).await {
        error!("Failed to publish failed crawl result to NATS: {}", e);
        false
    } else {
        true
    }
}

/// Probe sitemap/feed candidates among a page's own links and merge the
/// harvest into `audio_links` (enclosures) — returns page URLs for the
/// caller to fold into its discovered-URL list. Best-effort by contract:
/// every failure logs at debug and is skipped; this never fails the task.
///
/// Politeness note: these probes run WITHOUT the politeness limiter (they
/// would serialize behind every queued crawl of the same domain), but they
/// respect robots.txt and are hard-bounded — at most 3+3 single-document
/// fetches per page, each with its own 10s timeout and 5MB body cap. A
/// single low-rate probe burst per crawled page.
async fn probe_discovery_candidates(
    client: &reqwest::Client,
    robots: &RobotsManager,
    respect_robots: bool,
    page_links: &[String],
    audio_out: &mut Vec<String>,
) -> Vec<String> {
    let (sitemap_cands, feed_cands) =
        arachne_core::discovery::wire::discovery_candidates(page_links);
    let mut pages = Vec::new();
    let mut harvested_audio = Vec::new();

    const SITEMAP_MAX_CHILDREN: usize = 8;
    const PER_MAP_CAP: usize = 200;

    for cand in sitemap_cands {
        let Ok(cand_url) = Url::parse(cand.as_str()) else {
            continue;
        };
        if respect_robots && !robots.is_allowed(&cand_url).await {
            debug!(url = %cand, "sitemap candidate blocked by robots.txt");
            continue;
        }
        match arachne_core::discovery::wire::harvest_sitemap(
            client,
            &cand,
            SITEMAP_MAX_CHILDREN,
            PER_MAP_CAP,
        )
        .await
        {
            Ok(mut urls) => pages.append(&mut urls),
            Err(e) => debug!(url = %cand, "sitemap harvest failed: {e:#}"),
        }
    }

    for cand in feed_cands {
        let Ok(cand_url) = Url::parse(cand.as_str()) else {
            continue;
        };
        if respect_robots && !robots.is_allowed(&cand_url).await {
            debug!(url = %cand, "feed candidate blocked by robots.txt");
            continue;
        }
        match arachne_core::discovery::wire::harvest_feed(client, &cand).await {
            Ok((links, enclosures)) => {
                pages.extend(links);
                harvested_audio.extend(enclosures);
            }
            Err(e) => debug!(url = %cand, "feed harvest failed: {e:#}"),
        }
    }

    let found_pages = pages.len();
    let found_audio = harvested_audio.len();
    audio_out.append(&mut harvested_audio);
    if found_pages + found_audio > 0 {
        info!(
            pages = found_pages,
            audio = found_audio,
            "sitemap/feed discovery yielded URLs"
        );
    }
    pages
}

/// Decode a page body to a String using the browser-style charset cascade:
/// Content-Type header param, then BOM sniff, then `<meta charset>` /
/// `<meta http-equiv="content-type">` in the first 2048 bytes, then UTF-8
/// with replacement.
fn decode_page(body: &[u8], content_type_header: &str) -> String {
    // 1. Explicit charset in the Content-Type header.
    let lower_header = content_type_header.to_ascii_lowercase();
    if let Some(enc) = param_value(&lower_header, "charset")
        .filter(|l| !l.is_empty())
        .and_then(|label| encoding_rs::Encoding::for_label(label.as_bytes()))
    {
        return enc.decode(body).0.into_owned();
    }

    // 2. BOM sniff. encoding_rs consumes the BOM itself when given the right
    // encoding, so we only need to pick which one applies.
    let bom_enc = if body.starts_with(&[0xEF, 0xBB, 0xBF]) {
        Some(encoding_rs::UTF_8)
    } else if body.starts_with(&[0xFF, 0xFE]) {
        Some(encoding_rs::UTF_16LE)
    } else if body.starts_with(&[0xFE, 0xFF]) {
        Some(encoding_rs::UTF_16BE)
    } else {
        None
    };
    if let Some(enc) = bom_enc {
        return enc.decode(body).0.into_owned();
    }

    // 3. <meta charset="..."> or <meta http-equiv="content-type"
    // content="...charset=..."> — both carry a literal `charset=` inside the
    // tag, so one case-insensitive scan of the first 2048 bytes covers them.
    // ponytail: any meta tag's text containing "charset=" also matches
    // (browsers restrict to charset/http-equiv attrs); tighten if that bites.
    let head = String::from_utf8_lossy(&body[..body.len().min(2048)]).to_ascii_lowercase();
    for (pos, _) in head.match_indices("<meta") {
        let tag_end = head[pos..].find('>').map(|e| pos + e).unwrap_or(head.len());
        if let Some(enc) = param_value(&head[pos..tag_end], "charset")
            .filter(|l| !l.is_empty())
            .and_then(|label| encoding_rs::Encoding::for_label(label.as_bytes()))
        {
            return enc.decode(body).0.into_owned();
        }
    }

    // 4. Fallback: UTF-8 with replacement.
    encoding_rs::UTF_8.decode(body).0.into_owned()
}

/// Value following `key=` in a lowercased attribute/header string: optional
/// whitespace around '=', quoted (`"`/`'`) or unquoted value. Unquoted values
/// end at whitespace, ';' or '>'.
fn param_value<'a>(s: &'a str, key: &str) -> Option<&'a str> {
    let pos = s.find(key)?;
    let rest = s[pos + key.len()..].trim_start();
    let rest = rest.strip_prefix('=')?;
    let rest = rest.trim_start();
    match rest.chars().next() {
        Some(q @ ('"' | '\'')) => {
            let end = rest[1..].find(q)?;
            Some(&rest[1..1 + end])
        }
        _ => {
            let end = rest.find([' ', '\t', ';', '>']).unwrap_or(rest.len());
            Some(&rest[..end])
        }
    }
}

/// Await the next stream item, giving up if none arrives within `idle`.
///
/// Returns `None` on idle timeout (no item within `idle`; the underlying
/// stream is dropped and abandoned), `Some(None)` at clean end of stream, and
/// `Some(Some(item))` otherwise. This is the per-chunk TIME bound that
/// replaces the removed client-level total-request timeout: bytes are bounded
/// separately by size caps, so only stalls need a deadline.
async fn next_chunk<S, T, E>(stream: &mut S, idle: Duration) -> Option<Option<Result<T, E>>>
where
    S: StreamExt<Item = Result<T, E>> + Unpin,
{
    // `.ok()`: Err(elapsed) => None (idle — nothing arrived in time);
    // Ok(item) keeps the stream's own item-or-end-of-stream Option nested inside.
    timeout(idle, stream.next()).await.ok()
}

#[cfg(test)]
mod page_tests {
    use super::decode_page;
    #[test]
    fn latin1_meta_charset_decodes_accents() {
        let body = b"<html><head><meta charset=\"iso-8859-1\"></head><body>caf\xe9</body></html>";
        assert_eq!(
            decode_page(body, "text/html"),
            "<html><head><meta charset=\"iso-8859-1\"></head><body>caf\u{e9}</body></html>"
        );
    }

    #[test]
    fn cp1252_via_header_charset() {
        // 0x93/0x94 are cp1252 smart quotes; windows-1252 is their proper label.
        let body = b"<html><body>\x93hi\x94</body></html>";
        assert_eq!(
            decode_page(body, "text/html; charset=WINDOWS-1252"),
            "<html><body>\u{201c}hi\u{201d}</body></html>"
        );
    }

    #[test]
    fn utf8_bom_sniffed() {
        let mut body = vec![0xEF, 0xBB, 0xBF];
        body.extend_from_slice("caf\u{e9}".as_bytes());
        assert_eq!(decode_page(&body, ""), "caf\u{e9}");
    }

    #[test]
    fn plain_utf8_unchanged() {
        let body = "<html><body>héllo wörld</body></html>".as_bytes();
        assert_eq!(
            decode_page(body, "text/html; charset=utf-8"),
            "<html><body>héllo wörld</body></html>"
        );
    }

    #[test]
    fn garbage_label_falls_back_to_utf8() {
        let body = b"<html><meta charset=\"x-nonsense-42\"><body>ok</body></html>";
        assert_eq!(
            decode_page(body, "text/html; charset=x-nonsense-42"),
            "<html><meta charset=\"x-nonsense-42\"><body>ok</body></html>"
        );
    }
}

#[cfg(test)]
mod idle_timeout_tests {
    use super::next_chunk;
    use std::time::Duration;

    #[tokio::test]
    async fn yields_item_when_data_arrives() {
        // Boxed stream is Unpin regardless of the underlying stream type.
        let mut stream: std::pin::Pin<
            Box<dyn futures::Stream<Item = Result<u32, std::convert::Infallible>>>,
        > = Box::pin(futures::stream::iter(vec![Ok(7)]));
        assert_eq!(
            next_chunk(&mut stream, Duration::from_secs(1)).await,
            Some(Some(Ok(7)))
        );
        // Stream exhausted afterwards: the underlying `None` (end of stream)
        // passes straight through as `Some(None)`.
        assert_eq!(
            next_chunk(&mut stream, Duration::from_secs(1)).await,
            Some(None)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn returns_none_on_idle_timeout() {
        // Stream whose only item never arrives within the window.
        let mut stream: std::pin::Pin<
            Box<dyn futures::Stream<Item = Result<u32, std::convert::Infallible>>>,
        > = Box::pin(futures::stream::pending());
        assert_eq!(
            next_chunk(&mut stream, Duration::from_secs(5)).await,
            None,
            "idle window elapsed with no item"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn passes_stream_errors_through() {
        let err = std::io::Error::other("boom");
        let kind = err.kind();
        let mut stream: std::pin::Pin<
            Box<dyn futures::Stream<Item = Result<u32, std::io::Error>>>,
        > = Box::pin(futures::stream::iter(vec![Err(err)]));
        match next_chunk(&mut stream, Duration::from_secs(5)).await {
            Some(Some(Err(e))) => assert_eq!(e.kind(), kind),
            other => panic!("expected stream error passthrough, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod crawl_delay_tests {
    use super::clamp_delay;
    use arachne_core::config::ArachneConfig;
    use std::time::Duration;

    // Boundaries are asserted against the real config knob
    // (PolitenessConfig::default().max_crawl_delay_ms) built in-memory — no
    // disk access — so the capping math stays pinned to what ships.
    #[test]
    fn below_max_passes_through() {
        let max_ms = ArachneConfig::default().politeness.max_crawl_delay_ms;
        let delay = Duration::from_millis(max_ms.saturating_sub(1));
        assert_eq!(clamp_delay(delay, max_ms), delay);
    }

    #[test]
    fn at_max_is_unchanged() {
        let max_ms = ArachneConfig::default().politeness.max_crawl_delay_ms;
        let delay = Duration::from_millis(max_ms);
        assert_eq!(clamp_delay(delay, max_ms), delay);
    }

    #[test]
    fn above_max_is_capped() {
        let max_ms = ArachneConfig::default().politeness.max_crawl_delay_ms;
        // Hostile/over-conservative robots.txt: "Crawl-delay: 3600".
        let hostile = Duration::from_secs(3600);
        assert_eq!(clamp_delay(hostile, max_ms), Duration::from_millis(max_ms));
    }
}
