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
use reqwest::Client;
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};
use url::Url;

struct WorkerContext {
    config: ArachneConfig,
    nats: Arc<NatsManager>,
    robots: RobotsManager,
    politeness: PolitenessLimiter,
    http_client: Client,
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
    tokio::spawn(async move {
        info!("Starting metrics server on port {}", metrics_port);
        if let Err(e) = metrics::serve_metrics(metrics_clone, metrics_port).await {
            error!("Metrics server failed: {:?}", e);
        }
    });

    let nats = Arc::new(
        NatsManager::connect(&config.nats)
            .await
            .context("Failed to connect to NATS")?,
    );
    nats.ensure_streams()
        .await
        .context("Failed to ensure NATS streams")?;

    let robots = RobotsManager::new(
        &config.worker.user_agent,
        Duration::from_secs(config.politeness.robots_cache_ttl_secs),
    );

    let politeness = PolitenessLimiter::new(config.politeness.default_crawl_delay_ms);

    let http_client = Client::builder()
        .user_agent(&config.worker.user_agent)
        .timeout(Duration::from_secs(config.worker.request_timeout_secs))
        .connect_timeout(Duration::from_secs(config.worker.connect_timeout_secs))
        .redirect(reqwest::redirect::Policy::limited(
            config.worker.max_redirects,
        ))
        .pool_max_idle_per_host(100)
        .pool_idle_timeout(Duration::from_secs(90))
        .tcp_keepalive(Duration::from_secs(60))
        .tcp_nodelay(true)
        .build()
        .context("Failed to build HTTP client")?;

    let ctx = Arc::new(WorkerContext {
        config: config.clone(),
        nats: Arc::clone(&nats),
        robots,
        politeness,
        http_client,
        metrics: Arc::clone(&metrics),
    });

    let worker_name = format!("worker-{}", uuid::Uuid::new_v4());
    let consumer = nats
        .create_task_consumer(&worker_name)
        .await
        .context("Failed to create NATS task consumer")?;

    let semaphore = Arc::new(Semaphore::new(config.worker.max_concurrent_requests));
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
                                            let _ = msg.ack().await;
                                            continue;
                                        }
                                    };

                                    let permit = semaphore.clone().acquire_owned().await?;
                                    let ctx_clone = ctx.clone();

                                    tokio::spawn(async move {
                                        let _permit = permit;
                                        if process_task(ctx_clone, task).await {
                                            let _ = msg.ack().await;
                                        } else {
                                            warn!("Task processing or result publish failed, leaving task unacknowledged for redelivery");
                                        }
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

    let domain = domain::extract_root_domain(&task.url).unwrap_or_else(|| "unknown".to_string());

    if ctx.config.politeness.respect_robots_txt && !ctx.robots.is_allowed(&target_url).await {
        info!(url = %task.url, "URL blocked by robots.txt");
        ctx.metrics.urls_robots_blocked.inc();
        let res = record_failure(&ctx, &task, CrawlStatus::RobotsBlocked, 0).await;
        ctx.metrics.active_tasks.dec();
        return res;
    }

    if let Some(delay) = ctx.robots.get_crawl_delay(&target_url).await {
        ctx.politeness.set_domain_delay(&domain, delay);
    }
    ctx.politeness.wait_for_permission(&domain).await;

    let mut attempts = 0;
    let max_attempts = ctx.config.worker.retry_attempts;
    let mut response_res = None;

    while attempts < max_attempts {
        attempts += 1;
        match ctx.http_client.get(target_url.clone()).send().await {
            Ok(resp) => {
                response_res = Some(resp);
                break;
            }
            Err(e) => {
                warn!(attempt = attempts, max = max_attempts, url = %task.url, "Fetch failed: {}", e);
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

    while let Some(chunk_res) = stream.next().await {
        match chunk_res {
            Ok(chunk) => {
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
            Err(e) => {
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
        }
    }

    let (html_str, _, _) = encoding_rs::UTF_8.decode(&body_bytes);
    let extracted = extractor::extract_from_html(&html_str, &target_url);

    let mut hasher = Sha256::new();
    hasher.update(&body_bytes);
    let content_hash = hex::encode(hasher.finalize());

    let mut storage_path = PathBuf::from(&ctx.config.storage.content_dir);
    storage_path.push(&domain);
    if let Err(e) = fs::create_dir_all(&storage_path).await {
        error!("Failed to create storage directory: {}", e);
    }
    storage_path.push(format!("{}.html", content_hash));

    let content_ref = if ctx.config.storage.store_raw_html {
        match fs::File::create(&storage_path).await {
            Ok(mut file) => {
                if let Err(e) = file.write_all(html_str.as_bytes()).await {
                    error!("Failed to write HTML content to storage: {}", e);
                    None
                } else {
                    let abs_path = fs::canonicalize(&storage_path)
                        .await
                        .unwrap_or(storage_path);
                    Some(format!(
                        "file:///{}",
                        abs_path.to_string_lossy().replace('\\', "/")
                    ))
                }
            }
            Err(e) => {
                error!("Failed to create storage file: {}", e);
                None
            }
        }
    } else {
        None
    };

    let discovered_urls: Vec<DiscoveredUrl> = extracted
        .links
        .into_iter()
        .map(|link| DiscoveredUrl {
            url: link,
            source_url: task.url.clone(),
            job_id: task.job_id,
            depth: task.depth + 1,
        })
        .collect();

    let duration_ms = start_time.elapsed().as_millis() as u64;

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
        discovered_urls: discovered_urls.clone(),
        crawl_duration_ms: duration_ms,
        crawled_at: Utc::now(),
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
    };

    if let Err(e) = ctx.nats.publish_result(&result).await {
        error!("Failed to publish failed crawl result to NATS: {}", e);
        false
    } else {
        true
    }
}
