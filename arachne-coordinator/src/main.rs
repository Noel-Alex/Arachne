use anyhow::{Context, Result};
use arachne_core::{
    config::ArachneConfig,
    db::ArachneRepo,
    dedup::Deduplicator,
    domain, logging,
    metrics::CrawlerMetrics,
    models::{CrawlResult, CrawlTask, DiscoveredUrl},
    nats::NatsManager,
};
use dashmap::DashMap;
use futures::StreamExt;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::signal;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    logging::init_logging();
    info!("Starting Arachne Coordinator");

    let config = ArachneConfig::load(None).context("Failed to load configuration")?;

    let metrics = Arc::new(CrawlerMetrics::new());
    let metrics_clone = Arc::clone(&metrics);
    let metrics_port = config.metrics.port + 1;
    tokio::spawn(async move {
        info!("Starting metrics server on port {}", metrics_port);
        if let Err(e) = arachne_core::metrics::serve_metrics(metrics_clone, metrics_port).await {
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

    let repo = Arc::new(
        ArachneRepo::new(&config.scylla)
            .await
            .context("Failed to connect to ScyllaDB")?,
    );

    let deduplicator = Arc::new(Deduplicator::new(
        config.coordinator.dedup_bloom_capacity,
        config.coordinator.dedup_bloom_fp_rate,
    ));

    let domain_counts = Arc::new(DashMap::<String, i64>::new());
    let job_counts = Arc::new(DashMap::<Uuid, u64>::new());
    let job_limits = Arc::new(DashMap::<Uuid, Option<u64>>::new());

    let max_pages_per_domain = config.coordinator.max_pages_per_domain;
    let batch_size = config.coordinator.batch_size;

    let (shutdown_tx, shutdown_rx_results) = tokio::sync::broadcast::channel(1);
    let shutdown_rx_discovery = shutdown_tx.subscribe();

    let result_processor = tokio::spawn(process_results(
        Arc::clone(&nats),
        Arc::clone(&repo),
        Arc::clone(&metrics),
        batch_size,
        shutdown_rx_results,
    ));

    let discovery_processor = tokio::spawn(process_discovered_urls(
        Arc::clone(&nats),
        Arc::clone(&repo),
        Arc::clone(&deduplicator),
        Arc::clone(&domain_counts),
        Arc::clone(&job_counts),
        Arc::clone(&job_limits),
        Arc::clone(&metrics),
        max_pages_per_domain,
        batch_size,
        shutdown_rx_discovery,
    ));

    wait_for_shutdown().await;
    info!("Shutdown signal received, initiating graceful shutdown...");
    let _ = shutdown_tx.send(());

    let _ = tokio::join!(result_processor, discovery_processor);

    info!("Arachne Coordinator stopped.");
    Ok(())
}

async fn process_results(
    nats: Arc<NatsManager>,
    repo: Arc<ArachneRepo>,
    metrics: Arc<CrawlerMetrics>,
    batch_size: usize,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) {
    info!("Result processor loop started");

    let consumer = match nats.create_result_consumer().await {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to create result consumer: {:?}", e);
            return;
        }
    };

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Result processor received shutdown signal");
                break;
            }
            fetch_res = consumer.fetch().max_messages(batch_size).messages() => {
                match fetch_res {
                    Ok(mut messages) => {
                        let mut db_batch = Vec::new();
                        let mut msgs_to_ack = Vec::new();

                        while let Some(msg_res) = messages.next().await {
                            match msg_res {
                                Ok(msg) => {
                                    if let Ok(result) = serde_json::from_slice::<CrawlResult>(&msg.payload) {
                                        if result.status.is_success() {
                                            metrics.pages_crawled.inc();
                                        } else {
                                            metrics.pages_failed.inc();
                                        }

                                        let domain_name = result.domain.clone().unwrap_or_else(|| "unknown".to_string());
                                        db_batch.push((domain_name, result));
                                        msgs_to_ack.push(msg);
                                    } else {
                                        let _ = msg.ack().await;
                                    }
                                }
                                Err(e) => {
                                    warn!("Error consuming from result stream: {:?}", e);
                                }
                            }
                        }

                        if !db_batch.is_empty() {
                            if let Err(e) = repo.insert_crawl_results_batch(&db_batch).await {
                                error!("Failed to batch persist results to ScyllaDB: {:?}", e);
                            }
                            for msg in msgs_to_ack {
                                let _ = msg.ack().await;
                            }
                        }
                    }
                    Err(e) => {
                        debug!("Result fetch timeout or error: {:?}", e);
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    }
                }
            }
        }
    }
}

async fn process_discovered_urls(
    nats: Arc<NatsManager>,
    repo: Arc<ArachneRepo>,
    deduplicator: Arc<Deduplicator>,
    domain_counts: Arc<DashMap<String, i64>>,
    job_counts: Arc<DashMap<Uuid, u64>>,
    job_limits: Arc<DashMap<Uuid, Option<u64>>>,
    metrics: Arc<CrawlerMetrics>,
    max_pages_per_domain: i64,
    batch_size: usize,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) {
    info!("Discovery processor loop started");

    let consumer = match nats.create_discovery_consumer().await {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to create discovery consumer: {:?}", e);
            return;
        }
    };

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Discovery processor received shutdown signal");
                break;
            }
            fetch_res = consumer.fetch().max_messages(batch_size).messages() => {
                match fetch_res {
                    Ok(mut messages) => {
                        let mut candidates = Vec::new();
                        let mut raw_msgs = Vec::new();

                        while let Some(msg_res) = messages.next().await {
                            match msg_res {
                                Ok(msg) => {
                                    if let Ok(url_msg) = serde_json::from_slice::<DiscoveredUrl>(&msg.payload) {
                                        candidates.push(url_msg);
                                        raw_msgs.push(msg);
                                    } else {
                                        let _ = msg.ack().await;
                                    }
                                }
                                Err(e) => {
                                    warn!("Error fetching discovered URL message: {:?}", e);
                                }
                            }
                        }

                        if candidates.is_empty() {
                            continue;
                        }

                        metrics.urls_discovered.inc_by(candidates.len() as u64);

                        let mut db_checks = Vec::new();
                        let mut filtered_candidates = Vec::new();
                        let mut candidate_msgs = Vec::new();

                        for (idx, candidate) in candidates.into_iter().enumerate() {
                            let msg = &raw_msgs[idx];
                            let normalized_url = match domain::normalize_url(&candidate.url) {
                                Some(u) => u,
                                None => {
                                    let _ = msg.ack().await;
                                    continue;
                                }
                            };

                            if deduplicator.probably_seen(&normalized_url) {
                                metrics.urls_deduped.inc();
                                let _ = msg.ack().await;
                                continue;
                            }

                            let root_domain = domain::extract_root_domain(&normalized_url)
                                .unwrap_or_else(|| "unknown".to_string());

                            db_checks.push((root_domain.clone(), normalized_url.clone()));

                            let mut candidate_copy = candidate;
                            candidate_copy.url = normalized_url;
                            filtered_candidates.push((root_domain, candidate_copy));
                            candidate_msgs.push(msg);
                        }

                        let existing_urls = match repo.check_urls_batch(db_checks).await {
                            Ok(set) => set,
                            Err(e) => {
                                error!("Failed to check URL batch in DB: {:?}", e);
                                HashSet::new()
                            }
                        };

                        let mut tasks_to_publish = Vec::new();
                        let mut msgs_to_ack = Vec::new();

                        for (idx, (root_domain, candidate)) in filtered_candidates.into_iter().enumerate() {
                            let msg = candidate_msgs[idx];

                            if existing_urls.contains(&candidate.url) {
                                deduplicator.mark_seen(&candidate.url);
                                metrics.urls_deduped.inc();
                                let _ = msg.ack().await;
                                continue;
                            }

                            let d_count = *domain_counts.entry(root_domain.clone()).or_insert(0);
                            if d_count >= max_pages_per_domain {
                                debug!(domain = %root_domain, "Domain limit reached, skipping URL");
                                let _ = msg.ack().await;
                                continue;
                            }

                            let job_id = candidate.job_id;
                            if !job_limits.contains_key(&job_id) {
                                if let Ok(Some(job)) = repo.get_job(&job_id).await {
                                    job_limits.insert(job_id, job.max_pages);
                                }
                            }

                            if let Some(Some(limit)) = job_limits.get(&job_id).map(|r| *r) {
                                let current_job_count = *job_counts.entry(job_id).or_insert(0);
                                if current_job_count >= limit {
                                    debug!(job_id = %job_id, "Job limit reached, skipping URL");
                                    let _ = msg.ack().await;
                                    continue;
                                }
                            }

                            deduplicator.mark_seen(&candidate.url);
                            *domain_counts.entry(root_domain.clone()).or_insert(0) += 1;
                            *job_counts.entry(job_id).or_insert(0) += 1;

                            let task = CrawlTask {
                                url: candidate.url,
                                job_id: candidate.job_id,
                                domain: root_domain,
                                depth: candidate.depth,
                                priority: 1,
                            };

                            tasks_to_publish.push(task);
                            msgs_to_ack.push(msg);
                        }

                        if !tasks_to_publish.is_empty() {
                            if let Err(e) = nats.publish_tasks_batch(&tasks_to_publish).await {
                                error!("Failed to batch dispatch tasks to NATS: {:?}", e);
                            } else {
                                for msg in msgs_to_ack {
                                    let _ = msg.ack().await;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        debug!("Discovery fetch timeout or error: {:?}", e);
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    }
                }
            }
        }
    }
}

async fn wait_for_shutdown() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
