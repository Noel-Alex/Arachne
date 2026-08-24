use anyhow::{Context, Result};
use arachne_core::{
    config::ArachneConfig,
    db::ArachneRepo,
    dedup::Deduplicator,
    domain, logging,
    metrics::CrawlerMetrics,
    models::{CrawlJob, CrawlResult, CrawlStatus, CrawlTask, DiscoveredUrl, TaskKind, TrackRecord, TrackStatus},
    nats::NatsManager,
};
use dashmap::DashMap;
use std::time::{Duration, Instant};
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
        ArachneRepo::new(&config)
            .await
            .context("Failed to connect to database")?,
    );

    let deduplicator = Arc::new(Deduplicator::new(
        config.coordinator.dedup_bloom_capacity,
        config.coordinator.dedup_bloom_fp_rate,
    ));

    let domain_counts = Arc::new(DashMap::<String, i64>::new());
    let job_counts = Arc::new(DashMap::<Uuid, u64>::new());
    let jobs_cache: Arc<DashMap<Uuid, (Option<CrawlJob>, Instant)>> = Arc::new(DashMap::new());
    let job_cache_ttl = Duration::from_secs(60);

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
        DiscoveryState {
            nats: Arc::clone(&nats),
            repo: Arc::clone(&repo),
            deduplicator: Arc::clone(&deduplicator),
            domain_counts: Arc::clone(&domain_counts),
            job_counts: Arc::clone(&job_counts),
            jobs_cache: Arc::clone(&jobs_cache),
            job_cache_ttl,
            metrics: Arc::clone(&metrics),
            max_pages_per_domain,
        },
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

                                        // Audio tasks also complete the track manifest.
                                        if result.media_meta.is_some()
                                            && let Err(e) = complete_track_record(&repo, &result).await
                                        {
                                            error!("Failed to update track manifest: {:?}", e);
                                            // Don't ack: redelivery will retry the manifest write.
                                            continue;
                                        }

                                        let domain_name = result.domain.clone().unwrap_or_else(|| "unknown".to_string());
                                        db_batch.push((domain_name, result));
                                        msgs_to_ack.push(msg);
                                    } else {
                                        // Poison pill: undecodable payloads are
                                        // acked (infinite redelivery helps no
                                        // one) but loudly counted + logged.
                                        metrics.messages_malformed.inc();
                                        warn!(
                                            bytes = msg.payload.len(),
                                            "undecodable crawl result dropped"
                                        );
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
                                // DO NOT ACK if DB insertion fails - allow NATS redelivery!
                            } else {
                                for msg in msgs_to_ack {
                                    let _ = msg.ack().await;
                                }
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

/// Transition the track manifest row for an audio result. The coordinator is
/// the single writer for status transitions; workers only download. Rows are
/// created on demand so results arriving before a manifest insert still land.
async fn complete_track_record(repo: &ArachneRepo, result: &CrawlResult) -> Result<()> {
    // The task's MediaMeta is the identity anchor (source, source_id, license).
    let meta = match &result.media_meta {
        Some(m) => m,
        None => return Ok(()),
    };

    let status = match &result.status {
        CrawlStatus::Success => TrackStatus::Done,
        CrawlStatus::ProbeFailed | CrawlStatus::QualityRejected => TrackStatus::Rejected,
        _ => TrackStatus::Failed,
    };

    let probe = result.media_probe.as_ref();
    let record = TrackRecord {
        source: meta.source.clone(),
        source_id: meta.source_id.clone(),
        job_id: result.job_id,
        url: result.source_url.clone(),
        title: result.title.clone().or_else(|| meta.title.clone()),
        artist: probe.and_then(|p| p.artist.clone()).or_else(|| meta.artist.clone()),
        album: probe.and_then(|p| p.album.clone()).or_else(|| meta.album.clone()),
        year: probe.and_then(|p| p.year),
        genre: probe.and_then(|p| p.genre.clone()),
        license: meta.license.clone(),
        license_url: meta.license_url.clone(),
        origin_page_url: meta.origin_page_url.clone(),
        discovered_from_url: meta.discovered_from_url.clone(),
        collection: meta.collection.clone(),
        duration_secs: probe.map(|p| p.duration_secs),
        bitrate_kbps: probe.and_then(|p| p.bitrate_kbps),
        format: probe.map(|p| p.format.clone()),
        sha256: result.content_hash.clone().filter(|h| h.len() == 64),
        bytes: result.content_length.map(|l| l as i64),
        // Store the resolvable reference (file:// or object://) so consumers can locate audio.
        object_path: result.content_ref.clone(),
        status,
        error: None,
    };
    let mut record = record;
    if !result.status.is_success() {
        record.error = Some(format!("{:?}", result.status));
    }

    repo.upsert_track(&record).await
}

/// Stable content id for organically-discovered audio (no source adapter id).
fn md5_of(s: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut h);
    h.finish()
}

/// Shared state for the discovery pipeline (admission control + dedup + dispatch).
struct DiscoveryState {
    nats: Arc<NatsManager>,
    repo: Arc<ArachneRepo>,
    deduplicator: Arc<Deduplicator>,
    domain_counts: Arc<DashMap<String, i64>>,
    job_counts: Arc<DashMap<Uuid, u64>>,
    jobs_cache: Arc<DashMap<Uuid, (Option<CrawlJob>, Instant)>>,
    /// How long a cached job row is trusted before re-reading from the DB.
    /// Keeps pause/cancel responsive without a DB hit per URL.
    job_cache_ttl: Duration,
    metrics: Arc<CrawlerMetrics>,
    max_pages_per_domain: i64,
}

async fn process_discovered_urls(
    state: DiscoveryState,
    batch_size: usize,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) {
    let DiscoveryState {
        nats,
        repo,
        deduplicator,
        domain_counts,
        job_counts,
        jobs_cache,
        job_cache_ttl,
        metrics,
        max_pages_per_domain,
    } = state;
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
                                    // Parse both Vec<DiscoveredUrl> and single DiscoveredUrl
                                    if let Ok(url_vec) = serde_json::from_slice::<Vec<DiscoveredUrl>>(&msg.payload) {
                                        for url_msg in url_vec {
                                            candidates.push(url_msg);
                                        }
                                        raw_msgs.push(msg);
                                    } else if let Ok(url_msg) = serde_json::from_slice::<DiscoveredUrl>(&msg.payload) {
                                        candidates.push(url_msg);
                                        raw_msgs.push(msg);
                                    } else {
                                        metrics.messages_malformed.inc();
                                        warn!(
                                            bytes = msg.payload.len(),
                                            "undecodable discovered-URL message dropped"
                                        );
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

                        for candidate in candidates {
                            let normalized_url = match domain::normalize_url(&candidate.url) {
                                Some(u) => u,
                                None => continue,
                            };

                            let root_domain = domain::extract_root_domain(&normalized_url)
                                .unwrap_or_else(|| "unknown".to_string());

                            // Check job policy. Cache entries carry a TTL so
                            // pause/cancel status flips propagate without a
                            // per-URL DB read.
                            let job_id = candidate.job_id;
                            let needs_load = match jobs_cache.get(&job_id) {
                                Some(entry) => entry.1.elapsed() >= job_cache_ttl,
                                None => true,
                            };
                            if needs_load {
                                let loaded = repo.get_job(&job_id).await.ok().flatten();
                                jobs_cache.insert(job_id, (loaded, Instant::now()));
                            }

                            if let Some(Some(job)) = jobs_cache.get(&job_id).map(|e| e.0.clone()) {
                                // Terminal/paused jobs admit nothing new.
                                if !matches!(job.status, arachne_core::models::JobStatus::Running | arachne_core::models::JobStatus::Pending)
                                {
                                    debug!(job_id = %job_id, "job not running; skipping admission");
                                    continue;
                                }
                                if !job.is_url_allowed(&normalized_url, candidate.depth, &root_domain) {
                                    debug!(url = %normalized_url, "URL disallowed by job crawl policy");
                                    continue;
                                }
                            }

                            db_checks.push((root_domain.clone(), job_id, normalized_url.clone()));

                            let mut candidate_copy = candidate;
                            candidate_copy.url = normalized_url;
                            filtered_candidates.push((root_domain, candidate_copy));
                        }

                        let existing_urls = match repo.check_urls_batch(db_checks).await {
                            Ok(set) => set,
                            Err(e) => {
                                error!("Failed to check URL batch in DB: {:?}", e);
                                HashSet::new()
                            }
                        };

                        let mut tasks_to_publish = Vec::new();

                        for (root_domain, candidate) in filtered_candidates {
                            if existing_urls.contains(&candidate.url) {
                                deduplicator.mark_seen(&candidate.url);
                                metrics.urls_deduped.inc();
                                continue;
                            }

                            let d_count = *domain_counts.entry(root_domain.clone()).or_insert(0);
                            if d_count >= max_pages_per_domain {
                                debug!(domain = %root_domain, "Domain limit reached, skipping URL");
                                continue;
                            }

                            let job_id = candidate.job_id;
                            if let Some(Some(job)) = jobs_cache.get(&job_id).map(|e| e.0.clone())
                                && let Some(limit) = job.max_pages {
                                    let current_job_count = *job_counts.entry(job_id).or_insert(0);
                                    if current_job_count >= limit {
                                        debug!(job_id = %job_id, "Job limit reached, skipping URL");
                                        continue;
                                    }
                                }

                            *domain_counts.entry(root_domain.clone()).or_insert(0) += 1;
                            *job_counts.entry(job_id).or_insert(0) += 1;

                            // Organic media discovery: extension-matched URLs
                            // become media tasks licensed by the job's
                            // default_license (if any). No license ⇒ no task.
                            let is_audio =
                                arachne_core::discovery::audio_links::has_audio_extension(&candidate.url);
                            let is_media = is_audio
                                || arachne_core::discovery::media_links::has_video_extension(&candidate.url)
                                || arachne_core::discovery::media_links::has_document_extension(&candidate.url);
                            let (kind, media) = if is_media {
                                let license = jobs_cache.get(&job_id).and_then(|e| e.0.as_ref().cloned()).and_then(|j| j.default_license.clone());
                                match license {
                                    Some(l) => {
                                        let media_kind = if is_audio {
                                            TaskKind::AudioFile
                                        } else if arachne_core::discovery::media_links::has_video_extension(&candidate.url) {
                                            TaskKind::VideoFile
                                        } else {
                                            TaskKind::DocumentFile
                                        };
                                        (
                                        media_kind,
                                        Some(arachne_core::models::MediaMeta {
                                            source_id: format!("{:x}", md5_of(&candidate.url)),
                                            source: "discovered".into(),
                                            collection: None,
                                            license: l,
                                            // Provenance: remember the page that
                                            // linked this media so every stored
                                            // file traces back to its origin.
                                            origin_page_url: Some(candidate.source_url.clone()),
                                            license_url: None,
                                            discovered_from_url: Some(candidate.source_url.clone()),
                                            title: None,
                                            artist: None,
                                            album: None,
                                        }),
                                    )
                                    }
                                    None => {
                                        debug!(url = %candidate.url, "media URL without default_license; skipping");
                                        continue;
                                    }
                                }
                            } else {
                                (TaskKind::Page, None)
                            };

                            let task = CrawlTask {
                                url: candidate.url,
                                job_id: candidate.job_id,
                                domain: root_domain,
                                depth: candidate.depth,
                                priority: 1,
                                kind,
                                media,
                            };

                            tasks_to_publish.push(task);
                        }

                        if !tasks_to_publish.is_empty() {
                            if let Err(e) = nats.publish_tasks_batch(&tasks_to_publish).await {
                                error!("Failed to batch dispatch tasks to NATS: {:?}", e);
                            } else {
                                // Mark seen in Bloom ONLY AFTER successful NATS task publication
                                for t in &tasks_to_publish {
                                    deduplicator.mark_seen(&t.url);
                                }
                                for msg in raw_msgs {
                                    let _ = msg.ack().await;
                                }
                            }
                        } else {
                            for msg in raw_msgs {
                                let _ = msg.ack().await;
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
