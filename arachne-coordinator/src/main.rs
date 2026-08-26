use anyhow::{Context, Result};
use arachne_core::{
    config::ArachneConfig,
    db::ArachneRepo,
    dedup::Deduplicator,
    discovery::{audio_links, media_links},
    domain, logging,
    metrics::CrawlerMetrics,
    models::{
        CrawlJob, CrawlResult, CrawlStatus, CrawlTask, DiscoveredUrl, MediaMeta, TaskKind,
        TrackRecord, TrackStatus,
    },
    nats::NatsManager,
};
use dashmap::DashMap;
use futures::StreamExt;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::signal;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// A cached job row (`None` = job absent from DB), shared via `Arc` so reads
/// clone a pointer, not a full `CrawlJob`, per candidate.
type CachedJob = Option<Arc<CrawlJob>>;

#[tokio::main]
async fn main() -> Result<()> {
    logging::init_logging();
    info!("Starting Arachne Coordinator");

    let config = ArachneConfig::load(None).context("Failed to load configuration")?;

    let metrics = Arc::new(CrawlerMetrics::new());
    let metrics_clone = Arc::clone(&metrics);
    let metrics_port = config.metrics.port + 1;
    if config.metrics.enabled {
        tokio::spawn(async move {
            info!("Starting metrics server on port {}", metrics_port);
            if let Err(e) = arachne_core::metrics::serve_metrics(metrics_clone, metrics_port).await
            {
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
    let jobs_cache: Arc<DashMap<Uuid, (CachedJob, Instant)>> = Arc::new(DashMap::new());
    // Root domains of each job's seeds (see fill_seed_roots): needed to admit
    // internal links for follow_external_links=false jobs with no allowlist.
    let seed_roots_by_job: Arc<DashMap<Uuid, Arc<Vec<String>>>> = Arc::new(DashMap::new());
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
            seed_roots_by_job: Arc::clone(&seed_roots_by_job),
            job_cache_ttl,
            metrics: Arc::clone(&metrics),
            max_pages_per_domain,
        },
        batch_size,
        shutdown_rx_discovery,
    ));

    // Gauge refresher: feeds live frontier depth + running-job counts so the
    // Grafana dashboard can plot them instead of flat zeros.
    let metrics_refresher = {
        let nats = Arc::clone(&nats);
        let repo = Arc::clone(&repo);
        let metrics = Arc::clone(&metrics);
        let mut shutdown_rx_metrics = shutdown_tx.subscribe();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(15));
            loop {
                tokio::select! {
                    _ = shutdown_rx_metrics.recv() => break,
                    _ = ticker.tick() => {
                        match nats.stream_task_count().await {
                            Ok(count) => metrics.frontier_size.set(count as i64),
                            Err(e) => debug!("frontier_size refresh failed, skipping tick: {e:?}"),
                        }
                        match repo.list_jobs().await {
                            Ok(jobs) => metrics.jobs_running.set(
                                jobs.iter()
                                    .filter(|j| j.status == arachne_core::models::JobStatus::Running)
                                    .count() as i64,
                            ),
                            Err(e) => debug!("jobs_running refresh failed, skipping tick: {e:?}"),
                        }
                    }
                }
            }
        })
    };

    wait_for_shutdown().await;
    info!("Shutdown signal received, initiating graceful shutdown...");
    let _ = shutdown_tx.send(());
    let _ = metrics_refresher.await;

    let _ = tokio::join!(result_processor, discovery_processor);

    info!("Arachne Coordinator stopped.");
    Ok(())
}

/// One deferred track-manifest write: (root_domain, result, attempts so far).
type ManifestRetry = (String, CrawlResult, u32);
/// Upper bound on queued manifest retries. Shedding at capacity is safe: the
/// page row is already durable, only the derived track record would be lost.
const MANIFEST_RETRY_CAP: usize = 10_000;
/// Give up on a manifest entry after this many background attempts.
const MAX_MANIFEST_ATTEMPTS: u32 = 10;

/// Queue a failed manifest write for the 30s retry task, keeping the queue
/// bounded by dropping the oldest entry at capacity.
fn queue_manifest_retry(retries: &Mutex<Vec<ManifestRetry>>, domain: String, result: CrawlResult) {
    let mut q = retries.lock().expect("manifest retry queue poisoned");
    if q.len() >= MANIFEST_RETRY_CAP {
        error!("manifest retry queue full; dropping oldest pending manifest");
        q.remove(0);
    }
    q.push((domain, result, 0));
}

async fn process_results(
    nats: Arc<NatsManager>,
    repo: Arc<ArachneRepo>,
    metrics: Arc<CrawlerMetrics>,
    batch_size: usize,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) {
    info!("Result processor loop started");

    // Background lane for track-manifest writes that failed AFTER their page
    // row was durably persisted. They are retried every 30s, up to
    // MAX_MANIFEST_ATTEMPTS times, so a transient manifest failure never
    // costs the message its ack (withholding it risked losing the page row
    // outright once max_deliver was exhausted). In-memory only: across a
    // coordinator restart the page rows survive, pending manifests are lost.
    let manifest_retries: Arc<Mutex<Vec<ManifestRetry>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let repo = Arc::clone(&repo);
        let manifest_retries = Arc::clone(&manifest_retries);
        let mut shutdown_rx_retries = shutdown_rx.resubscribe();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(30));
            loop {
                tokio::select! {
                    _ = shutdown_rx_retries.recv() => break,
                    _ = ticker.tick() => {
                        let pending: Vec<ManifestRetry> = manifest_retries
                            .lock()
                            .expect("manifest retry queue poisoned")
                            .drain(..)
                            .collect();
                        for (domain, result, attempts) in pending {
                            let attempts = attempts + 1;
                            if let Err(e) = complete_track_record(&repo, &result).await {
                                if attempts < MAX_MANIFEST_ATTEMPTS {
                                    warn!(
                                        attempts,
                                        "track manifest retry failed; requeueing: {e:?}"
                                    );
                                    // Requeue only while under the cap; at
                                    // capacity this attempt is shed entirely.
                                    let mut q =
                                        manifest_retries.lock().expect("manifest retry queue poisoned");
                                    if q.len() < MANIFEST_RETRY_CAP {
                                        q.push((domain, result, attempts));
                                    }
                                    continue;
                                }
                                error!(
                                    attempts,
                                    "track manifest retry failed; giving up: {e:?}"
                                );
                            }
                        }
                    }
                }
            }
        });
    }

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
            // Clamp effective pull size (500 msgs ≈ well under ack_wait even
            // at ~1ms/row of DB write) to bound ACK latency per batch.
            fetch_res = consumer.fetch().max_messages(batch_size.min(500)).messages() => {
                match fetch_res {
                    Ok(mut messages) => {
                        let mut db_batch = Vec::new();
                        let mut msgs_to_ack = Vec::new();

                        while let Some(msg_res) = messages.next().await {
                            match msg_res {
                                Ok(msg) => {
                                    if let Ok(result) = serde_json::from_slice::<CrawlResult>(&msg.payload) {
                                        // NOTE: worker also counts locally for its own /metrics endpoint; coordinator counts are fleet-truth.
                                        if result.status.is_success() {
                                            metrics.pages_crawled.inc();
                                        } else {
                                            metrics.pages_failed.inc();
                                        }

                                        // Persist the page row FIRST: the track
                                        // manifest is a derived write we can
                                        // retry losslessly afterwards, the crawl
                                        // result itself is not. Manifest
                                        // completion runs once the batch insert
                                        // has succeeded (below).
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
                                error!("Failed to batch persist crawl results: {:?}", e);
                                // DO NOT ACK if DB insertion fails - allow NATS redelivery!
                            } else {
                                // Page rows are now durable: finish the derived
                                // track-manifest writes, queueing failures for
                                // the 30s retry task instead of withholding
                                // acks — redelivery is finite (max_deliver=3)
                                // and would take the page row down with it.
                                for (domain_name, result) in &db_batch {
                                    if result.media_meta.is_some()
                                        && let Err(e) = complete_track_record(&repo, result).await
                                    {
                                        error!("track manifest update failed; queueing retry: {e:?}");
                                        queue_manifest_retry(
                                            &manifest_retries,
                                            domain_name.clone(),
                                            result.clone(),
                                        );
                                    }
                                }
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
        artist: probe
            .and_then(|p| p.artist.clone())
            .or_else(|| meta.artist.clone()),
        album: probe
            .and_then(|p| p.album.clone())
            .or_else(|| meta.album.clone()),
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

/// Stable content id for organically-discovered media (no source adapter id).
/// SHA-256 of the URL, first 16 hex chars — stable across restarts and Rust
/// versions, unlike `DefaultHasher` whose algorithm is unspecified.
fn stable_id_of(s: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(s.as_bytes());
    hex::encode(&digest[..8])
}

/// Populate (or refresh) the seed root-domain set for one job.
///
/// Only computed for jobs where it can matter — `follow_external_links ==
/// false` with no effective allowlist (`None` or empty; an empty allowlist is
/// treated as unset by both `CrawlJob::is_url_allowed` and the policy gate).
/// For every other job no admission decision consults seed roots, so the map
/// entry is removed to keep memory bounded across many jobs.
///
/// Called right after a jobs_cache refresh so the set always reflects the
/// job's current seeds. Unparseable seeds are skipped; if none parse, an
/// empty set is stored (which denies all candidates — correct, since a job
/// whose seeds never normalized has no internal lineage).
fn fill_seed_roots(
    seed_roots_by_job: &DashMap<Uuid, Arc<Vec<String>>>,
    jobs_cache: &DashMap<Uuid, (CachedJob, Instant)>,
    job_id: &Uuid,
) {
    let relevant = match jobs_cache.get(job_id).and_then(|e| e.0.clone()) {
        Some(job) => {
            !job.follow_external_links && job.allowed_domains.as_ref().is_none_or(|d| d.is_empty())
        }
        None => false,
    };
    if !relevant {
        seed_roots_by_job.remove(job_id);
        return;
    }

    let mut roots: Vec<String> = jobs_cache
        .get(job_id)
        .and_then(|e| e.0.clone())
        .map(|job| {
            job.seed_urls
                .iter()
                .filter_map(|seed| domain::extract_root_domain(seed))
                .collect()
        })
        .unwrap_or_default();
    roots.sort();
    roots.dedup();
    seed_roots_by_job.insert(*job_id, Arc::new(roots));
}

/// Shared state for the discovery pipeline (admission control + dedup + dispatch).
struct DiscoveryState {
    nats: Arc<NatsManager>,
    repo: Arc<ArachneRepo>,
    deduplicator: Arc<Deduplicator>,
    domain_counts: Arc<DashMap<String, i64>>,
    job_counts: Arc<DashMap<Uuid, u64>>,
    jobs_cache: Arc<DashMap<Uuid, (CachedJob, Instant)>>,
    /// Root domains of each job's seed URLs, per `fill_seed_roots`. Consulted
    /// by the external-links gate for jobs that neither allowlist domains nor
    /// follow external links. A Vec (not HashSet) because job seed sets are
    /// tiny and `external_links_allowed` consumes a slice; dedup is a freebie.
    seed_roots_by_job: Arc<DashMap<Uuid, Arc<Vec<String>>>>,
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
        seed_roots_by_job,
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
            // Clamp effective pull size (500 msgs ≈ well under ack_wait even
            // at ~1ms/row of DB write) to bound ACK latency per batch.
            fetch_res = consumer.fetch().max_messages(batch_size.min(500)).messages() => {
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
                        // Within-batch dedup: the same URL twice in one fetch
                        // must not yield two published tasks.
                        let mut batch_seen: HashSet<String> = HashSet::new();

                        for candidate in candidates {
                            let normalized_url = match domain::normalize_url(&candidate.url) {
                                Some(u) => u,
                                None => continue,
                            };

                            if !batch_seen.insert(normalized_url.clone()) {
                                metrics.urls_deduped.inc();
                                continue;
                            }

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
                                match repo.get_job(&job_id).await {
                                    Ok(loaded) => {
                                        jobs_cache
                                            .insert(job_id, (loaded.map(Arc::new), Instant::now()));
                                        fill_seed_roots(&seed_roots_by_job, &jobs_cache, &job_id);
                                    }
                                    Err(e) => {
                                        // A transport error must NOT overwrite
                                        // the cached entry (caching None would
                                        // disable every policy gate — allowlist,
                                        // excludes, depth, caps, license — for a
                                        // full TTL). Leave the entry untouched
                                        // and skip this candidate instead:
                                        // conservative availability tradeoff;
                                        // redelivery retries once the DB heals.
                                        warn!(job_id = %job_id, error = ?e, "job policy load failed");
                                        debug!("job policy unavailable; skipping");
                                        continue;
                                    }
                                }
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
                                // follow_external_links=false with no
                                // allowlist: only the job's own seed domains
                                // count as "internal".
                                let seed_roots = seed_roots_by_job.get(&job_id).map(|r| r.clone());
                                if !arachne_core::policy::external_links_allowed(
                                    Some(&job),
                                    &root_domain,
                                    seed_roots.as_deref().map(|s| s.as_slice()),
                                ) {
                                    debug!(url = %normalized_url, job_id = %job_id, "URL disallowed: external link on no-external job without seed lineage");
                                    continue;
                                }
                            }

                            // Bloom pre-filter: skip URLs this process already
                            // dispatched, avoiding a DB existence check for
                            // each repeat. False positives permanently drop a
                            // URL (it is never re-admitted), which is the
                            // accepted tradeoff at capacity 100M / fp 0.001 —
                            // the DB batch-check below stays authoritative for
                            // cross-restart truth.
                            if deduplicator.probably_seen(&normalized_url) {
                                metrics.urls_deduped.inc();
                                continue;
                            }

                            db_checks.push((root_domain.clone(), job_id, normalized_url.clone()));

                            let mut candidate_copy = candidate;
                            candidate_copy.url = normalized_url;
                            filtered_candidates.push((root_domain, candidate_copy));
                        }

                        let existing_urls = match repo.check_urls_batch(db_checks).await {
                            Ok(set) => set,
                            Err(e) => {
                                // Fail closed: falling open to an empty set
                                // would republish every candidate as a
                                // duplicate task during a DB outage. Drop the
                                // whole candidate list for this batch and
                                // leave the discovery messages unacked so
                                // JetStream redelivers them once the DB
                                // answers again — the same no-ack-on-failure
                                // discipline used for result persistence.
                                error!("Failed to check URL batch in DB: {:?}", e);
                                continue;
                            }
                        };

                        let mut tasks_to_publish = Vec::new();

                        for (root_domain, candidate) in filtered_candidates {
                            if existing_urls.contains(&candidate.url) {
                                deduplicator.mark_seen(&candidate.url);
                                metrics.urls_deduped.inc();
                                continue;
                            }

                            // Decide kind/media/license BEFORE consuming any
                            // budget: candidates dropped below (licenseless
                            // media) must not permanently occupy domain/job
                            // slots. Counters increment only once this task is
                            // certain to be pushed onto tasks_to_publish.
                            let job_id = candidate.job_id;
                            let is_audio = audio_links::has_audio_extension(&candidate.url);
                            let is_media = is_audio
                                || media_links::has_video_extension(&candidate.url)
                                || media_links::has_document_extension(&candidate.url);
                            let (kind, media) = if is_media {
                                let license = jobs_cache
                                    .get(&job_id)
                                    .and_then(|e| e.0.clone())
                                    .and_then(|j| j.default_license.clone());
                                match license {
                                    Some(l) => {
                                        let media_kind = if is_audio {
                                            TaskKind::AudioFile
                                        } else if media_links::has_video_extension(&candidate.url) {
                                            TaskKind::VideoFile
                                        } else {
                                            TaskKind::DocumentFile
                                        };
                                        (
                                            media_kind,
                                            Some(MediaMeta {
                                                source_id: stable_id_of(&candidate.url),
                                                source: "discovered".into(),
                                                collection: None,
                                                license: l,
                                                // Provenance: remember the page that
                                                // linked this media so every stored
                                                // file traces back to its origin.
                                                origin_page_url: Some(
                                                    candidate.source_url.clone(),
                                                ),
                                                license_url: None,
                                                discovered_from_url: Some(
                                                    candidate.source_url.clone(),
                                                ),
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

                            let d_count = *domain_counts.entry(root_domain.clone()).or_insert(0);
                            if d_count >= max_pages_per_domain {
                                debug!(domain = %root_domain, "Domain limit reached, skipping URL");
                                continue;
                            }

                            if let Some(Some(job)) = jobs_cache.get(&job_id).map(|e| e.0.clone()) {
                                // Job-level domain cap, on top of the global one.
                                if let Some(cap) = job.max_pages_per_domain
                                    && d_count >= cap
                                {
                                    debug!(job_id = %job_id, domain = %root_domain, "Job domain limit reached, skipping URL");
                                    continue;
                                }
                                if let Some(limit) = job.max_pages {
                                    let current_job_count = *job_counts.entry(job_id).or_insert(0);
                                    if current_job_count >= limit {
                                        debug!(job_id = %job_id, "Job limit reached, skipping URL");
                                        continue;
                                    }
                                }
                            }

                            // The candidate is definitely publishing now:
                            // only here do the budgets get consumed.
                            *domain_counts.entry(root_domain.clone()).or_insert(0) += 1;
                            *job_counts.entry(job_id).or_insert(0) += 1;

                            let task = CrawlTask {
                                url: candidate.url,
                                job_id,
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

#[cfg(test)]
mod tests {
    use super::*;
    use super::{fill_seed_roots, stable_id_of};

    #[test]
    fn stable_id_is_deterministic() {
        assert_eq!(
            stable_id_of("https://x/a.mp3"),
            stable_id_of("https://x/a.mp3")
        );
    }

    fn seed_job(policies: impl FnOnce(&mut CrawlJob)) -> CrawlJob {
        let mut job = CrawlJob {
            seed_urls: vec![
                "https://www.example.com/start".to_string(),
                "not a url".to_string(), // skipped: unparseable
            ],
            ..CrawlJob::default()
        };
        policies(&mut job);
        job
    }

    #[test]
    fn fill_seeds_populates_for_gate_relevant_jobs() {
        let jobs = DashMap::new();
        let roots_map = DashMap::new();
        let job = seed_job(|j| {
            j.follow_external_links = false;
            j.allowed_domains = None;
        });
        let id = job.id;
        jobs.insert(id, (Some(Arc::new(job)), Instant::now()));

        fill_seed_roots(&roots_map, &jobs, &id);
        let roots = roots_map.get(&id).unwrap().clone();
        assert_eq!(roots.len(), 1);
        assert!(roots.iter().any(|r| r == "example.com"));
    }

    #[test]
    fn fill_seeds_skips_and_clears_irrelevant_jobs() {
        let jobs = DashMap::new();
        let roots_map = DashMap::new();

        // External-following job: irrelevant, entry removed.
        let external = seed_job(|j| j.follow_external_links = true);
        let ext_id = external.id;
        jobs.insert(ext_id, (Some(Arc::new(external)), Instant::now()));
        roots_map.insert(ext_id, Arc::new(vec!["stale.example".to_string()]));
        fill_seed_roots(&roots_map, &jobs, &ext_id);
        assert!(!roots_map.contains_key(&ext_id));

        // Non-empty allowlist already gates domains: irrelevant too.
        let allowlisted = seed_job(|j| {
            j.follow_external_links = false;
            j.allowed_domains = Some(vec!["example.com".into()]);
        });
        let al_id = allowlisted.id;
        jobs.insert(al_id, (Some(Arc::new(allowlisted)), Instant::now()));
        fill_seed_roots(&roots_map, &jobs, &al_id);
        assert!(!roots_map.contains_key(&al_id));
    }

    #[test]
    fn fill_seeds_empty_when_no_seed_parses() {
        let jobs = DashMap::new();
        let roots_map = DashMap::new();
        let job = CrawlJob {
            follow_external_links: false,
            seed_urls: vec!["not a url".to_string()],
            ..CrawlJob::default()
        };
        let id = job.id;
        jobs.insert(id, (Some(Arc::new(job)), Instant::now()));

        fill_seed_roots(&roots_map, &jobs, &id);
        let roots = roots_map.get(&id).unwrap().clone();
        assert!(roots.is_empty());
    }

    #[test]
    fn fill_seeds_handles_missing_cache_entry() {
        let jobs = DashMap::new();
        let roots_map = DashMap::new();
        let id = Uuid::new_v4();
        roots_map.insert(id, Arc::new(vec!["stale.example".to_string()]));
        fill_seed_roots(&roots_map, &jobs, &id);
        assert!(!roots_map.contains_key(&id));
    }
}
