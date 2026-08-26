//! NATS JetStream connection manager.

use anyhow::Result;
use async_nats::jetstream::{
    self,
    consumer::{self, pull::Config as PullConfig},
    stream::{Config as StreamConfig, RetentionPolicy},
};
use futures::future::join_all;
use std::time::Duration;
use tracing::{debug, info, warn};

use crate::config::NatsConfig;
use crate::models::{CrawlResult, CrawlTask, DiscoveredUrl};

pub const STREAM_CRAWL_TASKS: &str = "CRAWL_TASKS";
pub const STREAM_CRAWL_RESULTS: &str = "CRAWL_RESULTS";
pub const STREAM_DISCOVERED_URLS: &str = "DISCOVERED_URLS";

pub const SUBJECT_TASK: &str = "crawl.task";
pub const SUBJECT_RESULT: &str = "crawl.result";
pub const SUBJECT_DISCOVERED: &str = "crawl.discovered";

/// NATS Manager handles JetStream setup, publishing, and consuming.
pub struct NatsManager {
    _client: async_nats::Client, // Keep the client alive
    jetstream: jetstream::Context,
}

impl NatsManager {
    /// Connect to NATS and create the JetStream context.
    ///
    /// Uses `retry_on_initial_connect` so a NATS outage at startup does not
    /// crash the process: connect() returns once the client is constructed and
    /// keeps retrying in the background until the server is reachable.
    pub async fn connect(config: &NatsConfig) -> Result<Self> {
        info!("Connecting to NATS at {}", config.url);
        let opts = async_nats::ConnectOptions::new()
            .name("arachne")
            .retry_on_initial_connect()
            .connection_timeout(Duration::from_secs(5));
        let client = opts.connect(&config.url).await?;
        info!("Connected to NATS at {}", config.url);
        let jetstream = jetstream::new(client.clone());
        Ok(Self {
            _client: client,
            jetstream,
        })
    }

    /// Ensure required streams exist and their code-defined limits hold.
    pub async fn ensure_streams(&self) -> Result<()> {
        debug!("Ensuring NATS streams exist");

        // Tasks stream
        self.ensure_stream(
            StreamConfig {
                name: STREAM_CRAWL_TASKS.to_string(),
                subjects: vec![SUBJECT_TASK.to_string()],
                retention: RetentionPolicy::WorkQueue,
                max_bytes: 50 * 1024 * 1024 * 1024, // 50GB buffer limit for 1M+ internet-scale queues
                // Bound staleness of terminally-exhausted messages: without
                // max_age they linger until 50GB byte-pressure eviction. Live
                // (pending) work is unaffected by age limits.
                max_age: Duration::from_secs(30 * 24 * 3600),
                ..Default::default()
            },
            &["max_bytes", "max_age"],
        )
        .await?;

        // Results stream
        self.ensure_stream(
            StreamConfig {
                name: STREAM_CRAWL_RESULTS.to_string(),
                subjects: vec![SUBJECT_RESULT.to_string()],
                retention: RetentionPolicy::Limits,
                max_age: Duration::from_secs(7 * 24 * 3600), // 7 days
                max_bytes: 50 * 1024 * 1024 * 1024,          // 50GB limit
                ..Default::default()
            },
            &["max_bytes", "max_age"],
        )
        .await?;

        // Discovered URLs stream
        self.ensure_stream(
            StreamConfig {
                name: STREAM_DISCOVERED_URLS.to_string(),
                subjects: vec![SUBJECT_DISCOVERED.to_string()],
                retention: RetentionPolicy::WorkQueue,
                max_bytes: 50 * 1024 * 1024 * 1024, // 50GB limit
                // Same 30d staleness bound as CRAWL_TASKS above.
                max_age: Duration::from_secs(30 * 24 * 3600),
                ..Default::default()
            },
            &["max_bytes", "max_age"],
        )
        .await?;

        Ok(())
    }

    /// Get or create a stream, reconciling config drift on pre-existing ones.
    ///
    /// get_or_create_stream silently keeps the server-side settings when the
    /// stream already exists, so deployments created before a limits change
    /// never pick up the new values. Fields listed in `enforce` ("max_bytes",
    /// "max_age") are compared against the live config and pushed back with
    /// update_stream (with a warning) whenever they differ.
    async fn ensure_stream(&self, desired: StreamConfig, enforce: &[&str]) -> Result<()> {
        let mut stream = self.jetstream.get_or_create_stream(desired.clone()).await?;
        let actual = stream.info().await?.config.clone();

        let drifted: Vec<&str> = enforce
            .iter()
            .copied()
            .filter(|field| match *field {
                "max_bytes" => actual.max_bytes != desired.max_bytes,
                "max_age" => actual.max_age != desired.max_age,
                _ => false,
            })
            .collect();

        if drifted.is_empty() {
            return Ok(());
        }

        warn!(
            stream = %desired.name,
            fields = ?drifted,
            server_max_bytes = actual.max_bytes,
            server_max_age_secs = actual.max_age.as_secs(),
            desired_max_bytes = desired.max_bytes,
            desired_max_age_secs = desired.max_age.as_secs(),
            "NATS stream config drift detected; enforcing code-defined limits"
        );
        self.jetstream.update_stream(desired).await?;
        Ok(())
    }

    /// Synchronously publish a single crawl task (JSON format).
    pub async fn publish_task(&self, task: &CrawlTask) -> Result<()> {
        let payload = serde_json::to_vec(task)?;
        self.jetstream
            .publish(SUBJECT_TASK.to_string(), payload.into())
            .await?
            .await?;
        Ok(())
    }

    /// High-throughput concurrent batch publish for tasks using JSON.
    pub async fn publish_tasks_batch(&self, tasks: &[CrawlTask]) -> Result<()> {
        let mut futures = Vec::with_capacity(tasks.len());
        for task in tasks {
            let payload = serde_json::to_vec(task)?;
            let ack_fut = self
                .jetstream
                .publish(SUBJECT_TASK.to_string(), payload.into())
                .await?;
            futures.push(async move { ack_fut.await });
        }

        let results = join_all(futures).await;
        for res in results {
            res?;
        }
        Ok(())
    }

    /// ULTRA HIGH-THROUGHPUT bincode binary batch publish for tasks (1M+ msg/sec capable).
    pub async fn publish_tasks_bincode_batch(&self, tasks: &[CrawlTask]) -> Result<()> {
        let mut futures = Vec::with_capacity(tasks.len());
        for task in tasks {
            let payload = bincode::serialize(task)?;
            let ack_fut = self
                .jetstream
                .publish(SUBJECT_TASK.to_string(), payload.into())
                .await?;
            futures.push(async move { ack_fut.await });
        }

        let results = join_all(futures).await;
        for res in results {
            res?;
        }
        Ok(())
    }

    /// Synchronously publish a crawl result.
    pub async fn publish_result(&self, result: &CrawlResult) -> Result<()> {
        let payload = serde_json::to_vec(result)?;
        self.jetstream
            .publish(SUBJECT_RESULT.to_string(), payload.into())
            .await?
            .await?;
        Ok(())
    }

    /// High-throughput concurrent batch publish for results.
    pub async fn publish_results_batch(&self, results: &[CrawlResult]) -> Result<()> {
        let mut futures = Vec::with_capacity(results.len());
        for result in results {
            let payload = serde_json::to_vec(result)?;
            let ack_fut = self
                .jetstream
                .publish(SUBJECT_RESULT.to_string(), payload.into())
                .await?;
            futures.push(async move { ack_fut.await });
        }

        let acks = join_all(futures).await;
        for ack in acks {
            ack?;
        }
        Ok(())
    }

    /// Synchronously publish discovered URLs.
    pub async fn publish_discovered(&self, urls: &[DiscoveredUrl]) -> Result<()> {
        let payload = serde_json::to_vec(urls)?;
        self.jetstream
            .publish(SUBJECT_DISCOVERED.to_string(), payload.into())
            .await?
            .await?;
        Ok(())
    }

    /// Create a consumer for tasks (worker).
    ///
    /// ack_wait must exceed the worst-case task duration: a 500MB media
    /// download on a slow uplink takes minutes, and the server default of 30s
    /// causes redelivery mid-download. max_deliver is kept high so transient
    /// failures retry rather than silently terminating delivery.
    pub async fn create_task_consumer(
        &self,
        worker_name: &str,
    ) -> Result<consumer::Consumer<PullConfig>> {
        let stream = self.jetstream.get_stream(STREAM_CRAWL_TASKS).await?;
        let consumer = stream
            .get_or_create_consumer(
                worker_name,
                PullConfig {
                    durable_name: Some(worker_name.to_string()),
                    ack_wait: Duration::from_secs(600),
                    max_deliver: 100,
                    ..Default::default()
                },
            )
            .await?;
        Ok(consumer)
    }

    /// Create a consumer for results (coordinator).
    ///
    /// The coordinator fetches a batch, processes it, and writes to the DB
    /// before ACKing; ack_wait must exceed that worst-case window or the
    /// server redelivers mid-processing. max_deliver is high because each
    /// failed delivery burns one attempt per ack_wait (~10 min), so small
    /// values silently drop results after a modest DB outage. Mirrors
    /// create_task_consumer.
    pub async fn create_result_consumer(&self) -> Result<consumer::Consumer<PullConfig>> {
        let stream = self.jetstream.get_stream(STREAM_CRAWL_RESULTS).await?;
        let consumer = stream
            .get_or_create_consumer(
                "coordinator_results",
                PullConfig {
                    durable_name: Some("coordinator_results".to_string()),
                    ack_wait: Duration::from_secs(600),
                    // 3 attempts x 600s ack_wait = terminal after ~20 min of
                    // unavailability, dropping crawl results outright. 50
                    // survives ~8h of outage at worst-case redelivery
                    // cadence. DLQ/parking lot is roadmap M2 - not built here.
                    max_deliver: 50,
                    ..Default::default()
                },
            )
            .await?;
        Ok(consumer)
    }

    /// Current message count in the tasks stream (frontier depth proxy).
    pub async fn stream_task_count(&self) -> Result<u64> {
        let mut stream = self.jetstream.get_stream(STREAM_CRAWL_TASKS).await?;
        let info = stream.info().await?;
        Ok(info.state.messages)
    }

    /// Create a consumer for discovered URLs (coordinator).
    ///
    /// Same ack_wait rationale as create_result_consumer: the discovery loop
    /// does admission control + DB checks + task publication before ACKing.
    pub async fn create_discovery_consumer(&self) -> Result<consumer::Consumer<PullConfig>> {
        let stream = self.jetstream.get_stream(STREAM_DISCOVERED_URLS).await?;
        let consumer = stream
            .get_or_create_consumer(
                "coordinator_discovered",
                PullConfig {
                    durable_name: Some("coordinator_discovered".to_string()),
                    ack_wait: Duration::from_secs(600),
                    // Same rationale as create_result_consumer: low max_deliver
                    // drops discoveries after ~20 min of DB unavailability.
                    max_deliver: 50,
                    ..Default::default()
                },
            )
            .await?;
        Ok(consumer)
    }
}
