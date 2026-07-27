//! NATS JetStream connection manager.

use anyhow::Result;
use async_nats::jetstream::{
    self,
    consumer::{self, pull::Config as PullConfig},
    stream::{Config as StreamConfig, RetentionPolicy},
};
use std::time::Duration;
use tracing::{debug, info};

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
    pub async fn connect(config: &NatsConfig) -> Result<Self> {
        info!("Connecting to NATS at {}", config.url);
        let client = async_nats::connect(&config.url).await?;
        let jetstream = jetstream::new(client.clone());
        Ok(Self {
            _client: client,
            jetstream,
        })
    }

    /// Ensure required streams are created.
    pub async fn ensure_streams(&self) -> Result<()> {
        debug!("Ensuring NATS streams exist");

        // Tasks stream
        self.jetstream
            .get_or_create_stream(StreamConfig {
                name: STREAM_CRAWL_TASKS.to_string(),
                subjects: vec![SUBJECT_TASK.to_string()],
                retention: RetentionPolicy::WorkQueue,
                ..Default::default()
            })
            .await?;

        // Results stream
        self.jetstream
            .get_or_create_stream(StreamConfig {
                name: STREAM_CRAWL_RESULTS.to_string(),
                subjects: vec![SUBJECT_RESULT.to_string()],
                retention: RetentionPolicy::Limits,
                max_age: Duration::from_secs(7 * 24 * 3600), // 7 days
                ..Default::default()
            })
            .await?;

        // Discovered URLs stream
        self.jetstream
            .get_or_create_stream(StreamConfig {
                name: STREAM_DISCOVERED_URLS.to_string(),
                subjects: vec![SUBJECT_DISCOVERED.to_string()],
                retention: RetentionPolicy::WorkQueue,
                ..Default::default()
            })
            .await?;

        Ok(())
    }

    /// Publish a crawl task.
    pub async fn publish_task(&self, task: &CrawlTask) -> Result<()> {
        let payload = serde_json::to_vec(task)?;
        self.jetstream
            .publish(SUBJECT_TASK.to_string(), payload.into())
            .await?
            .await?;
        Ok(())
    }

    /// Publish a crawl result.
    pub async fn publish_result(&self, result: &CrawlResult) -> Result<()> {
        let payload = serde_json::to_vec(result)?;
        self.jetstream
            .publish(SUBJECT_RESULT.to_string(), payload.into())
            .await?
            .await?;
        Ok(())
    }

    /// Publish discovered URLs.
    pub async fn publish_discovered(&self, urls: &[DiscoveredUrl]) -> Result<()> {
        let payload = serde_json::to_vec(urls)?;
        self.jetstream
            .publish(SUBJECT_DISCOVERED.to_string(), payload.into())
            .await?
            .await?;
        Ok(())
    }

    /// Create a consumer for tasks (worker).
    pub async fn create_task_consumer(&self, worker_name: &str) -> Result<consumer::Consumer<PullConfig>> {
        let stream = self.jetstream.get_stream(STREAM_CRAWL_TASKS).await?;
        let consumer = stream
            .get_or_create_consumer(
                worker_name,
                PullConfig {
                    durable_name: Some(worker_name.to_string()),
                    ..Default::default()
                },
            )
            .await?;
        Ok(consumer)
    }

    /// Create a consumer for results (coordinator).
    pub async fn create_result_consumer(&self) -> Result<consumer::Consumer<PullConfig>> {
        let stream = self.jetstream.get_stream(STREAM_CRAWL_RESULTS).await?;
        let consumer = stream
            .get_or_create_consumer(
                "coordinator_results",
                PullConfig {
                    durable_name: Some("coordinator_results".to_string()),
                    ..Default::default()
                },
            )
            .await?;
        Ok(consumer)
    }

    /// Create a consumer for discovered URLs (coordinator).
    pub async fn create_discovery_consumer(&self) -> Result<consumer::Consumer<PullConfig>> {
        let stream = self.jetstream.get_stream(STREAM_DISCOVERED_URLS).await?;
        let consumer = stream
            .get_or_create_consumer(
                "coordinator_discovered",
                PullConfig {
                    durable_name: Some("coordinator_discovered".to_string()),
                    ..Default::default()
                },
            )
            .await?;
        Ok(consumer)
    }
}
