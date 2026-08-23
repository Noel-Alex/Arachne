//! Configuration for Arachne crawler.

use anyhow::Result;
use figment::{
    providers::{Env, Format, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};

/// Main configuration struct for Arachne.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ArachneConfig {
    pub nats: NatsConfig,
    pub scylla: ScyllaConfig,
    pub worker: WorkerConfig,
    pub coordinator: CoordinatorConfig,
    pub politeness: PolitenessConfig,
    pub storage: StorageConfig,
    pub metrics: MetricsConfig,
}

impl ArachneConfig {
    /// Load configuration from defaults, an optional TOML file, and environment variables.
    pub fn load(config_path: Option<&str>) -> Result<Self> {
        let mut figment = Figment::new().merge(figment::providers::Serialized::defaults(
            ArachneConfig::default(),
        ));

        if let Some(path) = config_path {
            figment = figment.merge(Toml::file(path));
        } else {
            figment = figment.merge(Toml::file("config/default.toml"));
        }

        figment = figment.merge(Env::prefixed("ARACHNE_").split("__"));

        let config: ArachneConfig = figment.extract()?;
        Ok(config)
    }
}

/// Configuration for NATS.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NatsConfig {
    pub url: String,
}

impl Default for NatsConfig {
    fn default() -> Self {
        Self {
            url: "nats://127.0.0.1:4222".to_string(),
        }
    }
}

/// Configuration for ScyllaDB.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScyllaConfig {
    pub uri: String,
    pub keyspace: String,
}

impl Default for ScyllaConfig {
    fn default() -> Self {
        Self {
            uri: "127.0.0.1:9042".to_string(),
            keyspace: "arachne".to_string(),
        }
    }
}

/// Configuration for Worker nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    pub max_concurrent_requests: usize,
    pub request_timeout_secs: u64,
    pub connect_timeout_secs: u64,
    pub max_content_size_bytes: usize,
    pub max_redirects: usize,
    pub user_agent: String,
    pub retry_attempts: u32,
    pub retry_backoff_ms: u64,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            max_concurrent_requests: 500,
            request_timeout_secs: 30,
            connect_timeout_secs: 10,
            max_content_size_bytes: 5 * 1024 * 1024, // 5MB
            max_redirects: 10,
            user_agent: "ArachneBot/2.0".to_string(),
            retry_attempts: 3,
            retry_backoff_ms: 1000,
        }
    }
}

/// Configuration for the Coordinator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinatorConfig {
    pub max_pages_per_domain: i64,
    pub batch_size: usize,
    pub batch_timeout_ms: u64,
    pub dedup_bloom_capacity: u64,
    pub dedup_bloom_fp_rate: f64,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            max_pages_per_domain: 1000,
            batch_size: 256,
            batch_timeout_ms: 200,
            dedup_bloom_capacity: 10_000_000,
            dedup_bloom_fp_rate: 0.001,
        }
    }
}

/// Configuration for politeness and rate limiting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolitenessConfig {
    pub default_crawl_delay_ms: u64,
    pub max_crawl_delay_ms: u64,
    pub robots_cache_ttl_secs: u64,
    pub respect_robots_txt: bool,
}

impl Default for PolitenessConfig {
    fn default() -> Self {
        Self {
            default_crawl_delay_ms: 1000,
            max_crawl_delay_ms: 30000,
            robots_cache_ttl_secs: 86400,
            respect_robots_txt: true,
        }
    }
}

/// Configuration for storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub content_dir: String,
    pub store_raw_html: bool,
    pub store_extracted_text: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            content_dir: "./crawled_data".to_string(),
            store_raw_html: false,
            store_extracted_text: true,
        }
    }
}

/// Configuration for metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub port: u16,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: 9191,
        }
    }
}
