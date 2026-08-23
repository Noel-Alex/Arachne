//! Robots.txt fetching, parsing, and caching.

use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use texting_robots::Robot;
use tracing::{debug, warn};
use url::Url;

#[derive(Clone)]
struct CachedRobots {
    raw_txt: Arc<Vec<u8>>,
    fetched_at: Instant,
    crawl_delay: Option<Duration>,
    /// Absolute sitemap URLs declared via `Sitemap:` lines.
    sitemaps: Arc<Vec<String>>,
}

/// Manages fetching, caching, and querying robots.txt files.
pub struct RobotsManager {
    cache: DashMap<String, CachedRobots>,
    http_client: reqwest::Client,
    cache_ttl: Duration,
    user_agent: String,
}

impl RobotsManager {
    /// Create a new RobotsManager.
    pub fn new(user_agent: &str, cache_ttl: Duration) -> Self {
        let http_client = reqwest::Client::builder()
            .user_agent(user_agent)
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();

        Self {
            cache: DashMap::new(),
            http_client,
            cache_ttl,
            user_agent: user_agent.to_string(),
        }
    }

    /// Check if a URL is allowed to be crawled according to robots.txt.
    pub async fn is_allowed(&self, url: &Url) -> bool {
        let authority = match url.authority() {
            "" => return true,
            auth => auth.to_string(),
        };

        let cached = self.fetch_robots(url, &authority).await;
        if let Some(robots_entry) = cached {
            if let Ok(robot) = Robot::new(&self.user_agent, &robots_entry.raw_txt) {
                return robot.allowed(url.as_str());
            }
        }
        true // Allow if robots.txt could not be fetched or parsed
    }

    /// Get the crawl delay for a domain from robots.txt, if specified.
    pub async fn get_crawl_delay(&self, url: &Url) -> Option<Duration> {
        let authority = match url.authority() {
            "" => return None,
            auth => auth.to_string(),
        };

        let cached = self.fetch_robots(url, &authority).await;
        cached.and_then(|r| r.crawl_delay)
    }

    /// Sitemap URLs declared in the domain's robots.txt (`Sitemap:` lines).
    /// Resolved against the host so relative declarations work.
    pub async fn get_sitemaps(&self, url: &Url) -> Vec<String> {
        if url.authority().is_empty() {
            return Vec::new();
        }
        let authority = url.authority().to_string();
        self.fetch_robots(url, &authority)
            .await
            .map(|r| r.sitemaps.to_vec())
            .unwrap_or_default()
    }

    fn empty_entry() -> CachedRobots {
        CachedRobots {
            raw_txt: Arc::new(Vec::new()),
            fetched_at: Instant::now(),
            crawl_delay: None,
            sitemaps: Arc::new(Vec::new()),
        }
    }

    async fn fetch_robots(&self, url: &Url, authority: &str) -> Option<CachedRobots> {
        let cache_key = format!("{}://{}", url.scheme(), authority);

        if let Some(entry) = self.cache.get(&cache_key) {
            if entry.fetched_at.elapsed() < self.cache_ttl {
                return Some(entry.value().clone());
            }
        }

        let robots_url = format!("{}://{}/robots.txt", url.scheme(), authority);
        match self.http_client.get(&robots_url).send().await {
            Ok(res) if res.status().is_success() => {
                if let Ok(body) = res.bytes().await {
                    let bytes = body.to_vec();
                    let delay_ms = parse_crawl_delay(&bytes);
                    let sitemaps = parse_sitemaps(&bytes, url);
                    let cached = CachedRobots {
                        raw_txt: Arc::new(bytes),
                        fetched_at: Instant::now(),
                        crawl_delay: delay_ms.map(Duration::from_millis),
                        sitemaps: Arc::new(sitemaps),
                    };

                    self.cache.insert(cache_key, cached.clone());
                    return Some(cached);
                }
            }
            Ok(res) => debug!(
                "Failed to fetch robots.txt for {}: HTTP {}",
                authority,
                res.status()
            ),
            Err(e) => warn!("Failed to fetch robots.txt for {}: {}", authority, e),
        }

        // Cache permissive empty fallback to avoid hammering 404/500 endpoints repeatedly
        let empty_cached = Self::empty_entry();
        self.cache.insert(cache_key, empty_cached.clone());
        Some(empty_cached)
    }
}

/// Extract absolute `Sitemap:` URLs from a robots.txt body. Relative values
/// are resolved against the robots.txt origin.
fn parse_sitemaps(body: &[u8], base: &Url) -> Vec<String> {
    let txt = match std::str::from_utf8(body) {
        Ok(t) => t,
        Err(_) => return Vec::new(),
    };
    txt.lines()
        .filter_map(|line| {
            let line = line.trim();
            let value = line.to_lowercase().starts_with("sitemap:").then(|| {
                line["sitemap:".len()..].trim().to_string()
            })?;
            Url::parse(&value)
                .or_else(|_| base.join(&value))
                .map(|u| u.to_string())
                .ok()
        })
        .collect()
}

/// Helper function to parse Crawl-delay from robots.txt body.
fn parse_crawl_delay(body: &[u8]) -> Option<u64> {
    let txt = std::str::from_utf8(body).ok()?;
    for line in txt.lines() {
        let line = line.trim();
        if line.to_lowercase().starts_with("crawl-delay:") {
            if let Some(val_str) = line.split(':').nth(1) {
                if let Ok(sec) = val_str.trim().parse::<f64>() {
                    return Some((sec * 1000.0) as u64);
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_and_resolves_sitemap_lines() {
        let body = b"User-agent: *\nDisallow: /private\n\nSitemap: https://ex.com/sitemap.xml\nsitemap: /relative-map.xml";
        let base = Url::parse("https://ex.com/robots.txt").unwrap();
        assert_eq!(
            parse_sitemaps(body, &base),
            vec![
                "https://ex.com/sitemap.xml".to_string(),
                "https://ex.com/relative-map.xml".to_string(),
            ]
        );
    }

    #[test]
    fn no_sitemaps_means_empty() {
        let base = Url::parse("https://ex.com/robots.txt").unwrap();
        assert_eq!(parse_sitemaps(b"User-agent: *\n", &base), Vec::<String>::new());
    }
}
