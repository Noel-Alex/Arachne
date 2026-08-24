//! Politeness and rate limiting for domains.

use dashmap::DashMap;
use governor::{
    clock::DefaultClock, state::direct::NotKeyed, state::InMemoryState, Quota, RateLimiter,
};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

type DomainRateLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

struct DomainLimit {
    limiter: Arc<DomainRateLimiter>,
    configured_delay: Option<Duration>,
}

/// Per-domain rate limiter.
pub struct PolitenessLimiter {
    limiters: DashMap<String, DomainLimit>,
    default_delay: Duration,
}

impl PolitenessLimiter {
    /// Create a new politeness limiter with a default delay between requests to the same domain.
    pub fn new(default_delay_ms: u64) -> Self {
        Self {
            limiters: DashMap::new(),
            default_delay: Duration::from_millis(default_delay_ms),
        }
    }

    /// Wait until it is permissible to make a request to the given domain.
    pub async fn wait_for_permission(&self, domain: &str) {
        let limiter = {
            self.limiters
                .entry(domain.to_string())
                .or_insert_with(|| DomainLimit {
                    limiter: Arc::new(RateLimiter::direct(
                        Quota::with_period(self.default_delay)
                            .expect("default delay must form a valid quota"),
                    )),
                    configured_delay: None,
                })
                .limiter
                .clone()
        };

        limiter.until_ready().await;
    }

    /// Set a custom delay for a specific domain (e.g. from robots.txt).
    ///
    /// Idempotent per (domain, delay): repeated calls with the same Crawl-delay
    /// reuse the existing limiter so its shared budget stays intact. Only a
    /// genuine change (robots.txt re-fetch after TTL) replaces the limiter,
    /// which necessarily starts a fresh budget.
    pub fn set_domain_delay(&self, domain: &str, delay: Duration) {
        // Sub-ms delays are clamped; zero falls back to 1/sec (governor's
        // quota requires a positive period).
        let period = if delay.is_zero() {
            Duration::from_secs(1)
        } else if delay < Duration::from_millis(1) {
            Duration::from_millis(1)
        } else {
            delay
        };

        self.limiters
            .entry(domain.to_string())
            .and_modify(|limit| {
                if limit.configured_delay != Some(delay) {
                    let quota = Quota::with_period(period)
                        .unwrap_or_else(|| Quota::per_second(NonZeroU32::new(1).unwrap()));
                    limit.limiter = Arc::new(RateLimiter::direct(quota));
                    limit.configured_delay = Some(delay);
                }
            })
            .or_insert_with(|| DomainLimit {
                limiter: Arc::new(RateLimiter::direct(
                    Quota::with_period(period)
                        .unwrap_or_else(|| Quota::per_second(NonZeroU32::new(1).unwrap())),
                )),
                configured_delay: Some(delay),
            });
    }
}
