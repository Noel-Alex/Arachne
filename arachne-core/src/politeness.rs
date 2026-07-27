//! Politeness and rate limiting for domains.

use dashmap::DashMap;
use governor::{clock::DefaultClock, state::direct::NotKeyed, state::InMemoryState, Quota, RateLimiter};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

type DomainRateLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

/// Per-domain rate limiter.
pub struct PolitenessLimiter {
    limiters: DashMap<String, Arc<DomainRateLimiter>>,
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
                .or_insert_with(|| {
                    Arc::new(RateLimiter::direct(Quota::with_period(self.default_delay).unwrap()))
                })
                .value()
                .clone()
        };

        limiter.until_ready().await;
    }

    /// Set a custom delay for a specific domain (e.g. from robots.txt).
    pub fn set_domain_delay(&self, domain: &str, delay: Duration) {
        let limit = Quota::with_period(delay).unwrap_or_else(|| {
            Quota::per_second(NonZeroU32::new(1).unwrap())
        });
        let limiter = Arc::new(RateLimiter::direct(limit));
        self.limiters.insert(domain.to_string(), limiter);
    }
}
