//! Politeness and rate limiting for domains.

use dashmap::DashMap;
use governor::{
    Quota, RateLimiter, clock::DefaultClock, state::InMemoryState, state::direct::NotKeyed,
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
    /// reuse the existing limiter so its shared budget stays intact.
    ///
    /// Updates form a monotonic ratchet: only a strictly *longer* delay
    /// replaces the limiter (which necessarily starts a fresh budget); an
    /// equal-or-shorter one leaves the limiter untouched. Sibling subdomains
    /// reporting conflicting Crawl-delays therefore cannot thrash the shared
    /// root-domain limiter with oscillation-induced budget resets; relaxation
    /// happens only via the TTL'd robots refresh cycle or a process restart.
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
            // Ratchet compares the RAW incoming delay against the stored one
            // (not the clamped period) so clamping can never make a longer
            // delay look equal-or-shorter.
            .and_modify(|limit| {
                if limit.configured_delay.is_none_or(|stored| delay > stored) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[tokio::test]
    async fn default_delay_throttles() {
        let limiter = PolitenessLimiter::new(20);
        let start = Instant::now();
        limiter.wait_for_permission("ex.com").await;
        limiter.wait_for_permission("ex.com").await;
        // First call is immediate; the second must wait out the 20ms default.
        // Generous floor tolerates CI jitter while still proving throttling.
        assert!(
            start.elapsed() >= Duration::from_millis(15),
            "two waits completed in {:?}, expected >= 15ms",
            start.elapsed()
        );
    }

    #[tokio::test]
    async fn set_domain_delay_overrides() {
        let limiter = PolitenessLimiter::new(1);
        limiter.set_domain_delay("ex.com", Duration::from_millis(40));

        let slow = Instant::now();
        limiter.wait_for_permission("ex.com").await;
        limiter.wait_for_permission("ex.com").await;
        let slow_elapsed = slow.elapsed();
        assert!(
            slow_elapsed >= Duration::from_millis(35),
            "overridden domain took only {:?}",
            slow_elapsed
        );

        // Unrelated domain still runs at the fast default (two waits < 35ms).
        let fast = Instant::now();
        limiter.wait_for_permission("other.com").await;
        limiter.wait_for_permission("other.com").await;
        let fast_elapsed = fast.elapsed();
        assert!(
            fast_elapsed < Duration::from_millis(35),
            "default domain took {:?}, override leaked?",
            fast_elapsed
        );
    }

    #[tokio::test]
    async fn idempotent_delay_keeps_budget() {
        let limiter = PolitenessLimiter::new(1);
        limiter.set_domain_delay("ex.com", Duration::from_millis(40));
        limiter.wait_for_permission("ex.com").await;

        // Re-setting the SAME delay must not replace the limiter, so the
        // budget already consumed by the first wait is preserved and this
        // second wait still pays the full delay. A replacement would reset
        // the bucket and complete near-instantly.
        limiter.set_domain_delay("ex.com", Duration::from_millis(40));

        let start = Instant::now();
        limiter.wait_for_permission("ex.com").await;
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(30),
            "second wait took {:?}; limiter was likely replaced",
            elapsed
        );
    }

    #[tokio::test]
    async fn monotonic_ratchet_ignores_shorter_delay() {
        let limiter = PolitenessLimiter::new(1);
        limiter.set_domain_delay("ex.com", Duration::from_millis(100));

        // First call is immediate; the second pays out the 100ms delay once.
        let start = Instant::now();
        limiter.wait_for_permission("ex.com").await;
        limiter.wait_for_permission("ex.com").await;
        assert!(
            start.elapsed() >= Duration::from_millis(50),
            "initial pair took {:?}, expected the 100ms delay",
            start.elapsed()
        );

        // A SHORTER re-config must not replace the limiter: the budget already
        // spent stays spent, so both waits here still space ~100ms apart
        // (~200ms total in steady state). A replacement would hand out a
        // fresh bucket whose first wait is free and second pays only ~40ms.
        limiter.set_domain_delay("ex.com", Duration::from_millis(40));
        let start = Instant::now();
        limiter.wait_for_permission("ex.com").await;
        limiter.wait_for_permission("ex.com").await;
        assert!(
            start.elapsed() >= Duration::from_millis(150),
            "pair after shorter re-config took {:?}; budget was likely reset",
            start.elapsed()
        );

        // A LONGER re-config does ratchet up: the limiter is replaced at
        // 300ms, so the first wait is free and the second pays ~300ms. Had
        // the old limiter survived, this pair would cost only ~200ms.
        limiter.set_domain_delay("ex.com", Duration::from_millis(300));
        let start = Instant::now();
        limiter.wait_for_permission("ex.com").await;
        limiter.wait_for_permission("ex.com").await;
        assert!(
            start.elapsed() >= Duration::from_millis(250),
            "pair after longer re-config took {:?}, expected ~300ms spacing",
            start.elapsed()
        );
    }
}
