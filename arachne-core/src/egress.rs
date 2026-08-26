//! Redirect-aware SSRF guard for reqwest clients.
//!
//! `reqwest::redirect::Policy::limited(n)` follows every redirect hop to ANY
//! host — including `127.0.0.1`, `169.254.169.254`, and IPv4-mapped IPv6
//! forms — without ever consulting [`crate::domain::is_safe_egress_url`],
//! which is only applied to the original crawl URL. This module supplies a
//! drop-in replacement policy that re-validates EVERY hop through the same
//! static guard, closing the redirect-following egress bypass.

use reqwest::redirect::{Attempt, Policy};
use url::Url;

/// Error surfaced when the hop budget from config (`worker.max_redirects`)
/// is exhausted. Mirrors the exact semantics of `Policy::limited`: the
/// previous-chain length (original URL included) must not EXCEED the limit.
const TOO_MANY_REDIRECTS: &str = "too many redirects";

/// Error surfaced when a redirect target fails the SSRF/egress guard.
const HOP_BLOCKED_BY_EGRESS_GUARD: &str = "redirect target blocked by egress guard";

/// Error surfaced when a redirect points at a non-HTTP(S) scheme.
const NON_HTTP_REDIRECT: &str = "non-http redirect blocked by egress guard";

/// Core redirect-hop decision, shared by the reqwest policy closure and the
/// unit tests below (reqwest's `Attempt` cannot be constructed outside the
/// crate, so the logic is factored into this plain function).
///
/// * `previous_count` is `attempt.previous().len()` — the number of URLs
///   already requested in this chain, original URL included.
/// * `next` is the redirect target about to be requested.
/// * `max_hops` is the configured `worker.max_redirects`.
pub fn hop_allowed(previous_count: usize, next: &Url, max_hops: usize) -> Result<(), &'static str> {
    if previous_count > max_hops {
        return Err(TOO_MANY_REDIRECTS);
    }

    // Cheap structural check first, so non-HTTP(S) schemes get their own
    // precise error instead of a generic guard rejection
    // (`is_safe_egress_url` would reject them too, but with less context).
    if next.scheme() != "http" && next.scheme() != "https" {
        return Err(NON_HTTP_REDIRECT);
    }

    // Same SSRF boundary applied to the original URL: blocks loopback,
    // private ranges, link-local/cloud-metadata, IPv4-mapped IPv6,
    // forbidden hostnames, and non-standard ports.
    if !crate::domain::is_safe_egress_url(next.as_str()) {
        return Err(HOP_BLOCKED_BY_EGRESS_GUARD);
    }

    Ok(())
}

/// Build a reqwest redirect policy that re-validates EVERY hop through the
/// SSRF guard ([`crate::domain::is_safe_egress_url`]).
///
/// This closes the TOCTOU-adjacent redirect bypass: previously the worker
/// built its client with `Policy::limited(max_redirects)`, which follows any
/// 30x chain to any host — an attacker-controlled page could bounce a crawl
/// straight at `127.0.0.1` or `169.254.169.254` while the static guard only
/// ever saw the benign ORIGINAL URL. Blocked hops abort the request with an
/// error that surfaces as `CrawlStatus::FetchError` downstream.
///
/// Hop-budget semantics are identical to `Policy::limited(max_redirects)`
/// (the limit is exceeded once the previous-chain length surpasses it), so
/// behavior changes ONLY for targets the guard rejects. The gap documented in
/// [`crate::domain::is_safe_egress_url`] ("REMAINING KNOWN GAP") remains
/// accurate as-is: it covers DNS-level rebinding only, which a redirect
/// policy cannot address.
pub fn guarded_redirect_policy(max_redirects: usize) -> Policy {
    Policy::custom(move |attempt: Attempt<'_>| {
        match hop_allowed(attempt.previous().len(), attempt.url(), max_redirects) {
            Ok(()) => attempt.follow(),
            Err(reason) => attempt.error(reason),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn u(s: &str) -> Url {
        Url::parse(s).unwrap()
    }

    #[test]
    fn loopback_hop_is_rejected() {
        assert_eq!(
            hop_allowed(0, &u("http://127.0.0.1/admin"), 5),
            Err(HOP_BLOCKED_BY_EGRESS_GUARD)
        );
    }

    #[test]
    fn ipv4_mapped_ipv6_private_hop_is_rejected() {
        assert_eq!(
            hop_allowed(0, &u("https://[::ffff:10.0.0.1]/"), 5),
            Err(HOP_BLOCKED_BY_EGRESS_GUARD)
        );
    }

    #[test]
    fn cloud_metadata_hop_is_rejected() {
        assert_eq!(
            hop_allowed(0, &u("http://169.254.169.254/latest/meta-data/"), 5),
            Err(HOP_BLOCKED_BY_EGRESS_GUARD)
        );
    }

    #[test]
    fn localhost_hostname_hop_is_rejected() {
        assert_eq!(
            hop_allowed(2, &u("http://localhost:8000/flag"), 5),
            Err(HOP_BLOCKED_BY_EGRESS_GUARD)
        );
    }

    #[test]
    fn non_http_scheme_is_rejected_with_scheme_error() {
        assert_eq!(
            hop_allowed(0, &u("file:///etc/passwd"), 5),
            Err(NON_HTTP_REDIRECT)
        );
        assert_eq!(
            hop_allowed(0, &u("gopher://example.com/"), 5),
            Err(NON_HTTP_REDIRECT)
        );
    }

    #[test]
    fn public_target_mid_chain_is_allowed() {
        assert_eq!(hop_allowed(3, &u("https://example.com/final"), 5), Ok(()));
        assert_eq!(hop_allowed(3, &u("https://example.co.uk/a"), 5), Ok(()));
    }

    #[test]
    fn hop_budget_matches_policy_limited_semantics() {
        // Exactly at the limit: still allowed...
        assert_eq!(hop_allowed(5, &u("https://example.com/edge"), 5), Ok(()));
        // ...one past the limit: rejected.
        assert_eq!(
            hop_allowed(6, &u("https://example.com/over"), 5),
            Err(TOO_MANY_REDIRECTS)
        );
    }

    #[test]
    fn zero_budget_blocks_first_hop_but_not_guard_order() {
        // With max_redirects = 0 the very first hop overruns the budget,
        // exactly like Policy::limited(0).
        assert_eq!(
            hop_allowed(1, &u("https://example.com/first-hop"), 0),
            Err(TOO_MANY_REDIRECTS)
        );
    }

    #[test]
    fn factory_produces_a_custom_policy() {
        // Smoke test: the factory wires `hop_allowed` into the real reqwest
        // policy type. Attempt values cannot be built outside reqwest, so
        // behavioral coverage lives in the hop_allowed tests above; this
        // pins the public constructor contract.
        let _policy = guarded_redirect_policy(5);
    }
}
