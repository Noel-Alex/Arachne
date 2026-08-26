//! Robots.txt fetching, parsing, and caching.

use crate::db::ArachneRepo;
use dashmap::DashMap;
use futures::StreamExt;
use std::sync::Arc;
use std::time::{Duration, Instant};
use texting_robots::Robot;
use tracing::{debug, warn};
use url::Url;

/// Hard cap on robots.txt body size. The file is legitimately tiny (<100 KiB
/// in the wild); without a cap a hostile origin could stream gigabytes into
/// memory through this unauthenticated endpoint.
const MAX_ROBOTS_BODY_BYTES: usize = 1024 * 1024;

/// Cache lifetime for known-absent robots.txt. Deliberately SHORTER than a
/// successful fetch's TTL: absence is the state most likely to change soon
/// (site publishes robots.txt), and serving stale allow-all for a full TTL
/// would keep crawling paths a site has meanwhile disallowed.
const MISSING_ROBOTS_TTL: Duration = Duration::from_secs(60 * 60);

#[derive(Clone)]
struct CachedRobots {
    raw_txt: Arc<Vec<u8>>,
    fetched_at: Instant,
    crawl_delay: Option<Duration>,
    /// Absolute sitemap URLs declared via `Sitemap:` lines.
    sitemaps: Arc<Vec<String>>,
}

/// What the cache holds for an authority: either a fetched robots.txt or the
/// knowledge that there is none (kept so dead endpoints are not re-requested
/// on every URL, while still expiring quickly — see [`MISSING_ROBOTS_TTL`]).
#[derive(Clone)]
enum CacheEntry {
    Fetched(CachedRobots),
    Missing { fetched_at: Instant },
}

/// Result of resolving an authority's robots.txt. Each variant carries its
/// own politeness policy downstream (`is_allowed` / `get_crawl_delay` /
/// `get_sitemaps`).
enum FetchOutcome {
    /// Robots.txt fetched and parsed.
    Ok(CachedRobots),
    /// No usable robots.txt (404, other non-2xx, transport error, oversized
    /// body). Standard robots semantics: absent file = allow. Transport
    /// errors land here too — fail-open on transient failures, availability
    /// over correctness; revisit at M2 per the audit roadmap.
    NotFound,
    /// Server answered 401/403: robots.txt exists but is gated behind auth.
    /// Treated as DENY by `is_allowed` — a site demanding credentials to
    /// even see its crawler policy is signaling "do not crawl".
    /// Deliberately NOT cached: unlike a stable 404, auth gating is often
    /// misconfiguration, and pinning a deny for a full TTL would freeze
    /// sites out of recovery; re-fetching per request stays bounded by the
    /// client timeout.
    Denied,
}

/// Manages fetching, caching, and querying robots.txt files.
pub struct RobotsManager {
    cache: DashMap<String, CacheEntry>,
    http_client: reqwest::Client,
    cache_ttl: Duration,
    /// Bare product token ("ArachneBot" out of "ArachneBot/1.0 (+…)").
    /// robots.txt groups match on the product NAME, never the full header —
    /// passing the full string made every named group unreachable.
    product_token: String,
    /// Optional persistence hook: when set, every successful robots.txt fetch
    /// is fire-and-forget saved to `domain_metadata` so `arachne inspect`
    /// has data. None by default (tests / stateless constructions).
    repo: Option<Arc<ArachneRepo>>,
}

impl RobotsManager {
    /// Create a new RobotsManager.
    pub fn new(user_agent: &str, cache_ttl: Duration) -> Self {
        let product_token = product_token(user_agent).to_string();

        // Guarded redirects: robots.txt follows the same SSRF discipline as
        // page fetches — every 30x hop is re-checked against the egress
        // guard, so a redirect chain cannot bounce us onto loopback or
        // link-local targets. Without it, `Policy::limited` would happily
        // follow robots.txt redirects anywhere.
        let http_client = reqwest::Client::builder()
            .user_agent(user_agent)
            .redirect(crate::egress::guarded_redirect_policy(3))
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();

        Self {
            cache: DashMap::new(),
            http_client,
            cache_ttl,
            product_token,
            repo: None,
        }
    }

    /// Enable persisting each successfully fetched robots.txt into
    /// `domain_metadata` (keyed by root domain) via the given repository.
    pub fn with_repo(mut self, repo: Arc<ArachneRepo>) -> Self {
        self.repo = Some(repo);
        self
    }

    /// Fire-and-forget save of the fetched robots state. Never fails the
    /// caller; persistence errors are logged at debug only.
    fn persist_fetched(&self, page_url: &Url, body: &[u8], delay_ms: Option<u64>) {
        let Some(repo) = self.repo.as_ref() else {
            return;
        };
        let domain = match crate::domain::extract_root_domain(page_url.as_str()) {
            Some(d) => d,
            None => {
                debug!(url = %page_url, "robots persistence skipped: no root domain");
                return;
            }
        };
        let body = String::from_utf8_lossy(body).into_owned();
        let delay_ms = delay_ms.map(|d| d as i32);
        let repo = Arc::clone(repo);
        tokio::spawn(async move {
            if let Err(e) = repo
                .save_domain_metadata(&domain, Some(&body), delay_ms)
                .await
            {
                debug!("robots.txt persistence for {} failed: {e:#}", domain);
            }
        });
    }

    /// Check if a URL is allowed to be crawled according to robots.txt.
    pub async fn is_allowed(&self, url: &Url) -> bool {
        let authority = match url.authority() {
            "" => return true,
            auth => auth.to_string(),
        };

        match self.fetch_robots(url, &authority).await {
            FetchOutcome::Ok(entry) => match Robot::new(&self.product_token, &entry.raw_txt) {
                Ok(robot) => robot.allowed(url.as_str()),
                // Wholly unparseable robots.txt counts as no restrictions.
                Err(_) => true,
            },
            // Fail-open on absence AND on transport errors: availability over
            // correctness for transient failures — a DNS blip must not halt
            // the whole crawl. Revisit at M2 per the audit roadmap.
            FetchOutcome::NotFound => true,
            FetchOutcome::Denied => false,
        }
    }

    /// Get the crawl delay for a domain from robots.txt, if specified.
    pub async fn get_crawl_delay(&self, url: &Url) -> Option<Duration> {
        let authority = match url.authority() {
            "" => return None,
            auth => auth.to_string(),
        };

        match self.fetch_robots(url, &authority).await {
            FetchOutcome::Ok(entry) => entry.crawl_delay,
            FetchOutcome::NotFound | FetchOutcome::Denied => None,
        }
    }

    /// Sitemap URLs declared in the domain's robots.txt (`Sitemap:` lines).
    /// Resolved against the host so relative declarations work.
    pub async fn get_sitemaps(&self, url: &Url) -> Vec<String> {
        if url.authority().is_empty() {
            return Vec::new();
        }
        let authority = url.authority().to_string();
        match self.fetch_robots(url, &authority).await {
            FetchOutcome::Ok(entry) => entry.sitemaps.to_vec(),
            FetchOutcome::NotFound | FetchOutcome::Denied => Vec::new(),
        }
    }

    async fn fetch_robots(&self, url: &Url, authority: &str) -> FetchOutcome {
        let cache_key = format!("{}://{}", url.scheme(), authority);

        if let Some(hit) = self.cache.get(&cache_key) {
            let fresh = match hit.value() {
                CacheEntry::Fetched(c) => c.fetched_at.elapsed() < self.cache_ttl,
                CacheEntry::Missing { fetched_at } => fetched_at.elapsed() < MISSING_ROBOTS_TTL,
            };
            if fresh {
                return match hit.value() {
                    CacheEntry::Fetched(c) => FetchOutcome::Ok(c.clone()),
                    CacheEntry::Missing { .. } => FetchOutcome::NotFound,
                };
            }
        }

        let robots_url = format!("{}://{}/robots.txt", url.scheme(), authority);
        // SSRF: the guarded redirect policy only re-validates hops 1+; the
        // INITIAL request must be screened too. Authority comes from caller-
        // supplied URLs (including page-controlled discovery candidates), so a
        // crafted href would otherwise direct this fetch at internal hosts.
        // Fail-open via cache_missing mirrors the transport-error semantics
        // below — robots unavailability never blocks crawling.
        if !crate::domain::is_safe_egress_url(robots_url.as_str()) {
            debug!("robots.txt for {} blocked by egress guard", authority);
            return self.cache_missing(cache_key);
        }
        let res = match self.http_client.get(&robots_url).send().await {
            Ok(res) => res,
            Err(e) => {
                warn!("Failed to fetch robots.txt for {}: {}", authority, e);
                return self.cache_missing(cache_key);
            }
        };

        match classify_robots_status(res.status()) {
            RobotsStatusClass::Denied => {
                debug!(
                    "robots.txt for {}: HTTP {} — auth-required, denying",
                    authority,
                    res.status()
                );
                FetchOutcome::Denied
            }
            RobotsStatusClass::Absent => {
                debug!("No robots.txt for {}: HTTP {}", authority, res.status());
                self.cache_missing(cache_key)
            }
            RobotsStatusClass::Success => {
                let Some(bytes) = read_capped(res, authority).await else {
                    // Oversized or broken body: treat like any other miss.
                    return self.cache_missing(cache_key);
                };
                let delay_ms = parse_crawl_delay_for(&bytes, &self.product_token);
                let sitemaps = parse_sitemaps(&bytes, url);
                self.persist_fetched(url, &bytes, delay_ms);
                let cached = CachedRobots {
                    raw_txt: Arc::new(bytes),
                    fetched_at: Instant::now(),
                    crawl_delay: delay_ms.map(Duration::from_millis),
                    sitemaps: Arc::new(sitemaps),
                };

                self.cache
                    .insert(cache_key, CacheEntry::Fetched(cached.clone()));
                FetchOutcome::Ok(cached)
            }
        }
    }

    /// Record "no robots.txt" for this authority and report the outcome.
    fn cache_missing(&self, cache_key: String) -> FetchOutcome {
        self.cache.insert(
            cache_key,
            CacheEntry::Missing {
                fetched_at: Instant::now(),
            },
        );
        FetchOutcome::NotFound
    }
}

/// Stream the response body, aborting past [`MAX_ROBOTS_BODY_BYTES`]. The
/// previous implementation buffered via `res.bytes()` with no limit, letting
/// any origin OOM the worker through its own robots endpoint.
async fn read_capped(res: reqwest::Response, authority: &str) -> Option<Vec<u8>> {
    let mut body = Vec::new();
    let mut stream = res.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(c) => c,
            Err(e) => {
                warn!("robots.txt body read failed for {}: {}", authority, e);
                return None;
            }
        };
        if body.len().saturating_add(chunk.len()) > MAX_ROBOTS_BODY_BYTES {
            warn!(
                "robots.txt for {} exceeds {} byte cap; aborting fetch",
                authority, MAX_ROBOTS_BODY_BYTES
            );
            return None;
        }
        body.extend_from_slice(&chunk);
    }
    Some(body)
}

/// Classify a robots.txt response status. Pure decision fn so the 401/403
/// deny policy is unit-testable without standing up a server.
enum RobotsStatusClass {
    Success,
    Denied,
    Absent,
}

fn classify_robots_status(status: reqwest::StatusCode) -> RobotsStatusClass {
    match status {
        reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN => {
            RobotsStatusClass::Denied
        }
        s if s.is_success() => RobotsStatusClass::Success,
        _ => RobotsStatusClass::Absent,
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
            let value = line
                .to_lowercase()
                .starts_with("sitemap:")
                .then(|| line["sitemap:".len()..].trim().to_string())?;
            Url::parse(&value)
                .or_else(|_| base.join(&value))
                .map(|u| u.to_string())
                .ok()
        })
        .collect()
}

/// Parse a `Crawl-delay:` value (seconds, possibly fractional) into
/// milliseconds. Rejects negatives and non-finite input; the previous parser
/// silently saturated "-3" to 0 ms via the float cast.
fn parse_delay_ms(value: &str) -> Option<u64> {
    let sec = value.parse::<f64>().ok()?;
    if !sec.is_finite() || sec < 0.0 {
        return None;
    }
    Some((sec * 1000.0) as u64)
}

/// Bare product token for robots.txt matching: everything before the first
/// '/'. "ArachneBot/1.0 (+https://…)" must register as group "ArachneBot".
fn product_token(user_agent: &str) -> &str {
    user_agent.split('/').next().unwrap_or("").trim()
}

/// Group-aware `Crawl-delay:` extraction (RFC 9309 grouping). Consecutive
/// `User-agent:` lines open ONE group; `Crawl-delay:` binds to the group
/// under construction. Selection: the group exactly matching `product_token`
/// if it declares a delay, else the `*` group's value, else None. Directives
/// before the first `User-agent:` line belong to no group and are ignored —
/// the previous parser took the FIRST delay anywhere in the file, so a
/// `googlebot` delay leaked onto unrelated crawlers.
fn parse_crawl_delay_for(body: &[u8], product_token: &str) -> Option<u64> {
    let txt = std::str::from_utf8(body).ok()?;
    let token = product_token.to_lowercase();

    let mut current_agents: Vec<String> = Vec::new();
    let mut group_open = false;
    let mut group_delay: Option<u64> = None;
    // Outer Some = an exact-matching group exists; inner = its declared delay.
    let mut exact_delay: Option<Option<u64>> = None;
    let mut star_delay: Option<u64> = None;

    // Fold the group under construction into the selection state.
    fn seal_group(
        agents: &[String],
        delay: Option<u64>,
        token: &str,
        exact: &mut Option<Option<u64>>,
        star: &mut Option<u64>,
    ) {
        if agents.iter().any(|a| a == "*") {
            *star = delay;
        }
        if agents.iter().any(|a| a == token) {
            *exact = Some(delay);
        }
    }

    for line in txt.lines() {
        // Trailing comments are legal on any directive line.
        let line = line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let lower = line.to_lowercase();
        if let Some(agent) = lower.strip_prefix("user-agent:") {
            if !group_open {
                // A user-agent line following a rule line opens a NEW group;
                // further consecutive user-agent lines join the same one.
                // Blank lines are skipped above, so they never split groups
                // (matches the RFC 9309 grammar).
                seal_group(
                    &current_agents,
                    group_delay.take(),
                    &token,
                    &mut exact_delay,
                    &mut star_delay,
                );
                current_agents.clear();
            }
            current_agents.push(agent.trim().to_string());
            group_open = true;
        } else {
            if let Some(value) = lower.strip_prefix("crawl-delay:") {
                group_delay = parse_delay_ms(value.trim());
            }
            // Any rule line closes the group: the next UA line starts a new
            // one rather than extending this group's agent list.
            group_open = false;
        }
    }
    seal_group(
        &current_agents,
        group_delay,
        &token,
        &mut exact_delay,
        &mut star_delay,
    );

    // Most specific group that DECLARES a delay wins; a matching group
    // without one defers to '*'.
    exact_delay.flatten().or(star_delay)
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
        assert_eq!(
            parse_sitemaps(b"User-agent: *\n", &base),
            Vec::<String>::new()
        );
    }

    #[test]
    fn crawl_delay_star_group_only() {
        let body = b"User-agent: *\nCrawl-delay: 5\nDisallow: /\n";
        assert_eq!(parse_crawl_delay_for(body, "arachne"), Some(5000));
    }

    #[test]
    fn crawl_delay_exact_group_wins_over_star() {
        let body = b"\
User-agent: *\n\
Crawl-delay: 10\n\
\n\
User-agent: arachne\n\
Crawl-delay: 2\n\
Disallow:\n";
        assert_eq!(parse_crawl_delay_for(body, "arachne"), Some(2000));
        assert_eq!(parse_crawl_delay_for(body, "otherbot"), Some(10000));
    }

    #[test]
    fn crawl_delay_consecutive_user_agents_share_one_group() {
        let body = b"User-agent: arachne\nUser-agent: FriendBot\nCrawl-delay: 7\n";
        assert_eq!(parse_crawl_delay_for(body, "arachne"), Some(7000));
        assert_eq!(parse_crawl_delay_for(body, "friendbot"), Some(7000));
        assert_eq!(parse_crawl_delay_for(body, "unrelated"), None);
    }

    #[test]
    fn crawl_delay_new_group_after_rule_gets_own_value() {
        // The second group's UA list must NOT absorb the first group's
        // crawl-delay: 'arachne' here declares nothing itself.
        let body = b"\
User-agent: googlebot\n\
Crawl-delay: 9\n\
Disallow: /priv\n\
\n\
User-agent: arachne\n\
Disallow:\n";
        assert_eq!(parse_crawl_delay_for(body, "arachne"), None);
        assert_eq!(parse_crawl_delay_for(body, "googlebot"), Some(9000));
    }

    #[test]
    fn crawl_delay_matching_group_without_value_falls_back_to_star() {
        let body = b"\
User-agent: *\n\
Crawl-delay: 10\n\
\n\
User-agent: arachne\n\
Disallow: /tmp\n";
        assert_eq!(parse_crawl_delay_for(body, "arachne"), Some(10000));
    }

    #[test]
    fn crawl_delay_outside_any_group_is_ignored() {
        // Directives before the first user-agent line belong to no group;
        // the old first-match parser accepted them for everyone.
        assert_eq!(parse_crawl_delay_for(b"Crawl-delay: 2\n", "arachne"), None);
    }

    #[test]
    fn crawl_delay_values_and_comments() {
        assert_eq!(
            parse_crawl_delay_for(b"user-agent: *\ncrawl-delay: 1.5\n", "arachne"),
            Some(1500)
        );
        assert_eq!(
            parse_crawl_delay_for(b"User-agent: *\nCrawl-delay: 4 # be nice\n", "arachne"),
            Some(4000)
        );
        assert_eq!(
            parse_crawl_delay_for(b"User-agent: *\nCrawl-delay: soon\n", "arachne"),
            None
        );
        assert_eq!(
            parse_crawl_delay_for(b"User-agent: *\nCrawl-delay: -3\n", "arachne"),
            None
        );
    }

    #[test]
    fn crawl_delay_no_declaration_anywhere_is_none() {
        assert_eq!(
            parse_crawl_delay_for(b"User-agent: *\nDisallow: /\n", "arachne"),
            None
        );
    }

    #[test]
    fn product_tokens_strip_version_and_comment() {
        assert_eq!(
            product_token("ArachneBot/1.0 (+https://example.org/bot)"),
            "ArachneBot"
        );
        assert_eq!(product_token("  Mozilla/5.0 (compatible)"), "Mozilla");
        assert_eq!(product_token("bare-bot-name"), "bare-bot-name");
        assert_eq!(product_token("/leading-slash"), "");
    }

    #[test]
    fn robots_status_classes_route_correctly() {
        use reqwest::StatusCode;
        assert!(matches!(
            classify_robots_status(StatusCode::OK),
            RobotsStatusClass::Success
        ));
        assert!(matches!(
            classify_robots_status(StatusCode::NO_CONTENT),
            RobotsStatusClass::Success
        ));
        assert!(matches!(
            classify_robots_status(StatusCode::UNAUTHORIZED),
            RobotsStatusClass::Denied
        ));
        assert!(matches!(
            classify_robots_status(StatusCode::FORBIDDEN),
            RobotsStatusClass::Denied
        ));
        assert!(matches!(
            classify_robots_status(StatusCode::NOT_FOUND),
            RobotsStatusClass::Absent
        ));
        assert!(matches!(
            classify_robots_status(StatusCode::INTERNAL_SERVER_ERROR),
            RobotsStatusClass::Absent
        ));
        assert!(matches!(
            classify_robots_status(StatusCode::TOO_MANY_REQUESTS),
            RobotsStatusClass::Absent
        ));
    }

    #[test]
    fn parses_padded_multiple_and_skips_invalid_sitemaps() {
        let body = b"Sitemap: https://a.com/s1.xml\n   Sitemap:   https://a.com/s2.xml\nsitemap: /s3.xml\nSitemap: https://a.com:notaport/broken.xml";
        let base = Url::parse("https://a.com/robots.txt").unwrap();
        assert_eq!(
            parse_sitemaps(body, &base),
            vec![
                "https://a.com/s1.xml".to_string(),
                "https://a.com/s2.xml".to_string(),
                "https://a.com/s3.xml".to_string(),
            ]
        );
    }
}
