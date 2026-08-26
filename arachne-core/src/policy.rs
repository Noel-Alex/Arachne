//! Job-policy enforcement helpers.
//!
//! Pure functions over [`CrawlJob`] that give effect to stored-but-easy-to-
//! ignore policy fields (`crawl_delay_ms`, `follow_external_links`,
//! `topic_keywords`). Everything here is side-effect free — no NATS, DB, or
//! clock access — so decisions are unit-testable in isolation.
//!
//! Constraints honored by design:
//! * Wire format and DB schema are untouched; every input is data already
//!   flowing through the system.
//! * `models.rs` is not modified; these functions borrow `&CrawlJob` as-is.
//!
//! Scope notes (why some functions are `#[allow(dead_code)]`):
//! * Topic focusing applies at admission today via the job's `url_patterns`
//!   (substring match on discovered URLs); the coordinator never sees titles
//!   or text for discovered URLs, and results arrive already crawled, so
//!   keyword scoring is a future M3 feature. [`topic_relevant`] implements
//!   the stable contract now so M3 is call-site wiring only.
//! * Politeness is owned by WORKERS, not the coordinator (a central enforcer
//!   would throttle dispatch fleet-wide while individual workers idle).
//!   [`worker_crawl_delay`] documents the M2-fleet-policy consumption path;
//!   the coordinator deliberately enforces no delays.

use std::time::Duration;

use crate::models::CrawlJob;

/// Effective politeness delay for a domain under this job.
///
/// Precedence (highest wins):
/// 1. robots.txt `Crawl-delay` for the domain being hit (`robots_delay`),
///    taken as-is — a site publishing the directive knows its own tolerance
///    best, and capping it would let a crawler override the target's wishes;
/// 2. the job's `crawl_delay_ms`;
/// 3. the fleet/config default (`default_ms`).
///
/// A `None` job falls back to robots-or-default.
pub fn effective_crawl_delay(
    job: Option<&CrawlJob>,
    robots_delay: Option<Duration>,
    default_ms: u64,
) -> Duration {
    if let Some(delay) = robots_delay {
        return delay;
    }
    if let Some(ms) = job.and_then(|j| j.crawl_delay_ms) {
        return Duration::from_millis(ms);
    }
    Duration::from_millis(default_ms)
}

/// Should this URL's root domain be admitted given job-level switches?
///
/// Contract between the coordinator and this function (must stay in sync):
/// * No job ⇒ permissive (`true`) — absence of policy never blocks.
/// * `follow_external_links == true` ⇒ `true`.
/// * Non-empty `allowed_domains` ⇒ `true`: the allowlist gate already ran
///   inside `CrawlJob::is_url_allowed`; repeating it here with a second
///   matcher would risk divergent verdicts. An EMPTY allowlist is treated as
///   "unset", mirroring `is_url_allowed`'s own semantics.
/// * Otherwise (`follow_external_links == false`, no effective allowlist):
///   admit only when `candidate_root_domain` equals (ASCII-case-
///   insensitively) the root domain of one of the JOB'S SEEDS. Those seed
///   roots are computed by the CALLER — the coordinator parses
///   `job.seed_urls` through `domain::extract_root_domain` once per job-cache
///   refresh and passes them as `seed_roots`. Seed tokens are compared
///   leniently: surrounding whitespace and one pair of matching ASCII quotes
///   are ignored, since CLI/config inputs often arrive quoted.
/// * Seeds unknown (`None`) or empty ⇒ deny: without lineage we cannot tell
///   an external link from an internal one, and the job asked us not to leave
///   its origin sites.
pub fn external_links_allowed(
    job: Option<&CrawlJob>,
    candidate_root_domain: &str,
    seed_roots: Option<&[String]>,
) -> bool {
    let Some(job) = job else {
        return true;
    };
    let allowlist_handles_it = matches!(job.allowed_domains, Some(ref d) if !d.is_empty());
    if job.follow_external_links || allowlist_handles_it {
        return true;
    }
    match seed_roots {
        Some(roots) => roots.iter().any(|root| {
            normalize_domain_token(root)
                .eq_ignore_ascii_case(normalize_domain_token(candidate_root_domain))
        }),
        None => false,
    }
}

/// Normalize a user-supplied domain token for comparison: trim whitespace and
/// strip ONE pair of surrounding ASCII double or single quotes. Comparison
/// helper only — never mutates stored job fields.
fn normalize_domain_token(raw: &str) -> &str {
    let mut s = raw.trim();
    if s.len() >= 2 {
        for (open, close) in [('"', '"'), ('\'', '\'')] {
            if let Some(inner) = s.strip_prefix(open).and_then(|t| t.strip_suffix(close)) {
                s = inner.trim();
                break;
            }
        }
    }
    s
}

/// Does this page look relevant to the job's `topic_keywords`?
///
/// * Job absent, or `topic_keywords` `None`/empty ⇒ `true` (focusing off).
/// * Otherwise `true` iff ANY keyword occurs — case-insensitive substring —
///   in the page title or in the FIRST 2000 characters of the extracted
///   text. Keywords that are empty/whitespace-only never match (an empty
///   needle is a substring of everything and would disable focusing).
///
/// TODO(M3): wire into admission. Today the coordinator sees neither titles
/// nor text for discovered URLs, and results-side gating would discard
/// content that was already crawled; keyword scoring lands when the
/// coordinator gains a scoring stage. The contract is stable, hence
/// implemented and tested now.
#[allow(dead_code)]
pub fn topic_relevant(
    job: Option<&CrawlJob>,
    page_title: Option<&str>,
    page_text_excerpt: Option<&str>,
) -> bool {
    let Some(keywords) = job.and_then(|j| j.topic_keywords.as_ref()) else {
        return true;
    };
    if keywords.is_empty() {
        return true;
    }

    const TEXT_WINDOW_CHARS: usize = 2000;
    // Char-boundary-safe truncation to the leading window of the text.
    let text_window =
        page_text_excerpt.map(|text| match text.char_indices().nth(TEXT_WINDOW_CHARS) {
            Some((byte_idx, _)) => &text[..byte_idx],
            None => text,
        });

    let title_lower = page_title.unwrap_or("").to_lowercase();
    let text_lower = text_window.unwrap_or("").to_lowercase();

    keywords.iter().any(|kw| {
        let kw = kw.trim().to_lowercase();
        !kw.is_empty() && (title_lower.contains(&kw) || text_lower.contains(&kw))
    })
}

/// Fleet-worker politeness delay for a task under this job.
///
/// TODO(M2-fleet-policy): the WORKER owns politeness, not the coordinator.
/// Workers currently derive their per-domain wait from robots.txt plus local
/// config only, because tasks carry no job snapshot. Once tasks (or a side
/// table keyed by `job_id`) expose the job's `crawl_delay_ms` metadata, a
/// worker computes its delay as:
///
/// ```text
/// delay = robots.txt Crawl-delay  >  job.crawl_delay_ms  >  worker default
/// ```
///
/// i.e. exactly [`effective_crawl_delay`]. This thin alias exists so the M2
/// change is call-site wiring, not a policy redesign. Deliberately not called
/// anywhere yet.
#[allow(dead_code)]
pub fn worker_crawl_delay(
    job: Option<&CrawlJob>,
    robots_delay: Option<Duration>,
    worker_default_ms: u64,
) -> Duration {
    effective_crawl_delay(job, robots_delay, worker_default_ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::CrawlJob;

    fn job_with(policies: impl FnOnce(&mut CrawlJob)) -> CrawlJob {
        let mut job = CrawlJob::default();
        policies(&mut job);
        job
    }

    // ---------- effective_crawl_delay ----------

    #[test]
    fn delay_robots_txt_wins_over_everything() {
        let job = job_with(|j| j.crawl_delay_ms = Some(100));
        assert_eq!(
            effective_crawl_delay(Some(&job), Some(Duration::from_millis(5000)), 1000),
            Duration::from_millis(5000)
        );
    }

    #[test]
    fn delay_job_wins_over_default_when_no_robots() {
        let job = job_with(|j| j.crawl_delay_ms = Some(250));
        assert_eq!(
            effective_crawl_delay(Some(&job), None, 1000),
            Duration::from_millis(250)
        );
    }

    #[test]
    fn delay_falls_back_to_default() {
        let job = job_with(|j| j.crawl_delay_ms = None);
        assert_eq!(
            effective_crawl_delay(Some(&job), None, 1000),
            Duration::from_millis(1000)
        );
        assert_eq!(
            effective_crawl_delay(None, None, 750),
            Duration::from_millis(750)
        );
    }

    #[test]
    fn delay_none_job_still_honors_robots() {
        assert_eq!(
            effective_crawl_delay(None, Some(Duration::from_millis(900)), 100),
            Duration::from_millis(900)
        );
    }

    // ---------- external_links_allowed ----------

    #[test]
    fn external_none_job_is_permissive() {
        assert!(external_links_allowed(None, "example.com", None));
    }

    #[test]
    fn external_follow_true_admits_all() {
        let job = job_with(|j| j.follow_external_links = true);
        assert!(external_links_allowed(Some(&job), "random.org", None));
    }

    #[test]
    fn external_nonempty_allowlist_defers_to_is_url_allowed() {
        let job = job_with(|j| {
            j.follow_external_links = false;
            j.allowed_domains = Some(vec!["other.com".into()]);
        });
        // Even a foreign root returns true here: the allowlist gate already
        // decided inside CrawlJob::is_url_allowed.
        assert!(external_links_allowed(Some(&job), "example.com", None));
    }

    #[test]
    fn external_empty_allowlist_treated_as_unset() {
        let job = job_with(|j| {
            j.follow_external_links = false;
            j.allowed_domains = Some(vec![]);
        });
        assert!(!external_links_allowed(Some(&job), "foreign.net", None));
    }

    #[test]
    fn external_seed_lineage_match_and_mismatch() {
        let seeds = vec!["example.com".to_string(), "blog.other.co.uk".to_string()];
        let job = job_with(|j| j.follow_external_links = false);

        assert!(external_links_allowed(
            Some(&job),
            "example.com",
            Some(&seeds)
        ));
        // Case-insensitive root-domain comparison.
        assert!(external_links_allowed(
            Some(&job),
            "EXAMPLE.com",
            Some(&seeds)
        ));
        assert!(external_links_allowed(
            Some(&job),
            "blog.other.co.uk",
            Some(&seeds)
        ));
        assert!(!external_links_allowed(
            Some(&job),
            "unrelated.io",
            Some(&seeds)
        ));
    }

    #[test]
    fn external_quoted_seed_tokens_are_tolerated() {
        let seeds = vec![
            " \"example.com\" ".to_string(),
            "'sub.other.org'".to_string(),
        ];
        let job = job_with(|j| j.follow_external_links = false);
        assert!(external_links_allowed(
            Some(&job),
            "example.com",
            Some(&seeds)
        ));
        assert!(external_links_allowed(
            Some(&job),
            "SUB.Other.ORG",
            Some(&seeds)
        ));
        assert!(!external_links_allowed(
            Some(&job),
            "third.net",
            Some(&seeds)
        ));
    }

    #[test]
    fn external_unknown_seeds_denies() {
        let job = job_with(|j| j.follow_external_links = false);
        assert!(!external_links_allowed(Some(&job), "example.com", None));
        assert!(!external_links_allowed(
            Some(&job),
            "example.com",
            Some(&[])
        ));
    }

    // ---------- topic_relevant ----------

    #[test]
    fn topic_no_keywords_is_relevant() {
        assert!(topic_relevant(None, Some("anything"), Some("whatever")));
        let no_kw = job_with(|j| j.topic_keywords = None);
        assert!(topic_relevant(Some(&no_kw), Some("anything"), None));
        let empty_kw = job_with(|j| j.topic_keywords = Some(vec![]));
        assert!(topic_relevant(Some(&empty_kw), Some("anything"), None));
    }

    #[test]
    fn topic_keyword_in_title_case_insensitive() {
        let job = job_with(|j| j.topic_keywords = Some(vec!["rustlang".into()]));
        assert!(topic_relevant(
            Some(&job),
            Some("The RUSTLANG blog"),
            Some("body")
        ));
        assert!(!topic_relevant(
            Some(&job),
            Some("Go versus Zig"),
            Some("unrelated body")
        ));
    }

    #[test]
    fn topic_keyword_in_text_window() {
        let job = job_with(|j| j.topic_keywords = Some(vec!["needle".into()]));

        let mut inside = "x".repeat(1900);
        inside.push_str("NEEDLE");
        assert!(topic_relevant(Some(&job), None, Some(&inside)));

        let mut outside = "x".repeat(2100);
        outside.push_str("needle");
        assert!(!topic_relevant(Some(&job), None, Some(&outside)));
    }

    #[test]
    fn topic_any_keyword_matches_and_blank_ignored() {
        let job = job_with(|j| {
            j.topic_keywords = Some(vec!["   ".into(), "ferries".into()]);
        });
        assert!(topic_relevant(
            Some(&job),
            Some("Island ferries schedule"),
            None
        ));

        // Only blank keywords ⇒ no match possible.
        let blanks = job_with(|j| j.topic_keywords = Some(vec!["  ".into(), "".into()]));
        assert!(!topic_relevant(
            Some(&blanks),
            Some("anything"),
            Some("everything")
        ));
    }
}
