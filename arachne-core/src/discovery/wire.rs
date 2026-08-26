//! Live wiring for the previously-dormant discovery parsers: fetch + parse
//! sitemaps and syndication feeds with hard content-type, size, and count
//! caps, plus candidate classification over a page's own link list.
//!
//! Contract: everything here is best-effort. Callers log failures at debug
//! and continue — discovery must never fail a page task.
//!
//! Security: every outbound discovery request funnels through
//! [`fetch_body_capped`], which enforces the crate-wide SSRF egress policy
//! ([`crate::domain::is_safe_egress_url`]) before any connection is attempted.
//! Candidate URLs are harvested from attacker-controlled page HTML, so the
//! guard at that choke point is load-bearing.

use std::time::Duration;

use anyhow::{Context, Result};
use tracing::debug;

use super::feeds::is_audio_mime;
use super::sitemap::{Sitemap, SitemapEntry, parse_sitemap};

/// Hard cap on any single discovery fetch body. Sitemaps and feeds are small
/// XML documents; anything bigger is hostile or mislabeled — abandon rather
/// than buffer.
const MAX_BODY_BYTES: usize = 5 * 1024 * 1024;

/// Per-request timeout for discovery probes (shorter than the shared page
/// fetch timeout; these are opportunistic side probes).
const FETCH_TIMEOUT: Duration = Duration::from_secs(10);

/// Fetch + parse a sitemap URL (one level of index recursion, capped at
/// `max_children` child sitemaps). Returns discovered page URLs.
///
/// Non-XML content types skip silently (empty vec); individual child-sitemap
/// failures are logged at debug and skipped — a partially working index
/// still contributes what it can.
pub async fn harvest_sitemap(
    client: &reqwest::Client,
    url: &str,
    max_children: usize,
    per_map_cap: usize,
) -> Result<Vec<String>> {
    let (content_type, body) = fetch_body_capped(client, url).await?;
    if !looks_like_xml(&content_type) {
        return Ok(Vec::new()); // mislabeled endpoint — skip silently
    }
    match parse_sitemap(&body)? {
        Sitemap::Urlset(entries) => Ok(capped_locs(&entries, per_map_cap)),
        Sitemap::Index(children) => {
            // Exactly ONE level of recursion: children go through
            // harvest_leaf_map, which ignores indexes found at that level.
            let mut urls = Vec::new();
            for child in children.iter().take(max_children) {
                match harvest_leaf_map(client, child, per_map_cap).await {
                    Ok(found) => urls.extend(found),
                    Err(e) => debug!(child = %child, error = %e, "child sitemap fetch failed"),
                }
            }
            Ok(urls)
        }
    }
}

/// Fetch one child sitemap and collect at most `per_map_cap` page URLs.
/// A sitemapindex found at this level contributes nothing (recursion is
/// exactly one level deep).
async fn harvest_leaf_map(
    client: &reqwest::Client,
    url: &str,
    per_map_cap: usize,
) -> Result<Vec<String>> {
    let (content_type, body) = fetch_body_capped(client, url).await?;
    if !looks_like_xml(&content_type) {
        return Ok(Vec::new());
    }
    match parse_sitemap(&body)? {
        Sitemap::Urlset(entries) => Ok(capped_locs(&entries, per_map_cap)),
        Sitemap::Index(_) => Ok(Vec::new()),
    }
}

/// Fetch + parse a feed; returns (page links, enclosure urls whose mime
/// passes `is_audio_mime` or carries an audio file extension).
///
/// Parses with feed-rs directly rather than through `feeds::parse_feed`:
/// that helper collapses each entry to `links.first()` and only reads
/// `entry.media`, but feed-rs keeps Atom `<link rel="enclosure">` in
/// `entry.links` (never in `entry.media`) — so Atom enclosures are invisible
/// through it. Here we keep every alternate link AND every `rel=enclosure`
/// link with an audio mime.
pub async fn harvest_feed(
    client: &reqwest::Client,
    url: &str,
) -> Result<(Vec<String>, Vec<String>)> {
    let (content_type, body) = fetch_body_capped(client, url).await?;
    if !looks_like_syndication(&content_type) {
        return Ok((Vec::new(), Vec::new())); // not a feed — skip silently
    }
    let base = url::Url::parse(url).ok();
    let feed = feed_rs::parser::Builder::new()
        .base_uri(base.as_ref().map(|u| u.as_str()))
        .build()
        .parse(&*body)?;
    let mut links = Vec::new();
    let mut enclosures = Vec::new();
    for entry in &feed.entries {
        for link in &entry.links {
            match link.rel.as_deref() {
                None | Some("alternate") => links.push(link.href.clone()),
                Some("enclosure") => {
                    let mime = link
                        .media_type
                        .as_ref()
                        .map(|t| t.to_string())
                        .unwrap_or_default();
                    if is_audio_mime(&mime) || has_audio_extension(&link.href) {
                        enclosures.push(link.href.clone());
                    }
                }
                _ => {} // self, via, replies... — not crawl targets
            }
        }
        // RSS2 <enclosure> and MediaRSS media:content land in entry.media.
        for obj in &entry.media {
            for content in &obj.content {
                let Some(enclosure_url) = content.url.as_ref().map(|u| u.to_string()) else {
                    continue;
                };
                let mime = content
                    .content_type
                    .as_ref()
                    .map(|t| t.to_string())
                    .unwrap_or_default();
                if is_audio_mime(&mime) || has_audio_extension(&enclosure_url) {
                    enclosures.push(enclosure_url);
                }
            }
        }
    }
    Ok((links, enclosures))
}

/// Classify a page's OWN links into (sitemap candidates, feed candidates).
/// Classes are mutually exclusive per URL (feed patterns win, so `/feed.xml`
/// is probed as a feed, not also as a sitemap), deduped, and each class is
/// capped at 3 so a link-farm page cannot trigger unbounded probing.
pub fn discovery_candidates(links: &[String]) -> (Vec<String>, Vec<String>) {
    /// Cap per candidate class per page.
    const CAP: usize = 3;

    let mut sitemaps: Vec<String> = Vec::new();
    let mut feeds: Vec<String> = Vec::new();
    for link in links {
        if sitemaps.len() >= CAP && feeds.len() >= CAP {
            break;
        }
        let path = link.split(['?', '#']).next().unwrap_or(link);
        let path_lower = path.to_ascii_lowercase();
        let last_segment = path
            .rsplit('/')
            .next()
            .unwrap_or_default()
            .to_ascii_lowercase();
        let is_feed = path_lower.contains("/feed")
            || ["rss", "atom", "feed"]
                .iter()
                .any(|token| last_segment.contains(token));
        if is_feed {
            if feeds.len() < CAP && !feeds.contains(link) {
                feeds.push(link.clone());
            }
        } else if last_segment.ends_with(".xml") && sitemaps.len() < CAP && !sitemaps.contains(link)
        {
            sitemaps.push(link.clone());
        }
    }
    (sitemaps, feeds)
}

/// True when a content type smells like sitemap XML.
fn looks_like_xml(content_type: &str) -> bool {
    content_type.to_ascii_lowercase().contains("xml")
}

/// True when a content type suggests RSS/Atom/feed-ish markup.
fn looks_like_syndication(content_type: &str) -> bool {
    let ct = content_type.to_ascii_lowercase();
    ["xml", "rss", "atom", "feed"]
        .iter()
        .any(|token| ct.contains(token))
}

/// First `cap` locations of a urlset — keeps a 50k-entry map from flooding
/// downstream admission in one shot.
fn capped_locs(entries: &[SitemapEntry], cap: usize) -> Vec<String> {
    entries.iter().take(cap).map(|e| e.loc.clone()).collect()
}

/// Audio by URL extension — fallback for servers that ship `type=""` or a
/// generic `application/octet-stream`.
fn has_audio_extension(url: &str) -> bool {
    let path = url.split(['?', '#']).next().unwrap_or(url);
    path.rsplit('.').next().is_some_and(|ext| {
        matches!(
            ext.to_ascii_lowercase().as_str(),
            "mp3" | "m4a" | "aac" | "ogg" | "oga" | "opus" | "wav" | "flac" | "wma"
        )
    })
}

/// True when the egress guard is actually enforcing: everywhere EXCEPT when
/// compiling arachne-core's own test harness. Exposed `pub(crate)` so tests
/// can assert on the composition.
///
/// Why the `cfg!(test)` allowance is sound: `cfg!(test)` is set only when
/// compiling arachne-core's OWN test harness (`cargo test -p arachne-core`).
/// Any dependent crate — arachne-worker included — and every release build
/// compile this module with `test=false`, so [`egress_guard_enforces`] is true
/// and enforcement intact there; the allowance exists purely so this module's
/// localhost unit tests can bind 127.0.0.1 servers that
/// [`crate::domain::is_safe_egress_url`] would rightly reject. It never
/// weakens the worker's runtime behavior.
pub(crate) fn egress_guard_enforces() -> bool {
    !cfg!(test)
}

/// Single egress-policy decision for every discovery fetch: when enforcing,
/// true only if `url` passes the crate-wide SSRF guard; inside this crate's
/// own test harness, always true (see [`egress_guard_enforces`]).
fn guard_ok(url: &str) -> bool {
    if !egress_guard_enforces() {
        return true; // test harness only — see egress_guard_enforces docs
    }
    crate::domain::is_safe_egress_url(url)
}

/// One bounded GET: 10s timeout, streamed body with a running length guard
/// capped at MAX_BODY_BYTES. Returns (content-type, body). Non-2xx statuses,
/// oversized bodies, and URLs rejected by the SSRF egress guard are errors;
/// callers treat them as skip-and-continue.
///
/// This function is the SINGLE enforcement choke point for discovery egress:
/// top-level sitemap/feed candidates, feed fetches, and sitemap-index child
/// recursion all reach the network only through here, so one check covers all.
async fn fetch_body_capped(client: &reqwest::Client, url: &str) -> Result<(String, Vec<u8>)> {
    if !guard_ok(url) {
        anyhow::bail!("discovery fetch blocked by egress guard: {url}");
    }

    let mut response = client
        .get(url)
        .timeout(FETCH_TIMEOUT)
        .send()
        .await
        .with_context(|| format!("discovery fetch failed: {url}"))?;

    let status = response.status();
    if !status.is_success() {
        anyhow::bail!("discovery fetch {url}: HTTP {status}");
    }

    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();

    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .context("discovery response body read failed")?
    {
        if body.len() + chunk.len() > MAX_BODY_BYTES {
            anyhow::bail!("discovery body exceeds {MAX_BODY_BYTES} bytes: {url}");
        }
        body.extend_from_slice(&chunk);
    }
    Ok((content_type, body))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------- pure helpers ----------

    #[test]
    fn capped_locs_truncates_urlset() {
        let entries: Vec<SitemapEntry> = (0..5)
            .map(|i| SitemapEntry {
                loc: format!("https://a.com/p{i}"),
                lastmod: None,
            })
            .collect();
        assert_eq!(capped_locs(&entries, 2).len(), 2);
        assert_eq!(capped_locs(&entries, 50).len(), 5);
        assert!(capped_locs(&[], 10).is_empty());
    }

    #[test]
    fn xml_gate_matches_only_xml_types() {
        assert!(looks_like_xml("application/xml"));
        assert!(looks_like_xml("text/xml; charset=utf-8"));
        assert!(!looks_like_xml("text/html"));
        assert!(!looks_like_xml("application/json"));
    }

    #[test]
    fn syndication_gate_accepts_xml_rss_atom_feed() {
        for ct in [
            "application/xml",
            "application/rss+xml",
            "application/atom+xml",
            "application/feed+json",
            "text/xml",
        ] {
            assert!(looks_like_syndication(ct), "{ct} should pass");
        }
        assert!(!looks_like_syndication("text/html"));
        assert!(!looks_like_syndication("image/png"));
    }

    #[test]
    fn audio_extension_fallback() {
        assert!(has_audio_extension("https://x.com/a/track.mp3?dl=1"));
        assert!(has_audio_extension("https://x.com/a/TRACK.FLAC#t=0"));
        assert!(has_audio_extension("/audio/ep1.ogg"));
        assert!(!has_audio_extension("https://x.com/a/clip.mp4"));
        assert!(!has_audio_extension("https://x.com/a/page.html"));
        assert!(!has_audio_extension("https://x.com/noextension"));
    }

    #[test]
    fn candidates_classify_cap_and_dedupe() {
        let links: Vec<String> = [
            "https://a.com/sitemap.xml",
            "https://a.com/sitemap.xml", // duplicate
            "https://a.com/feed",        // /feed path -> feed
            "https://b.com/news.rss",    // rss in last segment -> feed, NOT also sitemap
            "https://c.com/pages.xml",   // plain xml -> sitemap
            "https://d.com/sm1.xml",
            "https://e.com/sm2.xml",
            "https://f.com/sm3.xml",
            "https://g.com/sm4.xml", // fifth plain xml -> capped away at 3
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let (sitemaps, feeds) = discovery_candidates(&links);
        assert_eq!(
            feeds,
            vec![
                "https://a.com/feed".to_string(),
                "https://b.com/news.rss".to_string(),
            ]
        );
        assert_eq!(
            sitemaps,
            vec![
                "https://a.com/sitemap.xml".to_string(),
                "https://c.com/pages.xml".to_string(),
                "https://d.com/sm1.xml".to_string(),
            ]
        );
    }

    #[test]
    fn candidates_ignore_non_xml_non_feed_links() {
        let links: Vec<String> = [
            "https://a.com/page.html",
            "https://a.com/style.css",
            "https://a.com/api/data.json",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let (sitemaps, feeds) = discovery_candidates(&links);
        assert!(sitemaps.is_empty());
        assert!(feeds.is_empty());
    }

    // ---------- egress-guard composition ----------
    //
    // fetch_body_capped consults guard_ok before every request. Inside THIS
    // test harness cfg!(test) short-circuits guard_ok to true (so the
    // networked tests below may hit their 127.0.0.1 fixtures), which makes the
    // false branch of guard_ok untestable in-crate. What IS testable purely,
    // and what actually matters, is the composition: guard_ok's only
    // enforcement path delegates to domain::is_safe_egress_url, so we pin (a)
    // that policy function rejecting every blocked URL shape, (b) an honest
    // report from egress_guard_enforces(), and (c) guard_ok passing through to
    // that same verdict whenever enforcement is active.

    /// The exact blocked shapes an attacker plants in page HTML.
    #[test]
    fn policy_rejects_ssrf_url_shapes() {
        assert!(!crate::domain::is_safe_egress_url(
            "http://127.0.0.1/latest/meta-data/sitemap.xml"
        ));
        assert!(!crate::domain::is_safe_egress_url(
            "http://169.254.169.254/sm.xml"
        ));
        assert!(!crate::domain::is_safe_egress_url(
            "http://[::ffff:10.0.0.1]/sm.xml"
        ));
        assert!(!crate::domain::is_safe_egress_url(
            "file:///etc/sitemap.xml"
        ));
        assert!(!crate::domain::is_safe_egress_url("not a url at all"));
    }

    /// Honest self-report: within arachne-core's own harness the allowance is
    /// active (enforcement off); any other compilation context must see
    /// enforcement ON.
    #[test]
    fn egress_guard_enforces_reflects_test_mode() {
        assert_eq!(egress_guard_enforces(), !cfg!(test));
        assert!(!egress_guard_enforces(), "this IS the test harness");
    }

    /// In any non-test compilation guard_ok(url) must equal
    /// is_safe_egress_url(url): true for public http(s), false for everything
    /// the policy rejects. We cannot flip cfg!(test) here, so we pin the
    /// delegated verdict and guard_ok's observable harness behavior.
    #[test]
    fn guard_ok_delegates_to_policy_when_enforcing() {
        let public = "https://example.com/sitemap.xml";
        // The delegated verdict...
        assert!(crate::domain::is_safe_egress_url(public));
        for url in [
            "http://127.0.0.1/",
            "http://[::ffff:10.0.0.1]/",
            "gopher://127.0.0.1:70/",
        ] {
            assert!(!crate::domain::is_safe_egress_url(url));
        }
        // ...and guard_ok's observable behavior: identical to the policy for
        // the public URL, open even for rejected forms ONLY because this is
        // the one compilation context where the test allowance is active.
        assert_eq!(guard_ok(public), crate::domain::is_safe_egress_url(public));
        assert!(guard_ok("http://127.0.0.1/"));
        assert!(!egress_guard_enforces());
    }

    // ---------- networked tests through the real fetch path ----------
    //
    // Tiny hand-rolled HTTP server: one queued response per connection,
    // `__BASE__` in bodies replaced with the real base URL. Surplus
    // connections get HTTP 500 so over-fetching fails fast instead of
    // hanging. Handles are aborted at test end.

    async fn start_xml_server(
        bodies: Vec<(&'static str, String)>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        use tokio::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base = format!("http://{addr}");
        let bodies: Vec<(String, String)> = bodies
            .into_iter()
            .map(|(ct, b)| (ct.to_string(), b.replace("__BASE__", &base)))
            .collect();
        let handle = tokio::spawn(async move {
            let mut queue = bodies.into_iter();
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                match queue.next() {
                    Some((ct, body)) => serve_response(&mut sock, "200 OK", &ct, &body).await,
                    None => {
                        serve_response(&mut sock, "500 Internal Server Error", "text/plain", "")
                            .await
                    }
                }
            }
        });
        (base, handle)
    }

    async fn serve_response(
        sock: &mut tokio::net::TcpStream,
        status: &str,
        content_type: &str,
        body: &str,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut buf = [0u8; 4096];
        loop {
            let n = sock.read(&mut buf).await.unwrap_or(0);
            if n == 0 || buf[..n].windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        let head = format!(
            "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        let _ = sock.write_all(head.as_bytes()).await;
        let _ = sock.write_all(body.as_bytes()).await;
        let _ = sock.shutdown().await;
    }

    #[tokio::test]
    async fn harvests_urlset_pages_over_http() {
        let (base, handle) = start_xml_server(vec![(
            "application/xml",
            "<?xml version=\"1.0\"?>\
             <urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\
             <url><loc>__BASE__/p1</loc></url>\
             <url><loc>__BASE__/p2</loc><lastmod>2026-01-01</lastmod></url>\
             </urlset>"
                .into(),
        )])
        .await;
        let client = reqwest::Client::new();
        let urls = harvest_sitemap(&client, &format!("{base}/sm.xml"), 3, 10)
            .await
            .unwrap();
        assert_eq!(urls, vec![format!("{base}/p1"), format!("{base}/p2")]);
        handle.abort();
    }

    #[tokio::test]
    async fn index_recurses_exactly_one_level() {
        let (base, handle) = start_xml_server(vec![
            (
                "application/xml",
                "<sitemapindex>\
                 <sitemap><loc>__BASE__/c1.xml</loc></sitemap>\
                 <sitemap><loc>__BASE__/c2-nested.xml</loc></sitemap>\
                 </sitemapindex>"
                    .into(),
            ),
            (
                "text/xml",
                "<urlset><url><loc>__BASE__/a</loc></url>\
                 <url><loc>__BASE__/b</loc></url></urlset>"
                    .into(),
            ),
            // c2 is itself an index: at depth 1 it must contribute NOTHING
            // (no second-level recursion, no fetch of deeper.xml).
            (
                "application/xml",
                "<sitemapindex><sitemap><loc>__BASE__/deeper.xml</loc></sitemap></sitemapindex>"
                    .into(),
            ),
        ])
        .await;
        let client = reqwest::Client::new();
        let urls = harvest_sitemap(&client, &format!("{base}/idx.xml"), 5, 10)
            .await
            .unwrap();
        assert_eq!(urls, vec![format!("{base}/a"), format!("{base}/b")]);
        handle.abort();
    }

    #[tokio::test]
    async fn max_children_caps_child_fetches() {
        // Index lists four children but only two child bodies are queued;
        // surplus connections get 500. Only the first two may be fetched.
        let (base, handle) = start_xml_server(vec![
            (
                "application/xml",
                "<sitemapindex>\
                 <sitemap><loc>__BASE__/c1.xml</loc></sitemap>\
                 <sitemap><loc>__BASE__/c2.xml</loc></sitemap>\
                 <sitemap><loc>__BASE__/c3.xml</loc></sitemap>\
                 <sitemap><loc>__BASE__/c4.xml</loc></sitemap>\
                 </sitemapindex>"
                    .into(),
            ),
            (
                "text/xml",
                "<urlset><url><loc>__BASE__/one</loc></url></urlset>".into(),
            ),
            (
                "text/xml",
                "<urlset><url><loc>__BASE__/two</loc></url></urlset>".into(),
            ),
        ])
        .await;
        let client = reqwest::Client::new();
        let urls = harvest_sitemap(&client, &format!("{base}/idx.xml"), 2, 10)
            .await
            .unwrap();
        assert_eq!(urls, vec![format!("{base}/one"), format!("{base}/two")]);
        handle.abort();
    }

    #[tokio::test]
    async fn per_map_cap_bounds_entries_per_urlset() {
        let entries: String = (0..50)
            .map(|i| format!("<url><loc>__BASE__/p{i}</loc></url>"))
            .collect();
        let (base, handle) = start_xml_server(vec![(
            "application/xml",
            format!("<urlset>{entries}</urlset>"),
        )])
        .await;
        let client = reqwest::Client::new();
        let urls = harvest_sitemap(&client, &format!("{base}/big.xml"), 3, 7)
            .await
            .unwrap();
        assert_eq!(urls.len(), 7);
        assert_eq!(urls[0], format!("{base}/p0"));
        handle.abort();
    }

    #[tokio::test]
    async fn non_xml_content_type_skips_silently_sitemap() {
        let (base, handle) = start_xml_server(vec![(
            "text/html",
            "<html><body>not a sitemap</body></html>".into(),
        )])
        .await;
        let client = reqwest::Client::new();
        let urls = harvest_sitemap(&client, &format!("{base}/sm.xml"), 3, 10)
            .await
            .unwrap();
        assert!(urls.is_empty());
        handle.abort();
    }

    #[tokio::test]
    async fn non_feed_content_type_skips_silently() {
        let (base, handle) = start_xml_server(vec![("application/json", "{}".into())]).await;
        let client = reqwest::Client::new();
        let (links, audio) = harvest_feed(&client, &format!("{base}/feed"))
            .await
            .unwrap();
        assert!(links.is_empty() && audio.is_empty());
        handle.abort();
    }

    #[tokio::test]
    async fn oversized_body_is_aborted() {
        let big = "x".repeat(5 * 1024 * 1024 + 16);
        let (base, handle) = start_xml_server(vec![(
            "application/xml",
            format!("<urlset><!-- {big} --></urlset>"),
        )])
        .await;
        let client = reqwest::Client::new();
        let res = harvest_sitemap(&client, &format!("{base}/huge.xml"), 1, 10).await;
        assert!(res.is_err(), "oversized body must be rejected");
        handle.abort();
    }

    #[tokio::test]
    async fn http_error_status_is_an_error_not_a_panic() {
        let (base, handle) = start_xml_server(vec![]).await; // everything 500s
        let client = reqwest::Client::new();
        assert!(
            harvest_sitemap(&client, &format!("{base}/sm.xml"), 1, 10)
                .await
                .is_err()
        );
        assert!(
            harvest_feed(&client, &format!("{base}/f.xml"))
                .await
                .is_err()
        );
        handle.abort();
    }

    #[tokio::test]
    async fn harvests_rss_feed_with_enclosures() {
        let (base, handle) = start_xml_server(vec![(
            "application/rss+xml",
            "<?xml version=\"1.0\"?>\
             <rss version=\"2.0\"><channel><title>T</title>\
             <item>\
               <title>Episode 1</title>\
               <link>__BASE__/ep1</link>\
               <enclosure url=\"__BASE__/a1.mp3\" type=\"audio/mpeg\" length=\"1\"/>\
               <enclosure url=\"__BASE__/v1.mp4\" type=\"video/mp4\" length=\"1\"/>\
               <enclosure url=\"/audio/relative.ogg\"/>\
             </item>\
             </channel></rss>"
                .into(),
        )])
        .await;
        let client = reqwest::Client::new();
        let (links, audio) = harvest_feed(&client, &format!("{base}/feed.xml"))
            .await
            .unwrap();
        assert_eq!(links, vec![format!("{base}/ep1")]);
        assert_eq!(
            audio,
            vec![
                format!("{base}/a1.mp3"),
                format!("{base}/audio/relative.ogg"), // base-resolved + extension fallback
            ]
        );
        handle.abort();
    }

    #[tokio::test]
    async fn harvests_atom_entry_with_enclosure_link() {
        let (base, handle) = start_xml_server(vec![(
            "application/atom+xml",
            "<?xml version=\"1.0\"?>\
             <feed xmlns=\"http://www.w3.org/2005/Atom\"><title>A</title>\
             <entry>\
               <title>E1</title>\
               <link href=\"__BASE__/entry1\"/>\
               <link rel=\"enclosure\" href=\"__BASE__/e1.m4a\" type=\"audio/x-m4a\"/>\
             </entry>\
             </feed>"
                .into(),
        )])
        .await;
        let client = reqwest::Client::new();
        let (links, audio) = harvest_feed(&client, &format!("{base}/atom.xml"))
            .await
            .unwrap();
        assert_eq!(links, vec![format!("{base}/entry1")]);
        assert_eq!(audio, vec![format!("{base}/e1.m4a")]);
        handle.abort();
    }
}
