use anyhow::Result;
use arachne_core::{
    content::extractor,
    dedup::Deduplicator,
    domain,
    models::{CrawlJob, CrawlResult, CrawlStatus, CrawlTask, DiscoveredUrl},
    politeness::PolitenessLimiter,
};
use std::time::Duration;
use url::Url;
use uuid::Uuid;

#[tokio::test]
async fn test_e2e_pipeline_components() -> Result<()> {
    // 1. Domain & URL Normalization
    let raw_url = "example.com/search?q=rust#top";
    let normalized = domain::normalize_url(raw_url).unwrap();
    assert_eq!(normalized, "https://example.com/search?q=rust");

    let root_domain = domain::extract_root_domain(&normalized).unwrap();
    assert_eq!(root_domain, "example.com");

    // 2. SSRF & Egress Boundary Test
    assert!(domain::is_safe_egress_url("https://example.com/docs"));
    assert!(!domain::is_safe_egress_url("http://127.0.0.1/admin"));
    assert!(!domain::is_safe_egress_url(
        "http://169.254.169.254/latest/meta-data/"
    ));

    // 3. Deduplication Engine
    let dedup = Deduplicator::new(10_000, 0.001);
    assert!(!dedup.probably_seen(&normalized));
    dedup.mark_seen(&normalized);
    assert!(dedup.probably_seen(&normalized));
    assert_eq!(dedup.estimated_count(), 1);

    // 4. Politeness Engine & Rate Limiter
    let politeness = PolitenessLimiter::new(10);
    let start = std::time::Instant::now();
    politeness.wait_for_permission(&root_domain).await;
    politeness.wait_for_permission(&root_domain).await;
    assert!(start.elapsed() >= Duration::from_millis(5));

    // 5. Job Crawl Policy Test
    let job = CrawlJob {
        max_depth: Some(2),
        allowed_domains: Some(vec!["example.com".to_string()]),
        ..Default::default()
    };
    assert!(job.is_url_allowed("https://example.com/docs", 1, "example.com"));
    assert!(!job.is_url_allowed("https://other.com/about", 1, "other.com"));
    assert!(!job.is_url_allowed("https://example.com/deep", 3, "example.com"));

    // 6. HTML Extraction & Link Parser
    let html = r#"
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <title>Arachne Test Page</title>
        </head>
        <body>
            <h1>Welcome to Arachne</h1>
            <p>High performance web crawler in Rust.</p>
            <a href="/docs">Docs</a>
            <a href="https://other.org/about">About External</a>
        </body>
        </html>
    "#;
    let base_url = Url::parse("https://example.com/start").unwrap();
    let extracted = extractor::extract_from_html(html, &base_url);

    assert_eq!(extracted.title.as_deref(), Some("Arachne Test Page"));
    assert_eq!(extracted.language.as_deref(), Some("en"));
    assert!(extracted.text_content.contains("Welcome to Arachne"));
    assert_eq!(extracted.links.len(), 2);
    assert!(extracted
        .links
        .contains(&"https://example.com/docs".to_string()));

    // 7. Task & Result Models
    let job_id = Uuid::new_v4();
    let task = CrawlTask {
        url: normalized.clone(),
        job_id,
        domain: root_domain.clone(),
        depth: 0,
        priority: 10,
        kind: Default::default(),
        media: None,
    };

    let result = CrawlResult {
        source_url: task.url.clone(),
        job_id: task.job_id,
        status: CrawlStatus::Success,
        domain: Some(task.domain.clone()),
        content_ref: Some("file:///storage/hash.html".into()),
        title: extracted.title,
        language: extracted.language,
        content_length: Some(html.len()),
        content_hash: Some("abcdef123456".into()),
        discovered_urls: extracted
            .links
            .into_iter()
            .map(|l| DiscoveredUrl {
                url: l,
                source_url: task.url.clone(),
                job_id: task.job_id,
                depth: task.depth + 1,
            })
            .collect(),
        crawl_duration_ms: 25,
        crawled_at: chrono::Utc::now(),
        media_meta: None,
        media_probe: None,
    };

    assert!(result.status.is_success());
    assert_eq!(result.discovered_urls.len(), 2);
    assert_eq!(result.discovered_urls[0].depth, 1);

    Ok(())
}
