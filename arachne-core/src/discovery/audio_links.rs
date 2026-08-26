//! Generic audio-link detection in HTML: extension matches, `<audio>`/`<source>`
//! elements, and `rel=enclosure` anchors.

use url::Url;

/// File extensions we treat as direct audio links.
pub const AUDIO_EXTENSIONS: &[&str] = &["mp3", "flac", "ogg", "oga", "wav", "m4a", "opus", "aac"];

/// True when a URL (absolute or relative) path ends in a known audio extension.
pub fn has_audio_extension(url: &str) -> bool {
    // Take the final path component, ignoring query/fragment.
    let path = url.split(['?', '#']).next().unwrap_or(url);
    let last_seg = path.rsplit(['/']).next().unwrap_or("");
    last_seg
        .rsplit('.')
        .next()
        // Only trust it as an extension if there IS a dot and non-empty stem.
        .filter(|_| last_seg.contains('.'))
        .map(|ext| AUDIO_EXTENSIONS.iter().any(|e| ext.eq_ignore_ascii_case(e)))
        .unwrap_or(false)
}

/// Candidate audio URLs found in an HTML document, resolved against `base`.
/// Order of discovery: extension-matched anchors, `<audio>/<source> src`,
/// `rel=enclosure` anchors. Duplicates removed, order otherwise preserved.
pub fn find_audio_links(html: &str, base: &Url) -> Vec<String> {
    let doc = scraper::Html::parse_document(html);
    let mut found: Vec<String> = Vec::new();
    let mut push = |raw: &str| {
        if raw.is_empty() {
            return;
        }
        // data:/mailto:/javascript: etc. resolve to garbage; skip early.
        if !raw.starts_with("http")
            && !raw.starts_with('/')
            && !raw.starts_with('#')
            && !raw.contains(':')
        {
            // bare relative path like "song.mp3" — fine
        }
        let resolved = match base.join(raw) {
            Ok(u) => u,
            Err(_) => return,
        };
        if resolved.scheme() != "http" && resolved.scheme() != "https" {
            return;
        }
        let s = resolved.to_string();
        if !found.contains(&s) {
            found.push(s);
        }
    };

    for el in doc.tree.nodes().filter_map(|n| n.value().as_element()) {
        let name = el.name();
        if name == "a" {
            let href = el.attr("href").unwrap_or_default();
            if has_audio_extension(href)
                || el.attr("rel").is_some_and(|r| {
                    r.split_whitespace()
                        .any(|t| t.eq_ignore_ascii_case("enclosure"))
                })
            {
                push(href);
            }
        } else if matches!(name, "audio" | "source" | "embed")
            && let Some(src) = el.attr("src")
        {
            push(src);
        }
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_audio_sources() {
        let html = r#"
            <html><body>
              <a href="/songs/one.mp3">One</a>
              <a href="two.flac">Two</a>
              <a href="/pages/info.html" rel="nofollow">Not audio</a>
              <a href="/pod/ep.rss" rel="enclosure">Feed enclosure</a>
              <audio controls><source src="https://cdn.ex.com/three.ogg" type="audio/ogg"></audio>
              <img src="cover.jpg">
            </body></html>"#;
        let base = Url::parse("https://ex.com/music/").unwrap();
        let links = find_audio_links(html, &base);
        assert_eq!(
            links,
            vec![
                "https://ex.com/songs/one.mp3".to_string(),
                "https://ex.com/music/two.flac".to_string(),
                "https://ex.com/pod/ep.rss".to_string(), // rel=enclosure counts even without audio ext
                "https://cdn.ex.com/three.ogg".to_string(),
            ]
        );
    }

    #[test]
    fn extension_check_handles_queries() {
        assert!(has_audio_extension("https://x.com/a/song.mp3?token=1"));
        assert!(has_audio_extension("https://x.com/a/SONG.MP3"));
        assert!(!has_audio_extension("https://x.com/a/page.html"));
    }
}
