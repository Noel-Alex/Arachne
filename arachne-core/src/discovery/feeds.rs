//! RSS/Atom/JSON Feed parsing with enclosure extraction via `feed-rs`.
//!
//! Enclosures (including iTunes and MediaRSS variants) point directly at
//! media URLs — the cheapest large-scale audio discovery path that exists.

use anyhow::Result;
use feed_rs::parser;
use url::Url;

/// A feed item's interesting bits.
#[derive(Debug, Clone, PartialEq)]
pub struct FeedEntry {
    /// The item's own link (landing page).
    pub link: Option<String>,
    pub title: Option<String>,
    /// Directly attached media: (url, mime_type).
    pub enclosures: Vec<(String, String)>,
}

/// Parse a feed from bytes. `base` resolves relative enclosure URLs.
pub fn parse_feed(xml: &[u8], base: &str) -> Result<Vec<FeedEntry>> {
    let base_url = Url::parse(base).ok();
    let feed = parser::Builder::new()
        .base_uri(base_url.as_ref().map(|u| u.as_str()))
        .build()
        .parse(xml)?;

    Ok(feed
        .entries
        .into_iter()
        .map(|e| FeedEntry {
            link: e.links.first().map(|l| l.href.clone()),
            title: e.title.map(|t| t.content),
            enclosures: e
                .media
                .iter()
                .flat_map(|m| {
                    m.content.iter().filter_map(|c| {
                        let url = c.url.as_ref()?.to_string();
                        let mime = c
                            .content_type
                            .as_ref()
                            .map(|t| t.to_string())
                            .unwrap_or_default();
                        Some((url, mime))
                    })
                })
                .collect(),
        })
        .collect())
}

/// True when a MIME type smells like audio we can decode.
pub fn is_audio_mime(mime: &str) -> bool {
    let m = mime.to_ascii_lowercase();
    m.starts_with("audio/") || m == "application/ogg"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_podcast_enclosures() {
        let xml = br#"<?xml version="1.0"?>
        <rss version="2.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
          <channel><title>T</title>
            <item>
              <title>Episode 1</title>
              <link>https://ex.com/ep1</link>
              <enclosure url="/audio/ep1.mp3" type="audio/mpeg" length="1000"/>
            </item>
          </channel>
        </rss>"#;
        let entries = parse_feed(xml, "https://ex.com/feed.xml").unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].link.as_deref(), Some("https://ex.com/ep1"));
        assert_eq!(
            entries[0].enclosures,
            vec![("https://ex.com/audio/ep1.mp3".to_string(), "audio/mpeg".to_string())]
        );
        assert!(is_audio_mime("audio/mpeg"));
        assert!(!is_audio_mime("text/html"));
    }
}
