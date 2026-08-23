//! Sitemap (and sitemap-index) parsing via streaming quick-xml.

use anyhow::{Context, Result};
use quick_xml::events::Event;
use quick_xml::Reader;

/// One URL entry from a `<urlset>` sitemap.
#[derive(Debug, Clone, PartialEq)]
pub struct SitemapEntry {
    pub loc: String,
    pub lastmod: Option<String>,
}

/// Parsed sitemap content: either URL entries or child sitemap locations.
#[derive(Debug, Clone, PartialEq)]
pub enum Sitemap {
    Urlset(Vec<SitemapEntry>),
    /// Child sitemaps from a `<sitemapindex>`; each needs its own fetch.
    Index(Vec<String>),
}

/// Parse sitemap XML (utf-8 bytes). Handles both `<urlset>` and `<sitemapindex>`.
pub fn parse_sitemap(xml: &[u8]) -> Result<Sitemap> {
    let mut reader = Reader::from_reader(xml);
    reader.config_mut().trim_text(true);

    let mut urls = Vec::new();
    let mut child_maps = Vec::new();
    let mut current_text = String::new();
    // Which element we're inside: "url" or "sitemap"
    let mut in_block = Option::<&'static str>::None;
    let mut current_tag = Option::<String>::None;

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                match name.as_str() {
                    "url" => in_block = Some("url"),
                    "sitemap" => in_block = Some("sitemap"),
                    _ => {}
                }
                current_text.clear();
                if matches!(name.as_str(), "loc" | "lastmod") {
                    current_tag = Some(name);
                }
            }
            Ok(Event::Text(t)) => {
                if current_tag.is_some() {
                    current_text
                        .push_str(&String::from_utf8_lossy(t.unescape().unwrap_or_default().as_bytes()));
                }
            }
            Ok(Event::End(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if matches!(name.as_str(), "loc" | "lastmod") {
                    let tag = current_tag.take().unwrap_or_default();
                    let text = current_text.trim().to_string();
                    if !text.is_empty() {
                        match (in_block, tag.as_str()) {
                            (Some("sitemap"), "loc") => child_maps.push(text),
                            (Some("url"), "loc") => urls.push(SitemapEntry {
                                loc: text,
                                lastmod: None,
                            }),
                            (Some("url"), "lastmod") => {
                                if let Some(last) = urls.last_mut() {
                                    last.lastmod = Some(text);
                                }
                            }
                            _ => {}
                        }
                    }
                } else if matches!(name.as_str(), "url" | "sitemap") {
                    in_block = None;
                }
                current_text.clear();
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(e).context("sitemap XML parse error"),
            _ => {}
        }
    }

    if !child_maps.is_empty() {
        Ok(Sitemap::Index(child_maps))
    } else {
        Ok(Sitemap::Urlset(urls))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_urlset_with_lastmod() {
        let xml = br#"<?xml version="1.0" encoding="UTF-8"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
            <url><loc>https://a.com/x.mp3</loc><lastmod>2026-01-01</lastmod></url>
            <url><loc>https://a.com/y.flac</loc></url>
        </urlset>"#;
        let parsed = parse_sitemap(xml).unwrap();
        match parsed {
            Sitemap::Urlset(entries) => {
                assert_eq!(entries.len(), 2);
                assert_eq!(entries[0].loc, "https://a.com/x.mp3");
                assert_eq!(entries[0].lastmod.as_deref(), Some("2026-01-01"));
                assert_eq!(entries[1].lastmod, None);
            }
            other => panic!("expected urlset, got {other:?}"),
        }
    }

    #[test]
    fn parses_sitemap_index() {
        let xml = br#"<sitemapindex>
            <sitemap><loc>https://a.com/sm1.xml</loc></sitemap>
            <sitemap><loc>https://a.com/sm2.xml</loc></sitemap>
        </sitemapindex>"#;
        let parsed = parse_sitemap(xml).unwrap();
        assert_eq!(
            parsed,
            Sitemap::Index(vec![
                "https://a.com/sm1.xml".into(),
                "https://a.com/sm2.xml".into()
            ])
        );
    }
}
