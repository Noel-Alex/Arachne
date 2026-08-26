//! Generic media-link detection beyond audio: video containers and documents.
//!
//! Same philosophy as `audio_links`: extension-based classification of URLs
//! (works on absolute and relative forms), used by the coordinator to route
//! discovered links into the right download pipeline.

/// Video container extensions we treat as direct video links.
pub const VIDEO_EXTENSIONS: &[&str] = &["mp4", "m4v", "mkv", "webm", "avi", "mov", "mpg", "mpeg"];

/// Document extensions (papers, books, slides).
pub const DOCUMENT_EXTENSIONS: &[&str] = &["pdf", "epub", "doc", "docx", "ppt", "pptx", "txt"];

fn last_path_extension(url: &str) -> Option<&str> {
    let path = url.split(['?', '#']).next().unwrap_or(url);
    let last_seg = path.rsplit(['/']).next().unwrap_or("");
    let ext = last_seg.rsplit('.').next()?;
    // Only trust it as an extension if there IS a dot and non-empty stem.
    if !last_seg.contains('.') || ext.is_empty() || ext.len() == last_seg.len() {
        return None;
    }
    Some(ext)
}

fn matches_any(url: &str, exts: &[&str]) -> bool {
    last_path_extension(url).is_some_and(|ext| exts.iter().any(|e| ext.eq_ignore_ascii_case(e)))
}

/// True when a URL path ends in a known video extension.
pub fn has_video_extension(url: &str) -> bool {
    matches_any(url, VIDEO_EXTENSIONS)
}

/// True when a URL path ends in a known document extension.
pub fn has_document_extension(url: &str) -> bool {
    matches_any(url, DOCUMENT_EXTENSIONS)
}

/// Find anchors whose href passes `pred`, resolved against `base`. Deduped,
/// order preserved. Used by the worker to emit video/document candidates
/// alongside audio links.
pub fn links_by_extension(html: &str, base: &url::Url, pred: fn(&str) -> bool) -> Vec<String> {
    let doc = scraper::Html::parse_document(html);
    let mut found: Vec<String> = Vec::new();
    for el in doc.tree.nodes().filter_map(|n| n.value().as_element()) {
        if el.name() != "a" {
            continue;
        }
        let href = el.attr("href").unwrap_or_default();
        if href.is_empty() || !pred(href) {
            continue;
        }
        let resolved = match base.join(href) {
            Ok(u) => u,
            Err(_) => continue,
        };
        if resolved.scheme() == "http" || resolved.scheme() == "https" {
            let s = resolved.to_string();
            if !found.contains(&s) {
                found.push(s);
            }
        }
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_video_extensions() {
        assert!(has_video_extension("https://x.com/a/clip.mp4?token=1"));
        assert!(has_video_extension("https://x.com/a/CLIP.WEBM"));
        assert!(has_video_extension("vid.mkv"));
        assert!(!has_video_extension("https://x.com/a/song.mp3"));
        assert!(!has_video_extension("https://x.com/page.html"));
    }

    #[test]
    fn detects_document_extensions() {
        assert!(has_document_extension("https://x.com/papers/thesis.pdf"));
        assert!(has_document_extension("/books/book.epub#page=2"));
        assert!(!has_document_extension("https://x.com/a/movie.mp4"));
        assert!(!has_document_extension("no-extension"));
    }
}
