//! Content extraction from HTML.

use scraper::{Html, Selector};
use url::Url;

/// Represents content extracted from an HTML page.
pub struct ExtractedContent {
    pub title: Option<String>,
    pub meta_description: Option<String>,
    pub language: Option<String>,
    pub canonical_url: Option<String>,
    pub text_content: String,
    pub tag_skeleton: String,
    pub links: Vec<String>,
}

/// Extract relevant information from an HTML document.
pub fn extract_from_html(html: &str, base_url: &Url) -> ExtractedContent {
    let document = Html::parse_document(html);

    let title_selector = Selector::parse("title").unwrap();
    let title = document
        .select(&title_selector)
        .next()
        .map(|el| el.text().collect::<Vec<_>>().join(" "));

    let meta_desc_selector = Selector::parse("meta[name=\"description\"]").unwrap();
    let meta_description = document
        .select(&meta_desc_selector)
        .next()
        .and_then(|el| el.value().attr("content").map(String::from));

    let html_selector = Selector::parse("html").unwrap();
    let language = document
        .select(&html_selector)
        .next()
        .and_then(|el| el.value().attr("lang").map(String::from));

    let canon_selector = Selector::parse("link[rel=\"canonical\"]").unwrap();
    let canonical_url = document
        .select(&canon_selector)
        .next()
        .and_then(|el| el.value().attr("href").map(String::from));

    ExtractedContent {
        title,
        meta_description,
        language,
        canonical_url,
        text_content: extract_text(&document),
        tag_skeleton: extract_tag_skeleton(&document),
        links: extract_links(&document, base_url),
    }
}

/// Extract the tag skeleton of the HTML document.
pub fn extract_tag_skeleton(document: &Html) -> String {
    let mut skeleton = String::new();
    for node in document.tree.values() {
        if let scraper::node::Node::Element(el) = node {
            skeleton.push('<');
            skeleton.push_str(el.name());
            skeleton.push('>');
        }
    }
    skeleton
}

/// Extract clean text content from the HTML document.
pub fn extract_text(document: &Html) -> String {
    let mut text = String::new();
    for node in document.tree.values() {
        if let scraper::node::Node::Text(t) = node {
            let t_text = t.text.trim();
            if !t_text.is_empty() {
                if !text.is_empty() {
                    text.push(' ');
                }
                text.push_str(t_text);
            }
        }
    }
    text
}

/// Extract and resolve all links from the HTML document.
pub fn extract_links(document: &Html, base_url: &Url) -> Vec<String> {
    let a_selector = Selector::parse("a[href]").unwrap();
    let mut links = Vec::new();

    for element in document.select(&a_selector) {
        if let Some(href) = element.value().attr("href")
            && let Ok(resolved) = base_url.join(href)
        {
            // Ignore fragment
            let mut url = resolved;
            url.set_fragment(None);
            links.push(url.to_string());
        }
    }

    links
}
