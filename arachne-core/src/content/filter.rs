//! Utilities for filtering content based on headers and size.

/// Check if the content type is HTML.
pub fn is_html_content_type(content_type: &str) -> bool {
    content_type.to_lowercase().contains("text/html")
}

/// Check if the content type is among the allowed types.
pub fn is_acceptable_content_type(content_type: &str, allowed: &[String]) -> bool {
    if allowed.is_empty() {
        return true;
    }
    let lower_ct = content_type.to_lowercase();
    allowed
        .iter()
        .any(|ct| lower_ct.contains(&ct.to_lowercase()))
}

/// Check if the content size is within the allowed limit.
pub fn is_within_size_limit(size: usize, max_size: usize) -> bool {
    size <= max_size
}
