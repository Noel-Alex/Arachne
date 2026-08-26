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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn html_content_type_variants() {
        assert!(is_html_content_type("text/html"));
        assert!(is_html_content_type("Text/HTML; charset=utf-8"));
        // Known gap: application/xhtml+xml is HTML but is not matched today
        // (only the literal "text/html" substring is checked). Pins current
        // behavior so a widening change is deliberate.
        assert!(!is_html_content_type("application/xhtml+xml"));
        assert!(!is_html_content_type(""));
    }

    #[test]
    fn size_limit_boundary() {
        assert!(is_within_size_limit(1024, 1024));
        assert!(!is_within_size_limit(1025, 1024));
        assert!(is_within_size_limit(usize::MAX, usize::MAX));
    }
}
