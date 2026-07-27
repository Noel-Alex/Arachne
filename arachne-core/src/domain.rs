//! Domain extraction and normalization.

use psl::{List, Psl};
use url::{Host, Url};

/// Extract the root domain (e.g. example.com) from a URL string, using the public suffix list.
/// Returns IP addresses as-is.
pub fn extract_root_domain(url_str: &str) -> Option<String> {
    let parsed_url = Url::parse(url_str).ok()?;
    let host = parsed_url.host()?;

    match host {
        Host::Ipv4(ip) => Some(ip.to_string()),
        Host::Ipv6(ip) => Some(ip.to_string()),
        Host::Domain(host_str) => {
            if let Some(domain) = psl::domain(host_str.as_bytes()) {
                Some(std::str::from_utf8(domain.as_bytes()).ok()?.to_string())
            } else {
                Some(host_str.to_string())
            }
        }
    }
}

/// Get the full domain (e.g. www.example.com) from a URL string.
pub fn get_domain(url_str: &str) -> Option<String> {
    let parsed_url = Url::parse(url_str).ok()?;
    parsed_url.host_str().map(|s| s.to_string())
}

/// Normalize a URL string: ensure scheme, remove fragments, trailing slashes.
pub fn normalize_url(url_str: &str) -> Option<String> {
    let raw = if !url_str.contains("://") {
        format!("https://{}", url_str)
    } else {
        url_str.to_string()
    };

    let mut url = Url::parse(&raw).ok()?;
    url.set_fragment(None);
    let mut s = url.to_string();
    if s.ends_with('/') && s.as_str() != "https://" && s.as_str() != "http://" {
        s.pop();
    }
    Some(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_root_domain() {
        assert_eq!(extract_root_domain("http://example.com").unwrap(), "example.com");
        assert_eq!(extract_root_domain("https://www.example.com").unwrap(), "example.com");
        assert_eq!(extract_root_domain("http://blog.example.co.uk").unwrap(), "example.co.uk");
        assert_eq!(extract_root_domain("http://192.168.1.1").unwrap(), "192.168.1.1");
    }

    #[test]
    fn test_normalize_url() {
        assert_eq!(
            normalize_url("example.com/path/").as_deref(),
            Some("https://example.com/path")
        );
        assert_eq!(
            normalize_url("https://example.com/path#section").as_deref(),
            Some("https://example.com/path")
        );
    }
}
