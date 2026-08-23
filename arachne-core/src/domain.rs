use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
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

    if !is_safe_egress_url(url.as_str()) {
        return None;
    }

    let mut s = url.to_string();
    if s.ends_with('/') && s.as_str() != "https://" && s.as_str() != "http://" {
        s.pop();
    }
    Some(s)
}

/// SSRF & Egress Security Boundary Guard.
/// Blocks private IPs, loopback, link-local, cloud metadata (169.254.169.254), internal TLDs, and unsafe schemes/ports.
pub fn is_safe_egress_url(url_str: &str) -> bool {
    let parsed = match Url::parse(url_str) {
        Ok(u) => u,
        Err(_) => return false,
    };

    // Scheme check
    match parsed.scheme() {
        "http" | "https" => {}
        _ => return false,
    }

    // Port check
    if let Some(port) = parsed.port() {
        match port {
            80 | 443 | 8080 | 8443 => {}
            _ => return false,
        }
    }

    // Host check
    let host = match parsed.host() {
        Some(h) => h,
        None => return false,
    };

    match host {
        Host::Ipv4(ip) => is_safe_ipv4(ip),
        Host::Ipv6(ip) => is_safe_ipv6(ip),
        Host::Domain(d) => {
            let d_lower = d.to_lowercase();
            if d_lower == "localhost"
                || d_lower.ends_with(".local")
                || d_lower.ends_with(".localhost")
                || d_lower.ends_with(".internal")
                || d_lower.ends_with(".lan")
            {
                return false;
            }
            // Check if domain resolves as IP string
            if let Ok(ip) = d.parse::<IpAddr>() {
                match ip {
                    IpAddr::V4(v4) => is_safe_ipv4(v4),
                    IpAddr::V6(v6) => is_safe_ipv6(v6),
                }
            } else {
                true
            }
        }
    }
}

fn is_safe_ipv4(ip: Ipv4Addr) -> bool {
    let octets = ip.octets();
    // Loopback (127.0.0.0/8)
    if octets[0] == 127 {
        return false;
    }
    // Private 10.0.0.0/8
    if octets[0] == 10 {
        return false;
    }
    // Private 172.16.0.0/12
    if octets[0] == 172 && (16..=31).contains(&octets[1]) {
        return false;
    }
    // Private 192.168.0.0/16
    if octets[0] == 192 && octets[1] == 168 {
        return false;
    }
    // Link-local / Cloud Metadata (169.254.0.0/16)
    if octets[0] == 169 && octets[1] == 254 {
        return false;
    }
    // Broadcast / Unspecified
    if ip.is_unspecified() || ip.is_broadcast() {
        return false;
    }
    true
}

fn is_safe_ipv6(ip: Ipv6Addr) -> bool {
    if ip.is_loopback() || ip.is_unspecified() {
        return false;
    }
    let segments = ip.segments();
    // Link-local fe80::/10
    if (segments[0] & 0xffc0) == 0xfe80 {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_root_domain() {
        assert_eq!(
            extract_root_domain("http://example.com").unwrap(),
            "example.com"
        );
        assert_eq!(
            extract_root_domain("https://www.example.com").unwrap(),
            "example.com"
        );
        assert_eq!(
            extract_root_domain("http://blog.example.co.uk").unwrap(),
            "example.co.uk"
        );
    }

    #[test]
    fn test_ssrf_protection() {
        assert!(is_safe_egress_url("https://example.com/path"));
        assert!(!is_safe_egress_url("http://127.0.0.1/admin"));
        assert!(!is_safe_egress_url(
            "http://169.254.169.254/latest/meta-data/"
        ));
        assert!(!is_safe_egress_url("http://192.168.1.1"));
        assert!(!is_safe_egress_url("http://localhost:8000"));
        assert!(!is_safe_egress_url("file:///etc/passwd"));
        assert!(!is_safe_egress_url("gopher://127.0.0.1:70/"));
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
        assert_eq!(normalize_url("http://127.0.0.1/"), None);
    }
}
