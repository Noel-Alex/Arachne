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
///
/// Blocks:
/// - Non-HTTP(S) schemes and non-standard ports (only 80/443/8080/8443 allowed).
/// - IPv4: loopback (127.0.0.0/8), private ranges (10/8, 172.16/12, 192.168/16),
///   link-local & cloud metadata (169.254/16), shared address space / CGNAT
///   (100.64/10), benchmarking (198.18/15), documentation TEST-NETs
///   (192.0.2/24, 198.51.100/24, 203.0.113/24), reserved (240.0.0.0/4),
///   "this network" (0.0.0.0/8) and unspecified (0.0.0.0).
/// - IPv6: loopback (::1), unspecified (::), link-local (fe80::/10),
///   unique-local (fc00::/7), IPv4-mapped addresses (::ffff:0:0/96) and
///   NAT64-synthesized targets (64:ff9b::/96) — both re-checked against the
///   IPv4 rules above.
/// - Hostnames that are literally "localhost", ".local", ".localhost",
///   ".internal", or ".lan", including their trailing-dot FQDN spellings.
///
/// REMAINING KNOWN GAP: a DNS hostname that passes this guard may still resolve
/// to a private/loopback/link-local IP at connection time (TOCTOU between this
/// static check and reqwest's own resolver). Roadmap item; not addressed here.
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
            // FQDN canonical form: a single trailing dot is DNS-equivalent
            // to its absence ("localhost." resolves like "localhost"), so
            // normalize BEFORE the comparisons or the equality/suffix guards
            // miss the dotted spelling entirely.
            let d_norm = d_lower.strip_suffix('.').unwrap_or(&d_lower);
            if d_norm == "localhost"
                || d_norm.ends_with(".local")
                || d_norm.ends_with(".localhost")
                || d_norm.ends_with(".internal")
                || d_norm.ends_with(".lan")
            {
                return false;
            }
            // Check if domain resolves as IP string
            if let Ok(ip) = d_norm.parse::<IpAddr>() {
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
    // "This network" (0.0.0.0/8)
    if octets[0] == 0 {
        return false;
    }
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
    // Shared address space (100.64.0.0/10, RFC 6598): carrier CGNAT and
    // Tailscale-style overlays are not publicly routable targets.
    if octets[0] == 100 && (64..=127).contains(&octets[1]) {
        return false;
    }
    // Benchmarking (198.18.0.0/15, RFC 2544): never appears on the public
    // internet; common in lab networks an attacker may reach.
    if octets[0] == 198 && matches!(octets[1], 18 | 19) {
        return false;
    }
    // Documentation TEST-NET ranges (RFC 5737): reserved, must not be hit.
    if octets[0] == 192 && octets[1] == 0 && octets[2] == 2
        || octets[0] == 198 && octets[1] == 51 && octets[2] == 100
        || octets[0] == 203 && octets[1] == 0 && octets[2] == 113
    {
        return false;
    }
    // Reserved (240.0.0.0/4); broadcast 255.255.255.255 falls inside it.
    // (Unspecified 0.0.0.0 was already rejected by the 0.0.0.0/8 check.)
    if octets[0] >= 240 {
        return false;
    }
    true
}

fn is_safe_ipv6(ip: Ipv6Addr) -> bool {
    if ip.is_loopback() || ip.is_unspecified() {
        return false;
    }
    let segments = ip.segments();
    // IPv4-mapped (::ffff:0:0/96): re-check the embedded IPv4 address.
    if let Some(v4) = ip.to_ipv4_mapped() {
        return is_safe_ipv4(v4);
    }
    // NAT64 well-known prefix (64:ff9b::/96, RFC 6052): the last 32 bits are
    // an embedded IPv4 target reached via DNS64 synthesis; apply v4 rules.
    if segments[0] == 0x0064 && segments[1] == 0xff9b {
        let o = [
            (segments[6] >> 8) as u8,
            (segments[6] & 0xff) as u8,
            (segments[7] >> 8) as u8,
            (segments[7] & 0xff) as u8,
        ];
        return is_safe_ipv4(Ipv4Addr::from(o));
    }
    // Unique-local fc00::/7
    if (segments[0] & 0xfe00) == 0xfc00 {
        return false;
    }
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
    fn test_ssrf_ipv4_mapped_ipv6_blocked() {
        // IPv4-mapped IPv6 addresses must be re-checked against the IPv4 rules.
        assert!(!is_safe_egress_url("http://[::ffff:127.0.0.1]/"));
        assert!(!is_safe_egress_url(
            "http://[::ffff:169.254.169.254]/latest/meta-data/"
        ));
        assert!(!is_safe_egress_url("http://[::ffff:10.0.0.1]/"));
        assert!(!is_safe_egress_url("http://[::ffff:192.168.1.1]/"));
        assert!(!is_safe_egress_url("http://[::ffff:0.1.2.3]/"));
    }

    #[test]
    fn test_ssrf_unique_local_ipv6_blocked() {
        assert!(!is_safe_egress_url("http://[fc00::1]/"));
        assert!(!is_safe_egress_url("http://[fd12:3456::1]/"));
    }

    #[test]
    fn test_ssrf_ipv6_link_local_and_loopback_blocked() {
        assert!(!is_safe_egress_url("http://[fe80::1]/"));
        assert!(!is_safe_egress_url("http://[::1]/"));
    }

    #[test]
    fn test_ssrf_public_ipv6_allowed() {
        assert!(is_safe_egress_url("http://[2606:4700::1111]/"));
    }

    #[test]
    fn test_ssrf_this_network_ipv4_blocked() {
        // 0.0.0.0/8 ("this network") beyond the unspecified address itself.
        assert!(!is_safe_egress_url("http://0.1.2.3/"));
    }

    #[test]
    fn test_trailing_dot_fqdn_normalized() {
        // A single trailing dot is DNS-equivalent to its absence; guards must
        // not be bypassed by the FQDN spelling.
        assert!(!is_safe_egress_url("http://localhost./x"));
        assert!(!is_safe_egress_url("http://foo.internal./"));
        assert!(!is_safe_egress_url("http://box.lan.:8080/"));
        assert!(!is_safe_egress_url("http://host.internal./"));
        // Subdomains under the dotted form are caught by the same suffix
        // checks after normalization.
        assert!(!is_safe_egress_url("http://api.box.lan./"));
        // Public hosts with a canonical trailing dot remain crawlable.
        assert!(is_safe_egress_url("https://example.com./"));
    }

    #[test]
    fn test_ssrf_cgnat_blocked() {
        assert!(!is_safe_egress_url("http://100.64.0.1/"));
        assert!(!is_safe_egress_url("http://100.100.100.100/"));
        assert!(!is_safe_egress_url("http://100.127.255.255/"));
        // Public neighbors on both sides of 100.64.0.0/10 stay allowed.
        assert!(is_safe_egress_url("http://100.63.0.1/"));
        assert!(is_safe_egress_url("http://100.128.0.1/"));
    }

    #[test]
    fn test_ssrf_benchmarking_blocked() {
        assert!(!is_safe_egress_url("http://198.18.0.1/"));
        assert!(!is_safe_egress_url("http://198.19.255.255/"));
        assert!(is_safe_egress_url("http://198.20.0.1/"));
        assert!(is_safe_egress_url("http://198.17.0.1/"));
    }

    #[test]
    fn test_ssrf_test_nets_blocked() {
        assert!(!is_safe_egress_url("http://192.0.2.1/"));
        assert!(!is_safe_egress_url("http://198.51.100.7/"));
        assert!(!is_safe_egress_url("http://203.0.113.99/"));
        // Public neighbors of each /24 stay allowed.
        assert!(is_safe_egress_url("http://192.0.3.1/"));
        assert!(is_safe_egress_url("http://198.51.101.1/"));
        assert!(is_safe_egress_url("http://203.0.114.1/"));
    }

    #[test]
    fn test_ssrf_reserved_ipv4_blocked() {
        assert!(!is_safe_egress_url("http://240.0.0.1/"));
        assert!(!is_safe_egress_url("http://250.1.2.3/"));
        assert!(!is_safe_egress_url("http://254.254.254.254/"));
        assert!(!is_safe_egress_url("http://255.255.255.255/"));
    }

    #[test]
    fn test_ssrf_nat64_blocked_and_rechecked_as_v4() {
        // NAT64 well-known prefix embeds an IPv4 target in the last 32 bits.
        assert!(!is_safe_egress_url("http://[64:ff9b::127.0.0.1]/"));
        assert!(!is_safe_egress_url("http://[64:ff9b::169.254.169.254]/"));
        assert!(!is_safe_egress_url("http://[64:ff9b::10.0.0.1]/"));
        // Embedded public IPv4 is allowed (guard recurses, not blanket-bans).
        assert!(is_safe_egress_url("http://[64:ff9b::1.2.3.4]/"));
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
