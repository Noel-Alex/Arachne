//! Filesystem portability helpers.
//!
//! Arachne's dev host is Windows and its store fanout is user/URL derived,
//! so every path segment we write must survive: MAX_PATH limits, reserved
//! device names (CON, NUL, COM1...), trailing dots/spaces, and case-folding.

/// Windows reserved device names, case-insensitive, with or without extension.
const RESERVED_NAMES: &[&str] = &[
    "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
    "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

/// Longest final path component we will ever write (leaves headroom under the
/// 260-char legacy MAX_PATH even without long-path awareness).
pub const MAX_SEGMENT_LEN: usize = 200;

/// Sanitize a single path segment for cross-platform safe use.
///
/// - replaces path separators and control chars with `_`
/// - neutralizes Windows reserved names (`con` -> `_con`)
/// - strips trailing dots/spaces (illegal on Windows)
/// - truncates to [`MAX_SEGMENT_LEN`] chars
/// - collapses to `_` when empty
pub fn sanitize_path_segment(input: &str) -> String {
    let mut out: String = input
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            c if (c as u32) < 0x20 || c == '\x7f' => '_',
            c => c,
        })
        .collect();

    // Truncate on char boundaries before suffix work.
    if out.chars().count() > MAX_SEGMENT_LEN {
        out = out.chars().take(MAX_SEGMENT_LEN).collect();
    }

    // Reserved names: "CON", "CON.txt" both illegal -> prefix underscore.
    let stem = out.split('.').next().unwrap_or("");
    if RESERVED_NAMES.iter().any(|r| r.eq_ignore_ascii_case(stem)) {
        out.insert(0, '_');
    }

    // Windows strips trailing dots/spaces; other processes may not. Normalize.
    while out.ends_with('.') || out.ends_with(' ') {
        out.pop();
    }

    if out.is_empty() {
        out.push('_');
    }
    out
}

/// Retry a blocking rename a few times with backoff.
///
/// On Windows, antivirus (Defender) briefly holds handles on freshly written
/// files; the resulting ERROR_SHARING_VIOLATION surfaces as an OS error 32
/// here. Short retries absorb it.
pub async fn rename_with_retry(from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
    const DELAYS_MS: [u64; 5] = [10, 25, 50, 100, 250];
    let mut last = match tokio::fs::rename(from, to).await {
        Ok(()) => return Ok(()),
        Err(e) => e,
    };
    for ms in DELAYS_MS {
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
        match tokio::fs::rename(from, to).await {
            Ok(()) => return Ok(()),
            Err(e) => last = e,
        }
    }
    Err(last)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitizes_separators_and_reserved_names() {
        assert_eq!(sanitize_path_segment("a/b\\c"), "a_b_c");
        assert_eq!(sanitize_path_segment("con"), "_con");
        assert_eq!(sanitize_path_segment("NUL.mp3"), "_NUL.mp3");
        assert_eq!(sanitize_path_segment("trail... "), "trail");
        assert_eq!(sanitize_path_segment(""), "_");
        assert_eq!(sanitize_path_segment("../evil"), ".._evil"); // dots kept mid-segment...
        // ...but never escapes because separators are gone and prefix dots are inert inside one segment
        let long = "x".repeat(500);
        assert_eq!(sanitize_path_segment(&long).len(), MAX_SEGMENT_LEN);
    }
}
