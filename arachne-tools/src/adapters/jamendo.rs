//! Jamendo adapter — CC-licensed music catalog (~500k tracks).
//!
//! API: GET https://api.jamendo.com/v3.0/tracks/?client_id=... (verified 2026-08)
//! Key behaviors baked in from the live-verified spec:
//! - `limit` hard-caps at 200 per page; `order=id_asc` for deterministic walks.
//! - Default response only returns album tracks; pass `type=single+albumtrack`
//!   to enumerate everything.
//! - `audiodlformat=flac|mp32` controls the download link format.
//! - `audiodownload_allowed=false` tracks have empty download URLs → skip.
//! - Over-quota responses still return HTTP-success with a warning header
//!   field + empty results — treated as backoff signal, never end-of-catalog.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::sync::Arc;

use arachne_core::db::ArachneRepo;
use arachne_core::nats::NatsManager;
use tracing::{info, warn};

use super::{admit, build_task_and_record, harvest_job_id};

pub const SOURCE_NAME: &str = "jamendo";

#[derive(Debug, Clone)]
pub struct JamendoConfig {
    pub client_id: String,
    /// Download format: "flac" or "mp32" (VBR ~190-320 kbps).
    pub dl_format: String,
    pub page_size: u32,
    pub max_tracks: Option<u64>,
    /// Milliseconds between metadata API pages (35k req/month free tier ≈ 13ms
    /// sustained; we stay far below at ~2/s).
    pub page_delay_ms: u64,
    /// Contact address baked into the User-Agent (charter §UA).
    pub contact: Option<String>,
}

impl JamendoConfig {
    pub fn new(client_id: impl Into<String>) -> Self {
        Self {
            client_id: client_id.into(),
            dl_format: "mp32".into(),
            page_size: 200,
            max_tracks: None,
            page_delay_ms: 500,
            contact: None,
        }
    }
}

/// Charter-mandated UA: `ArachneBot/{version} (+{repo_url}; contact={c})`,
/// repo URL alone when no contact is configured.
fn user_agent(contact: Option<&str>) -> String {
    match contact {
        Some(c) => format!(
            "ArachneBot/{} (+https://github.com/Noel-Alex/Arachne; contact={c})",
            env!("CARGO_PKG_VERSION")
        ),
        None => format!(
            "ArachneBot/{} (+https://github.com/Noel-Alex/Arachne)",
            env!("CARGO_PKG_VERSION")
        ),
    }
}

#[derive(Deserialize)]
struct Envelope {
    #[serde(rename = "headers")]
    _headers: Headers,
    results: Vec<Track>,
}

#[derive(Deserialize)]
struct Headers {
    status: String,
    #[serde(default)]
    warnings: String,
    #[serde(default)]
    error_message: String,
}

#[derive(Deserialize)]
struct Track {
    id: String,
    name: String,
    #[serde(default)]
    artist_name: String,
    #[serde(default)]
    album_name: String,
    license_ccurl: String,
    #[serde(default)]
    audiodownload: String,
    #[serde(default, rename = "audiodownload_allowed")]
    download_allowed: bool,
    #[serde(default)]
    shareurl: String,
}

/// Parse "cc-by-nc" style code from a CC URL (segment after /licenses/).
fn license_code(ccurl: &str) -> Option<String> {
    let seg = ccurl
        .to_ascii_lowercase()
        .split("/licenses/")
        .nth(1)?
        .split('/')
        .next()?
        .to_string();
    if seg.is_empty() || !seg.starts_with("by") {
        return None;
    }
    Some(format!("cc-{seg}"))
}

fn download_url(track_id: &str, fmt: &str) -> String {
    format!("https://prod-1.storage.jamendo.com/download/track/{track_id}/{fmt}/")
}

/// Seconds to wait before the `retry`-th re-attempt of an over-quota page.
/// Fixed minute-ladder because Jamendo sends no reliable Retry-After header.
fn quota_backoff_secs(retry: u32) -> u64 {
    u64::from(retry) * 60
}

/// Walk the full catalog (id ascending), admitting pending manifest rows and
/// publishing AudioFile tasks. Returns (admitted, skipped_existing).
pub async fn harvest(
    cfg: &JamendoConfig,
    repo: Arc<ArachneRepo>,
    nats: Arc<NatsManager>,
) -> Result<(u64, u64)> {
    let job_id = harvest_job_id();
    let client = reqwest::Client::builder()
        .user_agent(user_agent(cfg.contact.as_deref()))
        .build()?;

    let mut offset: u32 = 0;
    let mut admitted_total: u64 = 0;
    let mut existing_total: u64 = 0;
    // Over-quota / transient-empty pages get retried with backoff before we
    // believe the walk is done.
    let mut consecutive_empty = 0u32;
    // Successive over-quota pages (reset by any page WITH results); a quota
    // wall mid-catalog is a pause, not end-of-catalog, so ride it out.
    let mut quota_retries = 0u32;

    loop {
        // --limit bounds catalog walk, not just new admits: count every
        // downloadable/licensed track processed (new or already present),
        // or a re-run against a mostly-known catalog would walk all ~500k.
        let seen = admitted_total + existing_total;
        if cfg.max_tracks.is_some_and(|max| seen >= max) {
            break;
        }

        let resp = client
            .get("https://api.jamendo.com/v3.0/tracks/")
            .query(&[
                ("client_id", cfg.client_id.as_str()),
                ("format", "json"),
                ("limit", &cfg.page_size.to_string()),
                ("offset", &offset.to_string()),
                ("order", "id_asc"),
                // Default returns ONLY albumtracks; both kinds for completeness.
                ("type", "single+albumtrack"),
                ("audiodlformat", cfg.dl_format.as_str()),
                ("include", "licenses"),
            ])
            .send()
            .await
            .context("jamendo api request failed")?;

        let env: Envelope = resp.json().await.context("jamendo api bad json")?;

        // Non-success is a real API problem: bail loudly.
        if env._headers.status != "success" {
            anyhow::bail!(
                "jamendo api problem: status={} warnings={} err={}",
                env._headers.status,
                env._headers.warnings,
                env._headers.error_message
            );
        }

        // Quota exhaustion masquerades as an empty success with a warning.
        // Per the module doc this is a backoff signal, never end-of-catalog:
        // retry in place (offset untouched) on an escalating minute-ladder,
        // and only give up once it persists across 5 attempts. Fixed ladder
        // rather than a Retry-After hint because Jamendo sends none reliably.
        if !env._headers.warnings.is_empty() && env.results.is_empty() {
            if quota_retries >= 5 {
                anyhow::bail!(
                    "jamendo over quota after {quota_retries} backoffs at offset {offset}"
                );
            }
            quota_retries += 1;
            let secs = quota_backoff_secs(quota_retries);
            warn!(
                offset,
                attempt = quota_retries,
                backoff_secs = secs,
                "jamendo over quota, backing off"
            );
            tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
            continue;
        }

        if env.results.is_empty() {
            consecutive_empty += 1;
            if consecutive_empty >= 3 {
                break; // genuinely walked off the end across retries
            }
            warn!("empty page at offset {offset}, retrying");
            tokio::time::sleep(std::time::Duration::from_millis(cfg.page_delay_ms * 4)).await;
            continue;
        }
        consecutive_empty = 0;
        quota_retries = 0;

        let page_len = env.results.len();
        let mut tasks = Vec::with_capacity(page_len);
        for t in env.results {
            if !t.download_allowed || t.audiodownload.is_empty() {
                continue; // no downloadable audio exists for this track
            }
            let url = download_url(&t.id, &cfg.dl_format);
            let Some(license) = license_code(&t.license_ccurl) else {
                continue; // unknown/unparseable license: never admit
            };

            let collection = (!t.album_name.is_empty()).then(|| t.album_name.clone());
            let title = (!t.name.trim().is_empty()).then(|| t.name.clone());
            let artist = (!t.artist_name.trim().is_empty()).then(|| t.artist_name.clone());
            let origin = super::OriginLinks {
                // Jamendo's shareurl is the canonical human-facing track page.
                page_url: (!t.shareurl.is_empty()).then_some(t.shareurl.clone()),
                license_url: (!t.license_ccurl.is_empty()).then_some(t.license_ccurl.clone()),
            };

            if let Some((task, record)) = build_task_and_record(
                job_id,
                SOURCE_NAME,
                t.id,
                url,
                license,
                origin,
                collection,
                title,
                artist,
                None,
            ) {
                if admit(&repo, &record).await? {
                    tasks.push(task);
                    admitted_total += 1;
                } else {
                    existing_total += 1;
                }
            }
        }

        if !tasks.is_empty() {
            nats.publish_tasks_batch(&tasks).await?;
        }

        info!(
            offset,
            page = page_len,
            admitted = admitted_total,
            existing = existing_total,
            "jamendo page ingested"
        );

        offset += page_len as u32;
        tokio::time::sleep(std::time::Duration::from_millis(cfg.page_delay_ms)).await;
    }

    Ok((admitted_total, existing_total))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_license_codes_from_ccurls() {
        assert_eq!(
            license_code("http://creativecommons.org/licenses/by-nc-sa/3.0/"),
            Some("cc-by-nc-sa".into())
        );
        assert_eq!(
            license_code("https://creativecommons.org/licenses/by/4.0"),
            Some("cc-by".into())
        );
        assert_eq!(license_code(""), None);
        assert_eq!(license_code("http://example.com/somethingelse"), None);
    }

    #[test]
    fn builds_storage_download_urls() {
        assert_eq!(
            download_url("1848357", "flac"),
            "https://prod-1.storage.jamendo.com/download/track/1848357/flac/"
        );
    }

    #[test]
    fn quota_backoff_ladder_is_minutes() {
        assert_eq!(quota_backoff_secs(0), 0); // unreachable: counter pre-incremented
        assert_eq!(quota_backoff_secs(1), 60);
        assert_eq!(quota_backoff_secs(5), 300); // final attempt before bail
    }
}
