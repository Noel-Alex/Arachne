//! Internet Archive adapter — cursor-scrape enumeration + per-item file listing.
//!
//! Live-verified 2026-08 against production APIs. Key behaviors baked in:
//! - Enumeration via /services/search/v1/scrape (opaque cursor); advancedsearch
//!   hard-fails past 10k results with HTTP-200 {"error":"[DEEP_PAGING]..."}.
//! - File listing via /metadata/{identifier}; missing items return `{}`.
//! - Download URLs require BYTE-EXACT percent-encoding of files[].name.
//! - Etiquette (bots.html): descriptive UA mandatory, ~1s spacing, honor
//!   throttling noise (not always clean 429s).
//!
//! LEGAL defaults: `netlabels` collection filtered to redistributable CC
//! licenses. georgeblood/Great-78 and etree are research/private-study ONLY
//! (IA rights statements + MMA + 2023 litigation) — never enable bulk harvest
//! of those without written permission from info@archive.org.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;

use arachne_core::db::ArachneRepo;
use arachne_core::nats::NatsManager;
use tracing::{info, warn};

use super::{admit, build_task_and_record, harvest_job_id};

pub const SOURCE_NAME: &str = "archive-org";

#[derive(Debug, Clone)]
pub struct ArchiveOrgConfig {
    /// Contact address baked into the User-Agent (required by bots.html).
    pub contact: String,
    /// Collection identifier to enumerate. Default: netlabels (CC-licensed).
    pub collection: String,
    /// Only admit items whose licenseurl resolves to cc-by/cc-by-sa/pd.
    pub redistributable_only: bool,
    /// Scrape page size (100..=10000).
    pub count: u32,
    pub max_items: Option<u64>,
    /// Inter-request delay honoring the sanctioned ~1s envelope.
    pub request_delay_ms: u64,
}

impl ArchiveOrgConfig {
    pub fn new(contact: impl Into<String>) -> Self {
        Self {
            contact: contact.into(),
            collection: "netlabels".into(),
            redistributable_only: true,
            count: 1000,
            max_items: None,
            request_delay_ms: 1000,
        }
    }

    fn user_agent(&self) -> String {
        format!(
            "ArachneBot/{} (+https://github.com/Noel-Alex/Arachne; contact: {})",
            env!("CARGO_PKG_VERSION"),
            self.contact
        )
    }
}

#[derive(Deserialize)]
struct ScrapeResponse {
    #[serde(default)]
    items: Vec<ScrapeItem>,
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Deserialize)]
struct ScrapeItem {
    identifier: String,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    licenseurl: Option<String>,
    #[serde(default)]
    rights: Option<String>,
}

#[derive(Deserialize)]
struct MetadataResponse {
    // Item-level metadata exists but the scrape page already carries
    // licenseurl/rights; kept out of the model until we need item fallback.
    #[serde(default)]
    files: Vec<IaFile>,
}

#[derive(Deserialize)]
struct IaFile {
    name: String,
    #[serde(default)]
    format: String,
}

/// Map a licenseurl/rights pair to our SPDX-ish code. `None` = unknown.
fn classify_license(licenseurl: Option<&str>, rights: Option<&str>) -> Option<String> {
    if let Some(u) = licenseurl {
        let l = u.to_ascii_lowercase();
        if l.contains("/licenses/") {
            let seg = l.split("/licenses/").nth(1)?;
            let code = seg.split('/').next()?;
            if code.starts_with("by") {
                return Some(format!("cc-{code}"));
            }
        }
        if l.contains("publicdomain/mark") {
            return Some("pd-mark".into());
        }
        if l.contains("publicdomain/zero") {
            return Some("cc0-1.0".into());
        }
    }
    // No licenseurl does NOT mean public domain; only accept an explicit PD-US rightstatement.
    match rights.map(str::trim) {
        Some(r)
            if r.eq_ignore_ascii_case("public domain in the usa.")
                || r.eq_ignore_ascii_case("public domain in the usa") =>
        {
            Some("pd-us".into())
        }
        _ => None,
    }
}

/// Is this license OK to redistribute downstream to Sivana?
fn redistributable(license: &str) -> bool {
    matches!(
        license,
        "cc-by" | "cc-by-sa" | "cc0-1.0" | "pd-mark" | "pd-us"
    )
}

/// Which derivative we want per track-stem, in preference order.
fn format_preference(fmt: &str) -> Option<u8> {
    let f = fmt.to_ascii_lowercase();
    if f.contains("vbr mp3") {
        Some(0) // small derivative, ideal for fingerprinting
    } else if f == "flac" || f.contains("24bit flac") {
        Some(1) // lossless master
    } else if f.contains("ogg vorbis") {
        Some(2)
    } else {
        None
    }
}

/// Pick the best downloadable audio file name per stem. Groups by filename
/// stem so a Flac master and its VBR MP3 derivative don't both enter the
/// manifest. Returns IA `files[].name` values, sorted; callers build the
/// byte-exact download URL from the item identifier + encoded name.
fn choose_audio_files(files: &[IaFile]) -> Vec<String> {
    // stem -> (preference, index)
    let mut best: HashMap<String, (u8, usize)> = HashMap::new();
    for (i, f) in files.iter().enumerate() {
        if let Some(pref) = format_preference(&f.format) {
            let stem = f
                .name
                .rsplit_once('.')
                .map(|(s, _)| s.to_string())
                .unwrap_or_else(|| f.name.clone());
            match best.get(&stem) {
                Some((existing_pref, _)) if *existing_pref <= pref => {}
                _ => {
                    best.insert(stem, (pref, i));
                }
            }
        }
    }

    let mut out: Vec<String> = best
        .into_values()
        .map(|(_, i)| files[i].name.clone())
        .collect();
    out.sort();
    out
}

/// Percent-encode an IA path segment byte-exactly (spaces %20, UTF-8 multibyte).
fn encode_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Walk one collection via the cursor scrape API and enqueue downloads.
pub async fn harvest(
    cfg: &ArchiveOrgConfig,
    repo: Arc<ArachneRepo>,
    nats: Arc<NatsManager>,
) -> Result<(u64, u64)> {
    let job_id = harvest_job_id();
    let client = reqwest::Client::builder()
        .user_agent(cfg.user_agent())
        .build()?;

    let mut cursor: Option<String> = None;
    let mut admitted_total: u64 = 0;
    let mut existing_total: u64 = 0;
    let mut items_seen: u64 = 0;
    let mut batch: Vec<arachne_core::models::CrawlTask> = Vec::new();

    loop {
        // Stop when the item cap is set and reached.
        if cfg.max_items.is_some_and(|max| items_seen >= max) {
            break;
        }

        let mut url = reqwest::Url::parse("https://archive.org/services/search/v1/scrape")?;
        url.query_pairs_mut()
            .append_pair("q", &format!("collection:({})", cfg.collection))
            .append_pair("fields", "identifier,title,licenseurl,rights")
            .append_pair("count", &cfg.count.to_string())
            .append_pair("sorts", "identifier asc");
        if let Some(c) = &cursor {
            url.query_pairs_mut().append_pair("cursor", c);
        }

        let resp = client
            .get(url)
            .send()
            .await
            .context("archive.org scrape request failed")?;
        if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            // Honor Retry-After (seconds form); fall back to 30s when absent
            // or unparseable.
            let retry_after = resp
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.trim().parse::<u64>().ok())
                .unwrap_or(30);
            warn!("archive.org 429 — backing off {retry_after}s");
            tokio::time::sleep(std::time::Duration::from_secs(retry_after)).await;
            continue;
        }
        let page: ScrapeResponse = resp.json().await.context("archive.org scrape bad json")?;

        let n = page.items.len();
        if n == 0 {
            break; // cursor exhausted
        }

        for item in page.items {
            items_seen += 1;
            let license = match classify_license(item.licenseurl.as_deref(), item.rights.as_deref())
            {
                Some(l) => l,
                None => {
                    if cfg.redistributable_only {
                        continue; // unlicensed: skip entirely under safe defaults
                    }
                    "unknown".to_string()
                }
            };
            if cfg.redistributable_only && !redistributable(&license) {
                continue;
            }

            // Per-item file listing (cache-friendly; IA asks we cache these).
            let meta_url = format!("https://archive.org/metadata/{}", item.identifier);
            let meta: MetadataResponse = match client.get(&meta_url).send().await {
                Ok(r) => match r.json().await {
                    Ok(m) => m,
                    Err(e) => {
                        warn!("metadata parse failed for {}: {e}", item.identifier);
                        continue;
                    }
                },
                Err(e) => {
                    warn!("metadata fetch failed for {}: {e}", item.identifier);
                    continue;
                }
            };

            // Empty object = transiently missing item; just move on.
            if meta.files.is_empty() {
                continue;
            }

            for f in choose_audio_files(&meta.files) {
                // Byte-exact URL construction: /download/{id}/{encoded-name}
                let dl = format!(
                    "https://archive.org/download/{}/{}",
                    encode_segment(&item.identifier),
                    encode_segment(&f)
                );
                let title = item
                    .title
                    .clone()
                    .or_else(|| f.rsplit_once('.').map(|(s, _)| s.to_string()));
                let origin = super::OriginLinks {
                    // The item's /details/ page is IA's canonical human URL.
                    page_url: Some(format!("https://archive.org/details/{}", item.identifier)),
                    license_url: item.licenseurl.clone(),
                };
                let Some((task, record)) = build_task_and_record(
                    job_id,
                    SOURCE_NAME,
                    // source_id uniquely identifies the ITEM+FILE.
                    format!("{}|{}", item.identifier, f),
                    dl,
                    license.clone(),
                    origin,
                    Some(cfg.collection.clone()),
                    title,
                    None, // artist lives in item metadata; leave to probe/tags
                    None,
                ) else {
                    continue;
                };

                if admit(&repo, &record).await? {
                    batch.push(task);
                    admitted_total += 1;
                } else {
                    existing_total += 1;
                }
            }

            tokio::time::sleep(std::time::Duration::from_millis(cfg.request_delay_ms)).await;
        }

        if !batch.is_empty() {
            let tasks = std::mem::take(&mut batch);
            nats.publish_tasks_batch(&tasks).await?;
        }

        info!(
            items = items_seen,
            admitted = admitted_total,
            existing = existing_total,
            "archive.org page ingested"
        );

        cursor = page.cursor;
        if cursor.is_none() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(cfg.request_delay_ms)).await;
    }

    Ok((admitted_total, existing_total))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_cc_urls() {
        assert_eq!(
            classify_license(
                Some("http://creativecommons.org/licenses/by-nc-sa/4.0/"),
                None
            ),
            Some("cc-by-nc-sa".into())
        );
        assert_eq!(
            classify_license(
                Some("http://creativecommons.org/publicdomain/mark/1.0/"),
                None
            ),
            Some("pd-mark".into())
        );
        assert_eq!(classify_license(None, None), None);
    }

    #[test]
    fn absence_of_license_is_not_pd() {
        assert_eq!(
            classify_license(None, Some("Public Domain in the USA.")),
            Some("pd-us".into())
        );
        assert_eq!(classify_license(None, Some("In Copyright")), None);
    }

    #[test]
    fn encodes_hostile_filenames_byte_exactly() {
        assert_eq!(
            encode_segment("TAMBURAŠKI ZBOR \"ŠOKADIJA\".mp3"),
            "TAMBURA%C5%A0KI%20ZBOR%20%22%C5%A0OKADIJA%22.mp3"
        );
    }

    #[test]
    fn prefers_mp3_derivative_over_flac_master_per_stem() {
        let files = vec![
            IaFile {
                name: "song.flac".into(),
                format: "24bit Flac".into(),
            },
            IaFile {
                name: "song.mp3".into(),
                format: "VBR MP3".into(),
            },
            IaFile {
                name: "song.ogg".into(),
                format: "Ogg Vorbis".into(),
            },
        ];
        let chosen = choose_audio_files(&files);
        assert_eq!(chosen.len(), 1);
        assert_eq!(chosen[0], "song.mp3");
    }
}
