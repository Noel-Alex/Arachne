//! FMA (Free Music Archive) dataset adapter — the classic ISMIR 2017 corpus.
//!
//! NOT the live freemusicarchive.org site (its API is dead; scraping it is
//! explicitly discouraged). The canonical corpus is static zips on the EPFL
//! mirror `os.unil.cloud.switch.ch/fma`:
//!   - fma_metadata.zip (358 MB): tracks.csv enumerates all 106,574 tracks
//!     offline — no API calls to enumerate, ever.
//!   - fma_large.zip (~100 GB): 30s excerpts for every track.
//!   - fma_full.zip (~943 GB): full-length originals.
//!
//! Flow: fetch + unzip metadata → parse tracks.csv → admit manifest rows with
//! per-track licenses (from the CSV's ('track','license') column) → emit
//! download tasks pointing at the chosen audio zip. The audio zip itself is
//! fetched by a dedicated bulk task (it is ONE file per subset), so this
//! adapter emits one special "archive" task plus per-track manifest rows whose
//! object extraction happens post-download.
//!
//! CSV format (verified from mdeff/fma utils.py): two header rows forming a
//! MultiIndex; row index = track_id. License values are human-readable CC
//! titles like "Creative Commons Attribution-NonCommercial 3.0".

use anyhow::{Context, Result};
use std::io::Read;
use std::sync::Arc;

use arachne_core::db::ArachneRepo;
use arachne_core::nats::NatsManager;
use tracing::info;

use super::{admit, harvest_job_id};

pub const SOURCE_NAME: &str = "fma";

pub const MIRROR_BASE: &str = "https://os.unil.cloud.switch.ch/fma";

#[derive(Debug, Clone)]
pub struct FmaConfig {
    /// Which zip to target: "fma_small" (7.2GB), "fma_medium" (23GB),
    /// "fma_large" (100GB), or "fma_full" (943GB).
    pub subset: String,
    pub max_tracks: Option<u64>,
    /// Keep only redistributable licenses (exclude NC/ND variants).
    pub redistributable_only: bool,
}

impl FmaConfig {
    pub fn new(subset: impl Into<String>) -> Self {
        Self {
            subset: subset.into(),
            max_tracks: None,
            redistributable_only: true,
        }
    }
}

/// Map a human-readable CC title from tracks.csv to our SPDX-ish code.
fn classify_license_title(title: &str) -> Option<String> {
    let t = title.to_ascii_lowercase();
    if !t.contains("creative commons") && !t.contains("public domain") {
        return None;
    }
    if t.contains("public domain") || t.contains("cc0") {
        return Some("cc0-1.0".into());
    }

    let mut code = String::from("by");
    if t.contains("noncommercial") {
        code.push_str("-nc");
    }
    // "NoDerivs"/"No Derivative Works"
    if t.contains("noderivs") || t.contains("no derivative") {
        code.push_str("-nd");
    }
    if t.contains("sharealike") {
        code.push_str("-sa");
    }
    Some(format!("cc-{code}"))
}

fn redistributable(license: &str) -> bool {
    matches!(license, "cc-by" | "cc-by-sa" | "cc0-1.0")
}

/// One parsed row of tracks.csv.
#[derive(Debug, Clone)]
pub struct FmaTrackRow {
    pub track_id: u64,
    pub title: String,
    pub artist: String,
    pub album: String,
    pub duration_secs: Option<f64>,
    pub bitrate_kbps: Option<i32>,
    pub license_raw: String,
}

/// Parse tracks.csv content (the MultiIndex double-header variant).
pub fn parse_tracks_csv(csv_text: &str) -> Result<Vec<FmaTrackRow>> {
    parse_tracks_csv_with_header(csv_text)
}

fn parse_tracks_csv_with_header(csv_text: &str) -> Result<Vec<FmaTrackRow>> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(csv_text.as_bytes());
    let raw: Vec<Vec<String>> = rdr
        .records()
        .map(|r| r.map(|rec| rec.iter().map(str::to_owned).collect()))
        .collect::<Result<_, _>>()?;

    // Locate the track_id row and the group-name row above it.
    let id_row_idx = raw
        .iter()
        .position(|r| r.first().map(String::as_str) == Some("track_id"))
        .context("no track_id header row")?;

    let id_row = &raw[id_row_idx];
    // Group labels live one row above (row 0 = groups, row 1 = sub-names) OR
    // the file may already be flattened; tolerate both.
    let group_row = if id_row_idx >= 1 { &raw[id_row_idx - 1] } else { id_row };

    let idx_of = |group: &str, name: &str| -> Option<usize> {
        for (i, cell) in id_row.iter().enumerate() {
            let sub_matches = cell.eq_ignore_ascii_case(name);
            let group_matches = group_row.get(i).map(|g| g.eq_ignore_ascii_case(group)).unwrap_or(false);
            if sub_matches && (group_matches || group_row.len() <= i) {
                return Some(i);
            }
        }
        None
    };

    let i_title = idx_of("track", "title");
    let i_dur = idx_of("track", "duration");
    let i_bitrate = idx_of("track", "bit_rate");
    let i_license = idx_of("track", "license");
    // Artist name appears both as ('artist','name') and ('album','artist').
    let i_artist = idx_of("artist", "name").or_else(|| idx_of("album", "artist"));
    let i_album = idx_of("album", "title");

    let mut out = Vec::new();
    for r in raw.iter().skip(id_row_idx + 1) {
        if r.is_empty() || r[0].trim().is_empty() {
            continue;
        }
        let track_id: u64 = match r[0].trim().parse() {
            Ok(v) => v,
            Err(_) => continue,
        };
        let get = |i: Option<usize>| -> Option<String> {
            i.and_then(|i| r.get(i)).map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
        };
        out.push(FmaTrackRow {
            track_id,
            title: get(i_title).unwrap_or_default(),
            artist: get(i_artist).unwrap_or_default(),
            album: get(i_album).unwrap_or_default(),
            duration_secs: get(i_dur).and_then(|s| s.parse().ok()),
            bitrate_kbps: get(i_bitrate).and_then(|s| s.parse::<f64>().ok()).map(|b| (b / 1000.0) as i32),
            license_raw: get(i_license).unwrap_or_default(),
        });
    }
    Ok(out)
}

/// Local relative path inside an extracted subset zip: NNN/NNNNNN.mp3.
pub fn subset_path(track_id: u64) -> String {
    let tid = format!("{track_id:06}");
    let dir = tid.get(..3).unwrap_or("000");
    format!("{dir}/{tid}.mp3")
}

/// Full FMA flow: pull metadata zip, parse, admit rows. Audio-zip download is
/// issued as ONE bulk task (see emit_archive_task) — extraction from the local
/// zip happens after that task lands.
pub async fn harvest(
    cfg: &FmaConfig,
    repo: Arc<ArachneRepo>,
    nats: Arc<NatsManager>,
) -> Result<(u64, u64)> {
    let client = reqwest::Client::builder()
        .user_agent(concat!("ArachneBot/", env!("CARGO_PKG_VERSION")))
        .build()?;

    info!("downloading fma_metadata.zip (358MB)…");
    let zip_bytes = client
        .get(format!("{MIRROR_BASE}/fma_metadata.zip"))
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    info!(bytes = zip_bytes.len(), "metadata downloaded");

    let mut archive =
        zip::ZipArchive::new(std::io::Cursor::new(&zip_bytes)).context("bad metadata zip")?;
    let mut tracks_entry = archive
        .by_name("fma_metadata/tracks.csv")
        .context("tracks.csv not in zip")?;
    let mut csv_text = String::new();
    tracks_entry.read_to_string(&mut csv_text)?;
    drop(tracks_entry);

    let rows = parse_tracks_csv(&csv_text)?;
    info!(rows = rows.len(), "tracks.csv parsed");

    let job_id = harvest_job_id();
    let mut admitted_total: u64 = 0;
    let mut existing_total: u64 = 0;
    let mut tasks = Vec::new();

    for row in rows {
        if cfg.max_tracks.is_some_and(|m| admitted_total >= m) {
            break;
        }

        let Some(license) = classify_license_title(&row.license_raw) else {
            continue; // unknown license text: never admit
        };
        if cfg.redistributable_only && !redistributable(&license) {
            continue;
        }

        let record = arachne_core::models::TrackRecord {
            source: SOURCE_NAME.into(),
            source_id: row.track_id.to_string(),
            job_id,
            url: format!(
                "{MIRROR_BASE}/{}.zip!{}",
                cfg.subset,
                subset_path(row.track_id)
            ),
            title: (!row.title.is_empty()).then_some(row.title.clone()),
            artist: (!row.artist.is_empty()).then_some(row.artist.clone()),
            album: (!row.album.is_empty()).then_some(row.album.clone()),
            year: None,
            genre: None,
            license: license.clone(),
            collection: Some(cfg.subset.clone()),
            duration_secs: row.duration_secs,
            bitrate_kbps: row.bitrate_kbps,
            format: Some("mp3".into()),
            sha256: None,
            bytes: None,
            object_path: None,
            status: arachne_core::models::TrackStatus::Pending,
            error: None,
        };

        if admit(&repo, &record).await? {
            admitted_total += 1;
            if admitted_total <= 1 {
                // First new track triggers the single bulk archive task.
                tasks.push(emit_archive_task(job_id, &cfg.subset));
            }
        } else {
            existing_total += 1;
        }
    }

    if !tasks.is_empty() {
        nats.publish_tasks_batch(&tasks).await?;
    }

    Ok((admitted_total, existing_total))
}

/// The single task that downloads the whole audio zip for a subset.
fn emit_archive_task(job_id: uuid::Uuid, subset: &str) -> arachne_core::models::CrawlTask {
    arachne_core::models::CrawlTask {
        url: format!("{MIRROR_BASE}/{subset}.zip"),
        job_id,
        domain: "unil.cloud.switch.ch".into(),
        depth: 0,
        priority: 0,
        kind: arachne_core::models::TaskKind::AudioFile,
        media: Some(arachne_core::models::MediaMeta {
            source_id: format!("{subset}-archive"),
            source: SOURCE_NAME.into(),
            collection: Some(subset.into()),
            license: "cc-by-4.0".into(), // dataset paper/code license
            title: Some(format!("{subset}.zip")),
            artist: None,
            album: None,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_cc_license_titles() {
        assert_eq!(
            classify_license_title("Creative Commons Attribution 4.0 International"),
            Some("cc-by".into())
        );
        assert_eq!(
            classify_license_title("Creative Commons Attribution-NonCommercial-ShareAlike 3.0"),
            Some("cc-by-nc-sa".into())
        );
        assert_eq!(
            classify_license_title("Creative Commons Attribution-NoDerivs"),
            Some("cc-by-nd".into())
        );
        assert_eq!(classify_license_title("FMA-Limited: Download Only"), None);
        assert_eq!(classify_license_title(""), None);
    }

    #[test]
    fn builds_subset_paths() {
        // Real FMA layout inside the zip: {tid[0:3]}/{tid:06}.mp3
        assert_eq!(subset_path(2), "000/000002.mp3");
        assert_eq!(subset_path(155), "000/000155.mp3");
        assert_eq!(subset_path(123456), "123/123456.mp3");
    }

    #[test]
    fn parses_double_header_csv() {
        // Mirrors the real tracks.csv shape: row 0 = group names REPEATED per
        // column (pandas MultiIndex expansion), row 1 = sub-names.
        let csv = ",album,track,artist,track,track,track\n\
                   track_id,title,title,name,duration,bit_rate,license\n\
                   2,Some Album,Song Title,Artist X,180,192000,Creative Commons Attribution 4.0 International\n\
                   3,Other Album,Other Song,Artist Y,240,256000,FMA-Limited: Download Only";
        let rows = parse_tracks_csv(csv).unwrap();
        let first = rows.iter().find(|r| r.track_id == 2).expect("row 2");
        assert_eq!(first.title, "Song Title"); // ('track','title'), not the album title
        assert_eq!(first.artist, "Artist X");
        assert_eq!(first.album, "Some Album");
        assert_eq!(first.duration_secs, Some(180.0));
        assert_eq!(first.bitrate_kbps, Some(192));
        assert_eq!(
            first.license_raw,
            "Creative Commons Attribution 4.0 International"
        );
    }
}
