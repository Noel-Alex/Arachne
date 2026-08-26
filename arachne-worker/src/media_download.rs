//! Streaming audio download path for the worker.
//!
//! Streams response bodies straight to `.part` files with a running SHA-256
//! (never buffering whole files in memory), resumes partial downloads with
//! Range requests whose 206 replies are validated against Content-Range,
//! sniffs magic bytes to catch lying extensions, probes with lofty, applies
//! quality gates, and commits content-addressed via MediaStore. Finished
//! staging files are promoted to a private `.done` name while the staging
//! lock is still held, so classification cannot race a redelivered twin.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use arachne_core::config::ArachneConfig;
use arachne_core::fsutil::rename_with_retry;
use arachne_core::media::store::{MediaObject, StoredMedia};
use arachne_core::media::{AudioQuality, MediaStore, probe_audio};
use arachne_core::models::{CrawlStatus, CrawlTask, TaskKind};
use bytes::Bytes;
use futures::StreamExt;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, Semaphore};
use tracing::{info, warn};

/// Size cap for non-audio media kinds. `max_audio_size_bytes` is tuned for
/// audio (~500MB) and would forbid virtually every video or large document;
/// until a dedicated config key lands (deferred to avoid a cross-crate edit),
/// these kinds cap at 8GiB. The free-space floor still bounds disk pressure.
const OTHER_MEDIA_MAX_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// Per-host download concurrency caps (home-IP politeness for media hosts).
#[derive(Default)]
pub struct HostLimits {
    semaphores: dashmap::DashMap<String, Arc<Semaphore>>,
    default_permits: usize,
}

impl HostLimits {
    pub fn new(default_permits: usize) -> Self {
        Self {
            semaphores: dashmap::DashMap::new(),
            default_permits,
        }
    }

    async fn acquire(&self, host: &str) -> tokio::sync::OwnedSemaphorePermit {
        let sem = self
            .semaphores
            .entry(host.to_string())
            .or_insert_with(|| Arc::new(Semaphore::new(self.default_permits)))
            .clone();
        sem.acquire_owned().await.expect("semaphore never closed")
    }
}

/// Everything the media path needs, shared across concurrent downloads.
pub struct MediaContext {
    pub config: ArachneConfig,
    pub store: MediaStore,
    pub host_limits: HostLimits,
    /// Total committed bytes; used against max_total_bytes.
    total_bytes: Mutex<u64>,
}

impl MediaContext {
    pub fn new(config: ArachneConfig) -> Result<Self> {
        let ctx = Self {
            store: MediaStore::local(&config.media.store_dir)?,
            host_limits: HostLimits::new(config.media.per_host_concurrency),
            config,
            total_bytes: Mutex::new(0),
        };
        ctx.sweep_stale_parts();
        Ok(ctx)
    }

    /// Best-effort removal of abandoned staging files.
    ///
    /// Age-based on purpose: a concurrent worker legitimately owns young
    /// `.part`/`.done` files (its own attempt or a locked primary), so only
    /// files untouched for 48h — far beyond any download — can be assumed
    /// orphaned by a crash. Runs once at startup; never fatal.
    fn sweep_stale_parts(&self) {
        const MAX_AGE: std::time::Duration = std::time::Duration::from_secs(48 * 60 * 60);
        let parts_dir = PathBuf::from(&self.config.media.store_dir).join("parts");
        tokio::task::spawn_blocking(move || {
            let entries = match std::fs::read_dir(&parts_dir) {
                Ok(e) => e,
                // Missing dir just means nothing was ever staged.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
                Err(e) => {
                    tracing::debug!("stale .part sweep: read_dir {}: {e}", parts_dir.display());
                    return;
                }
            };
            let cutoff = std::time::SystemTime::now() - MAX_AGE;
            for entry in entries.flatten() {
                // Unstatable entries are skipped: deleting blind is worse
                // than leaving one more orphan for the next sweep.
                let stale = entry
                    .metadata()
                    .and_then(|m| m.modified())
                    .is_ok_and(|m| m < cutoff);
                if !stale {
                    continue;
                }
                if let Err(e) = std::fs::remove_file(entry.path()) {
                    tracing::debug!("stale .part sweep: remove {}: {e}", entry.path().display());
                }
            }
        });
    }

    async fn disk_budget_ok(&self) -> bool {
        // Total-bytes cap (0 = unlimited).
        if self.config.media.max_total_bytes > 0
            && *self.total_bytes.lock().await >= self.config.media.max_total_bytes
        {
            return false;
        }

        // Free-space floor: pause harvesting before we starve the OS/other
        // processes of disk. Checked per-download; a single download can
        // still overshoot up to its per-kind size cap.
        match fs4::available_space(&self.config.media.store_dir) {
            Ok(free) if free < self.config.media.min_free_bytes => false,
            Ok(_) => true,
            Err(e) => {
                // Fail open: quota checks are advisory vs. losing all harvests
                // on a transient stat error, but surface it loudly.
                tracing::debug!("free-space check failed: {e}");
                true
            }
        }
    }
}

#[derive(Debug)]
pub struct DownloadOutcome {
    pub status: CrawlStatus,
    pub stored: Option<StoredMedia>,
    pub probe: Option<arachne_core::media::ProbeResult>,
    /// Sniffed+normalized container extension ("mp3", "flac", ...).
    pub format: Option<String>,
    pub bytes: u64,
    pub error: Option<String>,
}

/// Entry point: download + verify + probe an AudioFile task.
pub async fn harvest_media(
    ctx: &Arc<MediaContext>,
    client: &reqwest::Client,
    task: &CrawlTask,
) -> DownloadOutcome {
    match run(ctx, client, task).await {
        Ok(o) => o,
        Err(e) => DownloadOutcome {
            status: CrawlStatus::FetchError(e.to_string()),
            stored: None,
            probe: None,
            format: None,
            bytes: 0,
            error: Some(e.to_string()),
        },
    }
}

async fn run(
    ctx: &Arc<MediaContext>,
    client: &reqwest::Client,
    task: &CrawlTask,
) -> Result<DownloadOutcome> {
    let start = Instant::now();
    let url = reqwest::Url::parse(&task.url)?;
    let host = url.host_str().unwrap_or("unknown").to_string();

    if !ctx.disk_budget_ok().await {
        return Ok(DownloadOutcome {
            status: CrawlStatus::FetchError("storage quota reached".into()),
            stored: None,
            probe: None,
            format: None,
            bytes: 0,
            error: Some("max_total_bytes reached".into()),
        });
    }

    // Staging file keyed by URL hash so crash restarts can RESUME. Redelivery
    // while an original is still downloading must not stomp it, so we take an
    // exclusive OS file lock; a contended lock means "someone else is mid-
    // flight" and we fall back to a private nonce-suffixed file (no resume,
    // but never shared bytes). Cross-attempt dedup happens at the store.
    let staging_key: String = {
        let mut h = Sha256::new();
        h.update(task.url.as_bytes());
        hex::encode(h.finalize())[..24].to_string()
    };
    let attempt_nonce: u32 = std::process::id() as u32 ^ uuid::Uuid::new_v4().as_u128() as u32;
    let primary_part = PathBuf::from(&ctx.config.media.store_dir)
        .join("parts")
        .join(format!("{staging_key}.part"));
    let fallback_part = PathBuf::from(&ctx.config.media.store_dir)
        .join("parts")
        .join(format!("{staging_key}-{attempt_nonce:08x}.part"));

    // Try to own the primary .part exclusively. On success we WRITE THROUGH
    // this same handle (reopening would collide with our own lock on
    // Windows); on contention we fall back to a private nonce-suffixed file
    // (no resume, but never shared bytes). Cross-attempt dedup happens at the
    // content-addressed store.
    use fs4::tokio::AsyncFileExt as _;
    tokio::fs::create_dir_all(primary_part.parent().unwrap()).await?;
    let part_lock_holder = tokio::fs::OpenOptions::new()
        .create(true)
        .truncate(false) // never clobber a resumable prefix
        .write(true)
        .read(true)
        .open(&primary_part)
        .await?;

    enum Staging {
        /// Locked primary handle — resume + exclusive ownership.
        Owned {
            path: PathBuf,
            file: tokio::fs::File,
            resumed_len: u64,
        },
        /// Private fallback path (fresh download).
        Fallback { path: PathBuf },
    }

    let staging = match part_lock_holder.try_lock_exclusive() {
        Ok(()) => {
            // Lock methods take &self, so ownership stays with our binding.
            match part_lock_holder.metadata().await {
                Ok(m) => Staging::Owned {
                    path: primary_part,
                    file: part_lock_holder,
                    resumed_len: m.len(),
                },
                Err(e) => {
                    // Never resume blind: an unknown length means offset 0,
                    // and appending a fresh body onto a non-empty prefix we
                    // couldn't stat commits mixed content whose hash still
                    // verifies. A private fresh file is always safe.
                    tracing::warn!(
                        url = %task.url,
                        error = %e,
                        "staging metadata failed; falling back to private .part"
                    );
                    Staging::Fallback {
                        path: fallback_part,
                    }
                }
            }
        }
        Err(_) => {
            tracing::debug!(
                url = %task.url,
                "staging file locked by in-flight download; using private .part"
            );
            Staging::Fallback {
                path: fallback_part,
            }
        }
    };

    let mut part_path = match &staging {
        Staging::Owned { path, .. } => path.clone(),
        Staging::Fallback { path } => path.clone(),
    };
    // Resume offset comes from the staging state, NOT from re-statting the
    // file (which races with the other attempt's writer).
    let attempt_resume = match &staging {
        Staging::Owned { resumed_len, .. } => *resumed_len,
        Staging::Fallback { .. } => 0,
    };
    // The write handle for this attempt: the locked one itself, or a fresh
    // file on the fallback path.
    let mut staging_file = match staging {
        Staging::Owned {
            mut file,
            resumed_len,
            ..
        } => {
            if resumed_len > 0 {
                use tokio::io::AsyncSeekExt;
                file.seek(std::io::SeekFrom::End(0)).await?;
            }
            Some(file)
        }
        Staging::Fallback { .. } => None,
    };

    let _permit = ctx.host_limits.acquire(&host).await;

    let mut request = client
        .get(url.clone())
        .header(reqwest::header::ACCEPT_ENCODING, "identity");
    if attempt_resume > 0 {
        request = request.header(reqwest::header::RANGE, format!("bytes={attempt_resume}-"));
    }

    let resp = match request.send().await {
        Ok(r) => r,
        Err(e) => {
            cleanup(&part_path).await;
            return Err(anyhow::anyhow!("request failed: {e}"));
        }
    };

    let status = resp.status();
    if !(status.is_success() || status == reqwest::StatusCode::PARTIAL_CONTENT) {
        cleanup(&part_path).await;
        return Ok(DownloadOutcome {
            status: CrawlStatus::HttpError(status.as_u16()),
            stored: None,
            probe: None,
            format: None,
            bytes: 0,
            error: None,
        });
    }

    // A 206 that PROVABLY continues our .part prefix resumes it; a 200
    // (server ignored Range) or a misaligned 206 restarts it.
    //
    // Why Content-Range validation rather than If-Range: If-Range needs an
    // ETag/Last-Modified persisted across download attempts, which we do not
    // keep — the .part is the only cross-attempt state. Instead, a 206 must
    // echo "bytes {attempt_resume}-..." in Content-Range; anything else means
    // the remote content may have changed since the prefix was written, and
    // appending would splice new bytes onto old ones (a frankenfile whose
    // running hash still verifies). We fail closed to a fresh download.
    let server_resumed = status == reqwest::StatusCode::PARTIAL_CONTENT;
    let aligned = resume_aligned(
        resp.headers()
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok()),
        attempt_resume,
    );
    // attempt_resume > 0 additionally guards against a server answering an
    // un-Ranged request with 206 "bytes 0-...": with no prefix there is
    // nothing to validate a resume against, and the fallback path has no
    // staging handle to hash through.
    let resumed = server_resumed && aligned && attempt_resume > 0;
    if !resumed && attempt_resume > 0 {
        if server_resumed {
            // Misaligned (or missing) Content-Range on a 206 — log both
            // offsets so operators can see which hosts rewrite content
            // mid-harvest. parse_content_range_start returning None here is
            // expected when the header is absent/unparseable.
            warn!(
                url = %task.url,
                expected_start = attempt_resume,
                reported_start = ?parse_content_range_start(
                    resp.headers()
                        .get(reqwest::header::CONTENT_RANGE)
                        .and_then(|v| v.to_str().ok()),
                ),
                "206 resume offset mismatch; restarting download from scratch"
            );
        }
        // The stale prefix is garbage for hashing. For the locked handle we
        // must truncate in place (can't delete an open file on Windows); the
        // fallback path can just recreate.
        match &mut staging_file {
            Some(file) => {
                file.set_len(0).await?;
                use tokio::io::AsyncSeekExt;
                file.seek(std::io::SeekFrom::Start(0)).await?;
            }
            None => {
                let _ = tokio::fs::remove_file(&part_path).await;
            }
        }
    }

    // Hash the full file content: on a validated resume, stream-hash the
    // existing prefix first so the final digest covers the whole file. Only
    // reached after alignment is proven above, so the prefix hashed here is
    // guaranteed to be what the server is continuing.
    //
    // The prefix MUST be hashed through the locked staging handle: opening a
    // second read handle trips our own fs4 exclusive byte-range lock on
    // Windows (ERROR_LOCK_VIOLATION, os error 33). Reading leaves the cursor
    // at the resume offset, i.e. EOF of the prefix, ready to append.
    let mut hasher = Sha256::new();
    if resumed {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        let f = staging_file
            .as_mut()
            .expect("validated resume implies the owned locked handle");
        f.seek(std::io::SeekFrom::Start(0)).await?;
        let mut remaining = attempt_resume;
        let mut buf = vec![0u8; 256 * 1024];
        while remaining > 0 {
            let want = (remaining.min(buf.len() as u64)) as usize;
            let n = f.read(&mut buf[..want]).await?;
            if n == 0 {
                return Err(anyhow::anyhow!(
                    ".part shrank below resume offset {attempt_resume}"
                ));
            }
            hasher.update(&buf[..n]);
            remaining -= n as u64;
        }
    }
    let written_start = if resumed { attempt_resume } else { 0 };

    // Stream to .part — bounded memory regardless of file size. On the
    // primary path we write through the LOCKED handle (reopening would
    // collide with our own lock on Windows).
    let mut file = match staging_file.take() {
        Some(f) => f,
        None => {
            tokio::fs::OpenOptions::new()
                .create(true)
                .append(resumed)
                .write(true)
                .truncate(!resumed)
                .open(&part_path)
                .await?
        }
    };

    let mut stream = resp.bytes_stream();
    let mut written = written_start;
    // Audio keeps its configured cap; applying it to other kinds would make
    // videos/documents over ~500MiB unharvestable (see OTHER_MEDIA_MAX_BYTES).
    let max_size = match task.kind {
        TaskKind::AudioFile => ctx.config.media.max_audio_size_bytes as u64,
        _ => OTHER_MEDIA_MAX_BYTES,
    };
    while let Some(chunk) = stream.next().await {
        let chunk: Bytes = chunk?;
        written += chunk.len() as u64;
        if written > max_size {
            drop(file);
            cleanup(&part_path).await;
            return Ok(DownloadOutcome {
                status: CrawlStatus::ContentTooLarge,
                stored: None,
                probe: None,
                format: None,
                bytes: written,
                error: Some(format!("exceeds {max_size}-byte cap for {:?}", task.kind)),
            });
        }
        hasher.update(&chunk);
        file.write_all(&chunk).await?;
    }
    file.sync_all().await?;

    // Promote the finished staging file out of the shared namespace WHILE we
    // still hold the staging lock (this handle carries it). Everything after
    // this point only reads, and reading an unlocked `.part` let a
    // redelivered twin acquire the primary lock, truncate/delete the file
    // mid-classification, and have us commit bytes that don't match
    // `content_hash`. After the rename a twin finds no resumable prefix and
    // downloads fresh; its lookup-vs-put still dedups at the store. The
    // never-locked fallback path promotes identically (its nonce suffix
    // keeps concurrent `.done` names disjoint).
    let done_path = PathBuf::from(&ctx.config.media.store_dir)
        .join("parts")
        .join(format!("{staging_key}-{attempt_nonce:08x}.done"));
    if let Err(e) = rename_with_retry(&part_path, &done_path).await {
        // Un-promoted bytes cannot be classified safely — fail closed rather
        // than risk the mixed-content commit this guard exists for.
        cleanup(&part_path).await;
        return Err(anyhow::anyhow!("staging promote failed: {e}"));
    }
    part_path = done_path;
    drop(file);

    let content_hash = hex::encode(hasher.finalize());

    // Magic-byte sniff: reject HTML error pages saved with media names.
    // Acceptance depends on the requested TaskKind — audio tasks demand
    // audio magic bytes, video demands video, documents documents; BinaryFile
    // accepts anything infer recognizes.
    let head = read_head(&part_path, 4100).await?;
    match verify_magic_bytes(task, &head) {
        Ok(format_ext) => format_ext,
        Err(reason) => {
            quarantine(&ctx.config, &part_path, "wrong-magic-bytes").await;
            return Ok(DownloadOutcome {
                status: CrawlStatus::ProbeFailed,
                stored: None,
                probe: None,
                format: None,
                bytes: written,
                error: Some(reason),
            });
        }
    };

    // Probe + quality gates (audio only for now). Bulk-archive tasks (FMA
    // subset zips) carry AudioFile kind but a zip payload lofty cannot parse,
    // so they skip probing like non-audio media and commit for post-download
    // extraction.
    if task.kind != TaskKind::AudioFile || is_bulk_archive(task) {
        // Non-audio (or bulk-archive) media: verified + committed, no deep probe.
        return commit_stored(ctx, task, &part_path, &content_hash, written, start).await;
    }

    let quality = AudioQuality {
        min_duration_secs: ctx.config.media.min_duration_secs,
        max_duration_secs: ctx.config.media.max_duration_secs,
        min_bitrate_kbps: ctx.config.media.min_bitrate_kbps,
    };
    // lofty's full parse is CPU-bound and can take seconds on large files —
    // run it on the blocking pool so 512 concurrent tasks don't starve the
    // runtime threads.
    let probe_path = part_path.clone();
    let probe = match tokio::task::spawn_blocking(move || probe_audio(&probe_path)).await {
        Ok(Ok(p)) => Some(p),
        Ok(Err(e)) => {
            quarantine(&ctx.config, &part_path, "unprobeable").await;
            return Ok(DownloadOutcome {
                status: CrawlStatus::ProbeFailed,
                stored: None,
                probe: None,
                format: None,
                bytes: written,
                error: Some(e.to_string()),
            });
        }
        Err(e) => {
            quarantine(&ctx.config, &part_path, "unprobeable").await;
            return Ok(DownloadOutcome {
                status: CrawlStatus::ProbeFailed,
                stored: None,
                probe: None,
                format: None,
                bytes: written,
                error: Some(format!("probe task panicked: {e}")),
            });
        }
    };
    // `probe` is always Some here (Err paths returned above); unwrap once and
    // keep ownership for the quality check + outcome.
    let probe = probe.expect("probe present on success path");
    if let Err(rej) = probe.check_quality(&quality) {
        quarantine(&ctx.config, &part_path, "quality-rejected").await;
        return Ok(DownloadOutcome {
            status: CrawlStatus::QualityRejected,
            stored: None,
            probe: Some(probe),
            format: None,
            bytes: written,
            error: Some(rej.to_string()),
        });
    }

    // Commit content-addressed (shared with all media kinds), then attach the
    // audio probe to the outcome.
    let mut outcome = commit_stored(ctx, task, &part_path, &content_hash, written, start).await?;
    outcome.probe = Some(probe);
    Ok(outcome)
}

/// Extract the start offset from a `Content-Range` response header value.
///
/// Handles both full form ("bytes 123-456/789") and unsatisfied form
/// ("bytes */1000"); returns None for garbage, other units, or a missing
/// header. The unsatisfied form has no byte range to resume, so None is
/// correct there too.
fn parse_content_range_start(header: Option<&str>) -> Option<u64> {
    header?
        .strip_prefix("bytes ")?
        .split('-')
        .next()?
        .trim()
        .parse::<u64>()
        .ok()
}

/// Decide whether a 206 response is safe to resume onto the existing .part
/// prefix. True only when Content-Range proves the server's stream starts at
/// exactly `expected_start` — the resume offset our Range asked for. A
/// missing or unparseable header also fails closed: without it we cannot
/// distinguish an aligned resume from bytes spliced onto a stale prefix (the
/// remote file changed between attempts), which would hash corrupt content.
fn resume_aligned(content_range: Option<&str>, expected_start: u64) -> bool {
    parse_content_range_start(content_range) == Some(expected_start)
}

/// FMA bulk-download convention (`emit_archive_task` in the fma adapter):
/// one AudioFile task whose payload is the whole subset zip, marked by a
/// `source_id` ending in `-archive`.
fn is_bulk_archive(task: &CrawlTask) -> bool {
    task.kind == TaskKind::AudioFile
        && task
            .media
            .as_ref()
            .is_some_and(|m| m.source_id.ends_with("-archive"))
}

/// Validate sniffed magic bytes against the requested media kind.
/// Returns the normalized extension on success, a rejection reason on failure.
fn verify_magic_bytes(task: &CrawlTask, head: &[u8]) -> Result<String, String> {
    // Tokens below are infer 0.16's actual mime_type() strings (verified in
    // the crate source): e.g. Ogg Opus sniffs as "audio/opus" and M4A as
    // "audio/m4a"; there is no "application/ogg"/"audio/flac"/"audio/wav"
    // token for these matchers. Keep in sync with the crate version.
    const AUDIO_MIMES: &[&str] = &[
        "audio/mpeg",
        "audio/x-flac",
        "audio/opus", // Ogg Opus container
        "audio/ogg",
        "audio/x-wav",
        "audio/m4a", // ISO-BMFF audio container
        "video/mp4", // some muxers tag m4a with video-brand ftyp boxes
        "audio/aac",
    ];
    const VIDEO_MIMES: &[&str] = &[
        "video/mp4",
        "video/x-m4v",
        "video/webm",
        "video/x-matroska",
        "video/quicktime",
        "video/x-msvideo",
        "audio/m4a", // m4v/m4a ambiguity — accept, extension disambiguates
    ];
    const DOCUMENT_MIMES: &[&str] = &[
        "application/pdf",
        "application/zip",
        "application/x-ole-storage",
        "application/vnd.openxmlformats-officedocument",
    ];

    let kind = task.kind;
    let Some(info) = infer::get(head) else {
        // BinaryFile accepts opaque blobs; every other kind needs recognition.
        if kind == TaskKind::BinaryFile {
            return Ok("bin".to_string());
        }
        // Plain text has no magic bytes, so infer can never classify it and
        // the text/plain arm below is unreachable for it — sniff textiness
        // here instead of rejecting every .txt document.
        if kind == TaskKind::DocumentFile && looks_like_text(head) {
            return Ok("txt".to_string());
        }
        return Err("unrecognized magic bytes".to_string());
    };

    let mime = info.mime_type();
    let accepted = match kind {
        TaskKind::AudioFile => {
            // Bulk-archive exception: an FMA subset zip ships as an AudioFile
            // task but sniffs application/zip. Accept it so it lands in the
            // store for post-download extraction instead of quarantining a
            // multi-GB download after transfer.
            (is_bulk_archive(task) && mime == "application/zip") || AUDIO_MIMES.contains(&mime)
        }
        TaskKind::VideoFile => VIDEO_MIMES.contains(&mime),
        TaskKind::DocumentFile => {
            DOCUMENT_MIMES.iter().any(|d| mime.starts_with(d))
                || matches!(
                    mime,
                    "application/pdf"
                        | "application/epub+zip"
                        | "application/zip"
                        | "application/x-mobipocket-ebook"
                        | "text/plain"
                )
        }
        TaskKind::BinaryFile | TaskKind::Page => true,
    };
    if !accepted {
        return Err(format!("magic bytes say {mime}, expected {:?}", kind));
    }
    Ok(normalized_extension(mime, info.extension()))
}

/// Heuristic plain-text detector for magic-less content (DocumentFile tasks).
///
/// A head counts as text when it is valid UTF-8 with no NUL bytes and at
/// least 90% ASCII printable/whitespace — strict enough that binary formats
/// (whose headers are dense control/extended bytes) never pass.
fn looks_like_text(head: &[u8]) -> bool {
    let sample = &head[..head.len().min(4096)];
    if std::str::from_utf8(sample).is_err() {
        return false;
    }
    if sample.contains(&0u8) {
        return false;
    }
    let printable = sample
        .iter()
        .filter(|&&b| b.is_ascii_graphic() || b.is_ascii_whitespace())
        .count();
    // Empty files count as text (trivially all-printable by ratio).
    printable * 100 >= sample.len() * 90
}

/// Map a sniffed mime to our canonical extension token.
fn normalized_extension(mime: &str, fallback_ext: &str) -> String {
    match mime {
        "audio/mpeg" => "mp3".into(),
        "audio/x-flac" => "flac".into(),
        "audio/opus" | "audio/ogg" => "ogg".into(),
        "audio/x-wav" => "wav".into(),
        _ => fallback_ext.to_ascii_lowercase(),
    }
}

/// Commit a verified download: content-addressed store + outcome assembly
/// shared by all media kinds.
async fn commit_stored(
    ctx: &Arc<MediaContext>,
    task: &CrawlTask,
    part_path: &std::path::Path,
    content_hash: &str,
    written: u64,
    start: std::time::Instant,
) -> Result<DownloadOutcome> {
    let meta = task.media.clone();
    let head = read_head(part_path, 4100).await?;
    let extension = extension_for(task, &head);
    let obj = MediaObject {
        source: meta
            .as_ref()
            .map(|m| m.source.clone())
            .unwrap_or_else(|| "unknown".into()),
        collection: meta.as_ref().and_then(|m| m.collection.clone()),
        sha256: content_hash.to_string(),
        extension: extension.clone(),
    };

    // Commit content-addressed; dedup resolves existing content's real paths.
    let stored = match ctx.store.lookup(&obj).await? {
        Some(existing) => {
            // Dedup hit: bytes were already counted when first committed, so
            // don't inflate max_total_bytes with them again. (Counter resets
            // on worker restart — known limitation, documented.)
            cleanup(part_path).await;
            existing
        }
        None => {
            let stored = ctx.store.put_stream(&obj, part_path).await?;
            cleanup(part_path).await;
            // Only newly-committed bytes count against max_total_bytes.
            // (Counter resets on worker restart — known limitation,
            // documented.)
            let mut total = ctx.total_bytes.lock().await;
            *total += written;
            drop(total);
            stored
        }
    };

    info!(
        url = %task.url,
        hash = %content_hash,
        bytes = written,
        elapsed_ms = start.elapsed().as_millis() as u64,
        "media harvested"
    );

    Ok(DownloadOutcome {
        status: CrawlStatus::Success,
        stored: Some(stored),
        probe: None,
        format: Some(extension),
        bytes: written,
        error: None,
    })
}

fn extension_for(task: &CrawlTask, head: &[u8]) -> String {
    // Trust sniffed type over URL extension. Tokens are infer 0.16's actual
    // mime_type() strings (see verify_magic_bytes).
    if let Some(kind) = infer::get(head) {
        return match kind.mime_type() {
            "audio/mpeg" => "mp3".into(),
            "audio/x-flac" => "flac".into(),
            "audio/opus" | "audio/ogg" => "ogg".into(),
            "audio/x-wav" => "wav".into(),
            // Shared ISO-BMFF container: audio tasks get "m4a", but a real
            // video must keep its sniffed video extension (mp4/m4v/mov) from
            // the fallthrough below — not be mislabeled as audio.
            "audio/m4a" | "video/mp4" if task.kind != TaskKind::VideoFile => "m4a".into(),
            "audio/aac" => "aac".into(),
            _ => kind.extension().to_string(), // "mp4", "m4v", "mov", "zip", ...
        };
    }
    task.url
        .split(['?', '#'])
        .next()
        .unwrap_or("")
        .rsplit('.')
        .next()
        .unwrap_or("bin")
        .to_ascii_lowercase()
}

async fn read_head(path: &std::path::Path, n: usize) -> Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;
    let mut f = tokio::fs::File::open(path).await?;
    let mut buf = vec![0u8; n];
    let mut total = 0;
    loop {
        let read = f.read(&mut buf[total..]).await?;
        if read == 0 {
            break;
        }
        total += read;
        if total >= n {
            break;
        }
    }
    buf.truncate(total);
    Ok(buf)
}

/// Move a rejected file into quarantine (never silently delete evidence).
async fn quarantine(config: &ArachneConfig, part_path: &std::path::Path, reason: &str) {
    let dest_dir = PathBuf::from(&config.media.store_dir)
        .join("quarantine")
        .join(reason);
    if let Err(e) = tokio::fs::create_dir_all(&dest_dir).await {
        warn!("quarantine dir create failed: {e}");
        return;
    }
    let name = part_path.file_name().unwrap_or_default().to_owned();
    let dest = dest_dir.join(name);
    if let Err(e) = rename_with_retry(part_path, &dest).await {
        warn!("quarantine move failed: {e}");
    }
}

async fn cleanup(part_path: &std::path::Path) {
    if let Err(e) = tokio::fs::remove_file(part_path).await
        && e.kind() != std::io::ErrorKind::NotFound
    {
        warn!("failed removing .part {}: {e}", part_path.display());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arachne_core::models::{CrawlTask, MediaMeta, TaskKind};
    use tokio::io::AsyncReadExt;

    /// Generate a valid 16-bit PCM mono WAV of ~35s at 8kHz (128 kbps).
    fn wav_bytes(duration_secs: usize) -> Vec<u8> {
        let sample_rate: u32 = 8000;
        let data_len = (sample_rate as usize * duration_secs) * 2;
        let mut b = Vec::with_capacity(44 + data_len);
        b.extend_from_slice(b"RIFF");
        b.extend_from_slice(&(36 + data_len as u32).to_le_bytes());
        b.extend_from_slice(b"WAVE");
        b.extend_from_slice(b"fmt ");
        b.extend_from_slice(&16u32.to_le_bytes());
        b.extend_from_slice(&1u16.to_le_bytes()); // PCM
        b.extend_from_slice(&1u16.to_le_bytes()); // mono
        b.extend_from_slice(&sample_rate.to_le_bytes());
        b.extend_from_slice(&(sample_rate * 2).to_le_bytes()); // byte rate
        b.extend_from_slice(&2u16.to_le_bytes()); // block align
        b.extend_from_slice(&16u16.to_le_bytes()); // bits
        b.extend_from_slice(b"data");
        b.extend_from_slice(&(data_len as u32).to_le_bytes());
        // Low-amplitude sine-ish pattern so decoders stay happy.
        for i in 0..data_len / 2 {
            let v = ((i as f64 * 0.01).sin() * 3000.0) as i16;
            b.extend_from_slice(&v.to_le_bytes());
        }
        b
    }

    /// Serve `body` once on an ephemeral port; returns the URL + join handle.
    /// Fully async — safe under the single-threaded #[tokio::test] runtime.
    async fn spawn_one_shot_server(body: Vec<u8>) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::task::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                // Drain request head (headers end at CRLFCRLF).
                let mut buf = [0u8; 4096];
                loop {
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    if n == 0 || String::from_utf8_lossy(&buf[..n]).contains("\r\n\r\n") {
                        break;
                    }
                }
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: audio/wav\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = sock.write_all(head.as_bytes()).await;
                let _ = sock.write_all(&body).await;
                let _ = sock.flush().await;
                let _ = sock.shutdown().await;
            }
        });
        (format!("http://{addr}/test.wav"), handle)
    }

    async fn test_context(store_dir: &std::path::Path) -> Arc<MediaContext> {
        let mut cfg = arachne_core::config::ArachneConfig::default();
        cfg.media.store_dir = store_dir.to_string_lossy().to_string();
        Arc::new(MediaContext::new(cfg).unwrap())
    }

    fn task_for(url: &str) -> CrawlTask {
        CrawlTask {
            url: url.to_string(),
            job_id: uuid::Uuid::new_v4(),
            domain: "127.0.0.1".into(),
            depth: 0,
            priority: 0,
            kind: TaskKind::AudioFile,
            media: Some(MediaMeta {
                source_id: "t1".into(),
                source: "test".into(),
                collection: Some("unittest".into()),
                license: "cc-by-4.0".into(),
                origin_page_url: None,
                license_url: None,
                discovered_from_url: None,
                title: Some("Unit Test Tone".into()),
                artist: None,
                album: None,
            }),
        }
    }

    #[tokio::test]
    async fn downloads_probes_and_stores_wav_end_to_end() {
        let body = wav_bytes(35);
        let (url, server) = spawn_one_shot_server(body.clone()).await;
        let dir = std::env::temp_dir().join(format!("arachne-e2e-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        let ctx = test_context(&dir).await;
        let client = reqwest::Client::new();
        let outcome = harvest_media(&ctx, &client, &task_for(&url)).await;

        assert!(
            matches!(outcome.status, CrawlStatus::Success),
            "expected success, got {:?} err={:?}",
            outcome.status,
            outcome.error
        );
        let stored = outcome.stored.as_ref().expect("stored");
        let fs_path = stored.fs_path.as_ref().expect("local fs path");
        assert!(fs_path.is_file(), "committed file missing at {fs_path:?}");
        assert_eq!(outcome.format.as_deref(), Some("wav"));

        let probe = outcome.probe.expect("probe result");
        assert!(
            (probe.duration_secs - 35.0).abs() < 3.0,
            "unexpected duration {}",
            probe.duration_secs
        );
        // Raw generated WAV carries no tags; title enrichment from MediaMeta
        // happens in the worker's result assembly, not in harvest_audio.
        assert_eq!(probe.title, None);
        assert_eq!(outcome.bytes as usize, body.len());

        // Content-addressed layout: <source>/<collection>/<sha[0:2]>/...
        assert!(stored.object_path.starts_with("test/unittest/"));

        let _ = std::fs::remove_dir_all(&dir);
        let _ = server.await;
    }

    #[tokio::test]
    async fn dedup_resolves_real_store_path() {
        let body = wav_bytes(35);
        let dir = std::env::temp_dir().join(format!("arachne-e2e-dedup-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let ctx = test_context(&dir).await;
        let client = reqwest::Client::new();

        let (url1, s1) = spawn_one_shot_server(body.clone()).await;
        let first = harvest_media(&ctx, &client, &task_for(&url1)).await;
        assert!(matches!(first.status, CrawlStatus::Success), "{first:?}");
        let first_path = first.stored.as_ref().unwrap().fs_path.clone().unwrap();

        let (url2, s2) = spawn_one_shot_server(body).await; // same bytes, different URL
        let second = harvest_media(&ctx, &client, &task_for(&url2)).await;
        assert!(
            matches!(second.status, CrawlStatus::Success),
            "dedup must still succeed: {second:?}"
        );
        let second_stored = second.stored.as_ref().unwrap();

        // The dedup case must resolve to the SAME real file, not a placeholder.
        assert_eq!(
            second_stored.object_path,
            first.stored.as_ref().unwrap().object_path
        );
        assert!(second_stored.fs_path.as_ref().unwrap().is_file());
        assert_eq!(second_stored.fs_path.as_ref().unwrap(), &first_path);

        let _ = std::fs::remove_dir_all(&dir);
        let _ = s1.await;
        let _ = s2.await;
    }

    #[test]
    fn bulk_archive_zip_accepted_with_zip_extension() {
        let mut t = task_for("https://mirror.example/fma_large.zip");
        t.media.as_mut().unwrap().source_id = "fma_large-archive".into();
        assert_eq!(
            verify_magic_bytes(&t, b"PK\x03\x04payload"),
            Ok("zip".to_string())
        );
    }

    #[test]
    fn plain_audio_task_rejects_zip_payload() {
        let mut t = task_for("https://x.example/track.mp3");
        t.media.as_mut().unwrap().source_id = "12345".into();
        assert!(verify_magic_bytes(&t, b"PK\x03\x04payload").is_err());
    }

    #[test]
    fn extension_for_keeps_video_containers_video() {
        // ISO-BMFF heads: ftyp box with brand in bytes 8..12.
        let mp4_head = b"\x00\x00\x00\x18ftypisom\x00\x00\x02\x00isom";
        let mut t = task_for("https://x.example/clip");
        t.kind = TaskKind::VideoFile;
        assert_eq!(extension_for(&t, mp4_head), "mp4");
        t.kind = TaskKind::AudioFile;
        assert_eq!(extension_for(&t, mp4_head), "m4a"); // shared container

        let mov_head = b"\x00\x00\x00\x14ftypqt  \x00\x00\x02\x00qt  ";
        t.kind = TaskKind::VideoFile;
        assert_eq!(extension_for(&t, mov_head), "mov");
    }

    #[test]
    fn plain_text_document_accepted_as_txt() {
        let mut t = task_for("https://x.example/notes.txt");
        t.kind = TaskKind::DocumentFile;
        let head = b"The quick brown fox jumps over the lazy dog.\r\nSecond line of plain prose.\n";
        assert_eq!(verify_magic_bytes(&t, head), Ok("txt".to_string()));
    }

    #[test]
    fn utf8_text_with_accents_accepted_as_txt() {
        // Mostly-ASCII prose keeps the >=90% printable ratio despite the
        // multi-byte accented characters.
        let mut t = task_for("https://x.example/cafe.txt");
        t.kind = TaskKind::DocumentFile;
        let head = "A short essay about cafe culture in Sao Paulo; it keeps many \
                    ordinary ASCII sentences so the ratio stays high. Café!\n"
            .to_string()
            .into_bytes();
        assert_eq!(verify_magic_bytes(&t, &head), Ok("txt".to_string()));
    }

    #[test]
    fn zero_filled_binary_rejected_as_document() {
        let mut t = task_for("https://x.example/blob");
        t.kind = TaskKind::DocumentFile;
        // NUL-heavy bytes fail both the NUL check and the printable ratio...
        assert!(verify_magic_bytes(&t, &[0u8; 64]).is_err());
        // ...and random control-byte soup fails the ratio.
        let soup: Vec<u8> = (0..=255u8).collect();
        assert!(verify_magic_bytes(&t, &soup).is_err());
    }

    #[test]
    fn text_head_still_rejected_for_audio_tasks() {
        // Text sniffing is DocumentFile-only; a fake .mp3 stays rejected.
        let t = task_for("https://x.example/fake.mp3");
        assert!(verify_magic_bytes(&t, b"just words, no music\n").is_err());
    }

    /// Regression for the twin-truncation corruption: once the download
    /// finishes, the staging file must have left the shared `.part` name
    /// (rename-under-lock) so a redelivered twin cannot truncate it
    /// mid-classification. Checked at the filesystem level.
    #[tokio::test]
    async fn promoted_done_leaves_no_part_behind() {
        let body = wav_bytes(35);
        let (url, server) = spawn_one_shot_server(body).await;
        let dir = std::env::temp_dir().join(format!("arachne-done-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let ctx = test_context(&dir).await;
        let client = reqwest::Client::new();

        let outcome = harvest_media(&ctx, &client, &task_for(&url)).await;
        assert!(
            matches!(outcome.status, CrawlStatus::Success),
            "{:?} {:?}",
            outcome.status,
            outcome.error
        );

        let parts = PathBuf::from(&ctx.config.media.store_dir).join("parts");
        let leftovers: Vec<PathBuf> = std::fs::read_dir(&parts)
            .expect("parts dir exists")
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|ext| ext == "part"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "staging stayed in shared .part namespace after completion: {leftovers:?}"
        );

        let _ = std::fs::remove_dir_all(&dir);
        let _ = server.await;
    }

    #[test]
    fn parse_content_range_start_handles_valid_full_form() {
        assert_eq!(
            parse_content_range_start(Some("bytes 123-456/789")),
            Some(123)
        );
        assert_eq!(
            parse_content_range_start(Some("bytes 0-0/1")),
            Some(0),
            "start offset zero is a real value"
        );
    }

    #[test]
    fn parse_content_range_start_unsatisfied_and_garbage() {
        // Unsatisfied form: no range exists to resume from.
        assert_eq!(parse_content_range_start(Some("bytes */1000")), None);
        // Wrong unit / malformed.
        assert_eq!(parse_content_range_start(Some("items 5-9/10")), None);
        assert_eq!(parse_content_range_start(Some("bytes ")), None);
        assert_eq!(parse_content_range_start(Some("bytes x-9/10")), None);
        assert_eq!(parse_content_range_start(Some("")), None);
        // Missing header.
        assert_eq!(parse_content_range_start(None), None);
    }

    #[test]
    fn resume_aligned_accepts_only_matching_start() {
        assert!(resume_aligned(Some("bytes 4096-8191/99999"), 4096));
        assert!(
            !resume_aligned(Some("bytes 2048-8191/99999"), 4096),
            "mismatching start means the remote content changed; must restart"
        );
        assert!(
            !resume_aligned(Some("bytes */1000"), 4096),
            "unsatisfied form cannot prove alignment"
        );
        assert!(
            !resume_aligned(None, 4096),
            "missing header fails closed (non-compliant server)"
        );
        assert!(!resume_aligned(Some("bytes 4096-8191/99999"), 0));
    }

    /// Regression for the frankenfile corruption: a stale .part prefix plus a
    /// misaligned 206 must NOT be hashed/appended — the reset path runs
    /// instead. Drives `run` against a scripted server that replies 206 with
    /// a Content-Range starting before our resume offset.
    #[tokio::test]
    async fn mismatched_content_range_restarts_from_scratch() {
        use tokio::io::AsyncWriteExt;

        let body = wav_bytes(35);
        let dir = std::env::temp_dir().join(format!("arachne-resume-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let ctx = test_context(&dir).await;
        let client = reqwest::Client::new();

        // Pre-seed the primary staging file with a stale prefix (as a prior,
        // interrupted attempt would have left it). The attempt below then
        // takes the Owned/resume path rather than the nonce fallback.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://{addr}/stale.wav");
        let task = task_for(&url);

        let mut h = Sha256::new();
        h.update(task.url.as_bytes());
        let staging_key = hex::encode(h.finalize())[..24].to_string();
        let part = PathBuf::from(&ctx.config.media.store_dir)
            .join("parts")
            .join(format!("{staging_key}.part"));
        tokio::fs::create_dir_all(part.parent().unwrap())
            .await
            .unwrap();
        // Stale prefix that no longer matches the (rewritten) remote object.
        let mut pf = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&part)
            .await
            .unwrap();
        let stale_prefix = vec![0xEEu8; 1024];
        pf.write_all(&stale_prefix).await.unwrap();
        pf.flush().await.unwrap();
        drop(pf);

        // One-shot 206 whose Content-Range start (512) disagrees with the
        // on-disk prefix length (1024): the frankenfile trigger.
        let body_len = body.len() as u64;
        let served = body.clone();
        let handle = tokio::task::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = [0u8; 4096];
                loop {
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    if n == 0 || String::from_utf8_lossy(&buf[..n]).contains("\r\n\r\n") {
                        break;
                    }
                }
                let head = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Type: audio/wav\r\n\
                     Content-Range: bytes 512-{}/{body_len}\r\nContent-Length: {body_len}\r\n\
                     Connection: close\r\n\r\n",
                    body_len - 1
                );
                let _ = sock.write_all(head.as_bytes()).await;
                let _ = sock.write_all(&served).await;
                let _ = sock.flush().await;
                let _ = sock.shutdown().await;
            }
        });

        let outcome = harvest_media(&ctx, &client, &task).await;
        let _ = handle.await;
        assert!(
            matches!(outcome.status, CrawlStatus::Success),
            "restart path must still complete: {:?} {:?}",
            outcome.status,
            outcome.error
        );
        // Full fresh download: byte count is the whole body, not prefix+body.
        assert_eq!(outcome.bytes as usize, body.len());

        // The committed bytes are exactly the served body — no 0xEE splice.
        let stored = outcome.stored.expect("stored");
        let fs_path = stored.fs_path.as_ref().expect("local fs path");
        let committed = std::fs::read(fs_path).unwrap();
        assert_eq!(
            committed, body,
            "frankenfile: stale prefix leaked into commit"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The aligned case still resumes: a matching Content-Range start keeps
    /// the prefix and reports prefix+suffix bytes.
    #[tokio::test]
    async fn matched_content_range_still_resumes_prefix() {
        use tokio::io::AsyncWriteExt;

        let body = wav_bytes(35);
        let dir = std::env::temp_dir().join(format!("arachne-resume-ok-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let ctx = test_context(&dir).await;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://{addr}/resume.wav");
        let task = task_for(&url);

        let mut h = Sha256::new();
        h.update(task.url.as_bytes());
        let staging_key = hex::encode(h.finalize())[..24].to_string();
        let part = PathBuf::from(&ctx.config.media.store_dir)
            .join("parts")
            .join(format!("{staging_key}.part"));
        tokio::fs::create_dir_all(part.parent().unwrap())
            .await
            .unwrap();
        let prefix_len = 1024usize;
        let mut pf = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&part)
            .await
            .unwrap();
        pf.write_all(&body[..prefix_len]).await.unwrap();
        pf.flush().await.unwrap();
        drop(pf);

        // Compliant 206 continuing exactly at the prefix length.
        let suffix = body[prefix_len..].to_vec();
        let suffix_len = suffix.len() as u64;
        let total = body.len() as u64;
        let handle = tokio::task::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = [0u8; 4096];
                loop {
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    if n == 0 || String::from_utf8_lossy(&buf[..n]).contains("\r\n\r\n") {
                        break;
                    }
                }
                let head = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Type: audio/wav\r\n\
                     Content-Range: bytes {prefix_len}-{}/{total}\r\nContent-Length: {suffix_len}\r\n\
                     Connection: close\r\n\r\n",
                    total - 1
                );
                let _ = sock.write_all(head.as_bytes()).await;
                let _ = sock.write_all(&suffix).await;
                let _ = sock.flush().await;
                let _ = sock.shutdown().await;
            }
        });

        let client = reqwest::Client::new();
        let outcome = harvest_media(&ctx, &client, &task).await;
        let _ = handle.await;
        assert!(
            matches!(outcome.status, CrawlStatus::Success),
            "{:?} {:?}",
            outcome.status,
            outcome.error
        );
        // Resume accounting covers prefix + streamed suffix.
        assert_eq!(outcome.bytes as usize, body.len());
        let stored = outcome.stored.expect("stored");
        let fs_path = stored.fs_path.as_ref().expect("local fs path");
        let committed = std::fs::read(fs_path).unwrap();
        assert_eq!(committed, body, "resumed file must equal the full body");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
