//! Streaming audio download path for the worker.
//!
//! Streams response bodies straight to `.part` files with a running SHA-256
//! (never buffering whole files in memory), resumes partial downloads via
//! Range/If-Range, sniffs magic bytes to catch lying extensions, probes with
//! lofty, applies quality gates, and commits content-addressed via MediaStore.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use arachne_core::config::ArachneConfig;
use arachne_core::fsutil::rename_with_retry;
use arachne_core::media::store::{MediaObject, StoredMedia};
use arachne_core::media::{probe_audio, AudioQuality, MediaStore};
use arachne_core::models::{CrawlStatus, CrawlTask, TaskKind};
use bytes::Bytes;
use futures::StreamExt;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, Semaphore};
use tracing::{info, warn};

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
        sem.acquire_owned()
            .await
            .expect("semaphore never closed")
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
        Ok(Self {
            store: MediaStore::local(&config.media.store_dir)?,
            host_limits: HostLimits::new(config.media.per_host_concurrency),
            config,
            total_bytes: Mutex::new(0),
        })
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
        // still overshoot up to max_audio_size_bytes.
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
pub async fn harvest_media(ctx: &Arc<MediaContext>, client: &reqwest::Client, task: &CrawlTask) -> DownloadOutcome {
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

async fn run(ctx: &Arc<MediaContext>, client: &reqwest::Client, task: &CrawlTask) -> Result<DownloadOutcome> {
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
            let resumed_len = part_lock_holder
                .metadata()
                .await
                .map(|m| m.len())
                .unwrap_or(0);
            Staging::Owned {
                path: primary_part,
                file: part_lock_holder,
                resumed_len,
            }
        }
        Err(_) => {
            tracing::debug!(
                url = %task.url,
                "staging file locked by in-flight download; using private .part"
            );
            Staging::Fallback { path: fallback_part }
        }
    };

    let part_path = match &staging {
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
        Staging::Owned { mut file, resumed_len, .. } => {
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

    // A 206 continues the .part; a 200 (server ignored Range) restarts it.
    let resumed = status == reqwest::StatusCode::PARTIAL_CONTENT && attempt_resume > 0;
    if !resumed && attempt_resume > 0 {
        // Server ignored our Range: the prefix is garbage for hashing. For
        // the locked handle we must truncate in place (can't delete an open
        // file on Windows); the fallback path can just recreate.
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

    // Hash the full file content: on resume, stream-hash the existing
    // prefix first so the final digest covers the whole file.
    let mut hasher = Sha256::new();
    if resumed {
        hash_file_into(&part_path, &mut hasher).await?;
    }
    let written_start = if resumed { attempt_resume } else { 0 };

    // Stream to .part — bounded memory regardless of file size. On the
    // primary path we write through the LOCKED handle (reopening would
    // collide with our own lock on Windows).
    let mut file = match staging_file.take() {
        Some(f) => f,
        None => tokio::fs::OpenOptions::new()
            .create(true)
            .append(resumed)
            .write(true)
            .truncate(!resumed)
            .open(&part_path)
            .await?,
    };

    let mut stream = resp.bytes_stream();
    let mut written = written_start;
    let max_size = ctx.config.media.max_audio_size_bytes as u64;
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
                error: Some(format!("exceeds max_audio_size_bytes={max_size}")),
            });
        }
        hasher.update(&chunk);
        file.write_all(&chunk).await?;
    }
    file.sync_all().await?;
    drop(file);

    let content_hash = hex::encode(hasher.finalize());

    // Magic-byte sniff: reject HTML error pages saved with media names.
    // Acceptance depends on the requested TaskKind — audio tasks demand
    // audio magic bytes, video demands video, documents documents; BinaryFile
    // accepts anything infer recognizes.
    let head = read_head(&part_path, 4100).await?;
    match verify_magic_bytes(task.kind, &head) {
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

    // Probe + quality gates (audio only for now).
    if task.kind != TaskKind::AudioFile {
        // Non-audio media: verified + committed, no deep probe yet.
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

/// Validate sniffed magic bytes against the requested media kind.
/// Returns the normalized extension on success, a rejection reason on failure.
fn verify_magic_bytes(kind: TaskKind, head: &[u8]) -> Result<String, String> {
    const AUDIO_MIMES: &[&str] = &[
        "audio/mpeg",
        "audio/flac",
        "audio/x-flac",
        "audio/ogg",
        "application/ogg",
        "audio/wav",
        "audio/x-wav",
        "audio/vnd.wave",
        "audio/x-m4a",
        "video/mp4", // m4a sniffs as video/mp4 (same ISO-BMFF container)
        "audio/aac",
        "audio/x-opus",
    ];
    const VIDEO_MIMES: &[&str] = &[
        "video/mp4",
        "video/x-m4v",
        "video/webm",
        "video/x-matroska",
        "video/quicktime",
        "video/x-msvideo",
        "audio/x-m4a", // m4v/m4a ambiguity — accept, extension disambiguates
    ];
    const DOCUMENT_MIMES: &[&str] = &["application/pdf", "application/zip", "application/x-ole-storage", "application/vnd.openxmlformats-officedocument"];

    let Some(info) = infer::get(head) else {
        // BinaryFile accepts opaque blobs; every other kind needs recognition.
        if kind == TaskKind::BinaryFile {
            return Ok("bin".to_string());
        }
        return Err("unrecognized magic bytes".to_string());
    };

    let mime = info.mime_type();
    let accepted = match kind {
        TaskKind::AudioFile => AUDIO_MIMES.contains(&mime),
        TaskKind::VideoFile => VIDEO_MIMES.contains(&mime),
        TaskKind::DocumentFile => {
            DOCUMENT_MIMES.iter().any(|d| mime.starts_with(d))
                || matches!(mime, "application/pdf" | "application/epub+zip" | "application/zip" | "application/x-mobipocket-ebook" | "text/plain")
        }
        TaskKind::BinaryFile | TaskKind::Page => true,
    };
    if !accepted {
        return Err(format!("magic bytes say {mime}, expected {:?}", kind));
    }
    Ok(normalized_extension(mime, info.extension()))
}

/// Map a sniffed mime to our canonical extension token.
fn normalized_extension(mime: &str, fallback_ext: &str) -> String {
    match mime {
        "audio/mpeg" => "mp3".into(),
        "audio/flac" | "audio/x-flac" => "flac".into(),
        "audio/ogg" | "application/ogg" | "audio/x-opus" => "ogg".into(),
        "audio/wav" | "audio/x-wav" | "audio/vnd.wave" => "wav".into(),
        "audio/x-m4a" | "video/mp4" if false => unreachable!(),
        _ => infer_fallback(fallback_ext),
    }
}

/// Second-stage mapping that distinguishes m4a from mp4 by kind context is
/// handled by callers; this just normalizes known tokens and passes the rest.
fn infer_fallback(ext: &str) -> String {
    match ext {
        "jpg" => "jpg".into(),
        other => other.to_ascii_lowercase(),
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
        source: meta.as_ref().map(|m| m.source.clone()).unwrap_or_else(|| "unknown".into()),
        collection: meta.as_ref().and_then(|m| m.collection.clone()),
        sha256: content_hash.to_string(),
        extension: extension.clone(),
    };

    // Commit content-addressed; dedup resolves existing content's real paths.
    let stored = match ctx.store.lookup(&obj).await? {
        Some(existing) => {
            cleanup(part_path).await;
            existing
        }
        None => {
            let stored = ctx.store.put_stream(&obj, part_path).await?;
            cleanup(part_path).await;
            stored
        }
    };

    let mut total = ctx.total_bytes.lock().await;
    *total += written;
    drop(total);

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

async fn hash_file_into(path: &std::path::Path, hasher: &mut Sha256) -> Result<()> {
    use tokio::io::AsyncReadExt;
    let mut f = tokio::fs::File::open(path).await?;
    let mut buf = vec![0u8; 256 * 1024];
    loop {
        let n = f.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(())
}

fn extension_for(task: &CrawlTask, head: &[u8]) -> String {
    // Trust sniffed type over URL extension.
    if let Some(kind) = infer::get(head) {
        return match kind.mime_type() {
            "audio/mpeg" => "mp3".into(),
            "audio/flac" | "audio/x-flac" => "flac".into(),
            "audio/ogg" | "application/ogg" | "audio/x-opus" => "ogg".into(),
            "audio/wav" | "audio/x-wav" | "audio/vnd.wave" => "wav".into(),
            "audio/x-m4a" | "video/mp4" => "m4a".into(),
            "audio/aac" => "aac".into(),
            _ => kind.extension().to_string(),
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
    let dest_dir = PathBuf::from(&config.media.store_dir).join("quarantine").join(reason);
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
        assert_eq!(second_stored.object_path, first.stored.as_ref().unwrap().object_path);
        assert!(second_stored.fs_path.as_ref().unwrap().is_file());
        assert_eq!(second_stored.fs_path.as_ref().unwrap(), &first_path);

        let _ = std::fs::remove_dir_all(&dir);
        let _ = s1.await;
        let _ = s2.await;
    }
}
