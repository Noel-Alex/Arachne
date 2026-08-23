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
use arachne_core::models::{CrawlStatus, CrawlTask};
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
        if self.config.media.max_total_bytes == 0 {
            return true;
        }
        *self.total_bytes.lock().await < self.config.media.max_total_bytes
    }
}

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
pub async fn harvest_audio(ctx: &Arc<MediaContext>, client: &reqwest::Client, task: &CrawlTask) -> DownloadOutcome {
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

    // Staging file keyed by URL hash so concurrent/consumed tasks don't collide.
    let staging_key: String = {
        let mut h = Sha256::new();
        h.update(task.url.as_bytes());
        hex::encode(h.finalize())[..24].to_string()
    };
    let part_path = PathBuf::from(&ctx.config.media.store_dir)
        .join("parts")
        .join(format!("{staging_key}.part"));
    tokio::fs::create_dir_all(part_path.parent().unwrap()).await?;

    let _permit = ctx.host_limits.acquire(&host).await;

    let attempt_resume = match tokio::fs::metadata(&part_path).await {
        Ok(md) => md.len(),
        Err(_) => 0,
    };

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
        tokio::fs::remove_file(&part_path).await.ok();
    }

    // Hash the full file content: on resume, stream-hash the existing
    // prefix first so the final digest covers the whole file.
    let mut hasher = Sha256::new();
    if resumed {
        hash_file_into(&part_path, &mut hasher).await?;
    }
    let written_start = if resumed { attempt_resume } else { 0 };

    // Stream to .part — bounded memory regardless of file size.
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(resumed)
        .write(true)
        .truncate(!resumed)
        .open(&part_path)
        .await?;

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

    // Magic-byte sniff: reject HTML error pages saved with .mp3 names.
    let head = read_head(&part_path, 4100).await?;
    if let Some(kind) = infer::get(&head) {
        let ext_ok = matches!(
            kind.mime_type(),
            "audio/mpeg" | "audio/flac" | "audio/x-flac" | "audio/ogg" | "audio/wav" | "audio/x-m4a" | "video/mp4"
        );
        if !ext_ok {
            quarantine(&ctx.config, &part_path, "wrong-magic-bytes").await;
            return Ok(DownloadOutcome {
                status: CrawlStatus::ProbeFailed,
                stored: None,
                probe: None,
                format: None,
                bytes: written,
                error: Some(format!("magic bytes say {}", kind.mime_type())),
            });
        }
    } else {
        quarantine(&ctx.config, &part_path, "unknown-format").await;
        return Ok(DownloadOutcome {
            status: CrawlStatus::ProbeFailed,
            stored: None,
            probe: None,
            format: None,
            bytes: written,
            error: Some("unrecognized magic bytes".into()),
        });
    }

    // Probe + quality gates.
    let quality = AudioQuality {
        min_duration_secs: ctx.config.media.min_duration_secs,
        max_duration_secs: ctx.config.media.max_duration_secs,
        min_bitrate_kbps: ctx.config.media.min_bitrate_kbps,
    };
    let probe = match probe_audio(&part_path) {
        Ok(p) => p,
        Err(e) => {
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
    };
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

    // Commit content-addressed.
    let meta = task.media.clone();
    let extension = extension_for(task, &head);
    let obj = MediaObject {
        source: meta.as_ref().map(|m| m.source.clone()).unwrap_or_else(|| host.clone()),
        collection: meta.as_ref().and_then(|m| m.collection.clone()),
        sha256: content_hash.clone(),
        extension: extension.clone(),
    };

    // Skip commit if identical content already stored.
    let stored = if ctx.store.exists(&obj).await? {
        cleanup(&part_path).await;
        StoredMedia {
            object_path: format!("already-present:{}", obj.sha256),
            fs_path: None,
        }
    } else {
        let data = tokio::fs::read(&part_path).await?;
        let stored = ctx.store.put(&obj, data.into()).await?;
        cleanup(&part_path).await;
        stored
    };

    let mut total = ctx.total_bytes.lock().await;
    *total += written;
    drop(total);

    info!(
        url = %task.url,
        hash = %content_hash,
        bytes = written,
        duration_s = probe.duration_secs,
        elapsed_ms = start.elapsed().as_millis() as u64,
        "audio harvested"
    );

    Ok(DownloadOutcome {
        status: CrawlStatus::Success,
        stored: Some(stored),
        format: Some(extension),
        probe: Some(probe),
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
            "audio/ogg" => "ogg".into(),
            "audio/wav" => "wav".into(),
            "audio/x-m4a" | "video/mp4" => "m4a".into(),
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
