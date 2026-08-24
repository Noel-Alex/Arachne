//! Content-addressed media storage backed by `object_store`.
//!
//! Layout: `<root>/<source>/<collection>/<sha[0:2]>/<sha>.<ext>`
//! SHA-prefix sharding keeps directory fanout sane at millions of files.
//! The `object_store` abstraction means S3/R2 backends swap in with zero
//! call-site changes.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;

/// Destination for a stored media file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredMedia {
    /// object_store path (e.g. `jamendo/electronic/ab/deadbeef....mp3`), relative to the store root.
    pub object_path: String,
    /// Filesystem absolute path. Only populated for local backends; consumers
    /// needing raw bytes should fall back to reading via the object path.
    pub fs_path: Option<std::path::PathBuf>,
}

/// A media file ready to be committed to the store.
#[derive(Debug, Clone)]
pub struct MediaObject {
    pub source: String,
    pub collection: Option<String>,
    pub sha256: String,
    pub extension: String,
}

impl MediaObject {
    fn object_path(&self) -> object_store::path::Path {
        let collection = self.collection.as_deref().unwrap_or("misc");
        // Sanitize path segments: single flat token each.
        let seg = |s: &str| crate::fsutil::sanitize_path_segment(s);
        object_store::path::Path::from_iter([
            seg(&self.source),
            seg(collection),
            self.sha256[0..2].to_string(),
            format!("{}.{}", self.sha256, seg(&self.extension)),
        ])
    }
}

/// Storage backend for downloaded media.
#[derive(Clone)]
pub struct MediaStore {
    inner: Arc<dyn ObjectStore>,
    root_url: String,
    is_local: bool,
}

impl std::fmt::Debug for MediaStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MediaStore")
            .field("root", &self.root_url)
            .finish()
    }
}

impl MediaStore {
    /// Local filesystem store rooted at `root` (created if missing).
    pub fn local(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref();
        std::fs::create_dir_all(root)
            .with_context(|| format!("failed to create media store root {}", root.display()))?;
        let canonical = std::fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
        Ok(Self {
            inner: Arc::new(
                LocalFileSystem::new_with_prefix(&canonical)
                    .context("failed to init LocalFileSystem store")?,
            ),
            root_url: format!("file://{}", canonical.to_string_lossy().replace('\\', "/")),
            is_local: true,
        })
    }

    /// Whether an object with this exact content hash already exists.
    pub async fn exists(&self, media: &MediaObject) -> Result<bool> {
        match self.inner.head(&media.object_path()).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Resolve an already-stored object to its paths (dedup case): returns
    /// `Some` with the canonical object/fs paths when the content exists,
    /// `None` otherwise.
    pub async fn lookup(&self, media: &MediaObject) -> Result<Option<StoredMedia>> {
        let path = media.object_path();
        match self.inner.head(&path).await {
            Ok(_) => {
                let fs_path = if self.is_local {
                    Some(local_path_for(&self.root_url, path.as_ref()))
                } else {
                    None
                };
                Ok(Some(StoredMedia {
                    object_path: path.to_string(),
                    fs_path,
                }))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Store bytes content-addressed. Idempotent: re-storing identical
    /// content is a no-op overwrite of the same key.
    pub async fn put(&self, media: &MediaObject, data: Bytes) -> Result<StoredMedia> {
        let path = media.object_path();
        self.inner.put(&path, data.into()).await?;
        Ok(self.stored_paths(path))
    }

    /// Stream a file into the store with bounded memory (never loads the
    /// whole file). `source` must be an already-complete staging file; the
    /// content hash is supplied by the caller (computed during download).
    pub async fn put_stream(
        &self,
        media: &MediaObject,
        source: &std::path::Path,
    ) -> Result<StoredMedia> {
        let path = media.object_path();
        let mut writer =
            object_store::buffered::BufWriter::new(self.inner.clone(), path.clone());
        // 256KB chunks — bounded RAM regardless of file size.
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut file = tokio::fs::File::open(source).await?;
        let mut buf = vec![0u8; 256 * 1024];
        loop {
            let n = file.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            writer.write_all(&buf[..n]).await?;
        }
        writer.shutdown().await?;
        Ok(self.stored_paths(path))
    }

    fn stored_paths(&self, path: object_store::path::Path) -> StoredMedia {
        let fs_path = if self.is_local {
            Some(local_path_for(&self.root_url, path.as_ref()))
        } else {
            None
        };
        StoredMedia {
            object_path: path.to_string(),
            fs_path,
        }
    }

    /// Root URL for provenance records.
    pub fn root_url(&self) -> &str {
        &self.root_url
    }
}

fn local_path_for(root_url: &str, object_path: &str) -> std::path::PathBuf {
    let root = root_url.strip_prefix("file://").unwrap_or(root_url);
    let mut p = std::path::PathBuf::from(root);
    for seg in object_path.split('/') {
        p.push(seg);
    }
    p
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    fn sha(data: &[u8]) -> String {
        let mut h = Sha256::new();
        h.update(data);
        hex::encode(h.finalize())
    }

    #[tokio::test]
    async fn stores_content_addressed_and_idempotent() {
        let dir = std::env::temp_dir().join(format!("arachne-store-test-{}", std::process::id()));
        let store = MediaStore::local(&dir).unwrap();

        let data = b"fake mp3 bytes";
        let media = MediaObject {
            source: "jamendo".into(),
            collection: Some("electronic".into()),
            sha256: sha(data),
            extension: "mp3".into(),
        };

        assert!(!store.exists(&media).await.unwrap());
        let stored = store.put(&media, Bytes::from_static(data)).await.unwrap();

        assert!(stored.object_path.starts_with("jamendo/electronic/"));
        assert!(stored.fs_path.unwrap().exists());
        assert!(store.exists(&media).await.unwrap());

        // Same content under a different "collection" still lands at its own
        // key (collection is part of the path) — but same key re-put succeeds.
        store.put(&media, Bytes::from_static(data)).await.unwrap();
        assert!(store.exists(&media).await.unwrap());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn sanitizes_hostile_segments() {
        let dir = std::env::temp_dir().join(format!("arachne-store-test2-{}", std::process::id()));
        let store = MediaStore::local(&dir).unwrap();
        let data = b"x";
        let media = MediaObject {
            source: "../evil".into(),
            collection: Some("con".into()), // Windows reserved name
            sha256: sha(data),
            extension: "mp3".into(),
        };
        let stored = store.put(&media, Bytes::from_static(data)).await.unwrap();
        // Traversal safety: every path segment must be an inert name,
        // not "." / ".." and not a Windows reserved device name.
        for seg in stored.object_path.split('/') {
            assert_ne!(seg, ".");
            assert_ne!(seg, "..");
            let stem = seg.split('.').next().unwrap_or("");
            for reserved in ["CON", "NUL", "AUX", "PRN"] {
                assert!(
                    !stem.eq_ignore_ascii_case(reserved),
                    "segment {seg:?} contains reserved name"
                );
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
