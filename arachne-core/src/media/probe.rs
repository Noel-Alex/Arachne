//! Audio probing: duration, bitrate, and tag extraction via `lofty`.

use std::path::Path;

use anyhow::{Context, Result};
use lofty::file::{AudioFile, TaggedFileExt};
use lofty::tag::{Accessor, ItemKey};

/// Quality gates applied to probed audio before it is admitted to the manifest.
#[derive(Debug, Clone)]
pub struct AudioQuality {
    /// Minimum accepted duration in seconds (default 30).
    pub min_duration_secs: u64,
    /// Maximum accepted duration in seconds (default 1800: full live sets ok, 10h DJ mixes no).
    pub max_duration_secs: u64,
    /// Minimum accepted bitrate in kbps (default 96).
    pub min_bitrate_kbps: u32,
}

impl Default for AudioQuality {
    fn default() -> Self {
        Self {
            min_duration_secs: 30,
            max_duration_secs: 1800,
            min_bitrate_kbps: 96,
        }
    }
}

/// Metadata extracted from a downloaded audio file.
#[derive(Debug, Clone)]
pub struct ProbeResult {
    pub duration_secs: f64,
    pub bitrate_kbps: Option<u32>,
    pub title: Option<String>,
    pub artist: Option<String>,
    pub album: Option<String>,
    pub year: Option<i32>,
    pub genre: Option<String>,
}

impl ProbeResult {
    /// Evaluate against quality gates. Returns the failing reason if rejected.
    pub fn check_quality(&self, q: &AudioQuality) -> Result<(), QualityRejection> {
        if self.duration_secs < q.min_duration_secs as f64 {
            return Err(QualityRejection::TooShort(self.duration_secs));
        }
        if self.duration_secs > q.max_duration_secs as f64 {
            return Err(QualityRejection::TooLong(self.duration_secs));
        }
        // Lossless formats report no bitrate from lofty; absence passes the gate.
        if let Some(kbps) = self.bitrate_kbps
            && kbps < q.min_bitrate_kbps
        {
            return Err(QualityRejection::LowBitrate(kbps));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum QualityRejection {
    TooShort(f64),
    TooLong(f64),
    LowBitrate(u32),
}

impl std::fmt::Display for QualityRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooShort(d) => write!(f, "duration {d:.1}s below minimum"),
            Self::TooLong(d) => write!(f, "duration {d:.1}s above maximum"),
            Self::LowBitrate(b) => write!(f, "bitrate {b}kbps below minimum"),
        }
    }
}

fn tag_string(tag: Option<&lofty::tag::Tag>, key: &ItemKey) -> Option<String> {
    tag.and_then(|t| t.get_string(key))
        .map(str::to_owned)
        .filter(|s| !s.trim().is_empty())
}

/// Probe an audio file's properties. Fails if the file is not decodable audio.
///
/// Uses content sniffing (`read_from` + `guess_file_type`), NOT the file
/// extension — staging files are `.part`, and extensions lie anyway.
pub fn probe_audio(path: &Path) -> Result<ProbeResult> {
    let mut file = std::fs::File::open(path).context("failed to open file for probing")?;
    let tagged = lofty::read_from(&mut file).context("lofty could not parse file as audio")?;
    let props = tagged.properties();

    // Prefer the format's primary tag; fall back to whatever tag exists.
    let tag = tagged.primary_tag().or_else(|| tagged.first_tag());

    Ok(ProbeResult {
        duration_secs: props.duration().as_secs_f64(),
        bitrate_kbps: props.audio_bitrate(),
        title: tag_string(tag, &ItemKey::TrackTitle),
        artist: tag_string(tag, &ItemKey::TrackArtist),
        album: tag_string(tag, &ItemKey::AlbumTitle),
        year: tag.and_then(|t| t.year()).map(|y| y as i32),
        genre: tag_string(tag, &ItemKey::Genre),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quality_gates() {
        let q = AudioQuality::default();
        let ok = ProbeResult {
            duration_secs: 200.0,
            bitrate_kbps: Some(192),
            title: None,
            artist: None,
            album: None,
            year: None,
            genre: None,
        };
        assert!(ok.check_quality(&q).is_ok());

        let short = ProbeResult {
            duration_secs: 5.0,
            ..ok.clone()
        };
        assert_eq!(
            short.check_quality(&q),
            Err(QualityRejection::TooShort(5.0))
        );

        let long = ProbeResult {
            duration_secs: 5000.0,
            ..ok.clone()
        };
        assert!(matches!(
            long.check_quality(&q),
            Err(QualityRejection::TooLong(_))
        ));

        let quiet = ProbeResult {
            bitrate_kbps: Some(48),
            ..ok
        };
        assert_eq!(
            quiet.check_quality(&q),
            Err(QualityRejection::LowBitrate(48))
        );
    }

    #[test]
    fn probe_rejects_non_audio() {
        let dir = std::env::temp_dir().join("arachne-probe-test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("not_audio.txt");
        std::fs::write(&path, b"this is definitely not an mp3").unwrap();
        assert!(probe_audio(&path).is_err());
        let _ = std::fs::remove_file(&path);
    }
}
