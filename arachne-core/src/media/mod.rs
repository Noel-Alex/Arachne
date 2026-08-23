//! Media storage and audio handling.
//!
//! [`MediaStore`] persists downloaded media content-addressed by SHA-256 so
//! re-downloads are idempotent, and exposes the stored path for consumers
//! like Sivana. [`probe_audio`] extracts duration/bitrate/tags with `lofty`.

pub mod probe;
pub mod store;

pub use probe::{probe_audio, AudioQuality, ProbeResult};
pub use store::MediaStore;
