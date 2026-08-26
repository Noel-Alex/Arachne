//! Media storage and audio handling.
//!
//! [`MediaStore`] persists downloaded media content-addressed by SHA-256 so
//! re-downloads are idempotent, and exposes the stored path for consumers
//! like Sivana. [`probe_audio`] extracts duration/bitrate/tags with `lofty`.

pub mod probe;
pub mod store;

pub use probe::{AudioQuality, ProbeResult, probe_audio};
pub use store::MediaStore;
