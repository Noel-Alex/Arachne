//! URL discovery beyond `<a href>`: sitemaps, feeds with enclosures,
//! and generic HTML media-link detection (audio, video, documents).

pub mod audio_links;
pub mod feeds;
pub mod media_links;
pub mod sitemap;
pub mod wire;

pub use feeds::{FeedEntry, parse_feed};
pub use sitemap::parse_sitemap;
