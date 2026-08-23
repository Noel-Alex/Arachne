//! URL discovery beyond `<a href>`: sitemaps, feeds with enclosures,
//! and generic HTML audio-link detection.

pub mod audio_links;
pub mod feeds;
pub mod sitemap;

pub use feeds::{parse_feed, FeedEntry};
pub use sitemap::parse_sitemap;
