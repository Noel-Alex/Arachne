//! Storage layer: backend facade ([`ArachneRepo`]), PostgreSQL + legacy ScyllaDB implementations.

pub mod backend;
pub mod postgres;
pub mod repo;
pub mod schema;

pub use backend::ArachneRepo;
pub use repo::{CrawledPageRecord, DomainMetadata};
