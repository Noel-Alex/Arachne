//lib.rs
use serde::{Deserialize, Serialize};
use anyhow::Result;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use std::env;
use chrono::{DateTime, Utc, NaiveDateTime};
pub mod db;
use wreq::{Client, ClientBuilder};
use wreq_util::Emulation;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CrawlResult {
    pub source_url: String,
    pub status: CrawlStatus,
    pub content: Option<String>,
    pub discovered_urls: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum CrawlStatus {
    Success,
    HttpError(u16),
    FetchError(String),
}

impl CrawlStatus {
    /// Converts the status to an integer for database storage.
    /// Returns 200 for Success, the actual code for HttpError,
    /// and 0 for FetchError (DNS/Network issues).
    pub fn as_i32(&self) -> i32 {
        match self {
            CrawlStatus::Success => 200,
            CrawlStatus::HttpError(code) => *code as i32,
            // We use 0 to represent a non-HTTP error (like DNS failure)
            // since your DB schema only has an INT column.
            CrawlStatus::FetchError(_) => 0,
        }
    }
}

pub fn client(){
    let http_client = Client::builder()
        .emulation(Emulation::Chrome137)
        .build().unwrap();
}