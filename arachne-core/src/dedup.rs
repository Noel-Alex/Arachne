//! URL deduplication using Bloom filter.

use bloomfilter::Bloom;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

/// Fast, memory-efficient deduplication of URLs.
pub struct Deduplicator {
    bloom: RwLock<Bloom<String>>,
    count: AtomicU64,
}

impl Deduplicator {
    /// Create a new deduplicator with a given capacity and false positive rate.
    pub fn new(capacity: u64, fp_rate: f64) -> Self {
        let bloom = Bloom::new_for_fp_rate(capacity as usize, fp_rate);
        Self {
            bloom: RwLock::new(bloom),
            count: AtomicU64::new(0),
        }
    }

    /// Check if a URL has probably been seen before.
    pub fn probably_seen(&self, url: &str) -> bool {
        let guard = self.bloom.read().unwrap();
        guard.check(&url.to_string())
    }

    /// Mark a single URL as seen.
    pub fn mark_seen(&self, url: &str) {
        let mut guard = self.bloom.write().unwrap();
        guard.set(&url.to_string());
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Mark multiple URLs as seen.
    pub fn mark_many(&self, urls: &[String]) {
        let mut guard = self.bloom.write().unwrap();
        for url in urls {
            guard.set(url);
        }
        self.count.fetch_add(urls.len() as u64, Ordering::Relaxed);
    }

    /// Return the number of unique items added.
    pub fn estimated_count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }
}
