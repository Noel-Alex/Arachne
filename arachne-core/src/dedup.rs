//! URL deduplication using Bloom filter.

use bloomfilter::Bloom;
use std::sync::RwLock;

/// Fast, memory-efficient deduplication of URLs.
pub struct Deduplicator {
    bloom: RwLock<Bloom<String>>,
}

impl Deduplicator {
    /// Create a new deduplicator with a given capacity and false positive rate.
    pub fn new(capacity: u64, fp_rate: f64) -> Self {
        let bloom = Bloom::new_for_fp_rate(capacity as usize, fp_rate);
        Self {
            bloom: RwLock::new(bloom),
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
    }

    /// Mark multiple URLs as seen.
    pub fn mark_many(&self, urls: &[String]) {
        let mut guard = self.bloom.write().unwrap();
        for url in urls {
            guard.set(url);
        }
    }

    /// Provide a very rough estimate of the number of unique items added.
    /// Note: `bloomfilter` crate doesn't provide a direct way to get count, so this might return 0.
    pub fn estimated_count(&self) -> u64 {
        0 
    }
}
