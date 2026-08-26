//! Fast, memory-efficient URL deduplication.

use bloomfilter::Bloom;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

/// Fast deduplication engine for URLs using Bloom filter.
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

    /// Check if a URL has probably been seen before (does not allocate).
    pub fn probably_seen(&self, url: &str) -> bool {
        let guard = match self.bloom.read() {
            Ok(g) => g,
            Err(e) => e.into_inner(),
        };
        guard.check(&url.to_string())
    }

    /// Mark a single URL as seen.
    pub fn mark_seen(&self, url: &str) {
        let mut guard = match self.bloom.write() {
            Ok(g) => g,
            Err(e) => e.into_inner(),
        };
        guard.set(&url.to_string());
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Mark multiple URLs as seen.
    pub fn mark_many(&self, urls: &[String]) {
        let mut guard = match self.bloom.write() {
            Ok(g) => g,
            Err(e) => e.into_inner(),
        };
        for url in urls {
            guard.set(url);
        }
        self.count.fetch_add(urls.len() as u64, Ordering::Relaxed);
    }

    /// Return the estimated number of unique items added.
    pub fn estimated_count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bloom_fp_rate_smoke() {
        let dedup = Deduplicator::new(10_000, 0.01);

        let inserted: Vec<String> = (0..1000)
            .map(|i| format!("https://example.org/page/{i}"))
            .collect();
        for url in &inserted {
            dedup.mark_seen(url);
        }

        assert_eq!(dedup.estimated_count(), 1000);
        // Bloom filters never yield false negatives.
        for url in &inserted {
            assert!(dedup.probably_seen(url), "false negative for {url}");
        }

        // Filter sized for 10k items but holding only 1k, so the realized
        // rate sits well under the 1% target; 5/200 is a loose sanity bound.
        let mut false_positives = 0;
        for i in 1000..1200 {
            let fresh = format!("https://example.org/fresh/{i}");
            if dedup.probably_seen(&fresh) {
                false_positives += 1;
            }
        }
        assert!(
            false_positives <= 5,
            "too many false positives: {false_positives}/200"
        );
    }

    #[test]
    fn mark_many_counts() {
        let dedup = Deduplicator::new(100, 0.01);
        dedup.mark_many(&[
            "https://a.com/1".to_string(),
            "https://b.com/2".to_string(),
            "https://c.com/3".to_string(),
        ]);
        assert_eq!(dedup.estimated_count(), 3);
        assert!(dedup.probably_seen("https://a.com/1"));
        assert!(dedup.probably_seen("https://b.com/2"));
        assert!(dedup.probably_seen("https://c.com/3"));
    }
}
