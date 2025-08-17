//! Token bucket implementation for worker admission control
//!
//! Provides rate-limiting for worker threads to prevent memory exhaustion
//! while maintaining high throughput for the external sorter.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Token bucket for controlling admission rate of new work items
#[derive(Debug)]
pub struct TokenBucket {
    /// Current number of tokens available
    tokens: AtomicU64,
    /// Maximum number of tokens the bucket can hold
    capacity: u64,
    /// Rate at which tokens are replenished (tokens per second)
    refill_rate: u64,
    /// Last time the bucket was refilled
    last_refill: Arc<std::sync::Mutex<Instant>>,
}

impl TokenBucket {
    /// Create a new token bucket
    ///
    /// # Arguments
    /// * `capacity` - Maximum number of tokens the bucket can hold
    /// * `refill_rate` - Number of tokens added per second
    pub fn new(capacity: u64, refill_rate: u64) -> Self {
        Self {
            tokens: AtomicU64::new(capacity),
            capacity,
            refill_rate,
            last_refill: Arc::new(std::sync::Mutex::new(Instant::now())),
        }
    }

    /// Try to acquire a specified number of tokens
    ///
    /// Returns true if tokens were acquired, false if not enough tokens available
    pub fn try_acquire(&self, tokens_needed: u64) -> bool {
        self.refill();

        let current_tokens = self.tokens.load(Ordering::Acquire);
        if current_tokens >= tokens_needed {
            // Try to atomically subtract the tokens
            let new_tokens = current_tokens - tokens_needed;
            self.tokens
                .compare_exchange(
                    current_tokens,
                    new_tokens,
                    Ordering::Release,
                    Ordering::Relaxed,
                )
                .is_ok()
        } else {
            false
        }
    }

    /// Acquire tokens, blocking until they become available
    ///
    /// # Arguments
    /// * `tokens_needed` - Number of tokens to acquire
    /// * `timeout` - Maximum time to wait for tokens
    ///
    /// Returns true if tokens were acquired within timeout, false if timeout exceeded
    pub fn acquire_with_timeout(&self, tokens_needed: u64, timeout: Duration) -> bool {
        let start = Instant::now();

        while start.elapsed() < timeout {
            if self.try_acquire(tokens_needed) {
                return true;
            }

            // Short sleep to avoid busy waiting
            std::thread::sleep(Duration::from_millis(1));
        }

        false
    }

    /// Refill tokens based on elapsed time
    fn refill(&self) {
        let now = Instant::now();
        let mut last_refill = self.last_refill.lock().unwrap();
        let elapsed = now.duration_since(*last_refill);

        if elapsed >= Duration::from_millis(100) {
            // Refill at most every 100ms
            let tokens_to_add = (elapsed.as_secs_f64() * self.refill_rate as f64) as u64;

            if tokens_to_add > 0 {
                let current_tokens = self.tokens.load(Ordering::Acquire);
                let new_tokens = (current_tokens + tokens_to_add).min(self.capacity);
                self.tokens.store(new_tokens, Ordering::Release);
                *last_refill = now;
            }
        }
    }

    /// Get current number of available tokens
    pub fn available_tokens(&self) -> u64 {
        self.refill();
        self.tokens.load(Ordering::Acquire)
    }

    /// Get the capacity of the bucket
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Get the refill rate (tokens per second)
    pub fn refill_rate(&self) -> u64 {
        self.refill_rate
    }

    /// Check if the bucket is empty
    pub fn is_empty(&self) -> bool {
        self.available_tokens() == 0
    }

    /// Check if the bucket is full
    pub fn is_full(&self) -> bool {
        self.available_tokens() >= self.capacity
    }
}

/// Worker admission controller using token bucket
#[derive(Debug)]
pub struct WorkerAdmissionController {
    token_bucket: TokenBucket,
    tokens_per_item: u64,
}

impl WorkerAdmissionController {
    /// Create a new admission controller
    ///
    /// # Arguments
    /// * `max_items_per_second` - Maximum number of items to admit per second
    /// * `burst_capacity` - Maximum burst size (tokens that can accumulate)
    /// * `tokens_per_item` - Number of tokens required per work item
    pub fn new(max_items_per_second: u64, burst_capacity: u64, tokens_per_item: u64) -> Self {
        let refill_rate = max_items_per_second * tokens_per_item;
        let capacity = burst_capacity * tokens_per_item;

        Self {
            token_bucket: TokenBucket::new(capacity, refill_rate),
            tokens_per_item,
        }
    }

    /// Try to admit a new work item
    ///
    /// Returns true if the item was admitted, false if rate limit exceeded
    pub fn try_admit(&self) -> bool {
        self.token_bucket.try_acquire(self.tokens_per_item)
    }

    /// Admit a work item, blocking until capacity is available
    ///
    /// # Arguments
    /// * `timeout` - Maximum time to wait for admission
    ///
    /// Returns true if admitted within timeout, false if timeout exceeded
    pub fn admit_with_timeout(&self, timeout: Duration) -> bool {
        self.token_bucket
            .acquire_with_timeout(self.tokens_per_item, timeout)
    }

    /// Get current admission capacity (number of items that can be admitted immediately)
    pub fn current_capacity(&self) -> u64 {
        self.token_bucket.available_tokens() / self.tokens_per_item
    }

    /// Check if the controller is currently accepting new items
    pub fn is_accepting(&self) -> bool {
        self.current_capacity() > 0
    }

    /// Get statistics about the admission controller
    pub fn stats(&self) -> AdmissionStats {
        AdmissionStats {
            available_tokens: self.token_bucket.available_tokens(),
            capacity: self.token_bucket.capacity(),
            refill_rate: self.token_bucket.refill_rate(),
            tokens_per_item: self.tokens_per_item,
            current_capacity: self.current_capacity(),
        }
    }
}

/// Statistics about the admission controller
#[derive(Debug, Clone)]
pub struct AdmissionStats {
    pub available_tokens: u64,
    pub capacity: u64,
    pub refill_rate: u64,
    pub tokens_per_item: u64,
    pub current_capacity: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_token_bucket_basic() {
        let bucket = TokenBucket::new(10, 5); // 10 tokens max, 5 tokens/sec

        // Should have full capacity initially
        assert_eq!(bucket.available_tokens(), 10);

        // Should be able to acquire tokens
        assert!(bucket.try_acquire(5));
        assert_eq!(bucket.available_tokens(), 5);

        // Should not be able to acquire more than available
        assert!(!bucket.try_acquire(10));
        assert_eq!(bucket.available_tokens(), 5);
    }

    #[test]
    fn test_token_bucket_refill() {
        let bucket = TokenBucket::new(10, 10); // 10 tokens max, 10 tokens/sec

        // Consume all tokens
        assert!(bucket.try_acquire(10));
        assert_eq!(bucket.available_tokens(), 0);

        // Wait for refill
        thread::sleep(Duration::from_millis(200));

        // Should have refilled some tokens
        let available = bucket.available_tokens();
        assert!(available > 0);
        assert!(available <= 10);
    }

    #[test]
    fn test_worker_admission_controller() {
        let controller = WorkerAdmissionController::new(5, 10, 1); // 5 items/sec, burst 10, 1 token/item

        // Should be able to admit items up to burst capacity
        for _ in 0..10 {
            assert!(controller.try_admit());
        }

        // Should reject after burst capacity exhausted
        assert!(!controller.try_admit());

        // Wait for refill
        thread::sleep(Duration::from_millis(300));

        // Should be able to admit more items
        assert!(controller.try_admit());
    }

    #[test]
    fn test_admission_controller_stats() {
        let controller = WorkerAdmissionController::new(10, 20, 2);
        let stats = controller.stats();

        assert_eq!(stats.capacity, 40); // 20 * 2
        assert_eq!(stats.refill_rate, 20); // 10 * 2
        assert_eq!(stats.tokens_per_item, 2);
        assert_eq!(stats.current_capacity, 20); // 40 / 2
    }
}
