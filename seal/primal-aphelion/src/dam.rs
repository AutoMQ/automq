//! Dam Filter - Adaptive Token Bucket Rate Limiter
//!
//! Implements a "safety valve" for Thundering Herd IoT scenarios.
//! Uses lock-free atomics for high-performance token management.

use crate::protocol::IotPacket;
use crossbeam_queue::ArrayQueue;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Default configuration values
pub const DEFAULT_MAX_RATE: u64 = 500;      // msgs/sec refill rate
pub const DEFAULT_BURST_SIZE: u64 = 1000;   // Initial bucket capacity
pub const DEFAULT_QUEUE_LIMIT: usize = 10000; // Overflow queue size

/// Result of attempting to pass a packet through the Dam Filter
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DamResult {
    /// Packet allowed through immediately (token consumed)
    Allowed,
    /// Packet queued for later processing
    Queued,
    /// Packet dropped (queue full)
    Dropped,
}

/// Shared state for the Dam Filter (thread-safe)
pub struct DamFilter {
    /// Current token count (scaled by 1000 for sub-token precision)
    tokens: AtomicU64,
    /// Maximum tokens (burst capacity)
    max_tokens: u64,
    /// Refill rate per second (dynamically adjustable)
    refill_rate: AtomicU64,
    /// Overflow queue for packets when tokens exhausted
    queue: ArrayQueue<IotPacket>,
    /// Last refill timestamp
    last_refill: parking_lot::Mutex<Instant>,
    /// Metrics: dropped packets
    dropped_count: AtomicU64,
}

impl DamFilter {
    /// Create a new Dam Filter with specified configuration
    pub fn new(max_rate: u64, burst_size: u64, queue_limit: usize) -> Arc<Self> {
        Arc::new(Self {
            // Scale by 1000 for sub-token precision
            tokens: AtomicU64::new(burst_size * 1000),
            max_tokens: burst_size * 1000,
            refill_rate: AtomicU64::new(max_rate),
            queue: ArrayQueue::new(queue_limit),
            last_refill: parking_lot::Mutex::new(Instant::now()),
            dropped_count: AtomicU64::new(0),
        })
    }

    /// Create with default configuration
    pub fn with_defaults() -> Arc<Self> {
        Self::new(DEFAULT_MAX_RATE, DEFAULT_BURST_SIZE, DEFAULT_QUEUE_LIMIT)
    }

    /// Attempt to pass a packet through the filter
    pub fn try_acquire(&self, packet: IotPacket) -> DamResult {
        // Try to refill tokens first
        self.refill();

        // Try to consume a token (1000 units = 1 token)
        let token_cost: u64 = 1000;

        loop {
            let current = self.tokens.load(Ordering::Relaxed);
            if current >= token_cost {
                // Try to atomically decrement
                match self.tokens.compare_exchange_weak(
                    current,
                    current - token_cost,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return DamResult::Allowed,
                    Err(_) => continue, // Retry
                }
            } else {
                break; // No tokens available
            }
        }

        // No tokens - try to queue
        match self.queue.push(packet) {
            Ok(()) => DamResult::Queued,
            Err(_) => {
                // Queue full - drop packet
                self.dropped_count.fetch_add(1, Ordering::Relaxed);
                DamResult::Dropped
            }
        }
    }

    /// Pop a packet from the overflow queue (if tokens available)
    pub fn try_dequeue(&self) -> Option<IotPacket> {
        self.refill();

        let token_cost: u64 = 1000;

        loop {
            let current = self.tokens.load(Ordering::Relaxed);
            if current >= token_cost {
                // Try to get from queue first
                if let Some(packet) = self.queue.pop() {
                    // Try to consume token
                    match self.tokens.compare_exchange_weak(
                        current,
                        current - token_cost,
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => return Some(packet),
                        Err(_) => {
                            // Race condition - put packet back and retry
                            let _ = self.queue.push(packet);
                            continue;
                        }
                    }
                } else {
                    return None; // Queue empty
                }
            } else {
                return None; // No tokens
            }
        }
    }

    /// Refill tokens based on elapsed time
    fn refill(&self) {
        let now = Instant::now();

        // Quick check without lock
        let elapsed = {
            let last = self.last_refill.lock();
            now.duration_since(*last)
        };

        // Only refill if at least 1ms has passed
        if elapsed.as_millis() < 1 {
            return;
        }

        // Lock for actual refill
        let mut last = self.last_refill.lock();
        let actual_elapsed = now.duration_since(*last);

        if actual_elapsed.as_millis() < 1 {
            return;
        }

        // Calculate tokens to add (rate per second * elapsed seconds * 1000 scale)
        let rate = self.refill_rate.load(Ordering::Relaxed);
        let tokens_to_add = (rate as f64 * actual_elapsed.as_secs_f64() * 1000.0) as u64;

        if tokens_to_add > 0 {
            let current = self.tokens.load(Ordering::Relaxed);
            let new_tokens = (current + tokens_to_add).min(self.max_tokens);
            self.tokens.store(new_tokens, Ordering::Release);
            *last = now;
        }
    }

    /// Update the rate limit dynamically (for AI controller)
    pub fn set_rate(&self, new_rate: u64) {
        self.refill_rate.store(new_rate, Ordering::Release);
    }

    /// Get current rate limit
    pub fn get_rate(&self) -> u64 {
        self.refill_rate.load(Ordering::Relaxed)
    }

    /// Get current queue depth
    pub fn queue_depth(&self) -> usize {
        self.queue.len()
    }

    /// Get total dropped packet count
    pub fn dropped_count(&self) -> u64 {
        self.dropped_count.load(Ordering::Relaxed)
    }

    /// Get current token count (unscaled)
    pub fn available_tokens(&self) -> u64 {
        self.tokens.load(Ordering::Relaxed) / 1000
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_packet(seq: u32) -> IotPacket {
        IotPacket {
            device_id: "test".to_string(),
            sequence: seq,
            payload: serde_json::json!({}),
        }
    }

    #[test]
    fn test_allows_within_burst() {
        let filter = DamFilter::new(100, 10, 100);

        // Should allow up to burst size
        for i in 0..10 {
            let result = filter.try_acquire(test_packet(i));
            assert_eq!(result, DamResult::Allowed, "Packet {i} should be allowed");
        }
    }

    #[test]
    fn test_queues_when_tokens_exhausted() {
        let filter = DamFilter::new(100, 5, 100);

        // Exhaust tokens
        for i in 0..5 {
            let result = filter.try_acquire(test_packet(i));
            assert_eq!(result, DamResult::Allowed);
        }

        // Next should queue
        let result = filter.try_acquire(test_packet(5));
        assert_eq!(result, DamResult::Queued);
        assert_eq!(filter.queue_depth(), 1);
    }

    #[test]
    fn test_drops_when_queue_full() {
        let filter = DamFilter::new(100, 1, 2); // Small queue

        // Exhaust token
        filter.try_acquire(test_packet(0));

        // Fill queue
        filter.try_acquire(test_packet(1)); // Queued
        filter.try_acquire(test_packet(2)); // Queued

        // This should drop
        let result = filter.try_acquire(test_packet(3));
        assert_eq!(result, DamResult::Dropped);
        assert_eq!(filter.dropped_count(), 1);
    }

    #[test]
    fn test_rate_update() {
        let filter = DamFilter::new(500, 1000, 100);
        assert_eq!(filter.get_rate(), 500);

        filter.set_rate(1000);
        assert_eq!(filter.get_rate(), 1000);
    }

    #[test]
    fn test_dequeue_consumes_token() {
        let filter = DamFilter::new(100, 5, 100);

        // Exhaust tokens and queue some packets
        for i in 0..7 {
            filter.try_acquire(test_packet(i));
        }

        assert_eq!(filter.queue_depth(), 2);

        // Wait a bit for token refill (in real scenario)
        // For test, manually refill
        filter.tokens.store(1000, Ordering::Release);

        // Dequeue should work and consume token
        let packet = filter.try_dequeue();
        assert!(packet.is_some());
        assert_eq!(filter.queue_depth(), 1);
    }
}
