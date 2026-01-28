//! Dam Filter: Adaptive Token-Bucket Rate Limiter
//!
//! The Dam prevents downstream flooding during IoT "Thundering Herd" events.
//! Features:
//! - Token bucket with configurable refill rate and burst capacity
//! - Per-device rate limiting with global fallback
//! - Overflow queue with backpressure signaling
//! - Lock-free statistics for monitoring
//! - Dynamic rate adjustment via AI controller

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam::queue::ArrayQueue;
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::protocol::KlaisPacket;

/// Dam configuration parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DamConfig {
    /// Maximum sustained rate (tokens/second)
    pub max_rate: u64,
    
    /// Burst capacity (max tokens in bucket)
    pub burst_size: u64,
    
    /// Queue limit for overflow packets
    pub queue_limit: usize,
    
    /// Per-device rate limiting enabled
    pub per_device_limiting: bool,
    
    /// Per-device max rate (if enabled)
    pub per_device_rate: u64,
}

impl Default for DamConfig {
    fn default() -> Self {
        Self {
            max_rate: crate::defaults::DAM_MAX_RATE,
            burst_size: crate::defaults::DAM_BURST_SIZE,
            queue_limit: crate::defaults::DAM_QUEUE_LIMIT,
            per_device_limiting: false,
            per_device_rate: 100,
        }
    }
}

/// Dam statistics (lock-free)
#[derive(Debug, Default)]
pub struct DamStats {
    /// Total packets received
    pub received: AtomicU64,
    
    /// Packets passed through immediately
    pub passed: AtomicU64,
    
    /// Packets queued for later processing
    pub queued: AtomicU64,
    
    /// Packets dropped due to full queue
    pub dropped: AtomicU64,
    
    /// Current queue depth
    pub queue_depth: AtomicUsize,
    
    /// Current token count
    pub tokens: AtomicU64,
}

impl DamStats {
    /// Snapshot current stats
    pub fn snapshot(&self) -> DamStatsSnapshot {
        DamStatsSnapshot {
            received: self.received.load(Ordering::Relaxed),
            passed: self.passed.load(Ordering::Relaxed),
            queued: self.queued.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
            tokens: self.tokens.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time snapshot of dam statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DamStatsSnapshot {
    pub received: u64,
    pub passed: u64,
    pub queued: u64,
    pub dropped: u64,
    pub queue_depth: usize,
    pub tokens: u64,
}

/// Token bucket state for a single rate limiter
#[derive(Debug)]
struct TokenBucket {
    tokens: AtomicU64,
    last_refill: RwLock<Instant>,
    max_tokens: u64,
    refill_rate: u64,  // tokens per second
}

impl TokenBucket {
    fn new(max_tokens: u64, refill_rate: u64) -> Self {
        Self {
            tokens: AtomicU64::new(max_tokens),
            last_refill: RwLock::new(Instant::now()),
            max_tokens,
            refill_rate,
        }
    }

    /// Try to consume a token, returns true if successful
    fn try_consume(&self) -> bool {
        self.refill();
        
        loop {
            let current = self.tokens.load(Ordering::Acquire);
            if current == 0 {
                return false;
            }
            
            if self.tokens.compare_exchange_weak(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Relaxed
            ).is_ok() {
                return true;
            }
        }
    }

    /// Refill tokens based on elapsed time
    fn refill(&self) {
        let mut last_refill = self.last_refill.write();
        let now = Instant::now();
        let elapsed = now.duration_since(*last_refill);
        
        if elapsed.as_millis() < 1 {
            return;
        }

        let new_tokens = (elapsed.as_secs_f64() * self.refill_rate as f64) as u64;
        if new_tokens > 0 {
            let current = self.tokens.load(Ordering::Relaxed);
            let updated = (current + new_tokens).min(self.max_tokens);
            self.tokens.store(updated, Ordering::Release);
            *last_refill = now;
        }
    }

    /// Update refill rate dynamically
    fn set_rate(&mut self, new_rate: u64) {
        self.refill_rate = new_rate;
    }

    /// Get current token count
    fn current_tokens(&self) -> u64 {
        self.refill();
        self.tokens.load(Ordering::Relaxed)
    }
}

/// Result of attempting to pass a packet through the dam
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DamResult {
    /// Packet passed immediately
    Passed,
    /// Packet queued for later
    Queued,
    /// Packet dropped (queue full)
    Dropped,
}

/// The Dam Filter - Adaptive Rate Limiter
pub struct DamFilter {
    config: RwLock<DamConfig>,
    global_bucket: RwLock<TokenBucket>,
    device_buckets: DashMap<String, TokenBucket>,
    overflow_queue: ArrayQueue<KlaisPacket>,
    stats: Arc<DamStats>,
    output_tx: mpsc::Sender<KlaisPacket>,
}

impl DamFilter {
    /// Create a new DamFilter with the given configuration
    pub fn new(config: DamConfig, output_tx: mpsc::Sender<KlaisPacket>) -> Self {
        let overflow_queue = ArrayQueue::new(config.queue_limit);
        let global_bucket = TokenBucket::new(config.burst_size, config.max_rate);
        
        Self {
            config: RwLock::new(config),
            global_bucket: RwLock::new(global_bucket),
            device_buckets: DashMap::new(),
            overflow_queue,
            stats: Arc::new(DamStats::default()),
            output_tx,
        }
    }

    /// Attempt to pass a packet through the dam
    pub async fn try_pass(&self, packet: KlaisPacket) -> DamResult {
        self.stats.received.fetch_add(1, Ordering::Relaxed);

        // Check per-device limit first (if enabled)
        let config = self.config.read();
        if config.per_device_limiting {
            let device_id = packet.device_id.clone();
            let device_bucket = self.device_buckets
                .entry(device_id)
                .or_insert_with(|| TokenBucket::new(config.per_device_rate, config.per_device_rate));
            
            if !device_bucket.try_consume() {
                // Device rate limited - queue or drop
                return self.queue_or_drop(packet).await;
            }
        }
        drop(config);

        // Check global bucket
        if self.global_bucket.read().try_consume() {
            // Pass immediately
            self.stats.passed.fetch_add(1, Ordering::Relaxed);
            let _ = self.output_tx.send(packet).await;
            DamResult::Passed
        } else {
            // Rate limited - queue or drop
            self.queue_or_drop(packet).await
        }
    }

    /// Queue packet or drop if queue is full
    async fn queue_or_drop(&self, packet: KlaisPacket) -> DamResult {
        match self.overflow_queue.push(packet) {
            Ok(()) => {
                self.stats.queued.fetch_add(1, Ordering::Relaxed);
                self.stats.queue_depth.fetch_add(1, Ordering::Relaxed);
                DamResult::Queued
            }
            Err(_packet) => {
                self.stats.dropped.fetch_add(1, Ordering::Relaxed);
                tracing::warn!("Dam overflow: packet dropped");
                DamResult::Dropped
            }
        }
    }

    /// Drain queued packets (called when tokens become available)
    pub async fn drain_queue(&self) {
        while let Some(packet) = self.overflow_queue.pop() {
            self.stats.queue_depth.fetch_sub(1, Ordering::Relaxed);
            
            if self.global_bucket.read().try_consume() {
                self.stats.passed.fetch_add(1, Ordering::Relaxed);
                let _ = self.output_tx.send(packet).await;
            } else {
                // Put it back and stop draining
                let _ = self.overflow_queue.push(packet);
                self.stats.queue_depth.fetch_add(1, Ordering::Relaxed);
                break;
            }
        }
    }

    /// Update the rate limit dynamically (called by AI controller)
    pub fn set_rate(&self, new_rate: u64) {
        let mut config = self.config.write();
        config.max_rate = new_rate;
        
        // Update the bucket
        let mut bucket = self.global_bucket.write();
        bucket.refill_rate = new_rate;
        
        tracing::info!(new_rate = new_rate, "Dam rate limit updated");
    }

    /// Get current rate limit
    pub fn get_rate(&self) -> u64 {
        self.config.read().max_rate
    }

    /// Get queue depth
    pub fn queue_depth(&self) -> usize {
        self.stats.queue_depth.load(Ordering::Relaxed)
    }

    /// Get current token count
    pub fn current_tokens(&self) -> u64 {
        self.global_bucket.read().current_tokens()
    }

    /// Get statistics handle
    pub fn stats(&self) -> Arc<DamStats> {
        Arc::clone(&self.stats)
    }

    /// Get configuration
    pub fn config(&self) -> DamConfig {
        self.config.read().clone()
    }

    /// Spawn background task to periodically drain the queue
    pub fn spawn_drain_task(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(10));
            loop {
                interval.tick().await;
                self.drain_queue().await;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_packet(id: &str, seq: u32) -> KlaisPacket {
        KlaisPacket::new(id.to_string(), seq, bytes::Bytes::from_static(b"{}"))
    }

    #[tokio::test]
    async fn test_dam_passes_within_rate() {
        let (tx, mut rx) = mpsc::channel(100);
        let config = DamConfig {
            max_rate: 100,
            burst_size: 10,
            queue_limit: 100,
            ..Default::default()
        };
        
        let dam = DamFilter::new(config, tx);
        
        // Should pass up to burst_size immediately
        for i in 0..10 {
            let packet = create_test_packet("dev-1", i);
            let result = dam.try_pass(packet).await;
            assert_eq!(result, DamResult::Passed);
        }
        
        // Verify packets were sent
        for _ in 0..10 {
            assert!(rx.try_recv().is_ok());
        }
    }

    #[tokio::test]
    async fn test_dam_queues_on_burst() {
        let (tx, _rx) = mpsc::channel(100);
        let config = DamConfig {
            max_rate: 10,
            burst_size: 5,
            queue_limit: 10,
            ..Default::default()
        };
        
        let dam = DamFilter::new(config, tx);
        
        // Exhaust burst capacity
        for i in 0..5 {
            dam.try_pass(create_test_packet("dev-1", i)).await;
        }
        
        // Next should be queued
        let result = dam.try_pass(create_test_packet("dev-1", 5)).await;
        assert_eq!(result, DamResult::Queued);
        assert_eq!(dam.queue_depth(), 1);
    }

    #[tokio::test]
    async fn test_dam_drops_when_full() {
        let (tx, _rx) = mpsc::channel(100);
        let config = DamConfig {
            max_rate: 1,
            burst_size: 1,
            queue_limit: 2,
            ..Default::default()
        };
        
        let dam = DamFilter::new(config, tx);
        
        // Exhaust burst
        dam.try_pass(create_test_packet("dev-1", 0)).await;
        
        // Fill queue
        dam.try_pass(create_test_packet("dev-1", 1)).await;
        dam.try_pass(create_test_packet("dev-1", 2)).await;
        
        // Should drop
        let result = dam.try_pass(create_test_packet("dev-1", 3)).await;
        assert_eq!(result, DamResult::Dropped);
    }

    #[tokio::test]
    async fn test_set_rate() {
        let (tx, _rx) = mpsc::channel(100);
        let dam = DamFilter::new(DamConfig::default(), tx);
        
        assert_eq!(dam.get_rate(), crate::defaults::DAM_MAX_RATE);
        
        dam.set_rate(1000);
        assert_eq!(dam.get_rate(), 1000);
    }

    #[test]
    fn test_token_bucket_refill() {
        let bucket = TokenBucket::new(10, 100);
        
        // Consume all tokens
        for _ in 0..10 {
            assert!(bucket.try_consume());
        }
        assert!(!bucket.try_consume());
        
        // Simulate time passing (by manipulating last_refill)
        {
            let mut last = bucket.last_refill.write();
            *last = Instant::now() - Duration::from_millis(100);
        }
        
        // Should have refilled some tokens
        bucket.refill();
        assert!(bucket.current_tokens() > 0);
    }
}
