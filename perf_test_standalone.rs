use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, Duration, SystemTime, UNIX_EPOCH};
use std::sync::Arc;

// ============================================================================
// INLINE DAM LOGIC (Simplified for Zero-Dependency Bench)
// ============================================================================

#[derive(Debug, PartialEq)]
pub enum DamResult {
    Passed,
    Dropped,
    Queued,
}

pub struct DamConfig {
    pub max_rate: u64,
    pub burst_size: u64,
}

pub struct DamFilter {
    tokens: AtomicU64,
    last_update: AtomicU64,
    config: DamConfig,
}

impl DamFilter {
    pub fn new(config: DamConfig) -> Self {
        Self {
            tokens: AtomicU64::new(config.burst_size),
            last_update: AtomicU64::new(Self::now_micros()),
            config,
        }
    }

    fn now_micros() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u64
    }

    pub fn try_pass(&self) -> DamResult {
        // Refill
        let now = Self::now_micros();
        let last = self.last_update.load(Ordering::Relaxed);
        
        if now > last {
            let elapsed_micros = now - last;
            let new_tokens = (elapsed_micros * self.config.max_rate) / 1_000_000;
            if new_tokens > 0 {
                // Update timestamp first to prevent double-refill race (simplified)
                self.last_update.store(now, Ordering::Relaxed);
                self.tokens.fetch_add(new_tokens, Ordering::Relaxed);
            }
        }

        // Consume
        let current = self.tokens.load(Ordering::Relaxed);
        if current >= 1 {
            match self.tokens.compare_exchange(
                current, 
                current - 1, 
                Ordering::Relaxed, 
                Ordering::Relaxed
            ) {
                Ok(_) => DamResult::Passed,
                Err(_) => DamResult::Dropped, // CAS failed ( contention), treat as drop/retry
            }
        } else {
            DamResult::Dropped
        }
    }
}

fn main() {
    println!("Starting Standalone KLAIS Benchmark...");

    let config = DamConfig {
        max_rate: 1_000_000, 
        burst_size: 1_000_000,
    };
    let dam = Arc::new(DamFilter::new(config));
    
    let iterations = 10_000_000;
    let start = Instant::now();
    let mut passed = 0;
    
    for _ in 0..iterations {
        if let DamResult::Passed = dam.try_pass() {
            passed += 1;
        }
    }
    
    let duration = start.elapsed();
    let ops_per_sec = iterations as f64 / duration.as_secs_f64();
    
    println!("BENCHMARK_RESULT: {:.2} ops/sec", ops_per_sec);
    println!("Passed: {}", passed);
}
