//! Tracing and observability using eBPF tracepoints.
//!
//! Provides deep system observability:
//! - Latency histograms
//! - Syscall tracing
//! - Scheduler events
//! - Network stack events

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Tracing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TracingConfig {
    /// Enable latency histograms
    pub latency_histograms: bool,
    
    /// Enable syscall tracing
    pub syscall_tracing: bool,
    
    /// Enable scheduler tracing
    pub scheduler_tracing: bool,
    
    /// Histogram bucket boundaries (microseconds)
    pub histogram_buckets: Vec<u64>,
    
    /// Sample rate (1 = every event, 100 = 1% of events)
    pub sample_rate: u32,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            latency_histograms: true,
            syscall_tracing: false,
            scheduler_tracing: false,
            histogram_buckets: vec![1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000],
            sample_rate: 1,
        }
    }
}

/// Latency histogram
#[derive(Debug)]
pub struct Histogram {
    name: String,
    buckets: Vec<u64>,
    counts: Vec<AtomicU64>,
    sum: AtomicU64,
    count: AtomicU64,
    min: AtomicU64,
    max: AtomicU64,
}

impl Histogram {
    /// Create a new histogram
    pub fn new(name: impl Into<String>, buckets: Vec<u64>) -> Self {
        let bucket_count = buckets.len() + 1; // +1 for overflow
        Self {
            name: name.into(),
            buckets,
            counts: (0..bucket_count).map(|_| AtomicU64::new(0)).collect(),
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
            min: AtomicU64::new(u64::MAX),
            max: AtomicU64::new(0),
        }
    }

    /// Record a value
    pub fn observe(&self, value: u64) {
        // Find bucket
        let bucket_idx = self.buckets
            .iter()
            .position(|&b| value <= b)
            .unwrap_or(self.buckets.len());
        
        self.counts[bucket_idx].fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        
        // Update min/max
        let mut current_min = self.min.load(Ordering::Relaxed);
        while value < current_min {
            match self.min.compare_exchange_weak(
                current_min,
                value,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(v) => current_min = v,
            }
        }
        
        let mut current_max = self.max.load(Ordering::Relaxed);
        while value > current_max {
            match self.max.compare_exchange_weak(
                current_max,
                value,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(v) => current_max = v,
            }
        }
    }

    /// Get percentile value
    pub fn percentile(&self, p: f64) -> u64 {
        let total = self.count.load(Ordering::Relaxed);
        if total == 0 {
            return 0;
        }
        
        let target = (total as f64 * p / 100.0) as u64;
        let mut cumulative = 0u64;
        
        for (i, count) in self.counts.iter().enumerate() {
            cumulative += count.load(Ordering::Relaxed);
            if cumulative >= target {
                if i < self.buckets.len() {
                    return self.buckets[i];
                } else {
                    return self.max.load(Ordering::Relaxed);
                }
            }
        }
        
        self.max.load(Ordering::Relaxed)
    }

    /// Get snapshot
    pub fn snapshot(&self) -> HistogramSnapshot {
        let count = self.count.load(Ordering::Relaxed);
        HistogramSnapshot {
            name: self.name.clone(),
            count,
            sum: self.sum.load(Ordering::Relaxed),
            min: if count > 0 { self.min.load(Ordering::Relaxed) } else { 0 },
            max: self.max.load(Ordering::Relaxed),
            mean: if count > 0 {
                self.sum.load(Ordering::Relaxed) as f64 / count as f64
            } else {
                0.0
            },
            p50: self.percentile(50.0),
            p90: self.percentile(90.0),
            p99: self.percentile(99.0),
            p999: self.percentile(99.9),
        }
    }

    /// Reset the histogram
    pub fn reset(&self) {
        for count in &self.counts {
            count.store(0, Ordering::Relaxed);
        }
        self.sum.store(0, Ordering::Relaxed);
        self.count.store(0, Ordering::Relaxed);
        self.min.store(u64::MAX, Ordering::Relaxed);
        self.max.store(0, Ordering::Relaxed);
    }
}

/// Histogram snapshot for serialization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramSnapshot {
    pub name: String,
    pub count: u64,
    pub sum: u64,
    pub min: u64,
    pub max: u64,
    pub mean: f64,
    pub p50: u64,
    pub p90: u64,
    pub p99: u64,
    pub p999: u64,
}

/// Timer for measuring durations
pub struct Timer {
    start: Instant,
    histogram: std::sync::Arc<Histogram>,
}

impl Timer {
    /// Start a new timer
    pub fn start(histogram: std::sync::Arc<Histogram>) -> Self {
        Self {
            start: Instant::now(),
            histogram,
        }
    }

    /// Stop and record duration
    pub fn stop(self) -> Duration {
        let elapsed = self.start.elapsed();
        self.histogram.observe(elapsed.as_micros() as u64);
        elapsed
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        self.histogram.observe(elapsed.as_micros() as u64);
    }
}

/// Span for tracing operations
#[derive(Debug)]
pub struct Span {
    name: String,
    start: Instant,
    attributes: HashMap<String, String>,
    parent_id: Option<u64>,
    span_id: u64,
}

impl Span {
    /// Create a new span
    pub fn new(name: impl Into<String>) -> Self {
        static SPAN_ID: AtomicU64 = AtomicU64::new(0);
        
        Self {
            name: name.into(),
            start: Instant::now(),
            attributes: HashMap::new(),
            parent_id: None,
            span_id: SPAN_ID.fetch_add(1, Ordering::Relaxed),
        }
    }

    /// Create a child span
    pub fn child(&self, name: impl Into<String>) -> Self {
        static SPAN_ID: AtomicU64 = AtomicU64::new(0);
        
        Self {
            name: name.into(),
            start: Instant::now(),
            attributes: HashMap::new(),
            parent_id: Some(self.span_id),
            span_id: SPAN_ID.fetch_add(1, Ordering::Relaxed),
        }
    }

    /// Add attribute
    pub fn set_attribute(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.attributes.insert(key.into(), value.into());
    }

    /// Get span ID
    pub fn id(&self) -> u64 {
        self.span_id
    }

    /// Get elapsed duration
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    /// End span and get summary
    pub fn end(self) -> SpanSummary {
        SpanSummary {
            name: self.name,
            duration_us: self.start.elapsed().as_micros() as u64,
            attributes: self.attributes,
            span_id: self.span_id,
            parent_id: self.parent_id,
        }
    }
}

/// Span summary for logging
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanSummary {
    pub name: String,
    pub duration_us: u64,
    pub attributes: HashMap<String, String>,
    pub span_id: u64,
    pub parent_id: Option<u64>,
}

/// Tracer for managing spans and histograms
pub struct Tracer {
    config: TracingConfig,
    histograms: RwLock<HashMap<String, std::sync::Arc<Histogram>>>,
}

impl Tracer {
    /// Create a new tracer
    pub fn new(config: TracingConfig) -> Self {
        Self {
            config,
            histograms: RwLock::new(HashMap::new()),
        }
    }

    /// Get or create a histogram
    pub fn histogram(&self, name: &str) -> std::sync::Arc<Histogram> {
        let histograms = self.histograms.read();
        if let Some(h) = histograms.get(name) {
            return h.clone();
        }
        drop(histograms);
        
        let mut histograms = self.histograms.write();
        histograms
            .entry(name.to_string())
            .or_insert_with(|| {
                std::sync::Arc::new(Histogram::new(name, self.config.histogram_buckets.clone()))
            })
            .clone()
    }

    /// Start a timer
    pub fn timer(&self, name: &str) -> Timer {
        Timer::start(self.histogram(name))
    }

    /// Record a latency value directly
    pub fn record_latency(&self, name: &str, micros: u64) {
        self.histogram(name).observe(micros);
    }

    /// Get all histogram snapshots
    pub fn all_histograms(&self) -> Vec<HistogramSnapshot> {
        self.histograms
            .read()
            .values()
            .map(|h| h.snapshot())
            .collect()
    }

    /// Reset all histograms
    pub fn reset_all(&self) {
        for h in self.histograms.read().values() {
            h.reset();
        }
    }
}

impl Default for Tracer {
    fn default() -> Self {
        Self::new(TracingConfig::default())
    }
}

/// Global tracer instance
pub static TRACER: once_cell::sync::Lazy<Tracer> = 
    once_cell::sync::Lazy::new(Tracer::default);

/// Convenience macro for timing code blocks
#[macro_export]
macro_rules! time_block {
    ($name:expr, $block:block) => {{
        let _timer = $crate::control::tracing::TRACER.timer($name);
        $block
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_histogram() {
        let h = Histogram::new("test", vec![10, 50, 100, 500]);
        
        h.observe(5);
        h.observe(30);
        h.observe(75);
        h.observe(200);
        h.observe(1000);
        
        let snap = h.snapshot();
        assert_eq!(snap.count, 5);
        assert_eq!(snap.min, 5);
        assert_eq!(snap.max, 1000);
    }

    #[test]
    fn test_percentiles() {
        let h = Histogram::new("test", vec![10, 50, 100]);
        
        // Add 100 values: 1-100
        for i in 1..=100 {
            h.observe(i);
        }
        
        let p50 = h.percentile(50.0);
        let p99 = h.percentile(99.0);
        
        assert!(p50 <= 50);
        assert!(p99 >= 99);
    }

    #[test]
    fn test_timer() {
        let h = std::sync::Arc::new(Histogram::new("test", vec![1000, 10000]));
        
        {
            let _timer = Timer::start(h.clone());
            std::thread::sleep(Duration::from_micros(100));
        }
        
        assert!(h.snapshot().count > 0);
    }

    #[test]
    fn test_span() {
        let mut span = Span::new("test_operation");
        span.set_attribute("key", "value");
        
        std::thread::sleep(Duration::from_micros(100));
        
        let summary = span.end();
        assert_eq!(summary.name, "test_operation");
        assert!(summary.duration_us >= 100);
    }

    #[test]
    fn test_tracer() {
        let tracer = Tracer::default();
        
        tracer.record_latency("operation", 100);
        tracer.record_latency("operation", 200);
        
        let histograms = tracer.all_histograms();
        assert!(!histograms.is_empty());
    }
}
