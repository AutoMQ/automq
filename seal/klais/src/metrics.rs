//! Prometheus metrics for KLAIS.

use std::sync::Arc;
use prometheus::{
    register_counter_vec, register_gauge, register_histogram_vec,
    CounterVec, Gauge, HistogramVec, Encoder, TextEncoder,
};
use once_cell::sync::Lazy;

/// Packet counters
pub static PACKETS_RECEIVED: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "klais_packets_received_total",
        "Total packets received by type",
        &["status"]
    ).unwrap()
});

/// Bytes counter
pub static BYTES_RECEIVED: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "klais_bytes_received_total",
        "Total bytes received"
    ).unwrap()
});

/// Latency histogram
pub static PROCESSING_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "klais_processing_latency_seconds",
        "Processing latency in seconds",
        &["stage"],
        vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1]
    ).unwrap()
});

/// Queue depth gauge
pub static QUEUE_DEPTH: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "klais_queue_depth",
        "Current dam queue depth"
    ).unwrap()
});

/// Rate limit gauge
pub static RATE_LIMIT: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "klais_rate_limit",
        "Current rate limit"
    ).unwrap()
});

/// Inference latency histogram
pub static INFERENCE_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "klais_inference_latency_seconds",
        "ML inference latency in seconds",
        &["model"],
        vec![0.00001, 0.0001, 0.0005, 0.001, 0.005]
    ).unwrap()
});

/// Initialize all metrics (call at startup)
pub fn init() {
    // Touch lazy statics to ensure they're registered
    let _ = &*PACKETS_RECEIVED;
    let _ = &*BYTES_RECEIVED;
    let _ = &*PROCESSING_LATENCY;
    let _ = &*QUEUE_DEPTH;
    let _ = &*RATE_LIMIT;
    let _ = &*INFERENCE_LATENCY;
}

/// Get metrics in Prometheus text format
pub fn gather() -> String {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

/// Record a packet received
pub fn record_packet(status: &str) {
    PACKETS_RECEIVED.with_label_values(&[status]).inc();
}

/// Record bytes received
pub fn record_bytes(bytes: u64) {
    BYTES_RECEIVED.add(bytes as f64);
}

/// Record processing latency
pub fn record_latency(stage: &str, duration_secs: f64) {
    PROCESSING_LATENCY.with_label_values(&[stage]).observe(duration_secs);
}

/// Update queue depth
pub fn set_queue_depth(depth: usize) {
    QUEUE_DEPTH.set(depth as f64);
}

/// Update rate limit
pub fn set_rate_limit(limit: u64) {
    RATE_LIMIT.set(limit as f64);
}

/// Record inference latency
pub fn record_inference_latency(model: &str, duration_secs: f64) {
    INFERENCE_LATENCY.with_label_values(&[model]).observe(duration_secs);
}
