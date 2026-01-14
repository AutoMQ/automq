//! Prometheus metrics for the IoT Dam Gateway

use prometheus::{
    Histogram, HistogramOpts, IntCounter, IntGauge, Opts, Registry,
};
use std::sync::Arc;

/// All gateway metrics bundled together
#[derive(Clone)]
pub struct GatewayMetrics {
    pub registry: Registry,
    /// Total packets received via UDP
    pub packets_received: IntCounter,
    /// Total packets successfully sent to Kafka
    pub packets_sent: IntCounter,
    /// Total packets dropped due to rate limiting
    pub packets_dropped: IntCounter,
    /// Total packets queued (waiting for rate limit)
    pub packets_queued: IntCounter,
    /// Current queue depth
    pub queue_depth: IntGauge,
    /// Current rate limit setting
    pub rate_limit: IntGauge,
    /// Available tokens in the bucket
    pub available_tokens: IntGauge,
    /// Kafka send latency
    pub kafka_latency: Histogram,
    /// Protocol parse errors
    pub parse_errors: IntCounter,
}

impl GatewayMetrics {
    /// Create a new metrics registry with all gauges/counters
    pub fn new() -> Arc<Self> {
        let registry = Registry::new();

        let packets_received = IntCounter::with_opts(Opts::new(
            "gateway_packets_received_total",
            "Total number of UDP packets received",
        ))
        .unwrap();

        let packets_sent = IntCounter::with_opts(Opts::new(
            "gateway_packets_sent_total",
            "Total number of packets sent to Kafka",
        ))
        .unwrap();

        let packets_dropped = IntCounter::with_opts(Opts::new(
            "gateway_packets_dropped_total",
            "Total number of packets dropped due to rate limiting",
        ))
        .unwrap();

        let packets_queued = IntCounter::with_opts(Opts::new(
            "gateway_packets_queued_total",
            "Total number of packets queued for later processing",
        ))
        .unwrap();

        let queue_depth = IntGauge::with_opts(Opts::new(
            "gateway_queue_depth",
            "Current number of packets in the overflow queue",
        ))
        .unwrap();

        let rate_limit = IntGauge::with_opts(Opts::new(
            "gateway_rate_limit",
            "Current rate limit setting (msgs/sec)",
        ))
        .unwrap();

        let available_tokens = IntGauge::with_opts(Opts::new(
            "gateway_available_tokens",
            "Current number of available tokens in the bucket",
        ))
        .unwrap();

        let kafka_latency = Histogram::with_opts(HistogramOpts::new(
            "gateway_kafka_send_latency_seconds",
            "Kafka message send latency in seconds",
        ).buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]))
        .unwrap();

        let parse_errors = IntCounter::with_opts(Opts::new(
            "gateway_parse_errors_total",
            "Total number of packet parsing errors",
        ))
        .unwrap();

        // Register all metrics
        registry.register(Box::new(packets_received.clone())).unwrap();
        registry.register(Box::new(packets_sent.clone())).unwrap();
        registry.register(Box::new(packets_dropped.clone())).unwrap();
        registry.register(Box::new(packets_queued.clone())).unwrap();
        registry.register(Box::new(queue_depth.clone())).unwrap();
        registry.register(Box::new(rate_limit.clone())).unwrap();
        registry.register(Box::new(available_tokens.clone())).unwrap();
        registry.register(Box::new(kafka_latency.clone())).unwrap();
        registry.register(Box::new(parse_errors.clone())).unwrap();

        Arc::new(Self {
            registry,
            packets_received,
            packets_sent,
            packets_dropped,
            packets_queued,
            queue_depth,
            rate_limit,
            available_tokens,
            kafka_latency,
            parse_errors,
        })
    }

    /// Render metrics in Prometheus text format
    pub fn render(&self) -> String {
        use prometheus::Encoder;
        let encoder = prometheus::TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    }
}

impl Default for GatewayMetrics {
    fn default() -> Self {
        Arc::try_unwrap(Self::new()).unwrap_or_else(|arc| (*arc).clone())
    }
}
