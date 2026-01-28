//! AI Inference Engine for KLAIS.
//!
//! Features:
//! - ONNX Runtime integration for model inference
//! - GGML/llama.cpp based LLM inference
//! - Traffic prediction (Temporal Fusion Transformer)
//! - Anomaly detection with device fingerprinting
//! - Feature extraction from packet streams

pub mod features;
pub mod anomaly;
pub mod llm;

#[cfg(feature = "ml")]
pub mod engine;

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
use parking_lot::RwLock;

/// Inference result from the AI model
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceResult {
    /// Predicted packet rate for next interval
    pub predicted_rate: f32,
    
    /// Probability of burst event (0.0 - 1.0)
    pub burst_probability: f32,
    
    /// Recommended rate limit
    pub recommended_limit: u64,
    
    /// Anomaly score (0.0 = normal, 1.0 = anomalous)
    pub anomaly_score: f32,
    
    /// Inference latency in microseconds
    pub latency_us: u64,
}

impl Default for InferenceResult {
    fn default() -> Self {
        Self {
            predicted_rate: 0.0,
            burst_probability: 0.0,
            recommended_limit: 500,
            anomaly_score: 0.0,
            latency_us: 0,
        }
    }
}

/// Time-series window for feature extraction
#[derive(Debug)]
pub struct TimeSeriesWindow {
    /// Packet rates per second
    packet_rates: VecDeque<f32>,
    
    /// Unique device counts per second
    device_counts: VecDeque<f32>,
    
    /// Payload entropy per second
    payload_entropy: VecDeque<f32>,
    
    /// Window size in seconds
    window_size: usize,
}

impl TimeSeriesWindow {
    /// Create a new time-series window
    pub fn new(window_size: usize) -> Self {
        Self {
            packet_rates: VecDeque::with_capacity(window_size),
            device_counts: VecDeque::with_capacity(window_size),
            payload_entropy: VecDeque::with_capacity(window_size),
            window_size,
        }
    }

    /// Push a new observation
    pub fn push(&mut self, packet_rate: f32, device_count: f32, entropy: f32) {
        if self.packet_rates.len() >= self.window_size {
            self.packet_rates.pop_front();
            self.device_counts.pop_front();
            self.payload_entropy.pop_front();
        }
        
        self.packet_rates.push_back(packet_rate);
        self.device_counts.push_back(device_count);
        self.payload_entropy.push_back(entropy);
    }

    /// Check if window is full
    pub fn is_ready(&self) -> bool {
        self.packet_rates.len() >= self.window_size
    }

    /// Get feature vector for inference
    pub fn to_features(&self) -> Vec<f32> {
        let mut features = Vec::with_capacity(self.window_size * 3);
        
        for rate in &self.packet_rates {
            features.push(*rate);
        }
        for count in &self.device_counts {
            features.push(*count);
        }
        for entropy in &self.payload_entropy {
            features.push(*entropy);
        }
        
        features
    }

    /// Get statistics for the window
    pub fn stats(&self) -> WindowStats {
        let rates: Vec<f32> = self.packet_rates.iter().cloned().collect();
        
        WindowStats {
            mean_rate: mean(&rates),
            max_rate: rates.iter().cloned().fold(f32::NEG_INFINITY, f32::max),
            min_rate: rates.iter().cloned().fold(f32::INFINITY, f32::min),
            stddev_rate: stddev(&rates),
        }
    }
}

/// Statistics for a time window
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowStats {
    pub mean_rate: f32,
    pub max_rate: f32,
    pub min_rate: f32,
    pub stddev_rate: f32,
}

/// Rule-based inference (fallback when ML is not available)
pub struct RuleBasedInference {
    window: Arc<RwLock<TimeSeriesWindow>>,
    base_rate: u64,
}

impl RuleBasedInference {
    /// Create a new rule-based inference engine
    pub fn new(window_size: usize, base_rate: u64) -> Self {
        Self {
            window: Arc::new(RwLock::new(TimeSeriesWindow::new(window_size))),
            base_rate,
        }
    }

    /// Update window with new observation
    pub fn observe(&self, packet_rate: f32, device_count: f32, entropy: f32) {
        self.window.write().push(packet_rate, device_count, entropy);
    }

    /// Perform inference using heuristics
    pub fn infer(&self) -> InferenceResult {
        let window = self.window.read();
        
        if !window.is_ready() {
            return InferenceResult::default();
        }
        
        let stats = window.stats();
        let start = std::time::Instant::now();
        
        // Simple heuristics for burst detection
        let burst_probability = if stats.stddev_rate > stats.mean_rate * 0.5 {
            ((stats.stddev_rate / stats.mean_rate) - 0.5).min(1.0).max(0.0)
        } else {
            0.0
        };
        
        // Predict rate as exponential moving average
        let predicted_rate = stats.mean_rate * 1.1;  // Slight upward bias
        
        // Recommend limit based on burst probability
        let recommended_limit = if burst_probability > 0.5 {
            (self.base_rate as f32 * 0.5) as u64
        } else if burst_probability > 0.2 {
            (self.base_rate as f32 * 0.8) as u64
        } else {
            (self.base_rate as f32 * 1.2) as u64
        };
        
        // Simple anomaly detection: rate > 3 * mean
        let anomaly_score = if stats.max_rate > stats.mean_rate * 3.0 {
            ((stats.max_rate / stats.mean_rate) / 10.0).min(1.0)
        } else {
            0.0
        };
        
        let latency_us = start.elapsed().as_micros() as u64;
        
        InferenceResult {
            predicted_rate,
            burst_probability,
            recommended_limit,
            anomaly_score,
            latency_us,
        }
    }
}

// Utility functions
fn mean(data: &[f32]) -> f32 {
    if data.is_empty() {
        return 0.0;
    }
    data.iter().sum::<f32>() / data.len() as f32
}

fn stddev(data: &[f32]) -> f32 {
    if data.len() < 2 {
        return 0.0;
    }
    let m = mean(data);
    let variance = data.iter().map(|x| (x - m).powi(2)).sum::<f32>() / (data.len() - 1) as f32;
    variance.sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_series_window() {
        let mut window = TimeSeriesWindow::new(3);
        
        assert!(!window.is_ready());
        
        window.push(100.0, 10.0, 0.5);
        window.push(150.0, 15.0, 0.6);
        window.push(200.0, 20.0, 0.7);
        
        assert!(window.is_ready());
        
        let features = window.to_features();
        assert_eq!(features.len(), 9);  // 3 * 3
    }

    #[test]
    fn test_rule_based_inference() {
        let inference = RuleBasedInference::new(3, 500);
        
        // Fill window
        inference.observe(100.0, 10.0, 0.5);
        inference.observe(150.0, 15.0, 0.6);
        inference.observe(200.0, 20.0, 0.7);
        
        let result = inference.infer();
        
        assert!(result.predicted_rate > 0.0);
        assert!(result.latency_us < 1000);  // Should be fast
    }

    #[test]
    fn test_burst_detection() {
        let inference = RuleBasedInference::new(3, 500);
        
        // Normal traffic
        inference.observe(100.0, 10.0, 0.5);
        inference.observe(105.0, 10.0, 0.5);
        inference.observe(110.0, 10.0, 0.5);
        
        let result = inference.infer();
        assert!(result.burst_probability < 0.3);
        
        // Spike
        let inference2 = RuleBasedInference::new(3, 500);
        inference2.observe(100.0, 10.0, 0.5);
        inference2.observe(500.0, 50.0, 0.5);  // Big spike
        inference2.observe(100.0, 10.0, 0.5);
        
        let result2 = inference2.infer();
        assert!(result2.burst_probability > 0.3);
    }
}
