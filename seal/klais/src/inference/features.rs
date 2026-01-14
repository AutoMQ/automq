//! Feature extraction for ML inference.

use std::collections::HashMap;
use crate::protocol::KlaisPacket;

/// Feature extractor for packet streams
#[derive(Debug, Default)]
pub struct FeatureExtractor {
    /// Packet count in current window
    packet_count: u64,
    
    /// Unique devices seen in current window
    devices_seen: HashMap<String, u32>,
    
    /// Total payload bytes
    payload_bytes: u64,
    
    /// Last reset timestamp
    last_reset: std::time::Instant,
}

impl FeatureExtractor {
    /// Create a new feature extractor
    pub fn new() -> Self {
        Self {
            packet_count: 0,
            devices_seen: HashMap::new(),
            payload_bytes: 0,
            last_reset: std::time::Instant::now(),
        }
    }

    /// Process a packet
    pub fn observe(&mut self, packet: &KlaisPacket) {
        self.packet_count += 1;
        self.payload_bytes += packet.payload_raw.len() as u64;
        
        *self.devices_seen.entry(packet.device_id.clone()).or_insert(0) += 1;
    }

    /// Get current packet rate (packets/sec)
    pub fn packet_rate(&self) -> f32 {
        let elapsed = self.last_reset.elapsed().as_secs_f32();
        if elapsed < 0.001 {
            return 0.0;
        }
        self.packet_count as f32 / elapsed
    }

    /// Get unique device count
    pub fn device_count(&self) -> usize {
        self.devices_seen.len()
    }

    /// Calculate Shannon entropy of payload sizes (normalized)
    pub fn payload_entropy(&self) -> f32 {
        if self.packet_count == 0 {
            return 0.0;
        }
        
        // Simplified: entropy based on device distribution
        let total = self.packet_count as f32;
        let mut entropy = 0.0f32;
        
        for &count in self.devices_seen.values() {
            let p = count as f32 / total;
            if p > 0.0 {
                entropy -= p * p.log2();
            }
        }
        
        // Normalize to [0, 1]
        let max_entropy = (self.devices_seen.len() as f32).log2();
        if max_entropy > 0.0 {
            entropy / max_entropy
        } else {
            0.0
        }
    }

    /// Reset for new window
    pub fn reset(&mut self) {
        self.packet_count = 0;
        self.devices_seen.clear();
        self.payload_bytes = 0;
        self.last_reset = std::time::Instant::now();
    }

    /// Extract features and reset
    pub fn extract_and_reset(&mut self) -> ExtractedFeatures {
        let features = ExtractedFeatures {
            packet_rate: self.packet_rate(),
            device_count: self.device_count() as f32,
            payload_entropy: self.payload_entropy(),
            avg_payload_size: if self.packet_count > 0 {
                self.payload_bytes as f32 / self.packet_count as f32
            } else {
                0.0
            },
            window_duration_ms: self.last_reset.elapsed().as_millis() as u64,
        };
        
        self.reset();
        features
    }
}

/// Extracted features for a time window
#[derive(Debug, Clone)]
pub struct ExtractedFeatures {
    pub packet_rate: f32,
    pub device_count: f32,
    pub payload_entropy: f32,
    pub avg_payload_size: f32,
    pub window_duration_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn make_packet(id: &str) -> KlaisPacket {
        KlaisPacket::new(id.to_string(), 0, Bytes::from_static(b"{}"))
    }

    #[test]
    fn test_feature_extraction() {
        let mut extractor = FeatureExtractor::new();
        
        extractor.observe(&make_packet("dev-1"));
        extractor.observe(&make_packet("dev-2"));
        extractor.observe(&make_packet("dev-1"));
        
        assert_eq!(extractor.device_count(), 2);
        assert!(extractor.packet_rate() > 0.0);
        assert!(extractor.payload_entropy() > 0.0);
    }

    #[test]
    fn test_reset() {
        let mut extractor = FeatureExtractor::new();
        
        extractor.observe(&make_packet("dev-1"));
        assert_eq!(extractor.device_count(), 1);
        
        extractor.reset();
        assert_eq!(extractor.device_count(), 0);
    }
}
