//! Anomaly detection with device fingerprinting.

use std::collections::HashMap;
use std::time::Instant;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

/// Device behavioral profile
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceProfile {
    /// Device identifier
    pub device_id: String,
    
    /// Average interval between packets (ms)
    pub avg_interval_ms: f32,
    
    /// Standard deviation of interval
    pub interval_stddev: f32,
    
    /// Average payload size
    pub avg_payload_size: f32,
    
    /// Payload size histogram (bucketed)
    pub size_histogram: [u32; 8],
    
    /// Total packets observed
    pub packet_count: u64,
    
    /// Anomaly score (EWMA)
    pub anomaly_score: f32,
    
    /// Last seen timestamp (unix millis)
    pub last_seen: u64,
}

impl DeviceProfile {
    /// Create a new profile
    pub fn new(device_id: String) -> Self {
        Self {
            device_id,
            avg_interval_ms: 0.0,
            interval_stddev: 0.0,
            avg_payload_size: 0.0,
            size_histogram: [0; 8],
            packet_count: 0,
            anomaly_score: 0.0,
            last_seen: 0,
        }
    }

    /// Update profile with new observation
    pub fn update(&mut self, interval_ms: f32, payload_size: usize, timestamp: u64) {
        let n = self.packet_count as f32;
        
        // Incremental mean update
        if self.packet_count > 0 {
            let delta = interval_ms - self.avg_interval_ms;
            self.avg_interval_ms += delta / (n + 1.0);
            
            // Welford's algorithm for variance
            let delta2 = interval_ms - self.avg_interval_ms;
            let m2_new = self.interval_stddev.powi(2) * n + delta * delta2;
            self.interval_stddev = (m2_new / (n + 1.0)).sqrt();
        } else {
            self.avg_interval_ms = interval_ms;
            self.interval_stddev = 0.0;
        }
        
        // Update payload size average
        self.avg_payload_size = (self.avg_payload_size * n + payload_size as f32) / (n + 1.0);
        
        // Update histogram
        let bucket = (payload_size / 128).min(7);
        self.size_histogram[bucket] += 1;
        
        self.packet_count += 1;
        self.last_seen = timestamp;
    }

    /// Check if current observation is anomalous
    pub fn check_anomaly(&mut self, interval_ms: f32, payload_size: usize) -> f32 {
        if self.packet_count < 10 {
            return 0.0;  // Not enough data
        }

        let mut score = 0.0;
        
        // Timing anomaly (z-score)
        if self.interval_stddev > 0.0 {
            let z = (interval_ms - self.avg_interval_ms).abs() / self.interval_stddev;
            if z > 3.0 {
                score += (z - 3.0) / 10.0;
            }
        }
        
        // Payload size anomaly
        let size_z = (payload_size as f32 - self.avg_payload_size).abs() / (self.avg_payload_size + 1.0);
        if size_z > 2.0 {
            score += (size_z - 2.0) / 5.0;
        }
        
        // EWMA update of anomaly score
        let alpha = 0.1;
        self.anomaly_score = alpha * score + (1.0 - alpha) * self.anomaly_score;
        
        self.anomaly_score.min(1.0)
    }
}

/// Anomaly detector with device fingerprinting
pub struct AnomalyDetector {
    profiles: DashMap<String, DeviceProfile>,
    last_seen: DashMap<String, Instant>,
    
    /// Threshold for flagging anomalies
    threshold: f32,
    
    /// Maximum profiles to track
    max_profiles: usize,
}

impl AnomalyDetector {
    /// Create a new anomaly detector
    pub fn new(threshold: f32, max_profiles: usize) -> Self {
        Self {
            profiles: DashMap::new(),
            last_seen: DashMap::new(),
            threshold,
            max_profiles,
        }
    }

    /// Observe a packet and return anomaly score
    pub fn observe(&self, device_id: &str, payload_size: usize) -> f32 {
        let now = Instant::now();
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // Calculate interval since last packet
        let interval_ms = self.last_seen
            .get(device_id)
            .map(|last| now.duration_since(*last).as_millis() as f32)
            .unwrap_or(0.0);
        
        self.last_seen.insert(device_id.to_string(), now);

        // Get or create profile
        let mut profile = self.profiles
            .entry(device_id.to_string())
            .or_insert_with(|| DeviceProfile::new(device_id.to_string()));

        // Check anomaly before updating
        let anomaly_score = profile.check_anomaly(interval_ms, payload_size);
        
        // Update profile
        profile.update(interval_ms, payload_size, timestamp);

        anomaly_score
    }

    /// Check if device is anomalous
    pub fn is_anomalous(&self, device_id: &str) -> bool {
        self.profiles
            .get(device_id)
            .map(|p| p.anomaly_score > self.threshold)
            .unwrap_or(false)
    }

    /// Get profile for device
    pub fn get_profile(&self, device_id: &str) -> Option<DeviceProfile> {
        self.profiles.get(device_id).map(|p| p.clone())
    }

    /// Get all flagged devices
    pub fn flagged_devices(&self) -> Vec<String> {
        self.profiles
            .iter()
            .filter(|entry| entry.value().anomaly_score > self.threshold)
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Cleanup stale profiles
    pub fn cleanup_stale(&self, max_age_secs: u64) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let cutoff = now.saturating_sub(max_age_secs * 1000);

        self.profiles.retain(|_, profile| profile.last_seen > cutoff);
        
        // Also cap the number of profiles
        if self.profiles.len() > self.max_profiles {
            // Remove oldest profiles
            let mut profiles: Vec<_> = self.profiles.iter()
                .map(|e| (e.key().clone(), e.value().last_seen))
                .collect();
            profiles.sort_by_key(|(_, ts)| *ts);
            
            let to_remove = self.profiles.len() - self.max_profiles;
            for (id, _) in profiles.into_iter().take(to_remove) {
                self.profiles.remove(&id);
            }
        }
    }
}

/// Thundering herd detector
pub struct ThunderingHerdDetector {
    /// Window of new device IDs (per second)
    new_devices_per_second: [u32; 60],
    
    /// Current second index
    current_second: usize,
    
    /// Last update timestamp
    last_update: Instant,
    
    /// Threshold for detection
    threshold: u32,
}

impl ThunderingHerdDetector {
    /// Create a new thundering herd detector
    pub fn new(threshold: u32) -> Self {
        Self {
            new_devices_per_second: [0; 60],
            current_second: 0,
            last_update: Instant::now(),
            threshold,
        }
    }

    /// Record a new device connection
    pub fn record_new_device(&mut self) {
        self.advance_time();
        self.new_devices_per_second[self.current_second] += 1;
    }

    /// Advance time if needed
    fn advance_time(&mut self) {
        let elapsed = self.last_update.elapsed().as_secs() as usize;
        if elapsed > 0 {
            for _ in 0..elapsed.min(60) {
                self.current_second = (self.current_second + 1) % 60;
                self.new_devices_per_second[self.current_second] = 0;
            }
            self.last_update = Instant::now();
        }
    }

    /// Check if thundering herd is detected
    pub fn is_thundering_herd(&mut self) -> bool {
        self.advance_time();
        self.new_devices_per_second[self.current_second] > self.threshold
    }

    /// Get current new devices count
    pub fn current_new_devices(&mut self) -> u32 {
        self.advance_time();
        self.new_devices_per_second[self.current_second]
    }

    /// Get new devices in last N seconds
    pub fn new_devices_in_window(&mut self, seconds: usize) -> u32 {
        self.advance_time();
        let seconds = seconds.min(60);
        let mut total = 0;
        for i in 0..seconds {
            let idx = (self.current_second + 60 - i) % 60;
            total += self.new_devices_per_second[idx];
        }
        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_device_profile_update() {
        let mut profile = DeviceProfile::new("dev-1".to_string());
        
        profile.update(100.0, 256, 1000);
        profile.update(110.0, 260, 2000);
        profile.update(105.0, 250, 3000);
        
        assert_eq!(profile.packet_count, 3);
        assert!(profile.avg_interval_ms > 0.0);
        assert!(profile.avg_payload_size > 0.0);
    }

    #[test]
    fn test_anomaly_detector() {
        let detector = AnomalyDetector::new(0.5, 1000);
        
        // Normal traffic
        for _ in 0..20 {
            detector.observe("dev-1", 256);
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        
        assert!(!detector.is_anomalous("dev-1"));
    }

    #[test]
    fn test_thundering_herd() {
        let mut detector = ThunderingHerdDetector::new(10);
        
        assert!(!detector.is_thundering_herd());
        
        for _ in 0..15 {
            detector.record_new_device();
        }
        
        assert!(detector.is_thundering_herd());
    }
}
