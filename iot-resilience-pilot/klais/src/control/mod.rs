//! Control Plane for KLAIS.
//!
//! Features:
//! - PID-based adaptive rate limiting
//! - CPU affinity and NUMA awareness
//! - Circuit breakers for resilience
//! - Latency tracing and histograms
//! - eBPF map management (when enabled)
//! - Self-healing and failover logic

pub mod pid;
pub mod numa;
pub mod circuit_breaker;
pub mod tracing;

use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::dam::DamFilter;
use crate::inference::{InferenceResult, RuleBasedInference};

/// Control plane configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlConfig {
    /// Target Kafka lag (messages)
    pub target_lag: u64,
    
    /// Target CPU utilization (0.0 - 1.0)
    pub target_cpu_util: f32,
    
    /// PID controller gains
    pub kp: f32,
    pub ki: f32,
    pub kd: f32,
    
    /// Control loop interval (ms)
    pub loop_interval_ms: u64,
    
    /// Minimum rate limit
    pub min_rate: u64,
    
    /// Maximum rate limit
    pub max_rate: u64,
}

impl Default for ControlConfig {
    fn default() -> Self {
        Self {
            target_lag: 1000,
            target_cpu_util: 0.7,
            kp: 0.5,
            ki: 0.1,
            kd: 0.05,
            loop_interval_ms: 100,
            min_rate: 100,
            max_rate: 100000,
        }
    }
}

/// Control plane state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlState {
    pub current_rate: u64,
    pub last_inference: InferenceResult,
    pub pid_output: f32,
    pub is_thundering_herd: bool,
}

impl Default for ControlState {
    fn default() -> Self {
        Self {
            current_rate: 500,
            last_inference: InferenceResult::default(),
            pid_output: 0.0,
            is_thundering_herd: false,
        }
    }
}

/// Control plane manager
pub struct ControlPlane {
    config: RwLock<ControlConfig>,
    dam: Arc<DamFilter>,
    inference: RuleBasedInference,
    state: RwLock<ControlState>,
    pid: RwLock<pid::PidController>,
}

impl ControlPlane {
    /// Create a new control plane
    pub fn new(config: ControlConfig, dam: Arc<DamFilter>) -> Self {
        let pid = pid::PidController::new(config.kp, config.ki, config.kd);
        
        Self {
            config: RwLock::new(config),
            dam,
            inference: RuleBasedInference::new(60, 500),
            state: RwLock::new(ControlState::default()),
            pid: RwLock::new(pid),
        }
    }

    /// Get current state
    pub fn state(&self) -> ControlState {
        self.state.read().clone()
    }

    /// Update with new observations
    pub fn observe(&self, packet_rate: f32, device_count: f32, entropy: f32) {
        self.inference.observe(packet_rate, device_count, entropy);
    }

    /// Run one control loop iteration
    pub fn tick(&self, current_lag: u64, current_cpu: f32) {
        let config = self.config.read();
        
        // Run inference
        let inference_result = self.inference.infer();
        
        // Calculate PID error
        let lag_error = (config.target_lag as f32 - current_lag as f32) 
                        / config.target_lag as f32;
        let cpu_error = config.target_cpu_util - current_cpu;
        let error = 0.7 * lag_error + 0.3 * cpu_error;
        
        // Get PID output
        let pid_output = self.pid.write().compute(error);
        
        // Determine new rate
        let base_rate = inference_result.recommended_limit;
        let adjusted_rate = (base_rate as f32 * (1.0 + pid_output)) as u64;
        let clamped_rate = adjusted_rate.clamp(config.min_rate, config.max_rate);
        
        // Apply special handling for burst prediction
        let final_rate = if inference_result.burst_probability > 0.8 {
            (clamped_rate as f32 * 0.5) as u64  // Reduce by 50% during predicted burst
        } else if inference_result.burst_probability > 0.5 {
            (clamped_rate as f32 * 0.75) as u64  // Reduce by 25%
        } else {
            clamped_rate
        };
        
        // Update dam filter
        self.dam.set_rate(final_rate);
        
        // Update state
        let mut state = self.state.write();
        state.current_rate = final_rate;
        state.last_inference = inference_result;
        state.pid_output = pid_output;
        state.is_thundering_herd = state.last_inference.burst_probability > 0.8;
        
        tracing::debug!(
            rate = final_rate,
            burst_prob = state.last_inference.burst_probability,
            pid = pid_output,
            "Control loop tick"
        );
    }

    /// Set new rate limit directly (for external control)
    pub fn set_rate(&self, rate: u64) {
        let config = self.config.read();
        let clamped = rate.clamp(config.min_rate, config.max_rate);
        self.dam.set_rate(clamped);
        self.state.write().current_rate = clamped;
    }

    /// Spawn the control loop task
    pub fn spawn_loop(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        let interval_ms = self.config.read().loop_interval_ms;
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
            
            loop {
                interval.tick().await;
                
                // In a real implementation, we'd get these from system metrics
                // For now, use defaults
                let current_lag = 0u64;
                let current_cpu = 0.5f32;
                
                self.tick(current_lag, current_cpu);
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dam::DamConfig;
    use tokio::sync::mpsc;

    #[test]
    fn test_control_plane_creation() {
        let (tx, _rx) = mpsc::channel(100);
        let dam = Arc::new(DamFilter::new(DamConfig::default(), tx));
        let control = ControlPlane::new(ControlConfig::default(), dam);
        
        let state = control.state();
        assert_eq!(state.current_rate, 500);
    }

    #[test]
    fn test_set_rate() {
        let (tx, _rx) = mpsc::channel(100);
        let dam = Arc::new(DamFilter::new(DamConfig::default(), tx));
        let control = ControlPlane::new(ControlConfig::default(), dam);
        
        control.set_rate(1000);
        assert_eq!(control.state().current_rate, 1000);
        
        // Test clamping
        control.set_rate(1000000);  // Over max
        assert_eq!(control.state().current_rate, 100000);  // Clamped to max
    }
}
