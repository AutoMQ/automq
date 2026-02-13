//! Circuit breaker pattern for resilience.
//!
//! Provides automatic failure detection and recovery:
//! - Tracks failure rates
//! - Opens circuit on excessive failures
//! - Half-open state for probing recovery
//! - Automatic reset on success

use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Circuit breaker state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum CircuitState {
    /// Circuit is closed, requests flow through
    Closed = 0,
    
    /// Circuit is open, requests fail fast
    Open = 1,
    
    /// Circuit is half-open, testing recovery
    HalfOpen = 2,
}

impl From<u8> for CircuitState {
    fn from(v: u8) -> Self {
        match v {
            0 => CircuitState::Closed,
            1 => CircuitState::Open,
            2 => CircuitState::HalfOpen,
            _ => CircuitState::Closed,
        }
    }
}

/// Circuit breaker configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    /// Failure threshold to open circuit
    pub failure_threshold: u32,
    
    /// Success threshold to close circuit from half-open
    pub success_threshold: u32,
    
    /// Time window for counting failures (seconds)
    pub failure_window_secs: u64,
    
    /// Time before attempting recovery (seconds)
    pub reset_timeout_secs: u64,
    
    /// Maximum concurrent half-open requests
    pub half_open_max_requests: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 3,
            failure_window_secs: 60,
            reset_timeout_secs: 30,
            half_open_max_requests: 3,
        }
    }
}

/// Circuit breaker statistics
#[derive(Debug, Default)]
pub struct CircuitBreakerStats {
    pub total_requests: AtomicU64,
    pub successful_requests: AtomicU64,
    pub failed_requests: AtomicU64,
    pub rejected_requests: AtomicU64,
    pub state_changes: AtomicU64,
}

/// Circuit breaker implementation
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    state: AtomicU8,
    failure_count: AtomicU64,
    success_count: AtomicU64,
    last_failure_time: RwLock<Option<Instant>>,
    last_state_change: RwLock<Instant>,
    half_open_requests: AtomicU64,
    stats: CircuitBreakerStats,
    name: String,
}

impl CircuitBreaker {
    /// Create a new circuit breaker
    pub fn new(name: impl Into<String>, config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            state: AtomicU8::new(CircuitState::Closed as u8),
            failure_count: AtomicU64::new(0),
            success_count: AtomicU64::new(0),
            last_failure_time: RwLock::new(None),
            last_state_change: RwLock::new(Instant::now()),
            half_open_requests: AtomicU64::new(0),
            stats: CircuitBreakerStats::default(),
            name: name.into(),
        }
    }

    /// Get current state
    pub fn state(&self) -> CircuitState {
        CircuitState::from(self.state.load(Ordering::Acquire))
    }

    /// Check if request should be allowed
    pub fn allow_request(&self) -> bool {
        self.stats.total_requests.fetch_add(1, Ordering::Relaxed);
        
        match self.state() {
            CircuitState::Closed => true,
            CircuitState::Open => {
                // Check if we should transition to half-open
                if self.should_attempt_reset() {
                    self.transition_to(CircuitState::HalfOpen);
                    true
                } else {
                    self.stats.rejected_requests.fetch_add(1, Ordering::Relaxed);
                    false
                }
            }
            CircuitState::HalfOpen => {
                // Limit concurrent half-open requests
                let current = self.half_open_requests.fetch_add(1, Ordering::AcqRel);
                if current < self.config.half_open_max_requests as u64 {
                    true
                } else {
                    self.half_open_requests.fetch_sub(1, Ordering::AcqRel);
                    self.stats.rejected_requests.fetch_add(1, Ordering::Relaxed);
                    false
                }
            }
        }
    }

    /// Record a successful request
    pub fn record_success(&self) {
        self.stats.successful_requests.fetch_add(1, Ordering::Relaxed);
        
        match self.state() {
            CircuitState::Closed => {
                // Reset failure count on success
                self.failure_count.store(0, Ordering::Relaxed);
            }
            CircuitState::HalfOpen => {
                self.half_open_requests.fetch_sub(1, Ordering::AcqRel);
                let successes = self.success_count.fetch_add(1, Ordering::AcqRel) + 1;
                
                if successes >= self.config.success_threshold as u64 {
                    self.transition_to(CircuitState::Closed);
                    tracing::info!(name = %self.name, "Circuit breaker closed");
                }
            }
            CircuitState::Open => {
                // Shouldn't happen, but handle gracefully
            }
        }
    }

    /// Record a failed request
    pub fn record_failure(&self) {
        self.stats.failed_requests.fetch_add(1, Ordering::Relaxed);
        *self.last_failure_time.write() = Some(Instant::now());
        
        match self.state() {
            CircuitState::Closed => {
                let failures = self.failure_count.fetch_add(1, Ordering::AcqRel) + 1;
                
                // Check if within window
                if self.is_within_failure_window() {
                    if failures >= self.config.failure_threshold as u64 {
                        self.transition_to(CircuitState::Open);
                        tracing::warn!(
                            name = %self.name,
                            failures = failures,
                            "Circuit breaker opened"
                        );
                    }
                } else {
                    // Reset counter for new window
                    self.failure_count.store(1, Ordering::Relaxed);
                }
            }
            CircuitState::HalfOpen => {
                self.half_open_requests.fetch_sub(1, Ordering::AcqRel);
                self.transition_to(CircuitState::Open);
                tracing::warn!(name = %self.name, "Circuit breaker reopened after probe failure");
            }
            CircuitState::Open => {
                // Already open
            }
        }
    }

    /// Check if within failure window
    fn is_within_failure_window(&self) -> bool {
        if let Some(last) = *self.last_failure_time.read() {
            last.elapsed() < Duration::from_secs(self.config.failure_window_secs)
        } else {
            true
        }
    }

    /// Check if we should attempt reset
    fn should_attempt_reset(&self) -> bool {
        let elapsed = self.last_state_change.read().elapsed();
        elapsed >= Duration::from_secs(self.config.reset_timeout_secs)
    }

    /// Transition to a new state
    fn transition_to(&self, new_state: CircuitState) {
        let old_state = self.state.swap(new_state as u8, Ordering::AcqRel);
        
        if old_state != new_state as u8 {
            *self.last_state_change.write() = Instant::now();
            self.stats.state_changes.fetch_add(1, Ordering::Relaxed);
            
            // Reset counters on state change
            match new_state {
                CircuitState::Closed => {
                    self.failure_count.store(0, Ordering::Relaxed);
                    self.success_count.store(0, Ordering::Relaxed);
                }
                CircuitState::HalfOpen => {
                    self.success_count.store(0, Ordering::Relaxed);
                    self.half_open_requests.store(0, Ordering::Relaxed);
                }
                CircuitState::Open => {
                    // Keep failure count for diagnostics
                }
            }
        }
    }

    /// Force circuit to specific state (for testing/admin)
    pub fn force_state(&self, state: CircuitState) {
        self.transition_to(state);
    }

    /// Get name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get configuration
    pub fn config(&self) -> &CircuitBreakerConfig {
        &self.config
    }

    /// Get statistics
    pub fn stats(&self) -> &CircuitBreakerStats {
        &self.stats
    }

    /// Get failure count
    pub fn failure_count(&self) -> u64 {
        self.failure_count.load(Ordering::Relaxed)
    }
}

/// Circuit breaker guard for automatic success/failure recording
pub struct CircuitGuard<'a> {
    breaker: &'a CircuitBreaker,
    recorded: bool,
}

impl<'a> CircuitGuard<'a> {
    /// Create a new guard
    pub fn new(breaker: &'a CircuitBreaker) -> Option<Self> {
        if breaker.allow_request() {
            Some(Self {
                breaker,
                recorded: false,
            })
        } else {
            None
        }
    }

    /// Mark as successful
    pub fn success(mut self) {
        self.breaker.record_success();
        self.recorded = true;
    }

    /// Mark as failed (also called on drop if not recorded)
    pub fn failure(mut self) {
        self.breaker.record_failure();
        self.recorded = true;
    }
}

impl Drop for CircuitGuard<'_> {
    fn drop(&mut self) {
        if !self.recorded {
            self.breaker.record_failure();
        }
    }
}

/// Registry of circuit breakers
#[derive(Default)]
pub struct CircuitBreakerRegistry {
    breakers: dashmap::DashMap<String, std::sync::Arc<CircuitBreaker>>,
}

impl CircuitBreakerRegistry {
    /// Create a new registry
    pub fn new() -> Self {
        Self::default()
    }

    /// Get or create a circuit breaker
    pub fn get_or_create(
        &self,
        name: &str,
        config: CircuitBreakerConfig,
    ) -> std::sync::Arc<CircuitBreaker> {
        self.breakers
            .entry(name.to_string())
            .or_insert_with(|| std::sync::Arc::new(CircuitBreaker::new(name, config)))
            .clone()
    }

    /// Get an existing circuit breaker
    pub fn get(&self, name: &str) -> Option<std::sync::Arc<CircuitBreaker>> {
        self.breakers.get(name).map(|r| r.clone())
    }

    /// Get all circuit breaker states
    pub fn all_states(&self) -> Vec<(String, CircuitState)> {
        self.breakers
            .iter()
            .map(|r| (r.key().clone(), r.value().state()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_state() {
        let cb = CircuitBreaker::new("test", CircuitBreakerConfig::default());
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.allow_request());
    }

    #[test]
    fn test_opens_on_failures() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            ..Default::default()
        };
        let cb = CircuitBreaker::new("test", config);
        
        for _ in 0..3 {
            cb.allow_request();
            cb.record_failure();
        }
        
        assert_eq!(cb.state(), CircuitState::Open);
    }

    #[test]
    fn test_rejects_when_open() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            reset_timeout_secs: 3600,  // Long timeout
            ..Default::default()
        };
        let cb = CircuitBreaker::new("test", config);
        
        cb.allow_request();
        cb.record_failure();
        
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(!cb.allow_request());
    }

    #[test]
    fn test_closes_on_success() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            success_threshold: 2,
            reset_timeout_secs: 0,  // Immediate reset
            ..Default::default()
        };
        let cb = CircuitBreaker::new("test", config);
        
        // Open the circuit
        cb.allow_request();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
        
        // Should transition to half-open
        std::thread::sleep(Duration::from_millis(10));
        assert!(cb.allow_request());
        assert_eq!(cb.state(), CircuitState::HalfOpen);
        
        // Two successes should close it
        cb.record_success();
        cb.allow_request();
        cb.record_success();
        
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_guard_auto_failure() {
        let cb = CircuitBreaker::new("test", CircuitBreakerConfig::default());
        
        {
            let _guard = CircuitGuard::new(&cb);
            // Guard dropped without recording
        }
        
        assert_eq!(cb.failure_count(), 1);
    }

    #[test]
    fn test_guard_explicit_success() {
        let cb = CircuitBreaker::new("test", CircuitBreakerConfig::default());
        
        if let Some(guard) = CircuitGuard::new(&cb) {
            guard.success();
        }
        
        assert_eq!(cb.failure_count(), 0);
    }
}
