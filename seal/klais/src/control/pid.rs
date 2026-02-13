//! PID Controller implementation.

use serde::{Deserialize, Serialize};

/// PID Controller for adaptive rate limiting
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PidController {
    /// Proportional gain
    kp: f32,
    
    /// Integral gain
    ki: f32,
    
    /// Derivative gain
    kd: f32,
    
    /// Accumulated integral term
    integral: f32,
    
    /// Last error for derivative calculation
    last_error: f32,
    
    /// Integral windup limit
    integral_limit: f32,
    
    /// Output clamp
    output_limit: f32,
}

impl PidController {
    /// Create a new PID controller
    pub fn new(kp: f32, ki: f32, kd: f32) -> Self {
        Self {
            kp,
            ki,
            kd,
            integral: 0.0,
            last_error: 0.0,
            integral_limit: 10.0,
            output_limit: 1.0,
        }
    }

    /// Compute PID output for given error
    pub fn compute(&mut self, error: f32) -> f32 {
        // Proportional term
        let p = self.kp * error;
        
        // Integral term with anti-windup
        self.integral += error;
        self.integral = self.integral.clamp(-self.integral_limit, self.integral_limit);
        let i = self.ki * self.integral;
        
        // Derivative term
        let d = self.kd * (error - self.last_error);
        self.last_error = error;
        
        // Sum and clamp
        let output = p + i + d;
        output.clamp(-self.output_limit, self.output_limit)
    }

    /// Reset controller state
    pub fn reset(&mut self) {
        self.integral = 0.0;
        self.last_error = 0.0;
    }

    /// Update gains
    pub fn set_gains(&mut self, kp: f32, ki: f32, kd: f32) {
        self.kp = kp;
        self.ki = ki;
        self.kd = kd;
    }

    /// Set integral limit
    pub fn set_integral_limit(&mut self, limit: f32) {
        self.integral_limit = limit;
    }

    /// Set output limit
    pub fn set_output_limit(&mut self, limit: f32) {
        self.output_limit = limit;
    }
}

impl Default for PidController {
    fn default() -> Self {
        Self::new(0.5, 0.1, 0.05)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pid_proportional() {
        let mut pid = PidController::new(1.0, 0.0, 0.0);
        
        assert_eq!(pid.compute(0.5), 0.5);
        assert_eq!(pid.compute(-0.5), -0.5);
    }

    #[test]
    fn test_pid_integral() {
        let mut pid = PidController::new(0.0, 0.1, 0.0);
        
        // Integral accumulates
        pid.compute(1.0);
        pid.compute(1.0);
        let output = pid.compute(1.0);
        
        assert!(output > 0.2);  // Should have accumulated
    }

    #[test]
    fn test_pid_derivative() {
        let mut pid = PidController::new(0.0, 0.0, 1.0);
        
        pid.compute(0.0);
        let output = pid.compute(0.5);  // Change of 0.5
        
        assert_eq!(output, 0.5);
    }

    #[test]
    fn test_pid_output_clamp() {
        let mut pid = PidController::new(10.0, 0.0, 0.0);
        
        // Large error should be clamped
        let output = pid.compute(1.0);
        assert_eq!(output, 1.0);  // Clamped to output_limit
    }

    #[test]
    fn test_pid_reset() {
        let mut pid = PidController::new(0.0, 1.0, 0.0);
        
        pid.compute(1.0);
        pid.compute(1.0);
        pid.reset();
        
        let output = pid.compute(0.0);
        assert_eq!(output, 0.0);  // Integral was reset
    }
}
