//! LLM-based inference engine using GGML/llama.cpp.
//!
//! This module provides CPU-optimized LLM inference for:
//! - Traffic pattern analysis and prediction
//! - Anomaly classification with natural language explanations
//! - Intelligent rate limit suggestions
//! - Self-documenting diagnostics

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

/// LLM configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmConfig {
    /// Path to GGML model file
    pub model_path: String,
    
    /// Model type (llama, mistral, phi, etc.)
    pub model_type: ModelType,
    
    /// Number of threads for inference
    pub n_threads: usize,
    
    /// Context length
    pub context_length: usize,
    
    /// Maximum tokens to generate
    pub max_tokens: usize,
    
    /// Temperature for sampling
    pub temperature: f32,
    
    /// Top-p sampling
    pub top_p: f32,
    
    /// Batch size for processing
    pub batch_size: usize,
    
    /// Use GPU offloading (if available)
    pub gpu_layers: usize,
}

impl Default for LlmConfig {
    fn default() -> Self {
        Self {
            model_path: "models/llama-2-7b-chat.Q4_K_M.gguf".to_string(),
            model_type: ModelType::Llama,
            n_threads: num_cpus::get().min(8),
            context_length: 2048,
            max_tokens: 256,
            temperature: 0.1,  // Low for deterministic outputs
            top_p: 0.9,
            batch_size: 512,
            gpu_layers: 0,
        }
    }
}

/// Supported model types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelType {
    Llama,
    Llama2,
    Mistral,
    Phi,
    Qwen,
    Gemma,
}

/// LLM inference result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmInferenceResult {
    /// Generated text response
    pub response: String,
    
    /// Parsed structured output (if applicable)
    pub structured: Option<StructuredOutput>,
    
    /// Inference time in milliseconds
    pub inference_time_ms: u64,
    
    /// Tokens generated
    pub tokens_generated: usize,
    
    /// Tokens per second
    pub tokens_per_second: f32,
}

/// Structured output for traffic analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructuredOutput {
    /// Predicted traffic state
    pub traffic_state: TrafficState,
    
    /// Recommended action
    pub action: RecommendedAction,
    
    /// Confidence score (0.0 - 1.0)
    pub confidence: f32,
    
    /// Explanation
    pub explanation: String,
}

/// Traffic state classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TrafficState {
    Normal,
    Elevated,
    Burst,
    ThunderingHerd,
    Attack,
}

/// Recommended action
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RecommendedAction {
    /// Maintain current settings
    Maintain,
    
    /// Increase rate limit
    IncreaseRate { new_rate: u64 },
    
    /// Decrease rate limit
    DecreaseRate { new_rate: u64 },
    
    /// Activate burst mode
    ActivateBurstMode,
    
    /// Block specific devices
    BlockDevices { device_ids: Vec<String> },
    
    /// Alert operator
    Alert { severity: AlertSeverity, message: String },
}

/// Alert severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
}

/// Prompt templates for traffic analysis
pub mod prompts {
    pub const TRAFFIC_ANALYSIS: &str = r#"
You are KLAIS, an AI system that analyzes IoT traffic patterns on a Kafka-based data plane.

Current Metrics:
- Packet Rate: {packet_rate} msg/sec
- Unique Devices: {device_count}
- Queue Depth: {queue_depth}
- Drop Rate: {drop_rate}%
- Avg Latency: {latency_ms} ms

Historical Pattern (last 60 seconds):
{history}

Analyze this traffic pattern and provide:
1. Classification: NORMAL, ELEVATED, BURST, THUNDERING_HERD, or ATTACK
2. Recommended rate limit (current: {current_rate})
3. Confidence (0.0-1.0)
4. Brief explanation (one sentence)

Respond in JSON format:
{"classification": "...", "recommended_rate": ..., "confidence": ..., "explanation": "..."}
"#;

    pub const ANOMALY_CLASSIFICATION: &str = r#"
Device {device_id} is exhibiting anomalous behavior:
- Normal interval: {normal_interval_ms} ms (±{interval_stddev} ms)
- Current interval: {current_interval_ms} ms
- Payload size change: {size_change}%
- Anomaly score: {anomaly_score}

Is this:
A) Expected variation (e.g., network jitter)
B) Device malfunction
C) Possible attack/abuse
D) Thundering herd participant

Respond with letter and brief explanation.
"#;

    pub const SELF_DIAGNOSTIC: &str = r#"
KLAIS System Status:
- Uptime: {uptime}
- CPU Usage: {cpu_percent}%
- Memory: {memory_mb} MB
- Packets Processed: {total_packets}
- Current Throughput: {throughput} msg/sec
- Error Rate: {error_rate}%
- Kafka Lag: {kafka_lag}

Provide a one-paragraph health assessment and any recommendations.
"#;
}

/// LLM Engine (Stub implementation - would integrate with llama.cpp)
pub struct LlmEngine {
    config: LlmConfig,
    loaded: RwLock<bool>,
    stats: LlmStats,
}

/// LLM statistics
#[derive(Debug, Default)]
pub struct LlmStats {
    pub total_inferences: std::sync::atomic::AtomicU64,
    pub total_tokens: std::sync::atomic::AtomicU64,
    pub total_time_ms: std::sync::atomic::AtomicU64,
}

impl LlmEngine {
    /// Create a new LLM engine
    pub fn new(config: LlmConfig) -> Self {
        Self {
            config,
            loaded: RwLock::new(false),
            stats: LlmStats::default(),
        }
    }

    /// Load the model (would call llama.cpp initialization)
    pub fn load(&self) -> Result<(), LlmError> {
        let path = Path::new(&self.config.model_path);
        
        if !path.exists() {
            return Err(LlmError::ModelNotFound(self.config.model_path.clone()));
        }
        
        tracing::info!(
            model = %self.config.model_path,
            threads = self.config.n_threads,
            "Loading LLM model"
        );
        
        // In real implementation, this would:
        // 1. Initialize llama.cpp context
        // 2. Load GGML/GGUF weights
        // 3. Set up KV cache
        // 4. Warm up with test inference
        
        *self.loaded.write() = true;
        
        tracing::info!("LLM model loaded successfully");
        Ok(())
    }

    /// Check if model is loaded
    pub fn is_loaded(&self) -> bool {
        *self.loaded.read()
    }

    /// Run inference on a prompt
    pub fn infer(&self, prompt: &str) -> Result<LlmInferenceResult, LlmError> {
        if !self.is_loaded() {
            return Err(LlmError::ModelNotLoaded);
        }
        
        let start = Instant::now();
        
        // Stub implementation - would call llama.cpp
        let response = self.mock_inference(prompt);
        let tokens_generated = response.split_whitespace().count();
        
        let elapsed = start.elapsed();
        let inference_time_ms = elapsed.as_millis() as u64;
        let tokens_per_second = if elapsed.as_secs_f32() > 0.0 {
            tokens_generated as f32 / elapsed.as_secs_f32()
        } else {
            0.0
        };
        
        // Update stats
        use std::sync::atomic::Ordering;
        self.stats.total_inferences.fetch_add(1, Ordering::Relaxed);
        self.stats.total_tokens.fetch_add(tokens_generated as u64, Ordering::Relaxed);
        self.stats.total_time_ms.fetch_add(inference_time_ms, Ordering::Relaxed);
        
        Ok(LlmInferenceResult {
            response,
            structured: None,
            inference_time_ms,
            tokens_generated,
            tokens_per_second,
        })
    }

    /// Analyze traffic patterns
    pub fn analyze_traffic(&self, metrics: &TrafficMetrics) -> Result<StructuredOutput, LlmError> {
        let prompt = prompts::TRAFFIC_ANALYSIS
            .replace("{packet_rate}", &metrics.packet_rate.to_string())
            .replace("{device_count}", &metrics.device_count.to_string())
            .replace("{queue_depth}", &metrics.queue_depth.to_string())
            .replace("{drop_rate}", &format!("{:.1}", metrics.drop_rate))
            .replace("{latency_ms}", &format!("{:.1}", metrics.latency_ms))
            .replace("{current_rate}", &metrics.current_rate.to_string())
            .replace("{history}", &format_history(&metrics.history));
        
        let result = self.infer(&prompt)?;
        
        // Parse JSON response (stub - use actual JSON parsing)
        Ok(self.parse_traffic_response(&result.response, metrics))
    }

    /// Mock inference for testing
    fn mock_inference(&self, _prompt: &str) -> String {
        // Simulate inference latency
        std::thread::sleep(Duration::from_millis(10));
        
        r#"{"classification": "NORMAL", "recommended_rate": 500, "confidence": 0.85, "explanation": "Traffic patterns are within normal parameters."}"#.to_string()
    }

    /// Parse traffic analysis response
    fn parse_traffic_response(&self, response: &str, metrics: &TrafficMetrics) -> StructuredOutput {
        // Try to parse JSON response
        #[derive(Deserialize)]
        struct JsonResponse {
            classification: String,
            recommended_rate: u64,
            confidence: f32,
            explanation: String,
        }
        
        if let Ok(parsed) = serde_json::from_str::<JsonResponse>(response) {
            let traffic_state = match parsed.classification.as_str() {
                "ELEVATED" => TrafficState::Elevated,
                "BURST" => TrafficState::Burst,
                "THUNDERING_HERD" => TrafficState::ThunderingHerd,
                "ATTACK" => TrafficState::Attack,
                _ => TrafficState::Normal,
            };
            
            let action = if parsed.recommended_rate > metrics.current_rate {
                RecommendedAction::IncreaseRate { new_rate: parsed.recommended_rate }
            } else if parsed.recommended_rate < metrics.current_rate {
                RecommendedAction::DecreaseRate { new_rate: parsed.recommended_rate }
            } else {
                RecommendedAction::Maintain
            };
            
            StructuredOutput {
                traffic_state,
                action,
                confidence: parsed.confidence,
                explanation: parsed.explanation,
            }
        } else {
            // Fallback
            StructuredOutput {
                traffic_state: TrafficState::Normal,
                action: RecommendedAction::Maintain,
                confidence: 0.5,
                explanation: "Unable to parse LLM response, using defaults".to_string(),
            }
        }
    }

    /// Get configuration
    pub fn config(&self) -> &LlmConfig {
        &self.config
    }

    /// Get statistics
    pub fn stats(&self) -> &LlmStats {
        &self.stats
    }
}

/// Traffic metrics for analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrafficMetrics {
    pub packet_rate: u64,
    pub device_count: usize,
    pub queue_depth: usize,
    pub drop_rate: f32,
    pub latency_ms: f32,
    pub current_rate: u64,
    pub history: Vec<f32>,
}

/// LLM errors
#[derive(Debug, thiserror::Error)]
pub enum LlmError {
    #[error("Model not found: {0}")]
    ModelNotFound(String),
    
    #[error("Model not loaded")]
    ModelNotLoaded,
    
    #[error("Inference failed: {0}")]
    InferenceFailed(String),
    
    #[error("Invalid response: {0}")]
    InvalidResponse(String),
}

fn format_history(history: &[f32]) -> String {
    history
        .iter()
        .enumerate()
        .map(|(i, v)| format!("t-{}: {:.0}", history.len() - i, v))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Async LLM worker for non-blocking inference
pub struct LlmWorker {
    engine: Arc<LlmEngine>,
    request_rx: mpsc::Receiver<LlmRequest>,
    response_tx: mpsc::Sender<LlmResponse>,
}

/// LLM request
pub struct LlmRequest {
    pub id: u64,
    pub request_type: LlmRequestType,
}

/// Request types
pub enum LlmRequestType {
    TrafficAnalysis(TrafficMetrics),
    AnomalyClassification { device_id: String, details: String },
    CustomPrompt(String),
}

/// LLM response
pub struct LlmResponse {
    pub id: u64,
    pub result: Result<LlmInferenceResult, LlmError>,
}

impl LlmWorker {
    /// Create a new LLM worker
    pub fn new(
        engine: Arc<LlmEngine>,
        request_rx: mpsc::Receiver<LlmRequest>,
        response_tx: mpsc::Sender<LlmResponse>,
    ) -> Self {
        Self {
            engine,
            request_rx,
            response_tx,
        }
    }

    /// Run the worker loop
    pub async fn run(mut self) {
        while let Some(request) = self.request_rx.recv().await {
            let result = match request.request_type {
                LlmRequestType::TrafficAnalysis(metrics) => {
                    self.engine.analyze_traffic(&metrics)
                        .map(|s| LlmInferenceResult {
                            response: serde_json::to_string(&s).unwrap_or_default(),
                            structured: Some(s),
                            inference_time_ms: 0,
                            tokens_generated: 0,
                            tokens_per_second: 0.0,
                        })
                }
                LlmRequestType::CustomPrompt(prompt) => {
                    self.engine.infer(&prompt)
                }
                LlmRequestType::AnomalyClassification { device_id, details } => {
                    let prompt = prompts::ANOMALY_CLASSIFICATION
                        .replace("{device_id}", &device_id)
                        .replace("{details}", &details);
                    self.engine.infer(&prompt)
                }
            };
            
            let _ = self.response_tx.send(LlmResponse {
                id: request.id,
                result,
            }).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_llm_config_default() {
        let config = LlmConfig::default();
        assert!(config.n_threads > 0);
        assert!(config.temperature < 1.0);
    }

    #[test]
    fn test_traffic_metrics() {
        let metrics = TrafficMetrics {
            packet_rate: 1000,
            device_count: 50,
            queue_depth: 100,
            drop_rate: 0.1,
            latency_ms: 5.0,
            current_rate: 500,
            history: vec![100.0, 200.0, 500.0, 1000.0],
        };
        
        let history_str = format_history(&metrics.history);
        assert!(history_str.contains("t-1: 1000"));
    }

    #[test]
    fn test_structured_output_serialization() {
        let output = StructuredOutput {
            traffic_state: TrafficState::Normal,
            action: RecommendedAction::Maintain,
            confidence: 0.95,
            explanation: "All systems nominal".to_string(),
        };
        
        let json = serde_json::to_string(&output).unwrap();
        assert!(json.contains("Normal"));
    }
}
