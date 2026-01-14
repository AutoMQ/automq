//! KLAIS: Kernel-Level Adaptive Intelligence System
//!
//! A research-grade, kernel-integrated AI system for managing IoT data ingestion
//! at the Linux kernel layer using eBPF/XDP with sub-millisecond ML inference.
//!
//! ## Features
//!
//! - **High-Performance I/O**: io_uring, AF_XDP zero-copy
//! - **Kernel Integration**: eBPF/XDP packet filtering
//! - **AI Inference**: GGML-based LLM and rule-based heuristics
//! - **Adaptive Control**: PID controller with burst prediction
//! - **Observability**: Prometheus metrics, latency histograms
//! - **Resilience**: Circuit breakers, graceful degradation

pub mod protocol;
pub mod dam;
pub mod gateway;
pub mod inference;
pub mod control;
pub mod api;
pub mod metrics;
pub mod io;
pub mod ebpf;

pub use protocol::{KlaisPacket, parse_packet, ParseError};
pub use dam::{DamFilter, DamConfig, DamStats};
pub use gateway::{Gateway, GatewayConfig};

/// KLAIS version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Build information
pub mod build_info {
    /// Git commit hash (if available)
    pub const GIT_HASH: &str = option_env!("GIT_HASH").unwrap_or("unknown");
    
    /// Build timestamp
    pub const BUILD_TIME: &str = option_env!("BUILD_TIME").unwrap_or("unknown");
    
    /// Target triple
    pub const TARGET: &str = env!("TARGET");
    
    /// Rust version
    pub const RUSTC_VERSION: &str = option_env!("RUSTC_VERSION").unwrap_or("unknown");
}

/// Default configuration constants
pub mod defaults {
    /// Default UDP bind address
    pub const UDP_BIND_ADDR: &str = "0.0.0.0:5000";
    
    /// Default API bind address  
    pub const API_BIND_ADDR: &str = "127.0.0.1:8080";
    
    /// Default Kafka bootstrap servers
    pub const KAFKA_BOOTSTRAP: &str = "localhost:9092";
    
    /// Default Kafka topic
    pub const KAFKA_TOPIC: &str = "iot_telemetry";
    
    /// Default dam max rate (msgs/sec)
    pub const DAM_MAX_RATE: u64 = 500;
    
    /// Default dam burst size (tokens)
    pub const DAM_BURST_SIZE: u64 = 1000;
    
    /// Default dam queue limit
    pub const DAM_QUEUE_LIMIT: usize = 10000;
}

/// Prelude for convenient imports
pub mod prelude {
    pub use crate::protocol::{KlaisPacket, parse_packet};
    pub use crate::dam::{DamFilter, DamConfig, DamResult};
    pub use crate::control::{ControlPlane, ControlConfig};
    pub use crate::inference::{InferenceResult, RuleBasedInference};
}
