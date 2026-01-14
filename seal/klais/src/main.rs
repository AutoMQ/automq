//! KLAIS: Kernel-Level Adaptive Intelligence System
//!
//! High-performance IoT ingestion gateway with:
//! - UDP receiver (100k+ msg/sec)
//! - Token-bucket rate limiting (Dam Filter)
//! - Kafka producer with batching
//! - AI-controlled adaptive rate limiting
//! - REST API for monitoring and control

use std::sync::Arc;
use std::env;

use anyhow::Result;
use tokio::signal;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use klais::{
    dam::{DamConfig, DamFilter},
    gateway::{GatewayConfig, udp::{UdpConfig, UdpReceiver}, kafka::{KafkaConfig, spawn_producer}},
    control::{ControlConfig, ControlPlane},
    api,
    metrics,
};

/// Environment variable names
mod env_vars {
    pub const UDP_BIND: &str = "KLAIS_UDP_BIND";
    pub const API_BIND: &str = "KLAIS_API_BIND";
    pub const KAFKA_BOOTSTRAP: &str = "KLAIS_KAFKA_BOOTSTRAP";
    pub const KAFKA_TOPIC: &str = "KLAIS_KAFKA_TOPIC";
    pub const DAM_MAX_RATE: &str = "KLAIS_DAM_MAX_RATE";
    pub const DAM_BURST_SIZE: &str = "KLAIS_DAM_BURST_SIZE";
    pub const LOG_LEVEL: &str = "KLAIS_LOG_LEVEL";
}

/// Load configuration from environment
fn load_config() -> (UdpConfig, KafkaConfig, DamConfig, ControlConfig, String) {
    let udp_config = UdpConfig {
        bind_addr: env::var(env_vars::UDP_BIND)
            .unwrap_or_else(|_| klais::defaults::UDP_BIND_ADDR.to_string()),
        ..Default::default()
    };

    let kafka_config = KafkaConfig {
        bootstrap_servers: env::var(env_vars::KAFKA_BOOTSTRAP)
            .unwrap_or_else(|_| klais::defaults::KAFKA_BOOTSTRAP.to_string()),
        topic: env::var(env_vars::KAFKA_TOPIC)
            .unwrap_or_else(|_| klais::defaults::KAFKA_TOPIC.to_string()),
        ..Default::default()
    };

    let dam_config = DamConfig {
        max_rate: env::var(env_vars::DAM_MAX_RATE)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(klais::defaults::DAM_MAX_RATE),
        burst_size: env::var(env_vars::DAM_BURST_SIZE)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(klais::defaults::DAM_BURST_SIZE),
        ..Default::default()
    };

    let control_config = ControlConfig::default();

    let api_addr = env::var(env_vars::API_BIND)
        .unwrap_or_else(|_| klais::defaults::API_BIND_ADDR.to_string());

    (udp_config, kafka_config, dam_config, control_config, api_addr)
}

/// Initialize logging
fn init_logging() {
    let level = env::var(env_vars::LOG_LEVEL).unwrap_or_else(|_| "info".to_string());
    
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| format!("klais={},tower_http=debug", level).into()))
        .with(tracing_subscriber::fmt::layer())
        .init();
}

/// Shutdown signal handler
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("Shutdown signal received");
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize
    init_logging();
    metrics::init();
    
    tracing::info!(
        version = klais::VERSION,
        "Starting KLAIS - Kernel-Level Adaptive Intelligence System"
    );
    
    // Load configuration
    let (udp_config, kafka_config, dam_config, control_config, api_addr) = load_config();
    
    tracing::info!(
        udp_addr = %udp_config.bind_addr,
        kafka = %kafka_config.bootstrap_servers,
        topic = %kafka_config.topic,
        dam_rate = dam_config.max_rate,
        dam_burst = dam_config.burst_size,
        "Configuration loaded"
    );
    
    // Create message channel (UDP -> Kafka)
    let (packet_tx, packet_rx) = tokio::sync::mpsc::channel(dam_config.queue_limit);
    
    // Create Dam Filter
    let dam = Arc::new(DamFilter::new(dam_config, packet_tx));
    tracing::info!("Dam filter initialized");
    
    // Create Control Plane
    let control = Arc::new(ControlPlane::new(control_config, dam.clone()));
    tracing::info!("Control plane initialized");
    
    // Start Dam drain task
    let dam_clone = dam.clone();
    let dam_drain_handle = tokio::spawn(async move {
        Arc::clone(&dam_clone).spawn_drain_task().await
    });
    
    // Start Control loop
    let control_clone = control.clone();
    let control_handle = control_clone.spawn_loop();
    
    // Start Kafka producer
    let kafka_handle = match spawn_producer(kafka_config.clone(), packet_rx) {
        Ok(handle) => {
            tracing::info!("Kafka producer started");
            Some(handle)
        }
        Err(e) => {
            tracing::warn!(error = %e, "Failed to start Kafka producer (continuing without)");
            None
        }
    };
    
    // Start UDP receiver
    let udp_receiver = UdpReceiver::bind(udp_config, dam.clone()).await?;
    let udp_addr = udp_receiver.local_addr()?;
    tracing::info!(addr = %udp_addr, "UDP receiver bound");
    
    let udp_receiver = Arc::new(udp_receiver);
    let udp_clone = udp_receiver.clone();
    let udp_handle = tokio::spawn(async move {
        if let Err(e) = udp_clone.run().await {
            tracing::error!(error = %e, "UDP receiver error");
        }
    });
    
    // Start API server
    let dam_api = dam.clone();
    let control_api = control.clone();
    let api_handle = tokio::spawn(async move {
        if let Err(e) = api::run_server(&api_addr, dam_api, control_api).await {
            tracing::error!(error = %e, "API server error");
        }
    });
    
    tracing::info!("KLAIS started successfully - all components running");
    
    // Print startup summary
    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║  KLAIS - Kernel-Level Adaptive Intelligence System           ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║  Version: {:50} ║", klais::VERSION);
    println!("║  UDP:     {:50} ║", udp_addr);
    println!("║  API:     {:50} ║", api_addr);
    println!("║  Kafka:   {:50} ║", kafka_config.bootstrap_servers);
    println!("║  Topic:   {:50} ║", kafka_config.topic);
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║  Endpoints:                                                  ║");
    println!("║    GET  /health   - Health check                             ║");
    println!("║    POST /config   - Update rate limit                        ║");
    println!("║    GET  /stats    - Get statistics                           ║");
    println!("║    GET  /metrics  - Prometheus metrics                       ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");
    
    // Wait for shutdown signal
    shutdown_signal().await;
    
    // Cleanup
    tracing::info!("Shutting down...");
    
    udp_handle.abort();
    api_handle.abort();
    control_handle.abort();
    dam_drain_handle.abort();
    
    if let Some(handle) = kafka_handle {
        handle.abort();
    }
    
    tracing::info!("KLAIS shutdown complete");
    
    Ok(())
}
