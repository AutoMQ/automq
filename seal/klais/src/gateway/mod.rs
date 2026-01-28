//! Gateway module: UDP receiver and Kafka producer integration.

pub mod udp;
pub mod kafka;

use std::sync::Arc;
use tokio::sync::mpsc;

use crate::dam::{DamConfig, DamFilter};
use crate::protocol::KlaisPacket;

/// Gateway configuration
#[derive(Debug, Clone)]
pub struct GatewayConfig {
    /// UDP bind address
    pub udp_addr: String,
    
    /// Kafka bootstrap servers
    pub kafka_bootstrap: String,
    
    /// Kafka topic
    pub kafka_topic: String,
    
    /// Dam filter configuration
    pub dam_config: DamConfig,
    
    /// Channel buffer size
    pub channel_buffer_size: usize,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            udp_addr: crate::defaults::UDP_BIND_ADDR.to_string(),
            kafka_bootstrap: crate::defaults::KAFKA_BOOTSTRAP.to_string(),
            kafka_topic: crate::defaults::KAFKA_TOPIC.to_string(),
            dam_config: DamConfig::default(),
            channel_buffer_size: 10000,
        }
    }
}

/// Main gateway orchestrator
pub struct Gateway {
    config: GatewayConfig,
    dam: Arc<DamFilter>,
    packet_rx: mpsc::Receiver<KlaisPacket>,
}

impl Gateway {
    /// Create a new gateway with the given configuration
    pub fn new(config: GatewayConfig) -> Self {
        let (packet_tx, packet_rx) = mpsc::channel(config.channel_buffer_size);
        let dam = Arc::new(DamFilter::new(config.dam_config.clone(), packet_tx));
        
        Self {
            config,
            dam,
            packet_rx,
        }
    }

    /// Get reference to the dam filter
    pub fn dam(&self) -> Arc<DamFilter> {
        Arc::clone(&self.dam)
    }

    /// Get reference to config
    pub fn config(&self) -> &GatewayConfig {
        &self.config
    }

    /// Take the packet receiver for Kafka producer
    pub fn take_packet_rx(&mut self) -> mpsc::Receiver<KlaisPacket> {
        let (_, new_rx) = mpsc::channel(1);
        std::mem::replace(&mut self.packet_rx, new_rx)
    }
}
