//! High-Performance Kafka Producer with Batching
//!
//! Uses FutureProducer with optimized settings for throughput.
//! 
//! This module has two implementations:
//! - Real Kafka (when compiled with --features kafka)
//! - Mock Kafka (default, for development/testing without librdkafka)

use crate::protocol::IotPacket;
use thiserror::Error;

/// Kafka producer errors
#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum KafkaError {
    #[error("Failed to create producer: {0}")]
    CreateError(String),
    #[error("Failed to send message: {0}")]
    SendError(String),
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
}

// ============================================================================
// Real Kafka Implementation (requires librdkafka)
// ============================================================================

#[cfg(feature = "kafka")]
mod real_kafka {
    use super::*;
    use rdkafka::config::ClientConfig;
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use rdkafka::util::Timeout;
    use std::sync::Arc;
    use std::time::Duration;

    /// High-performance Kafka producer wrapper
    #[derive(Clone)]
    pub struct KafkaProducer {
        producer: Arc<FutureProducer>,
        topic: String,
    }

    impl KafkaProducer {
        /// Create a new producer with optimized settings
        pub fn new(bootstrap_servers: &str, topic: &str) -> Result<Self, KafkaError> {
            let producer: FutureProducer = ClientConfig::new()
                // Connection
                .set("bootstrap.servers", bootstrap_servers)
                
                // Reliability
                .set("acks", "all")
                .set("retries", "3")
                .set("retry.backoff.ms", "100")
                
                // Performance: Batching
                .set("linger.ms", "50")
                .set("batch.size", "16384")          // 16KB batches
                .set("batch.num.messages", "1000")   // Or 1000 messages
                
                // Performance: Compression (disabled per spec, but ready)
                .set("compression.type", "none")
                
                // Performance: In-flight requests
                .set("max.in.flight.requests.per.connection", "5")
                
                // Performance: Buffer memory
                .set("queue.buffering.max.messages", "100000")
                .set("queue.buffering.max.kbytes", "1048576") // 1GB
                
                // Timeouts
                .set("message.timeout.ms", "30000")
                .set("request.timeout.ms", "5000")
                
                .create()
                .map_err(|e| KafkaError::CreateError(e.to_string()))?;

            Ok(Self {
                producer: Arc::new(producer),
                topic: topic.to_string(),
            })
        }

        /// Send a packet to Kafka asynchronously (non-blocking)
        pub async fn send(&self, packet: &IotPacket) -> Result<(), KafkaError> {
            let key = packet.kafka_key().to_string();
            let value = packet.to_kafka_value()?;

            let record = FutureRecord::to(&self.topic)
                .key(&key)
                .payload(&value);

            self.producer
                .send(record, Timeout::After(Duration::from_secs(5)))
                .await
                .map_err(|(e, _)| KafkaError::SendError(e.to_string()))?;

            Ok(())
        }

        /// Fire-and-forget send (for maximum throughput)
        pub fn send_fire_and_forget(&self, packet: IotPacket) {
            let producer = self.clone();
            tokio::spawn(async move {
                if let Err(e) = producer.send(&packet).await {
                    tracing::warn!("Kafka send failed: {}", e);
                }
            });
        }
    }
}

#[cfg(feature = "kafka")]
pub use real_kafka::KafkaProducer;

// ============================================================================
// Mock Kafka Implementation (for development/testing)
// ============================================================================

#[cfg(not(feature = "kafka"))]
mod mock_kafka {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    /// Mock Kafka producer that logs to console
    #[derive(Clone)]
    #[allow(dead_code)]
    pub struct KafkaProducer {
        topic: String,
        send_count: Arc<AtomicU64>,
    }

    impl KafkaProducer {
        /// Create a new mock producer
        pub fn new(bootstrap_servers: &str, topic: &str) -> Result<Self, KafkaError> {
            tracing::warn!(
                "⚠️  MOCK KAFKA MODE - Messages will NOT be sent to Kafka!"
            );
            tracing::warn!(
                "    Configured for: {} -> {}",
                bootstrap_servers,
                topic
            );
            tracing::warn!(
                "    To enable real Kafka, rebuild with: cargo build --features kafka"
            );

            Ok(Self {
                topic: topic.to_string(),
                send_count: Arc::new(AtomicU64::new(0)),
            })
        }

        /// Mock send - just counts messages
        pub async fn send(&self, packet: &IotPacket) -> Result<(), KafkaError> {
            let count = self.send_count.fetch_add(1, Ordering::Relaxed);
            
            // Log every 1000 messages
            if count % 1000 == 0 {
                tracing::debug!(
                    "Mock Kafka: {} total messages (latest: device={}, seq={})",
                    count + 1,
                    packet.device_id,
                    packet.sequence
                );
            }

            // Simulate small latency
            tokio::time::sleep(std::time::Duration::from_micros(10)).await;
            
            Ok(())
        }

        /// Fire-and-forget send
        pub fn send_fire_and_forget(&self, packet: IotPacket) {
            let producer = self.clone();
            tokio::spawn(async move {
                let _ = producer.send(&packet).await;
            });
        }

        /// Get total messages "sent"
        pub fn get_send_count(&self) -> u64 {
            self.send_count.load(Ordering::Relaxed)
        }
    }
}

#[cfg(not(feature = "kafka"))]
pub use mock_kafka::KafkaProducer;
