//! Kafka producer for KLAIS ingestion gateway.
//! 
//! Features:
//! - Async batch producer using rdkafka (Feature: "kafka")
//! - Mock mode for development (Feature: "kafka" disabled)

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use crate::protocol::KlaisPacket;

/// Kafka producer statistics
#[derive(Debug, Default)]
pub struct KafkaStats {
    pub sent: AtomicU64,
    pub failed: AtomicU64,
    pub bytes_sent: AtomicU64,
    pub in_flight: AtomicU64,
}

/// Kafka producer configuration
#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    pub topic: String,
    pub acks: String,
    pub compression: String,
    pub linger_ms: u32,
    pub batch_size: usize,
    pub message_timeout_ms: u32,
    pub queue_buffering_max_messages: u32,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: crate::defaults::KAFKA_BOOTSTRAP.to_string(),
            topic: crate::defaults::KAFKA_TOPIC.to_string(),
            acks: "all".to_string(),
            compression: "none".to_string(),
            linger_ms: 50,
            batch_size: 16384,
            message_timeout_ms: 30000,
            queue_buffering_max_messages: 100000,
        }
    }
}

// ============================================================================
// Real Kafka Implementation
// ============================================================================
#[cfg(feature = "kafka")]
pub mod implementation {
    use super::*;
    use rdkafka::config::ClientConfig;
    use rdkafka::message::OwnedHeaders;
    use rdkafka::producer::{FutureProducer, FutureRecord};

    pub struct KafkaProducerWrapper {
        producer: FutureProducer,
        topic: String,
        stats: Arc<KafkaStats>,
    }

    impl KafkaProducerWrapper {
        pub fn new(config: KafkaConfig) -> Result<Self, rdkafka::error::KafkaError> {
            let producer: FutureProducer = ClientConfig::new()
                .set("bootstrap.servers", &config.bootstrap_servers)
                .set("acks", &config.acks)
                .set("compression.type", &config.compression)
                .set("linger.ms", config.linger_ms.to_string())
                .set("batch.size", config.batch_size.to_string())
                .set("message.timeout.ms", config.message_timeout_ms.to_string())
                .set("queue.buffering.max.messages", config.queue_buffering_max_messages.to_string())
                .create()?;

            Ok(Self {
                producer,
                topic: config.topic,
                stats: Arc::new(KafkaStats::default()),
            })
        }

        pub fn stats(&self) -> Arc<KafkaStats> {
            Arc::clone(&self.stats)
        }

        pub async fn send(&self, packet: &KlaisPacket) -> Result<(), String> {
            self.stats.in_flight.fetch_add(1, Ordering::Relaxed);
            
            let payload = packet.to_kafka_json().map_err(|e| format!("{}", e))?;
            let key = packet.device_id_bytes();
            
            let record = FutureRecord::to(&self.topic)
                .key(key)
                .payload(&payload)
                .headers(OwnedHeaders::new()
                    .insert(rdkafka::message::Header {
                        key: "seq",
                        value: Some(&packet.sequence.to_be_bytes()),
                    }));

            match self.producer.send(record, Duration::from_secs(5)).await {
                Ok(_) => {
                    self.stats.sent.fetch_add(1, Ordering::Relaxed);
                    self.stats.in_flight.fetch_sub(1, Ordering::Relaxed);
                    Ok(())
                }
                Err((e, _)) => {
                    self.stats.failed.fetch_add(1, Ordering::Relaxed);
                    self.stats.in_flight.fetch_sub(1, Ordering::Relaxed);
                    Err(format!("{}", e))
                }
            }
        }
    }
}

// ============================================================================
// Mock Kafka Implementation
// ============================================================================
#[cfg(not(feature = "kafka"))]
pub mod implementation {
    use super::*;

    pub struct KafkaProducerWrapper {
        stats: Arc<KafkaStats>,
    }

    impl KafkaProducerWrapper {
        pub fn new(_config: KafkaConfig) -> Result<Self, String> {
            tracing::warn!("⚠️  Using MOCK Kafka Producer (No real messages sent)");
            Ok(Self {
                stats: Arc::new(KafkaStats::default()),
            })
        }

        pub fn stats(&self) -> Arc<KafkaStats> {
            Arc::clone(&self.stats)
        }

        pub async fn send(&self, _packet: &KlaisPacket) -> Result<(), String> {
            self.stats.in_flight.fetch_add(1, Ordering::Relaxed);
            // Simulate I/O
            tokio::time::sleep(Duration::from_millis(1)).await;
            self.stats.sent.fetch_add(1, Ordering::Relaxed);
            self.stats.in_flight.fetch_sub(1, Ordering::Relaxed);
            Ok(())
        }
    }
}

// Re-export specific implementation
pub use implementation::KafkaProducerWrapper;

impl KafkaProducerWrapper {
    pub async fn run_from_channel(&self, mut rx: mpsc::Receiver<KlaisPacket>) {
        tracing::info!("Kafka producer loop started");
        while let Some(packet) = rx.recv().await {
            if let Err(e) = self.send(&packet).await {
                tracing::error!(error = %e, "Failed to send");
            }
        }
    }
}

pub fn spawn_producer(
    config: KafkaConfig,
    rx: mpsc::Receiver<KlaisPacket>,
) -> Result<tokio::task::JoinHandle<()>, String> {
    // Map implementation error to String for unified return type
    let producer = KafkaProducerWrapper::new(config).map_err(|e| e.to_string())?;
    
    let handle = tokio::spawn(async move {
        producer.run_from_channel(rx).await;
    });
    
    Ok(handle)
}
