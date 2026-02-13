//! Kafka producer for KLAIS ingestion gateway.
//!
//! Features:
//! - Async batch producer using rdkafka
//! - Configurable batching and compression
//! - Error handling with retry logic
//! - Statistics and metrics

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio::sync::mpsc;

use crate::protocol::KlaisPacket;

/// Kafka producer statistics
#[derive(Debug, Default)]
pub struct KafkaStats {
    /// Messages sent successfully
    pub sent: AtomicU64,
    
    /// Messages failed to send
    pub failed: AtomicU64,
    
    /// Bytes sent
    pub bytes_sent: AtomicU64,
    
    /// Current in-flight messages
    pub in_flight: AtomicU64,
}

/// Kafka producer configuration
#[derive(Debug, Clone)]
pub struct KafkaConfig {
    /// Bootstrap servers
    pub bootstrap_servers: String,
    
    /// Topic name
    pub topic: String,
    
    /// Acknowledgement mode ("0", "1", "all")
    pub acks: String,
    
    /// Compression type ("none", "gzip", "snappy", "lz4", "zstd")
    pub compression: String,
    
    /// Linger time in milliseconds
    pub linger_ms: u32,
    
    /// Batch size in bytes
    pub batch_size: usize,
    
    /// Message timeout in milliseconds
    pub message_timeout_ms: u32,
    
    /// Queue buffering max messages
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

/// Kafka producer wrapper
pub struct KafkaProducerWrapper {
    producer: FutureProducer,
    topic: String,
    stats: Arc<KafkaStats>,
}

impl KafkaProducerWrapper {
    /// Create a new Kafka producer
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

        tracing::info!(
            bootstrap = %config.bootstrap_servers,
            topic = %config.topic,
            "Kafka producer created"
        );

        Ok(Self {
            producer,
            topic: config.topic,
            stats: Arc::new(KafkaStats::default()),
        })
    }

    /// Get statistics handle
    pub fn stats(&self) -> Arc<KafkaStats> {
        Arc::clone(&self.stats)
    }

    /// Send a single packet to Kafka
    pub async fn send(&self, packet: &KlaisPacket) -> Result<(), String> {
        self.stats.in_flight.fetch_add(1, Ordering::Relaxed);
        
        // Serialize to Kafka JSON format
        let payload = packet.to_kafka_json()
            .map_err(|e| format!("Serialization error: {}", e))?;
        
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
                self.stats.bytes_sent.fetch_add(payload.len() as u64, Ordering::Relaxed);
                self.stats.in_flight.fetch_sub(1, Ordering::Relaxed);
                Ok(())
            }
            Err((e, _)) => {
                self.stats.failed.fetch_add(1, Ordering::Relaxed);
                self.stats.in_flight.fetch_sub(1, Ordering::Relaxed);
                Err(format!("Kafka send error: {}", e))
            }
        }
    }

    /// Run consumer loop from channel
    pub async fn run_from_channel(&self, mut rx: mpsc::Receiver<KlaisPacket>) {
        tracing::info!("Kafka producer loop started");
        
        while let Some(packet) = rx.recv().await {
            if let Err(e) = self.send(&packet).await {
                tracing::error!(error = %e, device = %packet.device_id, "Failed to send to Kafka");
            }
        }
        
        tracing::info!("Kafka producer loop ended");
    }
}

/// Spawn the Kafka producer worker
pub fn spawn_producer(
    config: KafkaConfig,
    rx: mpsc::Receiver<KlaisPacket>,
) -> Result<tokio::task::JoinHandle<()>, rdkafka::error::KafkaError> {
    let producer = KafkaProducerWrapper::new(config)?;
    
    let handle = tokio::spawn(async move {
        producer.run_from_channel(rx).await;
    });
    
    Ok(handle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = KafkaConfig::default();
        assert_eq!(config.acks, "all");
        assert_eq!(config.linger_ms, 50);
        assert_eq!(config.batch_size, 16384);
    }
}
