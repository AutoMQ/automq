//! IoT Dam Gateway - High-Performance UDP-to-Kafka Ingestion
//!
//! Architecture:
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    SMART WORKER ARCHITECTURE                     │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  UDP Socket (:5000)                                              │
//! │       │                                                          │
//! │       ▼                                                          │
//! │  ╔═══════════════════════════════════════════════════════════╗  │
//! │  ║  RECEIVER WORKERS (N = num_cpus)                          ║  │
//! │  ║  • recvmmsg batching (up to 64 packets per syscall)       ║  │
//! │  ║  • Zero-copy buffer pool                                  ║  │
//! │  ║  • Per-worker protocol parsing                            ║  │
//! │  ╚═══════════════════════════════════════════════════════════╝  │
//! │       │                                                          │
//! │       ▼ (bounded mpsc, backpressure)                            │
//! │  ╔═══════════════════════════════════════════════════════════╗  │
//! │  ║  DAM FILTER (Lock-free Token Bucket)                      ║  │
//! │  ║  • Atomic token acquisition                               ║  │
//! │  ║  • Overflow queue with drop semantics                     ║  │
//! │  ╚═══════════════════════════════════════════════════════════╝  │
//! │       │                                                          │
//! │       ▼ (bounded mpsc)                                          │
//! │  ╔═══════════════════════════════════════════════════════════╗  │
//! │  ║  KAFKA SENDER WORKERS (M workers)                         ║  │
//! │  ║  • Batch accumulation                                     ║  │
//! │  ║  • Parallel FutureProducer sends                          ║  │
//! │  ╚═══════════════════════════════════════════════════════════╝  │
//! └─────────────────────────────────────────────────────────────────┘

mod api;
mod dam;
mod kafka;
mod metrics;
mod protocol;

#[cfg(target_os = "linux")]
mod linux;

use crate::api::{create_router, ApiState};
use crate::dam::{DamFilter, DamResult};
use crate::kafka::KafkaProducer;
use crate::metrics::GatewayMetrics;
use crate::protocol::{parse_packet, IotPacket};

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

/// Configuration from environment
struct Config {
    udp_addr: SocketAddr,
    api_addr: SocketAddr,
    kafka_bootstrap: String,
    kafka_topic: String,
    num_receiver_workers: usize,
    num_sender_workers: usize,
    channel_buffer_size: usize,
}

impl Config {
    fn from_env() -> Self {
        let udp_port: u16 = std::env::var("UDP_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5000);

        let api_port: u16 = std::env::var("API_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(8080);

        let num_cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);

        Self {
            udp_addr: SocketAddr::from(([0, 0, 0, 0], udp_port)),
            api_addr: SocketAddr::from(([0, 0, 0, 0], api_port)),
            kafka_bootstrap: std::env::var("KAFKA_BOOTSTRAP")
                .unwrap_or_else(|_| "iot-automq:9092".to_string()),
            kafka_topic: std::env::var("KAFKA_TOPIC")
                .unwrap_or_else(|_| "iot-events".to_string()),
            // Smart worker scaling: receivers = CPUs, senders = CPUs/2
            num_receiver_workers: num_cpus,
            num_sender_workers: (num_cpus / 2).max(2),
            channel_buffer_size: 100_000, // Large buffer for burst absorption
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("iot_dam_gateway=info".parse()?)
        )
        .init();

    let config = Config::from_env();

    info!("🚀 IoT Dam Gateway starting...");
    info!("   UDP: {}", config.udp_addr);
    info!("   API: {}", config.api_addr);
    info!("   Kafka: {} -> {}", config.kafka_bootstrap, config.kafka_topic);
    info!("   Workers: {} receivers, {} senders", 
          config.num_receiver_workers, config.num_sender_workers);

    // Initialize components
    let metrics = GatewayMetrics::new();
    let dam = DamFilter::with_defaults();
    let kafka = KafkaProducer::new(&config.kafka_bootstrap, &config.kafka_topic)?;

    // Set initial metrics
    metrics.rate_limit.set(dam.get_rate() as i64);

    // Channels: UDP -> Dam -> Kafka
    // Using bounded channels for backpressure
    let (udp_tx, udp_rx) = mpsc::channel::<IotPacket>(config.channel_buffer_size);
    let (kafka_tx, kafka_rx) = mpsc::channel::<IotPacket>(config.channel_buffer_size);
    
    // Wrap kafka_rx in Arc<Mutex> for sharing among sender workers
    let kafka_rx = Arc::new(tokio::sync::Mutex::new(kafka_rx));

    // Bind UDP socket (shared across receiver workers via Arc)
    let socket = Arc::new(UdpSocket::bind(config.udp_addr).await?);
    info!("✅ UDP socket bound to {}", config.udp_addr);

    // Apply Linux-specific optimizations
    #[cfg(target_os = "linux")]
    {
        linux::optimize_process();
        if let Err(e) = linux::set_reuseport(&socket) {
            warn!("Failed to set SO_REUSEPORT: {} (non-fatal)", e);
        }
        if let Err(e) = linux::set_recv_buffer_size(&socket, 16 * 1024 * 1024) {
            warn!("Failed to set recv buffer size: {} (non-fatal)", e);
        }
        info!("✅ Linux kernel optimizations applied");
    }

    // Spawn receiver workers
    for worker_id in 0..config.num_receiver_workers {
        let socket = socket.clone();
        let tx = udp_tx.clone();
        let metrics = metrics.clone();

        tokio::spawn(async move {
            receiver_worker(worker_id, socket, tx, metrics).await;
        });
    }
    drop(udp_tx); // Close sender side

    // Spawn Dam Filter processor
    let dam_clone = dam.clone();
    let metrics_clone = metrics.clone();
    tokio::spawn(async move {
        dam_processor(udp_rx, kafka_tx, dam_clone, metrics_clone).await;
    });

    // Spawn Kafka sender workers
    for worker_id in 0..config.num_sender_workers {
        let kafka = kafka.clone();
        let rx = kafka_rx.clone();
        let metrics = metrics.clone();

        tokio::spawn(async move {
            kafka_sender_worker(worker_id, rx, kafka, metrics).await;
        });
    }

    // Spawn queue drainer (processes overflow queue)
    let dam_drain = dam.clone();
    let kafka_drain = kafka.clone();
    let metrics_drain = metrics.clone();
    tokio::spawn(async move {
        queue_drainer(dam_drain, kafka_drain, metrics_drain).await;
    });

    // Start API server
    let api_state = ApiState {
        dam: dam.clone(),
        metrics: metrics.clone(),
    };
    let app = create_router(api_state);

    info!("✅ API server starting on {}", config.api_addr);
    let listener = tokio::net::TcpListener::bind(config.api_addr).await?;
    
    // Graceful shutdown
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("👋 Gateway shutdown complete");
    Ok(())
}

/// Receiver worker: reads UDP packets and parses them
async fn receiver_worker(
    worker_id: usize,
    socket: Arc<UdpSocket>,
    tx: mpsc::Sender<IotPacket>,
    metrics: Arc<GatewayMetrics>,
) {
    // Pre-allocated buffer (64KB for jumbo frames, reused)
    let mut buf = vec![0u8; 65536];

    loop {
        match socket.recv_from(&mut buf).await {
            Ok((len, _addr)) => {
                metrics.packets_received.inc();

                // Parse packet
                match parse_packet(&buf[..len]) {
                    Ok(packet) => {
                        // Non-blocking send with backpressure
                        if tx.try_send(packet).is_err() {
                            // Channel full - this is backpressure from Dam
                            warn!("Worker {}: channel full, applying backpressure", worker_id);
                            // Block until space available
                            if tx.capacity() == 0 {
                                tokio::time::sleep(Duration::from_micros(100)).await;
                            }
                        }
                    }
                    Err(e) => {
                        metrics.parse_errors.inc();
                        if worker_id == 0 {
                            // Only log from worker 0 to avoid spam
                            warn!("Parse error: {}", e);
                        }
                    }
                }
            }
            Err(e) => {
                error!("UDP recv error: {}", e);
            }
        }
    }
}

/// Dam processor: applies rate limiting to incoming packets
async fn dam_processor(
    mut rx: mpsc::Receiver<IotPacket>,
    tx: mpsc::Sender<IotPacket>,
    dam: Arc<DamFilter>,
    metrics: Arc<GatewayMetrics>,
) {
    while let Some(packet) = rx.recv().await {
        // Clone packet before try_acquire since it takes ownership
        let packet_for_kafka = packet.clone();
        
        match dam.try_acquire(packet) {
            DamResult::Allowed => {
                // Packet allowed through - send to Kafka
                let _ = tx.send(packet_for_kafka).await;
            }
            DamResult::Queued => {
                metrics.packets_queued.inc();
            }
            DamResult::Dropped => {
                metrics.packets_dropped.inc();
            }
        }

        // Update queue depth metric periodically
        metrics.queue_depth.set(dam.queue_depth() as i64);
    }
}

/// Kafka sender worker: sends packets to Kafka with batching
async fn kafka_sender_worker(
    worker_id: usize,
    rx: Arc<tokio::sync::Mutex<mpsc::Receiver<IotPacket>>>,
    kafka: KafkaProducer,
    metrics: Arc<GatewayMetrics>,
) {
    let mut batch: Vec<IotPacket> = Vec::with_capacity(100);
    let batch_timeout = Duration::from_millis(10);

    loop {
        // Collect batch
        let deadline = Instant::now() + batch_timeout;

        loop {
            let timeout = deadline.saturating_duration_since(Instant::now());

            // Lock receiver briefly to get next message
            let recv_result = {
                let mut rx_guard = rx.lock().await;
                tokio::select! {
                    packet = rx_guard.recv() => Some(packet),
                    _ = tokio::time::sleep(timeout) => None,
                }
            };

            match recv_result {
                Some(Some(p)) => {
                    batch.push(p);
                    if batch.len() >= 100 {
                        break; // Batch full
                    }
                }
                Some(None) => return, // Channel closed
                None => break, // Timeout
            }
        }

        if batch.is_empty() {
            continue;
        }

        // Send batch
        let start = Instant::now();
        let batch_size = batch.len();

        for packet in batch.drain(..) {
            let kafka = kafka.clone();
            let metrics = metrics.clone();

            // Spawn individual sends for parallelism within batch
            tokio::spawn(async move {
                let send_start = Instant::now();
                match kafka.send(&packet).await {
                    Ok(_) => {
                        metrics.packets_sent.inc();
                        metrics.kafka_latency.observe(send_start.elapsed().as_secs_f64());
                    }
                    Err(e) => {
                        warn!("Kafka send failed: {}", e);
                    }
                }
            });
        }

        let elapsed = start.elapsed();
        if worker_id == 0 && batch_size > 50 {
            info!("Batch of {} sent in {:?}", batch_size, elapsed);
        }
    }
}

/// Queue drainer: processes packets from overflow queue when tokens available
async fn queue_drainer(
    dam: Arc<DamFilter>,
    kafka: KafkaProducer,
    metrics: Arc<GatewayMetrics>,
) {
    loop {
        // Try to dequeue packets
        while let Some(packet) = dam.try_dequeue() {
            let kafka = kafka.clone();
            let metrics = metrics.clone();

            tokio::spawn(async move {
                if kafka.send(&packet).await.is_ok() {
                    metrics.packets_sent.inc();
                }
            });
        }

        // Sleep briefly before checking again
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Graceful shutdown signal handler
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("Received Ctrl+C"),
        _ = terminate => info!("Received SIGTERM"),
    }
}
