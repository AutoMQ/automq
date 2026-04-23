
use klais::dam::{DamFilter, DamConfig, DamResult};
use klais::protocol::KlaisPacket;
use tokio::sync::mpsc;
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() {
    println!("Starting KLAIS Lightweight Performance Test...");

    // 1. Setup Dam Filter
    let (tx, _rx) = mpsc::channel(100_000); // Large channel
    let config = DamConfig {
        max_rate: 10_000_000, 
        burst_size: 1_000_000,
        queue_limit: 100_000,
        per_device_limiting: true,
        per_device_rate: 5_000,
    };
    let dam = Arc::new(DamFilter::new(config, tx));
    
    // 2. Prepare Data
    let packet = KlaisPacket::new(
        "device-bench-01",
        1,
        bytes::Bytes::from_static(b"{\"temp\": 20}")
    );
    
    // 3. Run Benchmark
    let start = Instant::now();
    let iterations = 1_000_000;
    
    println!("Running {} iterations...", iterations);
    
    for _ in 0..iterations {
        let _ = dam.try_pass(packet.clone()).await;
    }
    
    let duration = start.elapsed();
    let ops_per_sec = iterations as f64 / duration.as_secs_f64();
    
    println!("---------------------------------------------------");
    println!("test: dam_throughput");
    println!("iterations: {}", iterations);
    println!("duration: {:.2?}", duration);
    println!("throughput: {:.2} ops/sec", ops_per_sec);
    println!("---------------------------------------------------");
}
