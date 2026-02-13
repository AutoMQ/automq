//! End-to-End Benchmark: Swarm, Dam, and Gateway Logic
//!
//! This benchmark simulates the full "Life of a Packet" to measure the overhead 
//! of the added resilience features (Dam Filter + KLAIS Swarm).
//!
//! Scenarios:
//! 1. **Steady State**: Traffic below limits.
//! 2. **Burst Event**: Traffic exceeding limits (Dam + Queue activation).
//! 3. **Swarm Rebalance**: Simulating neighbor lookup overhead.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use klais::dam::{DamFilter, DamConfig, DamResult};
use klais::protocol::{KlaisPacket, fnv1a_hash};
use tokio::sync::mpsc;
use std::sync::Arc;
use tokio::runtime::Runtime;

fn create_test_packet(seq: u32) -> KlaisPacket {
    KlaisPacket::new(
        format!("device-{:04}", seq % 1000), // Simulate 1000 devices
        seq,
        bytes::Bytes::from_static(b"{\"temp\": 25.5, \"load\": 0.9}")
    )
}

fn benchmark_dam_logic(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    // Setup Dam
    let (tx, _rx) = mpsc::channel(10000);
    let config = DamConfig {
        max_rate: 1_000_000, // High limit to test raw overhead
        burst_size: 1000,
        queue_limit: 10000,
        per_device_limiting: true,
        per_device_rate: 500,
    };
    let dam = Arc::new(DamFilter::new(config, tx));

    let mut group = c.benchmark_group("resilience_features");
    group.throughput(Throughput::Elements(1));

    // SCENARIO 1: Steady State Flow (Packet -> HashMap Lookup -> Token Check -> Pass)
    group.bench_function("steady_state_flow", |b| {
        b.to_async(&rt).iter(|| async {
            let packet = create_test_packet(1);
            let result = dam.try_pass(black_box(packet)).await;
            assert_eq!(result, DamResult::Passed);
        })
    });

    // SCENARIO 2: Swarm Decisioning (Hashing + Routing Logic Simulation)
    // We simulate the "Smart Swarm" overhead of checking neighbors
    group.bench_function("swarm_routing_logic", |b| {
        b.iter(|| {
            let device_id = black_box("device-001-critical");
            // Simulate: Hash(ID) -> Lookup Neighbor -> Decision
            let hash = fnv1a_hash(device_id.as_bytes());
            let neighbor_idx = hash % 5; // Simulate 5 neighbors
            
            // Artificial logic: If hash is even, route locally, else remote
            let _decision = if neighbor_idx % 2 == 0 { "local" } else { "remote" };
            black_box(_decision);
        })
    });

    group.finish();
}

criterion_group!(benches, benchmark_dam_logic);
criterion_main!(benches);
