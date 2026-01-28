//! Benchmark for throughput testing.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use bytes::Bytes;

use klais::protocol::{parse_packet, KlaisPacket, fnv1a_hash};

fn make_test_packet(device_id: &str, seq: u32, payload: &str) -> Vec<u8> {
    let mut packet = vec![0xAA, 0x55];
    
    // Device ID (16 bytes, null-padded)
    let mut device_bytes = [0u8; 16];
    let id_bytes = device_id.as_bytes();
    device_bytes[..id_bytes.len().min(16)].copy_from_slice(&id_bytes[..id_bytes.len().min(16)]);
    packet.extend_from_slice(&device_bytes);
    
    // Sequence number (big-endian)
    packet.extend_from_slice(&seq.to_be_bytes());
    
    // Payload
    packet.extend_from_slice(payload.as_bytes());
    
    packet
}

fn benchmark_parse_packet(c: &mut Criterion) {
    let packet = make_test_packet("device-001", 12345, r#"{"temperature": 25.5, "humidity": 60.0}"#);
    
    let mut group = c.benchmark_group("protocol");
    group.throughput(Throughput::Elements(1));
    
    group.bench_function("parse_packet", |b| {
        b.iter(|| {
            let _ = black_box(parse_packet(black_box(&packet)));
        })
    });
    
    group.finish();
}

fn benchmark_hash(c: &mut Criterion) {
    let device_id = b"device-001000000";
    
    let mut group = c.benchmark_group("hash");
    group.throughput(Throughput::Elements(1));
    
    group.bench_function("fnv1a_hash", |b| {
        b.iter(|| {
            let _ = black_box(fnv1a_hash(black_box(device_id)));
        })
    });
    
    group.finish();
}

fn benchmark_kafka_serialize(c: &mut Criterion) {
    let packet_data = make_test_packet("device-001", 12345, r#"{"temp": 25.5}"#);
    let packet = parse_packet(&packet_data).unwrap();
    
    let mut group = c.benchmark_group("serialization");
    group.throughput(Throughput::Elements(1));
    
    group.bench_function("to_kafka_json", |b| {
        b.iter(|| {
            let _ = black_box(packet.to_kafka_json());
        })
    });
    
    group.finish();
}

criterion_group!(benches, benchmark_parse_packet, benchmark_hash, benchmark_kafka_serialize);
criterion_main!(benches);
