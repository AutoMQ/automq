# KLAIS: Kernel-Level Adaptive Intelligence System

<div align="center">

[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org/)
[![Linux](https://img.shields.io/badge/linux-5.15%2B-green.svg)](https://kernel.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![eBPF](https://img.shields.io/badge/eBPF-XDP-purple.svg)](https://ebpf.io/)

**A Research-Grade Kernel-Level AI System for High-Performance IoT Data Plane Management**

[Architecture](#architecture) • [Quick Start](#quick-start) • [Documentation](#documentation) • [Benchmarks](#benchmarks) • [Research Paper](#research-paper)

</div>

---

## 💡 KLAIS in Simple Terms

If you don't know much about programming, think of **KLAIS** as a **Smart Gatekeeper** for a very busy factory (the factory is a data system like Kafka).

### The Three Parts of KLAIS:

1.  **🛡️ The Bouncer (eBPF):** Stands at the very front door. It checks every visitor (packet) in less than a microsecond. If a visitor doesn't have the right "Magic Key," they are kicked out immediately before they even step inside.
2.  **🚧 The Dam (Rate Limiter):** If too many visitors arrive at once (a "Thundering Herd"), the Dam holds them back in a line. It lets them into the factory at a steady pace so the factory doesn't crash from being overwhelmed.
3.  **🧠 The Brain (AI):** It watches the crowd. If it sees a pattern that looks like trouble (an "Anomaly") or predicts a huge crowd coming, it tells the Bouncer and the Dam to tighten the rules automatically.

**In short: KLAIS makes sure your data factory stays fast and safe, even when millions of devices are trying to talk to it at once.**

---

## 🎯 Overview

KLAIS (Kernel-Level Adaptive Intelligence System) is a **research-grade, high-performance ingestion gateway** for IoT data streams. It combines:

- **eBPF/XDP** for wire-speed packet filtering in kernel space
- **Rust** for memory-safe, zero-cost abstraction userspace processing
- **AI/ML inference** for adaptive rate limiting and anomaly detection
- **Kafka** for durable, scalable message streaming

### Why KLAIS?

Traditional IoT gateways suffer from:

| Problem | Traditional Approach | KLAIS Solution |
|---------|---------------------|----------------|
| **GIL Bottleneck** | Python asyncio (~50k msg/s) | Rust async (~500k+ msg/s) |
| **Userspace Overhead** | Full network stack traversal | XDP kernel bypass |
| **Static Limits** | Fixed rate limiting | AI-adaptive control |
| **Thundering Herd** | Overwhelmed on reconnect storms | Predictive mitigation |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              KERNEL SPACE                                   │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                        XDP LAYER (NIC Driver)                         │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                   │  │
│  │  │ Magic Check │→ │ Device Hash │→ │ Rate Check  │→ XDP_PASS/DROP    │  │
│  │  │ (0xAA55)    │  │ (FNV-1a)    │  │ (Token Map) │                   │  │
│  │  └─────────────┘  └─────────────┘  └─────────────┘                   │  │
│  │         ↓ BPF_MAP_TYPE_RINGBUF (zero-copy to userspace)              │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────────────────────────────┤
│                              USER SPACE                                     │
│  ┌─────────────────────┐  ┌─────────────────────┐  ┌──────────────────┐    │
│  │    UDP Receiver     │  │    Dam Filter       │  │  Kafka Producer  │    │
│  │  (tokio/io_uring)   │→ │  (Token Bucket +    │→ │  (rdkafka +      │→ ● │
│  │                     │  │   Overflow Queue)   │  │   Batching)      │    │
│  └─────────────────────┘  └─────────┬───────────┘  └──────────────────┘    │
│                                     │                                       │
│  ┌─────────────────────────────────▼───────────────────────────────────┐   │
│  │                       CONTROL PLANE                                  │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌────────────┐  │   │
│  │  │ PID Controller │ │ LLM Inference │ │ Anomaly     │ │ Circuit    │  │   │
│  │  │ (Adaptive Rate)│ │ (Traffic Pred)│ │ Detection   │ │ Breakers   │  │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └────────────┘  │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                       REST API (:8080)                               │   │
│  │  /health • /config • /stats • /metrics                               │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Component Overview

| Layer | Component | Technology | Purpose |
|-------|-----------|------------|---------|
| **Kernel** | XDP Filter | eBPF/C | Wire-speed filtering, early drop |
| **I/O** | UDP Receiver | tokio/io_uring | High-throughput packet reception |
| **I/O** | AF_XDP | XDP Sockets | Zero-copy NIC-to-userspace |
| **Rate Limiting** | Dam Filter | Rust | Token bucket + overflow queue |
| **Messaging** | Kafka Producer | rdkafka | Batched, compressed delivery |
| **AI** | LLM Engine | GGML/ONNX | Traffic prediction, classification |
| **AI** | Anomaly Detector | Rust | Device fingerprinting, z-score |
| **Control** | PID Controller | Rust | Adaptive rate adjustment |
| **Control** | NUMA Manager | Rust | CPU affinity, memory locality |
| **Resilience** | Circuit Breaker | Rust | Failure isolation, recovery |
| **Observability** | Metrics | Prometheus | Counters, histograms, gauges |

---

## 🚀 Quick Start

### Prerequisites

- **Rust 1.75+**
- **Linux 5.15+** (for eBPF features)
- **librdkafka** (Kafka client library)
- **clang** (for eBPF compilation, optional)

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/klais.git
cd klais

# Build release binary
cargo build --release

# Run with defaults
./target/release/klais
```

### Docker Deployment

```bash
# Full stack: KLAIS + Kafka + Prometheus + Grafana
docker-compose up -d

# View logs
docker logs -f klais-gateway
```

### Configuration

All configuration via environment variables:

```bash
# Core settings
export KLAIS_UDP_BIND=0.0.0.0:5000        # UDP listen address
export KLAIS_API_BIND=127.0.0.1:8080      # REST API address
export KLAIS_KAFKA_BOOTSTRAP=kafka:9092   # Kafka brokers
export KLAIS_KAFKA_TOPIC=iot_telemetry    # Target topic

# Rate limiting
export KLAIS_DAM_MAX_RATE=10000           # Max sustained rate
export KLAIS_DAM_BURST_SIZE=50000         # Burst capacity

# Logging
export KLAIS_LOG_LEVEL=info               # Log verbosity
export RUST_BACKTRACE=1                   # Stack traces on panic
```

---

## 📊 Wire Protocol

Each UDP datagram follows this binary format:

```
┌────────────────────────────────────────────────────────────────┐
│ Offset │ Size │ Field      │ Type       │ Description          │
├────────┼──────┼────────────┼────────────┼──────────────────────┤
│ 0      │ 2    │ magic      │ u16 BE     │ 0xAA55               │
│ 2      │ 16   │ device_id  │ [u8; 16]   │ UTF-8, null-padded   │
│ 18     │ 4    │ sequence   │ u32 BE     │ Monotonic counter    │
│ 22     │ var  │ payload    │ JSON       │ Application data     │
└────────────────────────────────────────────────────────────────┘
```

### Example Packet (Python)

```python
import socket
import struct
import json

def send_packet(device_id: str, seq: int, data: dict):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    # Build packet
    packet = bytes([0xAA, 0x55])                          # Magic
    packet += device_id.encode().ljust(16, b'\x00')       # Device ID
    packet += struct.pack('>I', seq)                      # Sequence (BE)
    packet += json.dumps(data).encode()                   # Payload
    
    sock.sendto(packet, ('localhost', 5000))

# Usage
send_packet("sensor-001", 42, {"temperature": 25.5, "humidity": 60})
```

---

## 🔧 API Reference

### Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check with system status |
| `/config` | POST | Update runtime configuration |
| `/stats` | GET | Comprehensive statistics |
| `/metrics` | GET | Prometheus-format metrics |

### Health Check

```bash
curl http://localhost:8080/health
```

```json
{
  "status": "ok",
  "queue_depth": 42,
  "rate_limit": 10000,
  "version": "0.1.0"
}
```

### Update Rate Limit

```bash
curl -X POST http://localhost:8080/config \
  -H "Content-Type: application/json" \
  -d '{"new_rate": 20000}'
```

### Statistics

```bash
curl http://localhost:8080/stats
```

```json
{
  "dam": {
    "received": 1000000,
    "passed": 998000,
    "queued": 1500,
    "dropped": 500,
    "queue_depth": 150,
    "tokens": 8500
  },
  "control": {
    "current_rate": 10000,
    "burst_probability": 0.15,
    "anomaly_score": 0.02,
    "pid_output": 0.05
  }
}
```

---

## 📈 Benchmarks

### Throughput (Single Core)

| Configuration | Throughput | P99 Latency |
|--------------|------------|-------------|
| Tokio UDP (baseline) | 450k msg/s | 120μs |
| + Dam Filter | 420k msg/s | 150μs |
| + Kafka Producer | 380k msg/s | 200μs |
| + io_uring | 520k msg/s | 80μs |
| + AF_XDP | 650k+ msg/s | 40μs |

### Memory Usage

| Load | RSS | Heap |
|------|-----|------|
| Idle | 45 MB | 12 MB |
| 100k msg/s | 120 MB | 45 MB |
| 500k msg/s | 280 MB | 110 MB |

### Thundering Herd Recovery

| Scenario | Recovery Time | Packet Loss |
|----------|---------------|-------------|
| 1k devices reconnect | 1.2s | 0.1% |
| 10k devices reconnect | 3.5s | 0.8% |
| 100k devices reconnect | 12s | 2.1% |

---

## 📚 Documentation

### Project Structure

```
klais/
├── src/
│   ├── lib.rs              # Library root
│   ├── main.rs             # Async entrypoint
│   ├── protocol.rs         # Wire format parser
│   ├── dam.rs              # Rate limiter
│   ├── metrics.rs          # Prometheus integration
│   │
│   ├── gateway/            # Data plane
│   │   ├── udp.rs          # UDP receiver
│   │   └── kafka.rs        # Kafka producer
│   │
│   ├── inference/          # AI subsystem
│   │   ├── llm.rs          # LLM inference
│   │   ├── features.rs     # Feature extraction
│   │   └── anomaly.rs      # Anomaly detection
│   │
│   ├── control/            # Control plane
│   │   ├── pid.rs          # PID controller
│   │   ├── numa.rs         # NUMA/affinity
│   │   ├── circuit_breaker.rs  # Resilience
│   │   └── tracing.rs      # Histograms
│   │
│   ├── io/                 # High-perf I/O
│   │   ├── uring.rs        # io_uring
│   │   └── xdp.rs          # AF_XDP
│   │
│   └── ebpf/               # Kernel programs
│       └── loader.rs       # eBPF loader
│
├── bpf/                    # eBPF C sources
│   ├── xdp_filter.c        # XDP program
│   └── klais.h             # Shared types
│
└── docker/                 # Deployment
    └── prometheus.yml      # Metrics config
```

### Feature Flags

| Flag | Description |
|------|-------------|
| `default` | Standard build (tokio, rdkafka) |
| `ebpf` | Enable eBPF/XDP support |
| `ml` | Enable ONNX Runtime inference |
| `full` | All features |

```bash
cargo build --release --features full
```

---

## 📄 Research Paper

See [docs/RESEARCH_PAPER.md](docs/RESEARCH_PAPER.md) for the full academic paper:

> **KLAIS: Kernel-Level Adaptive Intelligence for IoT Data Plane Management**
>
> *Abstract*: We present KLAIS, a novel kernel-integrated AI system that achieves 500k+ msg/sec throughput with sub-100μs latency by combining eBPF/XDP packet filtering with ML-based adaptive rate limiting. Our evaluation demonstrates 10x improvement over Python-based gateways and 3x improvement in thundering herd recovery time.

---

## 🤝 Contributing

Contributions welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Development

```bash
# Run tests
cargo test

# Run benchmarks
cargo bench

# Format code
cargo fmt

# Lint
cargo clippy
```

---

## 📜 License

MIT License - see [LICENSE](LICENSE) for details.

---

## 🙏 Acknowledgments

- [eBPF](https://ebpf.io/) - Revolutionary kernel technology
- [tokio](https://tokio.rs/) - Async Rust runtime
- [rdkafka](https://github.com/fede1024/rust-rdkafka) - Kafka bindings
- [axum](https://github.com/tokio-rs/axum) - Web framework
- [AutoMQ](https://www.automq.com/) - Inspiration for S3-native Kafka

---

<div align="center">

**Built with ❤️ for high-performance IoT**

</div>
