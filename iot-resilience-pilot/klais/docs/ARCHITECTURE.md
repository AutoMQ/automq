# KLAIS Architecture Document

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Kernel Space Components](#2-kernel-space-components)
3. [User Space Components](#3-user-space-components)
4. [Data Flow](#4-data-flow)
5. [Control Flow](#5-control-flow)
6. [Failure Modes](#6-failure-modes)
7. [Scalability](#7-scalability)
8. [Security](#8-security)

---

## 1. System Overview

### 1.1 High-Level Architecture

```
                                    ┌─────────────────────────────────────┐
                                    │            KLAIS SYSTEM             │
                                    └─────────────────────────────────────┘
                                                      │
         ┌────────────────────────────────────────────┼────────────────────────────────────────────┐
         │                                            │                                            │
         ▼                                            ▼                                            ▼
┌─────────────────┐                        ┌─────────────────┐                        ┌─────────────────┐
│   DATA PLANE    │                        │  CONTROL PLANE  │                        │   MANAGEMENT    │
├─────────────────┤                        ├─────────────────┤                        ├─────────────────┤
│ • XDP Filter    │                        │ • PID Controller│                        │ • REST API      │
│ • UDP Receiver  │                        │ • LLM Inference │                        │ • Prometheus    │
│ • Dam Filter    │◄──────────────────────►│ • Anomaly Det.  │◄──────────────────────►│ • Health Check  │
│ • Kafka Producer│                        │ • Circuit Break │                        │ • Config Update │
└─────────────────┘                        └─────────────────┘                        └─────────────────┘
         │                                            │                                            │
         └────────────────────────────────────────────┼────────────────────────────────────────────┘
                                                      │
                                                      ▼
                                           ┌─────────────────┐
                                           │     STORAGE     │
                                           ├─────────────────┤
                                           │ • Kafka/AutoMQ  │
                                           │ • S3 (MinIO)    │
                                           └─────────────────┘
```

### 1.2 Design Principles

| Principle | Implementation |
|-----------|----------------|
| **Kernel bypass where possible** | XDP for packet filtering, AF_XDP for zero-copy |
| **Batch processing** | io_uring submission batching, Kafka producer batching |
| **Lock-free data structures** | Atomic counters, crossbeam queues |
| **NUMA awareness** | Thread pinning, local memory allocation |
| **Graceful degradation** | Circuit breakers, overflow queues |

### 1.3 Deployment Topology

```
                              ┌─────────────────────────────────────────┐
                              │           LOAD BALANCER (L4)            │
                              │           (UDP, Round Robin)            │
                              └───────────────────┬─────────────────────┘
                                                  │
                    ┌─────────────────────────────┼─────────────────────────────┐
                    │                             │                             │
                    ▼                             ▼                             ▼
           ┌────────────────┐            ┌────────────────┐            ┌────────────────┐
           │   KLAIS #1     │            │   KLAIS #2     │            │   KLAIS #3     │
           │   (Node 1)     │            │   (Node 2)     │            │   (Node 3)     │
           └───────┬────────┘            └───────┬────────┘            └───────┬────────┘
                   │                             │                             │
                   └─────────────────────────────┼─────────────────────────────┘
                                                 │
                                                 ▼
                              ┌─────────────────────────────────────────┐
                              │           KAFKA CLUSTER                 │
                              │   (3+ brokers, AutoMQ for S3 offload)   │
                              └─────────────────────────────────────────┘
```

---

## 2. Kernel Space Components

### 2.1 XDP Program

**Location**: `bpf/xdp_filter.c`

```
┌─────────────────────────────────────────────────────────────────────┐
│                        XDP PROCESSING PIPELINE                       │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │   BOUNDS CHECK      │
                         │   len >= 22 bytes   │
                         └──────────┬──────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    │ PASS                    FAIL  │
                    ▼                               ▼
         ┌─────────────────────┐          ┌─────────────────┐
         │   MAGIC CHECK       │          │   XDP_DROP      │
         │   0xAA55            │          │   stats.size++  │
         └──────────┬──────────┘          └─────────────────┘
                    │
    ┌───────────────┴───────────────┐
    │ PASS                    FAIL  │
    ▼                               ▼
┌─────────────────────┐   ┌─────────────────┐
│   DEVICE HASH       │   │   XDP_DROP      │
│   fnv1a(device_id)  │   │   stats.magic++ │
└──────────┬──────────┘   └─────────────────┘
           │
           ▼
┌─────────────────────┐
│   RATE LIMIT CHECK  │
│   token_bucket[hash]│
└──────────┬──────────┘
           │
    ┌──────┴──────┐
    │ PASS   FAIL │
    ▼             ▼
┌───────────┐ ┌───────────┐
│ RINGBUF   │ │ XDP_DROP  │
│ SUBMIT    │ │ stats.rate│
└─────┬─────┘ └───────────┘
      │
      ▼
┌───────────┐
│ XDP_PASS  │
└───────────┘
```

### 2.2 BPF Maps

| Map Name | Type | Key | Value | Purpose |
|----------|------|-----|-------|---------|
| `device_limits` | HASH | u32 (hash) | token_bucket | Per-device rate limiting |
| `statistics` | PERCPU_ARRAY | u32 | stats | Lock-free counters |
| `events` | RINGBUF | - | klais_event | Zero-copy to userspace |
| `ai_params` | ARRAY | u32 | ai_config | ML-updated thresholds |

### 2.3 Token Bucket Structure

```c
struct token_bucket {
    __u64 tokens;           // Current token count
    __u64 last_refill_ns;   // Last refill timestamp
    __u64 max_tokens;       // Bucket capacity
    __u64 refill_rate;      // Tokens per second
};
```

---

## 3. User Space Components

### 3.1 Module Dependency Graph

```
                                    ┌─────────────────┐
                                    │     main.rs     │
                                    └────────┬────────┘
                                             │
            ┌────────────────────────────────┼────────────────────────────────┐
            │                                │                                │
            ▼                                ▼                                ▼
   ┌─────────────────┐              ┌─────────────────┐              ┌─────────────────┐
   │     gateway     │              │     control     │              │       api       │
   ├─────────────────┤              ├─────────────────┤              ├─────────────────┤
   │ • udp.rs        │              │ • pid.rs        │              │ • mod.rs        │
   │ • kafka.rs      │              │ • numa.rs       │              │   (axum routes) │
   └────────┬────────┘              │ • circuit_*.rs  │              └─────────────────┘
            │                       │ • tracing.rs    │
            ▼                       └────────┬────────┘
   ┌─────────────────┐                       │
   │       dam       │◄──────────────────────┘
   ├─────────────────┤
   │ • Token bucket  │
   │ • Overflow queue│
   └────────┬────────┘
            │
            ▼
   ┌─────────────────┐              ┌─────────────────┐
   │    inference    │              │        io       │
   ├─────────────────┤              ├─────────────────┤
   │ • llm.rs        │              │ • uring.rs      │
   │ • anomaly.rs    │              │ • xdp.rs        │
   │ • features.rs   │              └─────────────────┘
   └─────────────────┘
            │
            ▼
   ┌─────────────────┐
   │      ebpf       │
   ├─────────────────┤
   │ • loader.rs     │
   │ • types.rs      │
   └─────────────────┘
```

### 3.2 Dam Filter State Machine

```
                              ┌─────────────────────┐
                              │       IDLE          │
                              │  (tokens = burst)   │
                              └──────────┬──────────┘
                                         │ packet arrives
                                         ▼
                              ┌─────────────────────┐
                              │   CHECK TOKEN       │
                              └──────────┬──────────┘
                                         │
                    ┌────────────────────┼────────────────────┐
                    │ tokens > 0         │ tokens = 0         │
                    ▼                    ▼                    │
         ┌─────────────────┐  ┌─────────────────┐            │
         │  CONSUME TOKEN  │  │   QUEUE PACKET  │            │
         │  tokens--       │  │   queue.push()  │            │
         └────────┬────────┘  └────────┬────────┘            │
                  │                    │                      │
                  │           ┌────────┴────────┐            │
                  │           │ queue.len <     │ queue full │
                  │           │ limit           │            │
                  │           ▼                 ▼            │
                  │    ┌───────────┐     ┌───────────┐       │
                  │    │  QUEUED   │     │  DROPPED  │       │
                  │    └───────────┘     │  stats++  │       │
                  │                      └───────────┘       │
                  ▼                                          │
         ┌─────────────────┐                                 │
         │     PASSED      │◄────────────────────────────────┘
         │  send to Kafka  │     (periodic drain)
         └─────────────────┘
```

### 3.3 PID Controller

```
                    ┌─────────────────────────────────────────────────────┐
                    │                 PID CONTROLLER                       │
                    └─────────────────────────────────────────────────────┘
                                           │
           ┌───────────────────────────────┼───────────────────────────────┐
           │                               │                               │
           ▼                               ▼                               ▼
    ┌─────────────┐                 ┌─────────────┐                 ┌─────────────┐
    │ PROPORTIONAL│                 │  INTEGRAL   │                 │ DERIVATIVE  │
    │             │                 │             │                 │             │
    │ P = Kp * e  │                 │ I = Ki * Σe │                 │ D = Kd * Δe │
    └──────┬──────┘                 └──────┬──────┘                 └──────┬──────┘
           │                               │                               │
           └───────────────────────────────┼───────────────────────────────┘
                                           │
                                           ▼
                              ┌─────────────────────┐
                              │   OUTPUT = P + I + D │
                              │   clamp(-1.0, 1.0)   │
                              └──────────┬──────────┘
                                         │
                                         ▼
                              ┌─────────────────────┐
                              │   NEW RATE =        │
                              │   base * (1 + out)  │
                              └─────────────────────┘
```

**Error Calculation**:
```
error = 0.7 * lag_error + 0.3 * cpu_error

where:
  lag_error = (target_lag - current_lag) / target_lag
  cpu_error = target_cpu - current_cpu
```

### 3.4 Circuit Breaker State Machine

```
                              ┌─────────────────┐
                              │     CLOSED      │
                              │  (Normal flow)  │
                              └────────┬────────┘
                                       │ failures >= threshold
                                       ▼
                              ┌─────────────────┐
                              │      OPEN       │◄────────────┐
                              │  (Fail fast)    │             │
                              └────────┬────────┘             │
                                       │ timeout expired      │
                                       ▼                      │
                              ┌─────────────────┐             │
                              │   HALF-OPEN     │─────────────┘
                              │  (Probe mode)   │  probe failed
                              └────────┬────────┘
                                       │ successes >= threshold
                                       ▼
                              ┌─────────────────┐
                              │     CLOSED      │
                              └─────────────────┘
```

---

## 4. Data Flow

### 4.1 Packet Processing Pipeline

```
┌───────────┐    ┌───────────┐    ┌───────────┐    ┌───────────┐    ┌───────────┐
│    NIC    │ → │    XDP    │ → │   UDP     │ → │    Dam    │ → │   Kafka   │
│  (wire)   │    │  (kernel) │    │ (tokio)  │    │  Filter   │    │ Producer  │
└───────────┘    └───────────┘    └───────────┘    └───────────┘    └───────────┘
     │                │                │                │                │
     │                │                │                │                │
     ▼                ▼                ▼                ▼                ▼
┌───────────┐    ┌───────────┐    ┌───────────┐    ┌───────────┐    ┌───────────┐
│ 10 Gbps   │    │ 10M pps   │    │ 1M pps    │    │ 500K pps  │    │ 500K pps  │
│ Line rate │    │ Filtered  │    │ Received  │    │ Rate lim  │    │ Delivered │
└───────────┘    └───────────┘    └───────────┘    └───────────┘    └───────────┘
```

### 4.2 Latency Breakdown

| Stage | P50 | P99 | P99.9 |
|-------|-----|-----|-------|
| XDP Filter | 2 μs | 5 μs | 10 μs |
| Ring Buffer | 1 μs | 3 μs | 8 μs |
| UDP Recv | 15 μs | 45 μs | 120 μs |
| Protocol Parse | 2 μs | 5 μs | 15 μs |
| Dam Filter | 5 μs | 15 μs | 50 μs |
| Kafka Submit | 50 μs | 180 μs | 500 μs |
| **Total** | **75 μs** | **253 μs** | **703 μs** |

---

## 5. Control Flow

### 5.1 Adaptive Control Loop

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          CONTROL LOOP (100ms interval)                       │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
          ┌────────────────────────────┼────────────────────────────┐
          │                            │                            │
          ▼                            ▼                            ▼
┌─────────────────┐          ┌─────────────────┐          ┌─────────────────┐
│   OBSERVE       │          │   INFER         │          │   ACT           │
├─────────────────┤          ├─────────────────┤          ├─────────────────┤
│ • Packet rate   │    →     │ • ML prediction │    →     │ • Update rate   │
│ • Queue depth   │          │ • Burst prob    │          │ • Update eBPF   │
│ • Kafka lag     │          │ • Anomaly score │          │ • Send alerts   │
│ • CPU usage     │          │                 │          │                 │
└─────────────────┘          └─────────────────┘          └─────────────────┘
```

### 5.2 Burst Prediction Response

```
burst_probability    Action
─────────────────────────────────────────────
     0.0 - 0.2       Normal operation
     0.2 - 0.5       Pre-scale Kafka batch
     0.5 - 0.8       Reduce rate limit 25%
     0.8 - 1.0       Reduce rate limit 50%
                     Activate dam overflow
                     Alert operator
```

---

## 6. Failure Modes

### 6.1 Failure Matrix

| Component | Failure Mode | Detection | Response |
|-----------|--------------|-----------|----------|
| XDP | Program crash | Kernel panic | Fallback to SKB |
| UDP Receiver | Socket error | Error counter | Reconnect |
| Dam Filter | Queue overflow | Queue depth | Increase drop |
| Kafka | Broker down | Produce error | Circuit break |
| ML Engine | Inference error | Timeout | Use heuristics |

### 6.2 Graceful Degradation

```
┌─────────────────────────────────────────────────────────────────┐
│                    DEGRADATION LEVELS                            │
└─────────────────────────────────────────────────────────────────┘

Level 0: NORMAL
├── All components healthy
├── ML inference active
└── Full throughput

Level 1: DEGRADED_ML
├── ML inference failed
├── Fallback to heuristics
└── Full throughput

Level 2: DEGRADED_KAFKA
├── Kafka circuit open
├── Buffer to local queue
└── Reduced throughput

Level 3: DEGRADED_FULL
├── Multiple failures
├── Minimal processing
└── Drop non-essential
```

---

## 7. Scalability

### 7.1 Horizontal Scaling

```
                              ┌─────────────────┐
                              │   DNS / L4 LB   │
                              └────────┬────────┘
                                       │
           ┌───────────────────────────┼───────────────────────────┐
           │                           │                           │
           ▼                           ▼                           ▼
    ┌─────────────┐             ┌─────────────┐             ┌─────────────┐
    │   KLAIS 1   │             │   KLAIS 2   │             │   KLAIS N   │
    │  (Core 0-7) │             │  (Core 0-7) │             │  (Core 0-7) │
    └──────┬──────┘             └──────┬──────┘             └──────┬──────┘
           │                           │                           │
           └───────────────────────────┼───────────────────────────┘
                                       │
                              ┌────────▼────────┐
                              │  Kafka Cluster  │
                              │  (Partitioned)  │
                              └─────────────────┘
```

**Scaling Guidelines**:
- 1 KLAIS instance per ~500k msg/sec
- Co-locate with NUMA node for memory locality
- Use Kafka partition keys for device affinity

### 7.2 Vertical Scaling

| Resource | Impact | Recommendation |
|----------|--------|----------------|
| CPU cores | Linear throughput | 4-8 cores per instance |
| Memory | Queue capacity | 2-4 GB per instance |
| NIC speed | I/O ceiling | 10 Gbps minimum |

---

## 8. Security

### 8.1 Attack Surface

| Vector | Mitigation |
|--------|------------|
| Malformed packets | XDP magic check, bounds validation |
| DoS flood | Per-device rate limiting |
| Device spoofing | Device fingerprinting, anomaly detection |
| API abuse | Authentication (future), rate limiting |

### 8.2 Security Boundaries

```
┌─────────────────────────────────────────────────────────────────┐
│                         TRUST ZONES                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────┐        ┌──────────────────┐               │
│  │   UNTRUSTED      │        │    TRUSTED       │               │
│  │   (Internet)     │        │   (Internal)     │               │
│  │                  │        │                  │               │
│  │  • IoT Devices   │───────►│  • KLAIS Gateway │               │
│  │  • Unknown IPs   │   XDP  │  • Kafka Cluster │               │
│  │                  │ Filter │  • REST API      │               │
│  └──────────────────┘        └──────────────────┘               │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Appendix: Configuration Reference

See [README.md](../README.md#configuration) for complete configuration options.
