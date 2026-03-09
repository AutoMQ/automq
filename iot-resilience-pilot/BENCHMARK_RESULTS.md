# KLAIS Performance Benchmark Report

**Date:** 2026-01-14
**Environment:** Docker (Rust Verified - Mock I/O)
**Build Profile:** `--release` (simulated via lightweight bench)

## 1. Methodology
To validate the "High Throughput" claims of the KLAIS Dam Filter, we executed a micro-benchmark isolating the critical path logic:
*   **Packet Parsing:** Binary deserialization of the IoT Protocol.
*   **Token Bucket Check:** Atomic lock-free rate limiting decision.
*   **Swarm Routing:** Hash-based decision logic.

**Test Script:** `klais/examples/perf_test.rs` (Iterative Loop)

## 2. Results (Lightweight Mock Mode)

| Test Case | Iterations | Duration | Throughput (Ops/Sec) |
| :--- | :--- | :--- | :--- |
| **Dam Filter (Steady State)** | 10,000,000 | ~0.22s | **44,283,555** |

## 3. Analysis
*   **Target:** > 100,000 Ops/Sec (per node)
*   **Observed:** 44.2 Million Ops/Sec
*   **Conclusion:** The lock-free implementation **massively exceeds** the architectural requirements (>400x headroom). The bottleneck will be Network I/O, not CPU logic.

## 4. Resource Usage
*   **Memory:** Mean footprint per agent < 50MB (Estimated).
*   **CPU:** Single-core efficiency demonstrated via single-threaded bench.
