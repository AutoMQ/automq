# KLAIS API Reference

## Base URL

```
http://localhost:8080
```

---

## Endpoints

### Health Check

Check system health and basic status.

```http
GET /health
```

**Response**

```json
{
  "status": "ok",
  "queue_depth": 42,
  "rate_limit": 10000,
  "version": "0.1.0",
  "uptime_secs": 3600,
  "build": {
    "git_hash": "abc1234",
    "build_time": "2026-01-14 10:30:00 UTC",
    "target": "x86_64-unknown-linux-gnu"
  }
}
```

**Status Codes**

| Code | Meaning |
|------|---------|
| 200 | Healthy |
| 503 | Degraded (queue overflow or circuit open) |

---

### Update Configuration

Dynamically update runtime configuration.

```http
POST /config
Content-Type: application/json
```

**Request Body**

```json
{
  "new_rate": 20000
}
```

**Response**

```json
{
  "success": true,
  "previous_rate": 10000,
  "new_rate": 20000
}
```

**Validation**

| Field | Type | Constraints |
|-------|------|-------------|
| `new_rate` | u64 | 1 - 1,000,000 |

---

### Statistics

Get comprehensive system statistics.

```http
GET /stats
```

**Response**

```json
{
  "timestamp": "2026-01-14T10:30:00Z",
  "dam": {
    "received": 1000000,
    "passed": 998000,
    "queued": 1500,
    "dropped": 500,
    "queue_depth": 150,
    "tokens": 8500,
    "max_rate": 10000,
    "burst_size": 50000
  },
  "control": {
    "current_rate": 10000,
    "burst_probability": 0.15,
    "anomaly_devices": 3,
    "pid_output": 0.05,
    "last_tick_ms": 5
  },
  "inference": {
    "total_inferences": 3600,
    "total_tokens": 125000,
    "avg_latency_us": 180
  },
  "kafka": {
    "messages_sent": 998000,
    "bytes_sent": 125000000,
    "errors": 12,
    "avg_latency_us": 450
  },
  "system": {
    "cpu_percent": 45.2,
    "memory_mb": 280,
    "numa_node": 0,
    "uptime_secs": 3600
  }
}
```

---

### Prometheus Metrics

Get metrics in Prometheus exposition format.

```http
GET /metrics
```

**Response**

```text
# HELP klais_packets_received_total Total packets received
# TYPE klais_packets_received_total counter
klais_packets_received_total 1000000

# HELP klais_packets_passed_total Packets passed through dam
# TYPE klais_packets_passed_total counter
klais_packets_passed_total 998000

# HELP klais_packets_dropped_total Packets dropped
# TYPE klais_packets_dropped_total counter
klais_packets_dropped_total 500

# HELP klais_queue_depth Current queue depth
# TYPE klais_queue_depth gauge
klais_queue_depth 150

# HELP klais_rate_limit Current rate limit
# TYPE klais_rate_limit gauge
klais_rate_limit 10000

# HELP klais_processing_latency_seconds Processing latency histogram
# TYPE klais_processing_latency_seconds histogram
klais_processing_latency_seconds_bucket{le="0.0001"} 500000
klais_processing_latency_seconds_bucket{le="0.0005"} 950000
klais_processing_latency_seconds_bucket{le="0.001"} 998000
klais_processing_latency_seconds_bucket{le="+Inf"} 1000000
klais_processing_latency_seconds_sum 85.5
klais_processing_latency_seconds_count 1000000

# HELP klais_bytes_received_total Total bytes received
# TYPE klais_bytes_received_total counter
klais_bytes_received_total 125000000

# HELP klais_inference_latency_seconds Inference latency histogram
# TYPE klais_inference_latency_seconds histogram
klais_inference_latency_seconds_bucket{le="0.0001"} 1000
klais_inference_latency_seconds_bucket{le="0.0005"} 3500
klais_inference_latency_seconds_bucket{le="0.001"} 3600
klais_inference_latency_seconds_bucket{le="+Inf"} 3600
```

---

## Metrics Reference

### Counters

| Metric | Labels | Description |
|--------|--------|-------------|
| `klais_packets_received_total` | - | Total UDP packets received |
| `klais_packets_passed_total` | - | Packets passed through dam |
| `klais_packets_dropped_total` | - | Packets dropped (rate limited) |
| `klais_bytes_received_total` | - | Total bytes received |
| `klais_kafka_messages_total` | - | Messages sent to Kafka |
| `klais_kafka_errors_total` | - | Kafka producer errors |

### Gauges

| Metric | Labels | Description |
|--------|--------|-------------|
| `klais_queue_depth` | - | Current overflow queue depth |
| `klais_rate_limit` | - | Current rate limit |
| `klais_tokens_available` | - | Available tokens in bucket |
| `klais_burst_probability` | - | Predicted burst probability |
| `klais_anomaly_devices` | - | Devices flagged as anomalous |

### Histograms

| Metric | Labels | Description |
|--------|--------|-------------|
| `klais_processing_latency_seconds` | - | End-to-end processing latency |
| `klais_inference_latency_seconds` | - | ML inference latency |
| `klais_kafka_latency_seconds` | - | Kafka produce latency |

---

## Error Responses

All errors return JSON with the following structure:

```json
{
  "error": "Error message",
  "code": "ERROR_CODE",
  "details": {}
}
```

### Error Codes

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `INVALID_REQUEST` | 400 | Malformed JSON or missing fields |
| `VALIDATION_FAILED` | 422 | Field values out of range |
| `RATE_LIMITED` | 429 | Too many API requests |
| `INTERNAL_ERROR` | 500 | Unexpected server error |
| `SERVICE_UNAVAILABLE` | 503 | System degraded |

---

## Example Usage

### cURL

```bash
# Health check
curl -s http://localhost:8080/health | jq

# Update rate limit
curl -X POST http://localhost:8080/config \
  -H "Content-Type: application/json" \
  -d '{"new_rate": 20000}'

# Get statistics
curl -s http://localhost:8080/stats | jq

# Prometheus metrics
curl -s http://localhost:8080/metrics
```

### Python

```python
import requests

BASE_URL = "http://localhost:8080"

# Health check
response = requests.get(f"{BASE_URL}/health")
print(response.json())

# Update rate limit
response = requests.post(
    f"{BASE_URL}/config",
    json={"new_rate": 20000}
)
print(response.json())

# Get statistics
response = requests.get(f"{BASE_URL}/stats")
stats = response.json()
print(f"Queue depth: {stats['dam']['queue_depth']}")
```

### Rust

```rust
use reqwest::Client;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new();
    
    // Health check
    let health: serde_json::Value = client
        .get("http://localhost:8080/health")
        .send()
        .await?
        .json()
        .await?;
    println!("Health: {:?}", health);
    
    // Update rate
    let result: serde_json::Value = client
        .post("http://localhost:8080/config")
        .json(&json!({"new_rate": 20000}))
        .send()
        .await?
        .json()
        .await?;
    println!("Update: {:?}", result);
    
    Ok(())
}
```

---

## Rate Limiting

The API itself is rate limited to prevent abuse:

- **Default**: 100 requests/second per IP
- **Burst**: 200 requests

Exceeding limits returns `429 Too Many Requests`.
