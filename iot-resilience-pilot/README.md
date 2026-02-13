# AutoMQ IoT Burst Resilience Pilot

> **Store-and-Forward** reference implementation demonstrating AutoMQ's ability to absorb "Thundering Herd" write bursts from IoT device fleets.

## 🎯 Overview

IoT devices buffer data during network outages. When connectivity is restored, thousands of devices simultaneously flush their buffers, creating massive write spikes that can overwhelm traditional Kafka brokers.

**AutoMQ's Solution**: S3 stream offloading and high-performance WAL absorbs these bursts cost-effectively without requiring expensive provisioned storage.

## 📦 Components

| Component | Description |
|-----------|-------------|
| **Infrastructure** | Docker Compose with AutoMQ, MinIO (S3), Prometheus, Grafana |
| **Ingestion Gateway** | Python asyncio UDP server → Kafka producer |
| **Device Simulator** | High-concurrency Python tool simulating 1000s of ESP32 devices |
| **ESP32 Firmware** | Reference C++ code with LittleFS buffering |

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.8+
- (Optional) PlatformIO for ESP32 firmware

### 1. Start Infrastructure

```powershell
cd iot-resilience-pilot
docker-compose up -d

# Wait for AutoMQ to be ready (about 30-60 seconds)
docker-compose logs -f automq 2>&1 | Select-String "started"
```

### 2. Create Kafka Topic

```powershell
docker-compose exec automq /opt/automq/kafka/bin/kafka-topics.sh `
    --create --topic iot_telemetry `
    --bootstrap-server localhost:9092 `
    --partitions 3 --replication-factor 1
```

### 3. Start Ingestion Gateway

```powershell
cd gateway
pip install -r requirements.txt

# Standard (Windows/Development)
python main.py

# High-Performance (Linux/Production)
python main_optimized.py
```

Gateway listens on:
- `UDP :5000` - IoT telemetry
- `HTTP :8080` - Prometheus metrics

### 4. Run Device Simulator

```powershell
cd simulator
python device_simulator.py --devices 100 --duration 300
```

**CLI Commands:**
- `offline` - Simulate network outage (devices buffer locally)
- `online` - Restore network (triggers high-speed flush)
- `status` - Show current statistics
- `quit` - Stop simulation

## ⚡ Performance Optimization

### Windows vs Linux Performance

| Setting | Windows | Linux (Prod) |
|---------|---------|--------------|
| Gateway | `main.py` | `main_optimized.py` |
| Workers | 4 | 8 |
| Batch Size | 16KB | 64KB |
| Compression | none | lz4 |
| P99 Latency | ~42ms | ~25ms (expected) |

### Linux Production Deployment

```bash
# 1. Apply kernel tuning (run as root)
sudo ./deploy/tune_linux.sh

# 2. Use optimized compose
docker-compose -f deploy/docker-compose.linux.yml up -d

# 3. Or run gateway with env vars
NUM_WORKERS=8 COMPRESSION=lz4 python gateway/main_optimized.py
```

## 🧪 Running the Burst Test

This test validates AutoMQ's resilience to "Thundering Herd" scenarios.

### Test Scenario

1. **Normal Load**: 1000 devices sending 1 msg/sec = 1000 msg/sec
2. **Outage**: 60 seconds (60,000 messages buffered)
3. **Burst**: Restore connection, throughput spikes to >5000 msg/sec
4. **Recovery**: System returns to normal within 30 seconds

### Execute Test

```powershell
# Terminal 1: Start infrastructure
docker-compose up -d

# Terminal 2: Start gateway
cd gateway && python main.py

# Terminal 3: Run simulator with 1000 devices
cd simulator
python device_simulator.py --devices 1000 --duration 300

# After 30 seconds of normal operation, type:
# > offline

# Wait 60 seconds, then type:
# > online

# Observe the burst in throughput!
# > status
```

### Success Criteria

| Metric | Target |
|--------|--------|
| AutoMQ Stability | No crashes or errors |
| Messages Ingested | All 60,000 buffered messages |
| Latency Recovery | < 30 seconds after burst |
| Peak Throughput | > 5,000 msg/sec during flush |

## 📊 Monitoring

### Grafana Dashboard
Open http://localhost:3000 (admin/admin)

Dashboard shows:
- Message throughput (msg/sec)
- Producer latency
- Active devices
- Error rates

### Prometheus Metrics
Available at http://localhost:9090

Key metrics:
- `iot_messages_received_total`
- `iot_messages_produced_total`
- `iot_produce_latency_ms`
- `iot_active_devices`

## 🔧 Configuration

### Gateway (`gateway/main.py`)

```python
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "iot_telemetry"
UDP_PORT = 5000
LINGER_MS = 50      # Batch timeout
BATCH_SIZE = 16384  # 16KB batches
```

### Simulator

```bash
python device_simulator.py \
    --devices 1000 \      # Number of virtual devices
    --duration 600 \      # Max runtime (seconds)
    --host 127.0.0.1 \    # Gateway host
    --port 5000           # Gateway port
```

## 📂 Project Structure

```
iot-resilience-pilot/
├── docker-compose.yml      # Infrastructure stack
├── prometheus/
│   └── prometheus.yml      # Scrape configuration
├── grafana/
│   ├── provisioning/       # Auto-config datasources & dashboards
│   └── dashboards/
│       └── iot_telemetry.json
├── gateway/
│   ├── main.py             # UDP→Kafka gateway
│   └── requirements.txt
├── simulator/
│   └── device_simulator.py # Virtual device fleet
└── esp32_firmware/
    ├── iot_sensor.cpp      # Reference ESP32 code
    └── platformio.ini      # PlatformIO build config
```

## 🔌 ESP32 Hardware (Optional)

For real hardware testing:

1. Install PlatformIO
2. Edit `esp32_firmware/iot_sensor.cpp`:
   - Set `WIFI_SSID` and `WIFI_PASSWORD`
   - Set `GATEWAY_HOST` to your gateway's IP
3. Build and upload:
   ```bash
   cd esp32_firmware
   pio run --target upload
   pio device monitor
   ```

## 📈 Why AutoMQ?

Traditional Kafka requires:
- Provisioned IOPS for burst absorption
- Expensive NVMe storage for WAL
- Manual scaling for peak loads

**AutoMQ advantages**:
- S3 offloading handles burst writes elastically
- WAL acts as circular buffer, absorbs spikes
- No over-provisioning needed
- Cost-effective for IoT use cases

## 📝 License

Apache 2.0 - Part of the AutoMQ project.
