#!/bin/bash
# Quick Start Script for IoT Burst Resilience Pilot
# Run this from the iot-resilience-pilot directory

echo "============================================="
echo "AutoMQ IoT Burst Resilience Pilot - Setup"
echo "============================================="

# Step 1: Start infrastructure
echo ""
echo "[1/4] Starting Docker infrastructure..."
docker-compose up -d

echo ""
echo "[2/4] Waiting for AutoMQ to be ready (60 seconds)..."
sleep 60

# Step 2: Create topic
echo ""
echo "[3/4] Creating Kafka topic..."
docker-compose exec automq /opt/automq/kafka/bin/kafka-topics.sh \
    --create --topic iot_telemetry \
    --bootstrap-server localhost:9092 \
    --partitions 3 --replication-factor 1 2>/dev/null || \
    echo "Topic may already exist, continuing..."

# Step 3: Install Python dependencies
echo ""
echo "[4/4] Installing Python dependencies..."
pip install -r gateway/requirements.txt

echo ""
echo "============================================="
echo "Setup Complete!"
echo "============================================="
echo ""
echo "Next steps:"
echo "  1. Start gateway:   cd gateway && python main.py"
echo "  2. Run simulator:   cd simulator && python device_simulator.py --devices 100"
echo "  3. Open Grafana:    http://localhost:3000 (admin/admin)"
echo ""
echo "For chaos test:"
echo "  python chaos_test.py --devices 100 --outage-seconds 60"
echo ""
