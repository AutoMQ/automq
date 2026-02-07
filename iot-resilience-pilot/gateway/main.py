#!/usr/bin/env python3
"""
IoT Ingestion Gateway
UDP Server that receives IoT telemetry and produces to AutoMQ/Kafka

This gateway translates UDP datagrams from IoT devices into Kafka records,
optimized for high-throughput burst scenarios.
"""

import asyncio
import json
import struct
import time
import logging
from dataclasses import dataclass
from typing import Optional
from collections import deque

try:
    from aiokafka import AIOKafkaProducer
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
except ImportError:
    print("Installing required packages...")
    import subprocess
    subprocess.check_call(['pip', 'install', 'aiokafka', 'prometheus-client'])
    from aiokafka import AIOKafkaProducer
    from prometheus_client import Counter, Gauge, Histogram, start_http_server

import os
# Configuration
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = "iot_telemetry"
UDP_HOST = "0.0.0.0"
UDP_PORT = int(os.getenv("UDP_PORT", 5000))
METRICS_PORT = 8080

# Producer optimizations for burst handling
LINGER_MS = 50          # Wait up to 50ms to batch messages
BATCH_SIZE = 16384      # 16KB batch size
MAX_REQUEST_SIZE = 1048576  # 1MB max request

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus Metrics
MESSAGES_RECEIVED = Counter('iot_messages_received_total', 'Total messages received from devices')
MESSAGES_PRODUCED = Counter('iot_messages_produced_total', 'Total messages produced to Kafka')
PRODUCE_ERRORS = Counter('iot_produce_errors_total', 'Total Kafka produce errors')
ACTIVE_DEVICES = Gauge('iot_active_devices', 'Number of active device IDs seen in last minute')
PRODUCE_LATENCY = Histogram('iot_produce_latency_ms', 'Produce latency in milliseconds',
                            buckets=[1, 5, 10, 25, 50, 100, 250, 500, 1000])


@dataclass
class TelemetryMessage:
    """Represents an IoT telemetry message"""
    device_id: str
    sequence: int
    timestamp: float
    payload: bytes
    
    def to_kafka_value(self) -> bytes:
        """Serialize for Kafka"""
        return json.dumps({
            "device_id": self.device_id,
            "sequence": self.sequence,
            "timestamp": self.timestamp,
            "payload": self.payload.hex() if isinstance(self.payload, bytes) else str(self.payload)
        }).encode('utf-8')
    
    def kafka_key(self) -> bytes:
        """Device ID as partition key for ordering"""
        return self.device_id.encode('utf-8')


class DeviceTracker:
    """Tracks active devices for metrics"""
    
    def __init__(self, window_seconds: int = 60):
        self.window = window_seconds
        self.device_times: dict[str, float] = {}
    
    def record(self, device_id: str):
        self.device_times[device_id] = time.time()
    
    def count_active(self) -> int:
        cutoff = time.time() - self.window
        # Cleanup old entries
        self.device_times = {
            k: v for k, v in self.device_times.items() 
            if v > cutoff
        }
        return len(self.device_times)


class UDPProtocol(asyncio.DatagramProtocol):
    """Asyncio UDP Protocol for receiving IoT datagrams"""
    
    def __init__(self, message_queue: asyncio.Queue, device_tracker: DeviceTracker):
        self.queue = message_queue
        self.tracker = device_tracker
        self.transport = None
    
    def connection_made(self, transport):
        self.transport = transport
        logger.info(f"UDP Server listening on {UDP_HOST}:{UDP_PORT}")
    
    def datagram_received(self, data: bytes, addr):
        """Parse incoming datagram and queue for Kafka production"""
        try:
            message = self._parse_datagram(data, addr)
            if message:
                MESSAGES_RECEIVED.inc()
                self.tracker.record(message.device_id)
                
                # Non-blocking queue put
                try:
                    self.queue.put_nowait(message)
                except asyncio.QueueFull:
                    logger.warning("Message queue full, dropping message")
                    
        except Exception as e:
            logger.error(f"Error parsing datagram from {addr}: {e}")
    
    def _parse_datagram(self, data: bytes, addr) -> Optional[TelemetryMessage]:
        """
        Parse UDP datagram into TelemetryMessage
        
        Packet format (optional header + payload):
        - If starts with 0xAA 0x55 (magic bytes): structured packet
          [2B magic][16B device_id][4B sequence][rest: payload]
        - Otherwise: treat entire packet as JSON payload
        """
        if len(data) < 2:
            return None
            
        # Check for magic header
        if data[0] == 0xAA and data[1] == 0x55:
            if len(data) < 22:  # 2 + 16 + 4
                return None
            
            device_id = data[2:18].decode('utf-8').strip('\x00')
            sequence = struct.unpack('>I', data[18:22])[0]
            payload = data[22:]
            
            return TelemetryMessage(
                device_id=device_id,
                sequence=sequence,
                timestamp=time.time(),
                payload=payload
            )
        else:
            # JSON format
            try:
                json_data = json.loads(data.decode('utf-8'))
                return TelemetryMessage(
                    device_id=json_data.get('device_id', f"udp_{addr[0]}_{addr[1]}"),
                    sequence=json_data.get('sequence', 0),
                    timestamp=json_data.get('timestamp', time.time()),
                    payload=json.dumps(json_data.get('data', {})).encode('utf-8')
                )
            except json.JSONDecodeError:
                # Raw binary payload
                return TelemetryMessage(
                    device_id=f"udp_{addr[0]}_{addr[1]}",
                    sequence=0,
                    timestamp=time.time(),
                    payload=data
                )


class KafkaProducerWorker:
    """Async worker that batches and produces messages to Kafka"""
    
    def __init__(self, message_queue: asyncio.Queue):
        self.queue = message_queue
        self.producer: Optional[AIOKafkaProducer] = None
        self.running = False
    
    async def start(self):
        """Initialize Kafka producer"""
        logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP}...")
        
        self.producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            linger_ms=LINGER_MS,
            max_batch_size=BATCH_SIZE,
            max_request_size=MAX_REQUEST_SIZE,
            acks='all',  # Wait for all replicas
            # compression_type='lz4'  # Disabled due to env issues
        )
        
        await self.producer.start()
        logger.info("Kafka producer connected!")
        self.running = True
    
    async def stop(self):
        """Graceful shutdown"""
        self.running = False
        if self.producer:
            await self.producer.stop()
            logger.info("Kafka producer stopped")
    
    async def run(self):
        """Main production loop - Optimized for Batching"""
        
        def on_send_success(record_metadata):
            PRODUCE_LATENCY.observe((time.time() - record_metadata.timestamp/1000.0) * 1000)
            MESSAGES_PRODUCED.inc()

        def on_send_error(exc):
            logger.error(f"Kafka produce error: {exc}")
            PRODUCE_ERRORS.inc()

        while self.running:
            try:
                # Get message with timeout
                message = await asyncio.wait_for(
                    self.queue.get(), 
                    timeout=1.0
                )
                
                # Fire and forget (Async batching)
                # The library handles batching via LINGER_MS
                future = await self.producer.send(
                    KAFKA_TOPIC,
                    value=message.to_kafka_value(),
                    key=message.kafka_key()
                )
                
                # Attach non-blocking callbacks
                future.add_done_callback(lambda f: on_send_success(f.result()) if not f.exception() else on_send_error(f.exception()))
                
                # Minimal yielding to let loop process other events
                # We don't await the future here!
                
            except asyncio.TimeoutError:
                # Normal timeout, continue loop
                pass
            except Exception as e:
                logger.error(f"Worker loop error: {e}")
                await asyncio.sleep(0.1)


async def update_metrics(device_tracker: DeviceTracker):
    """Periodically update device count metric"""
    while True:
        ACTIVE_DEVICES.set(device_tracker.count_active())
        await asyncio.sleep(5)


async def main():
    """Main entry point"""
    logger.info("=" * 50)
    logger.info("IoT Ingestion Gateway Starting")
    logger.info("=" * 50)
    
    # Start Prometheus metrics server
    start_http_server(METRICS_PORT)
    logger.info(f"Metrics server on port {METRICS_PORT}")
    
    # Message queue between UDP receiver and Kafka producer
    message_queue = asyncio.Queue(maxsize=100000)
    device_tracker = DeviceTracker()
    
    # Start Kafka producer worker
    producer_worker = KafkaProducerWorker(message_queue)
    await producer_worker.start()
    
    # Start UDP server
    loop = asyncio.get_event_loop()
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UDPProtocol(message_queue, device_tracker),
        local_addr=(UDP_HOST, UDP_PORT)
    )
    
    # Start background tasks
    producer_task = asyncio.create_task(producer_worker.run())
    metrics_task = asyncio.create_task(update_metrics(device_tracker))
    
    logger.info(f"Gateway ready! UDP: {UDP_PORT}, Kafka: {KAFKA_TOPIC}")
    logger.info("Press Ctrl+C to stop")
    
    try:
        await asyncio.gather(producer_task, metrics_task)
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("Shutting down...")
        transport.close()
        await producer_worker.stop()


if __name__ == "__main__":
    import sys
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Gateway stopped by user")
