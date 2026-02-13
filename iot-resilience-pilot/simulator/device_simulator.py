#!/usr/bin/env python3
"""
IoT Device Simulator
High-concurrency simulator for testing AutoMQ burst resilience

Simulates N ESP32 devices with:
- Normal mode: 1 message per second per device
- Offline mode: Buffer messages locally (simulating WiFi outage)
- Flush mode: Blast buffered messages at high speed (20-50ms interval)
"""

import asyncio
import socket
import struct
import json
import time
import random
import argparse
import sys
from dataclasses import dataclass, field
from typing import Optional
from collections import deque
from datetime import datetime

# Configuration defaults
GATEWAY_HOST = "127.0.0.1"
GATEWAY_PORT = 5000
DEFAULT_DEVICES = 100
DEFAULT_DURATION = 300  # 5 minutes
NORMAL_INTERVAL = 1.0  # 1 second between messages in normal mode
FLUSH_INTERVAL = 0.03  # 30ms between messages in flush mode


@dataclass
class SimulatorConfig:
    """Configuration for the simulator"""
    gateway_host: str = GATEWAY_HOST
    gateway_port: int = GATEWAY_PORT
    num_devices: int = DEFAULT_DEVICES
    duration: int = DEFAULT_DURATION
    verbose: bool = False


@dataclass
class DeviceState:
    """State of a single virtual device"""
    device_id: str
    sequence: int = 0
    buffer: deque = field(default_factory=lambda: deque(maxlen=10000))
    messages_sent: int = 0
    messages_buffered: int = 0
    messages_flushed: int = 0


class NetworkState:
    """Shared network state that controls all devices"""
    
    def __init__(self):
        self._online = True
        self._flush_mode = False
        self.outage_start: Optional[float] = None
        self.outage_duration: float = 0
    
    @property
    def online(self) -> bool:
        return self._online
    
    @property
    def flush_mode(self) -> bool:
        return self._flush_mode
    
    def go_offline(self):
        """Simulate network outage"""
        if self._online:
            self._online = False
            self._flush_mode = False
            self.outage_start = time.time()
            return True
        return False
    
    def go_online(self):
        """Restore network and trigger flush mode"""
        if not self._online:
            self._online = True
            self._flush_mode = True  # Enter flush mode to clear buffers
            self.outage_duration = time.time() - (self.outage_start or time.time())
            return True
        return False
    
    def exit_flush_mode(self):
        """Exit flush mode after buffers are cleared"""
        self._flush_mode = False


class VirtualDevice:
    """Simulates a single ESP32 IoT device"""
    
    def __init__(self, device_id: str, network_state: NetworkState, 
                 sock: socket.socket, gateway_addr: tuple, verbose: bool = False):
        self.state = DeviceState(device_id=device_id)
        self.network = network_state
        self.sock = sock
        self.gateway_addr = gateway_addr
        self.verbose = verbose
        self.running = True
    
    def _generate_sensor_data(self) -> dict:
        """Simulate sensor readings"""
        return {
            "temperature": round(20 + random.uniform(-5, 15), 2),
            "humidity": round(random.uniform(30, 80), 1),
            "pressure": round(random.uniform(980, 1020), 1),
            "battery": round(random.uniform(3.3, 4.2), 2)
        }
    
    def _create_packet(self, data: dict) -> bytes:
        """
        Create UDP packet with structured header
        Format: [2B magic 0xAA55][16B device_id][4B sequence][JSON payload]
        """
        self.state.sequence += 1
        
        # Header
        magic = bytes([0xAA, 0x55])
        device_id_bytes = self.state.device_id.encode('utf-8')[:16].ljust(16, b'\x00')
        seq_bytes = struct.pack('>I', self.state.sequence)
        
        # Payload
        payload = json.dumps({
            "ts": time.time(),
            "data": data
        }).encode('utf-8')
        
        return magic + device_id_bytes + seq_bytes + payload
    
    def _send_packet(self, packet: bytes) -> bool:
        """Send UDP packet to gateway"""
        try:
            self.sock.sendto(packet, self.gateway_addr)
            return True
        except Exception as e:
            if self.verbose:
                print(f"[{self.state.device_id}] Send error: {e}")
            return False
    
    async def run(self):
        """Main device loop"""
        # Stagger device start times
        await asyncio.sleep(random.uniform(0, 2))
        
        while self.running:
            sensor_data = self._generate_sensor_data()
            packet = self._create_packet(sensor_data)
            
            if self.network.online:
                # Flush mode: clear buffer first
                if self.network.flush_mode and self.state.buffer:
                    # High-speed flush
                    while self.state.buffer and self.network.online:
                        buffered_packet = self.state.buffer.popleft()
                        if self._send_packet(buffered_packet):
                            self.state.messages_flushed += 1
                        await asyncio.sleep(FLUSH_INTERVAL)
                    
                    if not self.state.buffer:
                        if self.verbose:
                            print(f"[{self.state.device_id}] Buffer flushed!")
                
                # Send current packet
                if self._send_packet(packet):
                    self.state.messages_sent += 1
                
                await asyncio.sleep(NORMAL_INTERVAL)
            else:
                # Offline: buffer the packet
                self.state.buffer.append(packet)
                self.state.messages_buffered += 1
                await asyncio.sleep(NORMAL_INTERVAL)
    
    def stop(self):
        self.running = False
    
    def get_stats(self) -> dict:
        return {
            "device_id": self.state.device_id,
            "sent": self.state.messages_sent,
            "buffered": self.state.messages_buffered,
            "flushed": self.state.messages_flushed,
            "buffer_size": len(self.state.buffer)
        }


class DeviceSimulator:
    """Manages fleet of virtual devices"""
    
    def __init__(self, config: SimulatorConfig):
        self.config = config
        self.network_state = NetworkState()
        self.devices: list[VirtualDevice] = []
        self.sock: Optional[socket.socket] = None
        self.running = False
        self.start_time: Optional[float] = None
    
    def _create_socket(self) -> socket.socket:
        """Create UDP socket for sending"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setblocking(False)
        return sock
    
    def _create_devices(self):
        """Spawn N virtual devices"""
        gateway_addr = (self.config.gateway_host, self.config.gateway_port)
        
        for i in range(self.config.num_devices):
            device_id = f"ESP32_{i:05d}"
            device = VirtualDevice(
                device_id=device_id,
                network_state=self.network_state,
                sock=self.sock,
                gateway_addr=gateway_addr,
                verbose=self.config.verbose
            )
            self.devices.append(device)
    
    def get_stats(self) -> dict:
        """Aggregate statistics from all devices"""
        total_sent = sum(d.state.messages_sent for d in self.devices)
        total_buffered = sum(d.state.messages_buffered for d in self.devices)
        total_flushed = sum(d.state.messages_flushed for d in self.devices)
        total_in_buffer = sum(len(d.state.buffer) for d in self.devices)
        
        elapsed = time.time() - (self.start_time or time.time())
        throughput = total_sent / elapsed if elapsed > 0 else 0
        
        return {
            "devices": self.config.num_devices,
            "elapsed_seconds": round(elapsed, 1),
            "messages_sent": total_sent,
            "messages_buffered": total_buffered,
            "messages_flushed": total_flushed,
            "current_buffer_size": total_in_buffer,
            "throughput_msg_sec": round(throughput, 1),
            "network_online": self.network_state.online,
            "flush_mode": self.network_state.flush_mode
        }
    
    async def cli_handler(self):
        """Handle CLI commands asynchronously"""
        loop = asyncio.get_event_loop()
        
        def print_help():
            print("\n" + "=" * 50)
            print("Available Commands:")
            print("  offline  - Simulate network outage (devices buffer)")
            print("  online   - Restore network (triggers flush)")
            print("  status   - Show current stats")
            print("  quit     - Stop simulation")
            print("  help     - Show this message")
            print("=" * 50 + "\n")
        
        print_help()
        
        while self.running:
            try:
                # Read from stdin asynchronously
                line = await loop.run_in_executor(None, sys.stdin.readline)
                cmd = line.strip().lower()
                
                if cmd == 'offline':
                    if self.network_state.go_offline():
                        print("\nNETWORK OFFLINE - Devices are buffering messages")
                    else:
                        print("\n⚠️  Already offline")
                        
                elif cmd == 'online':
                    if self.network_state.go_online():
                        print(f"\nNETWORK RESTORED - Flushing buffered messages")
                        print(f"   Outage duration: {self.network_state.outage_duration:.1f}s")
                    else:
                        print("\n⚠️  Already online")
                        
                elif cmd == 'status':
                    stats = self.get_stats()
                    print("\n📊 Simulation Status:")
                    print(f"   Devices: {stats['devices']}")
                    print(f"   Elapsed: {stats['elapsed_seconds']}s")
                    print(f"   Messages Sent: {stats['messages_sent']}")
                    print(f"   Messages Buffered: {stats['messages_buffered']}")
                    print(f"   Messages Flushed: {stats['messages_flushed']}")
                    print(f"   Current Buffer: {stats['current_buffer_size']}")
                    print(f"   Throughput: {stats['throughput_msg_sec']} msg/s")
                    print(f"   Network: {'🟢 ONLINE' if stats['network_online'] else '🔴 OFFLINE'}")
                    if stats['flush_mode']:
                        print(f"   Mode: ⚡ FLUSHING")
                    print()
                    
                elif cmd == 'quit' or cmd == 'exit':
                    print("\n👋 Stopping simulation...")
                    self.running = False
                    break
                    
                elif cmd == 'help':
                    print_help()
                    
                elif cmd:
                    print(f"Unknown command: {cmd}. Type 'help' for options.")
                    
            except Exception as e:
                print(f"CLI Error: {e}")
                await asyncio.sleep(0.5)
    
    async def stats_reporter(self):
        """Periodically print stats"""
        while self.running:
            await asyncio.sleep(10)
            if self.running:
                stats = self.get_stats()
                status = "ONLINE" if stats['network_online'] else "OFFLINE"
                mode = " FLUSH" if stats['flush_mode'] else ""
                print(f"\r[{status}{mode}] Sent: {stats['messages_sent']} | "
                      f"Buf: {stats['current_buffer_size']} | "
                      f"Rate: {stats['throughput_msg_sec']}/s", end='')
    
    async def run(self):
        """Start the simulation"""
        print("\n" + "=" * 60)
        print("IoT Device Simulator Starting")
        print("=" * 60)
        print(f"   Gateway: {self.config.gateway_host}:{self.config.gateway_port}")
        print(f"   Devices: {self.config.num_devices}")
        print(f"   Duration: {self.config.duration}s (or 'quit' to stop)")
        print("=" * 60 + "\n")
        
        # Initialize
        self.sock = self._create_socket()
        self._create_devices()
        self.running = True
        self.start_time = time.time()
        
        # Start all device tasks
        device_tasks = [asyncio.create_task(d.run()) for d in self.devices]
        cli_task = asyncio.create_task(self.cli_handler())
        stats_task = asyncio.create_task(self.stats_reporter())
        
        # Duration timeout
        async def duration_timer():
            await asyncio.sleep(self.config.duration)
            if self.running:
                print(f"\n\n⏰ Duration limit ({self.config.duration}s) reached")
                self.running = False
        
        timeout_task = asyncio.create_task(duration_timer())
        
        try:
            # Wait for either CLI quit or timeout
            await asyncio.gather(cli_task, timeout_task, return_exceptions=True)
        except asyncio.CancelledError:
            pass
        finally:
            # Stop all devices
            self.running = False
            for device in self.devices:
                device.stop()
            
            # Cancel remaining tasks
            for task in device_tasks:
                task.cancel()
            stats_task.cancel()
            
            # Final stats
            print("\n\n" + "=" * 60)
            print("Final Statistics")
            print("=" * 60)
            stats = self.get_stats()
            print(f"   Total Messages Sent: {stats['messages_sent']}")
            print(f"   Total Messages Buffered: {stats['messages_buffered']}")
            print(f"   Total Messages Flushed: {stats['messages_flushed']}")
            print(f"   Average Throughput: {stats['throughput_msg_sec']} msg/s")
            print("=" * 60 + "\n")
            
            if self.sock:
                self.sock.close()


def main():
    parser = argparse.ArgumentParser(
        description="IoT Device Simulator for AutoMQ Burst Testing"
    )
    parser.add_argument(
        '-d', '--devices',
        type=int,
        default=DEFAULT_DEVICES,
        help=f"Number of virtual devices (default: {DEFAULT_DEVICES})"
    )
    parser.add_argument(
        '-t', '--duration',
        type=int,
        default=DEFAULT_DURATION,
        help=f"Simulation duration in seconds (default: {DEFAULT_DURATION})"
    )
    parser.add_argument(
        '-H', '--host',
        type=str,
        default=GATEWAY_HOST,
        help=f"Gateway host (default: {GATEWAY_HOST})"
    )
    parser.add_argument(
        '-p', '--port',
        type=int,
        default=GATEWAY_PORT,
        help=f"Gateway port (default: {GATEWAY_PORT})"
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help="Enable verbose output"
    )
    
    args = parser.parse_args()
    
    config = SimulatorConfig(
        gateway_host=args.host,
        gateway_port=args.port,
        num_devices=args.devices,
        duration=args.duration,
        verbose=args.verbose
    )
    
    simulator = DeviceSimulator(config)
    
    try:
        asyncio.run(simulator.run())
    except KeyboardInterrupt:
        print("\n\n👋 Interrupted by user")


if __name__ == "__main__":
    main()
