#!/bin/bash
# Kernel Tuning for High-Throughput IoT Ingestion
# WARNING: This requires root privileges (sudo)

set -e

echo "Applying Kernel Optimizations for 100k+ msg/sec..."

# 1. Increase UDP & TCP Receive/Send Buffers (25MB)
# Prevents packet drops when Gateway is paused or overwhelmed
sysctl -w net.core.rmem_max=26214400
sysctl -w net.core.wmem_max=26214400
sysctl -w net.core.rmem_default=26214400
sysctl -w net.core.wmem_default=26214400

# 2. Network Device Backlog
# Allow more packets to queue at the network card level before the kernel processes them
sysctl -w net.core.netdev_max_backlog=10000

# 3. Connection Tracking & Backlog
# Critical for handling thousands of concurrent TCP connections from Gateway -> Broker
sysctl -w net.ipv4.tcp_max_syn_backlog=4096
sysctl -w net.core.somaxconn=4096

# 4. Ephemeral Ports
# Ensure we don't run out of ports for outgoing connections
sysctl -w net.ipv4.ip_local_port_range="1024 65535"

# 5. TCP optimization
sysctl -w net.ipv4.tcp_fastopen=3
sysctl -w net.ipv4.tcp_tw_reuse=1

echo "Done. Verify with 'sysctl -a | grep net.core.rmem'"
