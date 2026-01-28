#!/bin/bash
# Linux Kernel Tuning for High-Performance IoT Gateway
# Run as root: sudo ./tune_linux.sh

echo "============================================="
echo "Linux Kernel Tuning for IoT Gateway"
echo "============================================="

# UDP Buffer Tuning (Critical for burst absorption)
echo "Tuning UDP buffers..."
sysctl -w net.core.rmem_max=26214400          # 25MB max receive buffer
sysctl -w net.core.rmem_default=26214400       # 25MB default receive buffer
sysctl -w net.core.wmem_max=26214400          # 25MB max send buffer
sysctl -w net.core.wmem_default=26214400       # 25MB default send buffer
sysctl -w net.core.netdev_max_backlog=65536   # Increase backlog queue

# Network socket buffer limits
sysctl -w net.ipv4.udp_mem="26214400 26214400 26214400"
sysctl -w net.ipv4.udp_rmem_min=8192
sysctl -w net.ipv4.udp_wmem_min=8192

# TCP tuning (for Kafka connections)
echo "Tuning TCP stack..."
sysctl -w net.ipv4.tcp_rmem="4096 87380 26214400"
sysctl -w net.ipv4.tcp_wmem="4096 65536 26214400"
sysctl -w net.ipv4.tcp_max_syn_backlog=65536
sysctl -w net.core.somaxconn=65536

# File descriptor limits
echo "Tuning file descriptors..."
sysctl -w fs.file-max=2097152
sysctl -w fs.nr_open=2097152

# Virtual memory tuning
echo "Tuning virtual memory..."
sysctl -w vm.swappiness=10                    # Reduce swap usage
sysctl -w vm.dirty_ratio=60                   # Allow more dirty pages
sysctl -w vm.dirty_background_ratio=30        # Start background flush later

echo ""
echo "Current settings:"
echo "  rmem_max: $(sysctl -n net.core.rmem_max)"
echo "  wmem_max: $(sysctl -n net.core.wmem_max)"
echo "  netdev_max_backlog: $(sysctl -n net.core.netdev_max_backlog)"
echo ""
echo "To persist after reboot, add these to /etc/sysctl.conf"
echo "============================================="
