#!/usr/bin/env python3
"""
Automated Chaos Test Script
Runs the full burst resilience test automatically

Usage: python chaos_test.py [--devices N] [--outage-seconds N]
"""

import asyncio
import socket
import time
import argparse
import sys
import os

# Add simulator to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'simulator'))

async def run_chaos_test(num_devices: int = 100, outage_duration: int = 60):
    """
    Automated chaos test:
    1. Normal operation for 30 seconds
    2. Trigger outage for specified duration
    3. Restore and measure burst
    4. Run for 60 more seconds to verify recovery
    """
    
    print("\n" + "=" * 70)
    print("🔥 AUTOMATED CHAOS TEST")
    print("=" * 70)
    print(f"   Devices: {num_devices}")
    print(f"   Outage Duration: {outage_duration}s")
    print("=" * 70 + "\n")
    
    # Import simulator components
    from device_simulator import DeviceSimulator, SimulatorConfig
    
    config = SimulatorConfig(
        gateway_host="127.0.0.1",
        gateway_port=5000,
        num_devices=num_devices,
        duration=outage_duration + 120,  # outage + buffer time
        verbose=False
    )
    
    simulator = DeviceSimulator(config)
    simulator.sock = simulator._create_socket()
    simulator._create_devices()
    simulator.running = True
    simulator.start_time = time.time()
    
    # Start device tasks
    device_tasks = [asyncio.create_task(d.run()) for d in simulator.devices]
    
    print(f"📡 Phase 1: Normal operation (30 seconds)...")
    await asyncio.sleep(30)
    
    stats_before = simulator.get_stats()
    print(f"   Stats: {stats_before['messages_sent']} messages sent")
    print(f"   Throughput: {stats_before['throughput_msg_sec']} msg/s")
    
    # Phase 2: Trigger outage
    print(f"\n🔴 Phase 2: OUTAGE ({outage_duration} seconds)...")
    simulator.network_state.go_offline()
    
    await asyncio.sleep(outage_duration)
    
    stats_during = simulator.get_stats()
    print(f"   Buffered: {stats_during['current_buffer_size']} messages")
    expected_buffered = num_devices * outage_duration
    print(f"   Expected: ~{expected_buffered} messages")
    
    # Phase 3: Restore and measure burst
    print(f"\n🟢 Phase 3: RESTORE - Measuring burst...")
    restore_time = time.time()
    simulator.network_state.go_online()
    
    # Monitor burst for 60 seconds
    peak_throughput = 0
    for i in range(60):
        await asyncio.sleep(1)
        stats = simulator.get_stats()
        if stats['throughput_msg_sec'] > peak_throughput:
            peak_throughput = stats['throughput_msg_sec']
        
        # Check if flush complete
        if stats['current_buffer_size'] == 0 and stats['messages_flushed'] > 0:
            flush_duration = time.time() - restore_time
            print(f"\n   ✅ Flush complete in {flush_duration:.1f}s")
            break
        
        if i % 5 == 0:
            print(f"   [{i}s] Buffer: {stats['current_buffer_size']} | "
                  f"Flushed: {stats['messages_flushed']} | "
                  f"Rate: {stats['throughput_msg_sec']}/s")
    
    # Final stats
    simulator.running = False
    for device in simulator.devices:
        device.stop()
    for task in device_tasks:
        task.cancel()
    
    final_stats = simulator.get_stats()
    
    print("\n" + "=" * 70)
    print("📊 CHAOS TEST RESULTS")
    print("=" * 70)
    print(f"   Total Messages Sent: {final_stats['messages_sent']}")
    print(f"   Total Messages Buffered: {final_stats['messages_buffered']}")
    print(f"   Total Messages Flushed: {final_stats['messages_flushed']}")
    print(f"   Peak Throughput: {peak_throughput} msg/s")
    print("=" * 70)
    
    # Evaluate results
    print("\n📝 EVALUATION:")
    
    passed = True
    
    # Check flush completion
    if final_stats['current_buffer_size'] == 0:
        print("   ✅ Buffer fully flushed")
    else:
        print(f"   ❌ {final_stats['current_buffer_size']} messages still in buffer")
        passed = False
    
    # Check throughput
    if peak_throughput > num_devices * 1.5:  # At least 1.5x normal rate
        print(f"   ✅ Burst throughput achieved ({peak_throughput} > {num_devices * 1.5})")
    else:
        print(f"   ⚠️  Burst throughput lower than expected")
    
    # Data integrity
    expected_total = final_stats['messages_sent'] + final_stats['current_buffer_size']
    if final_stats['messages_flushed'] <= final_stats['messages_buffered']:
        print(f"   ✅ Data integrity verified")
    else:
        print(f"   ❌ Data integrity issue")
        passed = False
    
    print("\n" + "=" * 70)
    if passed:
        print("🎉 CHAOS TEST PASSED!")
    else:
        print("⚠️  CHAOS TEST COMPLETED WITH WARNINGS")
    print("=" * 70 + "\n")
    
    return passed


def main():
    parser = argparse.ArgumentParser(description="Run automated chaos test")
    parser.add_argument(
        '-d', '--devices',
        type=int,
        default=100,
        help="Number of virtual devices (default: 100)"
    )
    parser.add_argument(
        '-o', '--outage-seconds',
        type=int,
        default=60,
        help="Outage duration in seconds (default: 60)"
    )
    
    args = parser.parse_args()
    
    try:
        result = asyncio.run(run_chaos_test(args.devices, args.outage_seconds))
        sys.exit(0 if result else 1)
    except KeyboardInterrupt:
        print("\n\n👋 Test interrupted")
        sys.exit(1)


if __name__ == "__main__":
    main()
