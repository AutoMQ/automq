#!/bin/bash
pip install -r /app/gateway/requirements.txt
# Start Gateway in background
python /app/gateway/main.py &
GATEWAY_PID=$!
sleep 5

# Run Simulator
# Sequence: 10s Warmup -> Offline -> 30s Wait -> Online -> 30s Wait -> Quit
echo "status
sleep 10
offline
sleep 30
online
sleep 30
status
quit" | python /app/simulator/device_simulator.py --devices 1000 --host 127.0.0.1

kill $GATEWAY_PID
