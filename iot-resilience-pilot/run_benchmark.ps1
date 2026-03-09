# IoT Burst Benchmark Orchestrator
$ErrorActionPreference = "Stop"

Write-Host "Starting Benchmark Orchestration..." -ForegroundColor Cyan

# 1. Start Ingestion Gateway in Background
Write-Host "Starting Gateway..." -ForegroundColor Yellow
$gatewayProcess = Start-Process -FilePath "python" -ArgumentList "d:\internships\tencent\automq\iot-resilience-pilot\gateway\main.py" -PassThru -NoNewWindow
Start-Sleep -Seconds 5 # Wait for Gateway to initialize

# 2. Prepare Simulator Commands
# Sequence: Run for 10s (Warmup) -> Offline (Outage) -> Wait 30s -> Online (Burst) -> Wait 30s -> Stats -> Quit
$simCommands = @"
status
sleep 10
offline
sleep 30
online
sleep 30
status
quit
"@

# 3. Run Simulator with Piped Commands
Write-Host "Starting Simulator (1000 Devices)..." -ForegroundColor Yellow
$simProcess = $simCommands | python d:\internships\tencent\automq\iot-resilience-pilot\simulator\device_simulator.py --devices 1000 --duration 120

# 4. Cleanup
Write-Host "Stopping Gateway..." -ForegroundColor Yellow
Stop-Process -Id $gatewayProcess.Id -Force

Write-Host "Benchmark Complete." -ForegroundColor Green
