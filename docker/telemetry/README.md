## Telemetry Deployment Demo for AutoMQ for Kafka
This module provides a demo for deploying multiple telemetry services (OTel Collector, Prometheus, Tempo, Grafana) 
for AutoMQ for Kafka.

### Quick Start
1. (Optional) set environment variables for data storage
    ```
    export $DATA_PATH=/tmp/telemetry
    ```
2. start services
    ```
    ./install.sh start
    ```
3. direct to Grafana UI to view default dashboard
    ```
    http://${hostname}:3000
    ```
4. stop and clean up
    ```
    ./install.sh remove
    ```
   
### Configuration for AutoMQ for Kafka
1. add telemetry configuration to configuration file
    ```
    # enable metrics recording
    s3.telemetry.metrics.enable=true
   
    # use OTLP exporter to export metrics to OTel Collector
    s3.telemetry.metrics.exporter.type=otlp
   
    # enable tracing
    s3.telemetry.tracer.enable=true
    s3.telemetry.tracer.span.scheduled.delay.ms=1000
    s3.telemetry.tracer.span.max.queue.size=5120
    s3.telemetry.tracer.span.max.batch.size=1024
   
    # OTel Collector endpoint
    s3.telemetry.exporter.otlp.endpoint=http://${your_host_name}:4317
    # Metrics report interval
    s3.telemetry.exporter.report.interval.ms=5000
    ```
2. start AutoMQ for Kafka and metrics and traces will be ready to view on Grafana