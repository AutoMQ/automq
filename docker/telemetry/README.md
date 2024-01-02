## Observing AutoMQ for Kafka

This document provides a guide on leveraging various observability services (OTel Collector, Prometheus, Tempo, Grafana)
to observe and monitor AutoMQ for Kafka.

### Limitations
1. S3Stream module utilizes OpenTelemetry for instrumenting (including metrics and traces). AutoMQ for Kafka supports different methods to export them:
   - Metrics:
     - Prometheus: Metrics can be exposed via Prometheus HTTP server, which can be scraped by your own Prometheus backend.  
     - OTLP: Metrics can be exported to OTel Collector via OTLP protocol, which can be then exported to multiple backend as configured.
     - Logs: Metrics can be directly logged to file system (logs/s3stream-metrics.log)
   - Traces: Traces are only supported to be exported via OTLP protocol to OTel Collector.
2. The original JMX metrics from Apache Kafka remain unchanged, you can observe them via JMX exporter or JConsole.
We are actively working on transforming them into OpenTelemetry metrics for exportation
3. The deployment configuration contained in this module is only meant for preview purpose, and should not be used in production environment
due to performance and security concerns.


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
   
    # or expose metrics for Prometheus backend to scrape
    # s3.telemetry.metrics.exporter.type=prometheus
    # s3.metrics.exporter.prom.host=127.0.0.1
    # s3.metrics.exporter.prom.port=9090 
   
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