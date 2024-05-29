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
In addition, we also provide the ability to transform selected JMX metrics to OTLP protocol, which can be exported via the above methods.
Supported transformed JMX metrics can be found at `core/src/main/resources/jmx/rules`.
3. The deployment configuration contained in this module is only meant for preview purpose, and should not be used in production environment
due to performance and security concerns.


### Quick Start
1. (Optional) set environment variables for data storage
    ```
    export DATA_PATH=/tmp/telemetry
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
   
    # use OTLP exporter to export metrics to backend service that supports OTLP protocol
    s3.telemetry.metrics.exporter.type=otlp
   
    # or expose metrics for Prometheus backend to scrape
    # s3.telemetry.metrics.exporter.type=prometheus
    # s3.metrics.exporter.prom.host=127.0.0.1
    # s3.metrics.exporter.prom.port=9090
   
    # OTLP backend endpoint (if using OTLP exporter)
    s3.telemetry.exporter.otlp.endpoint=http://${your_endpoint}
   
    # Protocol of OTLP exporter (http or grpc)
    s3.telemetry.exporter.otlp.protocol=http
   
    # Sample OTLP endpoints
    # OTel Collector (gRPC): http://${endpoint}:4317
    # OTel Collector (HTTP): http://${endpoint}:4318/v1/metrics
    # Prometheus (OpenSource) OTLP receiver: http://${endpoint}:9090/api/v1/otlp/v1/metrics
   
    # Metrics report interval
    s3.telemetry.exporter.report.interval.ms=5000
    ```
2. start AutoMQ for Kafka and metrics and traces will be ready to view on Grafana