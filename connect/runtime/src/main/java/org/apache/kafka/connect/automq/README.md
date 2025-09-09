# Kafka Connect OpenTelemetry Metrics Integration

## Overview

This integration allows Kafka Connect to export metrics through the AutoMQ OpenTelemetry module, enabling unified observability across your Kafka ecosystem. The integration supports multiple export formats including Prometheus, OTLP, Remote Write, and S3.

## Features

- **Unified Metrics Export**: Export Connect metrics through multiple backends (Prometheus, OTLP, Remote Write, S3)
- **Authentication Support**: Support for various authentication methods including Basic Auth, Bearer Token, AWS SigV4, and Azure AD
- **Automatic Type Detection**: Automatically converts Kafka metrics to appropriate OpenTelemetry instruments
- **Flexible Filtering**: Include/exclude metrics based on configurable patterns
- **Low-overhead**: Minimal performance impact on Connect workers

## Configuration

### 1. Enable the MetricsReporter

Add the following to your Kafka Connect configuration file (`connect-distributed.properties` or `connect-standalone.properties`):

```properties
# Enable OpenTelemetry MetricsReporter
metric.reporters=org.apache.kafka.connect.automq.OpenTelemetryMetricsReporter

# OpenTelemetry configuration
opentelemetry.metrics.enabled=true
opentelemetry.metrics.prefix=kafka.connect

# Optional: Filter metrics
opentelemetry.metrics.include.pattern=.*connector.*|.*task.*|.*worker.*
opentelemetry.metrics.exclude.pattern=.*jmx.*|.*debug.*
```

### 2. AutoMQ Telemetry Configuration

Ensure the AutoMQ telemetry is properly configured. Add these properties to your application configuration:

#### Prometheus Export
```properties
# Telemetry export configuration
automq.telemetry.exporter.uri=prometheus://localhost:9464

# Service identification
service.name=kafka-connect
service.instance.id=connect-worker-1

# Export settings
automq.telemetry.exporter.interval.ms=30000
automq.telemetry.metric.cardinality.limit=10000
```

#### Remote Write Export
```properties
# Basic Remote Write configuration
automq.telemetry.exporter.uri=rw://?endpoint=http://prometheus.example.com:9090/api/v1/write&auth=no_auth&maxBatchSize=1000000

# With Basic Authentication
automq.telemetry.exporter.uri=rw://?endpoint=http://prometheus.example.com:9090/api/v1/write&auth=basic&username=user&password=pass&maxBatchSize=1000000

# With Bearer Token Authentication
automq.telemetry.exporter.uri=rw://?endpoint=http://prometheus.example.com:9090/api/v1/write&auth=bearer&token=your_token&maxBatchSize=1000000

# With SSL skip verification
automq.telemetry.exporter.uri=rw://?endpoint=https://prometheus.example.com:9090/api/v1/write&auth=bearer&token=your_token&insecureSkipVerify=true&maxBatchSize=1000000

# AWS Managed Prometheus (AMP) with SigV4
automq.telemetry.exporter.uri=rw://?endpoint=https://aps-workspaces.us-west-2.amazonaws.com/workspaces/ws-xxx/api/v1/remote_write&auth=sigv4&region=us-west-2&accessKey=ACCESS_KEY&secretKey=SECRET_KEY&maxBatchSize=1000000

# Azure Monitor with Azure AD
automq.telemetry.exporter.uri=rw://?endpoint=https://prometheus.monitor.azure.com/api/v1/write&auth=azuread&cloud=azure_public&clientId=CLIENT_ID&clientSecret=CLIENT_SECRET&tenantId=TENANT_ID&maxBatchSize=1000000

# With custom headers
automq.telemetry.exporter.uri=rw://?endpoint=http://prometheus.example.com:9090/api/v1/write&auth=no_auth&maxBatchSize=1000000&header_X-Custom-Header=value&header_Authorization-Extra=extra_token

# Service identification
service.name=kafka-connect
service.instance.id=connect-worker-1
```

#### OTLP Export
```properties
# OTLP export (for OpenTelemetry Collector, Jaeger, etc.)
automq.telemetry.exporter.uri=otlp://localhost:4317
automq.telemetry.exporter.otlp.protocol=grpc
automq.telemetry.exporter.otlp.compression=gzip

# Service identification
service.name=kafka-connect
service.instance.id=connect-worker-1
```

## Programmatic Usage

### 1. Initialize Telemetry Manager

```java
import com.automq.opentelemetry.AutoMQTelemetryManager;
import java.util.Properties;

// Initialize AutoMQ telemetry before starting Kafka Connect
Properties telemetryProps = new Properties();
telemetryProps.setProperty("automq.telemetry.exporter.uri", "prometheus://localhost:9090");
telemetryProps.setProperty("service.name", "kafka-connect");
telemetryProps.setProperty("service.instance.id", "worker-1");

// Initialize singleton instance
AutoMQTelemetryManager.initializeInstance(telemetryProps);

// Now start Kafka Connect - it will automatically use the OpenTelemetryMetricsReporter
```

### 2. Shutdown

```java
// When shutting down your application
AutoMQTelemetryManager.shutdownInstance();
```

## Exported Metrics

The integration automatically converts Kafka Connect metrics to OpenTelemetry format:

### Metric Naming Convention
- **Format**: `kafka.connect.{group}.{metric_name}`
- **Example**: `kafka.connect.connector.task.batch.size.avg` → `kafka.connect.connector_task_batch_size_avg`

### Metric Types
- **Counters**: Metrics containing "total", "count", "error", "failure"
- **Gauges**: All other numeric metrics (rates, averages, sizes, etc.)

### Attributes
Kafka metric tags are converted to OpenTelemetry attributes:
- `connector` → `connector`
- `task` → `task`
- `worker-id` → `worker_id`
- Plus standard attributes: `metric.group`, `service.name`, `service.instance.id`

## Example Metrics

Common Kafka Connect metrics that will be exported:

```
# Connector metrics
kafka.connect.connector.startup.attempts.total
kafka.connect.connector.startup.success.total
kafka.connect.connector.startup.failure.total

# Task metrics  
kafka.connect.connector.task.batch.size.avg
kafka.connect.connector.task.batch.size.max
kafka.connect.connector.task.offset.commit.avg.time.ms

# Worker metrics
kafka.connect.worker.connector.count
kafka.connect.worker.task.count
kafka.connect.worker.connector.startup.attempts.total
```

## Configuration Options

### OpenTelemetry MetricsReporter Options

| Property | Description | Default | Example |
|----------|-------------|---------|---------|
| `opentelemetry.metrics.enabled` | Enable/disable metrics export | `true` | `false` |
| `opentelemetry.metrics.prefix` | Metric name prefix | `kafka.connect` | `my.connect` |
| `opentelemetry.metrics.include.pattern` | Regex for included metrics | All metrics | `.*connector.*` |
| `opentelemetry.metrics.exclude.pattern` | Regex for excluded metrics | None | `.*jmx.*` |

### AutoMQ Telemetry Options

| Property | Description | Default |
|----------|-------------|---------|
| `automq.telemetry.exporter.uri` | Exporter endpoint | Empty |
| `automq.telemetry.exporter.interval.ms` | Export interval | `60000` |
| `automq.telemetry.metric.cardinality.limit` | Max metric cardinality | `20000` |

## Monitoring Examples

### Prometheus Queries

```promql
# Connector count by worker
kafka_connect_worker_connector_count

# Task failure rate
rate(kafka_connect_connector_task_startup_failure_total[5m])

# Average batch processing time
kafka_connect_connector_task_batch_size_avg

# Connector startup success rate
rate(kafka_connect_connector_startup_success_total[5m]) / 
rate(kafka_connect_connector_startup_attempts_total[5m])
```

### Grafana Dashboard

Common panels to create:

1. **Connector Health**: Count of running/failed connectors
2. **Task Performance**: Batch size, processing time, throughput
3. **Error Rates**: Failed startups, task failures
4. **Resource Usage**: Combined with JVM metrics from AutoMQ telemetry

## Troubleshooting

### Common Issues

1. **Metrics not appearing**
   ```
   Check logs for: "AutoMQTelemetryManager is not initialized"
   Solution: Ensure AutoMQTelemetryManager.initializeInstance() is called before Connect starts
   ```

2. **High cardinality warnings**
   ```
   Solution: Use include/exclude patterns to filter metrics
   ```

3. **Missing dependencies**
   ```
   Ensure connect-runtime depends on the opentelemetry module
   ```

### Debug Logging

Enable debug logging to troubleshoot:

```properties
log4j.logger.org.apache.kafka.connect.automq=DEBUG
log4j.logger.com.automq.opentelemetry=DEBUG
```

## Integration with Existing Monitoring

This integration works alongside:
- Existing JMX metrics (not replaced)
- Kafka broker metrics via AutoMQ telemetry
- Application-specific metrics
- Third-party monitoring tools

The OpenTelemetry integration provides a unified export path while preserving existing monitoring setups.
