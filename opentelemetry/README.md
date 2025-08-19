# AutoMQ OpenTelemetry Module

## Overview

The AutoMQ OpenTelemetry module is a telemetry data collection and export component based on OpenTelemetry SDK, specifically designed for AutoMQ Kafka. This module provides unified telemetry data management capabilities, supporting the collection of JVM metrics, JMX metrics, and Yammer metrics, and can export data to Prometheus or OTLP-compatible backend systems.

## Core Features

### 1. Metrics Collection
- **JVM Metrics**: Automatically collect JVM runtime metrics including CPU, memory pools, garbage collection, threads, etc.
- **JMX Metrics**: Define and collect JMX Bean metrics through configuration files
- **Yammer Metrics**: Bridge existing Kafka Yammer metrics system to OpenTelemetry

### 2. Multiple Exporter Support
- **Prometheus**: Expose metrics in Prometheus format through HTTP server
- **OTLP**: Support both gRPC and HTTP/Protobuf protocols for exporting to OTLP backends

### 3. Flexible Configuration
- Support parameter settings through Properties configuration files
- Configurable export intervals, compression methods, timeout values, etc.
- Support metric cardinality limits to control memory usage

## Module Structure

```
com.automq.opentelemetry/
├── AutoMQTelemetryManager.java    # Main management class for initialization and lifecycle
├── TelemetryConfig.java           # Configuration management class
├── TelemetryConstants.java        # Constants definition
├── common/
│   └── MetricsUtils.java          # Metrics utility class
├── exporter/
│   ├── MetricsExporter.java       # Exporter interface
│   ├── MetricsExporterURI.java    # URI parser
│   ├── OTLPMetricsExporter.java   # OTLP exporter implementation
│   └── PrometheusMetricsExporter.java # Prometheus exporter implementation
└── yammer/
    ├── DeltaHistogram.java        # Delta histogram implementation
    ├── OTelMetricUtils.java       # OpenTelemetry metrics utilities
    ├── YammerMetricsProcessor.java # Yammer metrics processor
    └── YammerMetricsReporter.java  # Yammer metrics reporter
```

## Quick Start

### 1. Basic Usage

```java
import com.automq.opentelemetry.AutoMQTelemetryManager;
import java.util.Properties;

// Create configuration
Properties props = new Properties();
props.setProperty("automq.telemetry.exporter.uri", "prometheus://localhost:9090");
props.setProperty("service.name", "automq-kafka");
props.setProperty("service.instance.id", "broker-1");

// Initialize telemetry manager
AutoMQTelemetryManager telemetryManager = new AutoMQTelemetryManager(props);
telemetryManager.init();

// Start Yammer metrics reporting (optional)
MetricsRegistry yammerRegistry = // Get Kafka's Yammer registry
telemetryManager.startYammerMetricsReporter(yammerRegistry);

// Application running...

// Shutdown telemetry system
telemetryManager.shutdown();
```

### 2. Get Meter Instance

```java
// Get OpenTelemetry Meter for custom metrics
Meter meter = telemetryManager.getMeter();

// Create custom metrics
LongCounter requestCounter = meter
    .counterBuilder("http_requests_total")
    .setDescription("Total number of HTTP requests")
    .build();

requestCounter.add(1, Attributes.of(AttributeKey.stringKey("method"), "GET"));
```

## Configuration

### Basic Configuration

| Configuration | Description | Default Value | Example |
|---------------|-------------|---------------|---------|
| `automq.telemetry.exporter.uri` | Exporter URI | Empty (no export) | `prometheus://localhost:9090` |
| `service.name` | Service name | `unknown-service` | `automq-kafka` |
| `service.instance.id` | Service instance ID | `unknown-instance` | `broker-1` |

### Exporter Configuration

#### Prometheus Exporter
```properties
# Prometheus HTTP server configuration
automq.telemetry.exporter.uri=prometheus://localhost:9090
```

#### OTLP Exporter
```properties
# OTLP exporter configuration
automq.telemetry.exporter.uri=otlp://localhost:4317
automq.telemetry.exporter.interval.ms=60000
automq.telemetry.exporter.otlp.protocol=grpc
automq.telemetry.exporter.otlp.compression=gzip
automq.telemetry.exporter.otlp.timeout.ms=30000
```

### Advanced Configuration

| Configuration | Description | Default Value |
|---------------|-------------|---------------|
| `automq.telemetry.exporter.interval.ms` | Export interval (milliseconds) | `60000` |
| `automq.telemetry.exporter.otlp.protocol` | OTLP protocol | `grpc` |
| `automq.telemetry.exporter.otlp.compression` | OTLP compression method | `none` |
| `automq.telemetry.exporter.otlp.timeout.ms` | OTLP timeout (milliseconds) | `30000` |
| `automq.telemetry.jmx.config.paths` | JMX config file paths (comma-separated) | Empty |
| `automq.telemetry.metric.cardinality.limit` | Metric cardinality limit | `20000` |

### JMX Metrics Configuration

Define JMX metrics collection rules through YAML configuration files:

```properties
automq.telemetry.jmx.config.paths=/jmx-config.yaml,/kafka-jmx.yaml
```

#### Configuration File Requirements

1. **Directory Requirements**:
   - Configuration files must be placed in the project's classpath (e.g., `src/main/resources` directory)
   - Support subdirectory structure, e.g., `/config/jmx-metrics.yaml`

2. **Path Format**:
   - Paths must start with `/` to indicate starting from classpath root
   - Multiple configuration files separated by commas

3. **File Format**:
   - Use YAML format (`.yaml` or `.yml` extension)
   - Filenames can be customized, meaningful names are recommended

#### Recommended Directory Structure

```
src/main/resources/
├── jmx-kafka-broker.yaml      # Kafka Broker metrics configuration
├── jmx-kafka-consumer.yaml    # Kafka Consumer metrics configuration
├── jmx-kafka-producer.yaml    # Kafka Producer metrics configuration
└── config/
    ├── custom-jmx.yaml        # Custom JMX metrics configuration
    └── third-party-jmx.yaml   # Third-party component JMX configuration
```

JMX configuration file example (`jmx-config.yaml`):
```yaml
rules:
  - bean: kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec
    metricAttribute:
      name: kafka_server_broker_topic_messages_in_per_sec
      description: Messages in per second
      unit: "1/s"
    attributes:
      - name: topic
        value: topic
```

## Supported Metric Types

### 1. JVM Metrics
- Memory usage (heap memory, non-heap memory, memory pools)
- CPU usage
- Garbage collection statistics
- Thread states

### 2. Kafka Metrics
Through Yammer metrics bridging, supports the following types of Kafka metrics:
- `BytesInPerSec` - Bytes input per second
- `BytesOutPerSec` - Bytes output per second  
- `Size` - Log size (for identifying idle partitions)

### 3. Custom Metrics
Support creating custom metrics through OpenTelemetry API:
- Counter
- Gauge
- Histogram
- UpDownCounter

## Best Practices

### 1. Production Environment Configuration
```properties
# Service identification
service.name=automq-kafka
service.instance.id=${HOSTNAME}

# Prometheus export
automq.telemetry.exporter.uri=prometheus://0.0.0.0:9090

# Metric cardinality control
automq.telemetry.metric.cardinality.limit=10000

# JMX metrics (configure as needed)
automq.telemetry.jmx.config.paths=/kafka-broker-jmx.yaml
```

### 2. Development Environment Configuration
```properties
# Local development
service.name=automq-kafka-dev
service.instance.id=local-dev

# OTLP export to local Jaeger
automq.telemetry.exporter.uri=otlp://localhost:4317
automq.telemetry.exporter.interval.ms=10000
```

### 3. Resource Management
- Set appropriate metric cardinality limits to avoid memory leaks
- Call `shutdown()` method when application closes to release resources
- Monitor exporter health status

## Troubleshooting

### Common Issues

1. **Metrics not exported**
   - Check if `automq.telemetry.exporter.uri` configuration is correct
   - Verify target endpoint is reachable
   - Check error messages in logs

2. **JMX metrics missing**
   - Confirm JMX configuration file path is correct
   - Check YAML configuration file format
   - Verify JMX Bean exists

3. **High memory usage**
   - Lower `automq.telemetry.metric.cardinality.limit` value
   - Check for high cardinality labels
   - Consider increasing export interval

### Logging Configuration

Enable debug logging for more information:
```properties
logging.level.com.automq.opentelemetry=DEBUG
logging.level.io.opentelemetry=INFO
```

## Dependencies

- Java 8+
- OpenTelemetry SDK 1.30+
- Apache Commons Lang3
- SLF4J logging framework

## License

This module is open source under the Apache License 2.0.
